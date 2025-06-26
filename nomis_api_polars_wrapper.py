import polars as pl
import urllib.parse
import re
import json
from typing import Optional, Dict, List, Union
from functools import lru_cache


class NOMIS_CONFIG_POLARS:
    """
    Polars-powered NOMIS API client with lazy loading capabilities.
    Optimized for memory efficiency and performance with large datasets.
    """

    def __init__(self):
        self.NOMIS_STUB = "https://www.nomisweb.co.uk/api/v01/dataset/"
        self.url = self.NOMIS_STUB
        self._codes_lazy = None
        self._metadata_cache = {}

    def _url_encode(self, params: Optional[Dict] = None) -> str:
        """URL encode parameters for API calls."""
        if params is not None and params != "" and params != {}:
            return f"?{urllib.parse.urlencode(params)}"
        return ""

    def _describe_dataset(self, df: pl.DataFrame) -> None:
        """Print dataset descriptions in a readable format."""
        for row in df.iter_rows(named=True):
            print(f"{row['idx']} - {row['name']}: {row['description']}\n")

    def _describe_metadata(
        self, idx: str, metadata: Dict, keys: List[str], pretty: bool = True
    ) -> None:
        """Print metadata descriptions."""
        if not pretty:
            for key in keys:
                print(f"---- {key} ----")
                df = metadata[key]
                for row in df.iter_rows(named=True):
                    print(f"{row['dimension']} - {row['description']}: {row['value']}")
        else:
            print(
                f'The following dimensions are available for {idx} ({self.dataset_lookup_property(idx, "name")}):\n'
            )
            for key in keys:
                df = metadata[key]
                items = [
                    f"{row['description']} ({row['value']})"
                    for row in df.iter_rows(named=True)
                ]
                print(f' - {key}: {", ".join(items)}')

    def help_url(self, idx: str = "NM_7_1") -> None:
        """Display help information for a dataset."""
        metadata = self.nomis_code_metadata(idx)
        keys = list(metadata.keys())
        keys.remove("core")
        print(
            f'Dataset {idx} ({self.dataset_lookup_property(idx, "name")}) supports the following dimensions: {", ".join(keys)}.'
        )

    @lru_cache(maxsize=128)
    def dataset_lookup_property(
        self, idx: Optional[str] = None, prop: Optional[str] = None
    ) -> str:
        """Get a specific property from dataset lookup with caching."""
        if idx is None or prop is None:
            return ""

        df = self.dataset_lookup(idx)
        if prop in df.columns:
            return str(df.select(pl.col(prop)).item(0, 0))
        return ""

    def dataset_lookup(
        self,
        idx: Optional[Union[str, List[str]]] = None,
        dimensions: bool = False,
        describe: bool = False,
    ) -> pl.DataFrame:
        """
        Lazy dataset lookup with optional filtering.
        Only materializes the data when needed.
        """
        if self._codes_lazy is None:
            self._codes_lazy = self._get_datasets_lazy(dimensions=True)

        # Start with lazy frame
        query = self._codes_lazy

        if idx is not None:
            if isinstance(idx, str):
                idx = [idx]
            query = query.filter(pl.col("idx").is_in(idx))

        # Select columns based on dimensions parameter
        if not dimensions:
            cols_to_select = [
                col for col in query.columns if col not in ["dimension", "concept"]
            ]
            query = query.select(cols_to_select).unique()

        # Materialize the result
        df = query.collect()

        if describe:
            self._describe_dataset(df)
        else:
            return df

    def _get_geo_from_postcode(
        self, postcode: str, areacode: Optional[str] = None
    ) -> str:
        """Generate geography string from postcode."""
        if areacode is None:
            areacode = "district"

        codemap = {"district": 486}
        if areacode in codemap:
            areacode = codemap[areacode]

        return f"POSTCODE|{postcode};{areacode}"

    def _dimension_mapper(self, idx: str, dim: str, dims: str) -> str:
        """Map natural language dimension values to codes."""
        if dim is not None:
            sc = self._nomis_codes_dimension_grab(dim, idx)

            # Create mapping dictionary efficiently with Polars
            mapping_df = sc.select(
                [pl.col("description").cast(pl.Utf8), pl.col("value")]
            )

            dimmap = dict(mapping_df.iter_rows())
            keys = sorted(dimmap.keys(), key=len, reverse=True)

            for s in keys:
                pattern = re.compile(s, re.IGNORECASE)
                dims = pattern.sub(str(dimmap[s]), str(dims))

        return dims

    def _sex_map(self, idx: str, sex: str) -> str:
        """Map sex dimension values."""
        return self._dimension_mapper(idx, "sex", sex)

    def _get_geo_code_helper(self, helper: str) -> tuple:
        """Get geography codes for common helpers."""
        geo_helpers = {
            "UK_WPC_2010": ("2092957697TYPE460", None),
            "LA_district": ("2092957697TYPE464", None),
        }
        return geo_helpers.get(helper, (None, None))

    def get_geo_code(
        self,
        value: Optional[str] = None,
        desc: Optional[str] = None,
        search: Optional[str] = None,
        helper: Optional[str] = None,
        chase: bool = False,
        idx: Optional[str] = None,
    ) -> pl.DataFrame:
        """Get geography codes with lazy evaluation."""
        if idx is None:
            print("You need to provide a base dataset idx.")
            return pl.DataFrame()

        if helper is not None:
            value, desc = self._get_geo_code_helper(helper)

        if chase and value:
            chaser = self.nomis_codes_geog(idx=idx, geography=value)
            if search is not None:
                chase_candidates = chaser.filter(
                    pl.col("description").str.contains(search)
                ).select(["description", "value"])
            else:
                chase_candidates = chaser.select(["description", "value"])

            locs = chase_candidates.get_column("value").to_list()
            if locs:
                value = ",".join(map(str, locs))

        geog = self.nomis_codes_geog(idx=idx, geography=value)

        if desc is not None:
            candidates = geog.select(["description", "value"])
            for row in candidates.iter_rows(named=True):
                if row["description"] == desc:
                    geog = self.nomis_codes_geog(idx=idx, geography=row["value"])
                    break

        if search is not None:
            result = geog.filter(pl.col("description").str.contains(search)).select(
                ["description", "value"]
            )
        else:
            result = geog.select(["description", "value"])

        return result.rename({"value": "geog"})

    def _get_datasets_lazy(
        self, search: Optional[str] = None, dimensions: bool = False
    ) -> pl.LazyFrame:
        """
        Get datasets as a lazy frame for efficient processing.
        Only loads JSON once and creates lazy frame for subsequent operations.
        """
        url = "http://www.nomisweb.co.uk/api/v01/dataset/def.sdmx.json"
        if search is not None:
            url = f'{url}{self._url_encode({"search": search})}'

        # Load JSON data
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read())

        keyfamilies = data.get("structure", {}).get("keyfamilies", {})
        if not keyfamilies:
            return pl.LazyFrame()

        datasets = []
        for keyfamily in keyfamilies.get("keyfamily", []):
            kf = {
                "agency": keyfamily.get("agencyid", ""),
                "idx": keyfamily.get("id", ""),
                "name": keyfamily.get("name", {}).get("value", ""),
                "description": keyfamily.get("description", {}).get("value", ""),
                "source": "",
            }

            # Process annotations
            annotations = keyfamily.get("annotations", {}).get("annotation", [])
            for annotation in annotations:
                if annotation.get("annotationtitle") == "contenttype/sources":
                    kf["source"] = annotation.get("annotationtext", "")
                elif (
                    annotation.get("annotationtitle") == "MetadataText0"
                    and not kf["description"]
                ):
                    kf["description"] = annotation.get("annotationtext", "")

            if dimensions:
                dimensions_list = keyfamily.get("components", {}).get("dimension", [])
                for dimension in dimensions_list:
                    kf_copy = kf.copy()
                    kf_copy["dimension"] = dimension.get("codelist", "")
                    kf_copy["concept"] = dimension.get("conceptref", "")
                    datasets.append(kf_copy)
            else:
                datasets.append(kf)

        return pl.DataFrame(datasets).lazy()

    def nomis_codes_datasets(
        self, search: Optional[str] = None, dimensions: bool = False
    ) -> pl.DataFrame:
        """Get datasets with optional search filtering."""
        lazy_df = self._get_datasets_lazy(search, dimensions)
        return lazy_df.collect()

    def _nomis_codes_parser_lazy(self, url: str) -> pl.LazyFrame:
        """Parse NOMIS codes response into lazy frame."""
        with urllib.request.urlopen(url) as response:
            jdata = json.loads(response.read())

        cl = jdata.get("structure", {}).get("codelists", {})
        if not cl:
            return pl.LazyFrame()

        codes_data = []
        header_id = jdata.get("header", {}).get("structure", {}).get("id", "")

        for codelist in cl.get("codelist", []):
            base_data = {
                "agencyid": codelist.get("agencyid", ""),
                "dataset": header_id,
                "dimension": codelist.get("id", ""),
                "name": codelist.get("name", {}).get("value", ""),
            }

            for code in codelist.get("code", []):
                code_data = base_data.copy()
                code_data["description"] = code.get("description", {}).get("value", "")
                code_data["value"] = code.get("value", "")
                codes_data.append(code_data)

        return pl.DataFrame(codes_data).lazy()

    def _nomis_codes_url_constructor(
        self, dim: str, idx: str, params: Optional[Dict] = None
    ) -> str:
        """Construct URL for dimension codes."""
        return f"{self.url}{idx}/{dim.lower()}.def.sdmx.json{self._url_encode(params)}"

    @lru_cache(maxsize=256)
    def _nomis_codes_dimension_grab(
        self, dim: str, idx: str, params: Optional[str] = None
    ) -> pl.DataFrame:
        """Get dimension codes with caching."""
        url = self._nomis_codes_url_constructor(dim, idx, None)
        return self._nomis_codes_parser_lazy(url).collect()

    def nomis_code_metadata(
        self, idx: str = "NM_1_1", describe: Optional[Union[str, List[str]]] = None
    ) -> Dict:
        """Get metadata for a dataset with caching."""
        if idx in self._metadata_cache:
            metadata = self._metadata_cache[idx]
        else:
            core = self.dataset_lookup(idx, dimensions=True)
            metadata = {"core": core}

            concepts = core.get_column("concept").unique().to_list()
            for dim in concepts:
                if dim:  # Skip empty concepts
                    dim_lower = str(dim).lower()
                    metadata[dim_lower] = self._nomis_codes_dimension_grab(
                        dim_lower, idx
                    )

            self._metadata_cache[idx] = metadata

        if describe == "all":
            keys = [k for k in metadata.keys() if k != "core"]
            self._describe_metadata(idx, metadata, keys)
        elif isinstance(describe, str) and describe in metadata:
            self._describe_metadata(idx, metadata, [describe])
        elif isinstance(describe, list):
            valid_keys = [k for k in describe if k in metadata]
            self._describe_metadata(idx, metadata, valid_keys)
        else:
            return metadata

    # Dimension-specific methods with lazy evaluation
    def nomis_codes_measures(self, idx: str = "NM_1_1") -> pl.DataFrame:
        return self._nomis_codes_dimension_grab("measures", idx)

    def nomis_codes_time(self, idx: str = "NM_1_1") -> pl.DataFrame:
        return self._nomis_codes_dimension_grab("time", idx)

    def nomis_codes_industry(self, idx: str = "NM_21_1") -> pl.DataFrame:
        return self._nomis_codes_dimension_grab("industry", idx)

    def nomis_codes_freq(self, idx: str = "NM_1_1") -> pl.DataFrame:
        return self._nomis_codes_dimension_grab("freq", idx)

    def nomis_codes_age_dur(self, idx: str = "NM_7_1") -> pl.DataFrame:
        return self._nomis_codes_dimension_grab("age_dur", idx)

    def nomis_codes_ethnicity(self, idx: str = "NM_118_1") -> pl.DataFrame:
        return self._nomis_codes_dimension_grab("ethnicity", idx)

    def nomis_codes_occupation(self, idx: str = "NM_7_1") -> pl.DataFrame:
        return self._nomis_codes_dimension_grab("occupation", idx)

    def nomis_codes_age(self, idx: str = "NM_18_1") -> pl.DataFrame:
        return self._nomis_codes_dimension_grab("age", idx)

    def nomis_codes_duration(self, idx: str = "NM_18_1") -> pl.DataFrame:
        return self._nomis_codes_dimension_grab("duration", idx)

    def nomis_codes_sex(
        self, idx: str = "NM_1_1", geography: Optional[str] = None
    ) -> pl.DataFrame:
        params = {}
        if geography is not None:
            params["geography"] = geography

        url = f"{self.url}{idx}/sex.def.sdmx.json{self._url_encode(params)}"
        return self._nomis_codes_parser_lazy(url).collect()

    def nomis_codes_geog(
        self,
        idx: str = "NM_1_1",
        geography: Optional[str] = None,
        search: Optional[str] = None,
    ) -> pl.DataFrame:
        params = {}
        geog_path = f"/{geography}" if geography else ""

        if search is not None:
            params["search"] = search

        url = f"{self.url}{idx}/geography{geog_path}.def.sdmx.json{self._url_encode(params)}"
        return self._nomis_codes_parser_lazy(url).collect()

    def nomis_codes_items(
        self,
        idx: str = "NM_1_1",
        geography: Optional[str] = None,
        sex: Optional[str] = None,
    ) -> pl.DataFrame:
        if sex:
            sex = self._sex_map(idx, sex)

        params = {}
        if geography is not None:
            params["geography"] = geography
        if sex is not None:
            params["sex"] = sex

        url = f"{self.url}{idx}/item.def.sdmx.json{self._url_encode(params)}"
        return self._nomis_codes_parser_lazy(url).collect()

    def _nomis_data_url(
        self,
        idx: str = "NM_1_1",
        postcode: Optional[str] = None,
        areacode: Optional[str] = None,
        select: Optional[List[str]] = None,
        **kwargs,
    ) -> str:
        """Construct data URL with lazy parameter resolution."""
        metadata = self.nomis_code_metadata(idx)

        # Geography from postcode
        if "geography" not in kwargs and postcode is not None:
            kwargs["geography"] = self._get_geo_from_postcode(postcode, areacode)

        # Map natural language dimension values
        for dim in set(metadata.keys()).intersection(kwargs.keys()):
            kwargs[dim] = self._dimension_mapper(idx, dim, kwargs[dim])

        # Default time period
        if "date" not in kwargs and "time" not in kwargs:
            kwargs["time"] = "latest"

        # Column selection
        if select and "select" not in kwargs:
            kwargs["select"] = ",".join(select)

        return f"{self.url}{idx}.data.csv{self._url_encode(kwargs)}"

    def nomis_data_lazy(
        self,
        idx: str = "NM_1_1",
        postcode: Optional[str] = None,
        areacode: Optional[str] = None,
        **kwargs,
    ) -> pl.LazyFrame:
        """
        Get NOMIS data as a lazy frame for efficient processing.
        Since NOMIS API doesn't support streaming CSV reads, we'll load the data
        first then convert to lazy for downstream operations.
        """
        # Get the data as DataFrame first (API limitation)
        df = self.nomis_data(idx, postcode, areacode, **kwargs)

        # Convert to lazy frame for efficient downstream processing
        return df.lazy()

    def nomis_data(
        self,
        idx: str = "NM_1_1",
        postcode: Optional[str] = None,
        areacode: Optional[str] = None,
        **kwargs,
    ) -> pl.DataFrame:
        """Get NOMIS data as materialized DataFrame."""
        url = self._nomis_data_url(idx, postcode, areacode, **kwargs)

        # Use pl.read_csv for direct URL reading (handles HTTP properly)
        try:
            df = pl.read_csv(url)
            return df.with_columns(pl.lit(idx).alias("_Code"))
        except Exception as e:
            print(f"Error reading CSV from URL: {url}")
            print(f"Error: {e}")
            # Fallback: try with urllib first
            import urllib.request
            import io

            try:
                with urllib.request.urlopen(url) as response:
                    csv_data = response.read().decode("utf-8")
                    df = pl.read_csv(io.StringIO(csv_data))
                    return df.with_columns(pl.lit(idx).alias("_Code"))
            except Exception as e2:
                print(f"Fallback also failed: {e2}")
                return pl.DataFrame()
