# nomisweb
Simple Python Wrapper for nomis / UK Census API

`!pip install git+https://github.com/ouseful-datasupply/nomisweb.git`

then:

```python
import nomis_api_wrapper as napi

nomis=napi.NOMIS_CONFIG()

nomis.dataset_lookup('NM_1_1')

nomis.nomis_codes_datasets()
```


Usage examples:

https://gist.github.com/psychemedia/b1fadd04af01ccd6a3a1