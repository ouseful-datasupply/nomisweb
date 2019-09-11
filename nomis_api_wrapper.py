import pandas as pd
import urllib
import re

class NOMIS_CONFIG:
    #TO DO implement cache to cache list of datasets and dimensions associated with datasets (except time/date?)
    
    def __init__(self):
        NOMIS_STUB='https://www.nomisweb.co.uk/api/v01/dataset/'
        
        self.url=NOMIS_STUB
        self.codes=None
        self.metadata={}

    def _url_encode(self,params=None):
        if params is not None and params!='' and params != {}:
            #params='?{}'.format( '&'.join( ['{}={}'.format(p,params[p]) for p in params] ) )
            params='?{}'.format(urllib.parse.urlencode(params))
        else:
            params=''
        return params


    def _describe_dataset(self,df):
        for row in df.iterrows():
            dfr=row[1]
            print('{idx} - {name}: {description}\n'.format(idx=dfr['idx'],
                                                         name=dfr['name'],
                                                         description=dfr['description']) )
                                                                               
    def _describe_metadata(self,idx,df,keys,pretty=True):
        if not pretty:
            for key in keys:
                print( '---- {} ----'.format(key) )
                for row in df[key].iterrows():
                    dfr=row[1]
                    print('{dimension} - {description}: {value}'.format(dimension=dfr['dimension'],
                                                                 description=dfr['description'],
                                                                 value=dfr['value']) )
        else:
            print('The following dimensions are available for {idx} ({name}):\n'.format(
                    idx=idx, 
                    name=self.dataset_lookup_property(idx,'name')))
            for key in keys:
                items =['{} ({})'.format(row[1]['description'],row[1]['value']) for row in df[key].iterrows()]
                print( ' - {key}: {items}'.format(key=key,items=', '.join(items)) )
            
    def help_url(self,idx='NM_7_1'):
        metadata=self.nomis_code_metadata(idx)
        keys=list(metadata.keys())
        keys.remove('core')
        print('Dataset {idx} ({name}) supports the following dimensions: {dims}.'.format(
                idx=idx,
                dims=', '.join(keys),
                name=self.dataset_lookup_property(idx,'name')))

    def dataset_lookup_property(self,idx=None,prop=None):
        if idx is None or prop is None: return ''
        df=self.dataset_lookup(idx)

        if prop in df.columns: return str(df[prop][0])
        else: return ''
        
    def dataset_lookup(self,idx=None,dimensions=False,describe=False):
        ##dimensions used in sense of do we display them or not
        if self.codes is None:
            self.codes=self.nomis_codes_datasets(dimensions=True)
        
        if idx is not None:
            #Test if idx is a list or single string
            if isinstance(idx, str): idx=[idx]
            df=self.codes[self.codes['idx'].isin(idx)]
        else:
            df=self.codes[:]
        
        cols=df.columns.tolist() 
        if not dimensions:
            for col in ['dimension','concept']:
                cols.remove(col)
        df=df[cols].drop_duplicates().reset_index(drop=True)
        if describe: self._describe_dataset(df)
        else: return df
        
    def _get_geo_from_postcode(self, postcode, areacode=None):
        #Set a default
        if areacode is None:
            areacode='district'
            
        codemap={ 'district':486 }

        if areacode in codemap:
            areacode=codemap[areacode]
        
        return 'POSTCODE|{postcode};{code}'.format(postcode=postcode,code=areacode)

    
    def _dimension_mapper(self,idx,dim,dims):
        ''' dims is a string of comma separated values for a particular dimension '''       
        if dim is not None:
            sc=self._nomis_codes_dimension_grab(dim,idx,params=None)
            dimmap=dict(zip(sc['description'].astype(str),sc['value']))
            keys=dimmap.keys()
            #keys.sort(key=len, reverse=True)
            keys = sorted(keys, reverse=True)
            for s in keys:
                pattern = re.compile(s, re.IGNORECASE)
                dims=pattern.sub(str(dimmap[s]), str(dims))
        return dims
        
    def _sex_map(self,idx,sex):
        return self._dimension_mapper(idx,'sex',sex)
                
    def _get_geo_code_helper(self,helper):
        value=None
        desc=None

        #I am baking values in, but maybe they should be searched for and retrieved that way?
        if helper=='UK_WPC_2010':
            #UK Westminster Parliamentary Constituency
            value='2092957697TYPE460'
        elif helper=='LA_district':
            value='2092957697TYPE464'

        return value,desc

    def get_geo_code(self,value=None,desc=None, search=None, helper=None, chase=False):
        #The semantics of this are quite tricky
        #value is a code for a geography, the thing searched within
        #desc identifies a description within a geography - on a match it takes you to this lower geography
        #search is term to search (free text search) with the descriptions of areas returned
        #helper is in place for shortcuts

        #Given a local authority code, eg 1946157281, a report can be previewed at:
        ##https://www.nomisweb.co.uk/reports/lmp/la/1946157281/report.aspx
        #default
        if helper is not None:
            value,desc=self._get_geo_code_helper(helper)
        if chase:
            chaser= self.nomis_codes_geog(geography=value)
            if search is not None:
                chasecands=chaser[ chaser['description'].str.contains(search) ][['description','value']].values
            else:
                chasecands=chaser[['description','value']].values
            locs=[]
            for chasecand in chasecands:
                locs.append(chasecand[1])
            if len(locs): value=','.join(map(str,locs))

        geog=self.nomis_codes_geog(geography=value)
        if desc is not None:
            candidates=geog[['description','value']].values
            for candidate in candidates:
                if candidate[0]==desc:
                    geog=self.nomis_codes_geog(geography=candidate[1])

        if search is not None:
            retval=geog[ geog['description'].str.contains(search) ][['description','value']].values
        else:
            retval=geog[['description','value']].values

        return pd.DataFrame(retval,columns=['description','geog'])

    def _get_datasets(self,search=None):
        url='http://www.nomisweb.co.uk/api/v01/dataset/def.sdmx.json'
        if search is not None:
            url='{url}{params}'.format(url=url,params=self._url_encode({'search':search}))
        data=pd.read_json(url)
        return data

    def nomis_code_metadata(self,idx='NM_1_1',describe=None):
        if idx in self.metadata:
            metadata=self.metadata[idx]
        else:
            core=self.dataset_lookup(idx,dimensions=True)
            metadata={'core':core}
            for dim in core['concept'].str.lower():
                metadata[dim]=self._nomis_codes_dimension_grab(dim,idx,params=None)
        self.metadata[idx]=metadata       
        if describe=='all':
            keys= list(metadata.keys())
            keys.remove('core')
            self._describe_metadata(idx,metadata,keys)
        elif isinstance(describe, str) and describe in metadata.keys():
            self._describe_metadata(idx,metadata,[describe])
        elif isinstance(describe, list):
            self._describe_metadata(idx,metadata,describe)
        else:
            return metadata
        
        
    def nomis_codes_datasets(self,search=None,dimensions=False):
        #TO DO - by default, use local dataset list and search in specified cols;
        #  add additional parameter to force a search on API
        
        df=self._get_datasets(search)

        keyfamilies=df.loc['keyfamilies']['structure']
        if keyfamilies is None: return pd.DataFrame()
        
        datasets=[]
        for keyfamily in keyfamilies['keyfamily']:
            kf={'agency':keyfamily['agencyid'],
                'idx':keyfamily['id'],
                'name':keyfamily['name']['value'],
                'description': keyfamily['description']['value'] if 'description' in keyfamily else ''
                #'dimensions':[dimensions['codelist'] for dimensions in keyfamily['components']['dimension']]
            }

            if dimensions:
                for _dimensions in keyfamily['components']['dimension']:
                    kf['dimension']= _dimensions['codelist']
                    kf['concept']= _dimensions['conceptref']
                    datasets.append(kf.copy())
            else:
                datasets.append(kf.copy())
                
        return pd.DataFrame(datasets)

    def _nomis_codes_parser(self,url):
        jdata=pd.read_json(url)
        cl=jdata.loc['codelists']['structure']
        if cl is None: return pd.DataFrame()
        
        codes_data=[]
        for codelist in cl['codelist']:
            code_data={'agencyid':codelist['agencyid'],
                       'dataset':jdata.loc['header']['structure']['id'],
                       'dimension':codelist['id'],
                       'name':codelist['name']['value']
                      }
            for code in codelist['code']:
                code_data['description']=code['description']['value']
                code_data['value']=code['value']
                codes_data.append(code_data.copy())
        return pd.DataFrame(codes_data)

    #Generic mininal constructor
    def _nomis_codes_url_constructor(self,dim,idx,params=None):
        #This doesn't cope with geography properly that can insert an element into the path?
        return '{nomis}{idx}/{dim}.def.sdmx.json{params}'.format(nomis=self.url,
                                                                 idx=idx,
                                                                 dim=dim.lower(),
                                                                 params=self._url_encode(params))
    def _nomis_codes_dimension_grab(self,dim,idx,params=None):
        url=self._nomis_codes_url_constructor(dim,idx,params=None)
        return self._nomis_codes_parser(url)
    
    #Set up shorthand functions to call particular dimensions
    #Select appropriate datsets as default to demo the call
    def nomis_codes_measures(self,idx='NM_1_1'):
        url=self._nomis_codes_url_constructor('measures',idx)
        return self._nomis_codes_parser(url)
 
    def nomis_codes_time(self,idx='NM_1_1'):
        url=self._nomis_codes_url_constructor('time',idx)
        return self._nomis_codes_parser(url)

    def nomis_codes_industry(self,idx='NM_21_1'):
        url=self._nomis_codes_url_constructor('industry',idx)
        return self._nomis_codes_parser(url)
    
    def nomis_codes_freq(self,idx='NM_1_1'):
        url=url=self._nomis_codes_url_constructor('freq',idx)
        return self._nomis_codes_parser(url)

    def nomis_codes_age_dur(self,idx='NM_7_1'):
        url=url=self._nomis_codes_url_constructor('age_dur',idx)
        return self._nomis_codes_parser(url)

    def nomis_codes_ethnicity(self,idx='NM_118_1'):
        url=url=self._nomis_codes_url_constructor('ethnicity',idx)
        return self._nomis_codes_parser(url)
    
    def nomis_codes_occupation(self,idx='NM_7_1'):
        url=url=self._nomis_codes_url_constructor('occupation',idx)
        return self._nomis_codes_parser(url)
    
    def nomis_codes_age(self,idx='NM_18_1'):
        url=url=self._nomis_codes_url_constructor('age',idx)
        return self._nomis_codes_parser(url)
    
    def nomis_codes_duration(self,idx='NM_18_1'):
        url=url=self._nomis_codes_url_constructor('duration',idx)
        return self._nomis_codes_parser(url)
    

    def nomis_codes_sex(self,idx='NM_1_1',geography=None):
        params={}
        if geography is not None:
            params['geography']=geography

        url='{nomis}{idx}/sex.def.sdmx.json{params}'.format(nomis=self.url,
                                                           idx=idx,
                                                           params=self._url_encode(params))

        return self._nomis_codes_parser(url)
    
    def nomis_codes_geog(self,idx='NM_1_1',geography=None,search=None):
        params={}
        if geography is not None:
            geog='/{geog}'.format(geog=geography)
        else:
            geog=''

        if search is not None:
            params['search']=search
        
        url='{nomis}{idx}/geography{geog}.def.sdmx.json{params}'.format(nomis=self.url,
                                                                       idx=idx,geog=geog,
                                                                       params=self._url_encode(params))
            
        return self._nomis_codes_parser(url)
    
    def nomis_codes_items(self,idx='NM_1_1',geography=None,sex=None):
        sex=self._sex_map(idx,sex)
        params={}

        if geography is not None:
            params['geography']=geography
        if sex is not None:
            params['sex']=sex

        url='{nomis}{idx}/item.def.sdmx.json{params}'.format(nomis=self.url,
                                                            idx=idx,
                                                            params=self._url_encode(params))

        return self._nomis_codes_parser(url)

    #TO DO have a dataset_explain(idx) function that will print a description of a dataset,
    #summarise what dimensions are available, and the value they can take,
    #and provide a stub function usage example (with eligible parameters) to call it

    def _nomis_data_url(self,idx='NM_1_1',postcode=None, areacode=None, **kwargs):

        #TO DO
        #Add an explain=True parameter that will print a natural language summary of what the command is calling
        
        
        ###---Time/date info from nomis API docs---
        #Useful time options:
        ##"latest" - the latest available data for this dataset
        ##"previous" - the date prior to "latest"
        ##"prevyear" - the date one year prior to "latest"
        ##"first" - the oldest available data for this dataset
        ##Using the "time" concept you are limited to entering two dates, 
        ##a start and end. All dates between these are returned.
        
        #date is more flexible for ranges
        ##With the "date" parameter you can specify relative dates, 
        ##so for example if you wanted the latest date, three months and six months prior to that
        ##you could specify "date=latest,latestMINUS3,latestMINUS6". 
        ##You can use ranges with the "date" parameter, 
        ##e.g. if you wanted data for 12 months ago, together with all dates in the last six month
        ##up to latest you could specify "date=prevyear,latestMINUS5-latest".
        
        ##To illustrate the difference between using "date" and "time";
        ##if you specified "time=first,latest" in your URI you would get all dates from first to latest inclusive,
        ##whereas with "date=first,latest" your output would contain only the first and latest dates.
 
        metadata=self.nomis_code_metadata(idx)
    
        #HELPERS
    
        #Find geography from postcode
        if 'geography' not in kwargs and postcode is not None:
            kwargs['geography']=self._get_geo_from_postcode(postcode, areacode)

        #Map natural language dimension values to corresponding codes
        for dim in set( metadata.keys() ).intersection( kwargs.keys() ):
            kwargs[dim]=self._dimension_mapper(idx,dim,kwargs[dim])
        
        #Set a default time period to be latest
        if 'date' not in kwargs and 'time' not in kwargs:
            kwargs['time']='latest'

        
        #Set up a default projection for the returned columns
        cols=['geography_code','geography_name','measures_name','measures','date_code','date_name','obs_value']

        for k in ['sex','age','item']:
            if k in kwargs: cols.insert(len(cols)-1,'{}_name'.format(k))
        
        if 'select' not in kwargs:
            kwargs['select']=','.join(cols)
        
        url='{nomis}{idx}.data.csv{params}'.format(nomis=self.url,
                                                  idx=idx,
                                                  params=self._url_encode(kwargs))
        return url
    
    def _nomis_data(self,idx='NM_1_1',postcode=None, areacode=None, **kwargs):
        url=self._nomis_data_url(idx,postcode, areacode, **kwargs)

        df=pd.read_csv(url)
        df['_Code']=idx
        return df