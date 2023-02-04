from pydoc import doc
import requests
import os
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import numpy as np
import math
import re
from math import isnan
from json import JSONDecoder 
import uuid
import requests
import uuid
import difflib 
import os
import pandas as pd
import json
import dateutil.parser as dparser
import pytz
from decimal import Decimal

##General functions to access Field-database###


def loadconfigs(configpath):
    #loads a json file from configpath as dict
    try:
        with open(configpath) as configfile:
            config = json.load(configfile)
        return config
    except:
        print('Filepath or JSON invalid.')

    

def couchDB_APIs(config, db_name = None):
    #creates the urls of the couchdb-api endpoints needed to access the data
    if not db_name:
        db_name = config['db_name']
    api = {}
    try:
        api['db_name'] = db_name
        api['auth'] = eval(config['auth'])
        api['find'] = config['db_url'] + '/' + db_name + '/_find'
        api['base'] = config['db_url'] + '/' + db_name 
        api['bulk'] = config['db_url'] + '/' + db_name + '/_bulk_docs'
        api['all_docs'] = config['db_url'] + '/' + db_name + '/_all_docs'
        api['all_dbs'] = config['db_url'] + '/' + '_all_dbs'
    except KeyError:
        print('Necessarry keys and values are missing in the config.json file')
    
    return api


def getAllDocs(api):
    try:
        response = requests.get(api['base'] , auth=api['auth'])    
        result = json.loads(response.text)
        print('The database ' + api['db_name'] + ' contains ' + str(result['doc_count']) + ' docs.',)
    except:
        print(api['base'])
        print('Cannot connect to database, is it on?')
    if result['doc_count'] > 10000:
        collect = {"total_rows":0,"rows":[], "offset": 0}
        limit = math.ceil(result['doc_count'] / 10000)
        for i in range(limit):    
            response = requests.get(api['all_docs'], auth=api['auth'], params={'limit':10000, 'include_docs':True, 'skip': i * 10000})
            i = i + 1
            result = json.loads(response.text)
            print('This is round ' + str(i) + 'offset :', str(result['offset']) )
            collect['total_rows'] = collect['total_rows'] + result['total_rows']
            collect['offset'] = result['offset']
            collect['rows'] = collect['rows'] + result['rows']
    else:
        response = requests.get(api['all_docs'], auth=api['auth'],params = {'include_docs':True})
        collect = json.loads(response.text)
    return collect

### General functions to convert documents from couchdb to pandas dataframe and reverse ###

def DOCtoDF(DOC):
    DFdocs = pd.DataFrame(DOC)
    print(DFdocs.columns)
    DFdocs = DFdocs.drop('resource', axis=1)
    DFresources = pd.DataFrame([i['resource'] for i in DOC])
    for col in DFdocs.columns:
        DFresources[str(col)]=DFdocs[str(col)]
    docfields = DFdocs.columns

    return DFresources, docfields


def allDocsToDf(DOC):
    outsideDF = pd.DataFrame([dict['doc'] for dict in DOC['rows']])
    outsideDF.drop(columns=['resource','_attachments'] , axis=1, inplace=True)
    insideDOC = [dict['doc'].get('resource') for dict in DOC['rows']]
    insideDOC = [i for i in insideDOC if i]
    print(len(insideDOC))
    DFdocs = pd.DataFrame(insideDOC)
    docfields = list(outsideDF.columns)
    print(docfields)
    for col in docfields:
        DFdocs[str(col)]=outsideDF[str(col)]
    return DFdocs, docfields
   

def DFtoDOCv1(DF, docfields):
    columns = [i for i in DF.columns if not i in docfields]
    print(DF.columns)
    DOC = []
    for index,row in DF.iterrows():
        toRessourceRow = row
        for col in docfields:
            try:
                toRessourceRow = toRessourceRow.drop(col)
            except:
                continue
        toRessourceRow=toRessourceRow.dropna() 
        row['resource']= toRessourceRow.to_dict()
        row = row.drop(columns)
        dict = row.to_dict()
        clean_dict = {k: dict[k] for k in dict.keys() if not isinstance(dict[k], (float)) or not isnan(dict[k])}

        DOC.append(clean_dict)
    
    DOChull={}
    DOChull['docs']=DOC
    return DOChull


def DFtoDOC(DFresources, docfields):
    columns = [i for i in DFresources.columns if not i in docfields]
    print(columns)
    DOC = []
    for index,row in DFresources.iterrows():
        cleanrow=row.dropna() 
        mydict = cleanrow.to_dict()
        cleandict = {}
        for k,v in mydict.items():
            if isinstance(v, str):
                if v:
                    cleandict[k]=v
            if isinstance(v, int):
                cleandict[k]=v
            if isinstance(v, float):
                if not isnan(v):
                    cleandict[k]=v
            if type(v) is dict:
                if v:
                    cleandict[k]=v
            if isinstance(v, list):
                if v:
                    cleandict[k]=[]                    
                    for item in v:
                        if isinstance(item, str):
                                if item:
                                    cleandict[k].append(item)
                        if isinstance(item, int):
                            cleandict[k].append(item)
                        if isinstance(item, float):
                            if not isnan(item):
                                cleandict[k].append(item)
                        if type(item) is dict:
                            if item:
                                cleandict[k].append(item)
     
        #clean_dict = {k:v for k in dict.keys() if not dict[k]=='' or not isnan(dict[k])}
        row['resource']= cleandict
        for field in docfields:
            try:
                del row['resource'][field]
            except:
                pass
        row = row.drop(columns)
        row=row.dropna() 
        yourdict = row.to_dict()
        
        
        DOC.append(yourdict)
    
    DOChull={}
    DOChull['docs']=DOC
    return DOChull


def flatten(t):
    return [item for sublist in t for item in sublist]

def getListOfDBs(db_url, auth):
    pouchDB_url_alldbs = f'{db_url}/_all_dbs'
    response = requests.get(pouchDB_url_alldbs, auth=auth)
    result = json.loads(response.text)
    return result


def enterCreated(doc):
    now = datetime.now()
    daytoSec = now.strftime('%Y-%m-%dT%H:%M:%S')
    sec = "{:.3f}".format(Decimal(now.strftime('.%f')))
    doc['created'] = {}
    doc['created']['user'] = 'Script mhaibt'
    doc['created']['date'] = daytoSec + str(sec)[1:] + 'Z'
    return doc

def addModifiedEntry(series):
    now = datetime.now()
    entry = {}
    entry['user'] = 'Script mhaibt'
    daytoSec = now.strftime('%Y-%m-%dT%H:%M:%S')
    sec = "{:.3f}".format(Decimal(now.strftime('.%f')))
    entry['date'] = daytoSec + str(sec)[1:] + 'Z'
    if not 'modified' in series.keys():
        series['modified']=[]
    series['modified'].append(entry)
    return series


def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n): 
        yield l[i:i + n]
def bulkSaveChanges(api, DOC, db_name=None):
    if not db_name:
        db_name = api['db_name']
    chunks = list(divide_chunks(DOC['docs'], 200))
    for chunk in chunks:
        #print(json.dumps(chunk, indent=4, sort_keys=True))
        chunkhull = {'docs':[]}
        chunkhull['docs'] = chunk
        answer = requests.post(api['bulk'] , auth=api['auth'], json=chunkhull)
        print(answer)
    return print('Documents uploaded')



def expand_dict_column(df, column_name):
    expansion = df[column_name].apply(pd.Series)
    expansion = expansion.rename(columns={col: f"{column_name}_{col}" for col in expansion.columns if col != column_name})
    expanded_df = pd.concat([df, expansion], axis=1)
    return expanded_df


def moveValues2otherField(series, inputfield, outputfield, valuelist):
    if isinstance(series[inputfield], str):
        if series[inputfield] in valuelist:
            output = series[inputfield]
            series[inputfield] = ''
    if isinstance(series[inputfield], list):
        output = []
        for item in series[inputfield]:
            if isinstance(item, str):
                if item in valuelist:
                    output.append(item)
                    series[inputfield].remove(item)

    if outputfield in series.keys():
        if isinstance(series[outputfield], str):
            series[outputfield] = str(output).replace('[','').replace(']','')
        if isinstance(series[outputfield], list):
            series[outputfield] = series[outputfield] + output
    return series






def handleduplicate(df, field):
    print(len(df[field]))
    groupsize = df.groupby( field ).size()
    cumcount = df.groupby( field ).cumcount()
    df = df.set_index(field)
    df['groupsize']= groupsize
    df = df.reset_index()
    df['cumcount']= cumcount
    pd.set_option("display.max_rows", None, "display.max_columns", None)
    print(df['cumcount'])
    dfnew = pd.DataFrame()
    for index, row in df.iterrows():
        if int(row['cumcount']) == 0 and int(row['groupsize'])== 1:
            row[field + '_undup'] = row[field]
        else: 
            row[field + '_undup'] = str(row[field]) + '_' + str(row['cumcount'])
        dfnew = dfnew.append(row)


def formatIdentifier(series):
    series['identifier']= str(series['identifier']) + '_' + str(series['REC_NO_'])
    return series

def formatDrawing2DrawingId(series):
    if series['identifier'] and series['DrawingId']:
        series['DrawingId']= str(series['identifier']) + '_' + str(series['DrawingId'])
    return series

def makeUuid (series, fieldname):
    series[fieldname] = str(uuid.uuid4())
    return series

def createTypeDocs (series, docshull, useuuid, useidentifier, liesWithin= None, isDepictedIn= None):
    doc = {}   
    doc['_id'] = str(series[useuuid])
    if 'rev' in series.keys():
        doc['_rev'] = series['rev']
    doc['modified'] = []
    doc = enterCreated(doc)
    resource = doc['resource'] = {}
    resource['type'] = 'Type'
    resource['id'] = str(series[useuuid])
    resource['identifier'] = str(series[useidentifier])
    #resource['id'] = str(series['catalogcoverdrawing_uuid'])
    resource['shortDescription'] = 'infoframeid: ' + str(series.get('infoframeid_clean')) + ' ' + series['detection_classesname'] + ' ' +series['HRID']
    #resource['originalFilename'] = os.path.basename(series['figure_path'])
    #resource['width'] = int(series['figure_width'])
    #resource['height'] = int(series['figure_height'])
    #resource['id'] = str(series['figure_tmpid'])
    if 'textall_clean' in series.keys():
        resource['description'] = 'textall: ' + str(series['textall_raw'] )
    if 'pageinfo_clean' in series.keys():
        resource['description'] += 'pageinfo: ' + str(series['pageinfo_clean'])
    if 'figureinfo_clean' in series.keys():
        resource['description'] += 'figureinfo: ' + str(series['figureinfo_clean'])
    resource['literature'] = []
    if series['pub_key'] == 'ZenonID':
        litdict = {'zenonId' : str(series['pub_value']), 'quotation' : str(series['pub_quote'])}
    if series.get("pageid_clean") is not None:
        litdict['page'] = str(series['pageid_clean'])
    if series.get("figureid_clean") is not None:
        litdict['figure'] = str(series['figureid_clean'])
    resource['literature'].append(litdict)
    resource['relations'] = {}
    if liesWithin:
        resource['relations']['liesWithin'] = [] 
        resource['relations']['liesWithin'].append(str(series[liesWithin]))
    if isDepictedIn:
        resource['relations']['isDepictedIn'] = []
        resource['relations']['isDepictedIn'].append(str(series[isDepictedIn]))
    doc['resource'] = resource
    
    return docshull['docs'].append(doc)

def attachImagesToDoc(series, isDepictedIn, DOC):
    if DOC['resource']['relations'].get('isDepictedIn') is None:
        DOC['resource']['relations']['isDepictedIn'] = []
    return DOC['resource']['relations']['isDepictedIn'].append(str(series[isDepictedIn]))


def getCatalogCover(df):
    if 'catalog_cover_pdfpage' in df.columns:
        firstrow = df.iloc[0]
        convert_from_path(firstrow['pubpdf_path'], fmt='png', thread_count=1, output_file= 'CatalogCover_'+firstrow['catalog_id'], first_page=int(firstrow['catalog_cover_pdfpage']),dpi=200, single_file=True, paths_only=False, use_pdftocairo=True, output_folder=firstrow['pubfolder_path'])
        
        firstrow['catalogcoverpath']=os.path.join(firstrow['pubfolder_path'],'CatalogCover_'+firstrow['catalog_id']+'.png')
        img = cv2.imread(firstrow['catalogcoverpath'])
        height, width, channels = img.shape
        firstrow['catalogcover_width'] = width
        firstrow['catalogcover_height'] = height

    return firstrow



def createRelationMapping(df, relation, missingReverseRelation= None):
    mapdictlist = []
    for index, row in df.iterrows():
        relationOfdoc = row['relations'].get(relation)
        if relationOfdoc:
            mapdictlist = mapdictlist + [{relation + 'id': relateddocid, 'docid': row['_id']  } for relateddocid in relationOfdoc]
    return mapdictlist

def writeReverseRelation(df, mapdicts, relation, rev_relation):
    newdf = pd.DataFrame()
    for index,row in df.iterrows():
        docids = [item['docid'] for item in mapdicts if item[relation + 'id']==row['_id']]
        if docids:
            if not 'relations' in row.keys():
                row['relations']= {}
            row['relations'][rev_relation]= docids
            newdf= newdf.append(row)
    return newdf

def formatListitems2periodvalues(series, inputfield):
    periodlist = series[inputfield]
    if type(periodlist)==list and len(periodlist)==2:
        series[inputfield] = {}
        series[inputfield]['value']=periodlist[0]
        series[inputfield]['endValue']=periodlist[1]
    if type(periodlist)==list and len(periodlist)==1:
        series[inputfield] = {}
        series[inputfield]['value']=periodlist[0]
        series[inputfield]['endValue']=periodlist[0]
    return series

def adaptListitemsByMap(series,inputfield,normmap):
    if type(series[inputfield]) == list:
        outputfield = []
        for i in series[inputfield]:
            try:
                #print('input color: ', i)
                match = normmap[i]
                outputfield.append(match)
            except:
                #print('No match found in normmap')
                if not i in ['None','none','nan']:
                    outputfield.append(i)
        if series[inputfield]:
            series[inputfield]=outputfield
        else : series[inputfield]= None
        #print(series[inputfield])
    
    return series


def fuzzyAdaptStringsByNormstrings(series,inputfield,normitems, fuzziness=0.85, keepunmatchedvalues = True):
    if type(series[inputfield]) == str:
        try:
            #print('input color: ', i)
            match =difflib.get_close_matches( series[inputfield], normitems,1,fuzziness)[0]
            #print('match color: ', match)
            if not match in ['None','none','nan']:
                series[inputfield] = match
            if match in ['None','none','nan']:
                series[inputfield] = None
        except:
            print('No norm found for: ',series[inputfield])
            if not keepunmatchedvalues:
                series[inputfield] = None
    return series

def sort_df_by_list(df, column_name, order_list):
    order_dict = {value: index for index, value in enumerate(order_list)}
    df[column_name] = df[column_name].map(order_dict)
    df = df.sort_values(by=[column_name])
    return df
    
def sort_series_by_list(series, sort_list):
    sort_index = [i for i in sort_list if i in series.index]
    return series.loc[sort_index].reindex(sort_index)

def fuzzyAdaptListitemsByNormitems(series,inputfield,normitems, fuzziness=0.85):
    if type(series[inputfield]) == list:
        outputfield = []
    
        #print('In: ', targetdfseries[id])

        
        for i in series[inputfield]:
            try:
                #print('input color: ', i)
                match =difflib.get_close_matches( i, normitems,1,fuzziness)[0]
                #print('match color: ', match)
                if not match in ['None','none','nan']:
                    outputfield.append(match)
            except:
                #print('No fuzzy match found.')
                if not i in ['None','none','nan']:
                    outputfield.append(i)
        if series[inputfield]:
            series[inputfield]=outputfield
        else : series[inputfield]= None
        #print(series[inputfield])
    
    return series

def mergelistitems(series, inputfield):
    if series[inputfield].InstanceOf(list):
        mergedlistitems = series[inputfield]




def strings2splitlistitems(series, inputfield, replacemap = {'example':'example','example2':'example2'},regex='; |, |-|\s|\*|\n'):
    strValue = None
    for word, replacement in replacemap.items():
        series[inputfield] = str(series[inputfield]).replace(word, replacement)

    #series[inputfield] = strValue.split()
    splitlistitems = re.split(regex,series[inputfield])
    series[inputfield]= splitlistitems
    return series


def createRelationFromField(series, inputfield, relation):
    if 'relations' in series.keys() :
        try:
            if isnan(series['relations']):
                series['relations'] = {}
        except:
            pass
        
        series['relations'].setdefault(relation, []).append(series[inputfield])
        series['relations'][relation]=list(set(series['relations'][relation]))

    return series

def combineFramesByFixId(inputdf,targetdf,id,combinefields, replacemap={'example':'example'}):
    for word, replacement in replacemap.items(): 
        #print(inputdf[id])
        inputdf[id] = inputdf[id].str.replace(word, replacement, regex=False)
        targetdf[id] = targetdf[id].str.replace(word, replacement, regex=False)
    combinefields = combinefields + [id]
    targetdf = pd.merge(left=targetdf, right=inputdf[combinefields], how='left', left_on=id, right_on=id)
    return targetdf

def createDictColumn(series, outputfield):
    series[outputfield]= {}
    return series         

def combineFramesByFuzzyId(targetdf,inputdfex,id,combinefields,replacemap = {'example':'example','example2':'example2'}, fuzziness=0.85, oneonone=False):
    inputdf = inputdfex
    targetdfout = pd.DataFrame()
    for word, replacement in replacemap.items():
            targetdf[id] = targetdf[id].str.replace(word, replacement, regex=False)
            inputdf[id] = inputdf[id].str.replace(word, replacement, regex=False)
    for row, targetdfseries in targetdf.iterrows():
        
        #print('In: ', targetdfseries[id])
        try:
            #print('Try match: ', targetdfseries[id])
            match =difflib.get_close_matches( targetdfseries[id], inputdf[id],1,fuzziness)[0]
            fuzzyresult=inputdf[inputdf[id]==match].iloc[0]
            targetdfseries[combinefields[0]]=fuzzyresult[combinefields[0]]
            
            if oneonone:
                    inputdf = inputdf[inputdf[id] != match]
                    #print('Removed match from inputdf ', len(inputdf))
                
        except:
            pass
        targetdfout = targetdfout.append(targetdfseries)
    return targetdfout

def fuzzy_search_with_value(data, search_key, max_ratio=0.6):
    def _search(data, search_key, path=None):
        if path is None:
            path = []
        for key in data.keys():
            ratio = difflib.SequenceMatcher(None, search_key, key).ratio()
            if ratio > max_ratio:
                yield path + [key], data[key]
            if isinstance(data[key], dict):
                yield from _search(data[key], search_key, path + [key])

    return list(_search(data, search_key))

def download_json_file(url):
    response = requests.get(url)
    if response.status_code == 200:
        print(response)
        return response.json()
    else:
        raise Exception("Failed to download JSON file. Response code: {}".format(response.status_code))
    
def commaNumber2int(series, inputfield):
    series[inputfield] = int(float(series[inputfield].replace(',','.')))
    return series

def singleStrfield2List(series,inputfield):
    series[inputfield] = [series[inputfield]]
    return series



def simpleDiameter2dimensionDiameter(series):
    try:
        diameter = float(series['DIAMETER_I'].replace(',','.'))
        if not diameter == 0:
            #print(series['DIAMETER_I'])
            #print(diameter)
            dimobj = {
                    "value": int(diameter * 10000),
                    "inputValue": diameter,
                    "measurementPosition": "",
                    "measurementComment": "",
                    "inputUnit": "cm",
                    "isImprecise": False,
                    "isRange": False,
                    "label": diameter
                }
            series['dimensionDiameter'] = []
            series['dimensionDiameter'].append(dimobj)
            #print('Diameter: ', series['dimensionDiameter'])
    except:
        pass
    return series

def docsByCreationdate(series, startdate, enddate=datetime.now()):
    utc=pytz.UTC
    startdate = dparser.parse(startdate,fuzzy=True).replace(tzinfo=None)
    #print(startdate)
    creationdatedate = dparser.parse(series['created'].get('date'),fuzzy=True).replace(tzinfo=None) 
    #print(creationdatedate)
    #print(enddate)
    if startdate <= creationdatedate <= enddate.replace(tzinfo=None):
        #print(creationdatedate, ' BETWEEN')
        return True
    else:
        return False


    

def format2FindDate(series, inputfield, outputfield):
    try:   
        finddate = dparser.parse(series[inputfield],fuzzy=False)
        series[outputfield] = finddate.strftime('%d.%m.%Y')
        return series
    except :
        return series






def idOfIdentifier(identifier, auth, pouchDB_url_find):
    queryByIdentifier={'selector':{}}
    queryByIdentifier['selector']['resource.identifier'] = {'$eq':str(identifier)}
    response = requests.post(pouchDB_url_find, auth=auth, json=queryByIdentifier)
    result = json.loads(response.text)
    #print (result)
    return result['docs'][0]['resource']['id']

def identifierOfId(id, auth, pouchDB_url_find):
    queryByIdentifier={'selector':{}}
    queryByIdentifier['selector']['resource.id'] = {'$eq':str(id)}
    response = requests.post(pouchDB_url_find, auth=auth, json=queryByIdentifier)
    result = json.loads(response.text)
    #print (result)
    return result['docs'][0]['resource']['identifier']


def getDocsRecordedInIdentifier(identifier, auth, pouchDB_url_find):
    querydict={'selector':{}}
    querydict['selector']['resource.relations.isRecordedIn'] = {'$elemMatch': str(idOfIdentifier(str(identifier), auth=auth, pouchDB_url_find=pouchDB_url_find))}
    response = requests.post(pouchDB_url_find, auth=auth, json=querydict)
    result = json.loads(response.text)
    return result

def objwalk(obj, path=(), memo=None):
    if memo is None:
        memo = set()
    iterator = None
    if isinstance(obj, Mapping):
        iterator = iteritems
    #elif isinstance(obj, (Sequence, Set)) and not isinstance(obj, string_types) and not isinstance(obj, np.ndarray):
        #iterator = enumerate
    if iterator:
        if id(obj) not in memo:
            memo.add(id(obj))
            for path_component, value in iterator(obj):
                for result in objwalk(value, path + (path_component,), memo):
                    yield result
            memo.remove(id(obj))
    else:
        yield path, obj

def find(element, JSON):        
    paths = element.replace('(','').replace(')','').split(",")
    data = JSON
    for i in range(0,len(paths)):
        data = data[paths[i]]
    return data

def getIdaifieldConfigs(idaifieldconfigpath):
    # read file
    with open(os.path.join(idaifieldconfigpath,Path('Library/Categories.json')), 'r') as myfile:
        data=myfile.read()
    # parse file
    categories = json.loads(data)

    return categories

def selectOfResourceTypes(listOfIncludedTypes, allDocs):
    selectedResources = [obj for obj in allDocs if obj['resource']['type'] in listOfIncludedTypes ]
    return selectedResources
#categories = getIdaifieldConfigs(idaifieldconfigpath)

def DocsStructure(allDocs):
    allDocs.sort(key=lambda x:x['resource']['type'])
    categoryStructureList = []
    for k,v in groupby(allDocs,key=lambda x:x['resource']['type']):
        print(k,v)
        for obj in v:
            categoryStructure={}
            categoryStructure['category']= k
            for path,obj in objwalk(obj, path=(), memo=None):
                categoryStructure['fieldpath'] = path
                categoryStructure['datatype'] = str(type(obj))
                #print(categoryStructure)
                categoryStructureList.append(categoryStructure.copy())
    categoriesStructureDF = pd.DataFrame(categoryStructureList)
    categoriesStructureDF = categoriesStructureDF.drop_duplicates()
    return categoriesStructureDF


