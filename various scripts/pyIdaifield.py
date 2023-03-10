from pydoc import doc
import requests
import uuid
import difflib 
import os
import pandas as pd
import json
from datetime import datetime
import dateutil.parser as dparser
import pytz
from pathlib import Path
import operator
import numpy as np
import math
import matplotlib.pyplot as plt
import re
from math import isnan
import itertools
from json import JSONDecoder
import seaborn as sns
import colorcet as cc
from decimal import Decimal  


##Functions for dataset adaptions in order to import to iDAI.field 3

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

def getAllDocs(db_url, auth, db_name):
    pouchDB_url_all = f'{db_url}/{db_name}/_all_docs'
    pouchDB_url_base = f'{db_url}/{db_name}'
    response = requests.get(pouchDB_url_base, auth=auth)
    result = json.loads(response.text)
    print('The database contains so much docs: ', result['doc_count'])
    if result['doc_count'] > 10000:
        collect = {"total_rows":0,"rows":[], "offset": 0}
        limit = math.ceil(result['doc_count'] / 10000)

        for i in range(limit):
            
            response = requests.get(pouchDB_url_all, auth=auth, params={'limit':10000, 'include_docs':True, 'skip': i * 10000})
            i = i + 1
            result = json.loads(response.text)
            print('This is round ' + str(i) + 'offset :', str(result['offset']) )
            collect['total_rows'] = collect['total_rows'] + result['total_rows']
            collect['offset'] = result['offset']
            collect['rows'] = collect['rows'] + result['rows']
    else:
        response = requests.get(pouchDB_url_all, auth=auth,params = {'include_docs':True})
        collect = json.loads(response.text)
    return collect

def flatten(t):
    return [item for sublist in t for item in sublist]

def getListOfDBs(db_url, auth):
    pouchDB_url_alldbs = f'{db_url}/_all_dbs'
    response = requests.get(pouchDB_url_alldbs, auth=auth)
    result = json.loads(response.text)
    return result



def addModifiedEntry(series):
    if not 'modified' in series.keys():
        series['modified']=[]
    try:
        if isnan(series['modified']):
            series['modified']=[]
    except:
        pass
    now = datetime.now()
    entry = {}
    entry['user'] = 'Script mhaibt'
    daytoSec = now.strftime('%Y-%m-%dT%H:%M:%S')
    sec = "{:.3f}".format(Decimal(now.strftime('.%f')))
    entry['date'] = daytoSec + str(sec)[1:] + 'Z'
    series['modified'].append(entry)
    return series



def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n): 
        yield l[i:i + n]
def bulkSaveChanges(db_url, auth, db_name, DOC, bulksize=1000):
    pouchDB_url_bulk = f'{db_url}/{db_name}/_bulk_docs'
    chunks = list(divide_chunks(DOC['docs'], bulksize))
    for chunk in chunks:
        #print(json.dumps(chunk, indent=4, sort_keys=True))
        chunkhull = {'docs':[]}
        chunkhull['docs'] = chunk
        answer = requests.post(pouchDB_url_bulk , auth=auth, json=chunkhull)
        print(answer)
    return print('Documents uploaded')