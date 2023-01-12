from pydoc import doc
import requests
import os
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import operator
import numpy as np
import math

import matplotlib.pyplot as plt
import re
from math import isnan
import itertools
from json import JSONDecoder
from typing import TextIO
import seaborn as sns
import colorcet as cc
from decimal import Decimal  
#from decimal import Decimal  

#pouchDB_url_find = f'{db_url}/{db_name}/_find'
#ouchDB_url_put = f'{db_url}/{db_name}/'

#pouchDB_url_alldbs = f'{db_url}/_all_dbs'



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
    DF = DFresources
    columns = [i for i in DFresources.columns if not i in docfields]
    #print(columns)
    DOC = []
    for index,row in DF.iterrows():
        
        cleanrow = row.drop(docfields)
        cleanrow=cleanrow.dropna() 
        row['resource']= cleanrow.to_dict()
        row = row.drop(columns)
        dict = row.to_dict()
        clean_dict = {k: dict[k] for k in dict.keys() if not isinstance(dict[k], (float)) or not isnan(dict[k])}

        DOC.append(clean_dict)
    
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
def bulkSaveChanges(db_url, auth, db_name, DOC):
    pouchDB_url_bulk = f'{db_url}/{db_name}/_bulk_docs'
    chunks = list(divide_chunks(DOC['docs'], 200))
    for chunk in chunks:
        #print(json.dumps(chunk, indent=4, sort_keys=True))
        chunkhull = {'docs':[]}
        chunkhull['docs'] = chunk
        answer = requests.post(pouchDB_url_bulk , auth=auth, json=chunkhull)
        print(answer)
    return print('Documents uploaded')

def create_find_JSONL(df: pd.DataFrame, file: TextIO):
    FIND_template = '{"category":"","identifier":"","relations":{"isChildOf":"","isDepictedIn":[],"isInstanceOf":[]}}'
    FIND = json.loads(FIND_template)
    FIND["identifier"] = 'Find_' + str(df['figure_tmpid'])
    FIND["category"] = 'Pottery'

    relations = FIND["relations"]
    relations["isChildOf"] = 'Findspot_refferedtoin_' + \
        str(df['pub_key']) + '_' + str(df['pub_value'])
    InstanceOfList = relations["isInstanceOf"]
    typename = 'Type_' + str(df['figure_tmpid'])
    InstanceOfList.append(typename)
    depictedInList = relations["isDepictedIn"]
    imagename = str(df['figure_tmpid']) + '.png'
    depictedInList.append(imagename)
    json.dump(FIND, file)
    file.write("\n")


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

def create_constructivisttype_JSONL(df: pd.DataFrame, file: TextIO):
    TYPE_template = '{"category":"","identifier":"","relations":{"isChildOf":""}}'
    TYPE = json.loads(TYPE_template)
    TYPE["identifier"] = str(df['HRID'])
    TYPE["category"] = 'Type'
    relations = TYPE["relations"]
    relations["isChildOf"] = 'Catalog_' + \
        str(df['pub_key']) + '_' + str(df['pub_value'])
    json.dump(TYPE, file)
    file.write("\n")

def create_normativtype_JSONL(df: pd.DataFrame, file: TextIO):
    TYPE_template = '{"category":"","identifier":"", "shortDescription":"Extracted by Mining Shapes","literature":[{"quotation":"none","zenonId":""}],"relations":{"isChildOf":"","isDepictedIn":[]}}'
    TYPE = json.loads(TYPE_template)
    TYPE["identifier"] = str(df['HRID'])
    TYPE["category"] = 'Type'
    relations = TYPE["relations"]
    relations["isChildOf"] = 'Catalog_' + \
        str(df['pub_key']) + '_' + str(df['pub_value'])
    depictedInList = relations["isDepictedIn"]
    imagename = str(df['figure_tmpid']) + '.png'
    depictedInList.append(imagename)
    TYPE["shortDescription"] = 'PAGEID_RAW: ' + \
        str(df['pageid_raw']) + '   ' + 'PAGEINFO_RAW: ' + str(df['pageinfo_raw'])\
        + '   ' + 'FIGID_RAW: ' + str(df['figid_raw'])
    literature = TYPE["literature"]
    literature0 = literature[0]
    literature0['zenonId'] = str(df['pub_value'])

    literature0['quotation'] = 'p. ' + str(df['pageid_clean']) + ', fig. ' + str(df['figid_clean'])
    json.dump(TYPE, file)
    file.write("\n")


def create_drawing_JSONL(df: pd.DataFrame, file: TextIO):
    DRAWING_template = '{"category":"","identifier":"", "description":"Extracted by Mining Shapes","literature":[{"quotation":"none","zenonId":""}]}'
    DRAWING = json.loads(DRAWING_template)
    DRAWING["identifier"] = str(df['figure_tmpid']) + '.png'
    DRAWING["category"] = 'Drawing'
    DRAWING["description"] = 'PAGEID_RAW: ' + \
        str(df['pageid_raw']) + '   ' + 'PAGEINFO_RAW: ' + str(df['pageinfo_raw'])\
        + '   ' + 'FIGID_RAW: ' + str(df['figid_raw'])
    literature = DRAWING["literature"]
    literature0 = literature[0]
    literature0['zenonId'] = str(df['pub_value'])
    literature0['quotation'] = 'p. ' + str(df['pageid_clean']) + ', fig. ' + str(df['figid_clean'])

    json.dump(DRAWING, file)
    file.write("\n")


def create_catalog_JSONL(df: pd.DataFrame, file: TextIO):
    CATALOG_template = '{"category":"","identifier":"","shortDescription":"Extracted by Mining Shapes", "relations":{"isDepictedIn":[]}}'
    CATALOG = json.loads(CATALOG_template)
    CATALOG["identifier"] = 'Catalog_' + \
        str(df['pub_key']) + '_' + str(df['pub_value'])
    relations = CATALOG["relations"]
    depictedInList = relations["isDepictedIn"]
    depictedInList.append(
        'Catalogcover_' + str(df['pub_key']) + '_' + str(df['pub_value']) + '.png')
    CATALOG["category"] = 'TypeCatalog'
    json.dump(CATALOG, file)
    file.write("\n")


def create_trench_JSONL(df: pd.DataFrame, file: TextIO):
    TRENCH_template = '{"category":"","identifier":"","shortDescription":"Where have the Objects been found?"}'
    TRENCH = json.loads(TRENCH_template)
    TRENCH["identifier"] = 'Findspot_refferedtoin_' + \
        str(df['pub_key']) + '_' + str(df['pub_value'])
    TRENCH["category"] = 'Trench'
    json.dump(TRENCH, file)
    file.write("\n")
