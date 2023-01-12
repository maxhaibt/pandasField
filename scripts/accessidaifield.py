import requests
import os
import pandas as pd
import json
from datetime import datetime
import operator
import itertools
from decimal import Decimal  
#from decimal import Decimal  
auth = ('', 'blub')
db_url = 'http://localhost:3000'
db_name = 'uruk'
pouchDB_url_find = f'{db_url}/{db_name}/_find'
pouchDB_url_put = f'{db_url}/{db_name}/'

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
    querydict['selector']['$not']['resource.relations.isRecordedIn'] = {'$elemMatch': str(idOfIdentifier(str(identifier), auth=auth, pouchDB_url_find=pouchDB_url_find))}
    response = requests.post(pouchDB_url_find, auth=auth, json=querydict)
    result = json.loads(response.text)
    return result
def getDocsNotRecordedInIdentifier(identifier, auth, pouchDB_url_find):
    querydict={'selector':{'$not':{'resource.relations.isRecordedIn':{}}}}
    selector1 = {'$in': [str(idOfIdentifier(str(identifier), auth=auth, pouchDB_url_find=pouchDB_url_find)), str(idOfIdentifier('StadtSurvey', auth=auth, pouchDB_url_find=pouchDB_url_find)) ]}
    querydict['selector']['$not']['resource.relations.isRecordedIn'] = selector1
    response = requests.post(pouchDB_url_find, auth=auth, json=querydict)
    result = json.loads(response.text)
    print (querydict)
    return result
def addModifiedEntry(doc):
    now = datetime.now()
    entry = {}
    entry['user'] = 'Script mhaibt'
    daytoSec = now.strftime('%Y-%m-%dT%H:%M:%S')
    sec = "{:.3f}".format(Decimal(now.strftime('.%f')))
    entry['date'] = daytoSec + str(sec)[1:] + 'Z'
    doc['modified'].append(entry)
    return doc

def saveChanges(doc, pouchDB_url_put, auth ):
    requests.put(pouchDB_url_put + doc['_id'], auth=auth, json=doc)

def statOfRessouceTypes(result):
    dataset = sorted(result['docs'], key= lambda x: x['resource']['type'])
    grouped = itertools.groupby(dataset,key = lambda x: x['resource']['type'])
    for k, v in grouped:
        print (k, len(list(v)))
    print('That was statOfRessouceTypes')
    return grouped

def statOfRecordedIn(result):
    dataset = sorted(result, key= lambda x: x['resource']['relations']['isRecordedIn'][0])
    grouped = itertools.groupby(dataset,key = lambda x: x['resource']['relations']['isRecordedIn'][0])
    for k, v in grouped:
        print (str(identifierOfId(str(k), auth=auth, pouchDB_url_find=pouchDB_url_find)), len(list(v)))
    print('That was statOfRecordedIn')
    return grouped
def statOfLiesWithin(result):
    dataset = sorted(result, key= lambda x: x['resource']['relations']['liesWithin'][0])
    grouped = itertools.groupby(dataset,key = lambda x: x['resource']['relations']['liesWithin'][0])
    for k, v in grouped:
        print (str(identifierOfId(str(k), auth=auth, pouchDB_url_find=pouchDB_url_find)), len(list(v)))
    print('That was statOfLiesWithin')
    return grouped

def filterOfResourceTypes(listOfNotIncludedTypes, result):
    filteredResources = [obj for obj in result['docs'] if not obj['resource']['type'] in listOfNotIncludedTypes ]
    return filteredResources
def selectOfResourceTypes(listOfIncludedTypes, result):
    selectedResources = [obj for obj in result if obj['resource']['type'] in listOfIncludedTypes ]
    return selectedResources
    

result = getDocsNotRecordedInIdentifier('WarkaEnvironsSurvey', auth=auth, pouchDB_url_find=pouchDB_url_find)
print(len(result['docs']))
#print(result)
grouped = statOfRessouceTypes(result)
#print (grouped)
listOfNotIncludedTypes = ['Drawing','Place','Project','Photo','Image','TypeCatalog','Survey']
filteredResources = filterOfResourceTypes(listOfNotIncludedTypes, result)
print(len(filteredResources))
p = statOfRecordedIn(filteredResources)
#listOfNotIncludedTypes = ['Drawing','Place','Project','Photo','Image','TypeCatalog','Survey']
#filteredResources = filterOfResourceTypes(listOfNotIncludedTypes, result)
#listOfIncludedTypes = ['SurveyUnit']
#selectedResources = selectOfResourceTypes(listOfIncludedTypes, filteredResources)
#print(selectedResources)
#x = statOfLiesWithin(selectedResources)
#p = statOfRessouceTypes(filteredResources)


#
#for doc in filteredResources:
    #print (doc)
    #doc['resource']['relations']['isRecordedIn'].clear() 
    #doc['resource']['relations']['isRecordedIn'].append(str(idOfIdentifier('WarkaEnvironsSurvey', auth=auth, pouchDB_url_find=pouchDB_url_find)))
    #doc = addModifiedEntry(doc)
    #print (doc)
    #saveChanges(doc, pouchDB_url_put= pouchDB_url_put, auth= auth)

#result_df = pd.DataFrame(result)
#print(result)

