import requests
import os
import pandas as pd
import json
from datetime import datetime
from decimal import Decimal  
#from decimal import Decimal  
auth = ('', 'blub')
db_url = 'http://localhost:3000'
db_name = 'wes'
pouchDB_url_find = f'{db_url}/{db_name}/_find'
pouchDB_url_put = f'{db_url}/{db_name}/'

def idOfIdentifier(identifier, auth, pouchDB_url_find):
    queryByIdentifier={'selector':{}}
    queryByIdentifier['selector']['resource.identifier'] = {'$eq':str(identifier)}
    response = requests.post(pouchDB_url_find, auth=auth, json=queryByIdentifier)
    result = json.loads(response.text)
    #print (result)
    return result['docs'][0]['resource']['id']

def getDocsWithIdentifier(identifier, auth, pouchDB_url_find):
    queryByIdentifier={'selector':{}}
    queryByIdentifier['selector']['resource.identifier'] = {'$eq':str(identifier)}
    response = requests.post(pouchDB_url_find, auth=auth, json=queryByIdentifier)
    result = json.loads(response.text)
    #print (result)
    return result

def getDocsWithId(id, auth, pouchDB_url_find):
    queryByIdentifier={'selector':{}}
    queryByIdentifier['selector']['resource.id'] = {'$eq':str(id)}
    response = requests.post(pouchDB_url_find, auth=auth, json=queryByIdentifier)
    result = json.loads(response.text)
    #print (result)
    return result

def getDocsRecordedInIdentifier(identifier, auth, pouchDB_url_find):
    querydict={'selector':{}}
    querydict['selector']['resource.relations.isRecordedIn'] = {'$elemMatch': str(idOfIdentifier(str(identifier), auth=auth, pouchDB_url_find=pouchDB_url_find))}
    response = requests.post(pouchDB_url_find, auth=auth, json=querydict)
    result = json.loads(response.text)
    return result

def getDocsSameIdentifier(identifier, auth, pouchDB_url_find):
    querydict={'selector':{}}
    querydict['selector']['resource.relations.isRecordedIn'] = {'$elemMatch': str(idOfIdentifier(str(identifier), auth=auth, pouchDB_url_find=pouchDB_url_find))}
    response = requests.post(pouchDB_url_find, auth=auth, json=querydict)
    result = json.loads(response.text)
    return result

def addModifiedEntry(doc):
    now = datetime.now()
    entry = {}
    entry['user'] = 'Script mhaibt'
    daytoSec = now.strftime('%Y-%m-%dT%H:%M:')
    sec = Decimal(now.strftime('%S.%f'))

    entry['date'] = daytoSec + str(round(sec,3)) + 'Z'
    #float("%.3f" % (dt.second + dt.microsecond / 1e6)),
    #dt.strftime('%z')
    doc['modified'].append(entry)
    return doc

def saveChanges(doc, pouchDB_url_put, auth ):
    requests.put(pouchDB_url_put + doc['_id'], auth=auth, json=doc)


    

result = getDocsWithIdentifier('WESid_2_3', auth=auth, pouchDB_url_find=pouchDB_url_find)
print(result)
#for doc in result['docs']:
    #print (doc)
    #doc['resource']['relations']['isRecordedIn'].clear() 
    #doc['resource']['relations']['isRecordedIn'].append(str(idOfIdentifier('WarkaEnvironsSurvey', auth=auth, pouchDB_url_find=pouchDB_url_find)))
    #doc = addModifiedEntry(doc)
    #print (doc)
    #saveChanges(doc, pouchDB_url_put= pouchDB_url_put, auth= auth)

#result_df = pd.DataFrame(result)
#print(result)

