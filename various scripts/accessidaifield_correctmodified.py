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

def getDocsRecordedInIdentifier(identifier, auth, pouchDB_url_find):
    querydict={'selector':{}}
    querydict['selector']['resource.relations.isRecordedIn'] = {'$elemMatch': str(idOfIdentifier(str(identifier), auth=auth, pouchDB_url_find=pouchDB_url_find))}
    response = requests.post(pouchDB_url_find, auth=auth, json=querydict)
    result = json.loads(response.text)
    return result

def getDocsNotRecordedInIdentifier(identifier, auth, pouchDB_url_find):
    querydict={'selector':{}}
    querydict['selector']['resource.relations.isRecordedIn'] = {'$not': str(idOfIdentifier(str(identifier), auth=auth, pouchDB_url_find=pouchDB_url_find)), '$not': str(idOfIdentifier(str('StadtSurvey'), auth=auth, pouchDB_url_find=pouchDB_url_find))}
    response = requests.post(pouchDB_url_find, auth=auth, json=querydict)
    result = json.loads(response.text)
    return result


def addModifiedEntry(doc):
    now = datetime.now()
    entry = {}
    entry['user'] = 'Script mhaibt'
    daytoSec = now.strftime('%Y-%m-%dT%H:%M:')
    sec = round(Decimal(now.strftime('%S.%f')),3)
    return doc
    #print(formsec)

    entry['date'] = daytoSec + str(sec) + 'Z'
    #float("%.3f" % (dt.second + dt.microsecond / 1e6)),
    #dt.strftime('%z')
    doc['modified'].append(entry)
    return doc

def correctModifiedEntry(doc):
    now = datetime.now()
    entry = {}
    entry['user'] = 'Script mhaibt'
    daytoSec = now.strftime('%Y-%m-%dT%H:%M:%S')
    sec = "{:.3f}".format(Decimal(now.strftime('.%f')))
    #sec = "{:0>2d}".format(Decimal(sec))
    print (sec)
    #return doc
    #print(formsec)

    entry['date'] = daytoSec + str(sec)[1:] + 'Z'
    print(entry['date'])
    #float("%.3f" % (dt.second + dt.microsecond / 1e6)),
    #dt.strftime('%z')
    doc['modified'].append(entry)
    return doc

def saveChanges(doc, pouchDB_url_put, auth ):
    requests.put(pouchDB_url_put + doc['_id'], auth=auth, json=doc)


    

result = getDocsNotRecordedInIdentifier('WarkaEnvironsSurvey', auth=auth, pouchDB_url_find=pouchDB_url_find)
print(len(result['docs']))
#keyValList = ['Script mhaibt']
Scriptmhaibtresourcelist = []
for resource in result['docs']:
    #resource['modified'][:] = [d for d in resource['modified'] if d.get('user') != 'Script mhaibt']
    for modifications in resource['modified']:
        
        if modifications['user']=='Script mhaibt':
            now = datetime.now()
            daytoSec = now.strftime('%Y-%m-%dT%H:%M:%S')
            sec = "{:.3f}".format(Decimal(now.strftime('.%f')))

            modifications['date']= daytoSec + str(sec)[1:] + 'Z'
            Scriptmhaibtresourcelist.append(resource)

#tobecorrected = [obj for obj in result['docs'] if [item['user']=='Script DAI-IT' for item in obj['modified'] ]]
#expectedResult = [resource for resource in result['docs'] if modifieditem for d['type'] in keyValList]
#list(filter(lambda d: d['modified']['user'] in ['Script mhaibt'], result['docs']))
print(len(Scriptmhaibtresourcelist))
for doc in Scriptmhaibtresourcelist:
    #print (doc)
    #doc['resource']['relations']['isRecordedIn'].clear() 
    #doc['resource']['relations']['isRecordedIn'].append(str(idOfIdentifier('WarkaEnvironsSurvey', auth=auth, pouchDB_url_find=pouchDB_url_find)))
    #doc = correctModifiedEntry(doc)
    print (doc)
    saveChanges(doc, pouchDB_url_put= pouchDB_url_put, auth= auth)

#result_df = pd.DataFrame(result)
#print(result)

