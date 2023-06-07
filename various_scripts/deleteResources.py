from cmath import nan
from math import isnan
import difflib
from tkinter.tix import COLUMN 
import pandas as pd
import numpy as np
import json
from pathlib import Path
from pyIdaifield import getAllDocs, docsByCreationdate, addModifiedEntry, format2FindDate, singleStrfield2List, simpleDiameter2dimensionDiameter, commaNumber2int, DOCtoDF, allDocsToDf, combineFramesByFixId, combineFramesByFuzzyId, createRelationFromField, strings2splitlistitems, fuzzyAdaptListitemsByNormitems, adaptListitemsByMap, formatListitems2periodvalues, formatDrawing2DrawingId, formatIdentifier,DFtoDOC, enterCreated, moveValues2otherField, bulkSaveChanges, makeUuid
auth = ('', 'blub')
#db_url = 'http://host.docker.internal:3000'
db_name = 'uruk'
db_url = 'http://localhost:3001'
pouchDB_url_find = f'{db_url}/{db_name}/_find'
pouchDB_url_put = f'{db_url}/{db_name}/'
pouchDB_url_bulk = f'{db_url}/{db_name}/_bulk_docs'
pouchDB_url_all = f'{db_url}/{db_name}/_all_docs'
imagestore = '/home/imagestore/'
FIG_SIZE = [20,30]

#db_name = 'shapes_import'
#pouchDB_url_find = f'{db_url}/{db_name}/_find'
#pouchDB_url_put = f'{db_url}/{db_name}/'
#pouchDB_url_bulk = f'{db_url}/{db_name}/_bulk_docs'
#pouchDB_url_all = f'{db_url}/{db_name}/_all_docs'
pouchDB_url_alldbs = f'{db_url}/_all_dbs'
imagestore = '/home/imagestore/'
importcsv_path ='C:/Users/mhaibt/Nextcloud2/Uruk/WarkaEnvironsSurvey/WES_collections/WES_potterycollectionsdata_beforeidaifield_complete_MASTERVERSION.csv'


    
targetROWs= getAllDocs(db_url, auth, db_name)
DFresources, docfields = allDocsToDf(targetROWs)
#result = getDocsRecordedInIdentifier('WarkaEnvironsSurvey', auth=auth, pouchDB_url_find=pouchDB_url_find)
print(len(targetROWs['rows']))
filter = DFresources.apply(docsByCreationdate, startdate='2022-10-6', axis=1)
DFresources_clean = DFresources[filter]


#print(docfields)
print(len(DFresources_clean), type(DFresources_clean))
#DFresources_clean = DFresources[DFresources.date ==)]
DFresources_clean.insert(0,'_deleted',True)
print(DFresources_clean.columns)
#DFresources_clean = DFresources_clean.apply(addModifiedEntry, axis=1)

#DFresources_clean = DFresources_clean.apply(deleteImagestorefile, axis=1)
print(type(docfields))

docfields.append('_deleted')
print(len(DFresources_clean))
print(docfields)
DFresources_clean = DFresources_clean[['_id','_rev','_deleted']]
#print(print([i for i in DFresources_clean.columns if not i in docfields]))
DOC = DFtoDOC(DFresources_clean, docfields)
#print(json.dumps(DOC['docs'], indent=4, sort_keys=True))
bulkSaveChanges(db_url, auth, db_name, DOC)
#print('DF RESOURCE')
#DFresources = DFresources.apply(replaceValue,key='temper', value='quartz', newvalue = 'quartz sand',axis=1)
#listOfFields = ['temper', 'temperAmount', 'temperParticles']
#statDF  = DFresources.apply(cleanNans,listOfFields=listOfFields, axis=1)
#notnull = statDF.notnull()
#good = statDF[notnull]
#print(good[['temper', 'temperAmount', 'temperParticles']])
#simpleTypoPlot (good, listOfFields)


#plot_bargraph_with_groupings(df=good, groupby='temper', colourby='identifier', title='Material', xlabel='Types', ylabel='frequency')
#DFresources = DFresources.apply(correctDiameter, axis=1)



#DFresources = DFresources.apply(combineColumns, columnlist=['Processor','processor'], newColumn='Processor', axis=1)
#DFresources = DFresources.apply(strColumnToList, column='processor', axis=1)
#DFresources = DFresources.apply(replaceValue,key='processor', value='MvE', newvalue = 'Margarete van Ess',axis=1)
#DFresources= DFresources.rename(columns={"Processor": "processor"})
#print(DFresources.columns)
#listOfFields= ['temper', 'temperAmount', 'temperParticles']
#groups = DFresources.groupby(listOfFields)
#if type(DFresources['temper']) == list:

#DFresources.groupby(by='temper').size()
#for group in groups:
#print (statsDF)
#print(groups)

#DFresources=DFresources.apply(addModifiedEntry, axis=1)

#interestproperties = ['type', 'amount', 'condition', 'manufacturingMethod', 'temper', 'temperAmount', 'temperParticles','coatOutsideType', 'vesselpart', 'processor', 'clayColorOutside','period', 'clayColorInside', 'Firing','periodEnd', 'conditionPercent','FormComparisonCompendia', 'FormComparisonCompendiadetail', 'Drawing','decorationTechnique', 'vesselForm','DecorationComparisonCompendiadetail', 'DecorationComparisonCompendia','dimensionLength', 'dimensionWidth', 'dimensionThickness', 'ApplicationComparisonCompendia','ApplicationComparisonCompendiadetail']
#interestproperties = ['processor']

#for column in interestproperties:
    #listvalues = [str(i) for i in DFresources[column]]
    #print(column, set(listvalues) )
#print (json.dumps(DOC['docs'], indent=1))
#print(DOC['docs'][['identifier']=='WESid_13_43'])
#bulkSaveChanges(DOC, pouchDB_url_bulk, auth)
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
