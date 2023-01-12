from cmath import nan
from math import isnan
import difflib
from tkinter.tix import COLUMN 
import pandas as pd
import numpy as np
import json
from pathlib import Path
from pyIdaifield import docsByCreationdate,getAllDocs, writeReverseRelation, addModifiedEntry, createRelationMapping, DOCtoDF, allDocsToDf, combineFramesByFixId, combineFramesByFuzzyId, createRelationFromField, strings2splitlistitems, fuzzyAdaptListitemsByNormitems, adaptListitemsByMap, formatListitems2periodvalues, formatDrawing2DrawingId, formatIdentifier,DFtoDOC, enterCreated, moveValues2otherField, bulkSaveChanges, makeUuid
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


### import Target iDAI.field DB ###
targetROWs= getAllDocs(db_url, auth, db_name)
targetDF, docfields  = allDocsToDf(targetROWs)
#print(targetDOCs['rows'][0].keys())
#targetDOCs = {}
#targetDOCs['docs'] = [dict['doc'] for dict in targetROWs['rows']]

#DrawingsDF = targetDF[targetDF['type']=='Drawing']
filter = targetDF.apply(docsByCreationdate, startdate='2022-08-2', axis=1)
targetDF = targetDF[filter]
print('length of DrawingsDF: ', len(targetDF), targetDF['type'].unique())
isDepictedIn_map = createRelationMapping(targetDF[targetDF['relations'].notnull()], relation='isDepictedIn')
print('length of mapping: ', len(isDepictedIn_map))
#print('length of mapping: ', len(isDepictedIn_map))
#print(isDepictedIn_map)
DrawingsDF = writeReverseRelation(targetDF, mapdicts=isDepictedIn_map, relation='isDepictedIn', rev_relation='depicts')
#DFresources = removeUndepicts(DFresources)
print('length with depicts: ', len(DrawingsDF))
DrawingsDF = DrawingsDF.apply(addModifiedEntry, axis=1)
DrawingsDOC = DFtoDOC(DrawingsDF, docfields)
#print(json.dumps(DrawingsDOC, indent=4))


bulkSaveChanges(db_url, auth, db_name, DrawingsDOC)






