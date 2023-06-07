from cmath import nan
from math import isnan
import difflib
from tkinter.tix import COLUMN 
import pandas as pd
import numpy as np
import json
from pathlib import Path
from pyIdaifield import getAllDocs, format2FindDate, singleStrfield2List, simpleDiameter2dimensionDiameter, commaNumber2int, DOCtoDF, allDocsToDf, combineFramesByFixId, combineFramesByFuzzyId, createRelationFromField, strings2splitlistitems, fuzzyAdaptListitemsByNormitems, adaptListitemsByMap, formatListitems2periodvalues, formatDrawing2DrawingId, formatIdentifier,DFtoDOC, enterCreated, moveValues2otherField, bulkSaveChanges, makeUuid
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
#exportProject = 'shapes_import'
personalMap = {
    'JHvenov18' : ['Jan Hubert', 'Margarete van Ess'],  'JH' : ['Jan Hubert'], 've' : ['Margarete van Ess'], 'veJH' : ['Jan Hubert', 'Margarete van Ess'], 'Greta' : ['Greta Fetting'], 'Greta&Jan' : ['Greta Fetting', 'Jan Hubert'], 'Jan&Greta' : ['Greta Fetting', 'Jan Hubert'],
 'BH': ['Barbara Huber'], 'salah': ['Salah'],'SALAH': ['Salah'] ,'Salah': ['Salah'] ,'Haidar': ['Haidar Wasmi'], 'JHve' : ['Jan Hubert', 'Margarete van Ess'], 'JHvE': ['Jan Hubert', 'Margarete van Ess'], 'vE' : ['Margarete van Ess'], 'RRvE': ['Rosa Reising', 'Margarete van Ess'],'RR':['Rosa Reising'],
 'MvE' : ['Margarete van Ess'], 'FW':['Friedrich Weigel'], 'NvE'  : ['Margarete van Ess'], 'Mve' : ['Margarete van Ess'], 'MH':['Max Haibt'], 'Jacob' : ['Jacub'] ,'Ithar':['Ithar'], 'Jacob3':['Jacub']}

temperMap = {
    'min': ['non clay minerals'], 'min-veg': ['non clay minerals', 'organic particles'],  'Sand': ['quartz sand'],'sand':['quartz sand'], '"sand"':['quartz sand'],'Min-veg':['non clay minerals', 'organic particles'], 'veg':['organic particles'],
 'min, sand': ['non clay minerals', 'quartz sand'], 'min-veg and sand':['non clay minerals', 'organic particles', 'quartz sand'], '"sand"min':['non clay minerals', 'quartz sand'], 'veg\nsand': ['organic particles', 'quartz sand'], 'veg, sand':['organic particles', 'quartz sand'],
 'min, "sand"':['non clay minerals', 'quartz sand'], 'veg, "sand"':[ 'organic particles', 'quartz sand']}


temperParticlesMap = {
    'vf':'very-fine', 'f':'fine', 'fine':'fine', 'mf-m, mc-v':'medium' , 'mg':'medium', 'Mf/c':'medium', '3':'medium','mf, coarse':'medium', 'mf':'medium-fine', 'fine, mf':'medium-fine', 'mf, mc':'medium', 'coarse':'coarse'}
temperAmountMap = {
    '0':'no inclusions' , '1':'sparse' ,'2':'occasional', '3':'medium', '3/1':'medium', '4':'frequent', '5':'very frequent', '9':'very frequent'}

periodMap = {'I/L':'Isin-Larsa', 'aB':'Old Babylonian', 'EDIII': 'Early Dynastic III', 'Akk':'Akkadian', 'EdIII': 'Early Dynastic III', 'oB':'Old Babylonian', 'Ur3':'Ur III', 'UrIII':'Ur III', 'mB':'Middle Babylonian',
 'ED': 'Early Dynastic', 'Akkad': 'Akkadian', 'akk': 'Akkadian', 'UrIII?':'Ur III', 'GN':'Jemdet Nasr', 'ED??':'Early Dynastic', 'selk': 'Seleucid-Parthian', 'part':'Seleucid-Parthian',
 'EDI' 'sel' 'parth' 'oI/L' 'SpB' 'lateUruk?' 'EDII' 'III' 'G' 'LUruk'
 'MB':'Middle Babylonian', 'sB':'Late Babylonian', 'IL':'Isin-Larsa',  'OB':'Old Babylonian', 'mb':'Middle Babylonian', 'ED2': 'Early Dynastic II', 'ED3': 'Early Dynastic III', 'ED1': 'Early Dynastic I', 'nB':'New Babylonian', 'Uruk':'Uruk','lB':'Late Babylonian', 'lB?':'Late Babylonian',
 'Sel':'Seleucid-Parthian', 'parth?':'Seleucid-Parthian', 'part?':'Seleucid-Parthian', 'endKassitisch':'Kassite', 
 'nB?':'New Babylonian', 'sass':'Sassanian', 'JN':'Jemdet Nasr', 'ED?':'Early Dynastic',
 'MB+later':'Middle Babylonian', 'mB?':'Middle Babylonian', 'oB?': 'Old Babylonian', 'akkad':'Akkadian', 'mUruk':'Middle Uruk','lateUruk':'Late Uruk',
 'Muruk':'Middle Uruk', 'lUruk':'Late Uruk', 'SpUruk':'Late Uruk', 'Uruk?':'Uruk', 'NB':'New Babylonian',
 'spB':'Late Babylonian', 'ur3':'Ur III', 'III?':'Early Dynastic III','Parth': 'Seleucid-Parthian', 'l.Uruk':'Late Uruk', 'Akkad?':'Akkadian', 'Ur':'Ur III', 'U3':'Ur III', 'selk.Part':'Seleucid-Parthian', 'Uru':'Uruk', 
 'part': 'Seleucid-Parthian', 'lUru':'Late Uruk', 'evtl.Akkad':'Akkadian',
 'FD':'Early Dynastic', 'UR3': 'Ur III','Il':'Isin-Larsa', 'Ur3?':'Ur III', 'ED".ED3':'Early Dynastic III', 'Archaic':'Uruk', 'par':'Seleucid-Parthian', 'ED>later':'Early Dynastic', 'FD1':'Early Dynastic I', 'ED1?':'Early Dynastic I', 'eUruk':'Early Uruk',
 'Mittelalter':'Islamic','ED1.ED2.ED3':'Early Dynastic'}

coatOutsideTypeMap= { 'wet sm':'wet smoothed', 'ws':'wet smoothed', 'insiezed':'incised'}

coatOutsideTypeExclude= ['D9a', 'D5a', 'D9', 'D8', 'D5a, D16',
 'D15', 'D5a, D15', 'D4']

vesselpartMap= { 'RS':'rim sherd', 'WS':'wall sherd', 'other':'other', 'BS':'base sherd', 'spout': 'spout', 'handle':'handle',
 'Wand': 'wall sherd', 'B8':'base sherd','lid':'lid','Lid':'lid', 'stand':'stand','completeprofile':'completeprofile', 'completeprofil':'completeprofile',
 'figurine':'other','Stand':'stand','ws':'wall sherd', 'bs':'base sherd','completevessel':'completevessel','shoulder':'shoulder',
 'RSofstand':'rim sherd','shouder':'shoulder'}

importcsvDF = pd.read_csv(importcsv_path) 
def createDictColumn(series, outputfield):
    series[outputfield]= {}
    return series 
importcsvDF= importcsvDF.apply(createDictColumn,outputfield='relations',axis=1)
importcsvDF = importcsvDF.rename(columns={
    'TRACK': 'identifier',
    'AMOUNT':'amount', 
    'PRESERVATI':'condition',
    'VESSEL_PAR':'vesselpart',
    'TECHNIQUE':'manufacturingMethod',
    'FIRING':'Firing',
    'RECORDED_B':'processor',
    'PERSERVED_':'conditionPercent',
    'FABRIC_TYP':'temper',
    'FABRIC_QU2':'temperAmount',
    'FABRIC_QUA':'temperParticles',
    'COLOUR_OF_': 'clayColorOutside',
    'SURFACE': 'coatOutsideType',
    'DATING_PRO': 'period',
    'DRAWING': 'DrawingId',
    'COMMENTS':'description',
    'TYPE_NAME':'FormComparisonCompendiadetail'
    })
### Easy Fields ###
amount = importcsvDF[importcsvDF['amount'].notnull()].apply(commaNumber2int, inputfield = 'amount', axis=1)
importcsvDF.update(amount)
importcsvDF['REC_NO_']=importcsvDF['REC_NO_'].fillna(0).astype(int)
importcsvDF['type']='Pottery'
#importcsvDF = importcsvDF.apply(singleStrfield2List, inputfield='vesselpart', axis=1)
importcsvDF = importcsvDF.apply(singleStrfield2List, inputfield='manufacturingMethod', axis=1)
importcsvDF = importcsvDF.apply(singleStrfield2List, inputfield='Firing', axis=1)
importcsvDF = importcsvDF.apply(singleStrfield2List, inputfield='coatOutsideType', axis=1)
conditionPercent = importcsvDF[importcsvDF['conditionPercent'].notnull()].apply(commaNumber2int, inputfield = 'conditionPercent', axis=1)
importcsvDF.update(conditionPercent)

### Date ###
importcsvDF['date']=np.nan
dateDF = importcsvDF.apply(format2FindDate,inputfield='identifier', outputfield='date', axis=1)
importcsvDF.update(dateDF)
print(importcsvDF['date'])


### strict Mappings ###
importcsvDF['processor'] = importcsvDF['processor'].map(personalMap).fillna(importcsvDF['processor'])
importcsvDF['temper'] = importcsvDF['temper'].map(temperMap).fillna(importcsvDF['temper'])
importcsvDF['temperAmount'] = importcsvDF['temperAmount'].map(temperAmountMap).fillna(importcsvDF['temperAmount'])
importcsvDF['temperParticles'] = importcsvDF['temperParticles'].map(temperParticlesMap).fillna(importcsvDF['temperParticles'])

### Part of vessel ###
importcsvDF = importcsvDF.apply(strings2splitlistitems,inputfield='vesselpart',replacemap={' ':'','?':''},regex=';|,|-|or|oder|with|\*|\n', axis=1)
importcsvDF = importcsvDF.apply(adaptListitemsByMap,inputfield='vesselpart',normmap=vesselpartMap, axis=1)
#uniquevesselparts = importcsvDF['vesselpart'].apply(pd.Series).stack().reset_index(drop=True)



### Period ###
importcsvDF = importcsvDF.apply(strings2splitlistitems,inputfield='period',replacemap={' ':'','I/I':'I-I','L/o':'L-o','ED.Akkad':'ED-Akkad'},regex=';|,|-|or|oder|bis|[^"I/L"]/|\*|\n', axis=1)
importcsvDF = importcsvDF.apply(adaptListitemsByMap,inputfield='period',normmap=periodMap, axis=1)
importcsvDF = importcsvDF.apply(formatListitems2periodvalues,inputfield='period',axis=1)



### dimensionDiameter ###
importcsvDF = importcsvDF.apply(simpleDiameter2dimensionDiameter, axis=1)
importcsvDF.drop(columns=['DIAMETER_I'])

### import Target iDAI.field DB ###
targetDOCs= getAllDocs(db_url, auth, db_name)
targetDF, docfields  = allDocsToDf(targetDOCs)

### Drawings ###

importcsvDFfix = combineFramesByFixId(surveyunitsDF,targetdf = targetdf ,id ='identifier',combinefields = ['id'])
importcsvDFfixnot = importcsvDFfix[importcsvDFfix['id'].isnull()]
importcsvDFfuzzy = importcsvDFfixnot.apply(combineFramesByFuzzyId,inputdf = surveyunitsDF,id ='identifier',combinefields = ['id'], axis=1)
importcsvDFfix.update(importcsvDFfuzzy[importcsvDFfuzzy['id'].notnull()])
importcsvDFfix = importcsvDFfix[importcsvDFfix['id'].notnull()].apply(createRelationFromField, inputfield= 'id', relation ='liesWithin', axis=1)
importcsvDF.update(importcsvDFfix)

importfinds = importcsvDF[importcsvDF['DrawingId'].notnull()]
print('So many imported finds have a DrawingId: ', len(importfinds))
importfinds = importfinds.apply(formatDrawing2DrawingId, axis=1)
drawings = targetDF[targetDF['type']=='Drawing']
drawings = drawings.rename(columns={'identifier':'DrawingId'})
importfinds = combineFramesByFixId(drawings,targetdf = importfinds ,id ='DrawingId',combinefields = ['id'])
importfindsfix = importcsvDFfix[importcsvDFfix['id'].notnull()]
importfindsfixnot = importcsvDFfix[importcsvDFfix['id'].isnull()]
print('imported finds found fixid Drawing: ', len(importfindsfix))
importfindsfixnot = importfindsfixnot.apply(combineFramesByFuzzyId,inputdf=drawings,id='DrawingId',combinefields=['id'], replacemap={'.JPG':'','.png':'','.jpg':''}, fuzziness=0.75, axis=1)
importfindsfuzzy = importcsvDFfixnot[importcsvDFfixnot['id'].notnull()]
print('imported finds found fuzzyid Drawing: ', len(importfindsfuzzy))
importcsvDF['id']= None

finds = finds[finds['id'].notnull()].apply(createRelationFromField, inputfield= 'id', relation ='isDepictedIn', axis=1)
importcsvDF.update(finds)
importcsvDF.drop(columns=['id'], inplace=True)
print('Rows after assign Drawings ', len(importcsvDF))
    #print(targetDF[targetDF['type']=='Drawing']['identifier'])


### Colour ###

uniquecolors = targetDF['clayColorOutside'].apply(pd.Series).stack().reset_index(drop=True)
importcsvDF = importcsvDF.apply(strings2splitlistitems,inputfield='clayColorOutside', axis=1)
importcsvDF= importcsvDF.apply(fuzzyAdaptListitemsByNormitems,inputfield='clayColorOutside',normitems=list(uniquecolors.unique()), fuzziness=0.85, axis=1)
importcsvDF['clayColorInside']=importcsvDF['clayColorOutside']

### Coat pottery ###
uniquecolors = targetDF['coatOutsideType'].apply(pd.Series).stack().reset_index(drop=True)
#importcsvDF = importcsvDF.apply(strings2splitlistitems,inputfield='coatOutsideType', axis=1)
importcsvDF['DecorationComparisonCompendiadetail'] = ''
move = importcsvDF[importcsvDF['coatOutsideType'].notnull()].apply(moveValues2otherField, inputfield='coatOutsideType', outputfield='DecorationComparisonCompendiadetail', valuelist=coatOutsideTypeExclude, axis=1)
importcsvDF.update(move)
importcsvDF = importcsvDF.apply(adaptListitemsByMap,inputfield='coatOutsideType',normmap=coatOutsideTypeMap, axis=1)
importcsvDF= importcsvDF.apply(fuzzyAdaptListitemsByNormitems,inputfield='coatOutsideType',normitems=list(uniquecolors.unique()), fuzziness=0.75, axis=1)
uniquecoatType = importcsvDF['coatOutsideType'].apply(pd.Series).stack().reset_index(drop=True)
with pd.option_context('display.max_rows', 50, 'display.max_columns', None):
    print(uniquecoatType.unique())
    print(importcsvDF['DecorationComparisonCompendiadetail'])

### assign SurveyUnits ###
surveyunitsDF = targetDF[targetDF['type']=='SurveyUnit']
targetdf = importcsvDF[importcsvDF['identifier'].notnull()]
importcsvDFfix = combineFramesByFixId(surveyunitsDF,targetdf = targetdf ,id ='identifier',combinefields = ['id'])
importcsvDFfixnot = importcsvDFfix[importcsvDFfix['id'].isnull()]
importcsvDFfuzzy = importcsvDFfixnot.apply(combineFramesByFuzzyId,inputdf = surveyunitsDF,id ='identifier',combinefields = ['id'], axis=1)
importcsvDFfix.update(importcsvDFfuzzy[importcsvDFfuzzy['id'].notnull()])
importcsvDFfix = importcsvDFfix[importcsvDFfix['id'].notnull()].apply(createRelationFromField, inputfield= 'id', relation ='liesWithin', axis=1)
importcsvDF.update(importcsvDFfix)
#importcsvDF.drop(columns=['id'], inplace=True)
#importcsvDF.drop(columns=['id'], inplace=True)
print('Rows after assign SurveyUnits ', len(importcsvDF))
#with pd.option_context('display.max_rows', 50, 'display.max_columns', None):
    #print(importcsvDFfix['relations'])


### individualize identifier###
def newREC_NO(series, idlist):
    series['REC_NO_'] = idlist[0]
    idlist.remove(series['REC_NO_'])
    return series
correctedduplicates = pd.DataFrame()
for number, group in importcsvDF.groupby(by=['identifier']):
    #print(group[['identifier','REC_NO_']].sort_values('REC_NO_'))
    idlist = list(range(0, len(group)))
    #print(idlist)
    grp = group.groupby(by=['REC_NO_'])
    #print(grp)
    #sorted(grp, key=lambda k: len(k), reverse=True)
    for number, innergroup in grp:
        #print('Number ', number, ' Length ',len(innergroup))
        if len(innergroup) == 1:
            try:
                idlist.remove(int(number))
                #print(int(number), ' in list')
                #print(idlist)
            except:
                continue
    
    for number, innergroup in grp:
        if len(innergroup) > 1:
            #print('These are duplicates')
            innergroup = innergroup.apply(newREC_NO, idlist=idlist, axis=1)
            correctedduplicates = correctedduplicates.append(innergroup)
            #print(idlist)
print('Correctedduplicates: ', len(correctedduplicates))
correctedduplicates=correctedduplicates.apply(formatIdentifier,axis=1)
importcsvDF.update(correctedduplicates)

### assign isRecordedIn ###
importcsvDF['recordedInID']= 'baa8937e-39f2-3dc8-b1cb-51566aedf871'
relationsupdate = importcsvDF[importcsvDF['relations'].notnull()].apply(createRelationFromField, inputfield= 'recordedInID', relation ='isRecordedIn', axis=1)
importcsvDF.update(relationsupdate)
importcsvDF.drop(columns=['recordedInID'], inplace=True)
importcsvDF = importcsvDF.apply(makeUuid, fieldname='id', axis=1)
importcsvDF['_id'] = importcsvDF['id']
### delete obsolte ###
importcsvDF.drop(columns=['DIAMETER_I'], inplace=True)
importcsvDF.drop(columns=['DrawingId'], inplace=True)
importcsvDF.drop(columns=['REC_NO_'], inplace=True)

### convert to docs ###
print('Final Rows ', len(importcsvDF))
#importcsvDF.to_json
DOCS = DFtoDOC(DFresources=importcsvDF, docfields=docfields)
DOCS2 = {'docs':[]}
for doc in DOCS['docs']:
    doc = enterCreated(doc)
    DOCS2['docs'].append(doc)
print(json.dumps(DOCS2, indent=4))
#bulkSaveChanges(db_url, auth, db_name, DOCS2)





