import requests
import os
#from collections import Mapping, Set, Sequence 
import json
import re
from lxml import html
#import bs4 as bs

#outputpath = 'C:/Users/mhaibt/Documents/mining_shapes/OUTPUT'
string_types = (str, unicode) if str is bytes else (str, bytes)
iteritems = lambda mapping: getattr(mapping, 'iteritems', mapping.items)()


def objwalk(obj, path=(), memo=None):
    if memo is None:
        memo = set()
    iterator = None
    if isinstance(obj, Mapping):
        iterator = iteritems
    elif isinstance(obj, (Sequence, Set)) and not isinstance(obj, string_types):
        iterator = enumerate
    if iterator:
        if id(obj) not in memo:
            memo.add(id(obj))
            for path_component, value in iterator(obj):
                for result in objwalk(value, path + (path_component,), memo):
                    yield result
            memo.remove(id(obj))
    else:
        yield path, obj

#bibtexurl = https://zenon.dainst.org/Record/000151459/Export?style=BibTeX

zenon_url = 'https://zenon.dainst.org/api/v1/record?id='
s = requests.Session()
Zenon_Id = '000151459'
zenonlink_url = 'https://zenon.dainst.org/api/v1/record?id=' + ZenonId + '&field[]=DAILinks'
htmlurl = 'https://zenon.dainst.org/Record/' + Zenon_Id
exporturl = 'https://zenon.dainst.org/Record/' + Zenon_Id + '/Export?style='
exportstyle ='BibTeX'
response = s.get(zenon_url + Zenon_Id)
result = json.loads(response.text)
result = result['records'][0]
#for path, obj in objwalk(result):
    #print (path,obj)

r = requests.get(exporturl + exportstyle)
print(result.text)
#with open(outputpath + Zenon_Id + '.bibtex', 'wb') as f:
    #f.write(r.content)

#h = requests.get(htmlurl)
##tree = html.fromstring(h.content)
#soup= BeautifulSoup(tree)
#buyers = tree.xpath('th[]')
#soup = bs.BeautifulSoup(h.text,'lxml')
#columns = soup.findAll('th', text = re.compile('iDAI.gazetteer:'), attrs = {'class' : 'pos'})
#for url in soup.find_all('a'):
    #urltext = url.get('href')
    #print(type(urltext))
    #if 'gazetteer.dainst.org' in str(url):
        #print(urltext)

#https://gazetteer.dainst.org
#print(h.text)
#<th>iDAI.gazetteer: </th>
#<td>
#                <a title="Chemtou" href="https://gazetteer.dainst.org/app/#!/show/2282612" target="_blank"><i class="fa fa-map-marker"></i> Chemtou <small><i class="fa fa-external-link"></i></small></a><br />

#print (firstrecord['authors'])
