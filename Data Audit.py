
"""File containing audit functions for street, postcode, phone and city"""
import xml.etree.ElementTree as ET
import re
from collections import defaultdict

OSM_FILE="san-jose_california.osm"
street_regex=re.compile(r'\S+$')
postcode_regex=re.compile(r'\S+$')
cityname_regex=re.compile(r'\S+\s*\S*')
street_group_count=defaultdict(int)
postcode_count=defaultdict(int)
city_count=defaultdict(int)
Phonelist=[]
Street_name_to_be_updated= {"Ln" :"Lane","Rd":"Road","ave":"Avenue","Ave":"Avenue","court":"Ct", "Blvd":"Boulevard",\
                           "Hwy":"Highway","Dr":"Drive","street":"Street","St":"Street","Sq":"Square",\
                            "Blvd.":"Boulevard"}
def parse():
    for elem in get_element(OSM_FILE):
        if elem.tag in ("way", "node"):
            for child in elem:
                if child.tag=="tag":
                    if child.attrib["k"]=="addr:street": 
                        audit_street_names(child.attrib["v"],street_group_count)
                    if child.attrib["k"]=="addr:postcode":
                        audit_postcode(child.attrib["v"])
                    if child.attrib["k"]=="phone":
                        audit_phone(child.attrib["v"])
                    if child.attrib["k"]=="addr:city":
                        audit_cityname(child.attrib["v"])

def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()

""" Function will return all different types of streetnames and the number of times they occur"""

def audit_street_names(street_name,street_group_count):
    m=street_regex.search(street_name)
    if m:
        street_type=m.group()
        street_group_count[street_type] +=1

""" Function will return all zipcodes and the number of items each zipcode occurs"""
def audit_postcode(postcode):
    m=postcode_regex.search(postcode)
    if m:
        postcode_type=m.group()
        postcode_count[postcode_type] +=1

"""Function will return all city names in the data and the number of times each city name occurs"""
def audit_cityname(cityname):
    m=cityname_regex.search(cityname)
    if m:
        city_type=m.group()
        city_count[city_type]+=1

"""Function will return all phone numbers in the data"""
def audit_phone(phoneNumber):
    Phonelist.append(phoneNumber)

parse()
print ""
print "Street name and count"
print "******************************************"
for streetName, count in street_group_count.items():
    print streetName, "\t" ,count

print ""
print "Postcode and count"
print "******************************************"
for postcode, count in postcode_count.items():
    print postcode, "\t" ,count

print ""
print "City and Count"
print "******************************************"
for city, count in city_count.items():
    print city, "\t" ,count

print ""
print "Phone list"
print "******************************************"
for items in Phonelist:
    print items

