#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
After auditing is complete the next step is to prepare the data to be inserted into a SQL database.
To do so you will parse the elements in the OSM XML file, transforming them from document format to
tabular format, thus making it possible to write to .csv files.  These csv files can then easily be
imported to a SQL database as tables.

The process for this transformation is as follows:
- Use iterparse to iteratively step through each top level element in the XML
- Shape each element into several data structures using a custom function
- Utilize a schema and validation library to ensure the transformed data is in the correct format
- Write each data structure to the appropriate .csv files

We've already provided the code needed to load the data, perform iterative parsing and write the
output to csv files. Your task is to complete the shape_element function that will transform each
element into the correct format. To make this process easier we've already defined a schema (see
the schema.py file in the last code tab) for the .csv files and the eventual tables. Using the 
cerberus library we can validate the output against this schema to ensure it is correct.

## Shape Element Function
The function should take as input an iterparse Element object and return a dictionary.

### If the element top level tag is "node":
The dictionary returned should have the format {"node": .., "node_tags": ...}

The "node" field should hold a dictionary of the following top level node attributes:
- id
- user
- uid
- version
- lat
- lon
- timestamp
- changeset
All other attributes can be ignored

The "node_tags" field should hold a list of dictionaries, one per secondary tag. Secondary tags are
child tags of node which have the tag name/type: "tag". Each dictionary should have the following
fields from the secondary tag attributes:
- id: the top level node id attribute value
- key: the full tag "k" attribute value if no colon is present or the characters after the colon if one is.
- value: the tag "v" attribute value
- type: either the characters before the colon in the tag "k" value or "regular" if a colon
        is not present.

Additionally,

- if the tag "k" value contains problematic characters, the tag should be ignored
- if the tag "k" value contains a ":" the characters before the ":" should be set as the tag type
  and characters after the ":" should be set as the tag key
- if there are additional ":" in the "k" value they and they should be ignored and kept as part of
  the tag key. For example:

  <tag k="addr:street:name" v="Lincoln"/>
  should be turned into
  {'id': 12345, 'key': 'street:name', 'value': 'Lincoln', 'type': 'addr'}

- If a node has no secondary tags then the "node_tags" field should just contain an empty list.

The final return value for a "node" element should look something like:

{'node': {'id': 757860928,
          'user': 'uboot',
          'uid': 26299,
       'version': '2',
          'lat': 41.9747374,
          'lon': -87.6920102,
          'timestamp': '2010-07-22T16:16:51Z',
      'changeset': 5288876},
 'node_tags': [{'id': 757860928,
                'key': 'amenity',
                'value': 'fast_food',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'cuisine',
                'value': 'sausage',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'name',
                'value': "Shelly's Tasty Freeze",
                'type': 'regular'}]}

### If the element top level tag is "way":
The dictionary should have the format {"way": ..., "way_tags": ..., "way_nodes": ...}

The "way" field should hold a dictionary of the following top level way attributes:
- id
-  user
- uid
- version
- timestamp
- changeset

All other attributes can be ignored

The "way_tags" field should again hold a list of dictionaries, following the exact same rules as
for "node_tags".

Additionally, the dictionary should have a field "way_nodes". "way_nodes" should hold a list of
dictionaries, one for each nd child tag.  Each dictionary should have the fields:
- id: the top level element (way) id
- node_id: the ref attribute value of the nd tag
- position: the index starting at 0 of the nd tag i.e. what order the nd tag appears within
            the way element

The final return value for a "way" element should look something like:

{'way': {'id': 209809850,
         'user': 'chicago-buildings',
         'uid': 674454,
         'version': '1',
         'timestamp': '2013-03-13T15:58:04Z',
         'changeset': 15353317},
 'way_nodes': [{'id': 209809850, 'node_id': 2199822281, 'position': 0},
               {'id': 209809850, 'node_id': 2199822390, 'position': 1},
               {'id': 209809850, 'node_id': 2199822392, 'position': 2},
               {'id': 209809850, 'node_id': 2199822369, 'position': 3},
               {'id': 209809850, 'node_id': 2199822370, 'position': 4},
               {'id': 209809850, 'node_id': 2199822284, 'position': 5},
               {'id': 209809850, 'node_id': 2199822281, 'position': 6}],
 'way_tags': [{'id': 209809850,
               'key': 'housenumber',
               'type': 'addr',
               'value': '1412'},
              {'id': 209809850,
               'key': 'street',
               'type': 'addr',
               'value': 'West Lexington St.'},
              {'id': 209809850,
               'key': 'street:name',
               'type': 'addr',
               'value': 'Lexington'},
              {'id': '209809850',
               'key': 'street:prefix',
               'type': 'addr',
               'value': 'West'},
              {'id': 209809850,
               'key': 'street:type',
               'type': 'addr',
               'value': 'Street'},
              {'id': 209809850,
               'key': 'building',
               'type': 'regular',
               'value': 'yes'},
              {'id': 209809850,
               'key': 'levels',
               'type': 'building',
               'value': '1'},
              {'id': 209809850,
               'key': 'building_id',
               'type': 'chicago',
               'value': '366409'}]}
"""

import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as ET

import cerberus

import schema

OSM_PATH = "san-jose_california.osm"

NODES_PATH = "nodes_sanjose.csv"
NODE_TAGS_PATH = "nodes_tags_sanjose.csv"
WAYS_PATH = "ways_sanjose.csv"
WAY_NODES_PATH = "ways_nodes_sanjose.csv"
WAY_TAGS_PATH = "ways_tags_sanjose.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
street_regex=re.compile(r'\S+$')

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']
Street_name_to_be_updated= {"Ln" :"Lane","Rd":"Road","ave":"Avenue","Ave":"Avenue","court":"Ct", "Blvd":"Boulevard",\
                           "Hwy":"Highway","Dr":"Drive","street":"Street","St":"Street","Sq":"Square",\
                            "Blvd.":"Boulevard"}
san_jose_zipcodes=["94088","94089","94538","94560","95002","95008","95013","95035","95037","95050","95054","95101",\
                   "95103","95106","95108","95109","95110","95111","95112","95113","95115","95116","95117","95118",\
                   "95119","95120","95121","95122","95123","95124","95125","95126","95127","95128","95129","95130",\
                   "95131","95132","95133","95134","95135","95136","95138","95139","95141","95148","95150","95151",\
                   "95152","95153","95154","95155","95156","95157","95158","95160","95161","95164","95170","95172",\
                   "95173","95190","95191","95192","95193","95194","95196"]

san_jose_citynames=["San jose","San Jose","San José".decode("utf8"),"san jose"]

def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements
    ignore_node=False
    if element.tag == "node":
        for node_attributes in NODE_FIELDS:
            node_attribs[node_attributes]=element.attrib[node_attributes]
        for child in element:
            tag={}
            m = PROBLEMCHARS.search(child.attrib["k"])
            if m:
                continue
            else:
                tag["id"]=element.attrib["id"]
                if child.attrib["k"]=="addr:street": 
                    updated_name=update_street_names(child.attrib["v"])
                    tag["value"]=updated_name
                elif child.attrib["k"]=="addr:postcode":
                    correct_sanjose_zipcode=clean_postcode(child.attrib["v"])
                    if correct_sanjose_zipcode is not None:
                        tag["value"]=correct_sanjose_zipcode
                    else:
                        return
                elif child.attrib["k"]=="phone":
                    updated_number=clean_phone(child.attrib["v"])
                    tag["value"]=updated_number
                elif child.attrib["k"] =="addr:city":
                    city_name=child.attrib["v"]
                    updated_city_name=clean_sanjose_cityname(city_name)
                    if updated_city_name is not None:
                        tag["value"]=updated_city_name
                    else:
                        return
                else:
                    tag["value"]=child.attrib["v"]
                if ":" in child.attrib["k"]:
                    tag["type"],tag["key"]=child.attrib["k"].split(":",1)
                else:
                    tag["key"]=child.attrib["k"]
                    tag["type"]='regular'
                tags.append(tag)
        return {'node': node_attribs, 'node_tags': tags}
    elif element.tag=="way":
        count=0
        for way_attributes in WAY_FIELDS:
            way_attribs[way_attributes]=element.attrib[way_attributes]
        for child in element:
            tag={}
            nd={}
            if child.tag =="tag":
                m = PROBLEMCHARS.search(child.attrib["k"])
                if m:
                    continue
                else:
                    tag["id"]=element.attrib["id"]
                if child.attrib["k"]=="addr:street": 
                    updated_name=update_street_names(child.attrib["v"])
                    tag["value"]=updated_name
                elif child.attrib["k"]=="addr:postcode":
                    correct_sanjose_zipcode=clean_postcode(child.attrib["v"])
                    if correct_sanjose_zipcode is not None:
                        tag["value"]=correct_sanjose_zipcode
                    else:
                        return
                elif child.attrib["k"]=="phone":
                    updated_number=clean_phone(child.attrib["v"])
                    tag["value"]=updated_number
                else:
                    tag["value"]=child.attrib["v"]
                if ":" in child.attrib["k"]:
                    tag["type"],tag["key"]=child.attrib["k"].split(":",1)
                else:
                    tag["key"]=child.attrib["k"]
                    tag["type"]='regular'
                tags.append(tag)
            elif child.tag=="nd":
                nd["id"]=element.attrib["id"]
                nd["node_id"]=child.attrib["ref"]
                nd["position"]=count
                way_nodes.append(nd)
                count=count+1
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}
# ================================================== #
#               Helper Functions                     #
# ================================================== #

"""If street name is in  the dictionary Street_name_to_be_updated, then modify the current street name with the value
in the dictionary. If the street name doesnt exist in the dictionary, the return the street name as is"""
def update_street_names(street_name):
    m=street_regex.search(street_name)
    if m.group() in Street_name_to_be_updated.keys():
        street_name=re.sub(m.group(),Street_name_to_be_updated[m.group()], street_name)
    return street_name

"""If the postcode contains - (95124-3452), then remove the - else keep it as is. Check to see if the postcode exists in
san_jose_zipcodes list. This lisy contains all the valid san jose zipcodes only. If the postcode is in this list, process
it forward to add it in the csv else discard the whole element that belongs to a non san jose zipcode"""
def clean_postcode(postcode):
    clean_postcode=None
    if "-" in postcode:
            clean_postcode_list=postcode.split("-")
            clean_postcode=clean_postcode_list[0]
    else:
        clean_postcode=postcode
    if clean_postcode in san_jose_zipcodes:
        return clean_postcode

""" Check the phone number and remove any non number character from the phone. For example -,"" should be replaced by "".
After this step, remove the 1 from the start of the phone number"""
def clean_phone(phoneNumber):
    updated_number_old=re.sub(r'\D+',"",phoneNumber)
    updated_number=re.sub(r'^1',"",updated_number_old)
    return updated_number

""" San jose spelling is inconsistent in the data. This function converts every spelling to "San Jose" """
def clean_sanjose_cityname(cityname):
    if cityname in san_jose_citynames:
        return "San Jose"
    else:
        return None

def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        
        raise Exception(message_string.format(field, error_string))


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file, \
         codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file, \
         codecs.open(WAYS_PATH, 'w') as ways_file, \
         codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file, \
         codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])


if __name__ == '__main__':
    # Note: Validation is ~ 10X slower. For the project consider using a small
    # sample of the map when validating.
    process_map(OSM_PATH, validate=True)