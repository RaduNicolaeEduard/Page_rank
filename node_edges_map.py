import time
from elasticsearch import Elasticsearch
import networkx as nx
import random
import threading
from faker import Faker
from random import uniform
es = Elasticsearch()

import uuid

def get_all_documents():
    return es.search(index="page_rank", body={"query": {"match_all": {}}, "size": 10000})["hits"]["hits"]

def delete_all_documents_in_index():
    # Delete index
    es.indices.delete(index="graph", ignore=[400, 404])

        



def create_index():
    es.indices.create(index="geo_point_name", ignore=400)
    es.indices.create(index="graph", ignore=400)
    es.indices.put_mapping(index="graph", body={
        "properties": {
            "from": {
                "type": "geo_point"
            },
            "to": {
                "type": "geo_point"
            }
        }
    })
    es.indices.put_mapping(index="geo_point_name", body={
        "properties": {
            "location": {
                "type": "geo_point"
            },
            "name": {
                "type": "keyword"
            }
        }
    })


def update_geopoint(url, source, destination):
    es.index(index="graph", id=uuid.uuid4(), body={
        "from": source,
        "to": destination,
        "url": url
    })
def get_random_geo_point():
    x , y = fake.latlng()
    return [x,y]


def create_geopoint(url):
    geo = get_random_geo_point()
    # print("---------------------------------------")
    # print(f'INFO\tCreating geo point for {url}')
    # print(geo)
    # print("---------------------------------------")
    return es.create(index="geo_point_name", id=url, body={
        "name": url,
        "location": geo
    })

def get_geo_point(url):
    try:
        # print("---------------------------------------"+url)
        point = es.get(index="geo_point_name", id=url)
        return point
    except:
        try:
            # print(f'INFO\tGeo point for {url} does not exist Creating one')
            create_geopoint(url)
            return es.get(index="geo_point_name", id=url)
        except:
            get_geo_point(url)


fake = Faker()
Faker.seed(0)
delete_all_documents_in_index()
create_index()
# print(get_random_geo_point())

all_documents = get_all_documents()
for document in all_documents:
    if(document["_source"]["url"] in "?" ):
        continue
    # print(document["_id"])
    source = get_geo_point(document["_id"])
    for link in document["_source"]["links"]:
        try:
            destination = get_geo_point(link)
            fr = source["_source"]["location"]
            to = destination["_source"]["location"]
            update_geopoint(document["_id"], fr, to)
            print(f'INFO\tUpdating geo point for {document["_id"]} from {fr} to {to}')
        except:
            pass







