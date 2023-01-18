import time
from elasticsearch import Elasticsearch
import networkx as nx
import random
import threading
from confluent_kafka import Consumer

es = Elasticsearch(
    )
c = Consumer({
    'bootstrap.servers': '192.168.1.243:9091',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

# count the documents in the index
def count_documents():
    return es.count(index="page_rank")["count"]


def get_random_document():
    return es.search(index=".ent-search-engine-documents-page-rank", body={"query": {"function_score": {"random_score": {}}}, "size": 1})["hits"]["hits"][0]["_source"]


def search_for_url(url):
    return es.search(index=".ent-search-engine-documents-page-rank", body={"query": {"match": {"url": url}}})["hits"]["hits"][0]["_source"]

# Create index for the page_rank with two fields: url and page_rank


def create_index():
    es.indices.create(index="page_rank", ignore=400)
    es.indices.put_mapping(index="page_rank", body={
        "properties": {
            "url": {
                "type": "keyword"
            },
            "visited": {
                "type": "long"
            },
            "random_walk_rank": {
                "type": "float"
            },
            "validation":{
                "type":"float"
            }
        }
    })

# update or add document to page_rank index

# get documet from the index


def get_document(url):
    return es.get(index="page_rank", id=url)


def walk(url, links):
    # get the document from the index
    try:
        existing_doc = get_document(url)
        document = es.get(index="page_rank", id=url)
        visit = document["_source"]["visited"] + 1
        # Update document
        es.index(index="page_rank", id=url, body={
            "url": url,
            "links": links,
            "random_walk_rank": existing_doc["_source"]["random_walk_rank"],
            "validation": existing_doc["_source"]["validation"],
            "visited": visit
        })
    except Exception as e:
        # if does not exit create the document with page_rank = 1
        print(f'INFO\tDocument {url[:20]}... does not exist in page_rank index creating it')
        es.create(index="page_rank", id=url, body={
            "url": url,
            "links": links,
            "validation":0,
            "random_walk_rank": 0,
            "visited": 1
        }, ignore=409)
        pass


def get_all_documents():
    # Get all documents from the index with the size of 10000
    return es.search(index="page_rank", body={"query": {"match_all": {}}, "size": 10000})["hits"]["hits"]

# Add kibana dashboard for visualizing page rank


def add_kibana_dashboard():
    es.index(index=".kibana", id="page_rank", body={
        "title": "Page Rank",
        "hits": 0,
        "description": "",
        "panelsJSON": '[{"col":1,"id":"page_rank","panelIndex":1,"row":1,"size_x":12,"size_y":3,"type":"visualization"},{"col":1,"id":"page_rank-2","panelIndex":2,"row":4,"size_x":12,"size_y":3,"type":"visualization"}]',
        "optionsJSON": '{"darkTheme":false}',
        "uiStateJSON": '{}',
        "version": 1,
        "timeRestore": False,
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": '{"filter":[],"query":{"query":"","language":"lucene"}}'
        }
    })


def update_rank(doc, rank):
    es.index(index="page_rank", id=doc["_source"]["url"], body={
        "url": doc["_source"]["url"],
        "links": doc["_source"]["links"],
        "visited": doc["_source"]["visited"],
        "validation": doc["_source"]["validation"],
        "random_walk_rank": rank
    })


def summary():
    # get the lentgh of the documents
    all_docs = get_all_documents()
    number_of_walkes = 0
    for doc in all_docs:
        try:
            number_of_walkes += doc["_source"]["visited"]
        except:
            pass
    for doc in all_docs:
        # if visited exists
        try:
            print(f'INFO\tUpdating Ranks\t{round((doc["_source"]["visited"]/number_of_walkes)*100, 3)}\t{doc["_source"]["url"]}')
            update_rank(doc, round(doc["_source"]["visited"]/number_of_walkes, 5))
        except Exception as e:
            pass


create_index()
# add_kibana_dashboard()


def thread(event):
    while (True):
        if event.is_set():
            # print("Thread is sleeping")
            continue
        document = get_random_document()
        if random.random() > 0.85:
            # print("INFO\tResettting the surrfer DF=0.85")
            document = get_random_document()
        if "links" not in document:
            document = get_random_document()
            continue
        url = document["url"]
        # Delete the random link from the list if it is equal with the url
        new_links = []
        for link in document["links"]:
            if link == url or link == url+"#" or link == "javascript:void(0);" or link == url+"/#":
                continue
            new_links.append(link)

        # Pick random link that is not equal to the url
        random_link_from_page = random.choice(new_links)

        # Search for the random link in the index
        try:
            random_link_document = search_for_url(random_link_from_page)
            # Update the page rank of the url
            # print("I am on page: ", url," and I am going to: ", random_link_from_page)
            document = random_link_document
            walk(random_link_document["url"], random_link_document["links"])
            # print(count_documents())
        except Exception as e:
            print("INFO\tDead end resseting the Surfer")
            document = get_random_document()
            pass


print("INFO\tCreating 100 Surfer Threads")
e = threading.Event()
threads = list()

e.set()
for index in range(100):
    x = threading.Thread(target=thread, args=(e,))
    threads.append(x)
    x.start()

c.subscribe(['status'])
while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    if msg.value().decode('utf-8') == "SURF":
        e.clear()
        print("INFO\t Surfing")
        break
    if msg.value().decode('utf-8') == "STOP":
        e.set()
        print("INFO\t Stopping")
        break
    print('Received message: {}'.format(msg.value().decode('utf-8')))

    # summary()
    # e.clear()
