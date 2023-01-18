import random
import re
import sys
import time
from elasticsearch import Elasticsearch
import random
import threading
import networkx as nx
es = Elasticsearch(
    # ['http://es01:9200', 'http://es02:9200', 'http://es03:9200'],
    ['https://changethis:9200'],
    # sniff before doing anything
    basic_auth=("", "")
    )

def get_all_documents():
    # Get all documents from the index with the size of 10000
    return es.search(index="page_rank", body={"query": {"match_all": {}}, "size": 10000})["hits"]["hits"]

def get_document(url):
    return es.get(index="page_rank", id=url)

def update_rank(doc, rank, hits):
    doc = get_document(doc)
    es.index(index="page_rank", id=str(doc["_source"]["url"]), body={
        "url": doc["_source"]["url"],
        "links": doc["_source"]["links"],
        "visited": doc["_source"]["visited"],
        "validation": doc["_source"]["validation"],
        "hits": str(hits),
        "random_walk_rank": str(rank)
    })

DAMPING = 0.85

def do_HITS(docs):
    # create a graph
    G = nx.DiGraph()
    # add nodes
    for doc in docs:
        G.add_node(doc["_id"])
    # add edges
    for doc in docs:
        for link in doc["_source"]["links"]:
            G.add_edge(doc["_id"], link)
    # compute HITS
    hubs, authorities = nx.hits(G)
    # update the ranks in the index
    return authorities

def main():
    docs_unparsed = get_all_documents()
    docs = dict()
    for doc in docs_unparsed:
        docs[doc["_id"]] = set(doc["_source"]["links"])
    # print(docs)
    ranks = iterate_pagerank(docs, DAMPING)
    hits = do_HITS(docs_unparsed)
    print(f"PageRank Results from Iteration")
    for page in ranks:
        rank = ranks[page]
        update_rank(page, rank, hits[page])
        print(f"Updating {page} with rank {rank} and hits {hits[page]}")



def iterate_pagerank(docs, damping_factor):
    # for each page, determine which/how many other pages link to it
    # then, apply the PageRank formula
    # if change (new_rank, old_rank) < threshold update counter
    # if by the end of the loop, counter == N,
    # it means that the change in rank for each page in the docs was within the threshold
    # so end the loop
    # return rank
    ranks = dict()
    # convergence threshold
    threshold = 0.0005

    N = len(docs)
    for key in docs:
        ranks[key] = 1 / N

    while True:

        count = 0

        for key in docs:

            new = (1 - damping_factor) / N
            sigma = 0

            for page in docs:

                if key in docs[page]:
                    num_links = len(docs[page])
                    sigma = sigma + ranks[page] / num_links

            sigma = damping_factor * sigma
            new += sigma

            if abs(ranks[key] - new) < threshold:
                count += 1
            
            ranks[key] = new
        
        if count == N:
            break

    return ranks


if __name__ == "__main__":
    main()
