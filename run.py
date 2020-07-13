import datetime
import documents

from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers


def extract(res):
    return [x['_source'] for x in res['hits']['hits']]


class ESDemo:
    def __init__(self, map_info):
        self.es = Elasticsearch("http://localhost:9200/")
        self.mapping_info = map_info

    def create_index(self, index_name):
        if not self.es.indices.exists(index=index_name):
            self.es.indices.create(index=index_name, body=self.mapping_info)
            self.insert_documents(index_name)

    def insert_documents(self, index_name):
        records = documents.create_documents("ratings.csv")
        for r in records:
            print(r)
            self.es.index(index=index_name, body=r)
        self.es.indices.refresh()

    # find users who have more than 2 products in common and get those items
    def get_recommendation(self, index_name, target_user_id):
        # find products of target user
        item_res = self.es.search(
            index=index_name,
            body={
                "query": {
                    "match": {
                        "userId": target_user_id
                    }
                }
            }
        )
        target_user_items = extract(item_res)[0]['products']

        # make term queries out of those items and get recommendation
        rec_res = self.es.search(
              index=index_name,
              body={
                "query": {
                  "bool": {
                    "should": [{"term": {"products": x}} for x in target_user_items],
                    "minimum_should_match": 2,
                  }
                },
                "aggs": {
                  "recommendations": {
                    "significant_terms": {
                      "field": "products",
                      "exclude": target_user_items,
                      "min_doc_count": 10
                    }
                  }
                }
              }
            )
        recommended_items = [x['key'] for x in rec_res['aggregations']['recommendations']['buckets']]
        print(recommended_items)
