import datetime
import documents

from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers


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

    # product number 2개 이상 겹치는 유저한테 가서 그 아이템 받아오기
    def get_recommendation(self, index_name, target_user_id):
        target_user_items = []
        res = self.es.search(
            index=index_name,
            body={
                "query": {
                    "match": {
                        "userId": target_user_id
                    }
                }
            }
        )
        print(res)
