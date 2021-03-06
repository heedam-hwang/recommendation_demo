from run import *


def main():
    mapping_info = \
            {
                "mappings": {
                    "properties": {
                        "userId": {"type": "keyword"},
                        "products": {"type": "keyword"}
                    }
                }
            }
    index_name = "recommendation_demo"
    es = ESDemo(mapping_info)
    es.create_index(index_name)
    while True:
        target_user_id = int(input("insert user id to get item recommendation(0: break, 1~671: valid): "))
        if target_user_id == 0:
            break
        es.get_recommendation(index_name, target_user_id)



if __name__ == "__main__":
    main()
