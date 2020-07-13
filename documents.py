# this module creates documents that will be inserted into indices
# from input csv files
import csv
import json


#
#
# def make_documents():
#     input_file = open('ratings.csv', 'r')
#     output_file = open('records.json', 'w')
#     field_names = ("userId", "movieId", "rating", "timestamp")
#     input_reader = csv.DictReader(input_file, field_names)
#     for row in input_reader:
#         json.dump(row, output_file)
#         output_file.write('\n')
#     input_file.close()
#     output_file.close()


# this function reads ratings.csv and prepare input documents for recommendation index
# i.e. it collects movieIds whose rating is over 4.0
def create_documents(filename):
    result = []
    records = {}
    with open(filename, 'r') as f:
        input_reader = csv.reader(f)
        next(input_reader)
        for line in input_reader:
            if float(line[2]) >= 4.0:
                user_id = int(line[0])
                movie_id = int(line[1])
                if not records.get(user_id):
                    records[user_id] = [movie_id]
                else:
                    records[user_id].append(movie_id)
    for k, v in records.items():
        record = {"userId": k, "products": v}
        result.append(record)
    return result
