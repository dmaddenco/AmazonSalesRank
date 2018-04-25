import ast
import json
import sys


def get_json_meta(path):
    sales_rank_data = []
    sales_rank_asin = set()
    non_sales_rank_data = []
    with open(path, 'r') as file:
        data = file.readlines()
        for line in data:
            parsed_json = ast.literal_eval(line)
            if parsed_json.keys().__contains__("salesRank"):
                sales_rank_asin.add(parsed_json.get("asin"))
                sales_rank_data.append(parsed_json)
            else:
                non_sales_rank_data.append(parsed_json)
    return sales_rank_data, non_sales_rank_data, sales_rank_asin


def get_json_reviews(path, good_asin):
    sales_rank_data = []
    non_sales_rank_data = []
    with open(path, 'r') as file:
        data = file.readlines()
        for line in data:
            parsed_json = ast.literal_eval(line)
            asin = parsed_json.get("asin")
            if asin in good_asin:
                sales_rank_data.append(parsed_json)
            else:
                non_sales_rank_data.append(parsed_json)
    return sales_rank_data, non_sales_rank_data


def write_output(path, data):
    with open(path, 'w') as file:
        for line in data:
            file.write(json.dumps(line) + "\n")


if __name__ == '__main__':
    meta = sys.argv[1]
    reviews = sys.argv[2]

    sales_rank, non_sales_rank, sales_rank_asin = get_json_meta(meta)
    sales_rank_reviews, non_sales_rank_reviews = get_json_reviews(reviews, sales_rank_asin)

    write_output("training_" + meta, sales_rank)
    write_output("testing_" + meta, non_sales_rank)

    write_output("training_" + reviews, sales_rank_reviews)
    write_output("testing_" + reviews, non_sales_rank_reviews)
