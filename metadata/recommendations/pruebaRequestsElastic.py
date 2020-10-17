"""
Simple example of querying Elasticsearch creating REST requests
"""
import requests
import json

def search(uri):
    response = requests.get(uri)
    results = json.loads(response.text)
    return results

def format_results(results):
    data = [doc for doc in results['hits']['hits']]
    print("%d documents found:" % results['hits']['total'])
    print(results['hits']['hits'])
    for doc in data:
	print((doc['_id'], doc['_source']['idu'], doc['_source']['id'], doc['_source']['DATE_MAX']))

if __name__ == '__main__':
    uri_search = 'http://0.0.0.0:9200/index_name/_search'

    results = search(uri_search)
    format_results(results)
