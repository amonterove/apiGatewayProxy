from elasticsearch import Elasticsearch

if __name__ == '__main__': 
    es = Elasticsearch(
		['0.0.0.0']
	)
    res1 = es.search(index="index_name", doc_type="index_type", body={"query": {"match": {"id": "10000"}}})
    print("%d documents found:" % res1['hits']['total'])
    size = res1['hits']['total']
    print(size)
    res2 = es.search(index="index_name", doc_type="index_type", body={"query": {"match": {"id": "10000"}}, "size": size})
    for doc in res2['hits']['hits']:
        print((doc['_id'], doc['_source']['idu'], doc['_source']['id'], doc['_source']['DATE_MAX']))

