import json
import datetime
from pytz import timezone, utc


onesec = datetime.timedelta(seconds=1)
oneday = datetime.timedelta(days=1)
chainID = "mychainid-111111111111111111111111111d1111111"
indexType = "node"

def gendoc(curdate, blockno, txcount):
    curdatestr = curdate.isoformat()

    #print(curdatestr)

    doc = {
        "@timestamp": f"{curdatestr}",
        "block_meta": {
            "header": {
                "block_no": blockno,
                "tx_count": txcount,
                "chain_id": "4pRYVm2aU3zN1PsczgQFihzrFZe8MZtTTaqovREp7vS8",
                "txs_root_hash": chainID,
                "confirms": 0,
                "sign": "381yXZ2oTXWBULPsXD4JQtjudynzE9TsvDRd9ERjf7Csxagb5brmt4X2U21NKfxagR2XxaPm2p157HqLQRCnxxbAro5nWxu4",
                "previous_block_hash": "2rMTL6iLLwadoqzE85vmtrqzqz9ey8Be8FTwXjWsd3r8",
                "blocks_root_hash": "BgH2S6ypRn3gi1kguAovqxaDHoT34nQxWyB4cQ2oX341",
                "timestamp": 1568764795614473900,
                "receipts_root_hash": "11111111111111111111111111111111",
                "pub_key": "GZsJqUVM3QHVANAb2U9TGGoawjn6Tn2Wipzdeuzy1CcYjfFxuq"
            },
            "hash": "FGxUBNkjAzQQ6GYcMbzWvmUHovuvcXiUv8P34XkyryVV"
        },
        "node_id": "d4b2fc83-976a-4eef-a342-6dc87f87afe8"
    }


    return doc

from elasticsearch import Elasticsearch
from elasticsearch import helpers


# 일레스틱서치 IP주소와 포트(기본:9200)로 연결한다
es = Elasticsearch("http://13.209.67.143:9200/") # 환경에 맞게 바꿀 것
es.info()

# 인덱스는 독립된 파일 집합으로 관리되는 데이터 덩어리이다
def makeIndex(es, index_name):
    """인덱스를 신규 생성한다(존재하면 삭제 후 생성) """
    print(f"make index for {index_name}")

    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
    print(es.indices.create(index=index_name))

def genBulkEntry(newdoc, indexName, indexType):
    #body = json.dumps(doc)
    #print(body)
    #es.index(index=indexName, doc_type='node', body=newdoc)
    return {
        "_index": indexName,
        "_type": indexType,
        "_source": newdoc
    }



def outer_func():
    previndexname = ""

    def genEsDocs(fromdate, todate, startblkno):
        nonlocal previndexname
        i = fromdate
        curblkno = startblkno

        while i < todate:
            indexName = f"everjs-{i.year}.{i.month}.{i.day}"

            if previndexname != indexName:
                print(f"prev{previndexname}, cur{indexName}")
                makeIndex(es, indexName)

            doc = gendoc(i, curblkno, 1)
            #genBulkEntry(doc, indexName, indexType)
            yield genBulkEntry(doc, indexName, indexType)

            previndexname = indexName
            curblkno += 1
            i += onesec

    return genEsDocs

fnGenEsDocs = outer_func()

testdate = datetime.datetime(2018, 1, 1, 0, 0, 0, 0, tzinfo=utc)
testblkno = 1

#fnGenEsDocs(testdate, testdate + onesec * 1, testblkno)
#exit(0)

for tempdoc in fnGenEsDocs(testdate, testdate + onesec * 1, testblkno):
    print(tempdoc)

#Elasticsearch.bulk(es, fnGenEsDocs(testdate, testdate + onesec * 1, testblkno))

ret = helpers.bulk(es, fnGenEsDocs(testdate, testdate + oneday * 31, testblkno))
print(ret)
exit(0)


# index_name = 'goods'
# make_index(es, index_name) # 상품 데이터 덩어리(인덱스)를 생성한다
#
# # 데이터를 저장한다
# doc1 = {'goods_name': '삼성 노트북 9',    'price': 1000000}
# doc2 = {'goods_name': '엘지 노트북 그램', 'price': 2000000}
# doc3 = {'goods_name': '애플 맥북 프로',   'price': 3000000}
# es.index(index=index_name, doc_type='string', body=doc1)
# es.index(index=index_name, doc_type='string', body=doc2)
# es.index(index=index_name, doc_type='string', body=doc3)
# es.indices.refresh(index=index_name)
#
# # 상품명에 '노트북'을 검색한다
# results = es.search(index=index_name, body={'from':0, 'size':10, 'query':{'match':{'goods_name':'노트북'}}})
# for result in results['hits']['hits']:
#     print('score:', result['_score'], 'source:', result['_source'])

