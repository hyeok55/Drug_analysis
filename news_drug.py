from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from google.cloud import storage
import pandas as pd
import json
import urllib.request
from airflow.hooks.base_hook import BaseHook


"""
네, 맞습니다. Apache Airflow의 XCom은 기본적으로 JSON 직렬화를 사용하여 값을 다른 태스크로 전달합니다. 이는 값이 다른 태스크에서 사용될 때 데이터의 일관성과 호환성을 보장하기 위한 방법입니다.

기본적으로 Airflow는 Python의 json.dumps() 함수를 사용하여 값을 JSON 형식으로 직렬화합니다. 그러나 DataFrame과 같은 특정 객체는 직렬화할 수 없는 경우가 있으므로 이러한 경우에는 직렬화 가능한 형식으로 변환해야 합니다.

DataFrame을 JSON으로 직렬화하려면 DataFrame을 리스트 또는 딕셔너리로 변환한 후 json.dumps()를 사용하여 JSON 형식으로 변환할 수 있습니다. 예를 들어, df.to_dict()를 사용하여 DataFrame을 딕셔너리로 변환한 후 json.dumps()를 사용하여 딕셔너리를 JSON 형식으로 변환할 수 있습니다.

이렇게 변환한 값을 XCom으로 전달하면 정상적으로 작동할 것입니다.

더 도움이 필요하시면 알려주세요!

"""

@task
def getnews(client_id, client_secret, query, display=100, start= 1, sort='date'):
    result_news = pd.DataFrame()
    for i in range(0, 10):
        start = 1 + 100 * i

        encText = urllib.parse.quote(query)
        url = "https://openapi.naver.com/v1/search/news?query=" + encText + \
            "&display=" + str(display) + "&start=" + str(start) + "&sort=" + sort

        request = urllib.request.Request(url)
        request.add_header("X-Naver-Client-Id", client_id)
        request.add_header("X-Naver-Client-Secret", client_secret)
        response = urllib.request.urlopen(request)
        rescode = response.getcode()
        if(rescode==200):
            response_body = response.read()
            response_json = json.loads(response_body)
        else:
            print("Error Code:" + rescode)

        result = pd.DataFrame(response_json['items'])
        result_news = pd.concat([result_news, result])

    result_news = result_news.to_json(orient='records')
    return result_news

# Naver News API를 사용하여 데이터를 가져오는 함수
@task
def getresult(result_all):

    result_all = pd.read_json(result_all, orient='records')

    result_all=result_all.reset_index() # index가 100단위로 중복되는것을 초기화
    result_all=result_all.drop('index',axis=1) # reset_index후 생기는 이전 index의 column을 삭제
    result_all['pubDate']=result_all['pubDate'].astype('datetime64[ns]') # pubDate의 타입을 object에서 datetime으로 변경
    result_all['Date'] = result_all['pubDate'].dt.strftime('%Y%m%d')#날짜별 집계를 위해 'YYYYmmdd' 타입의 column을  result_all = result_all[['title','originallink','Date']]
    result_all = result_all[['title','originallink','Date']]
    
    result_all = result_all.to_dict()
    result_all = json.dumps(result_all)

    return result_all



# Google Cloud Storage에 데이터를 보내는 함수

@task
def send_to_gcs(bucket_name,data):
    """Uploads a file to Google Cloud Storage."""
    result_all = json.loads(data)
    result_all = pd.DataFrame(result_all)
    current_time = datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d")    
    destination_blob_name = "drug_crawl/"+formatted_time
    storage_client = storage.Client.from_service_account_json('/opt/airflow/dags/gootrend-5d6987d9756f.json')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(result_all.to_csv(), 'text/csv')

# Airflow DAG를 정의
with DAG(
    dag_id = 'news_drug',
    start_date=datetime(2021, 1, 1),
    schedule = '0 4 * * *', #매일 오전 10시
    catchup=False,
) as dag:
    data = getnews('2H6Bed5BlAVm0yASXDr2', 'KPlgWNJk6R', '마약', 100, 1, 'date')
    transform = getresult(data)
    send_to_gcs('sol_search_trend',transform)
datetime.now().strftime("%Y-%m-%d") 