# Drug_analysis
Drug_analysis

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/e937a7f9-dece-4540-8e1e-3c5966896424/6964fb21-dc6b-4ec2-81d9-fb2c3eab2bc6/Untitled.png)

## ETL

1) 네이버 오픈 API 에서 뉴스를 크롤링 해서 GCS에 삽입합니다.

- 마약 관련 뉴스 크롤링 DAG
    
    ![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/e937a7f9-dece-4540-8e1e-3c5966896424/d15bfafb-5c6c-44f1-8780-039aaa513093/Untitled.png)
    
    ```python
    from datetime import datetime, timedelta
    from airflow import DAG
    from airflow.decorators import task
    from google.cloud import storage
    import pandas as pd
    import json
    import urllib.request
    from airflow.hooks.base_hook import BaseHook
    
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
    ```
    
- 펜타닐 관련 뉴스 크롤링 DAG
    
    ![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/e937a7f9-dece-4540-8e1e-3c5966896424/3617cb97-b726-4bed-ae7b-7bd12cc7678a/Untitled.png)
    
    ```python
    from datetime import datetime, timedelta
    from airflow import DAG
    from airflow.decorators import task
    from google.cloud import storage
    import pandas as pd
    import json
    import urllib.request
    from airflow.hooks.base_hook import BaseHook
    import logging
    
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
            logging.info(start)
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
        result_all = pd.DataFrame(result_all)
    
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
        destination_blob_name = "fentanyl_crawl/"+formatted_time
        storage_client = storage.Client.from_service_account_json('/opt/airflow/dags/gootrend-5d6987d9756f.json')
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(result_all.to_csv(), 'text/csv')
    
    # Airflow DAG를 정의
    with DAG(
        dag_id = 'news_fentanyl',
        start_date=datetime(2021, 1, 1),
        schedule = '0 4 * * *', #매일 오전 10시
        catchup=False,
    ) as dag:
        data = getnews('2H6Bed5BlAVm0yASXDr2', 'KPlgWNJk6R', '펜타닐', 100, 1, 'date')
        transform = getresult(data)
        send_to_gcs('sol_search_trend',transform)
    ```
    

2) GCE에 있는 mysql에 저장되어 있는 drug_death table과 opioids table을 접근하여 bigquery search dataset에 테이블로 저장합니다.

- Drug Death table to Biquery DAG
    
    ![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/e937a7f9-dece-4540-8e1e-3c5966896424/07382b45-a28e-49c9-b418-d2f5f8c3e208/Untitled.png)
    
    ```python
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    from datetime import timedelta
    from google.cloud import storage
    from google.cloud import bigquery
    import pandas as pd
    from io import StringIO
    import json
    import logging
    from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
    
    # GCS에서 CSV 데이터를 읽어서 DataFrame으로 변환하는 함수
    
    dag = DAG(
        dag_id = 'gcs_to_bigquery',
        start_date = datetime(2023,1,1), # 날짜가 미래인 경우 실행이 안됨
        schedule = '0 6 * * *',  # 적당히 조절
        max_active_runs = 1,
        catchup = False,
        default_args = {
            'retries': 1,
            'retry_delay': timedelta(minutes=3),
        }
    )
    
    gcs_to_bq_drug = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq_drug',
        bucket='sol_search_trend',
        source_objects=['drug_crawl/'+ datetime.now().strftime("%Y-%m-%d")],
        destination_project_dataset_table='gootrend.search.drug',
        source_format='CSV',
        autodetect=True,
        write_disposition='WRITE_APPEND',
        delegate_to=None,
        dag=dag)
    
    gcs_to_bq_fent = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq_fent',
        bucket='sol_search_trend',
        source_objects=['fentanyl_crawl/'+ datetime.now().strftime("%Y-%m-%d")],
        destination_project_dataset_table='gootrend.search.fent',
        source_format='CSV',
        autodetect=True,
        write_disposition='WRITE_APPEND',
        delegate_to=None,
        dag=dag)
    
    gcs_to_bq_drug >> gcs_to_bq_fent
    ```
    
- Opioids Death table to Bigquery DAG
    
    ![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/e937a7f9-dece-4540-8e1e-3c5966896424/e0cb85c0-9db6-4b3c-b6bd-db23a2f4cbaf/Untitled.png)
    
    ```python
    from datetime import datetime, timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from google.cloud import bigquery
    from google.oauth2 import service_account
    import pandas as pd
    import mysql.connector
    
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 23),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
    
    dag = DAG(
        'mysql_to_bigquery',
        default_args=default_args,
        description='A simple DAG to move data from MySQL to BigQuery',
        schedule_interval=timedelta(days=1),
    )
    
    def mysql_to_bigquery():
        # MySQL connection
        cnx = mysql.connector.connect(user='guest', password='guest1234!1',
                                    host='34.22.109.152', port = 3306,
                                    database='solution')
        query = "SELECT * FROM opioids"
        df = pd.read_sql(query, cnx)
    
        # BigQuery connection
        credentials = service_account.Credentials.from_service_account_file(
            '/opt/airflow/dags/gootrend-5d6987d9756f.json')
        client = bigquery.Client(credentials=credentials, project='gootrend')
    
        # Upload to BigQuery
        dataset_ref = client.dataset('search')
        table_ref = dataset_ref.table('opioids')
        job = client.load_table_from_dataframe(df, table_ref)
    
        job.result()  # Wait for the job to complete
    
    t1 = PythonOperator(
        task_id='mysql_to_bigquery',
        python_callable=mysql_to_bigquery,
        dag=dag,
    )
    
    t1
    ```
    

## ELT

1) GCS에 있는 날짜별 마약관련 뉴스 데이터를 bigquery로 가지고 옵니다.

- GCS to Bigquery DAG
    
    ![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/e937a7f9-dece-4540-8e1e-3c5966896424/a0b11447-e290-4cc8-87b7-edea3fa95c20/Untitled.png)
    
    ```python
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    from datetime import timedelta
    from google.cloud import storage
    from google.cloud import bigquery
    import pandas as pd
    from io import StringIO
    import json
    import logging
    from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
    
    # GCS에서 CSV 데이터를 읽어서 DataFrame으로 변환하는 함수
    
    dag = DAG(
        dag_id = 'gcs_to_bigquery',
        start_date = datetime(2023,1,1), # 날짜가 미래인 경우 실행이 안됨
        schedule = '0 6 * * *',  # 적당히 조절
        max_active_runs = 1,
        catchup = False,
        default_args = {
            'retries': 1,
            'retry_delay': timedelta(minutes=3),
        }
    )
    
    gcs_to_bq_drug = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq_drug',
        bucket='sol_search_trend',
        source_objects=['drug_crawl/'+ datetime.now().strftime("%Y-%m-%d")],
        destination_project_dataset_table='gootrend.search.drug',
        source_format='CSV',
        autodetect=True,
        write_disposition='WRITE_APPEND',
        delegate_to=None,
        dag=dag)
    
    gcs_to_bq_fent = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq_fent',
        bucket='sol_search_trend',
        source_objects=['fentanyl_crawl/'+ datetime.now().strftime("%Y-%m-%d")],
        destination_project_dataset_table='gootrend.search.fent',
        source_format='CSV',
        autodetect=True,
        write_disposition='WRITE_APPEND',
        delegate_to=None,
        dag=dag)
    
    gcs_to_bq_drug >> gcs_to_bq_fent
    ```
    

## 시각화

1) bigquery에 있는 search dataset과 analytics dataset을 통해서 시각화합니다.

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/e937a7f9-dece-4540-8e1e-3c5966896424/83347b18-d8ee-4735-8920-4f5366e5ee3b/Untitled.png)

1)뉴스를 통해 해당 날짜별로 관심도가 어떤지 확인할 수 있습니다

2)마약 종류별 과다복용 사망 비율을 알 수 있습니다

3) 연도마다 마약 과다복용을 통한 사망률의 상승세를 성별, 종합 으로 확인할 수 있습니다
