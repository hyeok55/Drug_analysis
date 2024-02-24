# DrugSafe - Drug_analysis
Through Naver Open api and opiod abuse data, the number of deaths caused by drug use and related news are analyzed and visualized to show the current state of the drug problem and interest in drugs.


## ETL

1) Crawling news from Naver Open API and inserting it into GCS.

- Drug-related news crawling DAG
  
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
    
    # Functionality using Na Server News API
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
    
    # Functions that send data to Google Cloud Storage
    
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
    
    # Airflow DAG
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
    
- Fentanyl-related news crawling DAG 
    
 
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
    
    # Functions to import data using the Naver News API
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
    
    # Functions that send data to Google Cloud Storage
    
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
    

2) Access the drug_death table and opioids table stored in mysql on the GCE and save them as tables in the bigquery search database.

- Drug Death table to Biquery DAG
        
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

1) Bring the date-specific drug-related news data from GCS to bigquery.

- GCS to Bigquery DAG
     
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
    

## Visualization

Visualize the search dataset and the analytics dataset in the bigquery through the Looker Studio.
<img width="783" alt="image" src="https://github.com/hyeok55/solution_challenge_2024/assets/67605795/85ae2a3e-c50c-440d-ab29-3eb15bb5dcf7">

1)You can check how interested you are by the date through the news
2)You can see the overdose death rate by drug type
3) We can see the rise in mortality rates through drug overdoses each year by gender and comprehensively

## Data
[Kaggle:https://www.kaggle.com/datasets/mexwell/us-opioid-abuse](https://www.kaggle.com/datasets/mexwell/us-opioid-abuse)
[Naver open API:https://developers.naver.com/docs/common/openapiguide](https://developers.naver.com/docs/common/openapiguide)
