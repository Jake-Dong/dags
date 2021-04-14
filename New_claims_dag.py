import xml.etree.ElementTree as ET
import epo_ops
import csv
import time
from datetime import datetime, timedelta
import pandas
import pymysql
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

def claim_DB():
    client = epo_ops.Client(key='o2TgZqLMPnGxmiFk7rUUB0bTq9VZDbe1', secret='ZHYxcGxi9UprBTUD')

    yesterday = datetime.today() - timedelta(8)

    # date = yesterday.strftime("%Y%m%d")
    date = '20210324'

    host_ip = '34.68.250.64'

    # DB 에서 pubilcation 정보를 불러오는 코드.
    try:
        conn =pymysql.connect(host=host_ip
                              , user='root'
                              , password= 'admin'
                              , database='test'
                              , write_timeout= 3000)
        cur = conn.cursor()
        sql = "SELECT * FROM pub_num_data_{}".format(date)
        cur.execute(sql)
        conn.commit()
    except Exception as ex:
        print(ex,"DB 문제")
    finally:
        conn.close()
    datas = cur.fetchall()

    # 불러온 publication 정보를 for 반목문을 돌리기위해 list 에 넣는 코드.
    app_doc_num_list = []
    country_list = []
    kind_list = []
    simple_family_id = []
    for data in datas:

        family_id = data[1]
        pub_country = data[2]
        pub_number = data[3]
        pub_kind = data[4]

        app_doc_num_list.append(pub_number)
        country_list.append(pub_country)
        kind_list.append(pub_kind)
        simple_family_id.append(family_id)

    claims_all_list = []
    for doc_num,country,kind in zip(app_doc_num_list,country_list,kind_list):
        pub_num = country+str(doc_num)+kind
        try:
            response = client.published_data(
                    reference_type = 'publication'
                    , input = epo_ops.models.Docdb(str(doc_num),country,kind)
                    ,endpoint='claims'
            )
        except Exception as ex:
            print(ex,"EPO 연결 문제")
        else:
            xmlStr = response.text
            roots = ET.fromstring(xmlStr)

            claims_list = []
            for claims in roots.iter('{http://www.epo.org/fulltext}claim-text'):
                claims_text = claims.text
                claims_list.extend([claims_text])
            claims_all_list.extend([pub_num,claims_list])

    try:
        conn = pymysql.connect(
            host=host_ip
            , user='root'
            , password='admin'
            , database='test'
        )
        cur = conn.cursor()
        for row in claims_all_list:
            sql_1 = "INSERT INTO citation_info_test_1 VALUES(%s,%s,%s,%s,%s);"
            val = row
            cur.execute(sql_1, val)
            conn.commit()
    except Exception as ex:
        print(ex,"DB 연결 문제")
    finally:
        conn.close()

local_tz = pendulum.timezone('Asia/Seoul')
today = datetime.today()

default_dag_args = {
    "owner": "airflow",
    "start_date": datetime(today.year, today.month, today.day, tzinfo=local_tz) - timedelta(hours=25)
}
dag = DAG(
    dag_id='claim_DB'
    , default_args=default_dag_args
    , schedule_interval='0 9 * * *'
    # , schedule_interval=timedelta(1)
)
task1 = PythonOperator(
    task_id='claim_DB'
    , python_callable=claim_DB
    , dag=dag
)
