import requests
import base64
import json
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import pandas
import pymysql
import time
from multiprocessing import Process, Queue
import pendulum
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

start_time = time.time()

# ====================================EPO 토큰 발행
# EPO 에서 도 request 툴을 사용하는 사람을위해 만든 암호토큰발급 서비스이다.
# request 요청시 header 값에 암호정보가 같이 보내 지게 되는데 이때 파라미터값으로는 1개의 입력값만 받을수 있게된다.
# 하지만
def make_token():
    token_url = 'https://ops.epo.org/3.2/auth/accesstoken'

    key = 'Basic %s' % base64.b64encode(b'o2TgZqLMPnGxmiFk7rUUB0bTq9VZDbe1:ZHYxcGxi9UprBTUD').decode('ascii')
    data = {'grant_type': 'client_credentials'}
    headers = {'Authorization': key, 'Content-Type': 'application/x-www-form-urlencoded'}

    r = requests.post(token_url, data=data, headers=headers)

    rs = r.content.decode()
    response = json.loads(rs)

    token = response['access_token']
    return token
def pub_info(date_start_range,date_end_range):
    dt_index = pandas.date_range(start=date_start_range, end=date_end_range)
    # yesterday = datetime.today() - timedelta(7) # 매일 매일 어제의 데이터(태이블)를 가지고 와야 할때
    # pandas.date_range(start='20200901', end='20201031',freq='W-MON') 을 하면 해당 기간 매주 월요일들만 추출합니다.
    # type(dt_index) => DatetimeIndex
    # DatetimeIndex => list(str)
    dt_list = dt_index.strftime("%Y%m%d").tolist()
    host_ip = '34.68.250.64'  # 내가 접속하려는 DB서버의 ip
    user_id = 'root'  # DB 서버에서 외부 접근 권한을 부여한 user id 본인은 root 계정으로 이름을 만들고 0.0.0.0 (모든 아이피 접근 권한) 을주었다.
    user_password = 'admin'  # 외부 권한 접근시(user_id : root) 필요한 password.
    port_number = 3306  # 외부 ip 가 접근할때 열어주는 포트번호 mariaDB 는 기본적으로 3306 포트를 사용한다.
    DB_database = 'test'  # mariaDB 에서 작업할 database 이름.
    for date in dt_list:
        # db 에 저장되어있는 publication 정보를 가져오는 코드
        try:
            conn = pymysql.connect(
                host=host_ip
                , user=user_id
                , password=user_password
                , database=DB_database
                , port = port_number
                , connect_timeout = 3000
            )
            cur = conn.cursor()
            sql = "SELECT DISTINCT * FROM pub_num_data_{}".format(date)
            cur.execute(sql)
            conn.commit()
        except Exception as ex:
            print(ex,"DB 접속 오류.")
        finally:
            conn.close()
        datas = cur.fetchall()
    return datas

def data(start_index,end_index):
    #=====================================tor 프록시 설정.
    proxies = {'http': 'socks5://127.0.0.1:9050',
               'https': 'socks5://127.0.0.1:9050'}
    # ====================================DB 접속 및 publication-number 데이터 가져오기

    host_ip = '34.68.250.64'  # 내가 접속하려는 DB서버의 ip
    user_id = 'root'  # DB 서버에서 외부 접근 권한을 부여한 user id 본인은 root 계정으로 이름을 만들고 0.0.0.0 (모든 아이피 접근 권한) 을주었다.
    user_password = 'admin'  # 외부 권한 접근시(user_id : root) 필요한 password.
    port_number = 3306  # 외부 ip 가 접근할때 열어주는 포트번호 mariaDB 는 기본적으로 3306 포트를 사용한다.
    DB_database = 'test'  # mariaDB 에서 작업할 database 이름.

    # dt_index = pandas.date_range(start='20210222', end='20210222')
    # yesterday = datetime.today() - timedelta(7) # 매일 매일 어제의 데이터(태이블)를 가지고 와야 할때
    # pandas.date_range(start='20200901', end='20201031',freq='W-MON') 을 하면 해당 기간 매주 월요일들만 추출합니다.
    # type(dt_index) => DatetimeIndex
    # DatetimeIndex => list(str)
    # dt_list = dt_index.strftime("%Y%m%d").tolist()

    yesterday = datetime.today() - timedelta(7)
    dt_list = yesterday.strftime("%Y%m%d")
    family_all_list = []
    for date in dt_list:
        # db 에 저장되어있는 publication 정보를 가져오는 코드
        try:
            conn = pymysql.connect(
                host=host_ip
                , user=user_id
                , password=user_password
                , database=DB_database
                , port = port_number
                , connect_timeout = 3000
            )
            cur = conn.cursor()
            sql = "SELECT DISTINCT * FROM pub_num_data_{}".format(date)
            cur.execute(sql)
            conn.commit()
        except Exception as ex:
            print(ex,"DB 접속 오류.")
        finally:
            conn.close()
        datas = cur.fetchall()

        docdb_pub_num_list = []
        family_id_list = []
        for data in datas[start_index:end_index]:
            family_id = data[0]
            pub_country = data[1]
            pub_number = data[2]
            pub_kind = data[3]

            docdb_pub_number = pub_country+str(pub_number)+pub_kind
            family_id_list.append(family_id)
            docdb_pub_num_list.append(docdb_pub_number)

        service = 'family'
        DB_type = 'docdb'
        header = {'Authorization': "Bearer " + str(make_token())}  # 헤더에 위에서 만든 토큰을 포함시켜 requests 요청을 보낼때 토큰정보가 전송되도록 해준다.
        i = 0
        for doc_number,simple_family in zip(docdb_pub_num_list,family_id_list):
            simple_family_pub_number_list = []
            simple_family_pub_country_list = []
            inp_family_pub_number_list = []
            inp_family_pub_country_list = []
            i += 1
            if i % 50 == 0:
                header = {'Authorization': "Bearer " + str(make_token())}  # 헤더에 위에서 만든 토큰을 포함시켜 requests 요청을 보낼때 토큰정보가 전송되도록 해준다.
                print('토큰 생성완료')
            else:
                pass

            myUrl = 'http://ops.epo.org/3.2/rest-services/' + service + '/publication/' + DB_type + '/' + str(doc_number)
            response = requests.get(myUrl, headers=header, proxies=proxies)
            xmlStr = response.text
            roots = ET.fromstring(xmlStr)
            # print(xmlStr)

            for family_member in roots.iter('{http://ops.epo.org}family-member'):
                family_member_id = family_member.attrib.get('family-id')
                family_member_id_set = list(set(family_member_id))
                family_id_list.extend([family_member_id_set])
                # simple_fam_id 과 inpadoc_fam_id 를 나누기위한 if 조건문.
                # 각각의 패밀리id 를 기존 simple_fam_id 와 비교하여 맞는것은 simple  아닌것은 inpadoc 으로 판단하고 각각 따로 변수에 담는다.
                if str(simple_family) == str(family_member_id):
                    pub_country = family_member[0][0][0].text
                    pub_doc_number = family_member[0][0][1].text
                    pub_kind = family_member[0][0][2].text
                    simple_family_pub_number_list.extend([
                        pub_country
                        + pub_doc_number
                        + pub_kind
                    ])
                    simple_family_pub_country_list.extend([pub_country])
                # ----------------------------inpadoc family 출력------------------------------
                else:
                    pub_country = family_member[0][0][0].text
                    pub_doc_number = family_member[0][0][1].text
                    pub_kind = family_member[0][0][2].text
                    inp_family_pub_number_list.extend([
                        pub_country
                        + pub_doc_number
                        + pub_kind
                    ])
                    inp_family_pub_country_list.extend([pub_country])

            count_simple_family_pub_number_list = len(simple_family_pub_number_list)
            count_simple_family_pub_country_list = len(set(simple_family_pub_country_list))
            simple_family_pub_country_set_list = list(set(simple_family_pub_country_list))
            count_inp_family_pub_number_list = len(inp_family_pub_number_list)
            count_inp_family_pub_country_list = len(set(inp_family_pub_country_list))
            inp_family_pub_country_set_list = list(set(inp_family_pub_country_list))

            if count_simple_family_pub_number_list == 0:
                count_simple_family_pub_number_list = 'null'

            if count_simple_family_pub_country_list == 0:
                count_simple_family_pub_country_list = 'null'

            if count_inp_family_pub_number_list == 0:
                count_inp_family_pub_number_list = 'null'

            if count_inp_family_pub_country_list == 0:
                count_inp_family_pub_country_list = 'null'
            else:
                pass

            # DB 에 적재 하려면 콤마구분자를 바꾸어줘야 함. 이유는 DB에서의 콤마명령어가 존재하기때문에 언어 명령이 꼬일수 있기때문.

            simple_family_pub_number_list_join = '|'.join(simple_family_pub_number_list)
            simple_family_pub_country_set_list_join = '|'.join(simple_family_pub_country_set_list)

            inp_family_pub_number_list_join = '|'.join(inp_family_pub_number_list)
            inp_family_pub_country_set_list_join = '|'.join(inp_family_pub_country_set_list)

            family_all_list.append([doc_number
                                       , simple_family_pub_number_list_join
                                       , count_simple_family_pub_number_list
                                       , simple_family_pub_country_set_list_join
                                       , count_simple_family_pub_country_list
                                       , inp_family_pub_number_list_join
                                       , count_inp_family_pub_number_list
                                       , inp_family_pub_country_set_list_join
                                       , count_inp_family_pub_country_list
                                       , date])
            print(doc_number)

    try:
        conn = pymysql.connect(
            host=host_ip
            , user=user_id
            , password=user_password
            , database=DB_database
            , port=port_number
        )
        cur = conn.cursor()
        for row in family_all_list:
            sql_1 = "INSERT INTO family_info VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            val = row
            cur.execute(sql_1, val)
            conn.commit()
    except Exception as ex:
        print(ex, "DB insert 문제")
    finally:
        conn.close()
#-------------------------------------멀티 스레딩 부분.
def New_familt_DB():
    yesterday = datetime.today() - timedelta(7)

    dt_list = yesterday.strftime("%Y%m%d")
    datas = pub_info(str(dt_list),str(dt_list))
    count_pub = len(datas)
    start = 0
    end = count_pub # 수집을 원하는 날짜의 pub_info의 total_count 를 획인하고 그를 6등분 하여 6개의 멀티 코드로 실행. 쉽게 말해서 1개의 리스트를 6개로 나누어 코드를 돌리는것이기 때문에 6배 빠름.
    if count_pub < 6:
        start_th1, end_th1 = start,end
        start_th2, end_th2 = start,start
        start_th3, end_th3 = start,start
        start_th4, end_th4 = start,start
        start_th5, end_th5 = start,start
        start_th6, end_th6 = start,start

    else:
        start_th1, end_th1 = start, end // 6 #만약 total_count 가 12 이면 1~2,2~3,3~4 식으로 짤라서 데이터를 수집후 DB에 저장 하도록 해줌.
        start_th2, end_th2 = 1 * (end // 6 ), 2 * (end // 6)
        start_th3, end_th3 = 2 * (end // 6) , 3 * (end // 6)
        start_th4, end_th4 = 3 * (end // 6) , 4 * (end // 6)
        start_th5, end_th5 = 4 * (end // 6) , 5 * (end // 6)
        start_th6, end_th6 = 5 * (end // 6) , end

    if __name__ == '__main__':
        queue = Queue()

        th1 = Process(target=data,args=(start_th1,end_th1)) # 프로세스 1개당 1개의 실행 으로 보면 된다. data 함수를 사용하고 입력값을 위에서 만든 튜플형태의 값을 넣어주게 된다.
        th2 = Process(target=data,args=(start_th2,end_th2))
        th3 = Process(target=data,args=(start_th3,end_th3))

        th4 = Process(target=data,args=(start_th4,end_th4))
        th5 = Process(target=data,args=(start_th5,end_th5))
        th6 = Process(target=data,args=(start_th6,end_th6))

        th1.start()
        th2.start()
        th3.start()

        th4.start()
        th5.start()
        th6.start()

        th1.join()
        th2.join()
        th3.join()

        th4.join()
        th5.join()
        th6.join()
    print(time.time()-start_time)
local_tz = pendulum.timezone('Asia/Seoul')
today = datetime.today()

default_dag_args = {
    "owner": "airflow",
    "start_date": datetime(today.year, today.month, today.day, tzinfo=local_tz) - timedelta(hours=25)
}
dag = DAG(
    dag_id='daily_familt_DB'
    , default_args=default_dag_args
    , schedule_interval='0 9 * * *'
    # , schedule_interval=timedelta(1)
)
task1 = PythonOperator(
    task_id='New_familt_DB'
    , python_callable=New_familt_DB
    , dag=dag
)