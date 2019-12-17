import os
import hmac
import hashlib
import base64
import json
import time
import urllib.request
import logging
import psycopg2
from email.message import EmailMessage
import smtplib

PRD_SERVER = 'dbprd.db.test.com'
PRD_PORT = '5433'
PRD_USER = 'dbusername'
PRD_PASSWORD = 'dbpassword'
PRD_DATABASE = 'dbname'

CMDB_PRD_URL = r"http://cmdb.test.com/rest_api/query/entity_data"
CMDB_PRD_KEY = r'djskaljdklasrpl6w'
CMDB_PRD_CODE = 'nbu'

sender = 'root@sz130001.test.com'
receivers = ['001@test.com', '002@test.com', '007@test.com',
             '006@test.com', '0012@test.com']
# receivers = ['002@test.com']
message = EmailMessage()
message['To'] = ['张三<001@test.com>', '李四<002@test.com>',
                 '王五<007@test.com>', '赵六<006@test.com>', '杨七<0012@test.com>']

restore_start_timestamp = time.strftime("%m/%d/%Y %H:%M:%S",
                                        time.localtime(time.time() - 7 * 24 * 60 * 60))  # 7天前
restore_end_timestamp = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())  # 开始时间为今天


class CmdbGetData:
    # def __init__(self, api_url=CMDB_SERVER_API_URL, app_code=CMDB_API_APP_CODE, key=CMDB_API_KEY):
    def __init__(self, api_url=CMDB_PRD_URL, app_code=CMDB_PRD_CODE, key=CMDB_PRD_KEY):
        self.default_page_size = 200
        self.default_page = 1
        self.logger = logging.getLogger("cmdb_api")
        self.app_code = app_code
        self.secret_key = key
        self.api_url = api_url

    # 用于使用加密key加密字符的模块
    # @ 参数：data：需要加密的字符串  secret_key：加密key
    def __make_signature(self, data):
        h2 = hmac.new(bytes(self.secret_key, 'utf-8'), digestmod=hashlib.sha1)
        h2.update(data)
        return base64.b64encode(h2.digest())

    # 用于生成token用于请CMDB数据
    # @ 参数：timeout：超时时间，单位：分
    def __generate_rest_api_token(self, timeout=1):
        now_time = int(round(time.time() * 1000, 0))
        expire_time = now_time + timeout * 60 * 1000
        data = {"app_code": self.app_code, "expire_time": expire_time}
        tmp = json.JSONEncoder().encode(data)
        data = base64.b64encode(tmp.encode('utf-8'))
        data = data + self.__make_signature(data)
        return data

    # 请求CMDB API接口获取数据
    # @ 参数：
    #       url：需请求内容URL  token：通过程序生成的token(不传入则自动新生成)
    def __from_api_get_data(self, url, token=None):
        try:
            if not token:
                token = self.__generate_rest_api_token().decode('utf-8')
            headers = {
                'AUTHORIZATION': "rest_api_token " + token,
                'Content-Type': 'application/json',
            }
            req = urllib.request.Request(url=url, headers=headers)
            response = urllib.request.urlopen(req)
            html = response.read().decode('utf-8')
            result = json.loads(html)
            self.logger.debug("result data:%s" % result)
            return result
        except Exception as e:
            self.logger.error("get cmdb data error!!!info:%s" % e)
            return {}

    def get_cmdb_data(self, entity='t_my_env_list_cmdb', page=1, page_size=200):
        if entity:
            act_url = "?entity_code=%s&page=%d&page_size=%s&env_type=PRD" % (entity, page, page_size)
            url = self.api_url + act_url
            out = self.__from_api_get_data(url)
            return out
        return False


# 创建pg数据库连接
def connect_db():
    try:
        conn2 = psycopg2.connect(database=PRD_DATABASE, user=PRD_USER, password=PRD_PASSWORD, host=PRD_SERVER,
                                 port=PRD_PORT)
        return conn2
    except psycopg2.Error:
        print('数据库发生错误，请检查！')


def get_nbu_info():
    get_backup_list_cmd = '/usr/openv/netbackup/bin/bplist -k nbu_backup_policy_name \
                                -C Netapp8020-01-A -R 1 -t 19  -s {} -e {} \
                                -l /vol/mysql_backup_1/'.format(restore_start_timestamp, restore_end_timestamp)
    # 获取NAS卷上的数据库名字信息，并写入列表content_dbname中。
    directory_name = os.popen(get_backup_list_cmd)  # 通过bplist获取到备份目录信息
    content = directory_name.read().split('\n')
    content_dbname = []  # 新建空列表，用于存放备份目录中的数据库名字
    for i in content:
        temp = i.split('/')
        if len(temp) == 5:
            dbname = temp[3]
            content_dbname.append(dbname)
    content_dbname = list(set(content_dbname))
    invalid_dbname = ['log', 'binlog', 'incr', 'full']  # NBU返回的目录可能存在无效数据库名，删除掉
    for i in invalid_dbname:
        if i in content_dbname:
            content_dbname.remove(i)
    return content_dbname


def sendmail(message_value):
    try:
        server = smtplib.SMTP()
        server.connect('127.0.0.1', 25)
        server.sendmail(sender, receivers, message_value.as_string())
        server.quit()
    except smtplib.SMTPException:
        print('Error: 邮件无法发送')


if __name__ == '__main__':
    a = CmdbGetData(api_url=CMDB_PRD_URL, key=CMDB_PRD_KEY, app_code=CMDB_PRD_CODE)
    mysql_dbname = a.get_cmdb_data()
    dbname_from_cmdb = []
    for item in mysql_dbname['results']:
        dbname_from_cmdb.append(item['dble'])
    # print('CMDB中MySQL清单如下，数量为{}个'.format(len(dbname_from_cmdb)))
    # print(dbname_from_cmdb)
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""select distinct dbname from backupdata.mysql_restore_history 
                    where status='Success' and cast(start_time as timestamp)  >
                      current_timestamp - interval '90 d';""")
    sql_result = cur.fetchall()
    conn.close()
    sql_result_dbname = []
    for row in sql_result:
        sql_result_dbname.append(row[0])
    # print('pgdb中过去90天已还原成功的MySQL数据库清单如下，共{}个'.format(len(sql_result_dbname)))
    # print(sql_result_dbname)
    cmdb_db_difference = list(set(dbname_from_cmdb).difference(set(sql_result_dbname)))
    # print('CMDB中以下数据库过去90天尚未还原过，共{}个'.format(len(cmdb_db_difference)))
    # print(cmdb_db_difference)
    nbu_nas_dbname = get_nbu_info()
    # print('NBU中MySQL清单如下，数量为{}'.format(len(nbu_nas_dbname)))
    # print(nbu_nas_dbname)
    nas_db_difference = list(set(nbu_nas_dbname).difference(set(sql_result_dbname)))
    # print('NAS中以下数据库过去90天尚未还原过，共{}个'.format(len(nas_db_difference)))
    # print(nas_db_difference)
    nas_db_difference2 = list(set(dbname_from_cmdb).difference(set(nbu_nas_dbname)))
    # print('CMDB中以下数据库没有备份在NAS卷中，共{}个'.format(len(nas_db_difference2)))
    # print(nas_db_difference2)
    nas_cmdb_difference = list(set(nbu_nas_dbname).difference(set(dbname_from_cmdb)))
    message.set_content('''
    CMDB中MySQL数据库清单如下，数量为【{}】个，清单如下：
    {}
    
    ===================================================
    NAS卷中MySQL数据库清单如下，数量为【{}】个，清单如下：
    {}
    
    ===================================================
    CMDB中以下MySQL数据库没有备份在NAS卷中，数量为【{}】个，清单如下，请忽略xxx数据库：
    {}
    
    ===================================================
    CMDB中以下MySQL数据库过去90天尚未还原成功过，数量为【{}】个，清单如下，请忽略xxx数据库：
    {}
    
    ===================================================
    NAS卷中以下数据库不在CMDB中，数量为【{}】个，清单如下：
    {}
    
    ===================================================
    NAS卷中以下数据库过去90天尚未还原过，数量为【{}】个，清单如下：
    {}
    '''.format(len(dbname_from_cmdb), dbname_from_cmdb, len(nbu_nas_dbname), nbu_nas_dbname, len(nas_db_difference2),
               nas_db_difference2, len(cmdb_db_difference), cmdb_db_difference, len(nas_cmdb_difference),
               nas_cmdb_difference, len(nas_db_difference), nas_db_difference))
    message['Subject'] = 'MySQL数据库季度还原报告'
    sendmail(message)
