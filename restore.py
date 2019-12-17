# 本程序可还原单个备份到NAS卷上的MySQL数据库或所有备份到NAS卷上的MySQL库
# author: alenlong
# create_time: 2019/12/2
# version: 1.0

import time
import os
import sys
import socket
import smtplib
import psycopg2
from email.message import EmailMessage

print('''
#######################################################################
1. 本脚本支持还原备份在nas卷上的单个数据库以及全部数据库；
2. 暂不支持standard类型的mysql备份；
3. 默认还原结束日期为系统当前时间，默认还原开始时间为系统当前时间7天前；
#######################################################################
''')

# --------------------email information--------------------
sender = 'root@test.com'
receivers = ['001@test.com', 'l002@test.com', 'l007@test.com',
             '006@test.com', 'ya001@test.com']
# receivers = ['longyy002@test.com']
message = EmailMessage()
message['To'] = ['张三<001@test.com>', '王五<002@test.com>',
                 '李四<007@test.com>', '王就<006@test.com>', '杨二<0012@test.com>']


# --------------------PRD qhinfo -------------------------------
PRD_SERVER = 'pgdb.test.com'
PRD_PORT = '5122'  # 端口信息
PRD_USER = 'testdbuser'
PRD_PASSWORD = 'testdbuserpassword'
PRD_DATABASE = 'dbname'


def menu():
    print('''欢迎使用MySQL数据库还原功能，请选择要还原的选项，
    >>> 1. 输入数字1，表示还原单个MySQL数据库；
    >>> 2. 输入数字2，表示还原NAS卷上的所有MySQL数据库；
    ''')
    choice = input('请输入您的选项：')
    return choice


def single_paras():
    # 单个数据库还原交互输入信息
    restore_dbname = input('请输入要还原的mysql数据库名字：')
    restore_start_timestamp = input('请输入还原开始日期，默认时间为{},直接按enter键为默认时间'.format(time.strftime(
        "%m/%d/%Y %H:%M:%S", time.localtime(time.time() - 7 * 24 * 60 * 60))))
    if restore_start_timestamp == '':
        restore_start_timestamp = time.strftime("%m/%d/%Y %H:%M:%S",
                                                time.localtime(time.time() - 7 * 24 * 60 * 60))  # 7天前
    restore_end_timestamp = input('请输入还原结束日期，默认时间为{},直接按enter键为默认时间'.format(time.strftime(
        "%m/%d/%Y %H:%M:%S", time.localtime())))
    if restore_end_timestamp == '':
        restore_end_timestamp = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())  # 开始时间为今天
    return restore_dbname, restore_start_timestamp, restore_end_timestamp


def all_paras():
    print('即将逐步还原NAS卷上所有的MySQL数据库')
    restore_start_timestamp = input('请输入还原开始日期，默认时间为{},直接按enter键为默认时间'.format(time.strftime(
        "%m/%d/%Y %H:%M:%S", time.localtime(time.time() - 7 * 24 * 60 * 60))))
    if restore_start_timestamp == '':
        restore_start_timestamp = time.strftime("%m/%d/%Y %H:%M:%S",
                                                time.localtime(time.time() - 7 * 24 * 60 * 60))  # 7天前
    restore_end_timestamp = input('请输入还原结束日期，默认时间为{},直接按enter键为默认时间'.format(time.strftime(
        "%m/%d/%Y %H:%M:%S", time.localtime())))
    if restore_end_timestamp == '':
        restore_end_timestamp = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())  # 开始时间为今天
    return restore_start_timestamp, restore_end_timestamp


def get_nbu_info():
    get_backup_list_cmd = '/usr/openv/netbackup/bin/bplist -k nbu_policy_name \
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


class RestoreSingleMysqlFromNbu(object):
    def __init__(self):
        self.restore_single_db_cmd = "/usr/openv/netbackup/bin/bprestore -C Netapp8020-01-A -D Netapp8020-01-A \
                                    -p nbu_backup_policy_name -t 19 -s {} -e {} \
                                    -L /usr/openv/netbackup/logs/user_ops/mysql_restore_ndmp_wg.log \
                                    -S bk-test.test.com -w -R /usr/openv/netbackup/ndmpchangepath \
                                    /vol/mysql_backup_1/{}".format(restore_start_timestamp, restore_end_timestamp,
                                                                   restore_dbname)

        self.create_dir_cmd = 'mkdir -p /qhapp/restore/mysql2/{0}/db/{{data,log,tmp,var}}\
                                        && chown -R mysql:mysql /qhapp/restore/mysql2/{0}/ \
                                        && chmod -R 755 /qhapp/restore/mysql2/{0}/'.format(restore_dbname)

        self.change_permission = 'chown -R mysql:mysql /qhapp/restore/mysql2/{0} \
                                        && chmod -R 755 /qhapp/restore/mysql2/{0}'.format(restore_dbname)

        self.copy_configuration_files_cmd = "cp -f /qhapp/restore/mysql2/my.cnf.example\
                                            /qhapp/restore/mysql2/{0}/db/my.cnf && \
                                            sed -i 's/my3411/{0}/g' \
                                            /qhapp/restore/mysql2/{0}/db/my.cnf".format(restore_dbname)

        self.shutdown_cmd = 'runuser -l mysql -c \"/qhapp/restore/mysql2/soft/base/bin/mysqladmin --login-path=root\
                            --socket=/qhapp/restore/mysql2/{}/db/var/mysql.sock shutdown\"'.format(restore_dbname)

        self.shutdown_gjs_cmd = 'runuser -l mysql -c \"/qhapp/restore/mysql2/soft/base/bin/mysqladmin \
                                --login-path=gjsroot\
                                --socket=/qhapp/restore/mysql2/{}/db/var/mysql.sock shutdown\"'.format(restore_dbname)

        self.startup_cmd = '/qhapp/restore/mysql2/soft/base/bin/mysqld_safe  \
                            --defaults-file=/qhapp/restore/mysql2/{}/db/my.cnf  \
                            --ledir=/qhapp/restore/mysql2/soft/base/bin &'.format(restore_dbname)

        self.database_path = '/qhapp/restore/mysql2/{}/'.format(restore_dbname)

        self.logfile = '/qhapp/restore/mysql2/{}/restore.log'.format(restore_dbname)

        self.restore_cmd = '/qhapp/restore/mysql2/soft/base/bin/mysqlbackup \
                            --defaults-file=/qhapp/restore/mysql2/{0}/db/my.cnf  \
                            -uroot --backup-dir=/nfsc/nbu_ndmp_restore_test_pool1/mysql_restore/{0}/full \
                            --datadir=/qhapp/restore/mysql2/{0}/db/data \
                            --uncompress copy-back-and-apply-log --force >> {1} 2>&1'.format(restore_dbname,
                                                                                             self.logfile)

    def check_dbname_exist(self):
        print('正在检测数据库名是否备份在NAS卷中')
        if restore_dbname not in content_dbname:
            print('输入的数据库不在NDMP备份中，请检查数据库名或规范数据库备份方式')
            sys.exit()

    def restore_ndmp_singledb(self):
        print('检测通过，开始从NBU还原数据库{}...等待还原任务完成'.format(restore_dbname))
        self.command_result = os.system(self.restore_single_db_cmd)
        if self.command_result != 0:
            print('还原作业异常，请检查NBU CONSOLE任务，脚本退出')
            message = EmailMessage()
            message['To'] = ['张三<001@test.com>', '王五<002@test.com>',
                 '李四<007@test.com>', '王就<006@test.com>', '杨二<0012@test.com>']
            message.set_content('数据库 {} 还原作业异常，退出任务'.format(restore_dbname))
            message['Subject'] = 'MySQL数据库还原---[{}]---[failed]'.format(restore_dbname)
            sendmail(message)
            return False
        else:
            print(r'数据库{}还原成功，路径为/nfsc/nbu_ndmp_restore_test_pool1/mysql_restore/{}'\
                  .format(restore_dbname, restore_dbname))
            os.system(r'chown -R mysql:mysql /nfsc/nbu_ndmp_restore_test_pool1/mysql_restore/{} && chmod -R 755\
                    /nfsc/nbu_ndmp_restore_test_pool1/mysql_restore/{}'.format(restore_dbname, restore_dbname))
            return 'Success'

    def initialize_instance_folder(self):
        create_folder = os.system(self.create_dir_cmd)
        copy_configuration_files = os.system(self.copy_configuration_files_cmd)
        if create_folder == 0 and copy_configuration_files == 0:
            print('初始化MySQL实例{}文件夹成功'.format(restore_dbname))
        else:
            print('MySQL实例{}文件夹初始化失败，请检查目录是否存在、权限是否正确、或者！'.format(restore_dbname))

    def check_port_status(self, port=3411, ip='127.0.0.1'):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((ip, int(port)))
            s.shutdown(2)
            # 利用shutdown()函数使socket双向数据传输变为单向数据传输。shutdown()需要一个单独的参数，
            # 该参数表示了如何关闭socket。具体为：0表示禁止将来读；1表示禁止将来写；2表示禁止将来读和写。
            # print('{} is open'.format(port))
            return True
        except:
            # print('{} is down'.format(port))
            return False

    def shutdown_mysql_instance(self):
        port_status = self.check_port_status()
        if port_status is True:
            print('开始停止数据库')
            stop_command = '''ps -ef|grep $(netstat -ntpl|grep :3411|awk -F":" '{print $7}'|awk -F" " '{print $3}'\
                            |awk -F"/" '{print $1}')|grep mysqld| awk -F"=" '{{print $2}}'|awk -F"/" '{{print $5}}' '''
            exist_database_name = os.popen(stop_command).read().strip('\n')
            if "gj" in exist_database_name:
                shutdown_command = 'runuser -l mysql -c \"/qhapp/restore/mysql2/soft/base/bin/mysqladmin \
                                                --login-path=gjsroot\
                                                --socket=/qhapp/restore/mysql2/{}/db/var/mysql.sock shutdown\"'\
                                                .format(exist_database_name)
                shutdown_mysql_instance = os.system(shutdown_command)
            else:
                shutdown_command = 'runuser -l mysql -c \"/qhapp/restore/mysql2/soft/base/bin/mysqladmin \
                                    --login-path=root --socket=/qhapp/restore/mysql2/{}/db/var/mysql.sock shutdown\"'\
                                    .format(exist_database_name)
                shutdown_mysql_instance = os.system(shutdown_command)
            time.sleep(120)
            port_status2 = self.check_port_status()
            if shutdown_mysql_instance == 0 and port_status2 is False:
                print('数据库{}停止成功'.format(exist_database_name))
            else:
                print('数据库{}在120秒内停止失败，请检查数据库状态及日志'.format(exist_database_name))
                message = EmailMessage()
                message['To'] = ['张三<001@test.com>', '王五<002@test.com>',
                 '李四<007@test.com>', '王就<006@test.com>', '杨二<0012@test.com>']
                message.set_content('数据库 {} 无法停止，请手动停止，停止后请继续任务'.format(exist_database_name))
                message['Subject'] = 'MySQL数据库还原---[{}]---[异常]'.format(restore_dbname)
                sendmail(message)
                input('手动处理完毕后，请输入任意字符继续还原程序')
        else:
            print('数据库端口3411未被占用，无需停止。')

    def startup_mysql_instance(self):
        print('开始启动数据库')
        port_status = self.check_port_status()
        if port_status is False:
            os.system(self.startup_cmd)
            time.sleep(150)
        port_status = self.check_port_status()
        if port_status is True:
            print('数据库{}启动成功'.format(restore_dbname))
        else:
            print('端口3411在150秒内未启动，数据库{}启动失败，请检查日志'.format(restore_dbname))
            message = EmailMessage()
            message['To'] = ['张三<001@test.com>', '王五<002@test.com>',
                 '李四<007@test.com>', '王就<006@test.com>', '杨二<0012@test.com>']
            message.set_content('数据库 {} 在150秒内启动失败失败，请检查日志'.format(restore_dbname))
            message['Subject'] = 'MySQL数据库还原---[{}]---[异常]'.format(restore_dbname)
            sendmail(message)
            return False

    def restore_mysql_instance(self):
        print('开始还原数据库实例')
        restore_mysql_instance = os.system(self.restore_cmd)
        os.system(self.change_permission)
        if restore_mysql_instance == 0:
            print('数据库{}实例还原成功'.format(restore_dbname))
            return 'Success'
        else:
            print('数据库{}实例还原失败，请查看日志{}'.format(restore_dbname, self.logfile))
            message = EmailMessage()
            message['To'] = ['张三<001@test.com>', '王五<002@test.com>',
                 '李四<007@test.com>', '王就<006@test.com>', '杨二<0012@test.com>']
            message.set_content('数据库 {} 还原实例异常，请检查'.format(restore_dbname))
            message['Subject'] = 'MySQL数据库还原---[{}]---[异常]'.format(restore_dbname)
            sendmail(message)
            return 'False'

    def sql_check(self):
        if "gj" not in restore_dbname:
            cmd = "runuser -l mysql -c \"/qhapp/restore/mysql2/soft/base/bin/mysql --login-path=root \
                    --socket=/qhapp/restore/mysql2/{}/db/var/mysql.sock -e 'select now();'\"".format(restore_dbname)
        else:
            cmd = "runuser -l mysql -c \"/qhapp/restore/mysql2/soft/base/bin/mysql --login-path=gjsroot \
                    --socket=/qhapp/restore/mysql2/{}/db/var/mysql.sock -e 'select now();'\"".format(restore_dbname)
        result = os.popen(cmd)
        tmplist = result.readlines()
        if len(tmplist) != 0:
            print('数据库{}执行查询时间语句正常。'.format(restore_dbname))
            return tmplist
        else:
            print('数据库{}查询SQL语句异常，请复核。'.format(restore_dbname))
            message = EmailMessage()
            message['To'] = ['张三<001@test.com>', '王五<002@test.com>',
                 '李四<007@test.com>', '王就<006@test.com>', '杨二<0012@test.com>']
            message.set_content('数据库 {} 执行SQL语句异常，请检查'.format(restore_dbname))
            message['Subject'] = 'MySQL数据库还原---[{}]---[异常]'.format(restore_dbname)
            sendmail(message)
            return 'False'

    def get_database_size(self):
        size = os.popen('du -sh {}'.format(self.database_path))
        database_size = size.read().split('\t')[0]
        return database_size


def notification(restore_dbname_value, restore_mysql_instance_value, get_database_size_value, sql_check_value):
    print('开始发送邮件')
    message = EmailMessage()
    message.set_content('''
    数据库 {} 还原结果如下：
    MySQL实例还原状态：{}
    大小：{}
    执行查询SQL结果：{}
    '''.format(restore_dbname_value, restore_mysql_instance_value, get_database_size_value,
               sql_check_value), 'plain', 'utf-8')
    message['To'] = ['张三<001@test.com>', '王五<002@test.com>',
                 '李四<007@test.com>', '王就<006@test.com>', '杨二<0012@test.com>']
    message['Subject'] = 'MySQL数据库还原---[{}]---[{}]'.format(restore_dbname_value, restore_mysql_instance_value)
    sendmail(message)


def sendmail(message_value):
    try:
        server = smtplib.SMTP()
        server.connect('127.0.0.1', 25)
        server.sendmail(sender, receivers, message_value.as_string())
        server.quit()
    except smtplib.SMTPException:
        print('Error: 邮件无法发送')


# 创建pg数据库连接
def connect_db():
    try:
        conn = psycopg2.connect(database=PRD_DATABASE, user=PRD_USER, password=PRD_PASSWORD, host=PRD_SERVER,
                                port=PRD_PORT)
        return conn
    except psycopg2.Error:
        print('数据库发生错误，请检查！')


# 关闭数据库连接并提交sql语句
def close_db_connection(conn):
    conn.commit()
    print('''数据库还原信息写入qhinfo成功
    ---------------------------------''')
    conn.close()


if __name__ == '__main__':
    choice = menu()
    if choice == '1':
        single_parametres = single_paras()
        restore_dbname = single_parametres[0]
        restore_start_timestamp = single_parametres[1]
        restore_end_timestamp = single_parametres[2]
        content_dbname = get_nbu_info()
        a = RestoreSingleMysqlFromNbu()
        a.check_dbname_exist()
        restore_result = a.restore_ndmp_singledb()
        if restore_result is False:
            sys.exit()
        a.initialize_instance_folder()
        a.shutdown_mysql_instance()
        restore_mysql_instance = a.restore_mysql_instance()
        if restore_mysql_instance is False:
            sys.exit()
        get_database_size = a.get_database_size()
        start_mysql_instance = a.startup_mysql_instance()
        if start_mysql_instance is False:
            sys.exit()
        sql_check = a.sql_check()
        notification(restore_dbname, restore_mysql_instance, get_database_size, sql_check)
        conn = connect_db()
        cur = conn.cursor()
        cur.execute("""insert into backupdata.mysql_restore_history (dbname, size, start_time, status) 
                        values (%s, %s, %s, %s);""", (restore_dbname, get_database_size,
                        time.strftime("%m/%d/%Y %H:%M:%S", time.localtime()), restore_mysql_instance))
        close_db_connection(conn)
    elif choice == '2':
        restore_time = all_paras()
        restore_start_timestamp = restore_time[0]
        restore_end_timestamp = restore_time[1]
        content_dbname = get_nbu_info()
        i = 1
        for restore_dbname in content_dbname:
            message = EmailMessage()
            message['To'] = ['张三<001@test.com>', '王五<002@test.com>',
                 '李四<007@test.com>', '王就<006@test.com>', '杨二<0012@test.com>']
            print('正在恢复第{}个数据库，共{}个数据库'.format(i, len(content_dbname)))
            a = RestoreSingleMysqlFromNbu()
            restore_result = a.restore_ndmp_singledb()
            if restore_result is False:
                i += 1
                continue
            a.initialize_instance_folder()
            a.shutdown_mysql_instance()
            restore_mysql_instance = a.restore_mysql_instance()
            get_database_size = a.get_database_size()
            a.startup_mysql_instance()
            sql_check = a.sql_check()
            notification(restore_dbname, restore_mysql_instance, get_database_size, sql_check)
            conn = connect_db()
            cur = conn.cursor()
            cur.execute("""insert into backupdata.mysql_restore_history (dbname, size, start_time, status) 
                            values (%s, %s, %s, %s);""", (restore_dbname, get_database_size,
                                                          time.strftime("%m/%d/%Y %H:%M:%S", time.localtime()),
                                                          restore_mysql_instance))
            close_db_connection(conn)
            i += 1
    else:
        print('请输入1或2，请重新运行程序')
