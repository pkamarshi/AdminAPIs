import os
import math
import psutil
import json
import web
import netifaces as ni
import socket
import platform
import subprocess
import datetime
import time
from AdminAPIs import getUsers, getJobs, getTasks, getUserInfo
from util import create_client
from Crypto.Hash import HMAC

urls = ('/server_info', 'ServerInfo',
        '/stages_logs', 'TaskInfo',
        '/logs', 'Logs',
        '/logfile', 'LogFile',
        '/jobs_info', 'Jobs',
        '/num_jobs_users', 'NumJobsUsers',
        '/user_info', 'UserInfo',
        '/server_jobs', 'ServerJobs')

app = web.application(urls, globals())

client = create_client('vaultedgewebtier')

def authcheck(msg):

    hour_words = {0: 'zero',
                  1: 'one',
                  2: 'two',
                  3: 'three',
                  4: 'four',
                  5: 'five',
                  6: 'six',
                  7: 'seven',
                  8: 'eight',
                  9: 'nine',
                  10: 'ten',
                  11: 'eleven',
                  12: 'twelve',
                  13: 'thirteen',
                  14: 'fourteen',
                  15: 'fifteen',
                  16: 'sixteen',
                  17: 'seventeen',
                  18: 'eighteen',
                  19: 'nineteen',
                  20: 'twenty',
                  21: 'twenty-one',
                  22: 'twenty-two',
                  23: 'twenty-three',
                  24: 'twenty-four'}

    dt = datetime.datetime.now()

    tok = '670399226500'

    hash = HMAC.new(hour_words[dt.hour]+str(dt.day)+'-'+str(dt.month)+'-'+str(dt.year))
    hash.update(tok)

    enc = str(hash.hexdigest())

    if enc == msg:
        return True

    else:
        return False


class ServerInfo:
    def GET(self):

        i = web.input(check = 'null', start = '', end = '')

        if i.check == 'null':
            return json.dumps({"Error": {"Code": 1, "Message": "Check missing"}})

        if not authcheck(i.check):
            return json.dumps({"Error": {"Code": 1, "Message": "Authentication failed"}})

        start = 0L
        end = 0

        if i.start:
            start = int(i.start)

        if i.end:
            end = float(i.end)

        if (end and end != 0) and start > end:
            return "Invalid time range"

        opdic = {}
        opdic['server_id'] = ni.ifaddresses('eth0')[2][0]['addr']
        opdic['server_name'] = socket.gethostname()

        p = psutil.virtual_memory()
        opdic['memory_use'] = (p.used / float(p.total)) * 100

        mpstat = subprocess.check_output('mpstat').split('\n')
        mem = mpstat[3].split()
        opdic['cpu_usage'] = float(mem[3]) + float(mem[5])

        df = subprocess.check_output(['df', '/']).split('\n')
        opdic['disk_use'] = df[1].split(' ')[-2]

        opdic['num_cpu'] = psutil.cpu_count()
        opdic['os'] = platform.system()

        opdic['jobs'] = 0

        return json.dumps(opdic, indent = 4)


class ServerJobs:
    def GET(self):

        i = web.input(check = 'null', start = '', end = '', server = '')

        if i.check == 'null':
            return json.dumps({"Error": {"Code": 1, "Message": "Check missing"}})

        if not authcheck(i.check):
            return json.dumps({"Error": {"Code": 1, "Message": "Authentication failed"}})

        start = 0L
        end = 0

        if i.start:
            start = int(i.start)

        if i.end:
            end = float(i.end)

        if (end and end != 0) and start > end:
            return "Invalid time range"

        tasks = getTasks(client, "", 0, "", "", 0, int(start), 0)

        if not tasks:
            return json.dumps({"Error": {"Code": 2, "Message": "getTasks returned None"}}, indent=4)

        job_lis = []

        for task in tasks:
            task = dict(task)

            """
            if end > 0 and float(task['endTime']) != 0 and float(task['endTime']) <= float(end) and task['server'] == i.server:
                job_lis.append(task['jobId'])

            elif float(task['endTime']) != 0 and float(task['endTime']) <= float(end) and task['server'] == i.server:
            """
            if float(task['endTime']) <= float(end) and task['server'] == i.server:
                job_lis.append(task['jobId'])


        return len(set(job_lis))


class Logs:

    def pad(self, dt_ob):
        dt_ob = str(dt_ob)

        if len(dt_ob) < 2:
            return '0' + dt_ob
        return dt_ob

    def Logs(self, pdt_s, pdt_e, pid):
        print '------------------------START---------------------------------------'

        if pdt_s != 'null' and pdt_e != 'null':
            dt_s = datetime.datetime.utcfromtimestamp(float(pdt_s))
            dt_e = datetime.datetime.utcfromtimestamp(float(pdt_e))

            print dt_s, dt_e

            i_dt = dt_s

            dir_lis = []
            path = '/home/Sajeev/classify_logs/'

            while i_dt <= dt_e:
                p = path + self.pad(i_dt.month) + '_' + self.pad(i_dt.day) + '_' + self.pad(i_dt.year) + '/'

                if os.path.isdir(p):
                    dir_lis.append(p)

                i_dt += datetime.timedelta(days=1)

            print dir_lis

            flg = 0

            if dir_lis:

                for di in dir_lis:

                    files_dt = [datetime.datetime.strptime(str(f),'%Y%m%d%H%M%S') for f in os.listdir(di) if os.path.isfile(os.path.join(di,f)) and '.' not in str(f)]

                    flg = 0

                    file_path = ''
                    print 'in dir loop'


                    for f in files_dt:
                        if dt_s <= f and dt_e >= f:
                            p = di + f.strftime('%Y%m%d%H%M%S')
                            print p,

                            f = open(p, 'r')
                            lines = f.read()
                            answer = lines.find(str('pid :: ' + str(pid)))

                            print answer, os.path.getsize(p), pid

                            if answer != -1:
                                flg = 1
                                #file_path = p + '\n\n' + lines
                                file_path = p
                                break
                        #else:
                            #print dt_s, dt_e, f

                    if flg == 1:
                        print '------------------------END---------------------------------' + '\n\n'
                        return 'the file path: ' + file_path

                print '------------------------END---------------------------------' + '\n\n'

                if flg == 0:
                    return 'log file does not exist'

            else:
                return 'log directory does not exist'

        else:
            return 'Date time or process id not entered'


        def GET(self):
            i = web.input(check = 'null',
                          start = 'null',
                          end = 'null',
                          pid = 'null')

            if i.check == 'null':
                return json.dumps({"Error": {"Code": 1, "Message": "Check missing"}})

            if not authcheck(i.check):
                return json.dumps({"Error": {"Code": 1, "Message": "Authentication failed"}})

            if i.start == 'null' or i.end == 'null':
                return json.dumps({"Error": {"Code": 3, "Message": "Time range not given properly"}})

            if i.pid == 'null':
                return json.dumps({"Error": {"Code": 3, "Message": "PID not given"}})

            return self.Logs(i.start, i.end, i.pid)


class TaskInfo:

    def Stages(self, start, end, job_id):

        dt_e = float(end)
        dt_s = float(start)

        tasks = getTasks(client, "", 0, "", "", 0, int(dt_s), 0)

        if not tasks:
            return json.dumps({"Error": {"Code": 2, "Message": "getTasks returned None"}}, indent=4)

        stages_list = []

        for task in tasks:
            task = dict(task)

            if dt_e > 0 and task['endTime'] != 0 and task['endTime'] < dt_e and str(task['jobId']) == job_id:
                dic = {}

                dic['isCurrent'] = 'false'
                dic['stage'] = task['taskName']
                dic['startTime'] = task['startTime']
                dic['endTime'] = task['endTime']
                dic['server'] = task['server']
                dic['cpu'] = '2bg'
                dic['memory'] = '2bg'
                dic['System'] = {'cpu' : '2bg', 'memory' : '2bg'}

                dic['Logs'] = task['pid']

                stages_list.append(dic)

            elif dt_e == 0 and str(task['jobId']) == job_id:
                dic = {}

                if task['endTime'] == 0:
                    dic['isCurrent'] = 'true'
                elif task['endTime'] > 0:
                    dic['isCurrent'] = 'false'

                dic['stage'] = task['taskName']
                dic['startTime'] = task['startTime']
                dic['endTime'] = task['endTime']
                dic['server'] = task['server']
                dic['cpu'] = '2bg'
                dic['memory'] = '2bg'
                dic['System'] = {'cpu' : '2bg', 'memory' : '2bg'}

                dic['Logs'] = task['pid']

                stages_list.append(dic)

        return json.dumps({'stages': stages_list}, indent = 4)


    def GET(self):
        i = web.input(check = 'null',
                      start = 'null',
                      end = 'null',
                      job_id = 'null')

        if i.check == 'null':
            return json.dumps({"Error": {"Code": 1, "Message": "Check missing"}})

        if not authcheck(i.check):
            return json.dumps({"Error": {"Code": 1, "Message": "Authentication failed"}})

        if i.job_id == 'null':
            return 'job_id not given'

        if i.start == 'null' and i.start == 'null':
            return self.Stages(0, 0, str(i.job_id))

        if i.start == 'null' or i.end == 'null':
            return "Time range not mentioned"

        if i.end != '0' and int(i.start) > float(i.end):
            return "Invalid time range"

        return self.Stages(i.start, i.end, str(i.job_id))

class Jobs:

    def jobs_get(self, start, end, updateType, emailId):

        print start, end

        jobs = getJobs(client, "", "", "", "", int(start), 0)

        if not jobs:
            return json.dumps({"Error": {"Code": 2, "Message": "getJobs returned None"}}, indent=4)

        print len(jobs)

        jobs_list = []

        for job in jobs:
            job_dic = dict(job)
            job_dic['jobId'] = job.key.id

	    if updateType and job_dic['updateType'] != updateType:
	        continue

            if emailId and job_dic['source'] != emailId:
                continue

            if end > 0 and job_dic['endTime'] != 0 and job_dic['endTime'] < end:

                dic = {}

                dic['syncType'] = job_dic['updateType']
                dic['jobId'] = job_dic['jobId']
                dic['sourceType'] = job_dic['sourceType']
                dic['emailId'] = job_dic['source']
                dic['startTime'] = job_dic['startTime']
                dic['endTime'] = job_dic['endTime']

                jobs_list.append(dic)

            if end == 0:

                dic = {}

                dic['syncType'] = job_dic['updateType']
                dic['jobId'] = job_dic['jobId']
                dic['sourceType'] = job_dic['sourceType']
                dic['emailId'] = job_dic['source']
                dic['startTime'] = job_dic['startTime']
                dic['endTime'] = job_dic['endTime']

                jobs_list.append(dic)

        print len(jobs_list)

        return json.dumps({"Jobs": jobs_list}, indent = 4)

    def GET(self):
        i = web.input(check = 'null',
                      start = 'null',
                      end = 'null',
                      jtype = "",
                      emailId = "")

        if i.check == 'null':
            return json.dumps({"Error": {"Code": 1, "Message": "Check missing"}})

        if not authcheck(i.check):
            return json.dumps({"Error": {"Code": 1, "Message": "Authentication failed"}})

        if i.start == 'null' and i.end == 'null':
            return self.jobs_get(0, 0, i.jtype, i.emailId)

        if i.start == 'null' or i.end == 'null':
            return "Time range not given properly"

        start = float(i.start)
        end = float(i.end)

        if end != 0 and start > end:
            return "Invalid time range"

        return self.jobs_get(start, end, i.jtype, i.emailId)

class NumJobsUsers:
    def GET(self):
        i = web.input(check = 'null',
                      start = 'null',
                      end = 'null')

        if i.check == 'null':
            return json.dumps({"Error": {"Code": 1, "Message": "Check missing"}})

        if not authcheck(i.check):
            return json.dumps({"Error": {"Code": 1, "Message": "Authentication failed"}})

        if i.start == 'null' and i.end == 'null':
            jobs = getJobs(client, "", "", "", "", 0L, 0)

            if not jobs:
                return json.dumps({"Error": {"Code": 2, "Message": "getJobs returned None"}}, indent=4)

            user_lis = []
            num_jobs = 0

            for job in jobs:
                job = dict(job)

                if job['endTime'] == 0:
                    num_jobs += 1
                    user_lis.append(job['userId'])

            dic = {}

            dic['jobs'] = num_jobs
            dic['users'] = len(set(user_lis))

            return json.dumps({'data': dic}, indent = 4)

        if (i.start != 'null' and i.end == 'null') or (i.start == 'null' and i.end != '0'):
            return "Time range not given properly"

        flg = 0

        if i.start == 'null' and i.end == '0':
            i.start = '0'
            flg = 1

        start = float(i.start)
        end = float(i.end)

        if end != 0 and start > end:
            return "Invalid time range"

        jobs = getJobs(client, "", "", "", "", int(start), 0)

        if not jobs:
            return json.dumps({"Error": {"Code": 2, "Message": "getJobs returned None"}}, indent=4)

        dic = {}

        dic['Baseline'] = {}
        dic['Baseline']['Jobs'] = 0
        dic['Baseline']['Users'] = 0

        dic['Update'] = {}
        dic['Update']['Jobs'] = 0
        dic['Update']['Users'] = 0

        dic['FailedJobs'] = 0

        baseline_users = []
        update_users = []

        for job in jobs:
            job = dict(job)

            if job['startTime'] < start or (end > 0 and (job['endTime'] > end or job['endTime'] == 0) ):
                continue

            if flg == 1 and job['endTime'] != 0:
                continue

            if job['updateType'] == 'baseline':
                dic['Baseline']['Jobs'] += 1
                baseline_users.append(job['userId'])

            elif job['updateType'] == 'update':
                dic['Update']['Jobs'] += 1
                update_users.append(job['userId'])

        dic['Baseline']['Users'] = len(set(baseline_users))
        dic['Update']['Users'] = len(set(update_users))

        dic['TotalJobs'] = dic['Baseline']['Jobs'] + dic['Update']['Jobs']

        return json.dumps({'data': dic}, indent=4)


class UserInfo:

    def user_get(self, start, end, jtype):

        print 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxSTART TIME = ' + str(start) + 'xxxxxxxxxxxxxxxxxxxxxxxxx'
        print 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxEND TIME = ' + str(end) + 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

        if start == "":
            jobs = getJobs(client, "", "", "", "", 0L, 0)
        else:
            jobs = getJobs(client, "", "", "", "", int(start), 0)

        if not jobs:
            return json.dumps({"Error": {"Code": 2, "Message": "getJobs returned None"}}, indent=4)

        user_lis = {}

        for job in jobs:
            job = dict(job)

            if jtype != 'null' and job['updateType'] != jtype:
                continue

            if end > 0 and job['endTime'] != 0 and job['endTime'] < end:

                print '---------------------USERS IN RANGE GIVEN--------------------------'

                if job['userId'] not in user_lis:
                    try:
                        user_info = list(getUserInfo(client, str(job['userId'])))[0]
                    except TypeError:
                         continue

                    user_lis[job['userId']] = {}

                    user_lis[job['userId']]['firstName'] = user_info['firstName']
                    user_lis[job['userId']]['lastName'] = user_info['lastName']
                    user_lis[job['userId']]['email_id'] = user_info['userHandle']

                    user_lis[job['userId']]['startTime'] = job['startTime']

                    user_lis[job['userId']]['endTime'] = job['endTime']

                if user_lis[job['userId']]['startTime'] > job['startTime']:
                    user_lis[job['userId']]['startTime'] = job['startTime']

                if user_lis[job['userId']]['endTime'] < job['endTime']:
                    user_lis[job['userId']]['endTime'] = job['endTime']

            elif start == 0 and end == 0:

                print '------------------ALL USERS IN DATABASE---------------------------'

                if job['userId'] not in user_lis:
                    try:
                        user_info = list(getUserInfo(client, str(job['userId'])))[0]
                    except TypeError:
                         continue

                    user_lis[job['userId']] = {}

                    user_lis[job['userId']]['firstName'] = user_info['firstName']
                    user_lis[job['userId']]['lastName'] = user_info['lastName']
                    user_lis[job['userId']]['email_id'] = user_info['userHandle']

                    user_lis[job['userId']]['startTime'] = job['startTime']

                    user_lis[job['userId']]['endTime'] = job['endTime']

                if user_lis[job['userId']]['startTime'] > job['startTime']:
                    user_lis[job['userId']]['startTime'] = job['startTime']

                if user_lis[job['userId']]['endTime'] < job['endTime']:
                    user_lis[job['userId']]['endTime'] = job['endTime']

            elif end == 0 and job['endTime'] == 0:

                print '------------------END TIME == 0---------------------------------------'

                if job['userId'] not in user_lis:
                    try:
                        user_info = list(getUserInfo(client, str(job['userId'])))[0]
                    except TypeError:
                         continue

                    user_lis[job['userId']] = {}

                    user_lis[job['userId']]['firstName'] = user_info['firstName']
                    user_lis[job['userId']]['lastName'] = user_info['lastName']
                    user_lis[job['userId']]['email_id'] = user_info['userHandle']

                    user_lis[job['userId']]['startTime'] = job['startTime']

                    user_lis[job['userId']]['endTime'] = job['endTime']

                if user_lis[job['userId']]['startTime'] > job['startTime']:
                    user_lis[job['userId']]['startTime'] = job['startTime']

        print len(user_lis.keys())

        return json.dumps({'userList':user_lis.values()}, indent = 4)



    def GET(self):
        print 'in user_info'

        start_time = time.time()

        i = web.input(check = 'null',
                      start = 'null',
                      end = 'null',
                      jtype = 'null')

        if i.check == 'null':
            return json.dumps({"Error": {"Code": 1, "Message": "Check missing"}})

        if not authcheck(i.check):
            return json.dumps({"Error": {"Code": 1, "Message": "Authentication failed"}})

        if i.start == 'null' and i.end == 'null':
            return self.user_get(0, 0, str(i.jtype))

        if i.start == 'null' and i.end == '0':
            return self.user_get("", 0, str(i.jtype))

        if i.start != 'null' and i.end == 'null':
            return "Time tange not given properly"

        start = float(i.start)
        end = float(i.end)

        if end != 0 and start > end:
            return "Invalid time range"

        return self.user_get(start, end, str(i.jtype))


class LogFile:
    def GET(self):
        i = web.input(check = 'null',
                      path = 'null')

        if i.check == 'null':
            return json.dumps({"Error": {"Code": 1, "Message": "Check missing"}})

        if not authcheck(i.check):
            return json.dumps({"Error": {"Code": 1, "Message": "Authentication failed"}})

        web.header('Content-Type', 'text/plain')

        lines = open(i.path, 'r')
        return lines


if __name__ == '__main__':
    app.run()
