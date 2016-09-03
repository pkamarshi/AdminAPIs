import os, sys
import time
from util import create_client
from gcloud import datastore
import traceback



def addUser(client, user_id, updateType, count):
    key = client.key('Users')
    user = datastore.Entity(key)

    try:
      user.update({
        'userId': user_id,
        'updateType': updateType,
        'count': count
      })
      client.put(user)
      r = user.key.id
    except Exception as e:
      print "addUser exception " + repr(e)
      r = None

    return r

def getUsers(client, user_id, updateType):
  query = client.query(kind='Users')
  if user_id:
    query.filter('userId', '=', user_id)
  if updateType:
    query.add_filter('updateType', '=', updateType)
  try:
    r = list(query.fetch())
  except Exception as e:
    tb = traceback.format_exc() 
    print tb
    print "getUser exception " + repr(e)
    r = None
  return r[0] if r else None

def getUserInfo(client, user_id):
  query = client.query(kind = 'RegisteredUsers')
  if user_id:
    query.add_filter('userId', '=' ,user_id)
  try:
    r = list(query.fetch())
  except Exception as e:
    tb = traceback.format_exc()
    print tb
    print "getUser exception " + repr(e)
    r = None
  return r if r else None 

def updateUser(client, user_id, id, step):
    with client.transaction():
        key = client.key('Users', id)
        user = client.get(key)

        if not user:
            raise ValueError(
                'User{} does not exist.'.format(id))

        user['count'] += step
        client.put(user)

def addJob(client, user_id, updateType, source, sourceType):
    key = client.key('Jobs')
    job = datastore.Entity(key)

    startTime = int(time.time())
    endTime = 0

    try:
      job.update({
        'userId': user_id,
        'updateType': updateType,
        'source': source,
        'sourceType': sourceType,
        'startTime': startTime,
        'endTime': endTime
      })
      client.put(job)
      r = job.key.id
    except Exception as e:
      print "addJob exception " + repr(e)
      r = None

    return r

def getJobs(client, user_id, updateType, source, sourceType, startTime, endTime):
  query = client.query(kind='Jobs')
  if user_id:
    query.add_filter('userId', '=', user_id)
  if updateType:
    query.add_filter('updateType', '=', updateType)
  if source:
    query.add_filter('source', '=', source)
  if sourceType:
    query.add_filter('sourceType', '=', sourceType)
  if startTime > 0:
    query.add_filter('startTime', '>', startTime)
  if endTime > 0:
    query.add_filter('endTime', '<', endTime)

  try:
    r = list(query.fetch())
  except Exception as e:
    tb = traceback.format_exc() 
    print tb
    print "getJobs exception " + repr(e)
    r = None
  return r if r else None


def addTask(client, user_id, jobId, taskName, server, pid):
    key = client.key('Tasks')
    task = datastore.Entity(key)

    startTime = int(time.time())
    endTime = 0

    try:
      task.update({
        'userId': user_id,
        'jobId': jobId,
        'taskName': taskName,
        'server': server,
        'pid': pid,
        'startTime': startTime,
        'endTime': endTime
      })
      client.put(task)
      r = task.key.id
    except Exception as e:
      print "addTask exception " + repr(e)
      r = None

    return r

def getTasks(client, user_id, jobId, taskName, server, pid, startTime, endTime):
  query = client.query(kind='Tasks')
  if user_id:
    query.add_filter('userId', '=', user_id)
  if jobId > 0:
    query.add_filter('jobId', '=', jobId)
  if taskName:
    query.add_filter('taskName', '=', taskName)
  if server:
    query.add_filter('server', '=', server)
  if pid > 0:
    query.add_filter('pid', '=', pid)
  if startTime > 0:
    query.add_filter('startTime', '>=', startTime)
  if endTime > 0:
    query.add_filter('endTime', '<=', endTime)

  try:
    r = list(query.fetch())
  except Exception as e:
    tb = traceback.format_exc() 
    print tb
    print "getTasks exception " + repr(e)
    r = None
  return r if r else None


def updateTask(client, taskId, endTime):
    with client.transaction():
        key = client.key('Tasks', taskId)
        task = client.get(key)

        if not task:
            raise ValueError(
                'Task{} does not exist.'.format(taskId))

        task['endTime'] = endTime
        client.put(task)

def addJobId(client, user_id, updateType, email, vendor, jobId):
    key = client.key('TempStore')
    tempStore = datastore.Entity(key)

    try:
      tempStore.update({
        'userId': user_id,
        'type': updateType + "%%" + email + "%%" + vendor,
        'count': jobId
      })
      client.put(tempStore)
      r = tempStore.key.id
    except Exception as e:
      print "addJobId exception " + repr(e)
      r = None

    return r
  
def getJobIds(client, user_id, updateType, email, vendor):
  query = client.query(kind='TempStore')
  if user_id:
    query.add_filter('userId', '=', user_id)
  if updateType and email and vendor:
    query.add_filter('type', '=', updateType + "%%" + email + "%%" + vendor)
  try:
    r = list(query.fetch())
  except Exception as e:
    tb = traceback.format_exc()
    print tb
    print "getJobIds exception " + repr(e)
    r = None
  return r[0] if r else None
  
def updateJobId(client, user_id, id, jobId):
    with client.transaction():
        key = client.key('TempStore', id)
        tempStore = client.get(key)

        if not tempStore:
            raise ValueError(
                'TempStore{} does not exist.'.format(id))

        tempStore['count'] = jobId
        client.put(tempStore)


def taskExists(client, user_id, jobId, taskName, server):
  tasks = getTasks(client, user_id, jobId, "", "", 0, 0L, 0)
  if tasks:
    for task in tasks:
      print task
      if task['taskName'] == taskName and task['server'] == server:
        return True 
  return False

def main():
  user_id = sys.argv[1]
  client = create_client('vaultedgewebtier')

  # user creation
  
  updateType = "baseline"
  user = getUsers(client, user_id, updateType)
  if not user:
    print user_id + " not exists"
    addUser(client, user_id, updateType, 1)
  else:
    print user_id + " exists"
    print user
  
  """
  print user.key.id
  print "adding 3 more jobs to user " + user_id
  updateUser(client, user_id, user.key.id, 3)
  user = getUser(client, user_id, "baseline")
  print user
  """
  
  """
  # list all baseline jobs for a user
  endTime = 1471456195L
  jobs = getJobs(client, user_id, updateType, "sasajeev@hotmail.com", "outlook", 1471456170L, 0)
  if jobs:
    for job in jobs:
      if job['endTime'] < endTime:
        print job
  else:
    print "no jobs found for user " + user_id
    source = "sasajeev@hotmail.com"
    sourceType = "outlook"
    addJob(client, user_id, updateType, source, sourceType)
    jobs = getJobs(client, user_id, updateType, "ab", "", 0, 0)
    if jobs:
      for job in jobs:
        if job['endTime'] < endTime:
          print job
  """
   

  endTime = 1471496195L
  tasks = getTasks(client, user_id, 0, "", "", 0, 1471456170L, 0)
  if tasks:
    for task in tasks:
      if task['endTime'] < endTime:
        print task
  else:
    print "no tasks found for user " + user_id
    addTask(client, user_id, 6050318894759936L, 'sync', 'win-instance-1', 978, 1471456170L, 0)
    tasks = getTasks(client, user_id, 0, "", "", 0, 1471456170L, 0)
    if tasks:
      for task in tasks:
        if task['endTime'] < endTime:
          print task




if __name__ == '__main__':
  main()
