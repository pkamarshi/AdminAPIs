#!/usr/bin/python

import json
import urllib
import httplib2
#import requests
from socket import error as SocketError
import errno
import base64
import sys, string, os
from urllib import urlencode
import requests
import re
import time
from time import gmtime, strftime
import datetime
import cchardet
import ntpath
import gcloud
from gcloud import datastore
from google import protobuf


bucket = 'vaultedgetemp'
METADATA_SERVER = 'http://metadata/computeMetadata/v1/instance/service-accounts'
SERVICE_ACCOUNT = '349060083150-j3fr01rnhpqunmbo9ggsapm8iq7kf9ic@developer.gserviceaccount.com'
GOOGLE_STORAGE_PROJECT_NUMBER = '349060083150'
http = httplib2.Http()

def create_client(project_id):
    return gcloud.datastore.Client(project_id)

# the user record is obtained from RegisteredUsers entity
def getUser(client, user_id, email):
  query = client.query(kind='RegisteredUsers')
  query.add_filter('userHandle', '=', email)
  query.add_filter('userId', '=', user_id)
  r = list(query.fetch())
  return r

# the document passwords is obtained from RegisteredUsers entity
def getDocPasswords(client, user_id, email):
  query = client.query(kind='RegisteredUsers')
  query.add_filter('userHandle', '=', email)
  query.add_filter('userId', '=', user_id)
  r = list(query.fetch())
  if len(r) > 0 and 'docPasswords' in r[0]:
    return r[0]['docPasswords']
  return None

# returns filename from path
def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

# returns a Unicode string
def decodeData(content, encoding):
  try:
    try:
      try:
        try:
          print "decoding using " + encoding
          if encoding == 'UTF-8':
            content2 = content.decode(encoding) #.encode('utf-8')
          else:
            content2 = content.decode(encoding) #.encode('utf-8')
        except:
          #print "exception while decoding using " + encoding
          print "decoding using " + 'utf-16'
          content2 = content.decode('utf-16') #.encode('utf-8')
          encoding = 'utf-16'
      except:
        #print "exception while decoding using " + 'utf-16'
        print "decoding using " + 'utf-8'
        content2 = content.decode('utf-8-sig') #.encode('utf-8')
        encoding = 'utf-8'
    except:
      #print "exception while decoding using " + 'utf-8'
      print "decoding using " + 'latin-1'
      content2 = content.decode('latin-1') #.encode('utf-8')
      encoding = 'latin-1'
  except:
    #print "exception while decoding using " + 'latin-1'
    print "decoding using " + 'ISO-8859-1'
    content2 = content.decode('ISO-8859-1') #.encode('utf-8')
    encoding = 'ISO-8859-1'
  return (content2, encoding)

def get_encoding(data, new_coding = 'UTF-8'):
  try:
    encoding = cchardet.detect(data)['encoding']
  except:
    encoding = ""
  return encoding

def putRequestHttp(url, headers, filePath, retryCount):
  for k in range(0,retryCount):
    try:
      resp, content = http.request(url, "PUT", headers=headers, body=open(filePath, "r"))
      if resp.status != 500 and resp.status != 503: # Internal error or Service unavailable
        break
      time.sleep(1)

    except SocketError as e:
      print "exception in put request while connecting to " + url + " " + str(e.errno)
      time.sleep(1)
  return (resp, content)
    
def postRequest(url, payload, headers, retryCount):
  for k in range(0,retryCount):
    try:
      r = requests.post(url, data=json.dumps(payload), headers=headers)
      break
    except SocketError as e:
      print "exception in post request while connecting to " + url + " " + str(e.errno)
      time.sleep(1)
  return r

def postRequestHttp(url, payload, headers, retryCount):
  for k in range(0,retryCount):
    try:
      resp, content = http.request(url, 'POST', body=payload, headers=headers)
      break
    except SocketError as e:
      print "exception in post request while connecting to " + url + " " + str(e.errno)
      time.sleep(1)
  return (resp, content)



def deleteRequestHttp(url, retryCount):
  for k in range(0,retryCount):
    try:
      resp, content = http.request(url, 'DELETE', body=None)
      break
    except SocketError as e:
      print "exception in delete request while connecting to " + url + " " + str(e.errno)
      time.sleep(1)
  return (resp, content)
    
def gsFileExists(fn):
  gsutil = which('gsutil')
  cmd = gsutil  + " ls " + fn  + " >/dev/null 2>&1"
  ec = os.system(cmd)
  if ec == 0:
    return True 
  return False

def which(program):
    import os
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    
    # TBD : fix this to work inside cron job
    return "/usr/local/bin/gsutil"
    #return None

def deleteFile(file1):
  print "Removing file %s" % file1
  try:
    os.remove(file1)
  except OSError as e:
    if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
      print "Removing file %s failed .. raising exception" % file1
      raise # re-raise exception if a different error occured
  return

def validateDate(date_text, format):
  try:
    datetime.datetime.strptime(date_text, format)
  except ValueError:
    return False
  return True 
