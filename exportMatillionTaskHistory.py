#!/usr/bin/env python

import boto3
import requests
from datetime import datetime, timedelta
from cStringIO import StringIO

# config/var def
matillionApiRootUrl = 'http://REDACTED FROM COMMIT/rest/v0/'
apiUser = 'REDACTED FROM COMMIT'
apiPassword = 'REDACTED FROM COMMIT'
taskHistoryBucket = 'REDACTED FROM COMMIT'

class Record:
    def __init__(self, groupName, projectName):
        self.groupName = groupName
        self.projectName = projectName
        self.taskHistory = ""
        self.sinceDate = ""

def GetRecords ( matillionApiRootUrl, apiUser, apiPassword ):
    records = []
    resp = requests.get(matillionApiRootUrl+'projects?export=false', auth=(apiUser, apiPassword))
    projectGroups = resp.json()['groups']
    for projectGroup in projectGroups:
        groupName = projectGroup['projectGroup']
        for project in projectGroup['projects']:
            projectName = project['name']
            currRecord = Record( groupName, projectName )
            records.append(currRecord)
    return records

def GetTaskHistories ( matillionApiRootUrl, apiUser, apiPassword, records ):
    for record in records:
        resp = requests.get(matillionApiRootUrl+'tasks?groupName='+record.groupName+'&projectName='+record.projectName+'&running=false&since='+getYesterday(), auth=(apiUser, apiPassword))
        record.taskHistory = str(resp.json())
        record.sinceDate = getYesterday()
    return records

def getYesterday():
    d = datetime.today() - timedelta(days=1)
    return "%s-%s-%s" % (d.year, d.month, d.day)

def uploadToS3 ( taskHistories, taskHistoryBucket ):
    s3 = boto3.client('s3')
    for taskHistory in taskHistories:
        key = "%s/%s/%s.log" % (taskHistory.groupName, taskHistory.projectName, taskHistory.sinceDate)
        fake_handle = StringIO(taskHistory.taskHistory) #http://stackoverflow.com/a/39145592
        s3.put_object(Bucket=taskHistoryBucket, Key=key, Body=fake_handle.read())


if __name__ == '__main__':
    records = GetRecords ( matillionApiRootUrl, apiUser, apiPassword )
    taskHistories = GetTaskHistories ( matillionApiRootUrl, apiUser, apiPassword, records )
    uploadToS3 ( taskHistories, taskHistoryBucket )

'''
TODO:
- refactor as a Lambda function (use an IAM role, keys will not be required)
- add try/except error handling
- add logging to CloudWatch Logs (include exceptions from error handling above)
- add s3 bucket creation to cfn templates (the one used here was created manually)
'''
