#!/usr/bin/python
#

try:
  from xml.etree import ElementTree # for Python 2.5 users
except ImportError:
  from elementtree import ElementTree
import httplib2
import sys
#import gdata.calendar.service
#import gdata.service
#import atom.service
#import gdata.calendar
#import atom
import getopt
import string
import time
import MySQLdb
import datetime
import smtplib
import MySQLdb.cursors
from ConfigParser import SafeConfigParser
import logging
import threading
from apiclient.discovery import build
from oauth2client.file import Storage
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.tools import run


#============= Log File name and Format
today = datetime.date.today()
logfile='Rain_Maker%s.log' % (today)

time.sleep(10)
logging.basicConfig(format='%(asctime)s %(message)s',filename=logfile,filemode='w',level=logging.DEBUG)

logging.debug('.....')
logging.info('Rain Maker Initiating')
print 'Logging to %s' % (logfile)

#============= Gather some data from config file
parser = SafeConfigParser()
parser.read('irrigation.ini')
ip_address = parser.get('DB_Con_Info','ip')
db_user = parser.get('DB_Con_Info','username')
db_pass = parser.get('DB_Con_Info','password')
db_database = parser.get('DB_Con_Info','database')

client_id = parser.get('Calendar_Config','client_id')
client_secret = parser.get('Calendar_Config','client_secret')
scope = parser.get('Calendar_Config','scope')
calendar_ID=parser.get('Calendar_Config','calendar_ID')

flow = OAuth2WebServerFlow(client_id, client_secret, scope)

logging.info('Connecting to %s database, with %s @ %s at IP: %s',db_database,db_user,db_pass,ip_address)

#============= Building data for connection to google calendar

class DBCleanThread(threading.Thread):
	def __init__(self,calendar_id,start_date,end_date,target_DB,thread1):
		threading.Thread.__init__(self)
		self.calendar_id=calendar_id
		self.start_date=start_date
		self.end_date=end_date
		self.target_DB=target_DB
		self.thread1=thread1

	def run(self):
		#if self.thread1.isAlive:
		#	self.thread1.join()
		print '> STARTING DB Clean %s %s %s %s %s' % (self.calendar_id,self.start_date,self.end_date,self.target_DB,self.thread1)
		#calendar_list=DateRangeQuery(self.calendar_service,self.start_date,self.end_date)

		#db = MySQLdb.connect(ip_address,"testuser","test123","TESTDB" )
		# prepare a cursor object using cursor() method
		#cursor = db.cursor()
		#cursor.execute('SHOW TABLES;')

		#table_list=[]
		#for (table_name,) in cursor:
		#	table_list.append(table_name)
		#	print table_name
		#	clean_db(table_name,cursor,db);
		#db.close()
		print '> Finished DB Clean'

class DBUpdateThread(threading.Thread):
	def __init__(self,calendar_id,start_date,end_date,flow):
		threading.Thread.__init__(self)
		self.calendar_id=calendar_id
		self.start_date=start_date
		self.end_date=end_date
		self.flow=flow

	def run(self):
		print '! STARTING Database Update %s %s %s %s' % (self.calendar_id,self.start_date,self.end_date,self.flow)
		storage = Storage('credentials.dat')
		credentials = storage.get()
		if credentials is None or credentials.invalid:
			credentials = run(flow, storage)
		http = httplib2.Http()
		http = credentials.authorize(http)
		service = build('calendar', 'v3', http=http)


		logging.debug('Opening DB for write') # Open database connection
		db = MySQLdb.connect(ip_address,db_user,db_pass,db_database)
		logging.debug('DB open for write')
		cursor = db.cursor() # prepare a cursor object using cursor() method
		logging.debug('Enumerating Results for database write')

		try:
			events = service.events().list(calendarId=self.calendar_id,maxResults=1000,orderBy='startTime',showDeleted='True',singleEvents='True',timeMax=self.end_date,timeMin=self.start_date).execute()
			while True:
				for event in events['items']:
					Write_DB(cursor,event['summary'],event['id'],event['start']['dateTime'],event['end']['dateTime'])
					#print 'found start time %s' % event['end.dateTime']
					try:
						if event['status'] == 'confirmed':
							#confirmed_list.append(event['status'] + event['summary'] + event['id'])
							print '>>>>> CONFIRMED Event %s with ID (%s) | Start Time = %s, End Time = %s' % (event['summary'],event['id'],event['start']['dateTime'],event['end']['dateTime'])
						if event['status'] == 'cancelled':
							#cancelled_list.append(event['status'] + event['summary'] + event['id'])
							print '!     CANCELLED Event %s with ID (%s) | Start Time = %s, End Time = %s' % (event['summary'],event['id'],event['start']['dateTime'],event['end']['dateTime'])
						#print '%s - %s - %s - %s - %s' %(event['status'],event['summary'],event['start.timeZone'],event['end'],event['id'])
						#print event
					except KeyError:
						print 'No Summary'
					except:
						print 'Some Exception'
				page_token = events.get('nextPageToken')
				if page_token:
					events = service.events().list(calendarId=calendar_id,maxResults=1000,orderBy='startTime',showDeleted='True',singleEvents='True',timeMax=end_date,timeMin=start_date,pageToken=page_token).execute()
				else:
					break


		except AccessTokenRefreshError:
		# The AccessTokenRefreshError exception is raised if the credentials
		# have been revoked by the user or they have expired.
			print ('The credentials have been revoked or expired, please re-run'
				'the application to re-authorize')


		#Update_DB(self.calendar_service,self.start_date,self.end_date)
		print '! Finished Database Update'


def Write_DB(cursor,event_summary,event_id,start_time,end_time):
	logging.info('Event %s found for writing is %s',i, event_summary)
	#Write to DB
	sqlinsert = "INSERT INTO %s(Event_ID, Start_Time, End_Time, Processed) VALUES ('%s', '%s', '%s', 'no' )" % (event_summary, event_id, start_time, end_time)
	logging.info('SQL insert string is: %s',sqlinsert)
	try:
		cursor.execute(sqlinsert)
		db.commit()
		logging.info('Wrote values to %s table: %s/%s/%s',event_summary, event_id, start_time, end_time)
		#except:
			#print '\t\tCouldnt write to DB'
	except MySQLdb.Error, e:
		if e.args[0]==1062:
			logging.warning('Error %d: %s',e.args[0], e.args[1])
			logging.debug('Record already exists, updating existing record!')
			sqlupdate = "UPDATE %s SET Start_Time = '%s', End_Time = '%s' WHERE Event_ID = '%s'" % (event_summary, start_time, end_time, event_id)
			logging.info('Updating record with SQL command: %s',sqlupdate)
			cursor.execute(sqlupdate)
			db.commit()
			logging.info('Updated %s record with key: %s',event_summary,event_id)
		else:
			logging.warning('Could not write %s to %s' % (event_id, event_summary))
			#print 'Unknown Error'
			db.rollback()
	logging.debug('Closing DB Connection')
	db.close()
	logging.debug('DB connection Closed')



hour = time.strftime('%X')[:2]
'16:08:12 05/08/03 AEST'

print hour
var = 1
thread1 = DBUpdateThread(calendar_ID,'2012-11-14T10:00:00-05:00','2012-11-24T10:00:00-05:00',flow)
thread2 = DBCleanThread(calendar_ID,'2012-11-14T10:00:00-05:00','2012-11-24T10:00:00-05:00','TESTDB',thread1)

while var == 1 :  # This constructs an infinite loop
	if thread1.isAlive():
		print '\ndont start thread1 as it is still running'
	else:
		print '\nstarting thread1'
		thread1 = DBUpdateThread(calendar_ID,'2012-11-14T10:00:00-05:00','2012-11-24T10:00:00-05:00',flow)
		thread1.start()
	if thread2.isAlive():
		print '\ndont start thread2 as it is still running'
	else:
		print '\nstarting thread2'
		thread2 = DBCleanThread(calendar_ID,'2012-11-14T10:00:00-05:00','2012-11-24T10:00:00-05:00','TESTDB',thread1)
		thread2.start()

	for p in '12345':
		print 'Main Thread %s' % (p)
		time.sleep(60)
	if thread1.isAlive():
		print 'Thread 1 is still running'
	print '\nloop done'
	time.sleep(.5)

