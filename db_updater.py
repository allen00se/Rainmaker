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
logging.basicConfig(format='%(asctime)s %(message)s',filename=logfile,filemode='w',level=logging.DEBUG)
logging.info('.....')
logging.info('Rain Maker Initiating')
print 'Logging to %s' % (logfile)

#============= Gather some data from config file
logging.info('Grabbing data from config file')
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
logging.info('Got data from config')

logging.info('Generating flow fror gmail calendar authentication')
flow = OAuth2WebServerFlow(client_id, client_secret, scope)

#================== Defining Classes
class DBCleanThread(threading.Thread):
	def __init__(self,calendar_id,start_date,end_date,target_DB,thread1):
		threading.Thread.__init__(self)
		self.calendar_id=calendar_id
		self.start_date=start_date
		self.end_date=end_date
		self.target_DB=target_DB
		self.thread1=thread1

	def run(self):
		logging.info('Starting DB Clean')
		logging.debug('Connecting to %s database, with %s @ %s at IP: %s',db_database,db_user,db_pass,ip_address)
		db = MySQLdb.connect(ip_address,db_user,db_pass,db_database)
		logging.debug('Connected to database')
		cursor = db.cursor() # prepare a cursor object using cursor() method
		clean_db('Irrigation',cursor,db);
		db.close()
		logging.info('Finished DB Clean')

class DBUpdateThread(threading.Thread):
	def __init__(self,calendar_id,start_date,end_date,flow):
		threading.Thread.__init__(self)
		self.calendar_id=calendar_id
		self.start_date=start_date
		self.end_date=end_date
		self.flow=flow

	def run(self):
		logging.info('Starting Database Update %s %s %s %s',self.calendar_id,self.start_date,self.end_date,self.flow)
		storage = Storage('credentials.dat')
		credentials = storage.get()
		logging.debug('Connecting to %s database, with %s @ %s at IP: %s',db_database,db_user,db_pass,ip_address)
		db = MySQLdb.connect(ip_address,db_user,db_pass,db_database)
		logging.debug('Opening DB for write') # Open database connection
		if credentials is None or credentials.invalid:
			credentials = run(flow, storage)
		http = httplib2.Http()
		http = credentials.authorize(http)
		service = build('calendar', 'v3', http=http)
		cursor = db.cursor() # prepare a cursor object using cursor() method
		logging.debug('DB open for write')

		try:
			events = service.events().list(calendarId=self.calendar_id,maxResults=1000,orderBy='startTime',showDeleted='True',singleEvents='True',timeMax=self.end_date,timeMin=self.start_date).execute()
			while True:
				for event in events['items']:
					logging.debug('Enumerating Results for database write')
					#Write_DB('Irrigation',db,cursor,event['summary'],event['id'],event['start']['dateTime'],event['end']['dateTime'],event['status'])
					#print 'found start time %s' % event['end.dateTime']
					try:
						if event['status'] == 'confirmed':
							Write_DB('Irrigation',db,cursor,event['summary'],event['id'],event['start']['dateTime'],event['end']['dateTime'],event['status'])
							logging.debug('Confirmed Event %s with ID (%s) | Start Time = %s, End Time = %s') % (event['summary'],event['id'],event['start']['dateTime'],event['end']['dateTime'])
						if event['status'] == 'cancelled':
							Write_DB('CANCELLED',db,cursor,event['summary'],event['id'],event['start']['dateTime'],event['end']['dateTime'],event['status'])
							logging.debug('Cancelled Event %s with ID (%s) | Start Time = %s, End Time = %s') % (event['summary'],event['id'],event['start']['dateTime'],event['end']['dateTime'])
					except KeyError:
						logging.info('Key Error Exception, check logfile for details')
						print 'No Summary'
					except:
						logging.info('Unidentified exception, check log for details')
						print 'Some Exception'
				page_token = events.get('nextPageToken')
				if page_token:
					events = service.events().list(calendarId=calendar_id,maxResults=1000,orderBy='startTime',showDeleted='True',singleEvents='True',timeMax=end_date,timeMin=start_date,pageToken=page_token).execute()
				else:
					break


		except AccessTokenRefreshError:
		# The AccessTokenRefreshError exception is raised if the credentials
		# have been revoked by the user or they have expired.
			logging.info('The credentials have been revoked or expired, please rerun the application to re-authorize')
			print ('The credentials have been revoked or expired, please re-run'
				'the application to re-authorize')


		#Update_DB(self.calendar_service,self.start_date,self.end_date)
		logging.debug('Closing DB Connection')
		db.close()
		logging.debug('DB connection Closed')
		logging.debug('Database update completed')

def Write_DB(table,db,cursor,event_summary,event_id,start_time,end_time,status):
	logging.debug('Found event %s for writing', event_summary)
	#Write to DB
	sqlinsert = "INSERT INTO %s(Event_ID, Area, Start_Time, End_Time, Processed,Status) VALUES ('%s','%s', '%s', '%s', 'no', '%s' )" % (table, event_id, event_summary, start_time, end_time, status)
	logging.debug('SQL insert string is: %s',sqlinsert)
	try:
		cursor.execute(sqlinsert)
		db.commit()
		logging.debug('Wrote values to %s table: %s/%s/%s/%s',event_summary, event_id, start_time, end_time, status)
		#except:
			#print '\t\tCouldnt write to DB'
	except MySQLdb.Error, e:
		if e.args[0]==1062:
			logging.warning('Warning %d: %s',e.args[0], e.args[1])
			logging.debug('Record already exists, updating existing record!')
			sqlupdate = "UPDATE %s SET Start_Time = '%s', End_Time = '%s', Status = '%s' WHERE Event_ID = '%s'" % (event_summary, start_time, end_time,status, event_id)
			logging.debug('Updating record with SQL command: %s',sqlupdate)
			cursor.execute(sqlupdate)
			db.commit()
			logging.debug('Updated %s record with key: %s',event_summary,event_id)
		else:
			logging.warning('Could not write %s to %s' % (event_id, event_summary))
			logging.info('Unknown Error')
			db.rollback()

def clean_db(tablename,cursor,db):
	sql = 'SELECT * FROM Irrigation WHERE Status = "cancelled"'
	sql_delete = "DELETE FROM %s WHERE Status = 'cancelled'" % (tablename)
	try:
		# Execute the SQL command
		cursor.execute(sql_delete)
		db.commit()
		logging.debug('Deleted cancelled entries from %s') % (tablename)
	except:
		# Rollback in case there is any error
		logging.debug('Could not delete cancelled entries from %s',tablename)
		db.rollback()


localtime = time.localtime(time.time())
print '%s-%s-%sT00:01:00-06:00' % (localtime.tm_year,localtime.tm_mon,localtime.tm_mday)
hour = time.strftime('%X')[:2]
'16:08:12 05/08/03 AEST'
var = 1

current_date='%s-%s-%sT00:01:00-06:00' % (localtime.tm_year,localtime.tm_mon,localtime.tm_mday)
thread1 = DBUpdateThread(calendar_ID,current_date,'2012-12-31T10:00:00-05:00',flow)
thread2 = DBCleanThread(calendar_ID,'2012-11-14T10:00:00-05:00','2012-11-24T10:00:00-05:00','TESTDB',thread1)

while var == 1 :  # This constructs an infinite loop
	logfile='Rain_Maker%s.log' % (today)
	localtime = time.localtime(time.time())
	current_date='%s-%s-%sT00:01:00-06:00' % (localtime.tm_year,localtime.tm_mon,localtime.tm_mday)
	if thread1.isAlive():
		logging.debug('Dont start thread1 as it is still running')
	else:
		logging.debug('Starting thread1')
		thread1 = DBUpdateThread(calendar_ID,current_date,'2012-12-31T10:00:00-05:00',flow)
		thread1.start()
	if thread2.isAlive():
		logging.debug('Dont start thread2 as it is still running')
	else:
		logging.debug('Starting thread2')
		thread2 = DBCleanThread(calendar_ID,'2012-11-14T10:00:00-05:00','2012-12-24T10:00:00-05:00','TESTDB',thread1)
		thread2.start()

	for p in '12345':
		print 'Main Thread %s' % (p)
		time.sleep(60)

	time.sleep(.5)

