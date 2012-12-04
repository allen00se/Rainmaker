#!/usr/bin/python

try:
  from xml.etree import ElementTree # for Python 2.5 users
except ImportError:
  from elementtree import ElementTree
import gdata.calendar.service
import gdata.service
import atom.service
import gdata.calendar
import atom
import getopt
import sys
import string
import time
import MySQLdb
import datetime
import smtplib
import MySQLdb.cursors
from ConfigParser import SafeConfigParser
import logging
import threading

#============= Log File name and Format
today = datetime.date.today()
logfile='Rain_Maker%s.log' % (today)
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
gmail_user=parser.get('Email_Config','username')
gmail_pass=parser.get('Email_Config','password')
gmail_port=parser.get('Email_Config','port')
gmail_host=parser.get('Email_Config','host')
calendar_ID=parser.get('Calendar_Config','calendar_ID')

logging.info('Connecting to %s database, with %s @ %s at IP: %s',db_database,db_user,db_pass,ip_address)

#============= Building data for connection to google calendar
calendar_service = gdata.calendar.service.CalendarService()
calendar_service.email = gmail_user
calendar_service.password = gmail_pass
calendar_service.source = 'Google-Calendar_Python_Sample-1.0'
calendar_service.ProgrammaticLogin()


class DBCleanThread(threading.Thread):
	def __init__(self,calendar_service,start_date,end_date,target_DB,thread1):
		threading.Thread.__init__(self)
		self.calendar_service=calendar_service
		self.start_date=start_date
		self.end_date=end_date
		self.target_DB=target_DB
		self.thread1=thread1

	def run(self):
		if self.thread1.isAlive:
			self.thread1.join()
		print 'STARTING DB Clean'
		calendar_list=DateRangeQuery(self.calendar_service,self.start_date,self.end_date)

		db = MySQLdb.connect(ip_address,"testuser","test123","TESTDB" )
		# prepare a cursor object using cursor() method
		cursor = db.cursor()
		cursor.execute('SHOW TABLES;')

		table_list=[]
		for (table_name,) in cursor:
			table_list.append(table_name)
			print table_name
			clean_db(table_name,cursor,db);
		db.close()
		print 'Finished DB Clean'

class DBUpdateThread(threading.Thread):
	def __init__(self,calendar_service,start_date,end_date):
		threading.Thread.__init__(self)
		self.calendar_service=calendar_service
		self.start_date=start_date
		self.end_date=end_date

	def run(self):
		print 'STARTING Database Update'
		Update_DB(self.calendar_service,self.start_date,self.end_date)
		print 'Finished Database Update'



def DateRangeQuery(calendar_service, start_date='2007-01-01', end_date='2007-07-01'):
  logging.info('Date range query for events on Primary Calendar: %s to %s',start_date, end_date)
  logging.info('Downloading Google Calendar Feed from %s using password %s',calendar_service.email,calendar_service.password)
  query = gdata.calendar.service.CalendarEventQuery(calendar_ID, 'private', 'full')
  query.start_min = start_date
  query.start_max = end_date
  logging.info('Querying feed for specified date range of %s to %s',query.start_min,query.start_max)
  feed = calendar_service.CalendarQuery(query)
  calendar_list=[]
  for i, an_event in enumerate(feed.entry):
    #logging.info('Event %s found for writing is %s',i, an_event.title.text)
    for a_when in an_event.when:
      calendar_list.append(a_when.start_time + an_event.title.text)
  return calendar_list

#calendar_list=DateRangeQuery(calendar_service,'2012-10-01','2012-12-30')

def clean_db(tablename,cursor,db):
	sql = 'SELECT * FROM %s WHERE Processed != "yes"' % tablename
	try:
		# Execute the SQL command
		cursor.execute(sql)
		# Fetch all the rows in a list of lists.
		results = cursor.fetchall()
		L=[]
		for row in results:
			pkey = row[0]
			L.append(row[0]+tablename)

		print len(L)
		calendar_list=DateRangeQuery(calendar_service,'2012-10-01','2012-12-30')
		for item in L:
			#check item against each event
			source = item
			signal=0
			for item in calendar_list:
				if item == source:
					signal=signal+1
					print 'matched %s and %s' % (source,item)
			if signal < 1:
				primary_key=source.replace(tablename,"");
				print '%s was not matched %s' % (primary_key,signal)
				sql_delete = "DELETE FROM %s WHERE String_Time = '%s'" % (tablename,primary_key)
				try:
					# Execute the SQL command
					cursor.execute(sql_delete)
					db.commit()
					print 'Deleted %s' % (primary_key)
				except:
					# Rollback in case there is any error
					print 'Could not delete %s ' % (primary_key)
					db.rollback()
	except: print "Error: unable to fecth data"

def Update_DB(calendar_service, start_date='2007-01-01', end_date='2007-07-01'):
  logging.info('Date range query for events on Primary Calendar: %s to %s',start_date, end_date)
  logging.info('Downloading Google Calendar Feed from %s using password %s',calendar_service.email,calendar_service.password)
  query = gdata.calendar.service.CalendarEventQuery(calendar_ID, 'private', 'full')
  query.start_min = start_date
  query.start_max = end_date
  logging.info('Querying feed for specified date range of %s to %s',query.start_min,query.start_max)
  feed = calendar_service.CalendarQuery(query)
  # Open database connection
  logging.debug('Opening DB for write')
  db = MySQLdb.connect(ip_address,db_user,db_pass,db_database)
  logging.debug('DB open for write')
  # prepare a cursor object using cursor() method
  cursor = db.cursor()
  #print feed
  logging.debug('Enumerating Results for database write')
  for i, an_event in enumerate(feed.entry):
    logging.info('Event %s found for writing is %s',i, an_event.title.text)
    for a_when in an_event.when:
      #Write to DB
      sqlinsert = "INSERT INTO %s(String_Time, Month, Day, Year, Start_Time, End_Time, Processed) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', 'no' )" % (an_event.title.text, a_when.start_time, a_when.start_time[5:7], a_when.start_time[8:10], a_when.start_time[:4], a_when.start_time[11:19], a_when.end_time[11:19])
      logging.info('SQL insert string is: %s',sqlinsert)
      try:
        cursor.execute(sqlinsert)
        db.commit()
        logging.info('Wrote values to %s table: %s/%s/%s - %s',an_event.title.text, a_when.start_time[5:7], a_when.start_time[8:10], a_when.start_time[:4], a_when.start_time[11:19])
      #except:
        #print '\t\tCouldnt write to DB'
      except MySQLdb.Error, e:
        if e.args[0]==1062:
          logging.warning('Error %d: %s',e.args[0], e.args[1])
          logging.debug('Record already exists, updating existing record!')
          sqlupdate = "UPDATE %s SET Start_Time = '%s', End_Time = '%s' WHERE String_Time = '%s'" % (an_event.title.text, a_when.start_time[11:19], a_when.end_time[11:19], a_when.start_time)
          logging.info('Updating record with SQL command: %s',sqlupdate)
          cursor.execute(sqlupdate)
          db.commit()
          logging.info('Updated %s record with key: %s',an_event.title.text,a_when.start_time)
        else:
          logging.warning('Could not write %s to %s' % (a_when.start_time, an_event.title.text))
          #print 'Unknown Error'
        db.rollback()
  logging.debug('Closing DB Connection')
  db.close()
  logging.debug('DB connection Closed')

def Send_Mail(mail_user,mail_pass,mail_recipient,mail_port,mail_host,mail_subject,mail_message):
	logging.info('Establishing a SMTP Email connection with server %s on port %s',mail_host,mail_port)
	smtpserver = smtplib.SMTP(mail_host,mail_port)
	smtpserver.ehlo()
	smtpserver.starttls()
	smtpserver.ehlo
	logging.info('Creating SMTP Login Credentials as user %s with password %s',mail_user,mail_pass)
	smtpserver.login(mail_user, mail_pass)
	logging.debug('SMTP Credentials successfully created')
	header = 'To:' + mail_recipient + '\n' + 'From: ' + mail_user + '\n' + 'Subject:' + mail_subject + ' \n'
	msg=header + mail_message
	logging.debug('Mail message and header have been created')
	smtpserver.sendmail(mail_user,mail_recipient,msg)
	logging.debug('Mail message has been sent!')
	smtpserver.close()





hour = time.strftime('%X')[:2]
'16:08:12 05/08/03 AEST'

print hour
var = 1
thread1 = DBUpdateThread(calendar_service,'2012-10-01','2012-12-30')
thread2 = DBCleanThread(calendar_service,'2012-10-01','2012-12-30','TESTDB',thread1)

while var == 1 :  # This constructs an infinite loop
	if thread1.isAlive():
		print '\ndont start thread1 as it is still running'
	else:
		print '\nstarting thread1'
		thread1 = DBUpdateThread(calendar_service,'2012-10-01','2012-12-30')
		thread1.start()
	if thread2.isAlive():
		print '\ndont start thread2 as it is still running'
	else:
		print '\nstarting thread2'
		thread2 = DBCleanThread(calendar_service,'2012-10-01','2012-12-30','TESTDB',thread1)
		thread2.start()






	#thread1.join()  # This waits until the thread has completed
	#thread2.join()
	# At this point, both threads have completed
	#result = thread1.total + thread2.total
	#print result
	for p in '12345':
		print 'Main Thread %s' % (p)
		time.sleep(60)
	if thread1.isAlive():
		print 'Thread 1 is still running'
	print '\nloop done'
	time.sleep(.5)