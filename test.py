
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


today = datetime.date.today()

logfile='Rain_Maker%s.log' % (today)
logging.basicConfig(format='%(asctime)s %(message)s',filename=logfile,filemode='w',level=logging.DEBUG)
logging.debug('.....')
logging.info('Rain Maker Initiating')
print 'Logging to %s' % (logfile)
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

calendar_service = gdata.calendar.service.CalendarService()
calendar_service.email = gmail_user
calendar_service.password = gmail_pass
calendar_service.source = 'Google-Calendar_Python_Sample-1.0'
calendar_service.ProgrammaticLogin()
# Open database connection

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

def clean_db(tablename):
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
	except: print "Error: unable to fecth data"

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
				print 'tried to delete %s' % (primary_key)
				# Commit your changes in the database
				db.commit()
			except:
				# Rollback in case there is any error
				print 'could not delete'
				db.rollback()



db = MySQLdb.connect("192.168.1.133","testuser","test123","TESTDB" )
# prepare a cursor object using cursor() method
cursor = db.cursor()
cursor.execute('SHOW TABLES;')

table_list=[]
for (table_name,) in cursor:
	table_list.append(table_name)
	print table_name
	clean_db(table_name);


db.close()