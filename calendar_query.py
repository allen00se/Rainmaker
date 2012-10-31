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


logging.info('Connecting to %s database, with %s @ %s at IP: %s',db_database,db_user,db_pass,ip_address)

calendar_service = gdata.calendar.service.CalendarService()
calendar_service.email = gmail_user
calendar_service.password = gmail_pass
calendar_service.source = 'Google-Calendar_Python_Sample-1.0'
calendar_service.ProgrammaticLogin()

def DateRangeQuery(calendar_service, start_date='2007-01-01', end_date='2007-07-01'):
  logging.info('Date range query for events on Primary Calendar: %s to %s',start_date, end_date)
  logging.info('Downloading Google Calendar Feed from %s using password %s',calendar_service.email,calendar_service.password)
  query = gdata.calendar.service.CalendarEventQuery('t95e41gvdfnkh4268fumg3pnsc@group.calendar.google.com', 'private', 'full')
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
      sqlinsert = "INSERT INTO %s(String_Time, Month, Day, Year, Start_Time, End_Time) VALUES ('%s', '%s', '%s', '%s', '%s', '%s' )" % (an_event.title.text, a_when.start_time, a_when.start_time[5:7], a_when.start_time[8:10], a_when.start_time[:4], a_when.start_time[11:19], a_when.end_time[11:19])
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

#PrintAllEventsOnDefaultCalendar(calendar_service);
#FullTextQuery(calendar_service, 'Milly');
DateRangeQuery(calendar_service,'2012-10-01','2012-12-30');
Send_Mail(gmail_user,gmail_pass,gmail_user,gmail_port,gmail_host,'Rain Maker made some Rain!','\n This was a test of python smtplib!\n\n')
#PrintUserCalendars(calendar_service);


#UPDATE  `TESTDB`.`Fescue` SET  `Start_Time` =  '13',
#`End_Time` =  '14' WHERE  `Fescue`.`String_Time` =  '2012-10-19T08:30:00.000-05:00';



def FullTextQuery(calendar_client, text_query='Tennis'):
  print 'Full text query for events on Primary Calendar: \'%s\'' % ( text_query,)
  query = gdata.calendar.client.CalendarEventQuery('t95e41gvdfnkh4268fumg3pnsc@group.calendar.google.com','private','full',text_query)
  feed = calendar_client.GetCalendarEventFeed(q=query)
  for i, an_event in enumerate(feed.entry):
    print '\t%s. %s' % (i, an_event.title.text,)
    print '\t\t%s. %s' % (i, an_event.content.text,)
    for a_when in an_event.when:
      print '\t\tStart time: %s' % (a_when.start,)
      print '\t\tEnd time:   %s' % (a_when.end,)

def PrintUserCalendars(calendar_service):
  feed = calendar_service.GetAllCalendarsFeed()
  #print feed.title.text
  print feed
  #for i, a_calendar in enumerate(feed.entry):
  #  print '\t%s. %s' % (i, a_calendar.title.text,)

def PrintAllEventsOnDefaultCalendar(calendar_service):
  feed = calendar_service.GetCalendarEventFeed()
  print 'Events on Primary Calendar: %s' % (feed.title.text,)
  for i, an_event in enumerate(feed.entry):
    print '\t%s. %s' % (i, an_event.title.text,)
    for p, a_participant in enumerate(an_event.who):
      print '\t\t%s. %s' % (p, a_participant.email,)
      print '\t\t\t%s' % (a_participant.name,)
      #print '\t\t\t%s' % (a_participant.attendee_status.value,)
