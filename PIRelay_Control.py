#!/usr/bin/python

import sys
import time
import RPi.GPIO as GPIO
import Queue
import threading
import time
import MySQLdb
from ConfigParser import SafeConfigParser

GPIO.setmode(GPIO.BOARD)
GPIO.setwarnings(False)
GPIO.setup(3, GPIO.OUT)
GPIO.setup(5, GPIO.OUT)
GPIO.setup(7, GPIO.OUT)
GPIO.setup(11, GPIO.OUT)
GPIO.setup(13, GPIO.OUT)
GPIO.setup(15, GPIO.OUT)
GPIO.setup(18, GPIO.OUT)
GPIO.setup(19, GPIO.OUT)
GPIO.setup(21, GPIO.OUT)
GPIO.setup(22, GPIO.OUT)
GPIO.setup(23, GPIO.OUT)
GPIO.setup(26, GPIO.OUT)

parser = SafeConfigParser()
parser.read('irrigation.ini')

ip_address = parser.get('DB_Con_Info','ip')
db_user = parser.get('DB_Con_Info','username')
db_pass = parser.get('DB_Con_Info','password')
db_database = parser.get('DB_Con_Info','database')

exitFlag = 0

class myThread (threading.Thread):
	def __init__(self, threadID, name, q):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.name = name
		self.q = q


	def run(self):
		print "Starting " + self.name
		process_data(self.name, self.q) #grab event_id from queue
		#grab info from db using event_id
		#wait until event start time
		#open relays for event duration

		print "Exiting " + self.name

def process_data(threadName, q):
	while not exitFlag:
		queueLock.acquire()
		if not workQueue.empty():
			data = q.get()
			queueLock.release()
			time.sleep(data)
			#GPIO.output(data,False)
			print "%s opening relay %s" % (threadName, data)
			time.sleep(1)
			#GPIO.output(data,True)
			print "%s closing relay %s" % (threadName, data)
		else:
			queueLock.release()
		#time.sleep(3)

#def activate_area(threadname,q,area,start_time,end_time):
	#find duration based on start_time and end_time from db
	#make a list from ini file to grab which zones are in area
	#for each zone in list
		#open zone relay for percentage of duration listed in ini file


#def db_grab_info():
	#use sql command to grab area,start_time,end_time
	#return info to thread

def get_todays_events():
	localtime = time.localtime(time.time())
	current_day = '%s-%s-%s' % (localtime.tm_year,localtime.tm_mon,localtime.tm_mday)
	db = MySQLdb.connect(ip_address,db_user,db_pass,db_database) # Open database connection
	cursor = db.cursor() # prepare a cursor object using cursor() method
	sql = 'SELECT * FROM Irrigation WHERE Start_Time LIKE "%%%s%%"' % current_day
	try:
		cursor.execute(sql)
		results = cursor.fetchall()
		eventlist=[]
		print eventlist
		for row in results:
			print row[0]
			eventID = row[0]
			eventlist.append(row[0])
	except:
		print 'fetched nothing'


	db.close()

	for event in eventlist:
		print event
	return eventlist



relaylist = [3,5,7,11,13,15,18,19,21,22,23,26]
threadList = ["Thread-1", "Thread-2", "Thread-3","Thread-4", "Thread-5", "Thread-6"]
nameList = ["One", "Two", "Three", "Four", "Five"]
queueLock = threading.Lock()
workQueue = Queue.Queue(15)
threads = []
threadID = 1

# Create new threads
for tName in threadList:
	thread = myThread(threadID, tName, workQueue)
	thread.start()
	threads.append(thread)
	threadID += 1

var = 1
while var==1: # not workQueue.empty():
	print 'Filling Q'
	eventlist=get_todays_events()
	for event in eventlist:
		queueLock.acquire()
		try:
			workQueue.put(event)
			print 'added %s to q' % (event)
			time.sleep(1)
		except:
			print 'queue full'
		queueLock.release()

	time.sleep(120)
	#grab a list of the event_id for all the irrigation events for the next 24 hours that havent been processed (in chronological order)
	# for loop
		#add each event_id to the queue
		#mark each event as processed

	#sleep until time to recheck for new events

# Notify threads it's time to exit
exitFlag = 1

# Wait for all threads to complete

for t in threads:
	t.join()

print "Exiting Main Thread"
