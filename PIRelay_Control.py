#!/usr/bin/python

import sys
import time
import RPi.GPIO as GPIO
import Queue
import threading
import time

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
			GPIO.output(data,False)
			print "%s opening relay %s" % (threadName, data)
			time.sleep(1)
			GPIO.output(data,True)
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

# Fill the queue
#fix full queue break
#print 'Filling Q'
#for relay in relaylist:
#	queueLock.acquire()
#	try:
#		workQueue.put(relay)
#		print 'added %s to q' % (relay)
#		time.sleep(1)
#	except:
#		print 'queue full'
#	queueLock.release()


#time.sleep(10)

#while not workQueue.empty():
#	pass


while var==1: # not workQueue.empty():
	print 'Filling Q'
	for relay in relaylist:
		queueLock.acquire()
		try:
			workQueue.put(relay)
			print 'added %s to q' % (relay)
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
