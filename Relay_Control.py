#!/usr/bin/python
import Queue
import threading
import time

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
			print "%s processing %s" % (threadName, data)
			time.sleep(2)
			print "Done Processing"
		else:
			queueLock.release()
		time.sleep(3)

#def activate_area(threadname,q,area,start_time,end_time):
	#find duration based on start_time and end_time from db
	#make a list from ini file to grab which zones are in area
	#for each zone in list
		#open zone relay for percentage of duration listed in ini file


#def db_grab_info():
	#use sql command to grab area,start_time,end_time
	#return info to thread

threadList = ["Thread-1", "Thread-2", "Thread-3","Thread-4", "Thread-5", "Thread-6"]
nameList = ["One", "Two", "Three", "Four", "Five"]
queueLock = threading.Lock()
workQueue = Queue.Queue(10)
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
print 'Filling Q'

time.sleep(1)


for word in nameList:
	queueLock.acquire()
	try:
		workQueue.put(word)
		print 'added %s to q' % (word)
		time.sleep(1)
	except:
		print 'queue full'
	queueLock.release()


time.sleep(10)


for word in nameList:
	queueLock.acquire()
	workQueue.put(word)
	queueLock.release()

# Wait for queue to empty
while not workQueue.empty():
	pass


#while var==1: # not workQueue.empty():
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
