import time
import os
import sys
import random
import math
from multiprocessing import Process, Pool, Lock
import multiprocessing

def doWork(N, pid, lock):
    """ Do something time consuming and locking at the end """

    startTime = time.time()
    result = 0 
    print "--> Process:", pid," ospid:",os.getpid(), " START"
    # Do something time consuming
    for i in range(N):
       result += 1*(math.log(3.14**(1.2)/14.3**(0.9))/math.exp(0.314**(1.314)/1.13))
    # when finished do something locking(=> other processes wait)
    lock.acquire()
    print "--> Process:", pid," ospid:",os.getpid(), " LOCKING"
    time.sleep(1)
    print "--> Process:", pid," ospid:",os.getpid(), " N=", N, " time:", time.time()-startTime, " END"
    lock.release()
    return result

if __name__ == '__main__':
    ncpus = multiprocessing.cpu_count()
    print "Available number of procs :", ncpus 
    nprocesses = int(sys.argv[1])
    print "Number of work processes  :", nprocesses

    lock = Lock()

    #mark the start time
    startTime = time.time()
     
    #create a process Pool with N processes
    pool = Pool(processes=nprocesses)

    print "---- Prepare all the jobs ----"
    jobs = []
    for p in range(nprocesses):
         job = Process(target=doWork, args=(int(random.random()), p, lock))
         jobs.append(job)

    print "---- Launch jobs ----"
    for job in jobs:
         job.start()

    # Block master until all jobs are done
    for job in jobs:
        job.join() # Block the calling thread (here master) until the job is finished
             
    #mark the end time
    endTime = time.time()
    #calculate the total time it took to complete the work
    workTime =  endTime - startTime
     
    #print results
    print "---- Master ----"
    print "The job took " + str(workTime) + " seconds to complete"
