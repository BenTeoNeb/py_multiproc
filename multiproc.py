"""
Experimenting with multiprocessing in python
"""

import time
import os
import sys
import random
import math
from Queue import Queue
from multiprocessing import Process, Pool, Lock
import multiprocessing


def hello_world(pid):
    print "hello world from pid: ", pid
    sleep(1)
    print " slept well from pid: ", pid 

def do_work(complexity, pid, lock):
    """ Do something time consuming and locking at the end """

    start_time = time.time()
    result = 0
    print "--> Process:", pid, " ospid:", os.getpid(), " START"
    # Do something time consuming
    time.sleep(complexity*2)
    # when finished do something locking
    # This means that only one worker can have the lock at the same time
    lock.acquire()
    print "--> Process:", pid, " ospid:", os.getpid(), " LOCKING"
    time.sleep(0.1)
    print "--> Process:", pid, " ospid:", os.getpid(), " time:", time.time() - start_time, " END"
    lock.release()


def multijobs(nprocesses):
    """ Do some parallel work by submitting
    all the jobs at the same time, with no
    regard to the number of cores availables.
    """

    lock = Lock()

    # mark the start time
    start_time = time.time()

    # create a process Pool with N processes
    #pool = Pool(processes=nprocesses)

    print "---- Prepare all the jobs ----"
    # Put all the jobs to be done in a queue
    jobs_q = Queue()
    jobs = []
    for process in range(nprocesses):
        job = Process(target=do_work, args=(random.random(), process, lock))
        jobs_q.put(job)
        jobs.append(job)

    # Batch mode
    print "---- Launch " + str(nprocesses) +" jobs ----"
    jobs_started_q = Queue()
    # Go as long as there are jobs to do
    while not jobs_q.empty():
        job = jobs_q.get()
        job.start()
        jobs_started_q.put(job)

    # Block master until all started jobs are done
    while not jobs_started_q.empty():
        job = jobs_started_q.get()
        job.join()  # join: Block the calling thread (here master) until the job is finished

    # mark the end time
    end_time = time.time()
    # calculate the total time it took to complete the work
    work_time = end_time - start_time

    # print some infos
    print "---- Master ----"
    print "The jobs took " + str(work_time) + " seconds to complete"


def multijobs_batch(nprocesses, batch_size):
    """ Do some parallel work by submitting
    a batch of jobs, waiting for this batch
    to finish, submitting another batch and
    so on until there are no more jobs to
    perform.
    So in this case the number of simultaneous
    jobs is between 1 and the batch size.
    """

    lock = Lock()

    # mark the start time
    start_time = time.time()

    # create a process Pool with N processes
    #pool = Pool(processes=nprocesses)

    print "---- Prepare all the jobs ----"
    # Put all the jobs to be done in a queue
    jobs_q = Queue()
    jobs = []
    for process in range(nprocesses):
        job = Process(target=do_work, args=(random.random(), process, lock))
        jobs_q.put(job)
        jobs.append(job)

    # Batch mode
    print "---- Launch " + str(nprocesses) +" jobs ----"
    jobs_started_q = Queue()
    # Go as long as there are jobs to do
    while not jobs_q.empty():

        print " --> Launching a batch of max " + str(batch_size) + " jobs"
        # Launch jobs by batch and put launched jobs in another queue
        while not jobs_q.empty() and jobs_started_q.qsize() != batch_size:
            job = jobs_q.get()
            job.start()
            jobs_started_q.put(job)

        print " --> Launched " + str(jobs_started_q.qsize()) + " jobs"
        # Block master until all started jobs are done
        while not jobs_started_q.empty():
            job = jobs_started_q.get()
            job.join()  # join: Block the calling thread (here master) until the job is finished

    # mark the end time
    end_time = time.time()
    # calculate the total time it took to complete the work
    work_time = end_time - start_time

    # print some infos
    print "---- Master ----"
    print "The jobs took " + str(work_time) + " seconds to complete"

def multijobs_pool(nprocesses, ncpus):
    """ Do some parallel work by submitting
    jobs with a max number of simultaneous
    jobs always set to the number of cpus
    until there are no more jobs to perform.
    """

    lock = Lock()

    # mark the start time
    start_time = time.time()

    # create a process Pool with N processes
    pool = Pool(processes=ncpus)

    print "---- Pool mode ----"
    for process in range(nprocesses):
        print "launching pid:", process
        # J AI PAS COMPRIS
        pool.apply_async(hello_world, args=(process,))

    print "---- close pool ----"
    pool.close()
    print "---- blocking ----"
    pool.join()

    # mark the end time
    end_time = time.time()
    # calculate the total time it took to complete the work
    work_time = end_time - start_time

    # print some infos
    print "---- Master ----"
    print "The jobs took " + str(work_time) + " seconds to complete"

if __name__ == '__main__':
    NCPUS = multiprocessing.cpu_count()
    print "Available number of procs :", NCPUS
    NPROCESSES = 10

    print "############################################"
    print "All jobs started at the same time. BANZAAAAI"
    print "############################################"
    multijobs(NPROCESSES)
    print "############################################"
    print "Start jobs by batches. Be reasonable."
    print "############################################"
    multijobs_batch(NPROCESSES, batch_size=NCPUS)
    print "############################################"
    print " WTF with pools"
    print "############################################"
    multijobs_pool(NPROCESSES, ncpus=NCPUS)