"""
Experimenting with multiprocessing in python
"""

import time
import os
import sys
import random
import math
from Queue import Queue
from multiprocessing import Process, Pool, Lock, JoinableQueue
import multiprocessing


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

def run_all(jobs_q):
    """ Do some parallel work by submitting
    all the jobs at the same time, with no
    regard to the number of cores availables.
    """

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

def run_batch(jobs_q, batch_size=multiprocessing.cpu_count()):
    """ Do some parallel work by submitting
    a batch of jobs, waiting for this batch
    to finish, submitting another batch and
    so on until there are no more jobs to
    perform.
    So in this case the number of simultaneous
    jobs is between 1 and the batch size.
    INPUTS:
        - jobs: queue of processes
        - batch_size, optional: max number of alive processes
                                default to number of cpus
    """
    # Batch mode
    jobs_started_q = Queue()
    # Go as long as there are jobs to do
    while not jobs_q.empty():

        # Launch jobs by batch and put launched jobs in another queue
        while not jobs_q.empty() and jobs_started_q.qsize() != batch_size:
            job = jobs_q.get()
            job.start()
            jobs_started_q.put(job)

        # Block master until all started jobs are done
        while not jobs_started_q.empty():
            job = jobs_started_q.get()
            job.join()  # join: Block the calling thread (here master) until the job is finished

def run_full_batch(jobs_q, batch_size=multiprocessing.cpu_count()):
    """ Do some parallel work by submitting
    a batch of jobs, and when one job is finished
    submit another so that the batch of jobs is
    always full.
    So in this case the number of
    jobs is always the batch size.
    INPUTS:
        - jobs: queue of processes
        - batch_size, optional: max number of alive processes
                                default to number of cpus
    """
    jobs_alive = {}
    all_jobs_done = False
    # Go as long as there are jobs to do
    while not all_jobs_done:

        # Check if some started jobs are finished
        done_jobs = []
        for pid, jobs in jobs_alive.iteritems():
            if not jobs.is_alive():
                done_jobs.append(pid)
        for done_job in done_jobs:
            jobs_alive.pop(done_job)

        # Submit one job if the number of alive jobs is
        # below the limit of batch size
        # and if there are still jobs to submit.
        if len(jobs_alive) < batch_size and not jobs_q.empty():
            job = jobs_q.get()
            job.start()
            jobs_alive[job.pid] = job

        # Everything is finished if all jobs have been
        # submitted and no job is alive.
        if jobs_q.empty() and len(jobs_alive) == 0:
            all_jobs_done = True

        time.sleep(0.01)

def multijobs(nprocesses, batch_size):
    """ Do some parallel work by submitting
    a batch of jobs, and when one job is finished
    submit another so that the batch of jobs is
    always full.
    So in this case the number of
    jobs is always the batch size.
    """

    lock = Lock()

    # mark the start time
    start_time = time.time()

    print "---- Prepare all the jobs ----"
    # Put all the jobs to be done in a queue
    jobs_q = Queue()
    for process in range(nprocesses):
        job = Process(target=do_work, args=(random.random(), process, lock))
        jobs_q.put(job)

    #run_all(jobs_q)
    #run_batch(jobs_q)
    run_full_batch(jobs_q)

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
    multijobs(NPROCESSES, batch_size=NCPUS)
