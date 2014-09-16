#!/usr/bin/env python

import sys
import os
import datetime
import time
import threading
import random
import collections
import hashlib

try:
    from mesos.native import MesosExecutorDriver, MesosSchedulerDriver
    from mesos.interface import Executor, Scheduler
    from mesos.interface import mesos_pb2
except ImportError:
    from mesos import Executor, MesosExecutorDriver, MesosSchedulerDriver, Scheduler
    import mesos_pb2


UPDATES_COUNT = 100  # number of updates per task life
RANDOM_BYTES_COUNT = 512  # size of chunk of random bytes read

# Pareto distribution parameters
PARETO_SHAPE = 2.0
PARETO_SCALE = 10.0  # in minutes


class ParetoExecutor(Executor):
    def __init__(self):
        self.lock = threading.Lock()
        self.tasksRunning = False
        random.seed()
    
    def paretoInverse(self, shape, scale, uniformSaple):
        return scale / pow(1.0 - uniformSaple, 1.0 / shape)
    
    def nextSample(self):
        r = random.random()
        return self.paretoInverse(PARETO_SHAPE, PARETO_SCALE, r)
    
    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
        self.id = executorInfo.executor_id.value;
        print "{} registered".format(self.id)

    def reregistered(self, driver, slaveInfo):
        print "{} reregistered".format(self.id)

    def disconnected(self, driver):
        print "{} disconnected".format(self.id)

    def launchTask(self, driver, task):
        def run_task():
            with self.lock:
                self.tasksRunning = True
            
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)
            
            # Determine task duration.
            taskDuration = self.nextSample()
            print "Task duration will be {} min".format(taskDuration)
            taskDuration *= 60.0  # convert duration to seconds
            
            # Determine the period between sending updates.
            periodDuration = taskDuration / UPDATES_COUNT

            # Simulate running task with sending regular updates.
            taskStarted = datetime.datetime.now()
            while (taskDuration > (datetime.datetime.now() -
                taskStarted).total_seconds()):
                
                # Period between updates.
                msg = ""
                periodStarted = datetime.datetime.now()
                while (periodDuration > (datetime.datetime.now() -
                    periodStarted).total_seconds()):

                    # Generate some CPU load.
                    token = hashlib.sha512()
                    token.update(os.urandom(RANDOM_BYTES_COUNT))
                    msg = "".join([token.hexdigest()] * 4)

                # Send a status update to the scheduler.
                update = mesos_pb2.TaskStatus()
                update.task_id.value = task.task_id.value
                update.state = mesos_pb2.TASK_RUNNING
                update.data = msg
                driver.sendStatusUpdate(update)
        
            # Mark executor as free after finishing the task but before
            # sending the update.
            with self.lock:
                self.tasksRunning = False
            
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED
            driver.sendStatusUpdate(update)


        with self.lock:
            is_busy = self.tasksRunning
                            
        if (is_busy == False):
            thread = threading.Thread(target=run_task)
            thread.start();
        else:
            print "There is already a task running, discarding the request"
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_LOST
            driver.sendStatusUpdate(update)


    def killTask(self, driver, taskId):
        shutdown(self, driver)

    def frameworkMessage(self, driver, message):
        print "Ignoring framework message: {}".format(message)

    def shutdown(self, driver):
        print "Shutting down"

    def error(self, error, message):
        print "Error: {}".format(message)


#
# Execution entry point:
#
if __name__ == "__main__":
    print "Starting Pareto Executor"
    driver = MesosExecutorDriver(ParetoExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
