#!/usr/bin/env python

import sys
import os
import time
import threading
import random
import collections

try:
    from mesos.native import MesosExecutorDriver, MesosSchedulerDriver
    from mesos.interface import Executor, Scheduler
    from mesos.interface import mesos_pb2
except ImportError:
    from mesos import Executor, MesosExecutorDriver, MesosSchedulerDriver, Scheduler
    import mesos_pb2


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
            
            # TODO(alex): generate CPU load.
            duration = self.nextSample()
            print "Task duration will be {} min".format(duration)
            time.sleep(duration)
        
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
