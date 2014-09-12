#!/usr/bin/env python

import sys
import os
import threading
import time

try:
    from mesos.native import MesosExecutorDriver, MesosSchedulerDriver
    from mesos.interface import Executor, Scheduler
    from mesos.interface import mesos_pb2
except ImportError:
    from mesos import Executor, MesosExecutorDriver, MesosSchedulerDriver, Scheduler
    import mesos_pb2


class ParetoExecutor(Executor):
    def __init__(self):
        self.name = "ParetoExecutor"
        self.lock = threading.Lock()
        self.tasksRunning = 0;
    
    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
        print "{} registered".format(self.name)

    def reregistered(self, driver, slaveInfo):
        print "{} reregistered".format(self.name)

    def disconnected(self, driver):
        print "{} disconnected".format(self.name)

    def launchTask(self, driver, task):
        def run_task():
            with lock:
                self.tasksRunning += 1
            
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)
            
            # TODO(alex): choose task duration according to pareto distribution.
            # TODO(alex): generate CPU load.
            time.sleep(10)
        
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED
            driver.sendStatusUpdate(update)
        
            with lock:
                self.tasksRunning -= 1

        thread = threading.Thread(target=run_task)
        thread.start();
        with lock:
            tasksQ = self.tasksRunning
        print "{} tasks are currently running".format(tasksQ)

    def killTask(self, driver, taskId):
        shutdown(self, driver)

    def frameworkMessage(self, driver, message):
        print "Ignoring framework message: {}".format(message)

    def shutdown(self, driver):
        print "Shutting down"
        sys.exit(0)

    def error(self, error, message):
        print "Error: {}".format(message)


#
# Execution entry point:
#
if __name__ == "__main__":
    print "Starting Pareto Executor"
    driver = MesosExecutorDriver(ParetoExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
