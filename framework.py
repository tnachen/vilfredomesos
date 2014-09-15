#!/usr/bin/env python

import os
import sys
import threading
import signal
import time
import collections

try:
    from mesos.native import MesosExecutorDriver, MesosSchedulerDriver
    from mesos.interface import Executor, Scheduler
    from mesos.interface import mesos_pb2
except ImportError:
    from mesos import Executor, MesosExecutorDriver, MesosSchedulerDriver, Scheduler
    import mesos_pb2

import task_state


TASK_CPUS = 0.5
TASK_MEM = 32
LEADING_ZEROS_COUNT = 5  # appended to task ID to facilitate lexicographical order
EXECUTOR_COUNT = 24  # number of executors in this framework
TASK_SEPARATOR = "@"


class VilfredoMesosScheduler(Scheduler):
    def __init__(self, paretoExecutor):
        self.paretoExecutor = paretoExecutor
        self.tasksCreated = 0
        self.tasksFailed = 0
        self.tasksKilled = 0
        self.tasksLost = 0
        self.tasksFinished = 0
        self.messagesReceived = 0
        self.messagesRunningReceived = 0
        self.freeExecutors = collections.deque()
        self.busyExecutors = {}
    
        # Create EXECUTOR_COUNT executors.
        for idx in range(EXECUTOR_COUNT):
            e = mesos_pb2.ExecutorInfo()
            e.CopyFrom(paretoExecutor)
            e.executor_id.value += "-{}".format(idx)
            self.freeExecutors.append(e)
        
    
    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID [{}]".format(frameworkId.value)
    
    def makeTaskPrototype(self, offer):
        task = mesos_pb2.TaskInfo()
        tid = self.tasksCreated
        self.tasksCreated += 1
        task.task_id.value = str(tid).zfill(LEADING_ZEROS_COUNT)
        task.slave_id.value = offer.slave_id.value
        
        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = TASK_CPUS
        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = TASK_MEM
        
        return task
    
    def printExecutorsStatus(self):
        print "Executors: {} free, {} busy".format(
            len(self.freeExecutors), len(self.busyExecutors))

    def printTasksStatus(self):
        print "Tasks: {} created, {} finished ({} failed, {} lost, {} killed)".format(
            self.tasksCreated, self.tasksFinished, self.tasksFailed,
            self.tasksLost, self.tasksKilled)
    
    def makeParetoTask(self, offer):
        if (not self.freeExecutors):
            raise Exception("Cannot create a task: no free executors")
        task = self.makeTaskPrototype(offer)
        task.name = "Pareto task {}".format(task.task_id.value)
        
        # Take a free exeutor and mark it as busy.
        # TODO(alex): don't move executor refs around, use indices instead.
        e = self.freeExecutors.popleft()
        self.busyExecutors[e.executor_id.value] = e
        task.task_id.value += " " + TASK_SEPARATOR + " " + e.executor_id.value
        
        task.executor.MergeFrom(e)
        return task
    
    def maxTasksForOffer(self, offer):
        count = 0
        cpus = next(rsc.scalar.value for rsc in offer.resources if rsc.name == "cpus")
        mem = next(rsc.scalar.value for rsc in offer.resources if rsc.name == "mem")
        while cpus >= TASK_CPUS and mem >= TASK_MEM:
            count += 1
            cpus -= TASK_CPUS
            mem -= TASK_MEM
        return count
    
    def resourceOffers(self, driver, offers):
        for offer in offers:
            maxTasks = self.maxTasksForOffer(offer)
            tasks = []
            
            for i in range(maxTasks):
                # If we have no free executors, go to scheduling.
                if (self.freeExecutors):
                    task = self.makeParetoTask(offer)
                    tasks.append(task)
                else:
                    break

            if (tasks):
                driver.launchTasks(offer.id, tasks)
            else:
                driver.declineOffer(offer.id)
    
    def statusUpdate(self, driver, update):
        self.messagesReceived += 1
        stateName = task_state.decode[update.state]
        print "Task [{}] is in state [{}]".format(update.task_id.value, stateName)
        
        if update.state == mesos_pb2.TASK_RUNNING:
            self.messagesRunningReceived += 1
        elif update.state == mesos_pb2.TASK_FAILED:
            self.tasksFailed +=1
        elif update.state == mesos_pb2.TASK_KILLED:
            self.tasksKilled +=1
        elif update.state == mesos_pb2.TASK_LOST:
            self.tasksLost +=1

        if update.state > 1: # Terminal state
            self.tasksFinished += 1

            # Release the corresponding executor.
            e_id = update.task_id.value.split(TASK_SEPARATOR)[-1].strip()
            if (e_id in self.busyExecutors):
                e = self.busyExecutors[e_id]
                self.freeExecutors.append(e)
                del self.busyExecutors[e_id]


def hard_shutdown(signal, frame):
    print "Shutting down..."
    vilfredo.printExecutorsStatus()
    vilfredo.printTasksStatus()
    driver.stop()


#
# Execution entry point:
#
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: {} master_ip:port".format(sys.argv[0])
        sys.exit(1)
    
    baseURI = os.path.dirname(os.path.abspath(__file__))
    uris = [ "pareto_executor.py", "task_state.py" ]
    uris = [os.path.join(baseURI, uri) for uri in uris]
    
    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""
    framework.name = "VilfredoMesos"

    paretoExecutor = mesos_pb2.ExecutorInfo()
    paretoExecutor.executor_id.value = "pareto-executor"
    paretoExecutor.name = "Pareto simulator"
    paretoExecutor.command.value = "python pareto_executor.py"
    for uri in uris:
        uri_proto = paretoExecutor.command.uris.add()
        uri_proto.value = uri
        uri_proto.extract = False

    vilfredo = VilfredoMesosScheduler(paretoExecutor)
    
    driver = MesosSchedulerDriver(vilfredo, framework, sys.argv[1])

    # driver.run() blocks; we run it in a separate thread
    def run_driver_async():
        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        driver.stop()
        sys.exit(status)
    framework_thread = threading.Thread(target = run_driver_async)
    framework_thread.start()

    print "(Listening for Ctrl-C)"
    signal.signal(signal.SIGINT, hard_shutdown)
    while framework_thread.is_alive():
        time.sleep(1)

    sys.exit(0)
