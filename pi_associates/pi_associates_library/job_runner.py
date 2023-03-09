import asyncio
from multiprocessing import Process
from threading import Thread


class JobRunner:
    def __init__(self):
        self.jobs = {}

    def create_job(self, job_name, target, args):
        pass

    def run_all(self):
        pass

    def wait_all(self):
        pass

    def run_until_complete(self):
        self.run_all()
        self.wait_all()


class ProcessJobRunner(JobRunner):
    def create_job(self, job_name, target, args):
        self.jobs[job_name] = Process(target=target, args=args)

    def run_all(self):
        [process.start() for _, process in self.jobs.items()]

    def wait_all(self):
        [process.join() for _, process in self.jobs.items()]


class ThreadJobRunner(JobRunner):
    def create_job(self, job_name, target, args):
        self.jobs[job_name] = Thread(target=target, args=args)
        self.jobs[job_name].setDaemon(True)

    def run_all(self):
        [thread.start() for _, thread in self.jobs.items()]

    def wait_all(self):
        [thread.join() for _, thread in self.jobs.items()]

#
# class CoroutinJobRunner(JobRunner):
#     def create_job(self, job_name, target, args):
#         self.jobs[job_name] = (target, args)
#         self.coroutins = {}
#
#     def run_all(self):
#         a = [asyncio.get_event_loop().run_in_executor(None, target, args)
#          for target, args in self.jobs.items()]
#         for i in a:
#             asyncio.wait_for(i)
#
#     def wait_all(self):
#         asyncio.w
