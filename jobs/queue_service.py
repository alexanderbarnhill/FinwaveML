import threading
import queue
import time
from typing import cast
from analysis.social.louvain_graphing.louvain_analysis import do_persist_analysis
from jobs.job import Job
from jobs.models.louvain import LouvainJob
from utilities.data_descriptor import singleton


class QueueService:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance.queue = queue.Queue()
                cls._instance._start_worker()
        return cls._instance

    def _start_worker(self):
        """Starts a background worker thread to process the queue."""
        worker_thread = threading.Thread(target=self._worker, daemon=True)
        worker_thread.start()

    def _worker(self):
        """Continuously processes jobs in the queue."""
        while True:
            job = self.queue.get()
            if job is None:
                break  # Stop signal
            self.process_job(job)
            # try:
            #
            # except Exception as e:
            #     print(f"Error processing job: {e}")

    def enqueue(self, job):
        """Adds a job to the queue."""
        self.queue.put(job)

    def process_job(self, job):
        """Process the job (replace this with your actual task)."""
        print(f"Processing job: {job.type}")
        if job.type == "louvain":
            do_persist_analysis(job)
        print(f"Job completed: {job.type}")

# Ensure a single instance is created
queue_service = QueueService()