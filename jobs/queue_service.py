import threading
import queue
import time

from analysis.social.louvain_graphing.louvain_analysis import do_persist_analysis
import logging as log

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

            try:
                job = self.queue.get()
                if job is None:
                    log.info(f"No job. Sleeping.")
                    time.sleep(5)
                    continue
                self.process_job(job)
            except Exception as e:
                log.error(f"Error processing job: {e}")

    def enqueue(self, job):
        """Adds a job to the queue."""
        self.queue.put(job)
        log.info(f"Job entered into queue")

    def process_job(self, job):
        """Process the job (replace this with your actual task)."""
        log.info(f"Processing job: {job.type}")
        if job.type == "louvain":
            do_persist_analysis(job)
        log.info(f"Job completed: {job.type}")

# Ensure a single instance is created
queue_service = QueueService()