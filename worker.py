import Queue
import threading

work_queue = Queue.Queue()

current_workers = 0
current_workers_lock = threading.Lock()
total_errors = 0
total_errors_lock = threading.Lock()

worker_max_threads = 0
worker_logger = None


def do_work():
    global current_workers, total_errors
    worker_logger.info('starting thread')
    current_workers_lock.acquire()
    current_workers += 1
    current_workers_lock.release()
    while True:
        try:
            function, args = work_queue.get(False)
        except Queue.Empty:
            break
        try:
            function(*args)
        except:
            worker_logger.exception("error while running working... going to get next job")
            total_errors_lock.acquire()
            total_errors += 1
            total_errors_lock.release()
    current_workers_lock.acquire()
    current_workers -= 1
    current_workers_lock.release()
    worker_logger.info('ending thread')

# run_in_worker is the main entry point. If there is a free thread, use it,
# if not, queue it

def run_in_worker(function, *args):
    worker_logger.info('queueing %s to run in worker' % function)
    current_workers_lock.acquire()
    work_queue.put((function, args))
    if current_workers < worker_max_threads:
        thread = threading.Thread(target=do_work)
        thread.start()
    current_workers_lock.release()
    return



# setup stores various things and needs to be done first.  We could
# do this as a class, but it is just a singleton

def setup(max_threads, logger):
    global worker_max_threads, worker_logger
    worker_max_threads = max_threads
    worker_logger = logger
