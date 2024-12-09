import concurrent.futures
import os
import queue
import threading


class ConsumedThreadPool:
    """This class solves a form of the producer-consumer problem.
    There is one main producer, whose items need to be processed in parallel by one of several
    workers, and finally the processed items are consumed by a single consumer thread.

    So the three steps are:

    1. The main thread constructs this object, then iterates and calls submit() for each item,
     passing the appropriate processing function and arguments to submit().
    2. The workers process the items in parallel threads, these are the threads created by the
     ThreadPoolExecutor.
    3. The consumer thread consumes the items, in the form of futures, running the consumer_main
     function originallt passed to the constructor.

    The main producer's loop is meant to be computationally inexpensive, something that generates "tasks".
    The worker threads do the heavy lifting.
    The consumer does something that must happen in a serial manner or otherwise must happen in the
    same, single thread.

    Example:

        def producer_main():
            with ConsumedThreadPool(consumer_main, main_args=('hello',), max_workers=8) as pool:
                for i in range(100):
                    pool.submit(process_fn, userdata='anything', args=(i,))

        def process_fn(i):
            return i * 2

        def consumer_main(greeting, future_iter):
            print(greeting)
            for future in future_iter:
                print(future.userdata)
                print(future.result())
    """
    def __init__(
            self, consumer_main, main_args=None, main_kwargs=None, max_workers=None,
            queue_size=None):
        if max_workers is None:
            max_workers = len(os.sched_getaffinity(0))
        if queue_size is None:
            queue_size = max_workers * 2
        self.q = queue.Queue(queue_size)
        self.semaphore = threading.Semaphore(queue_size)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers)

        self.consumer_error_queue = queue.Queue()
        self.consumer_main = consumer_main

        if main_kwargs is None:
            main_kwargs = {}
        self.consumer_thread = threading.Thread(
            target=self._safe_consumer_main, args=(main_args, main_kwargs))
        self.consumer_thread.start()

    def _safe_consumer_main(self, main_args, main_kwargs):
        try:
            main_kwargs = {**main_kwargs, 'future_iter': IterableQueue(self.q)}
            self.consumer_main(*main_args, **main_kwargs)
        except Exception as e:
            self.consumer_error_queue.put(e)

    def submit(self, fn=None, userdata=None, args=None, kwargs=None):
        if not self.consumer_error_queue.empty():
            consumer_exception = self.consumer_error_queue.get()
            raise RuntimeError('Consumer thread raised an exception') from consumer_exception

        self.semaphore.acquire()
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        if fn is None:
            fn = noop
        future = self.executor.submit(fn, *args, **kwargs)
        future.userdata = userdata
        future.add_done_callback(lambda f: self.semaphore.release())
        future.add_done_callback(self.q.put)

    def close(self):
        self.executor.shutdown(wait=True)
        self.q.put(None)
        self.q.join()
        self.consumer_thread.join()

        if not self.consumer_error_queue.empty():
            consumer_exception = self.consumer_error_queue.get()
            raise RuntimeError('Consumer thread raised an exception') from consumer_exception

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class IterableQueue:
    def __init__(self, q):
        self.q = q

    def __iter__(self):
        while (item := self.q.get()) is not None:
            yield item
            self.q.task_done()
        self.q.task_done()


def noop():
    pass