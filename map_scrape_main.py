from multiprocessing import Process, Manager, Pool, Queue
from itertools import product, cycle
from utils import Scraper, exit_safe
from selenium.webdriver.common.by import By
import random
import time
import os
import sys

def get_neighbors(core, lock, queue):

    scraper = Scraper()
    driver = scraper.driver

    no_to_scrape = -1  # initial setting for the other workers (the 1st worker to get the
                       # initial batch from seed)
    while True:
        print("At core {}: remain queue {}".format(core, queue.qsize()))
        try:
            lock.acquire()  # since it starts with nearly empty queue, the work is not set properly (not quite
                            # understand why, but only 2 procs will be working in this case)
                            # thus we need the lock for this
            s_name, s_url = queue.get()  #
            lock.release()
            # print("core {} get to work".format(core))
        except Queue.Empty:
            if len(to_scrape) == no_to_scrape and len(to_scrape) > 0:  # by setting no_to_scrape to -1 for initial value, will escape
                                                # this condition
                print("core {} get out of work".format(core))
                break   # the universe has been fully explored, stop the iteration
            else:
                no_to_scrape = len(to_scrape)  # snapshot the current value
                continue    # hang until the queue is filled up

        if s_name == 0 and s_url == 0:
            continue    # skip for the initial placeholders

        # get the neighbors
        driver.get(s_url)
        xp = '''//*[(contains(@id, 's') and contains(@class, 'S'))]'''  # the /text()[1] and the /@href will locate the attrs.
        # which would violate the requirements by find_elements_by()
        scraper.wait_until(scraper.visible, By.XPATH, xp)
        time.sleep(3)  # until everything shows up
        nbrs_name = [r.text for r in
                    driver.find_elements(By.XPATH, xp)[1:]]
        nbrs_url = [r.get_attribute("href") for r in
                    driver.find_elements(By.XPATH, xp)[1:]]
        assert(len(nbrs_url) == len(nbrs_name))
        nbrs = list(zip(nbrs_name, nbrs_url))  # zip return the iterator

        # add the neighbors to queue
        for n in nbrs:
            if n not in to_scrape:
                to_scrape.add(n)
                queue.put(n)
        # save the site and its neighbor info
        with open("log/{}_edgelist.tsv".format(core), 'a+') as f:
            f.write("{}".format(s_name))
            for n_name, _ in nbrs:
                f.write("\t{}".format(n_name))
            f.write("\n")

        # print("finished 1 here")

    exit_safe(driver)

    # todo: timeout error by chrome


def run(n):
    queue = Manager().Queue()  # original Queue() not working (?) "share between process error"
    pool = Pool(n)
    l = Manager().Lock()  # global lock for the set record

    # res = [pool.apply_async(worker_main, [core, param_cycle, queue]) for core in range(cores)]
    # [r.get() for r in res]
    queue.put(("Haruki Murakami", "https://www.literature-map.com/haruki+murakami"))
    for i in range(n-1):
        queue.put((0, 0))   # this to let map allocate the correct number of workers
    pool.starmap(get_neighbors, product(range(n), [l], [queue]))  # map would perform async
    pool.close()
    pool.join()

    pass



if __name__ == "__main__":
    to_scrape = set()

    run(n=5)