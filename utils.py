from bs4 import BeautifulSoup
import requests  # request pkg is a more human-friendly request handler alternative for urllib pkg
import re
import time
import pandas as pd
from multiprocessing import Process, Manager, Pool, Queue
from itertools import product, cycle
import random
import sys
import lxml
from lxml.html import fromstring
import signal
from contextlib import contextmanager
import selenium
from selenium import webdriver
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
# from webdriver_manager.chrome import ChromeDriverManager
import networkx as nx
import time
import datetime
from datetime import datetime
import copy
import numpy as np
import pickle as pkl
import functools
import os
import glob
from collections import Counter

root = "./"

class Scraper:
    def __init__(self, browser="chrome"):

        if browser == "chrome":
            # add headless option
            options = webdriver.ChromeOptions()
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--incognito')  # using incognito
            options.add_argument('--headless')  # mute for debug
            options.add_argument("--mute-audio")
            tmp_driver = webdriver.Chrome(
                # desired_capabilities=capabilities,
                executable_path=root + 'chromedriver',
                options=options)
        elif browser == "firefox":
            options = webdriver.FirefoxOptions()
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--incognito')  # using incognito
            options.add_argument('--headless')  # mute for debug
            options.add_argument("--mute-audio")
            tmp_driver = webdriver.Chrome(
                # desired_capabilities=capabilities,
                executable_path=root + 'geckodriver',
                options=options)
        else:
            print("Not implemented!")
            return

        proxies = list(get_proxies(tmp_driver))  #
        tmp_driver.quit()

        random.shuffle(proxies)
        proxies = cycle(proxies)
        connected = 0
        remain_attempts = 100
        while (not connected) and remain_attempts:
            PROXY = next(proxies)
            remain_attempts -= 1
            prox = Proxy()
            prox.proxy_type = ProxyType.MANUAL
            prox.http_proxy = PROXY
            prox.socks_proxy = PROXY
            prox.ssl_proxy = PROXY
            # capabilities = webdriver.DesiredCapabilities.CHROME
            # prox.add_to_capabilities(capabilities)
            options.Proxy = PROXY
            if browser == "chrome":
                # add headless option
                self.driver = webdriver.Chrome(
                    # desired_capabilities=capabilities,
                    executable_path=root + 'chromedriver',
                    options=options)
            elif browser == "firefox":
                self.driver = webdriver.Chrome(
                    # desired_capabilities=capabilities,
                    executable_path=root + 'geckodriver',
                    options=options)
            else:
                print("Not implemented!")
                return

            # self.driver = webdriver.Chrome(ChromeDriverManager().install(),
            #                                options=options)  # only available for python > 3.6
            # mix3 connection
            try:
                self.driver.get("https://www.google.com")
            except:
                continue
            else:
                # print("proxy found")
                connected = 1
        if connected == 0:
            raise ConnectionError("No available proxy. Check internet connection")

        self.wait = WebDriverWait(self.driver, 10)
        self.presence = EC.presence_of_element_located
        self.visible = EC.visibility_of_element_located
        self.clickable = EC.element_to_be_clickable

    def refresh_until_success(self, action, by, indicator):
        remain_retries = 5
        while remain_retries:
            try:
                self.wait.until(action((by, indicator)))
            except selenium.common.exceptions.TimeoutException:
                self.driver.refresh()
                remain_retries -= 1
            else:
                return -1

        return 0

    def wait_until(self, action, by, indicator):
        try:
            self.wait.until(action((by, indicator)))
        except:
            return -1

        return 0

def exit_safe(driver):
    '''
    for Chrome, bug
    :return:
    '''

    driver.close()
    driver.quit()
    # delete /tmp folders, needed
    try:
        os.system("rm -r /tmp/.com.google.Chrome*")
    except:
        print("/tmp deletion failed")

### decorators
def timer(func):
    """Print the runtime of the decorated function"""
    @functools.wraps(func) # keep func's identity, https://realpython.com/primer-on-python-decorators/
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()    # 1
        value = func(*args, **kwargs)
        end_time = time.perf_counter()      # 2
        run_time = end_time - start_time    # 3
        print("Finished {} in {:.4f} secs".format(repr(func.__name__), run_time),
              file=open(root + "logs/runningTime{}".format(
                                            random.choice(range(10))), 'a+'))
        return value
    return wrapper_timer

@contextmanager
def timeout(time):
    # Register a function to raise a TimeoutError on the signal.
    signal.signal(signal.SIGALRM, raise_timeout)
    # Schedule the signal to be sent after ``time``.
    signal.alarm(time)

    try:
        yield
    except TimeoutError:
        pass
    finally:
        # Unregister the signal so it won't be triggered
        # if the timeout is not reached.
        signal.signal(signal.SIGALRM, signal.SIG_IGN)


def raise_timeout(signum, frame):
    raise TimeoutError


## scraper utilities
def get_proxies(driver):
    """
    debugged

    https://www.scrapehero.com/how-to-rotate-proxies-and-ip-addresses-using-python-3/
    :return:  a list of available proxies
    """
    url = 'https://free-proxy-list.net/'
    proxies = set()

    driver.get(url)

    while 1:
        time.sleep(0.1)  # wait for the refresh
        response = driver.page_source
        soup = BeautifulSoup(response, features="html.parser")
        table = soup.find('tbody')
        for tr in table.find_all('tr', attrs={'role': "row"}):
            contents = tr.find_all('td')
            if contents[-2].text == 'yes':
                proxies.add(contents[0].text)
        # click the next button
        next_button_source = soup.find('li', attrs={'id': "proxylisttable_next"}
                                       )
        if 'disabled' in next_button_source["class"]:
            break
        else:
            next_button = driver.find_element(By.XPATH,
                                              '/html/body/section[1]/div/div[2]/div/div[3]/div[2]/div/ul/li[10]/a')
            # todo: multiple class
            next_button.click()

    return proxies

def get_page(url, ua, proxy=True):
    """

    :return:
    """
    if proxy == True:
        proxies = list(get_proxies())
        random.shuffle(proxies)
        proxy_pool = cycle(proxies)

    with timeout(100):  # use the timeout; try to connect for 100 sec
        while not success:
            proxy = next(proxy_pool)
            with timeout(5):
                try:
                    r = requests.get(url=url,
                                     headers={'User-Agent': ua},
                                     proxies={"https:": proxy}
                                     )  # for 418 teapot error
                    assert (r.status_code < 400)
                    success = 1
                except:
                    continue
    return r

def scroll_down(driver, last_height=0):
    '''
    https://stackoverflow.com/questions/20986631/how-can-i-scroll-a-web-page-using-selenium-webdriver-in-python
    :return:
    '''
    SCROLL_PAUSE_TIME = 0.5

    if last_height == 0:  # initial scroll
        # Get scroll height
        last_height = driver.execute_script("return document.body.scrollHeight")

    # Scroll down to bottom
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    # Wait to load page
    time.sleep(SCROLL_PAUSE_TIME)
    # Calculate new scroll height and compare with last scroll height
    new_height = driver.execute_script("return document.body.scrollHeight")
    if new_height == last_height:
        return -1  # bottom
    return new_height


def log(msg, core=0, log_file=root+'logs/log_c{}.txt'):
    print(datetime.now().strftime("%Y-%m-%d %H:%M: ") + "core {}, ".format(core) + msg, file=open(log_file.format(core), 'a+'))


##########################################
# analysis tools
##########################################

def author_name_check(n1, n2):
    # lower-upper cases
    # only partial names

    n1 = [s.lower() for s in n1.split()]
    n2 = [s.lower() for s in n2.split()]

    if len(set(n1) & set(n2)) > 5:
        return 1
    else:
        return 0

# for fuzzy phrase matching

def levenshteinDist(a, b, i, j):
    '''
    Return the Levenshtein distance between two strings a, b for the first i and j characters respectively
    :param a:
    :param b:
    :param i:
    :param j:
    :return:
    '''
    if min(i, j) == 0:
        return max(i, j)
    else:
        return min([levenshteinDist(a, b, i-1, j) + 1,
                    levenshteinDist(a, b, i, j-1) + 1,
                    levenshteinDist(a, b, i-1, j-1) + 1])


def levenshteinDist_dyn(a, b):
    '''
    Wagner–Fischer algorithm
    :param a:
    :param b:
    :return:
    '''
    m = len(a)
    n = len(b)
    d = np.zeros((m+1, n+1))

    if min(m, n) == 0:
        return -1

    for i in range(1, m):
        d[i, 0] = i

    for j in range(1, n):
        d[0, j] = j

    for j in range(1, n+1):
        for i in range(1, m+1):
            if a[i-1] == b[j-1]:
                substitutionCost = 0
            else:
                substitutionCost = 1
            d[i, j] = min([d[i-1, j] + 1, d[i, j-1] + 1, d[i-1, j-1] + substitutionCost])
            # deletion, insertion, substitution

    return d[m, n]



def find_author_group_nbrSide(author_l):
    '''
    (*)  ---> {...author...}
    :return:  set(*)
    '''

    group = []
    all_authors = set()
    log_l = glob.iglob("./log/*_edgelist.tsv")

    for log in log_l:
        with open(log) as f:
            line = f.readline()
            while line:
                authors_chk = line.strip('\n').split("\t")
                found = -1
                for author_i in author_l:
                    for author_j in authors_chk:
                        if 0 < levenshteinDist_dyn(author_i, author_j) < 2: # prepare for typos
                            group.append(authors_chk[0])  # only add in the direct reference
                            found = 1
                            break
                    if found:
                        break
                all_authors = all_authors | set(authors_chk)
                line = f.readline()
        print("{} checked.".format(log))

    print("found {} out of {} authors.".format(len(set(group)), len(all_authors)))

    with open("log/{}_simGroup_nbrSide.tsv".format('_'.join(author_l)), 'w+') as f:
        f.write('\n'.join(list(set(group))))

def find_author_group_referSide(author_l):
    '''
    (author) ---> {*******}
    :return: set(*)
    '''

    group = []
    all_authors = set()
    log_l = glob.iglob("./log/*_edgelist.tsv")

    for log in log_l:
        with open(log) as f:
            line = f.readline()
            while line:
                authors_chk = line.strip('\n').split("\t")
                for author_i in author_l:
                    if 0 <= levenshteinDist_dyn(author_i, authors_chk[0]) < 2: # prepare for typos
                        group += authors_chk[1:]  # only add in the direct reference
                        break
                all_authors = all_authors | set(authors_chk)
                line = f.readline()
        print("{} checked.".format(log))

    print("found {} out of {} authors.".format(len(set(group)), len(all_authors)))

    with open("log/{}_simGroup_referSide.tsv".format('_'.join(author_l)), 'w+') as f:
        f.write('\n'.join(list(set(group))))



if __name__ == "__main__":
    find_author_group_referSide(["Haruki Murakami",
                                 "John Irving", "Neil Gaiman", "Paul Auster", "Margret Atwood",
                                 "Italo Calvino"
                                 ])





