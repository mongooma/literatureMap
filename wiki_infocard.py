from multiprocessing import Process, Manager, Pool, Queue
from itertools import product, cycle
from utils import Scraper, exit_safe
from selenium import webdriver
from selenium.webdriver.common.by import By
import random
import time
import os
import sys
import pickle as pkl
import traceback
import pandas as pd
from bs4 import BeautifulSoup

def wiki_infocard(core, author_name):
    #  search in goodreads and get the author page
    search_term = author_name
    scraper = scrapers[core]
    driver = scraper.driver
    try:
        driver.get('''https://www.wikipedia.org/''')
        driver.find_element(By.XPATH, '''//*[@id="searchInput"]''').\
                        send_keys(search_term)
        button = driver.find_element(By.XPATH, '''//*[@id="search-form"]/fieldset/button''')
        webdriver.ActionChains(driver).move_to_element(button).click(button).perform()
        time.sleep(1)
    except:  # timeout?
        # exit_safe(scraper.driver)
        return -1

    try:
        # get the infocard table from the wiki page
        infocard_xp = '''//*[@class="infobox vcard"]'''
        infocard = driver.find_element(By.XPATH, infocard_xp).get_attribute("outerHTML")
        pass
    except:
        print("Author {} infocard not found! Check.".format(author_name), file=sys.stderr)
        # exit_safe(scraper.driver)
        return -1

    with open('log/authorInfocard.xml'.format(core), "a+") as f:
        f.write(infocard + '\n')

    print("Author {} infos got.".format(author_name), file=sys.stdout)
    time.sleep(1)

    return

def worker(core, author_name_l):
    '''
    wrap the run() in a try-except loop so that other unexpected exceptions are caught and the program
    is safely exited

    :return:
    '''
    sys.stdout = open("./log/{}.out".format(core), 'a')
    sys.stderr = open("./log/{}.err".format(core), 'a')

    try:  # catch whatever error in
        for author_name in author_name_l:
            wiki_infocard(core, author_name)
            pass
    except:
        traceback.print_exc()  # this will print to stderr
        # exit_safe(scrapers[core].driver)

    return


def worker_main(authors_all, n):
    pool = Pool(processes=n)
    authors_l_l = []
    for i in range(0, len(authors_all), int(len(authors_all)/n)):
        authors_l_l.append(authors_all[i:i+int(len(authors_all)/n)])

    pool.starmap(worker, product(range(n), authors_l_l))
    pool.close()
    pool.join()

    for s in scrapers:
        exit_safe(s.driver)

    return


def check_record(n):

    return

def organize_spreadsheet():

    soup = BeautifulSoup(open("log/authorInfocard.xml"), features="html.parser")
    tables = soup.find_all('tbody')
    info_fields = set()
    for table in tables:
        for th in table.find_all('th', attrs={"scope": "row"}):
            try:
                info_fields.add(th.text)
            except:
                continue

    with open("log/infocard_fields.tsv", 'w+') as f:
        for field in info_fields:
            f.write(field+'\n')

    return

if __name__ == "__main__":

    # authors_all = pkl.load(open('author_l_p1.pkl', 'rb'))
    # n = 8
    # scrapers = [Scraper() for i in range(n)]
    # worker_main(authors_all, n)

    organize_spreadsheet()