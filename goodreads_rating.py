from multiprocessing import Process, Manager, Pool, Queue
from itertools import product, cycle
from utils import Scraper, exit_safe, author_name_check
from selenium.webdriver.common.by import By
import random
import time
import os
import sys
import pickle as pkl
import traceback
import pandas as pd
import json
import glob

def goodread_rating(core, author_name):
    #  search in goodreads and get the author page
    search_term = '+'.join(author_name.split(' '))
    scraper = scrapers[core]
    lock = locks[core]
    dic = dics[core]
    driver = scraper.driver
    try:
        driver.get("https://www.goodreads.com/search?q={}&search%5Bfield%5D=author".
               format(search_term))
        time.sleep(1)
    except:  # timeout?
        # exit_safe(scraper.driver)
        return -1

    xp = '''//*[@class="authorName"]'''
    scraper.wait_until(scraper.visible, By.XPATH, xp)
    author_pgs = driver.find_elements(By.XPATH, xp)

    # locate within multiple authors
    found = -1
    for pg in author_pgs:
        if author_name_check(pg.find_element_by_tag_name('span').text, author_name):
            author_pg = pg
            found = 1
            break
    if found == -1:
        print("Author {} not found! Check.".format(author_name), file=sys.stdout)
        # exit_safe(scraper.driver)
        return -1

    try:
        driver.get(author_pg.get_attribute("href"))
        time.sleep(1)
        # get the user ratings from the user page
        scraper.wait_until(scraper.visible, By.XPATH, '''//*[@itemprop="ratingValue"]''')
        dataTitles = [d.text for d in driver.find_elements(By.XPATH, '''//*[@class="dataTitle"]''')]
        dataItems = [d.text for d in driver.find_elements(By.XPATH, '''//*[@class="dataItem"]''')]
        assert(len(dataTitles) == len(dataItems))
        avg_rating = driver.find_element(By.XPATH, '''//*[@itemprop="ratingValue"]''').text
        no_rating = int(''.join(driver.find_element(By.XPATH, '''//*[@itemprop="ratingCount"]''').text.split(',')))
        no_review = int(''.join(driver.find_element(By.XPATH, '''//*[@itemprop="reviewCount"]''').text.split(',')))
    except:
        print("Author {} ratings not found! Check.".format(author_name), file=sys.stderr)
        # exit_safe(scraper.driver)
        return -1

    # with open('log/core{}_authorRating.tsv'.format(core), 'a+') as f:
    #     f.write(author_name + '\t' +
    #             str(avg_rating) + '\t' + str(no_rating) + '\t' + str(no_review) + '\n')

    # write to the dictionary
    lock.acquire()
    dic[author_name] = dic.setdefault(author_name, dict())
    dic[author_name]["authorName"] = author_name
    dic[author_name]["ratingValue"] = avg_rating
    dic[author_name]["ratingCount"] = no_rating
    dic[author_name]["reviewCount"] = no_review
    for title, item in zip(dataTitles, dataItems):
        dic[author_name][title] = item
    lock.release()

    print("Author {} ratings got.".format(author_name), file=sys.stdout)
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
            goodread_rating(core, author_name)
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

    for i in range(n):
        with open('log/core{}_authorInfo_goodreads{}.json'.format(i, random.randint(0, 10000)), 'w') as f:
            json.dump(dics[i], f)   # todo: dump a {} into the file, check dics

def check_record(n):
    cnt = 0
    for f in ["log/core{}_authorRating.tsv".format(i) for i in range(n)]:
        cnt += len(open(f).readlines())

    print("{}/{} authors' ratings found. ".format(cnt, len(authors_all)))
    # 4172/4388 authors' ratings found.

    return

def get_top_authors(n):

    res = []
    for f in ["log/core{}_authorRating.tsv".format(i) for i in range(n)]:
        res.append(pd.read_csv(f, delimiter='\t', header=None))

    res = pd.concat(res)
    # res = res.reset_index()
    res.columns = ["author_name", "avg_rating", "no_rating", "no_review"]
    res.drop_duplicates(subset='author_name', inplace=True)

    res["avg_rating"] = res["avg_rating"].astype(float)
    res["avg_rating"] /= max(res["avg_rating"])
    res["no_rating"] = res["no_rating"].astype(float)
    res["no_rating"] /= max(res["no_rating"])
    res["no_review"] = res["no_review"].astype(float)
    res["no_review"] /= max(res["no_review"])

    res["score"] = res["avg_rating"]+res["no_rating"]+res["no_review"]

    res.sort_values("score", ascending=False, inplace=True)

    res["author_name"].to_csv("log/authors_sortedByRating.tsv", header=False, index=False)

    return


if __name__ == "__main__":

    MAX_ITER = 20
    cnt = 0
    while cnt < MAX_ITER:
        if cnt == 0:
            authors_all = pkl.load(open('author_l_p1.pkl', 'rb'))
        else:
            authors_all = list(set(pkl.load(open('author_l_p1.pkl', 'rb'))) -
                               set([dic["authorName"]
                                    for f in list(glob.iglob("log/core*_authorInfo_goodreads*"))
                                    for dic in json.loads(open(f).read())
                                    ]))
    # still have ~200 authors not found
        n = 8
        scrapers = [Scraper() for i in range(n)]
        locks = [Manager().Lock() for i in range(n)]
        dics = [dict() for i in range(n)]
        worker_main(authors_all, n)

        cnt += 1
    # check_record(n)

    # get_top_authors(n)

