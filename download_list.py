#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import multiprocessing as mp
import re
import sys
import time
from functools import partial

try:
    from cookielib import CookieJar

    cj = CookieJar()
    import urllib2

    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
    import urllib

    urlretrieve = urllib.urlretrieve
except ImportError:
    import http.cookiejar

    cj = http.cookiejar.CookieJar()
    import urllib

    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    urlretrieve = urllib.request.urlretrieve

import datetime
import json
import os

from bs4 import BeautifulSoup
from progressbar import ProgressBar

# If you wanna use some info, write them.
REQUIRED = [
    #    'page',
    #    'epub',
    #    'txt',
    #    'title',
    #    'author',
    #    'genres',
    #    'publish',
    #    'num_words',
    "b_idx",
]

SLEEP_SEC = 0.1
RETRY_SLEEP_SEC = 1.0
MAX_OPEN_COUNT = 3

search_url_pt = "https://www.smashwords.com/books/category/1/downloads/0/free/medium/{}"
search_urls = [search_url_pt.format(i) for i in range(0, 24000 + 1, 20)]

num_words_pt = re.compile(r"Words: (\d+)")
pub_date_pt = re.compile(r"Published: ([\w\.]+\s[\d]+,\s[\d]+)")


def process_book(b_url, book_index):
    """process a single book url and return the data"""
    results = []
    for try_count in range(MAX_OPEN_COUNT):
        try:
            response = opener.open(b_url)
            if try_count >= 1:
                sys.stderr.write("Succeeded in opening {}\n".format(b_url))
            break  # success
        except Exception as e:
            sys.stderr.write("Failed to open {}\n".format(b_url))
            sys.stderr.write("{}: {}\n".format(type(e).__name__, str(e)))
            time.sleep(RETRY_SLEEP_SEC)
    else:
        sys.stderr.write(" Gave up to open {}\n".format(b_url))
        return None

    body = response.read()
    soup = BeautifulSoup(body, "lxml")

    # get meta
    meta_infos = soup.find_all(class_="col-md-3")
    if not meta_infos:
        sys.stderr.write("Failed: meta_info {}\n".format(b_url))
        return None
    meta_txts = [m.text for m in meta_infos if "Language: English" in m.text]

    # check lang
    is_english = len(meta_txts) >= 1
    if not is_english:
        return None

    # get num words
    meta_txt = meta_txts[0].replace(",", "")
    match = num_words_pt.search(meta_txt)
    if match:
        num_words = int(match.group(1))
    elif "num_words" in REQUIRED:
        sys.stderr.write("Failed: num_words {}\n".format(b_url))
        return None
    else:
        num_words = 0

    # get publish date
    meta_txt = meta_txts[0]
    match = pub_date_pt.search(meta_txt)
    if match:
        pub_date = match.group(1)
    elif "publish" in REQUIRED:
        sys.stderr.write("Failed: publish {}\n".format(b_url))
        return None
    else:
        pub_date = ""

    # get genres
    genre_txts = soup.find_all(class_="category")
    if genre_txts:
        genres = [
            g.text.replace("\u00a0\u00bb\u00a0", "\t").strip() for g in genre_txts
        ]
    elif "genres" in REQUIRED:
        sys.stderr.write("Failed: genre {}\n".format(b_url))
        return None
    else:
        genres = []

    # get title
    title = soup.find("h1")
    if title:
        title = title.text
    elif "title" in REQUIRED:
        sys.stderr.write("Failed: title {}\n".format(b_url))
        return None
    else:
        title = ""

    # get author
    author = soup.find(itemprop="author")
    if author:
        author = author.text
    elif "author" in REQUIRED:
        sys.stderr.write("Failed: author {}\n".format(b_url))
        return None
    else:
        author = ""

    # get epub
    epub_links = soup.find_all(
        title="Supported by many apps and devices (e.g., Apple Books, Barnes and Noble Nook, Kobo, Google Play, etc.)"
    )
    if epub_links:
        epub_url = epub_links[0].get("href")
        if epub_url:
            epub_url = "https://www.smashwords.com" + epub_url
        elif "epub" in REQUIRED:
            sys.stderr.write("Failed: epub2 {}\n".format(b_url))
            return None
        else:
            epub_url = ""
    elif "epub" in REQUIRED:
        sys.stderr.write("Failed: epub1 {}\n".format(b_url))
        return None
    else:
        epub_url = ""

    # get txt if possible
    txt_links = soup.find_all(title="Plain text; contains no formatting")
    if not txt_links:
        txt_url = ""
    else:
        txt_url = txt_links[0].get("href")
        if not txt_url:
            txt_url = ""
        else:
            txt_url = "https://www.smashwords.com" + txt_url

    if not epub_url and not txt_url:
        sys.stderr.write("Failed: epub and txt {}\n".format(b_url))
        return None

    data = {
        "page": b_url,
        "epub": epub_url,
        "txt": txt_url,
        "title": title,
        "author": author,
        "genres": genres,
        "publish": pub_date,
        "num_words": num_words,
        "b_idx": book_index,
    }
    return data


def process_search_url(url, start_idx):
    """process a single search url and return the data for all books"""
    time.sleep(SLEEP_SEC)
    results = []

    for try_count in range(MAX_OPEN_COUNT):
        try:
            response = opener.open(url)
            if try_count >= 1:
                sys.stderr.write("Succeeded in opening {}\n".format(url))
            break  # success
        except Exception as e:
            sys.stderr.write("Failed to open {}\n".format(url))
            sys.stderr.write("{}: {}\n".format(type(e).__name__, str(e)))
            time.sleep(RETRY_SLEEP_SEC)
    else:
        sys.stderr.write(" Gave up to open {}\n".format(url))
        return results

    body = response.read()
    soup = BeautifulSoup(body, "lxml")

    book_links = soup.find_all(class_="library-title")

    for i, b_link in enumerate(book_links):
        book_index = start_idx + i
        b_url = b_link.get("href")
        data = process_book(b_url, book_index)
        if data:
            results.append(data)

    return results


def chunk_list(lst, n):
    """split a list into n chunks"""
    chunk_size = len(lst) // n
    remainder = len(lst) % n
    result = []
    idx = 0
    for i in range(n):
        size = chunk_size + (1 if i < remainder else 0)
        result.append(lst[idx : idx + size])
        idx += size
    return result


def main():
    start_time = time.time()
    sys.stderr.write(str(datetime.datetime.now()) + "\n")

    # split search urls into chunks for parallel processing
    chunks = chunk_list(search_urls, NUM_PROCESSES)

    with mp.Pool(processes=NUM_PROCESSES) as pool:
        # create a partial function with the starting index for each chunk
        start_indices = [0]
        for i in range(len(chunks) - 1):
            start_indices.append(
                start_indices[-1] + len(chunks[i]) * 20
            )  # approximate number of books per page

        results = []
        for i, chunk in enumerate(chunks):
            process_func = partial(process_search_url, start_idx=start_indices[i])
            chunk_results = pool.map(process_func, chunk)
            # flatten the list of lists
            for sublist in chunk_results:
                for item in sublist:
                    print(json.dumps(item))

    elapsed_time = time.time() - start_time
    sys.stderr.write(f"Completed in {elapsed_time:.2f} seconds\n")


if __name__ == "__main__":
    global NUM_PROCESSES
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-processes", type=int, default=4)
    NUM_PROCESSES = parser.parse_args().num_processes

    main()
