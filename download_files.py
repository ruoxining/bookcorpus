#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import multiprocessing as mp
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

import json
import os
import sys
from glob import glob

from progressbar import ProgressBar

import epub2txt

SLEEP_SEC = 0.05
SUCCESS_SLEEP_SEC = 0.001
RETRY_SLEEP_SEC = 1.0
MAX_OPEN_COUNT = 3


def write_txt(txt, out_path, num_words=None):
    # occasionally, some epubs text are decoded with errors
    # e.g. repeated bib lines
    # filter out them by comparing number of words
    counted_num_words = len(txt.split())
    if not txt.strip():
        pass
    elif num_words is None or (num_words * 0.5 < counted_num_words < num_words * 1.5):
        with open(
            out_path, "w", encoding="utf8"
        ) as txt_out:  # convert epub2txt and save
            txt_out.write(txt)


def process_line(line, out_dir, trash_bad_count, done_files):
    """process a single line/book"""
    if not line.strip():
        return

    # track any temp files we create so we can clean up
    tmp_path = None
    out_path = None

    try:
        # read data
        data = json.loads(line.strip())
        _, book_id = os.path.split(data["page"])
        _, file_name = os.path.split(data["epub"])

        out_file_name = "{}__{}".format(book_id, file_name.replace(".epub", ".txt"))
        out_path = os.path.join(out_dir, out_file_name)

        # skip if already done
        if out_file_name in done_files:
            return

        if data["txt"]:
            # try to download .txt file
            for try_count in range(MAX_OPEN_COUNT):
                try:
                    response = opener.open(data["txt"])
                    if try_count >= 1:
                        sys.stderr.write(
                            "Succeeded in opening {}\n".format(data["txt"])
                        )
                    time.sleep(SUCCESS_SLEEP_SEC)
                    break  # success
                except Exception as e:
                    sys.stderr.write("Failed to open {}\n".format(data["txt"]))
                    sys.stderr.write("{}: {}\n".format(type(e).__name__, str(e)))
                    time.sleep(RETRY_SLEEP_SEC)
            else:
                sys.stderr.write(" Gave up to open {}\n".format(data["txt"]))
                return

            txt = response.read().decode("utf-8", "ignore")
            write_txt(txt, out_path, None)
        else:
            # revenge by converting .epub to .txt
            tmp_path = os.path.join(out_dir, f"tmp_{book_id}_{file_name}")
            for try_count in range(MAX_OPEN_COUNT):
                try:
                    urlretrieve(data["epub"], tmp_path)  # download epub
                    if try_count >= 1:
                        sys.stderr.write(
                            "Succeeded in opening {}\n".format(data["epub"])
                        )
                    time.sleep(SUCCESS_SLEEP_SEC)
                    break  # success
                except Exception as e:
                    sys.stderr.write("Failed to open {}\n".format(data["epub"]))
                    sys.stderr.write("{}: {}\n".format(type(e).__name__, str(e)))
                    time.sleep(RETRY_SLEEP_SEC)
            else:
                sys.stderr.write(" Gave up to open {}\n".format(data["epub"]))
                return

            txt = epub2txt.epub2txt(tmp_path).convert()
            if trash_bad_count:
                if "num_words" in data:
                    write_txt(txt, out_path, data["num_words"])
            else:
                write_txt(txt, out_path, None)
    except Exception as e:
        sys.stderr.write(str(e) + "\n")
        if out_path and os.path.exists(out_path):
            os.remove(out_path)
    finally:
        # clean up temp file
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except:
                pass


def chunk_list(lst, n):
    """split a list into n chunks"""
    chunk_size = max(1, len(lst) // n)
    return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", "--out", type=str, required=True)
    parser.add_argument("--list-path", "--list", type=str, required=True)
    parser.add_argument("--trash-bad-count", action="store_true", default=False)
    parser.add_argument("--num-processes", type=int, default=mp.cpu_count())
    args = parser.parse_args()

    num_processes = args.num_processes
    out_dir = args.out_dir
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    filelist_path = args.list_path

    lines = list(open(filelist_path, encoding="utf8").readlines())
    lines = [line for line in lines if line.strip()]  # filter empty lines

    done_files = set(
        [os.path.split(path)[-1] for path in glob(os.path.join(out_dir, "*.txt"))]
    )
    sys.stderr.write(
        "{} files had already been saved in {}.\n".format(len(done_files), out_dir)
    )
    sys.stderr.write(f"Using {num_processes} processes\n")

    # create a process pool
    pool = mp.Pool(processes=num_processes)

    # create a partial function with fixed arguments
    process_func = partial(
        process_line,
        out_dir=out_dir,
        trash_bad_count=args.trash_bad_count,
        done_files=done_files,
    )

    # map the function to all lines
    list(pool.imap_unordered(process_func, lines))

    # close the pool
    pool.close()
    pool.join()


if __name__ == "__main__":
    main()
