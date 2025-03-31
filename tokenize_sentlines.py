import argparse
import multiprocessing as mp
import sys
from functools import partial
from itertools import islice

from blingfire import text_to_words


def process_chunk(lines):
    """process a chunk of lines and return tokenized results"""
    results = []
    for l in lines:
        if l.strip():
            results.append(text_to_words(l.strip()))
        else:
            results.append("")
    return results


def chunk_iterator(iterable, size):
    """yield chunks of specified size from iterable"""
    iterator = iter(iterable)
    while chunk := list(islice(iterator, size)):
        yield chunk


def main():
    parser = argparse.ArgumentParser(description="Tokenize words in parallel")
    parser.add_argument(
        "--num-processes",
        type=int,
        default=mp.cpu_count(),
        help="Number of processes to use (default: number of CPU cores)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Number of lines to process in each chunk (default: 1000)",
    )
    args = parser.parse_args()

    # create process pool
    pool = mp.Pool(processes=args.num_processes)

    # read all input lines to allow chunking
    # for very large inputs, consider using iterative approach instead
    stdin_lines = sys.stdin.readlines()

    # process chunks in parallel
    chunks = list(chunk_iterator(stdin_lines, args.chunk_size))
    results = pool.map(process_chunk, chunks)

    # output results
    for chunk_result in results:
        for line in chunk_result:
            print(line)

    pool.close()
    pool.join()


if __name__ == "__main__":
    main()
