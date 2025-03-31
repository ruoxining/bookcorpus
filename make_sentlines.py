import argparse
import multiprocessing as mp
import os
import sys
from functools import partial
from glob import glob

from blingfire import text_to_sentences


def convert_into_sentences(lines):
    stack = []
    sent_L = []
    n_sent = 0
    for chunk in lines:
        if not chunk.strip():
            if stack:
                sents = text_to_sentences(
                    " ".join(stack).strip().replace("\n", " ")
                ).split("\n")
                sent_L.extend(sents)
                n_sent += len(sents)
                sent_L.append("\n")
                stack = []
            continue
        stack.append(chunk.strip())

    if stack:
        sents = text_to_sentences(" ".join(stack).strip().replace("\n", " ")).split(
            "\n"
        )
        sent_L.extend(sents)
        n_sent += len(sents)
    return sent_L, n_sent


def process_file(file_path, file_idx, total_files):
    """process a single file and return its sentences"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        sents, n_sent = convert_into_sentences(lines)
        result = "\n".join(sents) + "\n\n\n\n"

        sys.stderr.write(
            "{}/{}\t{}\t{}\n".format(file_idx, total_files, n_sent, file_path)
        )

        return result
    except Exception as e:
        sys.stderr.write(f"Error processing {file_path}: {str(e)}\n")
        return ""


def main():
    parser = argparse.ArgumentParser(
        description="Extract sentences from text files in parallel"
    )
    parser.add_argument("file_dir", help="Directory containing text files")
    parser.add_argument(
        "--num-processes",
        type=int,
        default=mp.cpu_count(),
        help="Number of processes to use (default: number of CPU cores)",
    )
    args = parser.parse_args()

    file_list = list(sorted(glob(os.path.join(args.file_dir, "*.txt"))))
    total_files = len(file_list)

    sys.stderr.write(
        f"Processing {total_files} files using {args.num_processes} processes\n"
    )

    # create process pool
    pool = mp.Pool(processes=args.num_processes)

    # prepare the function with file index and total count
    process_func = partial(process_file, total_files=total_files)

    # process files in parallel and collect results
    results = []
    for i, file_path in enumerate(file_list):
        results.append(pool.apply_async(process_func, args=(file_path, i)))

    # close the pool and collect results in order
    pool.close()

    # print results in the original order
    for result in results:
        sys.stdout.write(result.get())

    pool.join()


if __name__ == "__main__":
    main()
