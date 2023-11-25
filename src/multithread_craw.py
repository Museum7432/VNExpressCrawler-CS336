import argparse

import pandas as pd
import numpy as np
# from tqdm import tqdm
from tqdm.auto import tqdm
import requests
from multiprocessing.dummy import Pool as ThreadPool
import os
import urllib

import time
import random

def append_df(df, r):
    df.loc[len(df.index)] = r

def process_row(r, download_file=False, testing=False, print_error=False):
    if download_file:
        row_id, (status, idx, url, save_path, *_) = r
    else:
        row_id, (status, idx, url, content, *_) = r

    if status == "done":
        return row_id, 'skip', None

    if testing:
        time.sleep(random.random()*0.5 + 0.1)
        if random.randint(0,4)%2:
            return row_id, 'done', "test"
        else:
            return row_id, 'failed', None
    
    if download_file:
        if os.path.isfile(save_path):
            return row_id, 'done', None
        try:
            urllib.request.urlretrieve(url, save_path)
        except Exception as e:
            if print_error:
                print("Error:", str(e))
            return row_id, 'failed', None
        return row_id, 'done', None
    

    try:
        req = requests.get(url)
        req.raise_for_status()
    except Exception as e:
        if print_error:
            print("Error:", str(e))
        return row_id, 'failed', None

    return row_id, 'done', req.content

def filter_jobs(jobs, download_file=False):
    for j in jobs:
        if download_file:
            row_id, (status, idx, url, save_path, *_) = j
        else:
            row_id, (status, idx, url, content, *_) = j

        if status == 'done':
            continue
        
        yield j
        

def main(args):

    if args.resume:
        print("resuming from ", args.tmp_path)
        prev = pd.read_json(args.tmp_path, orient="records", lines=True)
        prev = prev.set_index(prev.columns[1])

        jobs = pd.read_json(args.urls_list, orient="records", lines=True)
        jobs.insert(loc=0,column="status", value=None)

        if not args.download_file:
            jobs.insert(loc=3,column="content", value=None)

        for row_id, (status, idx, url, save_path, *_) in jobs.iterrows():
            if idx not in prev.index:
                continue

            jobs.loc[row_id, "status"] = prev.loc[idx]["status"]

            if not args.download_file:
                jobs.loc[row_id, "content"] = prev.loc[idx]["content"]

    else:
        jobs = pd.read_json(args.urls_list, orient="records", lines=True)
        jobs.insert(loc=0,column="status", value=None)

        if not args.download_file:
            jobs.insert(loc=3,column="content", value=None)

    pool = ThreadPool(args.nof_threads)

    i = 0
    for _ in range(args.retry + 1):

        pc = tqdm(pool.imap_unordered(
            lambda r: process_row(r=r, download_file=args.download_file, testing=args.testing, print_error=args.print_error), filter_jobs(jobs.iterrows(), download_file=args.download_file)),
            total=np.count_nonzero(jobs["status"] != "done")
        )
    
        for row_id, status, content in pc:
            i += 1

            pc.set_postfix({
                "failed": np.count_nonzero(jobs["status"] == "failed"),
                "done": np.count_nonzero(jobs["status"] == "done"),
            })

            if status == 'skip':
                continue

            if status == 'failed':
                jobs.loc[row_id, "status"] = 'failed'
                continue

            if args.download_file:
                jobs.loc[row_id, "status"] = 'done'
            else:
                if not content:
                    jobs.loc[row_id, "status"] = 'failed'
                    continue
                jobs.loc[row_id, "status"] = 'done'
                jobs.loc[row_id, "content"] = content

            if args.save_every and i % args.save_every == 0:
                jobs.to_json(args.tmp_path, orient="records", lines=True)
            
            pc.set_postfix({
                "failed": np.count_nonzero(jobs["status"] == "failed"),
                "done": np.count_nonzero(jobs["status"] == "done"),
            })


        if np.count_nonzero(jobs["status"] == "failed") == 0:
            break

    jobs.to_json(args.tmp_path, orient="records", lines=True)

    jobs[jobs["status"] == 'done'].drop(["status"], axis=1).to_json(args.output_path, orient="records", lines=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="craws contents from a list of urls"
    )

    parser.add_argument("--urls_list", type=str,help="path to urls file in jsonl format")

    parser.add_argument("--save_every", type=int,help="number of requests between periodic saves", default=None)
    parser.add_argument("--tmp_path", type=str,help="periodic saves' location", default=None)
    parser.add_argument("--resume", action='store_true', help="resume from periodic save (tmp_path)")

    parser.add_argument("--retry", type=int,help="number of retries for each failed request", default=0)

    parser.add_argument("--nof_threads", type=int,help="number of threads", default=1)
    
    parser.add_argument("--output_path", type=str,help="", default=None)

    parser.add_argument("--testing", action='store_true', help="sleep instead of requesting url")

    parser.add_argument("--download_file", action='store_true', help="download file")
    parser.add_argument("--print_error", action='store_true', help="download file")

    args = parser.parse_args()
    main(args)







