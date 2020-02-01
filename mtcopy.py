#!/usr/bin/python3
import argparse
import logging
import os
import shutil
import sys

from threading import Thread
from queue import Queue

MAX_THREADS = 100
ERROR = False

# Queue processing with copier function.


def copier(file_q, thread_nr):
    global source_folder
    global dest_folder
    while not file_q.empty():
        data = file_q.get()
        source_file = data[1]
        item_nr = data[0]
        shortname = os.path.basename(source_file)
        extra_path = source_file.split(source_folder)[1].strip('/')
        extra_folders = extra_path.split(shortname)[0].strip('/')
        if shortname != extra_path:
            dest_file = dest_folder + '/' + extra_path
            os.makedirs(dest_folder + '/' + extra_folders, exist_ok=True)
        else:
            dest_file = dest_folder + '/' + shortname
        try:
            shutil.copy2(source_file, dest_file)
        except:
            logging.error("Thread %d - Copy of %s failed. Dest : %s" %
                          (thread_nr, source_file, dest_file))
            logging.error('Shortname %s; extra path :%s; extra folders:%s' % (
                shortname, extra_path, extra_folders))
            file_q.task_done()
            ERROR = True
            return False
        if item_nr % 1000 == 0 and item_nr > 0:
            logging.info("Thread %d - Copied item %dth item." %
                         (thread_nr, item_nr))
        file_q.task_done()
    return True

# Thread worker to read inventory and populate the file queue.
# Each thread will read the content of a subfolder of source.
def reader(folder_q, file_q, thread_nr):
    while not folder_q.empty():
        sub_folder = folder_q.get()
        logging.info('Thread %d - reading sub-folder %s'
             % (thread_nr, sub_folder))
        for root, dirs, files in os.walk(sub_folder):
            for i, filename in enumerate(files):
                file_q.put(str(root + '/' + filename))
        folder_q.task_done()

if __name__ == "__main__":
    logging.basicConfig(filename='mtcopy.log', level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('==== Starting new run ====')

    parser = argparse.ArgumentParser()
    parser.add_argument("threads", nargs='?', const=100, type=int,
                    help="number of threads to be used when copying content")
    parser.add_argument("-s", "--source", required=True,
                    help="source folder, content of which will be copied")
    parser.add_argument("-d", "--destination", required=True,
                    help="specify full path to destination folder")
    args = parser.parse_args()
    dest_folder = os.path.abspath(args.destination)
    source_folder = os.path.abspath(args.source)
    if not os.path.isdir(source_folder):
        logging.error('The source path must be a folder.')
        sys.exit(1)
    if not os.path.isdir(dest_folder):
        try:
            os.mkdir(dest_folder)
        except OSError:
            logging.error(
                "Creation of the directory %s failed, parent folder exists ?"
                % args.destination)
            sys.exit(1)
        else:
            logging.info("Created the destination folder %s " % dest_folder)
    if int(args.threads) > MAX_THREADS:
        logging.error('The number of threads must be maximum %d.' %
                      MAX_THREADS)
        sys.exit(1)
    logging.info('Source folder: %s' % source_folder)
    logging.info('Destination folder: %s' % dest_folder)
    # define Queue and initializing threads
    file_q = Queue(maxsize=0)
    folder_q = Queue(maxsize=0)
    count = 0
    logging.info('Populating queue.')
    for f in os.listdir(source_folder):
        folder_q.put(os.path.abspath(source_folder) + '/' + f)
    logging.info('Starting threads to read the content of the %d subfolders.' \
                 % folder_q.qsize())
    for thread_nr in range(args.threads):
        worker = Thread(target=reader, args=(folder_q, file_q, thread_nr))
        worker.setDaemon(True)
        worker.start()
    folder_q.join()
    logging.info('Queue populated with %d items.' % file_q.qsize())
    size = file_q.qsize()
    files_q = Queue(maxsize=0)
    for i in range(size):
        files_q.put((i,file_q.get()))
    for i in range(int(args.threads)):
        logging.info('Starting thread %d ' % i)
        worker = Thread(target=copier, args=(files_q, i))
        worker.setDaemon(True)
        worker.start()
    logging.info('Threads started, waiting to complete.')
    files_q.join()
    if ERROR:
        logging.error("Errors occurred during copy - please check the logs.")
    logging.info('All tasks completed, processed %d files.' % size)
