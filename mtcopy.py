#!/usr/bin/python3
import argparse, logging,os, shutil, sys

from threading import Thread
from queue import Queue

MAX_THREADS = 100
ERROR = False

# Queue processing with copier function.


def copier(file_q, thread_nr):
    while not file_q.empty():
        data = file_q.get()
        source_file = data[1]
        item_nr = data[0]
        shortname = os.path.basename(source_file)
        extra_path = source_file.split(source_folder)[1].strip('/')
        extra_folders = extra_path.split(shortname)[0].strip('/')
        if shortname != extra_path:
            dest_file = dest_folder +'/' + extra_path
            os.makedirs(dest_folder + '/' + extra_folders, exist_ok=True)
        else:
            dest_file = dest_folder + '/' + shortname
        try:
            shutil.copy2(source_file, dest_file)
        except:
            logging.error("Thread %d - Copy of %s failed. Dest : %s" %
                          (thread_nr, source_file, dest_file))
            logging.error('Shortname %s; extra path :%s; extra folders:%s' % (shortname, extra_path, extra_folders))
            file_q.task_done()
            ERROR = True
            return False
        if item_nr % 1000 == 0 and item_nr > 0:
            logging.info("Thread %d - Copied item %dth item." %
                         (thread_nr, item_nr))
        file_q.task_done()
    return True


if __name__ == "__main__":
    logging.basicConfig(filename='mtcopy.log', level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('==== Starting new run ====')

    parser = argparse.ArgumentParser()
    parser.add_argument("threads",
        help="number of threads to be used when copying content")
    parser.add_argument("-s", "--source", required=True,
        help="source folder, content of which will be copied")
    parser.add_argument("-d", "--destination", required=True,
        help="specify full path to destination folder")
    args = parser.parse_args()
    if not os.path.isdir(args.source):
        logging.error('The source path must be a folder.')
        sys.exit(1)
    if not os.path.isdir(args.destination):
        try:
            os.mkdir(args.destination)
        except OSError:
            logging.error("Creation of the directory %s failed, parent folder exists ?" % args.destination)
            sys.exit(1)
        else:
            logging.info("Created the destination folder %s " % args.destination)
    if int(args.threads) > MAX_THREADS:
        logging.error('The number of threads must be maximum %d.' % MAX_THREADS)
        sys.exit(1)
    dest_folder = os.path.abspath(args.destination) 
    source_folder = os.path.abspath(args.source) 
    logging.info('Source folder: %s' % source_folder)
    logging.info('Destination folder: %s' % dest_folder)
    # define Queue and initializing threads
    file_q = Queue(maxsize=0)
    count = 0
    logging.info('Populating queue with filenames.')
    for root, dirs, files in os.walk(source_folder):
        for filename in files:
            if count % 1000 == 0 and count > 0 :
                logging.info('Queued %d items' % count)
            file_q.put((count, str(root + '/' + filename)))
            count += 1
    # now we wait until the queue has been processed
    logging.info('Queue populated with %d files.' % count)

    for i in range(int(args.threads)):
        logging.info('Starting thread %d ' % i)
        worker = Thread(target=copier, args=(file_q, i))
        worker.setDaemon(True)
        worker.start()  
    logging.info('Threads started, waiting to complete.')
    file_q.join()
    if ERROR :
        logging.error("Errors occurred during copy - please check the logs.")
    logging.info('All tasks completed, processed %d files.' % count)
