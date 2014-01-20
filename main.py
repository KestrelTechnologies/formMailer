#!/usr/bin/env python
'''
Project: form mailer
Author: Spencer Rathbun
Date: 1/19/2014

Summary: This project pulls down data from the internet and places it into a form mailer for printing
'''
import argparse, logging, datetime
from collector import collector
from recordFilter import recordFilter
import dbInserter

def main(**kwargs):
    """Entry point for the program."""
    # set up logging
    logger = logging.getLogger('formMailer')
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename=kwargs['logfile'],
                        filemode='a')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    starttime = datetime.datetime.now()
    logger.info('Begin processing data...')
    logger.debug('Current run inputs: {0}'.format(locals()))

    collector = collector()
    recFilter = recordFilter(collector)
    db = dbInserter.dbInserter()

    # build the first record individually, to create the necessary schema
    rec = recFilter.next()
    db.insertRec(rec, firstrec=True)
    # loop over the rest of the records and insert them
    try:
        for c, rec in enumerate(recFilter):
            db.insertRec(rec)
            if not (c % 5000) and (c > 0):
                logger.info('Added {0} total records to session so far'.format(c))
            if not ((c+1) % 30000):
                db.flush()
        db.flush()
        db.updatesAndViews()
        db.commit()
    except Exception, e:
        logger.error(str(e))
        db.rollback()
    # Build a report of the relevent data from the run.
    logger.info("Finishing...")
    logger.info("run time: {0}".format(datetime.datetime.now() - starttime))
    logger.info("total records: {0}".format(recFilter.recInfo['total']))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process the website and use the form to print letters', version='%(prog)s 0.1')
    parser.add_argument('--logfile', type=str, default='taxConverter.log', help='name of the log file to use')

    args = parser.parse_args()

    main(**vars(args))
