#!/usr/bin/env python

# Usage:
#       python cleaner.py --help
#
# For a MySQL Master server with replication turned ON,
# this script performs the deletion of older (as per defined threshold)
# binlog files in accordance with the status of the Slave Server replication.

from argparse import ArgumentParser
import getpass
import MySQLdb
import shutil
import os
import logging
from datetime import datetime

__author__ = 'franklinsijo'


def clean_older_binlogs(current_binlog):
    try:
        file_mtime = os.stat(current_binlog).st_mtime
    except OSError:
        LOG.error("Looks like we don't have the stated binlog. \n Is replication ON?", exc_info=True)
        raise

    actual_oldts = file_mtime - args.OLD_DAYS * 86400
    LOG.info('Moving files older than %d' % actual_oldts)

    deletable_files = list()
    for f in os.listdir(BINLOG_DIRECTORY):
        file_abspath = os.path.join(BINLOG_DIRECTORY, f)
        if os.path.isfile(file_abspath) and file_abspath != current_binlog and not file_abspath.endswith('.index'):
            mod_ts = os.stat(file_abspath).st_mtime
            if mod_ts < actual_oldts:
                deletable_files.append(file_abspath)
    LOG.info('%d older binlog files found' % len(deletable_files))

    if len(deletable_files) > 0:
        BACKUP_DIR = os.path.join(BINLOG_DIRECTORY, 'BACKUP')
        if args.RETENTION_ENABLED:
            LOG.info('Retention enabled. Retaining current list of deletable binlogs as backup and purging older.')
            INTERMEDIATE_BACKUP_DIR = os.path.join(BACKUP_DIR, datetime.now().strftime('%Y%m%d'))
            if os.path.exists(INTERMEDIATE_BACKUP_DIR):
                shutil.rmtree(INTERMEDIATE_BACKUP_DIR)
            os.makedirs(INTERMEDIATE_BACKUP_DIR)
            for dfile in deletable_files:
                LOG.debug('Moving %s file to %s directory' % (dfile, INTERMEDIATE_BACKUP_DIR))
                shutil.move(dfile, INTERMEDIATE_BACKUP_DIR)

            LOG.debug('Purging older backups from %s' % BACKUP_DIR)
            for backup_dir in os.listdir(BACKUP_DIR):
                backup_dir_fp = os.path.join(BACKUP_DIR, backup_dir)
                if backup_dir_fp != INTERMEDIATE_BACKUP_DIR:
                    shutil.rmtree(backup_dir_fp)
        else:
            LOG.info('Retention disabled. Deleting current list of deletable binlogs and purging all available backups.')
            for dfile in deletable_files:
                LOG.debug('Deleting %s' % dfile)
                os.remove(dfile)
            shutil.rmtree(BACKUP_DIR)
        LOG.info('Cleanup Completed.')


if __name__ == '__main__':
    argparser = ArgumentParser()
    mandatory_args = argparser.add_argument_group('mandatory arguments')
    mandatory_args.add_argument('--host',
                                dest='SLAVE_HOST',
                                type=str,
                                required=True,
                                help='IP Address / FQDN / Hostname of Slave MySQL Server')
    mandatory_args.add_argument('--user',
                                dest='SLAVE_USER',
                                type=str,
                                required=True,
                                help='MySQL user to connect with Slave MySQL Server')
    mandatory_args_password = mandatory_args.add_mutually_exclusive_group(required=True)
    mandatory_args_password.add_argument('-P',
                                         dest='SLAVE_PASSWORD_PROMPT',
                                         action="store_true",
                                         help='Prompt user for Password to connect with Slave MySQL Server')
    mandatory_args_password.add_argument('--password-file',
                                         dest='SLAVE_PASSWORD_FILE',
                                         type=str,
                                         help='Path of plain text file containing password to connect with Slave MySQL Server ')
    mandatory_args_password.add_argument('--password',
                                         dest='SLAVE_PASSWORD',
                                         type=str,
                                         help='Password to connect with Slave MySQL Server')
    mandatory_args.add_argument('--binlog-dir',
                                dest='BINLOG_DIRECTORY',
                                type=str,
                                required=True,
                                help='Binlog files directory path of Master MySQL Server')
    argparser.add_argument('--threshold',
                           dest='OLD_DAYS',
                           default=7,
                           type=int,
                           help='Delete Binlog files older than threshold (days). Defaults to 7 days')
    argparser.add_argument('--enable-retention',
                           dest='RETENTION_ENABLED',
                           default=True,
                           action="store_true",
                           help='Performs soft delete on the current deletable logs and Purges older backups')

    args = argparser.parse_args()

    if args.SLAVE_PASSWORD_PROMPT:
        PASSWORD = getpass.getpass('Enter Slave MySQL Password for %s : ' % args.SLAVE_USER)
    elif args.SLAVE_PASSWORD_FILE:
        password_file = os.path.abspath(args.SLAVE_PASSWORD_FILE)
        try:
            with open(password_file, 'r') as pf:
                PASSWORD = pf.read().strip()
        except:
            raise
    else:
        PASSWORD = args.SLAVE_PASSWORD

    BINLOG_DIRECTORY = os.path.abspath(args.BINLOG_DIRECTORY)

    logging.basicConfig(level=logging.INFO,
                        filename=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'clean_mysql_binlogs.log'),
                        format='%(asctime)s - %(levelname)s - %(message)s')
    LOG = logging.getLogger(__name__)

    LOG.info('Starting MySQL Binlog Cleaner for Host: %s' % args.SLAVE_HOST)

    try:
        connection = MySQLdb.connect(args.SLAVE_HOST, args.SLAVE_USER, PASSWORD)
        cursor = connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('show slave status')
        slave_status = cursor.fetchone()
        if slave_status:
            binlog_inuse = os.path.join(BINLOG_DIRECTORY, slave_status["Relay_Master_Log_File"])
            LOG.info('Current Binlog file in use: %s' % binlog_inuse)
            clean_older_binlogs(current_binlog=binlog_inuse)
        else:
            err_message = 'Unable to get the status of the Slave Server.'
            LOG.error(err_message)
            raise Exception(err_message)
    except Exception as e:
        LOG.error(msg=e.message, exc_info=True)
        raise e