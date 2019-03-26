import sqlite3
import os
import re
from datetime import datetime
import argparse
import logging

parser = argparse.ArgumentParser(description='Filter schedules for a given day into a table.')
date_today = datetime.today().strftime('%Y-%m-%d')
parser.add_argument('-d', '--date', action='store', help='Optional, schedule date (YYYY-MM-DD), defaults to today',
                    default=date_today)
parser.add_argument('-s', '--start', action='store',
                    help='Optional, CIF start date (YYYY-MM-DD), defaults to < schedule date', default=None)
parser.add_argument('-e', '--end', action='store',
                    help='Optional, CIF end date (YYYY-MM-DD), defaults to > schedule date', default=None)
parser.add_argument('-X', '--eXpired', action='store_true',
                    help='Remove expired schedules from the database.')
args = parser.parse_args()

logging.basicConfig(level=logging.DEBUG,
                    filename='log.log',
                    format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')


class CreateToday:

    """This class provides the functionality to identify applicable and valid schedules for a given day"""

    CIF_DB = './cif_record.db'  # The database file that contains CIF information.

    def __init__(self, db_file):

        if self.check_db_file(db_file):
            self.db_file = db_file
            try:
                self.db_conn = sqlite3.connect(self.db_file)
                self.c = self.db_conn.cursor()

            except sqlite3.Error as e:
                print(e)
        else:
            logging.error('Cannot find database file, exiting...')

    @staticmethod
    def check_db_file(db_file):

        """This function checks that the database file exists"""
        if os.path.isfile(db_file):
            return True
        else:
            return False

    @staticmethod
    def get_current_cif():

        """This function returns the current CIF from the database"""

        db_conn = sqlite3.connect(CreateToday.CIF_DB)
        c = db_conn.cursor()
        c.execute('SELECT `txt_current_cif` FROM `tbl_current_cif` LIMIT 1;')
        return c.fetchone()[0]

    @staticmethod
    def format_sql(sql_string):

        """This method formats the sql by removing multiple spaces and new lines."""

        return re.sub(r" {2,}|\n", "", sql_string.strip())

    def create_table(self):

        """This method ensures that the relevant table is purged, ready for the new data"""

        logging.debug('Clearing `tbl_current_schedule` from {}'.format(self.db_file))
        self.c.execute('DROP TABLE IF EXISTS `tbl_current_schedule`;')

        sql_string = """
        CREATE TABLE IF NOT EXISTS
            `tbl_current_schedule` (
                int_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                int_header_id INTEGER,
                txt_transaction_type TEXT,
                txt_uid TEXT,
                txt_start_date TEXT,
                txt_end_date TEXT,
                txt_days_run TEXT,
                txt_bank_holiday TEXT,
                txt_status TEXT,
                txt_category TEXT,
                txt_identity TEXT,
                txt_headcode TEXT,
                txt_service_code TEXT,
                txt_portion_id TEXT,
                txt_power_type TEXT,
                txt_timing_load TEXT,
                txt_speed TEXT,
                txt_op_char TEXT,
                txt_train_class TEXT,
                txt_sleepers TEXT,
                txt_reservations TEXT,
                txt_catering TEXT,
                txt_stp_indicator TEXT);
        """
        self.c.execute(CreateToday.format_sql(sql_string))
        self.db_conn.commit()

    def remove_expired_schedules(self):

        """This function removes schedules from the database that have expired"""

        sql_string = """
        DELETE FROM `tbl_basic_schedule` 
        WHERE `tbl_basic_schedule`.`txt_end_date` < DATE('now')
        """
        self.c.execute(CreateToday.format_sql(sql_string))
        self.db_conn.commit()
        logging.debug('{} expired schedules removed'.format(self.c.rowcount))

    def get_current_schedules(self, cif_date=None, start_date=None, end_date=None):

        """This function runs a query to return schedules valid in the specified date"""

        day_string = list('_______')

        if cif_date:
            cf_dt = datetime.strptime(cif_date, '%Y-%m-%d')
            day_string[cf_dt.weekday()] = '1'
        else:
            day_string[datetime.today().weekday()] = '1'

        if start_date and end_date:
            cif_start_date = datetime.strptime(start_date, '%Y-%m-%d')
            cif_end_date = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            cif_start_date = datetime.strptime(cif_date, '%Y-%m-%d')
            cif_end_date = datetime.strptime(cif_date, '%Y-%m-%d')

        logging.debug('Getting valid schedules for {}'.format(cif_date))

        sql_string = """
            INSERT INTO 
                `tbl_current_schedule`
            SELECT 
                *
            FROM 
                `tbl_basic_schedule`
            WHERE 
                `tbl_basic_schedule`.`int_record_id` IN (
                    SELECT 
                        `tbl_basic_schedule`.`int_record_id`
                    FROM 
                        `tbl_basic_schedule`
                    WHERE 
                        `txt_start_date` <= date('{}') 
                        AND `txt_end_date` >= date('{}') 
                        AND `txt_days_run` LIKE '{}' 
                        AND `txt_stp_indicator` NOT LIKE '%C%' 
                        AND `txt_status` NOT IN ('B', 'S', 4, 5))
        """.format(cif_start_date, cif_end_date, ''.join(day_string))

        self.c.execute(CreateToday.format_sql(sql_string))
        self.db_conn.commit()
        logging.debug('{} Schedules match into `tbl_current_schedule`'.format(self.c.rowcount))

    def remove_duplicates(self):

        """This function removes duplicate schedules, ensuring the correct versions are in the current schedule"""

        logging.debug('Removing duplicate schedules from `tbl_current_schedule` in {}'.format(self.db_file))

        sql_string = """
            DELETE FROM 
                `tbl_current_schedule`
            WHERE
                `tbl_current_schedule`.`txt_stp_indicator` = "P" AND
                `tbl_current_schedule`.`txt_uid` IN
                    (SELECT 
                        `tbl_current_schedule`.`txt_uid`
                    FROM 
                        `tbl_current_schedule`
                    WHERE 
                        `tbl_current_schedule`.`txt_stp_indicator` = "O");
        """
        self.c.execute(CreateToday.format_sql(sql_string))
        self.db_conn.commit()
        logging.debug('{} duplicate schedules removed'.format(self.c.rowcount))


if __name__ == '__main__':

    logging.debug('Running script to update `tbl_current_schedule`')
    logging.debug('Script run with the following arguments...')
    logging.debug(args)
    conn = CreateToday(CreateToday.get_current_cif())
    if args.eXpired:
        logging.debug('Attempting to remove expired schedules...')
        conn.remove_expired_schedules()
    conn.create_table()
    conn.get_current_schedules(cif_date=args.date, start_date=args.start, end_date=args.end)
    conn.remove_duplicates()

