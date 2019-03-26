import sqlite3
import os
import re
from datetime import datetime
import argparse

parser = argparse.ArgumentParser(description='Filter schedules for a given day into a table.')
date_today = datetime.today().strftime('%Y-%m-%d')
parser.add_argument('-d', '--date', action='store', help='Optional, schedule date (YYYY-MM-DD), defaults to today', default=date_today)
parser.add_argument('-s', '--start', action='store', help='Optional, CIF start date (YYYY-MM-DD), defaults to < schedule date', default=None)
parser.add_argument('-e', '--end', action='store', help='Optional, CIF end date (YYYY-MM-DD), defaults to > schedule date', default=None)
args = parser.parse_args()

class CreateToday:

    def __init__(self, db_file):

        if self.check_db_file(db_file):
            self.db_file = db_file
            try:
                self.db_conn = sqlite3.connect(self.db_file)
                self.c = self.db_conn.cursor()

            except sqlite3.Error as e:
                print(e)
        else:
            print('Cannot find file, exiting...')

    def check_db_file(self, db_file):
        if os.path.isfile(db_file):
            return True
        else:
            return False

    @staticmethod
    def format_sql (sql_string):
        """This method formats the sql by removing tabs and new lines."""
        return(re.sub(r" {2,}|\n", "", sql_string.strip()))

    def create_table(self):
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
        print('`tbl_current_schedule` Created in {}'.format(self.db_file))

    def get_current_schedules(self, cif_date=None, start_date=None, end_date=None):

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
        print('{} Schedules match into `tbl_current_schedule`'.format(self.c.rowcount))

    def remove_duplicates(self):

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
        print('{} duplicate schedules removed'.format(self.c.rowcount))

if __name__ == '__main__':

    conn = CreateToday('/home/jmoss2/PycharmProjects/timetable_processor/DFROC2R(A)/DFROC2R(A).db')
    conn.create_table()
    conn.get_current_schedules(cif_date=args.date, start_date=args.start, end_date=args.end)
    conn.remove_duplicates()

