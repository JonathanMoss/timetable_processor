#!/usr/bin/python3
import sqlite3
import time
import re


class DBConnection:

    def __init__(self, db_name):

        self.db_name = db_name
        self.db_conn = None
        self.db_cursor = None

    def get_conn(self):

        if self.db_conn is None:
            self.db_conn = sqlite3.connect(self.db_name)

        return self.db_conn

    def get_cursor(self):

        if self.db_cursor is None:
            self.db_cursor = self.get_conn().cursor()

        return self.db_cursor

    def execute_query(self, sql_string):

        self.get_cursor().execute(sql_string)
        self.get_conn().commit()


def create_table(database_name):

    print('Creating Table in {}'.format(database_name))
    conn = sqlite3.connect(database_name)
    c = conn.cursor()
    c.execute("""CREATE TABLE `header` (`ID` INTEGER PRIMARY KEY AUTOINCREMENT,
                                        `record_identity` TEXT NOT NULL,
                                        `mainframe_identity` TEXT NOT NULL,
                                        `date_of_extract` TEXT NOT NULL,
                                        `time_of_extract` TEXT NOT NULL,
                                        `current_file_reference` TEXT NOT NULL,
                                        `last_file_reference` TEXT,
                                        `update_indicator` TEXT NOT NULL,
                                        `version` TEXT NOT NULL,
                                        `user_start_date` TEXT NOT NULL,
                                        `user_end_date` TEXT NOT NULL,
                                        `record_time_stamp` TEXT NOT NULL);""")
    conn.commit()
    c.close()
    conn.close()

def update_header_record(name,
                         record_identity,
                         mainframe_identity,
                         date_of_extract,
                         time_of_extract,
                         current_file_reference,
                         last_file_reference,
                         update_indicator,
                         version,
                         user_start_date,
                         user_end_date):

    db_conn = sqlite3.connect(name)
    c = db_conn.cursor()

    try:
        create_table(name)
    except Exception as e:
        print(e.args)

    try:
        sql_string = """   INSERT INTO 
                        `header`
                        (`ID`,
                        `record_identity`,
                        `mainframe_identity`,
                        `date_of_extract`,
                        `time_of_extract`,
                        `current_file_reference`,
                        `last_file_reference`,
                        `update_indicator`,
                        `version`,
                        `user_start_date`,
                        `user_end_date`,
                        `record_time_stamp`) 
                        VALUES 
                        (NULL,'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');
                        """.format(record_identity,
                                   mainframe_identity,
                                   date_of_extract,
                                   time_of_extract,
                                   current_file_reference,
                                   last_file_reference,
                                   update_indicator,
                                   version,
                                   user_start_date,
                                   user_end_date,
                                   time.time())

        final_sql_string = re.sub(r"\s+", " ", sql_string).strip()
        print(final_sql_string)
        c.execute(final_sql_string)
    except Exception as e:
        print('Exception')
        if 'no such table' in e.args:
            create_table(name)
            c.execute(final_sql_string)

    db_conn.commit()
    c.close()
    db_conn.close()

def run_db_query(query_string):

    conn = sqlite3.connect(database_name)
    c = conn.cursor()
    c.execute("""CREATE TABLE `header` (`ID` INTEGER PRIMARY KEY AUTOINCREMENT,
                                            `record_identity` TEXT NOT NULL,
                                            `mainframe_identity` TEXT NOT NULL,
                                            `date_of_extract` TEXT NOT NULL,
                                            `time_of_extract` TEXT NOT NULL,
                                            `current_file_reference` TEXT NOT NULL,
                                            `last_file_reference` TEXT,
                                            `update_indicator` TEXT NOT NULL,
                                            `version` TEXT NOT NULL,
                                            `user_start_date` TEXT NOT NULL,
                                            `user_end_date` TEXT NOT NULL,
                                            `record_time_stamp` TEXT NOT NULL);""")
    conn.commit()
    c.close()
    conn.close()