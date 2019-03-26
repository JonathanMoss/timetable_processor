#!/usr/bin/python3
import subprocess
import os
import logging
from pathlib import Path
from db_access import DBConnection
import re
import time
import datetime


class CifExtract:

    STP_IND = {"C": "CANCEL", "N": "NEW", "O": "STP", "P": "LTP"}

    # TODO: Need to check this; appears that it is not being used.
    ASSOC_TYPE = {"JJ": "JOIN", "VV": "SPLIT", "NP": "NEXT"}

    logging.basicConfig(level=logging.DEBUG,
                        filename='log.log',
                        format='%(asctime)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S')
    CIF_DB = './cif_record.db'  # The DB that is used to record downloaded CIF.gz and current database references.
    CIF_DIR = './cif'  # The directory that contains the un-archived CIF files.

    def __init__(self, file_name):

        # PATHS
        self.cif_file = file_name
        self.cif_dir = None
        self.working_dir = None
        self.db_file = None
        self.db_directory = None

        # DATABASE
        self.db_conn = None
        self.cif_db_conn = None
        self.header_row_id = None

        # TIPLOC INSERT RECORDS
        self.tot_tiploc_ins_in_cif = 0
        self.tiploc_ins_processed_count = 0

        # TIPLOC AMMEND
        self.tot_tiploc_amd_in_cif = 0
        self.tot_tiploc_amd_processed = 0

        # TIPLOC DELETE
        self.tot_tiploc_del_in_cif = 0
        self.tot_tiploc_del_processed = 0

        # ASSSOCIATION RECORDS
        self.tot_assoc_records_in_cif = 0
        self.tot_new_assoc_in_cif = 0
        self.tot_rev_assoc_in_cif = 0
        self.tot_del_assoc_in_cif = 0
        self.tot_assoc_records_processed = 0

        # GENERAL PROCESSING
        self.full_cif = False

        # Start the processing - get the header record
        self.get_header()

    def grep(self, search_string):

        """This function runs a grep process as return the resulting matches"""

        process = subprocess.Popen(['grep', search_string, self.cif_file], stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return stdout.decode('utf-8')

    def get_header(self):

        """This function gets the HEADER RECORD from the CIF and validates pre-processing"""

        # Check if the cif file passed at initialisation exists
        if os.path.isfile(self.cif_file):
            self.cif_file = os.path.abspath(self.cif_file)
            self.working_dir = Path(os.path.split(self.cif_file)[0]).parent
            logging.debug('CIF found... {}'.format(self.cif_file))
        else:
            # CIF does not exist - cannot continue!
            logging.error('{} is not a valid file (no header record found), exiting...'.format(self.cif_file))
            exit()

        logging.debug('Looking for header record in {}'.format(self.cif_file))
        header = self.grep('^HD')  # Grep the file and return the header record.

        if len(header) == 81:  # Check the header record contains the specified length & parse.
            logging.debug('Header found in {}, parsing...'.format(self.cif_file))

            # Parse header record...
            record_identity = header[0:2]
            mainframe_identity = header[2:22]
            date_of_extract = header[22:28]
            time_of_extract = header[28:32]
            current_file_reference = header[32:39]
            last_file_reference = header[39:46]
            update_indicator = header[46]
            if update_indicator == 'F':
                self.full_cif = True  # Needed as some of the code is different for full or update CIF.
            version = header[47]
            start_date = header[48:54]
            end_date = header[54:60]

            # This creates the text that is written into the header text file
            header_text = ('Record_Identity: {}\n'
                           'Mainframe Identity: {}\n'
                           'Date of Extract: {}\n'
                           'Time of Extract: {}\n'
                           'Current File Reference: {}\n'
                           'Last File Reference: {}\n'
                           'Update Indicator: {}\n'
                           'Version: {}\n'
                           'Start Date: {}\n'
                           'End Date: {}\n'.format(record_identity,
                                                   mainframe_identity,
                                                   date_of_extract,
                                                   time_of_extract,
                                                   current_file_reference,
                                                   last_file_reference,
                                                   update_indicator,
                                                   version,
                                                   start_date,
                                                   end_date))

            self.db_file = '{}({}).db'.format(current_file_reference, version)
            self.db_directory = os.path.join(self.working_dir, '{}({})'.format(current_file_reference, version))

            # Check if the CIF has already been processed - check CIF_DB
            self.cif_db_conn = DBConnection(CifExtract.CIF_DB)
            sql_string = 'SELECT COUNT(*) FROM `tbl_header` WHERE `txt_current_file_ref` LIKE "{}";'.format(current_file_reference)
            ret_records = self.cif_db_conn.run_select(sql_string)
            if str(ret_records[0][0]).strip() == '0':
                # No entry in cif_record.db, thus it has not been processed - update database...
                logging.debug('CIF reference has not been processed, continuing...')
                self.cif_db_conn.execute_sql(self.cif_db_conn.format_sql("""
                INSERT INTO 
                    `tbl_header` ( 
                        `txt_mainframe_id`, 
                        `txt_extract_date`, 
                        `txt_extract_time`, 
                        `txt_current_file_ref`, 
                        `txt_last_file_ref`,
                        `txt_update_indicator`,
                        `txt_version`,
                        `txt_start_date`,
                        `txt_end_date`) 
                    VALUES (
                        '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(mainframe_identity,
                                                                                        date_of_extract,
                                                                                        time_of_extract,
                                                                                        current_file_reference,
                                                                                        last_file_reference,
                                                                                        update_indicator,
                                                                                        version,
                                                                                        start_date,
                                                                                        end_date)), True)

            else:
                # The CIF reference has already been processed, check version...
                logging.debug('CIF reference already processed, checking db for version conflicts...')
                sql_string = """
                SELECT 
                    COUNT(*) 
                FROM `tbl_header` 
                WHERE `txt_current_file_ref` LIKE "{}" 
                AND `txt_version` LIKE "{}";""".format(current_file_reference,
                                                       version)

                # Run the query to find further matches based on current file reference and version
                ret_records = self.cif_db_conn.run_select(self.cif_db_conn.format_sql(sql_string))
                if str(ret_records[0][0]).strip() == '0':
                    # New Version of the same reference
                    sql_string = "SELECT `txt_version` FROM `tbl_header` WHERE `txt_current_file_ref` LIKE '{}';".format(current_file_reference)
                    ret_records = self.cif_db_conn.run_select(sql_string)
                    cif_version = str(version).strip()
                    conflicting_version = False

                    # Check that the new version is an incremental update to that already listed with the DB.
                    for ver in ret_records:
                        if ver[0] > cif_version:
                            conflicting_version = True

                    # Attempting to process CIF update out of sequence, cannot continue.
                    if conflicting_version:
                        logging.error('Attempting to process out of sequence CIF, cannot continue...')
                        exit()
                    else:
                        self.cif_db_conn.execute_sql(self.cif_db_conn.format_sql("""INSERT INTO `tbl_header` 
                                        (`txt_mainframe_id`, 
                                        `txt_extract_date`, 
                                        `txt_extract_time`, 
                                        `txt_current_file_ref`, 
                                        `txt_last_file_ref`,
                                        `txt_update_indicator`,
                                        `txt_version`,
                                        `txt_start_date`,
                                        `txt_end_date`) 
                                        VALUES ('{}', '{}', '{}', '{}',
                                        '{}', '{}', '{}', '{}', '{}')""".format(mainframe_identity,
                                                                                date_of_extract,
                                                                                time_of_extract,
                                                                                current_file_reference,
                                                                                last_file_reference,
                                                                                update_indicator,
                                                                                version,
                                                                                start_date,
                                                                                end_date)), True)

                else:
                    # Same Reference, Same Version - has already been processed. 
                    logging.error('Attempting to process same CIF version again; cannot continue...')
                    exit()

            if self.full_cif:
                if not os.path.isdir(self.db_directory):
                    logging.debug('Cant find database directory, creating {}'.format(self.db_directory))
                    os.mkdir(self.db_directory)  # Path not found - create Directory
                else:
                    logging.debug('Database directory {} already exists, moving on...'.format(self.db_directory))

                # Check if the CIF has already been processed - check database file?
                if not os.path.exists(os.path.join(self.db_directory, self.db_file)):
                    # Create database
                    self.db_conn = DBConnection(os.path.join(self.db_directory, self.db_file))
                    logging.debug('Creating {}'.format(os.path.join(self.db_directory, self.db_file)))
                else:
                    answer = input('This CIF has already been processed, continue? (YES/NO) ')
                    if answer != 'YES':
                        logging.warning('CIF already processed, user elected to exit')
                        exit()
                    else:
                        logging.warning('User elected to re-run CIF, deleting tables')
                        self.db_conn = DBConnection(os.path.join(self.db_directory, self.db_file))
                        self.db_conn.drop_tables()          
            else:
                # Update CIF - get the current CIF DB reference.
                db_file = self.cif_db_conn.run_select('SELECT * FROM tbl_current_cif')[0][0]
                self.db_conn = DBConnection(db_file)
                self.working_dir = os.path.dirname(db_file)

            # Write a text file that contains the header record information
            if self.full_cif:
                file_name = 'HEADER.TXT'  # Full CIF
                fp = os.path.join(self.working_dir, '{}({})'.format(current_file_reference,
                                                                     version), file_name)
            else:  # Update CIF
                file_name = '{}({}).txt'.format(current_file_reference, version)
                fp = os.path.join(self.working_dir, file_name)

            with open(fp, 'w') as header_file:
                header_file.write(header_text)  # Write HEADER.txt

            # Insert the header record into the database table header table.
            self.db_conn.execute_sql(self.db_conn.format_sql("""INSERT INTO `tbl_header` 
                                        (`txt_mainframe_id`, 
                                        `txt_extract_date`, 
                                        `txt_extract_time`, 
                                        `txt_current_file_ref`, 
                                        `txt_last_file_ref`,
                                        `txt_update_indicator`,
                                        `txt_version`,
                                        `txt_start_date`,
                                        `txt_end_date`) 
                                        VALUES ('{}', '{}', '{}', '{}',
                                        '{}', '{}', '{}', '{}', '{}')""".format(mainframe_identity,
                                                                                date_of_extract,
                                                                                time_of_extract,
                                                                                current_file_reference,
                                                                                last_file_reference,
                                                                                update_indicator,
                                                                                version,
                                                                                start_date,
                                                                                end_date)), True)

            # Get the header row - this is needed to ensure that each BS knows which CIF it came from.
            self.header_row_id = self.db_conn.run_select('SELECT '
                                                         '`int_record_id` '
                                                         'FROM `tbl_header` '
                                                         'WHERE `txt_current_file_ref` = "{}" '
                                                         'AND `txt_version` = "{}";'.format(current_file_reference,
                                                                                            version))[0][0]

        else:

            logging.error('No valid header file found in {}, exiting programme'.format(self.cif_file))
            exit()

        # Full CIF - Update the CIF .db to show which full CIF is being used as the base CIF.
        if self.full_cif:
            self.cif_db_conn.execute_sql('DELETE FROM `tbl_current_cif`;', True)  # Delete the previous reference
            sql_string = 'INSERT INTO `tbl_current_cif` (`txt_current_cif`) VALUES ("{}");'.format(os.path.join(self.db_directory, self.db_file))
            self.cif_db_conn.execute_sql(sql_string, True)

        self.get_tiploc_inserts()

    def get_tiploc_inserts(self):

        """This function parses the TI (TIPLOC INSERT) Records from the CIF and updates the database."""

        start_time = time.time() # Start the timer
        logging.debug('Parsing TIPLOC INSERT Records...')
        self.db_conn.create_location_table() # Make sure that the db table has been created
        ret_val = self.grep('^TI') # Grep statement - looking for TIPLOC insert records
        self.tot_tiploc_ins_in_cif = int('{:.0f}'.format(len(ret_val) / 81))
        logging.debug('{} TIPLOC INSERT records found in {}'.format(self.tot_tiploc_ins_in_cif, self.cif_file))

        if self.tot_tiploc_ins_in_cif != 0: # Check to make sure there are some TIPLOC insert records before we carry on

            for line in ret_val.splitlines(): # Parse the returned records, line by line.
                self.tiploc_ins_processed_count += 1 # Increase the tally for records processed.
                tiploc = line[2:9].strip() # Get the TIPLOC
                nlc = line[11:17] # Get the NLC
                desc = re.sub(r'"', '', line[18:44]).strip() # Get the TPS Description
                stanox = line[44:49] # Get the STANOX
                alpha = line[53:56] # Get the 3 letter alpha code; if blank, insert ***
                if alpha.isspace():
                    alpha = "***"

                # Format the SQL String
                self.db_conn.execute_sql("""INSERT INTO `tbl_location` (
                                            `int_header_id`, 
                                            `txt_tiploc`, 
                                            `txt_nlc`, 
                                            `txt_tps_description`, 
                                            `txt_stanox`, 
                                            `txt_alpha`) 
                                            VALUES 
                                            ('{}', '{}', '{}', "{}", '{}', '{}')""".format(self.header_row_id,
                                                                                           tiploc,
                                                                                           nlc,
                                                                                           desc,
                                                                                           stanox,
                                                                                           alpha))
                
                # Each 1000 TIPLOC INSERTS, update the log with a suitable entry.
                if self.tiploc_ins_processed_count and self.tiploc_ins_processed_count % 1000 == 0:
                    time_now = datetime.datetime.now()
                    current_time = time.time()
                    elapsed_seconds = current_time - start_time
                    estimated_time_per_record = elapsed_seconds / self.tiploc_ins_processed_count
                    records_left = int(self.tot_tiploc_ins_in_cif) - self.tiploc_ins_processed_count
                    time_left = str(datetime.timedelta(seconds=(estimated_time_per_record * records_left))).split('.')[0]
                    est_time_to_complete = time_now + datetime.timedelta(0, estimated_time_per_record * records_left)

                    logging.debug(
                        'Parsed {}/{} TIPLOC INSERT records in {:.0f} seconds - {} records remaining '
                        '({} HH:MM:SS to complete (ETA: {:%H:%M:%S}))'.format(self.tiploc_ins_processed_count,
                                                                               self.tot_tiploc_ins_in_cif,
                                                                               elapsed_seconds,
                                                                               records_left,
                                                                               time_left,
                                                                               est_time_to_complete))

            self.db_conn.get_conn().execute('COMMIT') # Update the database
            end_time = time.time() # Finished processing TI records, Stop the clock
            total_time = (end_time - start_time)
            
            # Give the user a summary
            if total_time <= 59:
                logging.debug(
                    'Parsed {}/{} TIPLOC INSERT records in {:.2f} seconds'.format(self.tiploc_ins_processed_count, self.tot_tiploc_ins_in_cif, total_time))
            else:
                logging.debug(
                    'Parsed {}/{} TIPLOC INSERT records in {:.2f} MM:SS'.format(self.tiploc_ins_processed_count, self.tot_tiploc_ins_in_cif,
                                                                           total_time / 60))
            logging.debug('{} CIF TIPLOC INSERT records parsed.'.format(self.tiploc_ins_processed_count))

        else:  # No TIPLOC INSERT records contained within the CIF
            logging.debug('No TIPLOC INSERT records found within {}'.format(self.cif_file))

        self.get_tiploc_amends()

    def get_tiploc_amends(self):

        """This function takes the TI records from the CIF and updates those present in the database"""

        logging.debug('Parsing TIPLOC AMMEND records...')

        ret_val = self.grep('^TA') # GREP Statement

        # The total of TIPLOC AMMEND records within the database.
        self.tot_tiploc_amd_in_cif = '{:.0f}'.format(len(ret_val) / 81)
        logging.debug('{} TIPLOC AMMEND records found in {}'.format(self.tot_tiploc_amd_in_cif,
                                                                    self.cif_file))  # Show an appropriate log entry.
        self.tot_tiploc_amd_processed = 0

        if int(self.tot_tiploc_amd_in_cif) > 0:  # Make sure there are some records before we carry on.
            for line in ret_val.splitlines():  # Split the records up - parse line by line.

                tiploc = line[2:9].strip()
                nlc = line[11:17]
                desc = line[18:44].strip()
                stanox = line[44:49]
                alpha = line[53:56]
                new_tiploc = line[72: 79]
                if alpha.isspace():
                    alpha = "***"
                amend_dict = {'tiploc': tiploc,
                              'nlc': nlc,
                              'tps_description': desc,
                              'stanox': stanox,
                              'alpha': alpha,
                              'new_tiploc': new_tiploc}

                # Send the the database and confirm update action.
                if self.db_conn.tiploc_ammend(amend_dict) == 1:
                    self.tot_tiploc_amd_processed += 1
            logging.debug('{}/{} location amendments processed'.format(self.tot_tiploc_amd_processed,
                                                                       self.tot_tiploc_amd_in_cif))
        else:
            # No TIPLOC AMEND records within the CIF; make a log entry.
            logging.debug('No TIPLOC AMMEND records contained within {}'.format(self.cif_file))

        self.get_tiploc_delete()

    def get_tiploc_delete(self):

        """This function parses the TD records contained in the CIF and deletes the corresponding record from the DB"""

        logging.debug('Parsing TIPLOC DELETE records...')

        ret_val = self.grep('^TD')  # Grep statement for TIPLOC DELETE record

        self.tot_tiploc_del_in_cif = '{:.0f}'.format(len(ret_val) / 81)  # This is the total if TD records found

        # Suitable summary within the log
        logging.debug('{} TIPLOC DELETE record(s) found in {}'.format(self.tot_tiploc_del_in_cif, self.cif_file))

        if int(self.tot_tiploc_del_in_cif) > 0:  # Make sure there are some records to process
            for line in ret_val.splitlines():  # Split and parse each entry
                tiploc = line[2:9].strip()  # Get the TIPLOC
                if self.db_conn.tiploc_delete(tiploc) == 1:  # Run the SQL query to delete the record.
                    self.tot_tiploc_del_processed += 1  # Increment the Deleted TIPLOC tally.

            # Suitable log summary entry...
            logging.debug('{}/{} location deletions processed'.format(self.tot_tiploc_del_processed,
                                                                  self.tot_tiploc_del_in_cif))
        else:
            # There are no TIPLOC DELETE records within the CIF; make a log entry.
            logging.debug('No TIPLOC DELETE records found within {}'.format(self.cif_file))

        self.get_associations()

    def get_associations(self):

        """This function parsed the AA records from the CIF and inserts into the database"""

        start_time = time.time()  # Start the clock
        logging.debug('Parsing associations data...')
        self.db_conn.create_association_table()  # Make sure the database contains the correct table.
        ret_val = self.grep('^AA')  # GREP expression to search for Association (AA) records
        self.tot_assoc_records_in_cif = '{:.0f}'.format(len(ret_val) / 81)
        logging.debug('{} Association record(s) found in {}'.format(self.tot_assoc_records_in_cif, self.cif_file))
        self.tot_new_assoc_in_cif = 0  # Tally for new associations
        self.tot_rev_assoc_in_cif = 0  # Tally for revised associations
        self.tot_del_assoc_in_cif = 0  # Tally for deleted associations
        self.tot_assoc_records_processed = 0  # Tally for all association types processed
        assoc_string = ""  # A string that will contain the sql statements/
        self.db_conn.get_conn().execute('BEGIN TRANSACTION')  # Begin the database transaction.

        for line in ret_val.splitlines():  # Split the AA records - parse line by line.

            self.tot_assoc_records_processed += 1  # Increment total processed counter
            assoc_type = line[2:3]  # Transaction Type: N-New, D-Delete, R-Revise
            if assoc_type == 'N':  # NEW
                self.tot_new_assoc_in_cif += 1  # Increment Tally
                uids = (line[3:9], line[9:15])  # BASE, ASSOC
                dates = (line[15:21], line[21:27])  # BASE, ASSOC (YYMMDD)
                days_run = line[27:34]

                try:
                    activity = CifExtract.ASSOC_TYPE[line[34:36]]
                except Exception:
                    activity = CifExtract.ASSOC_TYPE["NP"]

                date_indicator = line[36: 37]
                if date_indicator.isspace():
                    date_indicator = 'S'
                tiploc = line[37:44].strip()
                base_suffix = line[44:45]
                assoc_suffix = line[45:46]

                try:
                    stp_indicator = CifExtract.STP_IND[line[79:80]]
                except:
                    stp_indicator = CifExtract.STP_IND["N"]

                # Create a dictionary object that contains the AA Record as Key, Value pairs.
                tmp_dict = {'header_id': self.header_row_id,
                            'transaction_type': assoc_type,
                            'main_uid': uids[0],
                            'secondary_uid': uids[1],
                            'start_date': dates[0],
                            'end_date': dates[1],
                            'days_run': days_run,
                            'date_indicator': date_indicator,
                            'tiploc': tiploc,
                            'main_suffix': base_suffix,
                            'secondary_suffix': assoc_suffix,
                            'stp_indicator': stp_indicator}

                # Add the statement to the SQL string ready for bulk processing.
                assoc_string += self.db_conn.insert_association(tmp_dict)

            elif assoc_type == 'D':  # Delete Association
                self.tot_del_assoc_in_cif += 1
                # TODO: Not sure if these should be deleted (ie. from the db or shown as 'C'
            elif assoc_type == 'R':  # Revise Association
                self.tot_rev_assoc_in_cif += 1
                # TODO: Either update in the DB or show as an overlay to the existing Association - need to clarify.

            # Update the database each 1000 records, or when complete (100% processed)
            if self.tot_assoc_records_processed and self.tot_assoc_records_processed % 1000 == 0:
                for sql in assoc_string.split(';'):
                    self.db_conn.get_conn().execute(sql)  # process each SQL statement
                self.db_conn.get_conn().execute('COMMIT')  # Commit into the database.
                assoc_string = ""  # Clear the sql string.

                #  Calculate progress and timings
                time_now = datetime.datetime.now()
                current_time = time.time()
                elapsed_seconds = current_time - start_time
                estimated_time_per_record = elapsed_seconds / self.tot_assoc_records_processed
                records_left = int(self.tot_assoc_records_in_cif) - self.tot_assoc_records_processed
                time_left = str(datetime.timedelta(seconds=(estimated_time_per_record * records_left))).split('.')[0]
                est_time_to_complete = time_now + datetime.timedelta(0, estimated_time_per_record * records_left)

                logging.debug(
                        'Parsed {}/{} association records in {:.0f} seconds - {} records remaining '
                        '({} HH:MM:SS to complete (ETA: {:%H:%M:%S}))'.format(self.tot_assoc_records_processed,
                                                                              self.tot_assoc_records_in_cif,
                                                                              elapsed_seconds,
                                                                              records_left,
                                                                              time_left,
                                                                              est_time_to_complete))
        # Make sure all remaining SQL statements have been processed
        if assoc_string:
            for sql in assoc_string.split(';'):
                self.db_conn.get_conn().execute(sql)  # process each SQL statement
            
        end_time = time.time()  # Stop the clock - log a summary.
        total_time = (end_time - start_time)
        if total_time <= 59:
            logging.debug(
                'Parsed {}/{} Association records in {:.2f} seconds'.format(self.tot_assoc_records_processed,
                                                                            self.tot_assoc_records_in_cif,
                                                                            total_time))
        else:
            logging.debug(
                'Parsed {}/{} Association records in {:.2f} MM:SS'.format(self.tot_assoc_records_processed,
                                                                          self.tot_assoc_records_in_cif,
                                                                          total_time / 60))

        if self.db_conn.get_conn().in_transaction:
            self.db_conn.get_conn().execute('COMMIT')  # Commit into the database.
        self.get_schedules()

    def get_last_row(self, db_table):

        """This function returns the last row used from the database for the table passed"""

        sql_string = 'SELECT seq from `sqlite_sequence` WHERE `sqlite_sequence`.`name` = "{}";'.format(db_table)
        next_row = self.db_conn.run_select(sql_string)
        if not next_row:
            return 0
        else:
            return next_row[0][0]

    def get_schedules(self):

        """This function takes the schedules from the CIF and inserts into the database"""

        self.db_conn.get_conn().execute('BEGIN TRANSACTION')
        total_processed = 0 # The total schedules that have been processed
        logging.debug('Parsing schedule data...')

        # Create the neccessary database tables (if they do not already exist)
        self.db_conn.create_basic_schedule_table()
        self.db_conn.create_extra_schedule_table()
        self.db_conn.create_origin_location_table()
        self.db_conn.create_intermediate_location_table()
        self.db_conn.create_terminating_location_table()
        self.db_conn.create_change_er_table()
        self.db_conn.get_conn().execute('COMMIT')

        # Grep to check how many schedules are contained within the CIF (Count BS records)
        process = subprocess.Popen(['grep', '-c', '^BS', self.cif_file], stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()
        schedule_count = int(stdout.decode('utf-8'))
        logging.debug('{} schedules found in {}'.format(schedule_count, self.cif_file))

        # Match all schedule records and assign to a variable.
        search_string = '^BS.*|^BX.*|^LO.*|^LI.*|^LT.*|^CR.*'
        process = subprocess.Popen(['egrep', search_string, self.cif_file], stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()
        schedules = stdout.decode('utf-8')
               
        index = self.get_last_row('tbl_basic_schedule') # Get the last row within the table.

        start_time = time.time() # Start the clock on the timer.

        sql_string = ""
        for line in schedules.splitlines(): # Iterate through each schedule line.
            if re.match('^BS', line): # Basic Schedule match
                index += 1 # Increment the index.
                total_processed += 1 # Show the processed tally +1

                # This ensures that we only commit to the database every 1000 records.
                if index % 1000 == 0:
                    if sql_string != "":
                        for stat in sql_string.split(';'):
                            self.db_conn.get_conn().execute(stat)
                        self.db_conn.get_conn().execute('COMMIT')

                        sql_string = ""

                        # Calculate how we are doing - log an entry to the log.
                        time_now = datetime.datetime.now()
                        current_time = time.time()
                        elapsed_seconds = current_time - start_time
                        estimated_time_per_record = elapsed_seconds / total_processed
                        records_left = int(schedule_count) - total_processed
                        time_left = str(datetime.timedelta(seconds=(estimated_time_per_record * records_left))).split('.')[0]
                        est_time_to_complete = time_now + datetime.timedelta(0,
                                                             estimated_time_per_record * records_left)

                        logging.debug(
                            'Parsed {}/{} schedule records in {:.0f} seconds - {} records remaining '
                            '({} HH:MM:SS to complete (ETA: {:%H:%M:%S}))'.format(total_processed,
                                                                                      schedule_count,
                                                                                      elapsed_seconds,
                                                                                      records_left,
                                                                                      time_left,
                                                                                      est_time_to_complete))
                sql_string += self.db_conn.insert_basic_schedule(self.header_row_id, self.db_conn.parse_basic_schedule(line))

            if re.match('^LO', line): # Origin record
                sql_string += self.db_conn.insert_origin_record(index, self.db_conn.parse_origin_record(line))
            if re.match('^LI', line): # Intermediate record
                sql_string += self.db_conn.insert_intermediate_location(index, self.db_conn.parse_intermediate_location(index, line))
            if re.match('^CR', line): # Change record - TODO
                pass
            if re.match('^BX', line): # Basic Extra record
                sql_string += self.db_conn.insert_extra_schedule(index, self.db_conn.parse_extra_schedule(line))
            if re.match('^LT', line): # Terminating record
                sql_string += self.db_conn.insert_terminating_location(index, self.db_conn.parse_terminating_location(line))

        # Mop up any schedules that have not been commited above.
        if sql_string != "":
            for stat in sql_string.split(';'):
                self.db_conn.get_conn().execute(stat)
            self.db_conn.get_conn().execute('COMMIT')

        # End of processing - show the user a summary
        end_time = time.time()
        total_time = (end_time - start_time)
        if total_time <= 59:
            logging.debug(
                'Parsed {}/{} schedule records in {:.2f} seconds'.format(total_processed, schedule_count, total_time))
        else:
            logging.debug(
                'Parsed {}/{} schedule records in {:.2f} MM:SS'.format(total_processed, schedule_count,
                                                                       total_time / 60))

    def success(self):

        """This function inserts a record into the database that summarises both success and what was processed."""

        # TIPLOC INSERT
        self.tot_tiploc_ins_in_cif = 0
        self.tiploc_ins_processed_count = 0
        # TIPLOC AMMEND
        self.tot_tiploc_amd_in_cif = 0
        self.tot_tiploc_amd_processed = 0
        # TIPLOC DELETE
        self.tot_tiploc_del_in_cif = 0
        self.tot_tiploc_del_processed = 0
        # ASSOCIATION RECORDS
        self.tot_assoc_records_in_cif = 0
        self.tot_new_assoc_in_cif = 0
        self.tot_rev_assoc_in_cif = 0
        self.tot_del_assoc_in_cif = 0
        self.tot_assoc_records_processed = 0


if __name__ == "__main__":

    cif_files = ['toc-full.cif', ]
    for cif in cif_files:
        CifExtract(os.path.join(CifExtract.CIF_DIR, cif))
