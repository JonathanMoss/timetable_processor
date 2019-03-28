import sqlite3
import re


class DBConnection:
    """This class provides sqlite database connectivity"""
    def __init__(self, name='default.db'):

        self.db_conn = sqlite3.connect(name)
        self.db_cursor = None
        self.db_name = name
        self.create_header_table()

    def get_conn(self):

        if self.db_conn is None:
            self.db_conn = sqlite3.connect(self.db_name)
        return self.db_conn

    def get_cursor(self):
        
        if self.db_cursor is None:
            self.db_cursor = self.get_conn().cursor()
        return self.db_cursor

    def run_select(self, sql_string):

        self.get_cursor().execute(sql_string)
        #self.get_conn().commit()
        return_value = self.db_cursor.fetchall()
        return return_value

    def execute_sql(self, sql_string, commit=False, last_row=False):
        
        self.get_cursor().execute(sql_string)
        if commit:
            self.get_conn().commit()
        row_count = self.get_cursor().rowcount
        if last_row:
            return self.get_cursor().lastrowid
        else:
            return row_count

    def clear_table(self, table):
        
        if self.check_table_exists(table):
            self.get_cursor().execute("""DELETE FROM `{}`;""".format(table))
            self.get_cursor().execute("""DELETE FROM `sqlite_sequence` WHERE name='{}';""".format(table))
            #self.get_conn().commit()

    def check_table_exists(self, table):
        
        sql_string = """SELECT count(*) 
                                        FROM sqlite_master 
                                        WHERE type = 'table' 
                                        AND name = '{}'""".format(table)
        if self.run_select(sql_string)[0][0] == 1:
            return True
        else:
            return False

    def drop_tables(self):

        self.clear_table('tbl_association')
        self.clear_table('tbl_basic_schedule')
        self.clear_table('tbl_extra_schedule')
        self.clear_table('tbl_header')
        self.clear_table('tbl_location')
        self.clear_table('tbl_origin')
        self.clear_table('tbl_terminating')
        self.clear_table('tbl_change_er')

    def create_header_table(self):
        """This method specifies the sql string to create the table to hold CIF file header data."""
        sql_string = """CREATE TABLE IF NOT EXISTS `tbl_header` (
                                        int_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        txt_mainframe_id TEXT(20) NOT NULL,
                                        txt_extract_date TEXT(6) NOT NULL,
                                        txt_extract_time TEXT(4) NOT NULL,
                                        txt_current_file_ref TEXT(7) NOT NULL,
                                        txt_last_file_ref TEXT(7),
                                        txt_update_indicator TEXT(1) NOT NULL,
                                        txt_version TEXT(1) NOT NULL,
                                        txt_start_date TEXT(6) NOT NULL,
                                        txt_end_date TEXT(4) NOT NULL,
                                        time_process_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP);"""
                                        
        self.execute_sql(self.format_sql(sql_string), True)
        
    def create_location_table(self):
        """This method specifies the sql string to create the table to hold the location data."""
        sql_string = """CREATE TABLE IF NOT EXISTS `tbl_location` (
                                        int_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        int_header_id INTEGER NOT NULL,
                                        txt_tiploc TEXT NOT NULL,
                                        txt_nlc TEXT NOT NULL,
                                        txt_tps_description TEXT NOT NULL,
                                        txt_stanox TEXT NOT NULL,
                                        txt_alpha TEXT);"""
                                        
        self.execute_sql(self.format_sql(sql_string))
        
    def tiploc_delete(self, tiploc):
        """This method is called when a DELETE TIPLOC record is received."""
        try:
            filtered_tiploc = re.match('^[A-Z0-9]{3,7}$', tiploc).group().upper()
            sql_string = """DELETE FROM `tbl_location` WHERE `txt_tiploc` = '{}';""".format(filtered_tiploc)

            row_count = int(self.execute_sql(self.format_sql(sql_string)))

            return row_count
        except Exception as e:
            print('Bad TIPLOC passed to function')
            return 0
            
    def tiploc_ammend(self, details):
        """This method is called when a delete TIPLOC AMEND record is received."""
        tiploc = details['tiploc']
        nlc = details['nlc']
        tps_description = details['tps_description']
        stanox = details['stanox']
        alpha = details['alpha']
        if details['new_tiploc'].isspace():
            new_tiploc = details['tiploc']
        else:
            new_tiploc = details['new_tiploc']

        sql_string = """
        UPDATE `tbl_location` 
        SET 
        `txt_nlc` = '{}', 
        `txt_tps_description` = '{}',
        `txt_stanox` = '{}',
        `txt_alpha` = '{}',
        `txt_tiploc` = '{}'
        WHERE `txt_tiploc` = '{}';
        """.format(nlc, tps_description, stanox, alpha, new_tiploc, tiploc)
        row_count = int(self.execute_sql(self.format_sql(sql_string)))
        return row_count
        
    def create_association_table(self):
        """This method specifies the sql string to create the table to hold association data."""
        sql_string = """CREATE TABLE IF NOT EXISTS `tbl_association` (
                                        int_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        int_header_id INTEGER NOT NULL,
                                        txt_transaction_type TEXT(1) NOT NULL,
                                        txt_main_uid TEXT(6) NOT NULL,
                                        txt_secondary_uid TEXT(6) NOT NULL,
                                        txt_start_date TEXT(6) NOT NULL,
                                        txt_end_date TEXT(6) NOT NULL,
                                        txt_days_run TEXT(7) NOT NULL,
                                        txt_date_indicator TEXT(1) NOT NULL,
                                        txt_tiploc TEXT(7) NOT NULL,
                                        txt_main_suffix TEXT(1) NOT NULL,
                                        txt_secondary_suffix TEXT(1) NOT NULL,
                                        txt_stp_indicator TEXT(1) NOT NULL);"""
                                        
        self.execute_sql(self.format_sql(sql_string), commit=True)

    def insert_association(self, parameters):

        sql_string = """
        INSERT INTO `tbl_association` 
        (`int_header_id`, 
        `txt_transaction_type`, 
        `txt_main_uid`, 
        `txt_secondary_uid`, 
        `txt_start_date`, 
        `txt_end_date`, 
        `txt_days_run`,
        `txt_date_indicator`,
        `txt_tiploc`,
        `txt_main_suffix`,
        `txt_secondary_suffix`,
        `txt_stp_indicator`)
        VALUES
        ({}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');
        """.format( parameters['header_id'],
                    parameters['transaction_type'],
                    parameters['main_uid'],
                    parameters['secondary_uid'],
                    parameters['start_date'],
                    parameters['end_date'],
                    parameters['days_run'],
                    parameters['date_indicator'],
                    parameters['tiploc'],
                    parameters['main_suffix'],
                    parameters['secondary_suffix'],
                    parameters['stp_indicator'])

        return self.format_sql(sql_string)

    def create_basic_schedule_table(self):
        """This method specifies the sql string to create the table to hold basic schedule data."""
        sql_string = """CREATE TABLE IF NOT EXISTS `tbl_basic_schedule` (
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
                                        txt_stp_indicator TEXT);"""
        
        self.execute_sql(self.format_sql(sql_string))

    @staticmethod
    def parse_basic_schedule(bs_record):

        return dict({'transaction_type': bs_record[2:3],
                     'uid': bs_record[3:9],
                     'start_date': bs_record[9:15],
                     'end_date': bs_record[15:21],
                     'days_run': bs_record[21:28],
                     'bank_holiday': bs_record[28:29],
                     'status': bs_record[29:30],
                     'category': bs_record[30:32],
                     'identity': bs_record[32:36],
                     'headcode': bs_record[36:40],
                     'service_code': bs_record[41:49],
                     'portion_id': bs_record[49:50],
                     'power_type': bs_record[50:53],
                     'timing_load': bs_record[53:57],
                     'speed': bs_record[57:60],
                     'op_char': bs_record[60:66],
                     'train_class': bs_record[66:67],
                     'txt_sleepers': bs_record[67:68],
                     'reservations': bs_record[68:69],
                     'catering': bs_record[70:74],
                     'stp_indicator': bs_record[79:80]})

    def create_extra_schedule_table(self):

        sql_string = """CREATE TABLE IF NOT EXISTS `tbl_extra_schedule` (
                        int_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        int_basic_id INTEGER NOT NULL,
                        txt_atoc_code TEXT,
                        txt_applicable_code TEXT,
                        txt_retail_service_id TEXT);"""

        self.execute_sql(self.format_sql(sql_string))

    @staticmethod
    def parse_extra_schedule(bx_record):

        return dict({'atoc_code': bx_record[11:13],
                     'applicable_code': bx_record[13:14],
                     'retail_service_id': bx_record[14:22]})

    def insert_extra_schedule(self, basic_id, bx_record):

        sql_string = """
        INSERT INTO `tbl_extra_schedule` (
        `int_basic_id`,
        `txt_atoc_code`,
        `txt_applicable_code`,
        `txt_retail_service_id`)
        VALUES ({}, '{}', '{}', '{}');""".format(basic_id, bx_record['atoc_code'],
                                                 bx_record['applicable_code'],
                                                 bx_record['retail_service_id'])

        #self.execute_sql(self.format_sql(sql_string))
        return sql_string

    def insert_basic_schedule(self, header_id, bs_record):

        sql_string = """
        INSERT INTO `tbl_basic_schedule` (
        `int_header_id`, 
        `txt_transaction_type`, 
        `txt_uid`, 
        `txt_start_date`, 
        `txt_end_date`, 
        `txt_days_run`,
        `txt_bank_holiday`,
        `txt_status`,
        `txt_category`,
        `txt_identity`,
        `txt_headcode`,
        `txt_service_code`,
        `txt_portion_id`,
        `txt_power_type`,
        `txt_timing_load`,
        `txt_speed`,
        `txt_op_char`,
        `txt_train_class`,
        `txt_sleepers`,
        `txt_reservations`,
        `txt_catering`,
        `txt_stp_indicator`) VALUES (
        {}, '{}', '{}', '20{}-{}-{}', '20{}-{}-{}', '{}', '{}', '{}', '{}', 
        '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');""".format(
                    header_id,
                    bs_record['transaction_type'],
                    bs_record['uid'],
                    bs_record['start_date'][0:2],
                    bs_record['start_date'][2:4],
                    bs_record['start_date'][4:6],
                    bs_record['end_date'][0:2],
                    bs_record['end_date'][2:4],
                    bs_record['end_date'][4:6],
                    bs_record['days_run'],
                    bs_record['bank_holiday'],
                    bs_record['status'],
                    bs_record['category'],
                    bs_record['identity'],
                    bs_record['headcode'],
                    bs_record['service_code'],
                    bs_record['portion_id'],
                    bs_record['power_type'],
                    bs_record['timing_load'],
                    bs_record['speed'],
                    bs_record['op_char'],
                    bs_record['train_class'],
                    bs_record['txt_sleepers'],
                    bs_record['reservations'],
                    bs_record['catering'],
                    bs_record['stp_indicator'])

        #self.get_cursor().execute(self.format_sql(sql_string))
        #self.get_conn().commit()
        return_value = self.db_cursor.lastrowid
        #return return_value
        return sql_string

    def create_origin_location_table(self):
        """This method specified the sql string to create the table to hold origin location details."""
        sql_string = """CREATE TABLE IF NOT EXISTS `tbl_origin` (
                                        int_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        int_basic_schedule_id INTEGER NOT NULL,
                                        txt_location_and_suffix TEXT NOT NULL,
                                        txt_wtt_dep_time TEXT NOT NULL,
                                        txt_public_dep_time TEXT,
                                        txt_platform TEXT,
                                        txt_line_out TEXT,
                                        txt_eng_allowance TEXT,
                                        txt_pathing_allowance TEXT,
                                        txt_activity TEXT,
                                        txt_perf_allowance TEXT);"""
        
        self.execute_sql(self.format_sql(sql_string))

    def parse_time(self, time_string):

        if str(time_string).strip() == '':
            return ''
        else:
            hour = time_string[0:2]
            minute = time_string[2:4]
            second = "00"
            if len(time_string) == 5:
                if time_string[4] == "H":
                    second = "30"

        return '{}:{}:{}'.format(hour, minute, second)

    def parse_origin_record(self, lo_record):

        return dict({'location_and_suffix': lo_record[2:10], 
                     'wtt_dep_time': self.parse_time(lo_record[10:15]), 
                     'public_dep_time': self.parse_time(lo_record[15:19]), 
                     'platform': lo_record[19:22],
                     'line_out': lo_record[22:25],
                     'eng_allowance': lo_record[25:27],
                     'pathing_allowance': lo_record[27:29],
                     'activity': lo_record[29:41],
                     'perf_allowance': lo_record[41:43]})
    
    def insert_origin_record(self, basic_id, lo_record):
        
        sql_string = """
        INSERT INTO `tbl_origin` (
        `int_basic_schedule_id`,
        `txt_location_and_suffix`,
        `txt_wtt_dep_time`,
        `txt_public_dep_time`,
        `txt_platform`,
        `txt_line_out`,
        `txt_eng_allowance`,
        `txt_pathing_allowance`,
        `txt_activity`,
        `txt_perf_allowance`)
        VALUES ({}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}'
        );""".format(basic_id,
                     lo_record['location_and_suffix'],
                     lo_record['wtt_dep_time'],
                     lo_record['public_dep_time'],
                     lo_record['platform'],
                     lo_record['line_out'],
                     lo_record['eng_allowance'],
                     lo_record['pathing_allowance'],
                     lo_record['activity'],
                     lo_record['perf_allowance'])

        #self.execute_sql(self.format_sql(sql_string))
        return sql_string

    def create_intermediate_location_table(self):
        """This method specifies the sql string to create the table to hold intermediate location details."""
        sql_string = """CREATE TABLE IF NOT EXISTS `tbl_intermediate` (
                                        int_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        int_basic_schedule_id INTEGER NOT NULL,
                                        txt_location_and_suffix TEXT NOT NULL,
                                        txt_wtt_arr_time TEXT,
                                        txt_wtt_dep_time TEXT,
                                        txt_wtt_pass_time TEXT,
                                        txt_public_arr_time TEXT,
                                        txt_public_dep_time TEXT,
                                        txt_platform TEXT,
                                        txt_path_in TEXT,
                                        txt_line_out TEXT,
                                        txt_activity TEXT,
                                        txt_eng_allowance TEXT,
                                        txt_pathing_allowance TEXT,
                                        txt_performance TEXT);"""
        
        self.execute_sql(self.format_sql(sql_string))

    def parse_intermediate_location(self, index, li_record):

        return dict({
            'int_basic_schedule_id': index,
            'location_and_suffix': li_record[2:10],
            'wtt_arr_time': self.parse_time(li_record[10:15]),
            'wtt_dep_time': self.parse_time(li_record[15:20]),
            'wtt_pass_time': self.parse_time(li_record[20:25]),
            'public_arr_time': self.parse_time(li_record[25:29]),
            'public_dep_time': self.parse_time(li_record[29:33]),
            'platform': li_record[33:36],
            'path_in': li_record[39:42],
            'line_out': li_record[36:39],
            'activity': li_record[42:54],
            'eng_allowance': li_record[54:56],
            'pathing_allowance': li_record[56:58],
            'performance': li_record[58:60]})

    def insert_intermediate_location(self, basic_id, li_record):

        sql_string = """
        INSERT INTO `tbl_intermediate`
        (`int_basic_schedule_id`,
        `txt_location_and_suffix`,
        `txt_wtt_arr_time`,
        `txt_wtt_dep_time`,
        `txt_wtt_pass_time`,
        `txt_public_arr_time`,
        `txt_public_dep_time`,
        `txt_platform`,
        `txt_path_in`,
        `txt_line_out`,
        `txt_activity`,
        `txt_eng_allowance`,
        `txt_pathing_allowance`,
        `txt_performance`)
        VALUES
        ({}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');
        """.format(basic_id,
                    li_record['location_and_suffix'],
                    li_record['wtt_arr_time'],
                    li_record['wtt_dep_time'],
                    li_record['wtt_pass_time'],
                    li_record['public_arr_time'],
                    li_record['public_dep_time'],
                    li_record['platform'],
                    li_record['path_in'],
                    li_record['line_out'],
                    li_record['activity'],
                    li_record['eng_allowance'],
                    li_record['pathing_allowance'],
                    li_record['performance'])

        #self.execute_sql(self.format_sql(sql_string))
        return sql_string

    def create_terminating_location_table(self):
        """This method specifies the sql string to create the table to hold terminating location details."""
        sql_string = """CREATE TABLE IF NOT EXISTS `tbl_terminating` (
                                        int_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        int_basic_schedule_id INTEGER NOT NULL,
                                        txt_location_and_suffix TEXT NOT NULL,
                                        txt_wtt_arr_time TEXT NOT NULL,
                                        txt_public_arr_time TEXT,
                                        txt_platform TEXT,
                                        txt_path_in TEXT,
                                        txt_activity TEXT);"""
                                        
        self.execute_sql(self.format_sql(sql_string))


    def insert_terminating_location(self, basic_id, lt_record):

        sql_string = """
                INSERT INTO `tbl_terminating` (
                `int_basic_schedule_id`,
                `txt_location_and_suffix`,
                `txt_wtt_arr_time`,
                `txt_public_arr_time`,
                `txt_platform`,
                `txt_path_in`,
                `txt_activity`)
                VALUES ({}, '{}', '{}', '{}', '{}', '{}', '{}'
                );""".format(basic_id,
                             lt_record['location_and_suffix'],
                             lt_record['wtt_arr_time'],
                             lt_record['public_arr_time'],
                             lt_record['platform'],
                             lt_record['path_in'],
                             lt_record['activity'])

        # self.execute_sql(self.format_sql(sql_string))
        return sql_string

    def parse_terminating_location(self, lt_record):

        return dict({
            'location_and_suffix': lt_record[2:10],
            'wtt_arr_time': self.parse_time(lt_record[10:15]),
            'public_arr_time': self.parse_time(lt_record[15:19]),
            'platform': lt_record[19:22],
            'path_in': lt_record[22:25],
            'activity': lt_record[25:37]})
    
    def create_change_er_table(self):
        """This method specified the sql string to create the table to hold change-en-route details."""
        sql_string = """CREATE TABLE IF NOT EXISTS `tbl_change_er` (
                                        int_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        int_basic_schedule_id INTEGER NOT NULL,
                                        txt_location_and_suffix TEXT NOT NULL,
                                        txt_train_cat TEXT,
                                        txt_train_identity TEXT,
                                        txt_headcode TEXT,
                                        txt_course_ind TEXT,
                                        txt_service_code TEXT,
                                        txt_business_sector TEXT,
                                        txt_power_type TEXT,
                                        txt_timing_load TEXT,
                                        txt_speed TEXT,
                                        txt_op_char TEXT,
                                        txt_train_class TEXT,
                                        txt_sleepers TEXT,
                                        txt_reservation TEXT,
                                        txt_connection_ind TEXT,
                                        txt_catering_code TEXT,
                                        txt_service_brand TEXT,
                                        txt_traction_class TEXT,
                                        txt_uic TEXT);"""
        
        self.execute_sql(self.format_sql(sql_string))

    @staticmethod
    def format_sql (sql_string):
        """This method formats the sql by removing spaces and new lines."""
        return re.sub(r" {2,}|\n", "", sql_string.strip())

    def create_current_table(self):

        sql_string = """
        CREATE TABLE 
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
                txt_stp_indicator TEXT)
        """

        self.execute_sql(self.format_sql(sql_string))
        self.execute_sql('DELETE FROM `tbl_current_schedule`', commit=True)

    def delete_current():
        sql_string = 'DELETE FROM tbl_current_'


if __name__ == '__main__':
    db = DBConnection('test.db')
    db.tiploc_delete('CREWE')
    db.tiploc_ammend({    'nlc': 'nlc', 
                                        'nlc_check': 'nlc_check', 
                                        'tps_description': 'tps_description',
                                        'stanox': 'stanox',
                                        'alpha': 'alpha',
                                        'new_tiploc': 'PADTON1',
                                        'tiploc': 'PADTON'})

