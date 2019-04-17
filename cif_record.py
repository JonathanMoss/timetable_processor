import sqlite3
import re

CIF_DB = './cif_record.db'


def format_sql(sql_str):
    """This method formats the sql by removing spaces and new lines."""
    return re.sub(r" {2,}|\n", "", sql_str.strip())


if __name__ == '__main__':

    with sqlite3.connect(CIF_DB) as con:
        c = con.cursor()

        sql_string = """
        DROP TABLE IF EXISTS `tbl_current_cif`; 
        CREATE TABLE IF NOT EXISTS `tbl_current_cif` 
            (`txt_current_cif` TEXT NOT NULL);
        DROP TABLE IF EXISTS `tbl_downloaded_cif`;
        CREATE TABLE IF NOT EXISTS `tbl_downloaded_cif` 
            (`int_index` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
            `txt_filename` TEXT NOT NULL,
            `txt_uncompressed_filename` TEXT NOT NULL, 
            `txt_date_time` TEXT NOT NULL,
            `txt_processed_date_time` TEXT, 
            `int_success` INTEGER NOT NULL, 
            `txt_arguments` TEXT NOT NULL,
            `txt_mainframe_id` TEXT NOT NULL, 
            `txt_extract_date` TEXT NOT NULL, 
            `txt_extract_time` TEXT NOT NULL, 
            `txt_current_file_ref` TEXT NOT NULL, 
            `txt_last_file_ref` TEXT NOT NULL,
            `txt_update_indicator` TEXT NOT NULL,
            `txt_version` TEXT NOT NULL,
            `txt_start_date` TEXT NOT NULL,
            `txt_end_date` TEXT NOT NULL,
            `txt_uncompressed_size` TEXT NOT NULL,
            `txt_compressed_size` TEXT NOT NULL);
        DROP TABLE IF EXISTS `tbl_header`;
        CREATE TABLE IF NOT EXISTS `tbl_header` 
            (`int_record_id` INTEGER PRIMARY KEY AUTOINCREMENT, 
            `txt_mainframe_id` TEXT ( 20 ) NOT NULL, 
            `txt_extract_date` TEXT ( 6 ) NOT NULL, 
            `txt_extract_time` TEXT ( 4 ) NOT NULL, 
            `txt_current_file_ref` TEXT ( 7 ) NOT NULL, 
            `txt_last_file_ref` TEXT ( 7 ), 
            `txt_update_indicator` TEXT ( 1 ) NOT NULL, 
            `txt_version` TEXT ( 1 ) NOT NULL, 
            `txt_start_date` TEXT ( 6 ) NOT NULL, 
            `txt_end_date` TEXT ( 4 ) NOT NULL, 
            `time_process_timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP, 
            `int_complete` INTEGER DEFAULT 0);
        DROP TABLE IF EXISTS `tbl_header_stats`;
        CREATE TABLE IF NOT EXISTS `tbl_header_stats`
            (`int_record_id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            `int_header_record_id` INTEGER NOT NULL,
            `int_TI` INTEGER NOT NULL,
            `int_TI_proc` INTEGER NOT NULL,
            `int_TA` INTEGER NOT NULL,
            `int_TA_proc` INTEGER NOT NULL,
            `int_TD` INTEGER NOT NULL,
            `int_TD_proc` INTEGER NOT NULL,
            `int_AA_tot` INTEGER NOT NULL,
            `int_AA_new` INTEGER NOT NULL,
            `int_AA_del` INTEGER NOT NULL,
            `int_AA_rev` INTEGER NOT NULL,
            `int_AA_proc` INTEGER NOT NULL,
            `int_BS` INTEGER NOT NULL,
            `int_BS_proc` INTEGER NOT NULL);
        """
        c.executescript(format_sql(sql_string))
        con.commit()
