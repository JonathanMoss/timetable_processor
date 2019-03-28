SELECT     
    CASE
        WHEN tbl_basic_schedule.txt_stp_indicator = 'P' THEN 'WTT'
        WHEN tbl_basic_schedule.txt_stp_indicator = 'N' THEN 'STP'
        WHEN tbl_basic_schedule.txt_stp_indicator = 'O' THEN 'VAR'
    END as STP,
    case
        when length(trim(tbl_basic_schedule.txt_identity))), THEN '****'
        else
            tbl_basic_schedule.txt_identity
        end as 'Head Code',
    tbl_basic_schedule.txt_uid as UID, 
    case
        when length(tbl_origin.txt_location_and_suffix) > 0 then
            ifnull((SELECT tbl_location.txt_tps_description FROM tbl_location WHERE trim(tbl_location.txt_tiploc) = trim(substr(tbl_origin.txt_location_and_suffix, 1, 7)) LIMIT 1), tbl_origin.txt_location_and_suffix)
    END as Origin,    
    case
        when length(tbl_terminating.txt_location_and_suffix) > 0 then
            ifnull((SELECT tbl_location.txt_tps_description FROM tbl_location WHERE substr(trim(tbl_location.txt_tiploc),1,7) = trim(substr(tbl_terminating.txt_location_and_suffix, 1, 7)) LIMIT 1), tbl_terminating.txt_location_and_suffix)
    END as Destination,    
    '' as 'Path In', 
    tbl_origin.txt_platform AS Platform, 
    tbl_origin.txt_line_out as 'Line Out',
    '' as ArrivalTime, 
    tbl_origin.txt_wtt_dep_time as DepartureTime,
    tbl_origin.txt_wtt_dep_time as SortTime
FROM tbl_origin, tbl_basic_schedule, tbl_terminating
    WHERE tbl_origin.txt_location_and_suffix LIKE '%STAFFRD %'
        AND tbl_origin.int_basic_schedule_id = tbl_basic_schedule.int_record_id
        AND tbl_origin.int_basic_schedule_id = tbl_terminating.int_basic_schedule_id
        AND tbl_origin.txt_wtt_dep_time >= time('now')
        AND tbl_origin.int_basic_schedule_id IN
            (SELECT tbl_current_schedule.int_record_id FROM tbl_current_schedule)
UNION
SELECT 
    CASE
        WHEN tbl_basic_schedule.txt_stp_indicator = 'P' THEN 'WTT'
        WHEN tbl_basic_schedule.txt_stp_indicator = 'N' THEN 'STP'
        WHEN tbl_basic_schedule.txt_stp_indicator = 'O' THEN 'VAR'
    END as STP, 
    case
        when length(trim(tbl_basic_schedule.txt_identity))), THEN '****'
        else
            tbl_basic_schedule.txt_identity
        end as 'Head Code',
    tbl_basic_schedule.txt_uid as UID, 
    case
        when length(tbl_origin.txt_location_and_suffix) > 0 then
            ifnull((SELECT tbl_location.txt_tps_description FROM tbl_location WHERE trim(tbl_location.txt_tiploc) = trim(substr(tbl_origin.txt_location_and_suffix, 1, 7)) LIMIT 1), tbl_origin.txt_location_and_suffix)
    END as Origin,    
    case
        when length(tbl_terminating.txt_location_and_suffix) > 0 then
            ifnull((SELECT tbl_location.txt_tps_description FROM tbl_location WHERE substr(trim(tbl_location.txt_tiploc),1,7) = trim(substr(tbl_terminating.txt_location_and_suffix, 1, 7)) LIMIT 1), tbl_terminating.txt_location_and_suffix)
    END as Destination,    
    tbl_terminating.txt_path_in as 'Path In',
    tbl_terminating.txt_platform AS Platform,
    '' as 'Line Out',     
    tbl_terminating.txt_wtt_arr_time as ArrivalTime, 
    '' as DepartureTime,
    tbl_terminating.txt_wtt_arr_time as SortTime
FROM tbl_origin, tbl_basic_schedule, tbl_terminating
    WHERE tbl_terminating.txt_location_and_suffix LIKE '%STAFFRD %'
        AND tbl_terminating.int_basic_schedule_id = tbl_basic_schedule.int_record_id
        AND tbl_terminating.int_basic_schedule_id = tbl_origin.int_basic_schedule_id
        AND tbl_terminating.txt_wtt_arr_time >= time('now')
        AND tbl_terminating.int_basic_schedule_id IN
            (SELECT tbl_current_schedule.int_record_id FROM tbl_current_schedule)
UNION
SELECT     
    CASE
        WHEN tbl_basic_schedule.txt_stp_indicator = 'P' THEN 'WTT'
        WHEN tbl_basic_schedule.txt_stp_indicator = 'N' THEN 'STP'
        WHEN tbl_basic_schedule.txt_stp_indicator = 'O' THEN 'VAR'
    END as STP, 
    case
        when length(trim(tbl_basic_schedule.txt_identity))), THEN '****'
        else
            tbl_basic_schedule.txt_identity
        end as 'Head Code',
    tbl_basic_schedule.txt_uid as UID, 
    case
        when length(tbl_origin.txt_location_and_suffix) > 0 then
            ifnull((SELECT tbl_location.txt_tps_description FROM tbl_location WHERE trim(tbl_location.txt_tiploc) = trim(substr(tbl_origin.txt_location_and_suffix, 1, 7)) LIMIT 1), tbl_origin.txt_location_and_suffix)
    END as Origin,    
    case
        when length(tbl_terminating.txt_location_and_suffix) > 0 then
            ifnull((SELECT tbl_location.txt_tps_description FROM tbl_location WHERE substr(trim(tbl_location.txt_tiploc),1,7) = trim(substr(tbl_terminating.txt_location_and_suffix, 1, 7)) LIMIT 1), tbl_terminating.txt_location_and_suffix)
    END as Destination,    
    tbl_intermediate.txt_path_in as 'Path In',
    tbl_intermediate.txt_platform AS Platform,
    tbl_intermediate.txt_line_out as 'Line Out',
    CASE 
        WHEN length(tbl_intermediate.txt_wtt_arr_time)), THEN ''
        ELSE tbl_intermediate.txt_wtt_arr_time
    END As ArrivalTime,
    CASE 
        WHEN length(tbl_intermediate.txt_wtt_dep_time)), then tbl_intermediate.txt_wtt_pass_time
        ELSE tbl_intermediate.txt_wtt_dep_time
    END AS DepartureTime,
    CASE 
        WHEN length(tbl_intermediate.txt_wtt_dep_time)), then tbl_intermediate.txt_wtt_pass_time
        ELSE tbl_intermediate.txt_wtt_dep_time
    END AS SortTime
FROM tbl_intermediate, tbl_origin, tbl_basic_schedule, tbl_terminating
    WHERE tbl_intermediate.txt_location_and_suffix LIKE '%STAFFRD %'
        AND tbl_intermediate.int_basic_schedule_id = tbl_basic_schedule.int_record_id
        AND tbl_intermediate.int_basic_schedule_id = tbl_origin.int_basic_schedule_id
        AND tbl_intermediate.int_basic_schedule_id = tbl_terminating.int_basic_schedule_id
        AND SortTime >= time('now')
        AND tbl_intermediate.int_basic_schedule_id IN
            (SELECT tbl_current_schedule.int_record_id FROM tbl_current_schedule)
ORDER BY SortTime

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
        txt_stp_indicator TEXT)

INSERT INTO 
    tbl_current_schedule
SELECT 
    *
FROM 
    tbl_basic_schedule
WHERE 
    tbl_basic_schedule.int_record_id IN (
        SELECT 
            tbl_basic_schedule.int_record_id
        FROM 
            tbl_basic_schedule
        WHERE 
            `txt_start_date` <= date('now') 
            AND `txt_end_date` >= date('now') 
            AND `txt_days_run` LIKE '_____1_' 
            AND `txt_stp_indicator` NOT LIKE '%C%' 
            AND `txt_status` NOT REGEXP 'B|S|4|5')

SELECT 
    tbl_current_schedule.txt_uid, tbl_current_schedule.txt_identity, COUNT(*)
FROM 
    tbl_current_schedule
WHERE 
    `txt_start_date` <= date('now') 
    AND `txt_end_date` >= date('now') 
    AND `txt_days_run` LIKE '____1__' 
    AND `txt_stp_indicator` NOT LIKE '%C%' 
    AND `txt_status` NOT REGEXP 'B|S|4|5'    
GROUP BY 
    tbl_current_schedule.txt_uid, tbl_current_schedule.txt_identity
HAVING 
    COUNT(*) > 1

DELETE
FROM 
    tbl_current_schedule
WHERE
    tbl_current_schedule.txt_stp_indicator = "P" AND
    tbl_current_schedule.txt_uid IN
        (SELECT 
            tbl_current_schedule.txt_uid
        FROM 
            tbl_current_schedule
        WHERE 
            tbl_current_schedule.txt_stp_indicator = "O")


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
`int_BS_proc` INTEGER NOT NULL)

CREATE TABLE IF NOT EXISTS `tbl_current_cif` 
(`txt_current_cif` TEXT NOT NULL)

CREATE TABLE IF NOT EXISTS `tbl_downloaded_cif` 
(`int_index` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
`txt_filename` TEXT NOT NULL, 
`txt_date_time` TEXT NOT NULL, 
`int_success` INTEGER NOT NULL, 
`txt_arguments` TEXT NOT NULL)

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
`int_complete` INTEGER DEFAULT 0)
