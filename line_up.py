import subprocess
def return_sql(tiploc): 

    p = subprocess.Popen(['xclip','-selection','clipboard'], stdin=subprocess.PIPE)

    sql_string = """
    SELECT
    "TB" AS Activity,
    CASE
        WHEN tbl_current_schedule.txt_stp_indicator = 'P' THEN 'LTP'
        WHEN tbl_current_schedule.txt_stp_indicator = 'N' THEN 'STP'
        WHEN tbl_current_schedule.txt_stp_indicator = 'O' THEN 'VAR'
    END as STP,
    CASE
        WHEN length(trim(tbl_current_schedule.txt_identity)) = 0 THEN '****'
    ELSE
        tbl_current_schedule.txt_identity
    END AS 'Head Code',
    tbl_current_schedule.txt_uid as UID, 
    CASE
        WHEN length(tbl_origin.txt_location_and_suffix) > 0 THEN
        ifnull((SELECT tbl_location.txt_tps_description FROM tbl_location WHERE trim(tbl_location.txt_tiploc) = trim(substr(tbl_origin.txt_location_and_suffix, 1, 7)) LIMIT 1), tbl_origin.txt_location_and_suffix)
    END AS Origin, 
    CASE
        WHEN length(tbl_terminating.txt_location_and_suffix) > 0 THEN
        ifnull((SELECT tbl_location.txt_tps_description FROM tbl_location WHERE substr(trim(tbl_location.txt_tiploc),1,7) = trim(substr(tbl_terminating.txt_location_and_suffix, 1, 7)) LIMIT 1), tbl_terminating.txt_location_and_suffix)
    END as Destination,
    '' as 'Path In',
    tbl_origin.txt_platform AS Platform,
    tbl_origin.txt_line_out AS 'Line Out', 
    '' as ArrivalTime, 
    tbl_origin.txt_wtt_dep_time AS DepartureTime,
    tbl_origin.txt_wtt_dep_time as SortTime,
    tbl_extra_schedule.txt_atoc_code as TOC,
    tbl_origin.txt_wtt_dep_time as OriginDepartureTime,
    tbl_terminating.txt_wtt_arr_time as DestArrivalTime
    FROM tbl_origin, tbl_current_schedule, tbl_terminating, tbl_extra_schedule

    WHERE tbl_origin.txt_location_and_suffix LIKE '%{} %'
    AND tbl_origin.int_basic_schedule_id = tbl_current_schedule.int_record_id
    AND tbl_origin.int_basic_schedule_id = tbl_terminating.int_basic_schedule_id
    /*AND tbl_origin.txt_wtt_dep_time >= time('now')*/
    AND tbl_origin.int_basic_schedule_id IN
    (SELECT tbl_current_schedule.int_record_id FROM tbl_current_schedule)
    AND tbl_extra_schedule.int_basic_id = tbl_current_schedule.int_record_id
    UNION

    SELECT 
    "TF" AS Activity,
    CASE
        WHEN tbl_current_schedule.txt_stp_indicator = 'P' THEN 'LTP'
        WHEN tbl_current_schedule.txt_stp_indicator = 'N' THEN 'STP'
        WHEN tbl_current_schedule.txt_stp_indicator = 'O' THEN 'VAR'
    END as STP, 
    CASE
        WHEN length(trim(tbl_current_schedule.txt_identity)) = 0 THEN '****'
    ELSE
        tbl_current_schedule.txt_identity
    END AS 'Head Code',
    tbl_current_schedule.txt_uid as UID, 
    CASE
        WHEN length(tbl_origin.txt_location_and_suffix) > 0 THEN
        ifnull((SELECT tbl_location.txt_tps_description FROM tbl_location WHERE trim(tbl_location.txt_tiploc) = trim(substr(tbl_origin.txt_location_and_suffix, 1, 7)) LIMIT 1), tbl_origin.txt_location_and_suffix)
    END AS Origin, 
    CASE
        WHEN length(tbl_terminating.txt_location_and_suffix) > 0 THEN
        ifnull((SELECT tbl_location.txt_tps_description FROM tbl_location WHERE substr(trim(tbl_location.txt_tiploc),1,7) = trim(substr(tbl_terminating.txt_location_and_suffix, 1, 7)) LIMIT 1), tbl_terminating.txt_location_and_suffix)
    END as Destination,
    tbl_terminating.txt_path_in as 'Path In',
    tbl_terminating.txt_platform AS Platform,
    '' as 'Line Out', 
    tbl_terminating.txt_wtt_arr_time as ArrivalTime, 
    '' as DepartureTime,
    tbl_terminating.txt_wtt_arr_time as SortTime,
    tbl_extra_schedule.txt_atoc_code as TOC,
    tbl_origin.txt_wtt_dep_time as OriginDepartureTime,
    tbl_terminating.txt_wtt_arr_time as DestArrivalTime
    FROM tbl_origin, tbl_current_schedule, tbl_terminating, tbl_extra_schedule
    WHERE tbl_terminating.txt_location_and_suffix LIKE '%{} %'
    AND tbl_terminating.int_basic_schedule_id = tbl_current_schedule.int_record_id
    AND tbl_terminating.int_basic_schedule_id = tbl_origin.int_basic_schedule_id
    /*AND tbl_terminating.txt_wtt_arr_time >= time('now')*/
    AND tbl_terminating.int_basic_schedule_id IN
    (SELECT tbl_current_schedule.int_record_id FROM tbl_current_schedule)
    AND tbl_extra_schedule.int_basic_id = tbl_current_schedule.int_record_id

    UNION

    SELECT
    "" as Activity,
    CASE
        WHEN tbl_current_schedule.txt_stp_indicator = 'P' THEN 'LTP'
        WHEN tbl_current_schedule.txt_stp_indicator = 'N' THEN 'STP'
        WHEN tbl_current_schedule.txt_stp_indicator = 'O' THEN 'VAR'
    END as STP, 
    CASE
        WHEN length(trim(tbl_current_schedule.txt_identity)) = 0 THEN '****'
    ELSE
        tbl_current_schedule.txt_identity
    END AS 'Head Code',
    tbl_current_schedule.txt_uid as UID, 
    CASE
        WHEN length(tbl_origin.txt_location_and_suffix) > 0 THEN
            ifnull((SELECT tbl_location.txt_tps_description FROM tbl_location WHERE trim(tbl_location.txt_tiploc) = trim(substr(tbl_origin.txt_location_and_suffix, 1, 7)) LIMIT 1), tbl_origin.txt_location_and_suffix)
    END as Origin,
    CASE
        WHEN length(tbl_terminating.txt_location_and_suffix) > 0 then
            ifnull((SELECT tbl_location.txt_tps_description FROM tbl_location WHERE substr(trim(tbl_location.txt_tiploc),1,7) = trim(substr(tbl_terminating.txt_location_and_suffix, 1, 7)) LIMIT 1), tbl_terminating.txt_location_and_suffix)
    END as Destination,
    tbl_intermediate.txt_path_in as 'Path In',
    tbl_intermediate.txt_platform AS Platform,
    tbl_intermediate.txt_line_out as 'Line Out',
    CASE 
        WHEN length(tbl_intermediate.txt_wtt_arr_time) = 0 THEN ''
        ELSE tbl_intermediate.txt_wtt_arr_time
    END As ArrivalTime,
    CASE 
        WHEN length(tbl_intermediate.txt_wtt_dep_time) = 0 then tbl_intermediate.txt_wtt_pass_time
        ELSE tbl_intermediate.txt_wtt_dep_time
    END AS DepartureTime,
    CASE 
        WHEN length(tbl_intermediate.txt_wtt_dep_time) = 0 then tbl_intermediate.txt_wtt_pass_time
        ELSE tbl_intermediate.txt_wtt_dep_time
    END AS SortTime,
    tbl_extra_schedule.txt_atoc_code as TOC,
    tbl_origin.txt_wtt_dep_time as OriginDepartureTime,
    tbl_terminating.txt_wtt_arr_time as DestArrivalTime
    FROM tbl_intermediate, tbl_origin, tbl_current_schedule, tbl_terminating, tbl_extra_schedule
    WHERE tbl_intermediate.txt_location_and_suffix LIKE '%{} %'
    AND tbl_intermediate.int_basic_schedule_id = tbl_current_schedule.int_record_id
    AND tbl_intermediate.int_basic_schedule_id = tbl_origin.int_basic_schedule_id
    AND tbl_intermediate.int_basic_schedule_id = tbl_terminating.int_basic_schedule_id
    /*AND SortTime >= time('now')*/
    AND tbl_intermediate.int_basic_schedule_id IN
    (SELECT tbl_current_schedule.int_record_id FROM tbl_current_schedule)
    AND tbl_extra_schedule.int_basic_id = tbl_current_schedule.int_record_id
    ORDER BY SortTime""".format(tiploc, tiploc, tiploc)

    p.stdin.write(sql_string.encode('utf-8'))
    p.stdin.close()
    retcode = p.wait()
    print('Copied to Clipboard...')

if __name__ == '__main__':
    print(return_sql('CREWE'))