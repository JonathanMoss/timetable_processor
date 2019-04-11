	SELECT 
		tbl_origin.txt_location_and_suffix AS TIPLOC,
		"" AS ARR,
		tbl_origin.txt_wtt_dep_time AS DEP,
		"" AS PATH_IN,
		tbl_origin.txt_platform as PLT,
		tbl_origin.txt_line_out as LINE_OUT,
		tbl_origin.txt_wtt_dep_time AS SORT
	FROM 
		tbl_origin
	WHERE 
		tbl_origin.int_basic_schedule_id = "20140"

UNION 

	SELECT
		tbl_intermediate.txt_location_and_suffix AS TIPLOC,
		tbl_intermediate.txt_wtt_arr_time AS ARR,
		CASE 
	        WHEN length(tbl_intermediate.txt_wtt_dep_time) = 0 then tbl_intermediate.txt_wtt_pass_time
	        ELSE tbl_intermediate.txt_wtt_dep_time
	    END AS DEP,
	    tbl_intermediate.txt_path_in AS PATH_IN,
		tbl_intermediate.txt_platform as PLT,
		tbl_intermediate.txt_line_out as LINE_OUT,
	    CASE 
	        WHEN length(tbl_intermediate.txt_wtt_dep_time) = 0 then tbl_intermediate.txt_wtt_pass_time
	        ELSE tbl_intermediate.txt_wtt_dep_time
	    END AS SORT
	FROM 
		tbl_intermediate
	WHERE 
		tbl_intermediate.int_basic_schedule_id = "20140"

UNION

	SELECT 
		tbl_terminating.txt_location_and_suffix AS TIPLOC,
		tbl_terminating.txt_wtt_arr_time AS ARR,
		"" AS DEP,
		tbl_terminating.txt_path_in AS PATH_IN,
		tbl_terminating.txt_platform AS PLT,
		"" AS LINE_OUT,
		tbl_terminating.txt_wtt_arr_time AS SORT
	FROM 
		tbl_terminating
	WHERE 
		tbl_terminating.int_basic_schedule_id = "20140"

ORDER BY SORT