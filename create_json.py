#!/usr/bin/python3
import json
import sqlite3

class CreateJson:

	def __init__(self, file_name='train_service.json', db=None):

		self.file_name = file_name
		self.db_conn = sqlite3.connect(db)
		self.schedule_id = []

		self.get_schedule_id()
		self.process_schedules()


	def get_schedule_id(self):

		sql_string = """

			SELECT
				tbl_current_schedule.int_record_id AS RECORD_ID
			FROM 
				tbl_current_schedule, tbl_origin
			WHERE 
				tbl_origin.txt_location_and_suffix LIKE '%CREWE %'
			AND 
				tbl_origin.int_basic_schedule_id = tbl_current_schedule.int_record_id
			UNION 
			SELECT 
				tbl_current_schedule.int_record_id AS RECORD_ID
			FROM 
				tbl_current_schedule, tbl_intermediate
			WHERE 
				tbl_intermediate.txt_location_and_suffix LIKE '%CREWE %'
			AND 
				tbl_intermediate.int_basic_schedule_id = tbl_current_schedule.int_record_id
			UNION 
			SELECT
				tbl_current_schedule.int_record_id AS RECORD_ID
			FROM 
				tbl_current_schedule, tbl_terminating
			WHERE 
				tbl_terminating.txt_location_and_suffix LIKE '%CREWE %'
			AND 
				tbl_terminating.int_basic_schedule_id = tbl_current_schedule.int_record_id
		"""

		c = self.db_conn.cursor()
		c.execute(sql_string)
		for id in c.fetchall():
			self.schedule_id.append(id[0])



	def process_schedules(self):

		sched = []
		for id in self.schedule_id:
			sql_string = """
				SELECT
					tbl_basic_schedule.txt_uid, tbl_basic_schedule.txt_identity, tbl_basic_schedule.txt_stp_indicator
				FROM
					tbl_basic_schedule
				WHERE
					tbl_basic_schedule.int_record_id = {}
			""".format(id)

			c = self.db_conn.cursor()
			c.execute(sql_string)
			for ret_val in c.fetchall():
				uid = ret_val[0].strip()
				headcode = ret_val[1].strip()
				stp = ret_val[2].strip()

			sql_string = """
				SELECT
					tbl_extra_schedule.txt_atoc_code
				FROM
					tbl_extra_schedule
				WHERE
					tbl_extra_schedule.int_basic_id = {}
			""".format(id)

			c.execute(sql_string)
			for ret_val in c.fetchall():
				toc = ret_val[0].strip()

			sql_string = """
				SELECT
					tbl_origin.txt_location_and_suffix, tbl_origin.txt_wtt_dep_time, tbl_origin.txt_platform, tbl_origin.txt_line_out, tbl_origin.txt_activity
				FROM
					tbl_origin
				WHERE
					tbl_origin.int_basic_schedule_id = {}
			""".format(id)

			c.execute(sql_string)
			for ret_val in c.fetchall():
				origin_tiploc = ret_val[0].strip()
				dep_time = ret_val[1].strip()
				platform = ret_val[2].strip()
				line_out = ret_val[3].strip()
				activity = ret_val[4].strip()

			dep_line = [origin_tiploc, "", dep_time, "", "", platform, line_out, activity]

			sql_string = """
				SELECT
					tbl_intermediate.txt_location_and_suffix, tbl_intermediate.txt_wtt_arr_time, tbl_intermediate.txt_wtt_dep_time, tbl_intermediate.txt_wtt_pass_time, tbl_intermediate.txt_path_in, tbl_intermediate.txt_platform, tbl_intermediate.txt_line_out, tbl_intermediate.txt_activity
				FROM
					tbl_intermediate
				WHERE
					tbl_intermediate.int_basic_schedule_id = {}
			""".format(id)

			c.execute(sql_string)
			int_lines = []
			for ret_val in c.fetchall():
				tiploc = ret_val[0].strip()
				arr_time = ret_val[1].strip()
				dep_time = ret_val[2].strip()
				pass_time = ret_val[3].strip()
				path_in = ret_val[4].strip()
				platform = ret_val[5].strip()
				line_out = ret_val[6].strip()
				activity = ret_val[7].strip()

				int_line = [tiploc, arr_time, dep_time, pass_time, path_in, platform, line_out, activity]
				int_lines.append(int_line)

			sql_string = """
				SELECT
					tbl_terminating.txt_location_and_suffix, tbl_terminating.txt_wtt_arr_time, tbl_terminating.txt_platform, tbl_terminating.txt_path_in, tbl_terminating.txt_activity
				FROM
					tbl_terminating
				WHERE
					tbl_terminating.int_basic_schedule_id = {}
			""".format(id)

			c.execute(sql_string)
			for ret_val in c.fetchall():
				dest_tiploc = ret_val[0].strip()
				arr_time = ret_val[1].strip()
				platform = ret_val[2].strip()
				path_in = ret_val[3].strip()
				activity = ret_val[4].strip()

			arr_line = [dest_tiploc, arr_time, "", "", path_in, platform, "", activity]



			train_description = '{} ({}) {} {} to {} ({})'.format(headcode, toc, dep_line[2], dep_line[0], arr_line[0], arr_line[1])


			sched_dict = {"uid": uid, "type": stp, "train_description": train_description, "schedule_line":[dep_line]}

			for entry in int_lines:
				sched_dict['schedule_line'].append(entry)

			sched_dict['schedule_line'].append(arr_line)

			sched.append(sched_dict)

		with open('train_service.json', 'w') as file:
			json_output = {"location": "Crewe", "version": 1, "date_created": "2019-04-10", "schedules": []}
			json_output['schedules'] = sched
			file.write(json.dumps(json_output))





if __name__ == "__main__":
	db = '/home/jmoss2/PycharmProjects/timetable_processor/DFROC2F(A)/DFROC2F(A).db'
	js = CreateJson(db=db)