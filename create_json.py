#!/usr/bin/python3
import json
import sqlite3
import argparse
import re
from datetime import datetime

parser = argparse.ArgumentParser(description='Extract the timetable into json format for use by the Platform Docker.')
parser.add_argument('-t', '--tiploc', help='Provide a single TIPLOC to prepare.', required=True)
parser.add_argument('-o', '--output', help='The output filename.', required=True)
parser.add_argument('-d', '--database', help='The database filename.', required=True)

args = parser.parse_args()

class CreateJson:

	def __init__(self, args):

		self.tiploc = args.tiploc
		self.output_file = args.output
		self.db_filename = args.database
		self.db_conn = sqlite3.connect(self.db_filename)
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
				tbl_origin.txt_location_and_suffix LIKE '%{} %'
			AND 
				tbl_origin.int_basic_schedule_id = tbl_current_schedule.int_record_id
			UNION 
			SELECT 
				tbl_current_schedule.int_record_id AS RECORD_ID
			FROM 
				tbl_current_schedule, tbl_intermediate
			WHERE 
				tbl_intermediate.txt_location_and_suffix LIKE '%{} %'
			AND 
				tbl_intermediate.int_basic_schedule_id = tbl_current_schedule.int_record_id
			UNION 
			SELECT
				tbl_current_schedule.int_record_id AS RECORD_ID
			FROM 
				tbl_current_schedule, tbl_terminating
			WHERE 
				tbl_terminating.txt_location_and_suffix LIKE '%{} %'
			AND 
				tbl_terminating.int_basic_schedule_id = tbl_current_schedule.int_record_id
		""".format(self.tiploc, self.tiploc, self.tiploc)

		c = self.db_conn.cursor()
		c.execute(sql_string)
		for id in c.fetchall():
			self.schedule_id.append(id[0])


	def process_schedules(self):

		all_schedules = []

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

			schedule_dict = {"uid": uid, "type": stp, "train_description": train_description, "schedule_line":[dep_line]}
			for entry in int_lines:
				schedule_dict['schedule_line'].append(entry)
			schedule_dict['schedule_line'].append(arr_line)

			all_schedules.append(schedule_dict)

		with open(self.output_file, 'w+') as file:
			date_now = datetime.now()
			json_output = {"location": "Crewe", "version": 1, "date_created": str(date_now), "schedules": []}
			json_output['schedules'] = all_schedules
			file.write(json.dumps(json_output))

if __name__ == "__main__":

	js = CreateJson(args)