#!/usr/bin/python3
import svgwrite
import json
from datetime import datetime, timedelta
from datetimerange import DateTimeRange

class SVGObject:

	JSON = '{"platforms": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14], "start_time": "09:20", "end_time": "14:00"}'
	#JSON = '{"platforms": [1, 2, 3, 4, 5]}'
	
	def parse_platforms(self, json_string):

		x = json.loads(json_string)
		return x['platforms']

	def parse_times(self, json_string):

		x = json.loads(json_string)
		return [datetime.strptime(x['start_time'], '%H:%M'), datetime.strptime(x['end_time'], '%H:%M')]

	def draw_platform_index(self):

		# Calculate row height
		sep_line_height = 2  # Pixels height
		bottom_border = 200
		total_sep = len(self.platforms) - 1
		total_sep_pixels = sep_line_height * total_sep
		row_height = ((self.svg_height - bottom_border) - total_sep_pixels) / len(self.platforms)
		y = 0
		alt=True
		
		for platform in self.platforms:
			
			#Add background rectangle
			if alt:
				self.dwg.add(self.dwg.rect((0, y), (self.svg_width, row_height), fill='blue', opacity='0.179'))
				alt = False
			else:
				self.dwg.add(self.dwg.rect((0, y), (self.svg_width, row_height), fill='green', opacity='0.179'))
				alt = True

			y += sep_line_height + row_height

			# Add text label
			self.dwg.add(self.dwg.text(platform, insert=(10, y - 12), stroke='none', fill='black', font_size='20px', font_weight="bold", font_family="Arial"))
			
			# Add seperator line	
			self.dwg.add(self.dwg.line(start=(0,y-1), end=(self.svg_width, y-1), stroke_width=2, stroke='black'))

		# Add number column
		self.dwg.add(self.dwg.rect((0,0), (42, 500), fill='blue', opacity='0.179'))

	def draw_time_line(self, start_time, end_time):
		
		# Draw time line
		self.dwg.add(self.dwg.line(start=(43, 520), end=(self.svg_width, 520), stroke_width=2, stroke='black'))
		self.dwg.add(self.dwg.rect((36, 508), (self.svg_width - 26, 32), rx='5', ry='5', fill='yellow', opacity='0.179', stroke='black'))
		time_range = DateTimeRange(start_time, end_time)
		tm_between = end_time - start_time	
		range_time = int(tm_between.total_seconds() / 60)  # Minutes between start and end time...
		length = self.svg_width - 43

		self.ticks = length / range_time

		
		# hour = int(start_time.hour)
		# for y in range(43, int(self.svg_width), int(hour_ticks)):
		# 	#self.dwg.add(self.dwg.line(start=(y, 510), end=(y, 530), stroke_width=2, stroke='red'))
		# 	self.dwg.add(self.dwg.text(hour, insert=(y-5, 538), fill='black', font_size='10px', font_weight='bold', font_family='Arial'))
		# 	hour += 1

		y = 43

		for val in time_range.range(timedelta(minutes=1)):
			
			if str(val.minute) == '0':
				if len(str(val.hour)) == 1:
					hr = '0{}'.format(str(val.hour))
				else:
					hr = str(val.hour)
				self.dwg.add(self.dwg.line(start=(y, 510), end=(y, 530), stroke_width=2, stroke='red'))
				self.dwg.add(self.dwg.text(hr, insert=(y-5, 538), fill='black', font_size='10px', font_weight='bold', font_family='Arial'))
			if str(val.minute) == '15' or str(val.minute) == '45':
				self.dwg.add(self.dwg.line(start=(y, 515), end=(y, 525), stroke_width=2, stroke='black'))
			if str(val.minute) == '30':
				self.dwg.add(self.dwg.circle(center=(y, 520), r=4, fill='red'))
			if str(val.minute) in ('5', '10', '20', '25', '35', '40', '50', '55'):
				self.dwg.add(self.dwg.line(start=(y, 518), end=(y, 522), stroke_width=1, stroke='black'))
			y += self.ticks
			
	def draw_time_now(self):

		now = datetime.now()
		start_time = self.start_time.replace(year=now.year, month=now.month, day=now.day)
		tm_between = now - start_time
		range_time = int(tm_between.total_seconds() / 60)	
		y = self.ticks * range_time + 43
		print(y)
		self.dwg.add(self.dwg.line(start=(y,0), end=(y, 505), stroke_width=2, stroke='blue', id='time_now'))


	def __init__(self, filename='test.svg'):

		self.svg_width = 1000
		self.svg_height = 700
		self.filename = filename
		self.ticks = 0
		self.platforms = self.parse_platforms(SVGObject.JSON)

		self.dwg = svgwrite.Drawing(self.filename, (self.svg_width + 30, self.svg_height), profile='tiny')
		self.draw_platform_index()
		self.start_time, self.end_time = self.parse_times(SVGObject.JSON)
		self.draw_time_line(self.start_time, self.end_time)
		# self.dwg.add(self.dwg.rect((0,0), (100,100), stroke=svgwrite.rgb(10,10,16,'%'), fill='red'))
		self.draw_time_now()
		self.dwg.save(pretty=True)	




if __name__ == '__main__':
	svg=SVGObject()
