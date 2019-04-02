#!/usr/bin/python3
import svgwrite
import json
from datetime import datetime, timedelta
from datetimerange import DateTimeRange

class SVGObject:

	JSON = '{"platforms": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14], "start_time": "00:00", "end_time": "23:00"}'
	JSON = '{"platforms": [1, 2, 3, 4, 5, 6], "start_time": "00:00", "end_time": "23:00"}'
	#JSON = '{"platforms": [1, 2, 3, 4, 5]}'
	
	def parse_platforms(self, json_string):

		x = json.loads(json_string)
		return x['platforms']

	def parse_times(self, json_string):

		x = json.loads(json_string)
		return [datetime.strptime(x['start_time'], '%H:%M'), datetime.strptime(x['end_time'], '%H:%M')]

	def draw_docker_background(self):

		# Calculate row height
		sep_line_height = 2  # Pixels height
		bottom_border = 200
		total_sep = len(self.platforms) - 1
		total_sep_pixels = sep_line_height * total_sep
		row_height = ((self.svg_height - bottom_border) - total_sep_pixels) / len(self.platforms)
		y = 0
		alt=True

		blue_row = self.main_dwg.rect(id='blue_row', size=(self.main_svg_width, row_height), fill='blue', opacity='0.179')
		green_row = self.main_dwg.rect(id='green_row', size=(self.main_svg_width, row_height), fill='green', opacity='0.179')
		self.main_dwg.defs.add(blue_row)
		self.main_dwg.defs.add(green_row)
		
		for platform in self.platforms:
			
			#Add background rectangle
			if alt:
				#self.main_dwg.add(self.main_dwg.rect((0, y), (self.svg_width, row_height), fill='blue', opacity='0.179'))
				self.main_dwg.add(self.main_dwg.use(blue_row, insert=(0, y)))
				alt = False
			else:
				#self.main_dwg.add(self.main_dwg.rect((0, y), (self.svg_width, row_height), fill='green', opacity='0.179'))
				self.main_dwg.add(self.main_dwg.use(green_row, insert=(0, y)))
				alt = True

			y += sep_line_height + row_height

			# # Add seperator line
			# if total_sep > 0:  # Ensures we only draw the right amount of seperators
			# 	self.main_dwg.add(self.main_dwg.line(start=(0,y-1), end=(self.main_svg_width, y-1), stroke_width=2, stroke='black'))
			# 	total_sep -= 1



	def draw_time_line(self, start_time, end_time):
		
		# Draw time line
		bottom_border = 200
		time_line_offset = 40
		y = (self.svg_height - bottom_border) + time_line_offset
		self.main_dwg.add(self.main_dwg.line(start=(self.index_svg_width + 10,  y), end=(self.main_svg_width - 10, y), stroke_width=2, stroke='black'))
		self.main_dwg.add(self.main_dwg.rect((self.index_svg_width, y - 20), (self.main_svg_width - 120, 50), rx='5', ry='5', fill='yellow', opacity='0.179', stroke='black'))
		time_range = DateTimeRange(start_time, end_time)
		tm_between = end_time - start_time	
		range_time = int(tm_between.total_seconds() / 60)  # Minutes between start and end time...
		length = self.main_svg_width - 150

		self.ticks = length / range_time

		
		# hour = int(start_time.hour)
		# for y in range(43, int(self.svg_width), int(hour_ticks)):
		# 	#self.main_dwg.add(self.main_dwg.line(start=(y, 510), end=(y, 530), stroke_width=2, stroke='red'))
		# 	self.main_dwg.add(self.main_dwg.text(hour, insert=(y-5, 538), fill='black', font_size='10px', font_weight='bold', font_family='Arial'))
		# 	hour += 1

		x = self.index_svg_width + 10

		for val in time_range.range(timedelta(minutes=1)):
			
			if str(val.minute) == '0':
				if len(str(val.hour)) == 1:
					hr = '0{}'.format(str(val.hour))
				else:
					hr = str(val.hour)
				self.main_dwg.add(self.main_dwg.line(start=(x, (y - 15)), end=(x, (y + 15)), stroke_width=2, stroke='red'))
				self.main_dwg.add(self.main_dwg.text(hr, insert=(x - 5, y + 26), fill='black', font_size='10px', font_weight='bold', font_family='Arial'))
			if str(val.minute) == '15' or str(val.minute) == '45':
				self.main_dwg.add(self.main_dwg.line(start=(x, (y - 10)), end=(x, (y + 10)), stroke_width=2, stroke='black'))
			if str(val.minute) == '30':
				self.main_dwg.add(self.main_dwg.circle(center=(x, y), r=4, fill='red'))
			if str(val.minute) in ('5', '10', '20', '25', '35', '40', '50', '55'):
				self.main_dwg.add(self.main_dwg.line(start=(x, y - 3), end=(x, y + 3), stroke_width=1, stroke='black'))
			x += self.ticks
			
	def draw_time_now(self):

		now = datetime.now()
		start_time = self.start_time.replace(year=now.year, month=now.month, day=now.day)
		tm_between = now - start_time
		range_time = int(tm_between.total_seconds() / 60)	
		x = self.ticks * range_time + self.index_svg_width + 10
		
		self.main_dwg.add(self.main_dwg.line(start=(x, 0), end=(x, 620), stroke_width=2, stroke='blue', id='time_now'))
		self.scroll_to_pixels = x

	def return_index_svg(self):

		return self.index_dwg.tostring()

	def return_main_svg(self):
		return self.main_dwg.tostring()

	def return_scroll_script(self):
		script = """
		<script>
		
				window.scrollTo({}, 0);
			
		
		</script>
		""".format(int(self.scroll_to_pixels))
		return script

	def create_platform_index(self):

		sep_line_height = 2  # Height in pixels for the seperator bar.
		bottom_border = 200  # Space we need to maintain at the bottom for things like the time bar etc...
		total_sep = len(self.platforms) - 1  # The number of seperators we need.
		total_sep_pixels = sep_line_height * total_sep  # How many pixels are taken up by the seperator
		row_height = ((self.svg_height - bottom_border) - total_sep_pixels) / len(self.platforms)  # The height of each row.

		y = 0
		alt=True  # Boolean used to alternate row colors.

		# Create definitions
		index_blue_row = self.index_dwg.rect(id='index_blue_row', size=(self.index_svg_width, row_height), fill='blue')
		index_green_row = self.index_dwg.rect(id='index_green_row', size=(self.index_svg_width, row_height), fill='green')
		self.index_dwg.defs.add(index_blue_row)
		self.index_dwg.defs.add(index_green_row)
		
		for platform in self.platforms:
			
			#Add background rectangle
			if alt:
				#self.main_dwg.add(self.main_dwg.rect((0, y), (self.svg_width, row_height), fill='blue', opacity='0.179'))
				self.index_dwg.add(self.index_dwg.use(index_blue_row, insert=(0, y)))
				alt = False
			else:
				#self.main_dwg.add(self.main_dwg.rect((0, y), (self.svg_width, row_height), fill='green', opacity='0.179'))
				self.index_dwg.add(self.index_dwg.use(index_green_row, insert=(0, y)))
				alt = True

			y += sep_line_height + row_height

			# Add text label
			p_text = "Platform {}".format(platform)
			self.index_dwg.add(self.index_dwg.text(p_text, insert=(10, y - 12), stroke='none', fill='white', font_size='20px', font_weight="bold", font_family="Arial"))
			
			# # Add seperator line
			# if total_sep > 0:  # Ensures we only draw the right amount of seperators
			# 	self.index_dwg.add(self.index_dwg.line(start=(0,y-1), end=(self.index_svg_width, y-1), stroke_width=2, stroke='black'))
			# 	total_sep -= 1




	def __init__(self):

		self.main_svg_width = 3000  # Width of the svg layout
		self.svg_height = 600 # Height of the svg layout.
		self.index_svg_width = 130 # Width of the platform index column
		self.scroll_to_pixels = 0
		self.ticks = 0  # Calcuation of ratio pixels to minutes.
		self.platforms = self.parse_platforms(SVGObject.JSON)  # Get the platform details from the configuration
		
		self.index_dwg = svgwrite.Drawing(id='index_dwg',size=(self.index_svg_width, self.svg_height), profile='tiny')
		self.main_dwg = svgwrite.Drawing(id='main_dwg', size=(self.main_svg_width + 30, self.svg_height), profile='tiny')

		self.create_platform_index()
		self.draw_docker_background()
		self.start_time, self.end_time = self.parse_times(SVGObject.JSON)
		self.draw_time_line(self.start_time, self.end_time)
		self.draw_time_now()
		
		# self.platform_row = self.main_dwg.rect(size=(100,100), id='platform_row_bg', fill='blue', opacity='0.179')
		# self.main_dwg.defs.add(self.platform_row)
		# self.draw_platform_index()
		# 
		
		# self.main_dwg.add(self.main_dwg.rect((0,0), (100,100), stroke=svgwrite.rgb(10,10,16,'%'), fill='red'))
		#self.draw_time_now()
		# self.main_dwg.add(self.main_dwg.use(self.platform_row, insert=(30,30)))
		# self.main_dwg.add(self.main_dwg.use(self.platform_row, insert=(50,100), fill='red'))
		#self.main_dwg.save(pretty=True)
		self.svg_string = '{}{}'.format(self.index_dwg.tostring(), self.main_dwg.tostring())


if __name__ == '__main__':
	svg=SVGObject()
