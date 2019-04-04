#!/usr/bin/python3
import svgwrite
import json
import re
from datetime import datetime, timedelta
from datetimerange import DateTimeRange


class SVGObject:

    # JSON = '{"platforms": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14], "start_time": "00:00", "end_time": "23:00"}'
    JSON = '{"location" : "Crewe Station",' \
           '"platforms": [' \
           '[1, "Platform 1"],' \
           '[2, "Platform 2"],' \
           '[3, "Platform 3"],' \
           '[4, "Platform 4"],' \
           '[5, "Platform 5"],' \
           '["UFL", "Up Fast Line"],' \
           '["DFL", "Down Fast Line"],' \
           '[6, "Platform 6"],' \
           '[7, "Platform 7"],' \
           '[8, "Platform 8"],' \
           '[9, "Platform 9"],' \
           '[10, "Platform 10"],' \
           '[11, "Platform 11"],' \
           '["UDL", "Up & Dn Loop"],' \
           '[12, "Platform 12"]], ' \
           '"start_time": "00:00", ' \
           '"end_time": "23:00"}'

    # JSON = '{"platforms": [1, 2, 3, 4, 5]}'

    def parse_csv(self):
        with open('CREWE.csv') as fl:
            for line in fl:
                details = line.split(',')
                activity = details[0].strip()
                headcode = details[2].strip()
                platform = details[7].strip()
                arrival_time = details[9].strip()
                departure_time = details[10].strip()

                self.trains.append({'activity': activity, 'id': headcode, 'plt': platform, 'a': arrival_time, 'd': departure_time})

    def return_x_coordinate(self, time):

        offset = 10
        now = datetime.now()
        if not isinstance(time, datetime):
            tm = datetime.strptime(time, '%H:%M:%S')
        else:
            tm = time
        tm = tm.replace(year=now.year, month=now.month, day=now.day)
        graph_start_time = self.start_time.replace(year=now.year, month=now.month, day=now.day)
        tm_difference = tm - graph_start_time
        range_time = int(tm_difference.total_seconds() / 60)
        return (self.ticks * range_time) + offset

    def return_y_coordinate(self, platform):

        for key, val in self.platform_bottom_line.items():

            if str(key) == platform.strip():
                return val

        return None  
        
    def draw_passing_train(self, x, y, width, identity):

        y_offset = 2
        height_offset = 4
        text_x_offset = 5
        text_y_offset = 15
        
        self.main_dwg.add(self.main_dwg.rect((x, y + y_offset), (width, self.row_height - height_offset), stroke='white', fill='#1f8417', stroke_width='2', rx=8, ry=8).dasharray([2, 2]))
        self.main_dwg.add(self.main_dwg.text(identity, insert=(x + text_x_offset, y + text_y_offset), fill='white', font_size='12px', font_weight='bold', font_family='Arial'))

    def draw_calling_train(self, arr_x, dep_x, y, width, identity):

        y_offset = 2
        height_offset = 4
        text_x_offset = 5
        text_y_offset = 15

        self.main_dwg.add(self.main_dwg.rect((arr_x, y + y_offset), (width, self.row_height - height_offset), stroke='white', fill='#f45042', stroke_width='2', rx=8, ry=8))
        self.main_dwg.add(self.main_dwg.text(identity, insert=(arr_x + text_x_offset, y + text_y_offset), fill='white', font_size='12px', font_weight='bold', font_family='Arial'))

    def draw_terminating_train(self, x, y, width, identity):

        y_offset = 2
        height_offset = 4
        text_x_offset = 5
        text_y_offset = 15

        self.main_dwg.add(self.main_dwg.rect((x, y + y_offset), (width, self.row_height - height_offset), stroke='white', fill='yellow', stroke_width='2', rx=8, ry=8))
        self.main_dwg.add(self.main_dwg.text(identity, insert=(x + text_x_offset, y + text_y_offset), fill='black', font_size='12px', font_weight='bold', font_family='Arial'))

    def draw_starting_train(self, x, y, width, identity):

        y_offset = 2
        height_offset = 4
        text_x_offset = 5
        text_y_offset = 32

        self.main_dwg.add(self.main_dwg.rect((x, y + y_offset), (width, self.row_height - height_offset), stroke='white', fill='grey', stroke_width='2', rx=8, ry=8))
        self.main_dwg.add(self.main_dwg.text(identity, insert=(x + text_x_offset, y + text_y_offset), fill='pink', font_size='12px', font_weight='bold', font_family='Arial'))

    def render_trains(self):

        for entry in self.trains:

            width = 40

            if re.search('TF|TB', entry['activity']) is not None:
                # Train either starts or terminates at the TIPLOC.
                if entry['activity'].strip() == 'TF':
                    # Train terminates at the location
                    x = self.return_x_coordinate(entry['a'])
                    y = self.return_y_coordinate(entry['plt'])
                    id = entry['id'] 
                    self.draw_terminating_train(x, y, width, id)
                if entry['activity'].strip() == 'TB':
                    # Train starts at the location
                    x = self.return_x_coordinate(entry['d'])
                    y = self.return_y_coordinate(entry['plt'])
                    id = entry['id'] 
                    self.draw_starting_train(x, y, width, id)

            else:
                if entry['a'].strip():
                    arr_x = self.return_x_coordinate(entry['a'])
                    dep_x = self.return_x_coordinate(entry['d'])
                    y = self.return_y_coordinate(entry['plt'])
                    id = entry['id']
                    width = dep_x - arr_x
                    self.draw_calling_train(arr_x, dep_x, y, width, id)

                else:
                    # Train passes through
                    x = self.return_x_coordinate(entry['d'])
                    y = self.return_y_coordinate(entry['plt'])
                    id = entry['id']
                    self.draw_passing_train(x, y, width, id)

            # if entry['a']:  # An Arrival Time (Means the service calls at the TIPLOC)
            #     a = datetime.strptime(entry['a'], '%H:%M:%S')
            #     if entry['d']:
            #         d = datetime.strptime(entry['d'], '%H:%M:%S')
            #     if a and d:
            #         h = entry['id']
            #         p = entry['plt']

            #         arr_x = self.return_x_coordinate(a)
            #         dep_x = self.return_x_coordinate(d)
            #         width = dep_x - arr_x

            #         for key, val in self.platform_bottom_line.items():

            #             if str(key) == p.strip():

            #                 self.main_dwg.add(self.main_dwg.rect((arr_x, val + 2), (width, self.row_height - 4), stroke='black', fill='#f45042', stroke_width='1.5', rx=8, ry=8))
            #                 text_x_pos = arr_x + 5
            #                 self.main_dwg.add(self.main_dwg.text(h, insert=(text_x_pos, val + 15), fill='white', font_size='12px', font_weight='bold', font_family='Arial'))
            
            # else:
            #     if entry['activity']:
            #         if entry['activity'].strip() == 'TF':
            #             # Service starts at the location:
            #             pass
            #         else:
            #             # Service

            #     h = entry['id']
            #     p = entry['plt']
            #     print(h)
            #     d = datetime.strptime(entry['d'], '%H:%M:%S')
            #     now = datetime.now()
            #     departure_time = d.replace(year=now.year, month=now.month, day=now.day)
            #     graph_start_time = self.start_time.replace(year=now.year, month=now.month, day=now.day)
            #     tm_between_departure = departure_time - graph_start_time
            #     range_dep = int(tm_between_departure.total_seconds() / 60)
            #     dep_x = (self.ticks * range_dep) + 10   # Time the minutes * ticks.
            #     width = 42
            #     for key, val in self.platform_bottom_line.items():

            #             if str(key) == p.strip():
            #                 self.main_dwg.add(self.main_dwg.rect((dep_x, val + 2), (width, self.row_height - 4), stroke='white', fill='#1f8417', stroke_width='2', rx=8, ry=8).dasharray([2, 2]))
            #                 text_x_pos = dep_x + 5
            #                 self.main_dwg.add(self.main_dwg.text(h, insert=(text_x_pos, val + 15), fill='white', font_size='12px', font_weight='bold', font_family='Arial'))


    @staticmethod
    def parse_platforms(json_string):

        x = json.loads(json_string)
        return x['platforms']

    @staticmethod
    def parse_times(json_string):

        x = json.loads(json_string)
        return [datetime.strptime(x['start_time'], '%H:%M'), datetime.strptime(x['end_time'], '%H:%M')]

    def draw_docker_background(self):

        # Calculate row height
        total_sep = len(self.platforms) - 1  # Calculate how many seperators are needed.
        total_sep_pixels = self.seperator_line_height * total_sep  # Calculate how many pixels taken up by seperators.
        row_height = ((self.svg_height - self.bottom_border) - total_sep_pixels) / len(self.platforms)  # Calculate the row height

        y = 0
        alt = True

        # Create definitions
        blue_row = self.main_dwg.rect(id='blue_row', size=(self.main_svg_width, row_height), fill='blue', opacity='0.179')
        green_row = self.main_dwg.rect(id='green_row', size=(self.main_svg_width, row_height), fill='green', opacity='0.179')
        self.main_dwg.defs.add(blue_row)
        self.main_dwg.defs.add(green_row)
        
        for platform in self.platforms:
            
            # Add background rectangle
            if alt:
                self.main_dwg.add(self.main_dwg.use(blue_row, insert=(0, y)))
                alt = False
            else:
                self.main_dwg.add(self.main_dwg.use(green_row, insert=(0, y)))
                alt = True

            y += self.seperator_line_height + row_height

    def draw_time_line(self, start_time, end_time):
        
        # Draw time line
        time_line_y_offset = 30  # Distance from last row
        y = (self.svg_height - self.bottom_border) + time_line_y_offset  # Set y coordinate
        
        # Draw main time line (horizontal line)
        self.main_dwg.add(self.main_dwg.line(start=(self.tl_padding,  y), end=(self.main_svg_width - self.tl_padding, y), stroke_width=2, stroke='black'))
        
        # Add timeline background border
        self.main_dwg.add(self.main_dwg.rect((0, y - 20), (self.main_svg_width, 50), rx='5', ry='5', fill='yellow', opacity='0.179', stroke='black'))
        
        # Create time range between start and end times
        time_range = DateTimeRange(start_time, end_time)
        tm_between = end_time - start_time    
        range_time = int(tm_between.total_seconds() / 60)  # Minutes between start and end time...
        length = self.main_svg_width - (self.tl_padding * 2)

        # Set ticks (ratio of pixels to minutes)
        self.ticks = length / range_time

        x = self.tl_padding

        # Iterate through the time range and draw time elements along timeline
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

        now = datetime.now()  # Get the current time
        # now = datetime.strptime('00:00', "%H:%M")  # For testing TODO: Remove in production
        start_time = self.start_time.replace(year=now.year, month=now.month, day=now.day)  # Get the Docker start time.
        tm_between = now - start_time  # Calculate how long has passed from start time
        range_time = int(tm_between.total_seconds() / 60)  # Work out the total minutes.
        x = (self.ticks * range_time) + 10   # Time the minutes * ticks.
        self.scroll_left = x

        # Plot time-line
        self.main_dwg.add(self.main_dwg.line(start=(x, 0), end=(x, self.svg_height - self.bottom_border), stroke_width=2, stroke='blue', id='time_now'))
        time_string = now.strftime("%H:%M")
        self.main_dwg.add(self.main_dwg.text(time_string, id="time_clock", insert=(x - 14, self.svg_height - self.bottom_border + 8.5), font_size='10px', font_weight='bold', font_family='Arial'))

    def return_index_svg(self):
        return self.index_dwg.tostring()

    def return_main_svg(self):
        return self.main_dwg.tostring()

    def create_platform_index(self):

        """This function creates the platform index SVG"""

        total_sep = len(self.platforms) - 1  # The number of separators we need.
        total_sep_pixels = self.seperator_line_height * total_sep  # How many pixels are taken up by the separator
        # The height of each row.
        self.row_height = ((self.svg_height - self.bottom_border) - total_sep_pixels) / len(self.platforms)
        text_y_offset = 20  # The amount of y offset for platform text.

        y = 0
        alt = True  # Boolean used to alternate row colors.

        # Create definitions
        index_blue_row = self.index_dwg.rect(id='index_blue_row', size=(self.index_svg_width, self.row_height), fill='#405577')
        index_green_row = self.index_dwg.rect(id='index_green_row', size=(self.index_svg_width, self.row_height), fill='#184c17')
        self.index_dwg.defs.add(index_blue_row)
        self.index_dwg.defs.add(index_green_row)

        for platform in self.platforms:
            
            # Add background rectangle
            if alt:
                self.index_dwg.add(self.index_dwg.use(index_blue_row, insert=(0, y)))
                alt = False
            else:
                self.index_dwg.add(self.index_dwg.use(index_green_row, insert=(0, y)))
                alt = True

            self.platform_bottom_line.update({platform[0]: y})

            y += self.seperator_line_height + self.row_height

            # Add text label
            self.index_dwg.add(self.index_dwg.text(platform[1], insert=(10, (y - text_y_offset)), class_='platform_text'))
    
    def return_scroll_left(self):

        # This function provides JS to scroll the current time line into view.

        js_string = """
        <script>
            function scroll_left() {{
                // Scroll the current time line into view
                var element = document.getElementById("main_div");
                var maxScrollLeft = element.scrollWidth - element.clientWidth; // Get the maximum scrollable value
                var x = (maxScrollLeft / 100) * {};  // Calculate the ratio value from the SVG
                console.log(x);
                element.scrollLeft = x + (element.clientWidth / 3.5); // Scroll left
            }};
        </script>""".format((100 / self.main_svg_width) * self.scroll_left)
        return re.sub(r" {2,}|\t", "", js_string.strip())    

    def return_auto_scroll_js(self):
        
        # This javascript function is called every 30 seconds and animates the timeline.

        js_string = """
        <script>
            function scroll_tl(){{
                var time_line = document.getElementById("time_now");
                var start_time = new Date('{}');
                start_time.setYear(2019);
                start_time.setMonth(3, 4);
                var current_time = new Date();
                var diff = current_time - start_time;
                var minutes = Math.floor(diff / 1000 / 60);
                time_line.setAttribute('x1', minutes * {} + 10);
                time_line.setAttribute('x2', minutes * {} + 10);
                var time_clock = document.getElementById("time_clock");
                time_clock.setAttribute('x', minutes * {} - 2);
                hh = current_time.getHours();
                mm = current_time.getMinutes();
                time_clock.textContent = hh + ":" + mm;
                centre_timeline();
            }};

            function centre_timeline() {{
                var clock_switch = document.getElementById("track_time");
                if (clock_switch.checked) {{
                      scroll_left();
                }}    
            }};
        </script>""".format(self.start_time, self.ticks, self.ticks, self.ticks)

        return re.sub(r" {2,}|\t", "", js_string.strip())

    def __init__(self):

        self.start_time, self.end_time = self.parse_times(SVGObject.JSON)  # Get Start and End Times
        self.main_svg_width = 30000 # Width of the svg layout
        self.svg_height = 700 # Height of the svg layout.
        self.index_svg_width = 130 # Width of the platform index column
        self.ticks = 0  # Calcuation of ratio pixels to minutes.
        self.bottom_border = 75  # pixels at the bottom of the docker we keep for timeline.
        self.platforms = self.parse_platforms(SVGObject.JSON)  # Get the platform details from the configuration
        self.seperator_line_height = 2  # The height of the seperators lines.
        self.tl_padding = 10  # The padding either side of the timeline
        self.scroll_left = 0
        self.trains = []
        self.platform_bottom_line = {}
        self.row_height = 0
        
        # Instantiate the 2 svg drawings - one for the index, the other for the docker background (main)
        self.index_dwg = svgwrite.Drawing(id='index_dwg',size=(self.index_svg_width, self.svg_height), profile='tiny')
        self.main_dwg = svgwrite.Drawing(id='main_dwg', size=(self.main_svg_width + 30, self.svg_height), profile='tiny')

        self.create_platform_index()  # Create the platform index (platform numbers)
        self.draw_docker_background()  # Create the actual Docker graph/background
        
        self.draw_time_line(self.start_time, self.end_time)  # Create the time line
        self.draw_time_now()  # Draw the time_now line
        self.parse_csv()
        self.render_trains()


if __name__ == '__main__':
    svg=SVGObject()
