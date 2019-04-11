#!/usr/bin/python3
import svgwrite
import json
import re
from datetime import datetime, timedelta
from datetimerange import DateTimeRange
from collections import OrderedDict
import pprint

pp = pprint.PrettyPrinter(indent=4)

class SVGObject:

    # JSON = '{"platforms": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14], "start_time": "00:00", "end_time": "23:00"}'
    PLATFORM_TYPE = {'1': ['Up Facing Bay - Left hand platform', ],
                     '2': ['Up Facing Bay - Right hand platform', ],
                     '3': ['Down Facing Bay - Left hand platform', ],
                     '4': ['Down Facing Bay - Right hand platform', ],
                     '5': ['Bi/Di Platform (Up) - Left hand platform', ],
                     '6': ['Bi/Di Platform (Up) - Right hand platform', ],
                     '7': ['Bi/Di Platform (Down) - Left hand platform', ],
                     '8': ['Bi/Di Platform (Down) - Right hand platform', ],
                     '9': ['Bi/Di Platform - Left hand platform', ],
                     '10': ['Bi/Di Platform - Right hand platform', ],
                     '11': ['Up Platform', ],
                     '12': ['Down Platform', ],
                     '13': ['Bi/Di Running Line (Up)', ],
                     '14': ['Bi/Di Running Line (Down)', ],
                     '15': ['Running Line (Up)', ],
                     '16': ['Running Line (Down)', ],
                     '17': ['Bi/Di Passenger Loop', ],
                     '18': ['Bi/Di Passenger Loop (Up)', ],
                     '19': ['Bi/Di Passenger Loop (Down)', ],
                     '20': ['Bi/Di Goods Loop', ],
                     '21': ['Bi/Di Goods Loop (Up)', ],
                     '22': ['Bi/Di Goods Loop (Down)', ],
                     '23': ['Through Yard/Siding', ],
                     '24': ['Yard/Siding', ]}

    JSON = '{"location" : "Crewe Station",' \
           '"platforms": [' \
           '[1, "Platform 1", 10, 311, 332, "PP"],' \
           '[2, "Platform 2", 1, 159, "PP"],' \
           '[3, "Platform 3", 1, 90, "PP"],' \
           '[4, "Platform 4", 2, 132, "PP"],' \
           '[5, "Platform 5", 9, 273, 256, "PP-A"],' \
           '["UFL", "Up Fast Line", 13],' \
           '["DFL", "Down Fast Line", 14],' \
           '[6, "Platform 6", 10, 387, 446, "PP-A"],' \
           '[7, "Platform 7", 1, 154, "PP"],' \
           '[8, "Platform 8", 2, 116, "PP"],' \
           '[9, "Platform 9", 4, 202, "PP"],' \
           '[10, "Platform 10", 3, 80, "PP"],' \
           '[11, "Platform 11", 9, 299, 308, "PP-A"],' \
           '["UDL", "Up & Dn Loop", 17, 361, "PF"],' \
           '[12, "Platform 12", 10, 424, 432, "PF-A"]], ' \
           '"start_time": "00:00", ' \
           '"end_time": "23:59"}'

    # JSON = '{"platforms": [1, 2, 3, 4, 5]}'

    def parse_csv(self):

        with open('CREWE.csv') as fl:
            for line in fl:
                details = line.split(',')
                activity = details[0].strip()  # CIF Activity.
                headcode = details[2].strip()
                platform = details[7].strip()
                arrival_time = details[9].strip()
                departure_time = details[10].strip()
                origin = details[4].strip()
                dest = details[5].strip()
                schedule = details[1].strip()
                line_out = details[8].strip()
                uid = details[3].strip()
                toc = details[12].strip()
                dt = details[13].strip()
                at = details[14].strip()

                try:
                    pl = self.train_service_dict[platform]
                    pl.update({uid: {'uid': uid, 'toc': toc, 'dt': dt, 'at': at, 'plt': platform, 'lo': line_out, 'schedule': schedule, 'activity': activity, 'id': headcode, 'a': arrival_time, 'd': departure_time, 'o': origin, 'dest': dest}})
                    self.train_service_dict[platform] = pl
                except Exception as e:
                    self.train_service_dict[platform] = OrderedDict({uid: {'uid': uid, 'toc': toc, 'dt': dt, 'at': at, 'plt': platform, 'lo': line_out, 'schedule': schedule, 'activity': activity, 'id': headcode, 'a': arrival_time, 'd': departure_time, 'o': origin, 'dest': dest}})
                else:
                    pass

        prev_working_time = ''
        for k, v in self.train_service_dict.items():
            for item in v:
                if v[item]['activity'] == 'TF':
                    prev_working_time = v[item]['a']
                if v[item]['activity'] == 'TB' and prev_working_time:
                    self.train_service_dict[k][item].update({'inbound_time': prev_working_time})
                    prev_working_time = ""
                    
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
        
    def draw_passing_train(self, x, y, width, entry):

        y_offset = 2
        height_offset = 4
        text_x_offset = -10
        text_y_offset = 15
        id_string = '{}_{}'.format(entry['uid'], self.train_count)
        
        train_plot = self.main_dwg.rect((x - 15, y + y_offset), (width, self.row_height - height_offset), class_='train_plot', id=id_string, stroke='white', fill='#1f8417', stroke_width='2', rx=8, ry=8).dasharray([2, 2])
        evt = 'document.getElementById("{}").addEventListener("click", train_click, false);'.format(id_string, id_string)
        title_text = '{id} ({toc}) {dt} {o} to {dest} ({at})\n\nUID:\t{uid}\nTYPE:\t{schedule}'.format(**entry)
        if entry['lo']:
            title_text += '\nLINE OUT:\t{lo}'.format(**entry)
        script = self.main_dwg.script(content=evt)
        title_text += '\n\nPASS: {d}'.format(**entry)
        train_plot.set_desc(title=title_text, desc=entry['uid'])
        
        self.main_dwg.add(train_plot)
        self.main_dwg.add(script)
        self.main_dwg.add(self.main_dwg.text(entry['id'], insert=(x + text_x_offset, y + text_y_offset), fill='white', font_size='12px', font_weight='bold', font_family='Arial'))
        self.train_count += 1

    def draw_calling_train(self, arr_x, dep_x, y, width, entry):

        y_offset = 2
        height_offset = 4
        text_x_offset = 5
        text_y_offset = 15
        id_string = '{}_{}'.format(entry['uid'], self.train_count)

        train_plot = self.main_dwg.rect((arr_x, y + y_offset), (width, self.row_height - height_offset), class_='train_plot', id=id_string, stroke='white', fill='#f45042', stroke_width='2', rx=8, ry=8)
        evt = 'document.getElementById("{}").addEventListener("click", train_click, false);'.format(id_string, id_string)
        title_text = '{id} ({toc}) {dt} {o} to {dest} ({at})\n\nUID:\t{uid}\nTYPE:\t{schedule}'.format(**entry)
        if entry['lo']:
            title_text += '\nLINE OUT:\t{lo}'.format(**entry)
        script = self.main_dwg.script(content=evt)
        title_text += '\n\nARR: {a}, DEP: {d}'.format(**entry)
        train_plot.set_desc(title=title_text)
        self.main_dwg.add(train_plot)
        self.main_dwg.add(script)
        self.main_dwg.add(self.main_dwg.text(entry['id'], insert=(arr_x + text_x_offset, y + text_y_offset), fill='white', font_size='12px', font_weight='bold', font_family='Arial'))
        self.train_count += 1

    def draw_terminating_train(self, x, y, width, entry):

        y_offset = 2
        height_offset = 4
        text_x_offset = 5
        text_y_offset = 15
        id_string = '{}_{}'.format(entry['uid'], self.train_count)

        train_plot = self.main_dwg.rect((x, y + y_offset), (width, self.row_height - height_offset), class_='train_plot', id=id_string, stroke='white', fill='white', stroke_width='2', rx=8, ry=8)
        evt = 'document.getElementById("{}").addEventListener("click", train_click, false);'.format(id_string, id_string)
        title_text = '{id} ({toc}) {dt} {o} to {dest} ({at})\n\nUID:\t{uid}\nTYPE:\t{schedule}'.format(**entry)
        title_text += '\n\nARR: {a}'.format(**entry)
        train_plot.set_desc(title=title_text)
        script = self.main_dwg.script(content=evt)
        self.main_dwg.add(train_plot)
        self.main_dwg.add(script)
        self.main_dwg.add(self.main_dwg.text(entry['id'], insert=(x + text_x_offset, y + text_y_offset), fill='red', font_size='12px', font_weight='bold', font_family='Arial'))
        self.train_count += 1

    def draw_starting_train(self, x, y, width, entry):

        y_offset = 2
        height_offset = 4
        text_x_offset = 5
        text_y_offset = 32
        id_string = '{}_{}'.format(entry['uid'], self.train_count)

        train_plot = self.main_dwg.rect((x - width, y + y_offset), (width, self.row_height - height_offset), class_='train_plot', id=id_string, stroke='white', fill='white', stroke_width='2', rx=8, ry=8)
        evt = 'document.getElementById("{}").addEventListener("click", train_click, false);'.format(id_string, id_string)
        title_text = '{id} ({toc}) {dt} {o} to {dest} ({at})\n\nUID:\t{uid}\nTYPE:\t{schedule}'.format(**entry)
        if entry['lo']:
            title_text += '\nLINE OUT:\t{lo}'.format(**entry)
        title_text += '\n\nDEP: {d}'.format(**entry)
        train_plot.set_desc(title=title_text)
        script = self.main_dwg.script(content=evt)
        self.main_dwg.add(train_plot)
        self.main_dwg.add(script)
        self.main_dwg.add(self.main_dwg.text(entry['id'], insert=((x - width) + text_x_offset, y + text_y_offset), fill='green', font_size='12px', font_weight='bold', font_family='Arial'))
        self.train_count += 1

    def draw_assoc(self, x1, x2, y):

        y_offset = 2
        height_offset = 4
        train_plot = self.main_dwg.rect((x1, y + y_offset), (x2 - x1, self.row_height - height_offset), id='ASSOC', stroke='white', opacity='1', fill='white', stroke_width='2', rx=8, ry=8)
        self.main_dwg.add(train_plot)

    def render_trains(self):

        for k,v in self.train_service_dict.items():
            for entry in v:
                if re.search('TF|TB', v[entry]['activity']) is not None:
                    if v[entry]['activity'].strip() == 'TB':
                        x = self.return_x_coordinate(v[entry]['d'])
                        y = self.return_y_coordinate(v[entry]['plt'])
                        try:
                            inbound_time = self.return_x_coordinate(v[entry]['inbound_time'])
                            self.draw_assoc(inbound_time, x, y)
                        except Exception as e:
                            pass
                        
        for k, v in self.train_service_dict.items():
            for entry in v:
                
                width = 40
                if re.search('TF|TB', v[entry]['activity']) is not None:
                    # Train either starts or terminates at the TIPLOC.
                    if v[entry]['activity'].strip() == 'TF':
                        # Train terminates at the location
                        x = self.return_x_coordinate(v[entry]['a'])
                        y = self.return_y_coordinate(v[entry]['plt'])
                        id = v[entry]['id'] 
                        self.draw_terminating_train(x, y, width, v[entry])
                    if v[entry]['activity'].strip() == 'TB':
                        # Train starts at the location
                        x = self.return_x_coordinate(v[entry]['d'])
                        y = self.return_y_coordinate(v[entry]['plt'])
                        id = v[entry]['id'] 
                    
                        self.draw_starting_train(x, y, width, v[entry])

                else:  # Train calls
                    if v[entry]['a'].strip():
                        arr_x = self.return_x_coordinate(v[entry]['a'])
                        dep_x = self.return_x_coordinate(v[entry]['d'])
                        y = self.return_y_coordinate(v[entry]['plt'])
                        width = dep_x - arr_x
                        self.draw_calling_train(arr_x, dep_x, y, width, v[entry])

                    else:
                        # Train passes through
                        x = self.return_x_coordinate(v[entry]['d'])
                        y = self.return_y_coordinate(v[entry]['plt'])
                        self.draw_passing_train(x, y, width, v[entry])   


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

            tm_hour = f'0{val.hour}'[-2:]
            tm_min = f'0{val.minute}'[-2:]

            if str(val.minute) == '0':
                self.main_dwg.add(self.main_dwg.line(start=(x, (y - 15)), end=(x, (y + 15)), stroke_width=2, stroke='red'))
                self.main_dwg.add(self.main_dwg.text(tm_hour + ':00', insert=(x - 15, y + 26), fill='black', font_size='12px', font_weight='bold', font_family='Arial'))
                self.main_dwg.add(self.main_dwg.line(start=(x, 0), end=(x, self.svg_height - self.bottom_border), stroke_width=1, stroke='grey', opacity='0.40'))
            if str(val.minute) == '15' or str(val.minute) == '45':
                self.main_dwg.add(self.main_dwg.line(start=(x, (y - 10)), end=(x, (y + 10)), stroke_width=2, stroke='black'))
                self.main_dwg.add(self.main_dwg.text(tm_min, insert=(x - 7, y + 20), fill='black', font_size='12px', font_weight='bold', font_family='Arial'))
                self.main_dwg.add(self.main_dwg.line(start=(x, 0), end=(x, self.svg_height - self.bottom_border), stroke_width=1, stroke='grey', opacity='0.40'))
            if str(val.minute) == '30':
                self.main_dwg.add(self.main_dwg.circle(center=(x, y), r=4, fill='red'))
                self.main_dwg.add(self.main_dwg.text(str(tm_min), insert=(x - 7, y + 15), fill='black', font_size='12px', font_weight='bold', font_family='Arial'))
                self.main_dwg.add(self.main_dwg.line(start=(x, 0), end=(x, self.svg_height - self.bottom_border), stroke_width=1, stroke='grey', opacity='0.40'))
            if str(val.minute) in ('5', '10', '20', '25', '35', '40', '50', '55'):
                self.main_dwg.add(self.main_dwg.line(start=(x, y - 3), end=(x, y + 3), stroke_width=1, stroke='black'))
                self.main_dwg.add(self.main_dwg.text(tm_min, insert=(x - 5, y + 15), fill='black', font_size='10px', font_weight='bold', font_family='Arial'))
                self.main_dwg.add(self.main_dwg.line(start=(x, 0), end=(x, self.svg_height - self.bottom_border), stroke_width=1, stroke='grey', opacity='0.40'))
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
        text_y_offset = -25  # The amount of y offset for platform text.

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

            # Add text label
            self.index_dwg.add(self.index_dwg.text(platform[1], insert=(10, (y - text_y_offset)), class_='platform_text'))
            img = self.index_dwg.image('/static/{}.png'.format(platform[2]), insert=(130, y + 10), height=20)
            
            try:

                length = platform[3]
                try:
                    length_down = int(platform[4])
                except Exception as ex:
                    title_text = 'Length: {}m [{}]'.format(length, platform[4])
                else:
                    title_text = 'Length (UP): {}m, (DOWN): {}m [{}]'.format(length, length_down, platform[5])
            except Exception as e:
                pass
            else:
                img.set_desc(title=title_text)
            finally:
                self.index_dwg.add(img)

            y += self.seperator_line_height + self.row_height

    def return_scroll_left(self):

        # This function provides JS to scroll the current time line into view.

        js_string = """
        <script>
            function scroll_left() {{
                // Scroll the current time line into view
                /*var time_now_x = document.getElementById("time_now").getAttribute('x1');
                var main_div = document.getElementById("main_div");
                var svg_width = {};
                time_line_percentage = (time_now_x / svg_width) * 100;
                var scrollable = main_div.scrollWidth - main_div.clientWidth;
                var scroll_left = (time_line_percentage * scrollable) / 100;
                main_div.scrollLeft = scroll_left;*/

                var element = document.getElementById("time_now");
                element.scrollIntoView({{behaviour: 'smooth', inline: "center"}});

            }};
        </script>""".format(self.main_svg_width)
        return re.sub(r" {2,}|\t", "", js_string.strip())    

    def return_auto_scroll_js(self):
        
        # This javascript function is called every 30 seconds and animates the timeline.

        js_string = """
        <script>
            function scroll_tl(){{
                var time_line = document.getElementById("time_now");
                var start_time = new Date('{}');
                var current_time = new Date();
                start_time.setFullYear(current_time.getFullYear());
                start_time.setMonth(current_time.getMonth());
                start_time.setDate(current_time.getDate());
                var diff = current_time - start_time;
                var minutes = Math.floor(diff / 1000 / 60);
                time_line.setAttribute('x1', minutes * {} + 10);
                time_line.setAttribute('x2', minutes * {} + 10);
                var time_clock = document.getElementById("time_clock");
                time_clock.setAttribute('x', minutes * {} - 2);
                hh = ("0" + current_time.getHours()).slice(-2);
                mm = ("0" + current_time.getMinutes()).slice(-2);
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
        self.index_svg_width = 160 # Width of the platform index column
        self.ticks = 0  # Calcuation of ratio pixels to minutes.
        self.bottom_border = 75  # pixels at the bottom of the docker we keep for timeline.
        self.platforms = self.parse_platforms(SVGObject.JSON)  # Get the platform details from the configuration
        self.seperator_line_height = 2  # The height of the seperators lines.
        self.tl_padding = 10  # The padding either side of the timeline
        self.scroll_left = 0
        #self.trains = []
        self.platform_bottom_line = {}
        self.row_height = 0
        self.train_service_dict = {}
        self.train_count = 0
        
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
