#!/usr/bin/python3
import argparse
import os
import subprocess
import sys
import json

parser = argparse.ArgumentParser(description='Enriches schedules passed with b-plan data for missing line/path codes.')
parser.add_argument('-n', '--nwk', help='The text file that contains the NWK data.', required=True)
parser.add_argument('-a', '--tiploc_a', help='The starting TIPLOC', required=True)
parser.add_argument('-b', '--tiploc_b', help='The end TIPLOC', required=True)

args = parser.parse_args()

JSON = """
    {"uid": "C10906", 
    "type": "P", 
    "train_description": "1O04 (XC) 05:11:00 MNCRPIC to BOMO (10:13:00)", 
    "schedule_line": [
        ["MNCRPIC", "", "05:11:00", "", "", "6", "SL", "TB"], 
        ["ARDWCKJ", "", "", "05:13:30", "", "", "", ""], 
        ["SLDLJN", "", "", "05:17:00", "", "", "", ""], 
        ["HLDG", "", "", "05:22:00", "", "2", "", ""], 
        ["HLDGSJ", "", "", "05:23:00", "", "", "", ""], 
        ["WLMSL", "", "", "05:26:00", "", "2", "", ""], 
        ["ALDEDGE", "", "", "05:27:30", "", "", "", ""], 
        ["SBCH", "", "", "05:35:30", "", "1", "FL", ""], 
        ["CREWSBG", "", "", "05:40:00", "", "", "", ""], 
        ["CREWE", "05:42:30", "05:47:30", "", "", "5", "", "T"], 
        ["CREWSJN", "", "", "05:50:00", "", "UDP", "", ""], 
        ["BTHLYJN", "", "", "05:53:00", "", "", "", ""], 
        ["ALSAGER", "", "", "05:55:00", "", "", "", ""], 
        ["KIDSGRV", "", "", "05:58:30", "", "", "", ""], 
        ["STOKEOT", "06:06:30", "06:08:30", "", "", "1", "", "T"], 
        ["STOKOTJ", "", "", "06:09:30", "", "", "", ""], 
        ["STONE", "", "", "06:15:30", "", "", "", ""], 
        ["YFIELDJ", "", "", "06:18:30", "", "", "UNB", ""], 
        ["LBRDFJN", "", "", "06:20:30", "", "", "SL", ""], 
        ["STAFFRD", "06:24:00", "06:25:30", "", "SL", "4", "SL", "T"], 
        ["STAFTVJ", "", "", "06:26:30", "", "", "", ""], 
        ["PNKRDG", "", "", "06:30:00", "", "", "", ""], 
        ["BSBYJN", "", "", "06:34:30", "", "", "", ""], 
        ["WVRMTNJ", "", "", "06:36:00", "", "", "", ""], 
        ["WVRMPTN", "06:37:30", "06:41:00", "", "", "3", "", "T"], 
        ["DUDLPT", "", "", "06:47:30", "", "", "", ""], 
        ["GALTONJ", "", "", "06:51:30", "", "", "", ""], 
        ["SOHOSJ", "", "", "06:53:00", "", "", "", ""], 
        ["BHAMNWS", "06:57:00", "07:04:00", "", "", "1A", "WL", "T"], 
        ["PROOFHJ", "", "", "07:06:00", "", "", "", ""], 
        ["STECHFD", "", "", "07:09:00", "", "", "", ""], 
        ["BHAMINT", "07:12:30", "07:14:00", "", "", "4", "", "T"], 
        ["BKSWELL", "", "", "07:18:00", "", "", "", ""], 
        ["COVNTRY", "07:23:30", "07:25:00", "", "", "2", "", "T"], 
        ["COVNGHJ", "", "", "07:28:30", "", "", "", ""], 
        ["KENLWTH", "", "", "07:30:00", "", "", "", ""], 
        ["MLVTJN", "", "", "07:33:00", "", "", "", ""], 
        ["LMNGTNS", "07:36:30", "07:38:00", "", "", "3", "", "T"], 
        ["FENNYCM", "", "", "07:47:00", "", "", "", ""], 
        ["BNBRRJN", "", "", "07:51:30", "", "", "UCV", ""], 
        ["BNBR", "07:53:30", "07:55:00", "", "", "3", "", "T"], 
        ["AYNHOJ", "", "", "08:00:30", "", "", "", ""], 
        ["HEYFORD", "", "", "08:04:30", "", "1", "", ""], 
        ["WVCTJN", "", "", "08:11:00", "", "", "UML", ""], 
        ["OXFDNNJ", "", "", "08:12:30", "", "", "UML", ""], 
        ["OXFD", "08:14:00", "08:16:00", "", "UML", "3", "", "T"], 
        ["KNNGTNJ", "", "", "08:18:30", "", "", "", ""], 
        ["DIDCTNJ", "", "", "08:24:30", "", "", "", ""], 
        ["DIDCTEJ", "", "", "08:25:30", "", "", "ML", ""], 
        ["GORASTR", "", "", "08:31:00", "", "", "ML", ""], 
        ["RDNGHLJ", "", "", "08:38:00", "", "", "FVL", ""], 
        ["RDNGSTN", "08:40:30", "08:49:00", "", "", "3", "WL", "T RM"], 
        ["RDNGORJ", "", "", "08:50:30", "", "", "", ""], 
        ["SCOTEJN", "", "", "08:52:00", "", "", "", ""], 
        ["BMLY", "", "", "09:01:30", "", "", "", ""], 
        ["BSNGSTK", "09:10:00", "09:11:30", "", "", "1", "SL", "T"], 
        ["WRTINGJ", "", "", "09:14:30", "", "", "", ""], 
        ["WALRSAL", "", "", "09:21:30", "", "", "", ""], 
        ["WNCHSTR", "09:25:00", "09:26:30", "", "", "2", "", "T"], 
        ["SHAWFDJ", "", "", "09:29:30", "", "", "FL", ""], 
        ["ELGH", "", "", "09:32:00", "", "DF", "", ""], 
        ["SOTPKWY", "09:33:30", "09:35:00", "", "", "2", "", "T"], 
        ["STDENYS", "", "", "09:39:00", "", "", "FL", ""], 
        ["NTHMJN", "", "", "09:40:30", "", "", "", ""], 
        ["SOTON", "09:43:00", "09:45:00", "", "", "4", "SL", "T"], 
        ["REDBDGE", "", "", "09:48:30", "", "", "", ""], 
        ["BKNHRST", "09:57:30", "09:59:00", "", "", "3", "", "T"], 
        ["BOMO", "10:13:00", "", "", "", "3", "", "TF"]]}
"""

class EnrichSchedule:
    """This class enriches schedules passed with b-plan data (missing line/path codes)"""
    def __init__(self, args):
        
        self.nwk = args.nwk
        self.tiploc_a = args.tiploc_a
        self.tiploc_b = args.tiploc_b
        self.schedule_json = None
        return_string = '{}\n'.format(str(self.return_options(self.tiploc_a, self.tiploc_b)))
        sys.stdout.write(return_string)

    def grep(self, search_string):

        """This function runs a grep process and returns any matches"""

        process = subprocess.Popen(['grep', '-P', search_string, self.nwk], stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return stdout.decode('utf-8')

    def return_options(self, tiploc_a, tiploc_b):

        """This function takes 2 TIPLOC's and returns a list containing all possible line out options"""
        ret_val = []
        search_string = f"NWK\\tA\\t{tiploc_a}\\t{tiploc_b}"
        grep_return = self.grep(search_string)

        for line in grep_return.splitlines():
            direction = line.split('\t')[8].strip()
            val = line.split('\t')[4].strip()
            if val:
            	ret_val.append(line.split('\t')[4].strip())
        
        return {'direction': direction, 'valid_lines': ret_val}

    def parse_json_string(self, json_string):

        curr_line = None
        js = json.loads(json_string)
        x = len(js['schedule_line'])
        for i in range(0, x - 1, 1):
            data = js['schedule_line'][i]
            current_tiploc = data[0].strip()
            arrival_time = data[1].strip()
            departure_time = data[2].strip()
            passing_time = data[3].strip()
            path_in = data[4].strip()
            platform = data[5].strip()
            line_out = data[6].strip()
            activity = data[7].strip()

            if i < x:
                data = js['schedule_line'][i + 1]
                next_tiploc = data[0].strip()
                next_arrival_time = data[1].strip()
                next_departure_time = data[2].strip()
                next_passing_time = data[3].strip()
                next_path_in = data[4].strip()
                next_platform = data[5].strip()
                next_line_out = data[6].strip()
                next_activity = data[7].strip()
            
            val = self.return_options(current_tiploc, next_tiploc)
            direction = val['direction']
            options = val['valid_lines']
            if i == 0:  #First TIPLOC
                if line_out: # Check there is a TIPLOC
                    curr_line = direction + line_out
                    line_out = curr_line
                else:
                    if not options:
                        curr_line = direction + 'L'
                        line_out = curr_line
                    else:
                        curr_line = direction + options[0]
                        line_out = curr_line
            else:
                if not path_in:
                    path_in = curr_line
                    curr_line = None

                if not line_out:
                    if not options:
                        line_out = direction + 'L'
                        curr_line = line_out
                    else:
                        print(options)
                        curr_line = direction + options[0]
                        line_out = curr_line
 
                



            print(f'{current_tiploc}, {arrival_time}, {departure_time}, {passing_time}, {path_in}, {platform}, {line_out}, {activity}')
            




if __name__ == "__main__":
    es = EnrichSchedule(args)
    es.parse_json_string(JSON)