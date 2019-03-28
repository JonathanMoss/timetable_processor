import re


class AssociationEngine:
    def __init__(self, fname, location, header=False):

        self.fname = fname
        self.location = location
        self.entry = []
        self.include_header_row = header
        self.trace = False
        self.parse_csv()

    def get_next_departures(self, platform=1, starting_index=0):

        start = starting_index  # What row to start the search
        max_index = len(self.entry)  # What column to finish the search

        next_workings = []  # Create an empty list

        # Increment i by 1 each time
        for i in range(start, max_index):
            sl = self.entry[i].split(',')  # Get the entry[i] and split it by comma value
            if sl[7].strip() == platform:  # We have found a platform match
                if sl[0].strip() == 'TB':  # We have found a TB match
                    # Found a next working candidate; need to keep going and search for another arrival (join)
                    next_workings.append(sl[2])  # Add the next working to the list.

                    # Starting at the next index, continue to look forward for another TB - indicating a possible join
                    for x in range(i+1, max_index):
                        sp = self.entry[x].split(',')
                        if sp[7].strip() == platform:  # Another platform match
                            if sp[0].strip() == 'TF':  # Another train begins match
                                return next_workings
                            elif sp[0].strip() == 'TB':
                                next_workings.append(sp[2])
                    return next_workings

        return None

    def get_inbound_working(self, platform=1, starting_index=0):

        start = starting_index -1 # What row to start the search
        min_index = -1  # What row to end the search
        previous_workings = []  # List to contain the matches.

        for i in range(start, min_index, -1):  # Reverse the range (step backwards)

            sl = self.entry[i].split(',')
            if sl[7].strip() == platform:
                if sl[0].strip() == 'TF':
                    # Found a candidate previous working
                    previous_workings.append(sl[2])

                    for x in range(i-1, min_index, -1):
                        sp = self.entry[x].split(',')
                        if sp[7].strip() == platform:
                            if sp[0].strip() == 'TB':
                                return previous_workings
                            elif sp[0].strip() == 'TF':
                                previous_workings.append(sp[2])
                    return previous_workings

        return None

    def parse_csv(self):

        with open(self.fname, newline='\n') as f:
            for row in f:
                self.entry.append(re.sub('\n', '', row))

        if not self.include_header_row:
            self.entry.pop(0)

        # Get next working and splits

        for line in self.entry:
            index = self.entry.index(line)
            sl = line.split(',')
            if re.match('^TF', line):
                platform = sl[7].strip()

                nw = self.get_next_departures(platform, index)

                if nw:
                    if len(nw) > 1:
                        self.entry[index] += ', Split Next Working {}'.format(nw)
                    else:
                        self.entry[index] += ', Next Working {}'.format(nw)

        # Get previous workings and joins
        for line in self.entry:
            index = self.entry.index(line)
            sl = line.split(',')
            if re.match('^TB', line):
                platform = sl[7].strip()
                pw = self.get_inbound_working(platform=platform, starting_index=index)

                if pw:
                    if len(pw) > 1:
                        self.entry[index] += ', Join Previous Working {}'.format(pw)
                    else:
                        self.entry[index] += ', Previous Working {}'.format(pw)

        for line in self.entry:
            print(line)
            pass


if __name__ == '__main__':
    assoc = AssociationEngine(fname='LVRPLSH.csv', location='LONDON PADDINGTON', header=False)


# with open('PADTON.csv', newline='\n') as csvfile:
#     ln = 0
#     terminating_trains = []
#     starting_trains = []
#     assoc = []
#     for line in csvfile:
#         ln += 1
#         entry = line.split(',')

#         if str(entry[4]).strip() == 'LONDON PADDINGTON':
#             if str(entry[8]).strip() != '':
#                 if str(entry[9]).strip() == '':
#                     # Terminating Train
#                     terminating_trains.append({'train': str(entry[1]).strip(), 'platform': str(entry[6]).strip(), 'entry': ln})
        
#         if str(entry[3]).strip() == 'LONDON PADDINGTON':
#             if str(entry[8]).strip() == '':
#                 if str(entry[9]).strip() != '':
#                     # Starting Train
#                     starting_trains.append({'train': str(entry[1]).strip(), 'platform': str(entry[6]).strip(), 'entry': ln})

# for term_train in terminating_trains:
#     train = term_train['train']
#     platform = term_train['platform']
#     entry = int(term_train['entry'])
#     for start_train in starting_trains:
#         if not start_train['platform'] == platform:
#             pass
#         else:
#             if not int(start_train['entry']) > entry:
#                 pass
#             else:
#                 #print('[{}]Train: {} (Platform {}), next working is [{}]Train: {} (Platform {})'.format(entry, train, platform, start_train['entry'], start_train['train'], start_train['platform']))
#                 assoc.append({'line': entry, 'next_working': start_train['train']})
#                 assoc.append({'line': start_train['entry'], 'previous_working': train})

#                 break

# scan_line = 0
# new_csv = ''
# no_assoc = True
# with open('PADTON.csv', newline='\n') as csvfile:
#     for line in csvfile:
#         no_assoc = True
#         scan_line += 1
#         ln = re.sub('\n', '', line)
#         if scan_line > 1:
#             for item in assoc:
#                 if int(item['line']) == scan_line:
#                     if 'previous_working' in item:
                   
#                         new_csv += '{},(P): {}\n'.format(ln, item['previous_working'])
#                         no_assoc = False
#                     else:
                       
#                         new_csv += '{},(N): {}\n'.format(ln, item['next_working'])
#                         no_assoc = False
#         if no_assoc:
#             new_csv += '{}\n'.format(ln)
#             no_assoc = True

# print(new_csv)