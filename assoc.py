import re
with open('PADTON.csv', newline='\n') as csvfile:
    ln = 0
    terminating_trains = []
    starting_trains = []
    assoc = []
    for line in csvfile:
        #print(line)
        ln += 1
        entry = line.split(',')

        if str(entry[4]).strip() == 'LONDON PADDINGTON':
            if str(entry[8]).strip() != '':
                if str(entry[9]).strip() == '':
                    # Terminating Train
                    terminating_trains.append({'train': str(entry[1]).strip(), 'platform': str(entry[6]).strip(), 'entry': ln})
        
        if str(entry[3]).strip() == 'LONDON PADDINGTON':
            if str(entry[8]).strip() == '':
                if str(entry[9]).strip() != '':
                    # Starting Train
                    starting_trains.append({'train': str(entry[1]).strip(), 'platform': str(entry[6]).strip(), 'entry': ln})

for term_train in terminating_trains:
    train = term_train['train']
    platform = term_train['platform']
    entry = int(term_train['entry'])
    for start_train in starting_trains:
        if not start_train['platform'] == platform:
            pass
        else:
            if not int(start_train['entry']) > entry:
                pass
            else:
                #print('[{}]Train: {} (Platform {}), next working is [{}]Train: {} (Platform {})'.format(entry, train, platform, start_train['entry'], start_train['train'], start_train['platform']))
                assoc.append({'line': entry, 'next_working': start_train['train']})
                assoc.append({'line': start_train['entry'], 'previous_working': train})
                break


scan_line = 0
new_csv = ''
no_assoc = True
with open('PADTON.csv', newline='\n') as csvfile:
    for line in csvfile:
        no_assoc = True
        scan_line += 1
        ln = re.sub('\n', '', line)
        if scan_line > 1:
            for item in assoc:
                if int(item['line']) == scan_line:
                    if 'previous_working' in item:
                   
                        new_csv += '{},(P): {}\n'.format(ln, item['previous_working'])
                        no_assoc = False
                    else:
                       
                        new_csv += '{},(N): {}\n'.format(ln, item['next_working'])
                        no_assoc = False
        if no_assoc:
            new_csv += '{}\n'.format(ln)
            no_assoc = True

print(new_csv)