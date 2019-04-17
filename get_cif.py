import requests
import os
import shutil
import gzip
from datetime import datetime, timedelta
import argparse
import sqlite3
import re
from progress.bar import Bar
from dateutil.relativedelta import relativedelta, FR
import subprocess

parser = argparse.ArgumentParser(description='Download CIF files from Network Rail.')
group = parser.add_mutually_exclusive_group()
group.add_argument('-f', '--full',
                   action='store_true',
                   help='Download full CIF')
group.add_argument('-u', '--update',
                   action='store_true',
                   help='Download CIF update (default action)')
group.add_argument('-r', '--reset',
                   action='store_true',
                   help='Download the Full CIF and all relevant updates')
parser.add_argument('-q', '--quiet',
                    action='store_true',
                    help='Suppress progress bar')
parser.add_argument('-d', '--day',
                    choices=['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'],
                    help='Override the automatic day selection (forces -u)')
parser.add_argument('-k', '--keep',
                    action='store_true',
                    help='Do not delete the downloaded cif from "/tmp" after unzip operation')
args = parser.parse_args()


class GetCif:

    username = 'joth.moss@googlemail.com'
    password = 'Chester42!'
    url = 'https://datafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate'
    folder = './tmp'
    downloaded_cif = 'dl_cif.gz'
    cif_folder = './cif'
    file_name = os.path.join(folder, downloaded_cif)
    db_file_name = './cif_record.db'

    @staticmethod
    def reset_list(date_time_now):

        for the_file in os.listdir(GetCif.cif_folder):
            file_path = os.path.join(GetCif.cif_folder, the_file)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print('Removed "{}"'.format(file_path))
            except Exception as e:
                print(e)
        file_list = []
        today = date_time_now
        last_friday = today + relativedelta(weekday=FR(-1))
        delta = today - last_friday
        if delta.days > 0:
            for d in range(0, delta.days):
                if d == 0:
                    file_list.append({'full': True})
                else:
                    day = last_friday + timedelta(days=d)
                    day = day.strftime('%a').lower()
                    file_list.append({'cif_day': day})
        else:
            file_list.append({'full': True})

        return file_list

    def __init__(self, cif_day=None, full_cif=False, arguments=None):

        self.full_cif = full_cif
        self.show_progress = True
        self.downloaded_cif = ""
        self.tmp_path = ""
        self.hd = {}
        self.arguments = arguments
        self.file_size_compressed = ""
        self.file_size_uncompressed = ""
        self.uncompressed_file_path = ""
        self.out_of_sequence = False

        if cif_day is None:
            self.yesterday = datetime.today() - timedelta(days=1)
            self.yesterday = self.yesterday.strftime('%a').lower()

        # Set the connection parameters based on the CIF type - full bleed or update.

            if self.full_cif:
                self.params = (
                    ('type', 'CIF_ALL_FULL_DAILY'),
                    ('day', 'toc-full.CIF.gz'),
                )
                self.downloaded_cif = self.params[1][1]
                self.tmp_path = os.path.join(GetCif.folder, self.downloaded_cif)

            else:
                day = 'toc-update-{}.CIF.gz'.format(self.yesterday)
                self.params = (
                    ('type', 'CIF_ALL_UPDATE_DAILY'),
                    ('day', day),
                )
                self.downloaded_cif = day
                self.tmp_path = os.path.join(GetCif.folder, self.downloaded_cif)
        else:
            day = 'toc-update-{}.CIF.gz'.format(cif_day)
            self.params = (
                ('type', 'CIF_ALL_UPDATE_DAILY'),
                ('day', day),
            )

            self.downloaded_cif = self.params[1][1]
            self.tmp_path = os.path.join(GetCif.folder, self.downloaded_cif)

    def clear_folder(self):

        """ This function clears out the CIF folder. """

        for the_file in os.listdir(GetCif.folder):
            file_path = os.path.join(GetCif.folder, the_file)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print('Removed "{}"'.format(file_path))
            except Exception as e:
                print(e)

    def download_cif(self):

        """ This is the function that actually downloads the CIF - showing a progress bar (if not supressed). """

        try:
            with requests.get(GetCif.url,
                              allow_redirects=True,
                              params=self.params,
                              auth=(GetCif.username, GetCif.password), stream=True) as r:
                total_length = int(r.headers['Content-Length'])
                self.file_size_compressed = self.sizeof_fmt(total_length)
                dl = 0
                with open(self.tmp_path, 'wb') as f:
                    bar = Bar('Downloading {}'.format(self.tmp_path), max=total_length, suffix='%(percent)d%%')
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            dl += len(chunk)
                            if self.show_progress:
                                bar.index = dl
                                bar.next()
                            f.write(chunk)
            bar.finish()
            print('\n{} downloaded [{} bytes]'.format(self.tmp_path, dl))

        except Exception as e:
            print(e)

    def sizeof_fmt(self, num, suffix='B'):

        """ This function returns human readable representation of a file size specified as an argument. """

        for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, 'Yi', suffix)

    def unzip_file(self, keep=False):

        """ This function uncompresses the CIF file, if keep is True, the .gz file is kept, otherwise deleted. """

        print(f'Uncompressing {self.downloaded_cif}...')
        self.uncompressed_file_path = os.path.join(GetCif.cif_folder, str(self.downloaded_cif).lower()[0:-3])

        with gzip.open(self.tmp_path, 'rb') as f_in:
            with open(self.uncompressed_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                print(f'CIF uncompressed to "{self.uncompressed_file_path}"')
                self.file_size_uncompressed = self.sizeof_fmt(os.path.getsize(self.uncompressed_file_path))
        if not keep:
            if os.path.isfile(self.tmp_path):
                os.remove(self.tmp_path)
                print(f'Removed temporary file: "{self.tmp_path}"')

    def update_db(self):

        """ This function updates the cif_record.db records """
        if not self.out_of_sequence:
            sql_string = """
                INSERT into `tbl_downloaded_cif`
                    (`txt_filename`,
                    `txt_uncompressed_filename`,
                    `txt_date_time`,
                    `int_success`,
                    `txt_arguments`,
                    `txt_mainframe_id`,
                    `txt_extract_date`,
                    `txt_extract_time`,
                    `txt_current_file_ref`,
                    `txt_last_file_ref`,
                    `txt_update_indicator`,
                    `txt_version`,
                    `txt_start_date`,
                    `txt_end_date`,
                    `txt_uncompressed_size`,
                    `txt_compressed_size`)
                    VALUES ("{}", "{}", "{}", 1, "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}");""".format(
                        self.downloaded_cif,
                        self.uncompressed_file_path,
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        self.arguments,
                        self.hd['mainframe_identity'],
                        self.hd['date_of_extract'],
                        self.hd['time_of_extract'],
                        self.hd['current_file_reference'],
                        self.hd['last_file_reference'],
                        self.hd['update_indicator'],
                        self.hd['version'],
                        self.hd['start_date'],
                        self.hd['end_date'],
                        self.file_size_uncompressed,
                        self.file_size_compressed)
            sql_string = re.sub(r" {2,}|\n", "", sql_string.strip())
            conn = sqlite3.connect(GetCif.db_file_name)
            conn.execute(sql_string)
            conn.commit()
            conn.close()

    def grep(self, file, search_string):

        """This function runs a grep process as return the resulting matches"""

        process = subprocess.Popen(['grep', search_string, file], stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return stdout.decode('utf-8')

    def get_header(self):

        """ This function searches the cif file for a header record and writes it to a dictionary """

        file_path = os.path.join(GetCif.cif_folder, str(self.downloaded_cif).lower()[0:-3])
        if os.path.isfile(file_path):
            header = self.grep(file_path, '^HD')
            if len(header) == 81:
                self.hd = {'record_identity': header[0:2],
                           'mainframe_identity': header[2:22],
                           'date_of_extract': header[22:28],
                           'time_of_extract': header[28:32],
                           'current_file_reference': header[32:39],
                           'last_file_reference': header[39:46],
                           'update_indicator': header[46],
                           'version': header[47],
                           'start_date': header[48:54],
                           'end_date': header[54:60]}

        if 'U' in self.hd['update_indicator']: 
            if not self.confirm_sequence():
                print(f'Out of sequence CIF downloaded: {self.downloaded_cif}')
                self.out_of_sequence = True
                GetCif.delete_file(file_path)
            else:
                self.out_of_sequence = False
    
    @staticmethod
    def delete_file(file_name):

        try:
            os.remove(file_name)
        except:
            print(f'Cannot remove file: {file_name}')
        else:
            print(f'{file_name} deleted')

    @staticmethod
    def create_db():

        """ This function runs a python script that cleans the cif_record.db """

        process = subprocess.Popen(['python3', 'cif_record.py'], stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()

    @staticmethod
    def reset(arg):

        """ This function initiates the deletion and re-download of all CIF
        up to and including the last available full bleed """

        GetCif.create_db()  # Clear the database (i.e. start from fresh)
        l = GetCif.reset_list(datetime.now())  # Create the list of required CIF files based on today's date.

        # Itterate through the CIF list
        for fn in l:
            try:
                if fn['full']:
                    reset_cif = GetCif(full_cif=True, arguments=arg)
            except Exception:
                reset_cif = GetCif(fn['cif_day'], full_cif=False, arguments=arg)
            finally:
                reset_cif.clear_folder()
                reset_cif.download_cif()
                reset_cif.unzip_file(arg.keep)
                reset_cif.get_header()
                reset_cif.update_db()
    
    def confirm_sequence(self):
        
        sql_string = """SELECT tbl_downloaded_cif.txt_current_file_ref, tbl_downloaded_cif.txt_update_indicator FROM tbl_downloaded_cif ORDER BY tbl_downloaded_cif.int_index DESC LIMIT 1"""
        conn = sqlite3.connect(GetCif.db_file_name)
        c = conn.cursor()
        c.execute(sql_string)
        res = c.fetchone()
        conn.close()
        last_letter_db = res[0][-1]
        next_in_sequence = chr(ord(last_letter_db) + 1)
        last_update_indicator = res[1]
        current_letter_cif = self.hd['current_file_reference'][-1]

        if last_update_indicator == 'F':
            return True
        
        if last_letter_db == 'Z' and current_letter_cif == 'A':
            return True
        
        if current_letter_cif == next_in_sequence:
            return True

        return False
        
if __name__ == '__main__':

    if args.reset:
        GetCif.reset(args)
    else:
        if args.full:
            cif = GetCif(full_cif=True, arguments=args)
        else:
            cif = GetCif(args.day, full_cif=False, arguments=args)
        if args.quiet:
            cif.show_progress = False
        else:
            cif.show_progress = True

        cif.download_cif()
        cif.unzip_file(args.keep)
        cif.get_header()
        cif.update_db()
