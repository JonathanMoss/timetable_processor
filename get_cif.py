import requests
import os
import shutil
import sys
import gzip
from datetime import datetime, timedelta
import argparse
import sqlite3
import re
from progress.bar import Bar
from dateutil.relativedelta import relativedelta, FR


parser = argparse.ArgumentParser(description='Download CIF files from Network Rail.')
group = parser.add_mutually_exclusive_group()
group.add_argument('-f', '--full', action='store_true', help='Download full CIF')
group.add_argument('-u', '--update', action='store_true', help='Download CIF update (default action)')
group.add_argument('-r', '--reset', action='store_true', help='Download the Full CIF and all relevant updates')
parser.add_argument('-q', '--quiet', action='store_true', help='Suppress progress bar')
parser.add_argument('-c', '--compressed', action='store_true',
                    help='CIF remains compressed - not unzipped and placed into CIF folder')
parser.add_argument('-d', '--day',choices=['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'],
                    help='Override the automatic day selection (forces -u)')
parser.add_argument('-k', '--keep', action='store_true',
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
        file_list = []
        today = date_time_now
        #today = datetime(2019, 4, 19, 20, 28, 00)
        last_friday = today + relativedelta(weekday=FR(-1))
        delta = today - last_friday
        if delta.days > 0:
            for d in range(0, delta.days):
                if d == 0:
                    file_list.append('toc-full.CIF.gz')
                else:
                    day = last_friday + timedelta(days=d)
                    day = day.strftime('%a').lower()
                    file_list.append(f'toc-update-{day}.CIF.gz')
        else:
            file_list.append('toc-full.CIF.gz')
           
        return file_list

    def __init__(self, cif_day=None, full_cif=False):
        self.full_cif = full_cif
        self.show_progress = True
        self.downloaded_cif = ""
        self.tmp_path = ""

        if cif_day is None:
            self.yesterday = datetime.today() - timedelta(days=1)
            self.yesterday = self.yesterday.strftime('%a').lower()

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
  
    @staticmethod
    def clear_folder():

        for the_file in os.listdir(GetCif.folder):
            file_path = os.path.join(GetCif.folder, the_file)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print('Removed {}'.format(file_path))
            except Exception as e:
                print(e)

    def download_cif(self, arguments, **kwargs):

        lst = kwargs.get('lst')

        try:
            with requests.get(GetCif.url, 
                              allow_redirects=True, 
                              params=self.params, 
                              auth=(GetCif.username, GetCif.password), stream=True) as r:
                total_length = int(r.headers['Content-Length'])
                dl = 0
                with open(self.tmp_path, 'wb') as f:
                    bar = Bar('Downloading {}'.format(self.tmp_path), max=total_length, suffix='%(percent)d%%')
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            dl += len(chunk)
                            if self.show_progress:
                                bar.index=dl
                                bar.next()
                            f.write(chunk)
            bar.finish()
            print('\n{} downloaded [{} bytes]'.format(self.tmp_path, dl))

        except Exception as e:
            print(e)
        finally:
            # Update Database
            sql_string = """
            INSERT into `tbl_downloaded_cif` 
                (`txt_filename`, 
                `txt_date_time`, 
                `int_success`, 
                `txt_arguments`) 
            VALUES ("{}", datetime("now"), 1, "{}");""".format(self.downloaded_cif, str(arguments))
            sql_string = re.sub(r" {2,}|\n", "", sql_string.strip())
            conn = sqlite3.connect(GetCif.db_file_name)
            conn.execute(sql_string)
            conn.commit()
            conn.close()

    def unzip_file(self, keep=False):

        print(f'Uncompressing {self.downloaded_cif}...')
        new_cif_fn = os.path.join(GetCif.cif_folder, str(self.downloaded_cif).lower()[0:-3])

        with gzip.open(self.tmp_path, 'rb') as f_in:
            with open(new_cif_fn, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                print(f'CIF uncompressed to {new_cif_fn}')             

        if not keep:
            if os.path.isfile(self.tmp_path):
                os.remove(self.tmp_path)
                print(f'Removed temporary file: {self.tmp_path}')


if __name__ == '__main__':
    
    if args.reset:
        l = GetCif.reset_list(datetime.now())
        for fn in l:
            cif = GetCif(args.day, ls = l)
    if args.full:
        cif = GetCif(args.day, full_cif=True)
    else:
        cif = GetCif(args.day, full_cif=False)
    if args.quiet:
        cif.show_progress = False
    else:
        cif.show_progress = True

    cif.clear_folder()
    cif.download_cif(args)
    
    if not args.compressed:
        cif.unzip_file(args.keep)
