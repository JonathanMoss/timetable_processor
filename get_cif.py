import requests
import os, shutil
import sys
import gzip
from datetime import datetime, timedelta
import argparse
import sqlite3

parser = argparse.ArgumentParser(description='Download CIF files from Network Rail.')
group = parser.add_mutually_exclusive_group()
group.add_argument('-f', '--full', action='store_true', help='Download full CIF')
group.add_argument('-u', '--update', action='store_true', help='Download CIF update (default action)')
parser.add_argument('-q', '--quiet', action='store_true', help='Suppress progress bar')
parser.add_argument('-c', '--compressed', action='store_true', help='CIF remains compressed - not unzipped and placed into CIF folder')
parser.add_argument('-d', '--day',choices=['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'], help='Override the automatic day selection (forces -u)')
parser.add_argument('-k', '--keep', action='store_true', help='Do not delete the downloaded cif from "/tmp" after unzip operation')
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

    def __init__(self, cif_day=None, full_cif=False):
        self.full_cif = full_cif
        self.show_progress = True
        self.downloaded_cif = ""
        self.tmp_path  = ""
        if cif_day == None:
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

    def download_cif(self, args):

        try:
            with requests.get(GetCif.url, allow_redirects=True, params=self.params, auth=(GetCif.username, GetCif.password), stream=True) as r:
                total_length = int(r.headers['Content-Length'])
                dl = 0
                with open(self.tmp_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            dl += len(chunk)
                            if self.show_progress:
                                os.system('clear')
                                print('Downloading CIF - {} bytes remaining...'.format(total_length - dl))
                                done = int(50 * dl / total_length)
                                sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50-done)))
                                sys.stdout.flush()
                            f.write(chunk)
            print('\n{} Downloaded [{} bytes]'.format(self.tmp_path, dl))
        except Exception as e:
            print(e)
        finally:
            # Update Database
            sql_string = 'INSERT into `tbl_downloaded_cif` (`txt_filename`, `txt_date_time`, `int_success`, `txt_arguments`) VALUES ("{}", datetime("now"), 1, "{}")'.format(self.downloaded_cif, str(args))
            conn = sqlite3.connect(GetCif.db_file_name)
            conn.execute(sql_string)
            conn.commit()
            conn.close()

        

    def unzip_file(self, keep=False):

        new_cif_fn = os.path.join(GetCif.cif_folder, str(self.downloaded_cif).lower()[0:-3])

        with gzip.open(self.tmp_path, 'rb') as f_in:
            with open(new_cif_fn, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        if not keep:
            if os.path.isfile(self.tmp_path):
                os.remove(self.tmp_path)


if __name__ == '__main__':
    
    os.system('clear')
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
