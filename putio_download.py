#!/usr/bin/env python

"""
put.io download script


This script will start downloads on put.io, then download it locally.
ONE DOWNLOAD AT A TIME, and It will delete file on put.io after
successfully downloading it. This way, you don't need much space on put.io.

It is used as an execution script in flexget.
"""


########################################################
#
# Changeable variables:
#
########################################################

TOKEN = 'yourtokenhere'
MQTT_SERVER = 'localhost'

WORK_DIR = '/tmp/put.io'
SHOW_DIR = '/tmp/download/serier'
MOVIE_DIR = '/tmp/download/filmer'
GENERIC_DIR = '/tmp/download'
MUSIC_DIR = '/mnt/download/disk/downloads/musikk'

LOG_FILE = '/tmp/put.io/putio.log'


########################################################

import putio

import sys
import argparse
import os
import pickle
from time import sleep, time
import logging
import subprocess
import zipfile
import datetime
import paho.mqtt.publish as publish


logger = logging.getLogger(__name__)


def find_pickle(directory):
    files = os.listdir(directory)
    for f in sorted(files):
        if f.endswith('.putio.pickle'):
            return os.path.join(directory, f)
    return None


def unzip(file, target):
    try:
        zf = zipfile.ZipFile(file)
        for member in zf.infolist():
            zf.extract(member, target)
            newfile = os.path.join(target, member.filename)
            os.chmod(newfile, 0o664)
    except Exception as e:
        logger.error('unzip exception:' + str(e))
        return False
    return True

def progress_callback(info):
    publish.single('myflexget/download/progress', str(info['string']), hostname=MQTT_SERVER)

def main():
    pid = str(os.getpid())
    pidfile = WORK_DIR + '/putio_flexget.pid'

    try:
        fd = os.open(pidfile, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    except OSError:
        fd = 0

    if not fd:
        print('%s already exists. Exiting' % pidfile)
        logger.warning('%s already exists. Exiting' % pidfile)
        return False
    else:
        client = putio.Client(TOKEN)
        if not client.check_token():
            logger.error('Invalid token. Exiting')
            os.unlink(pidfile)
            return False
        os.write(fd, pid)

    #Go through all the downloads:
    while(True):
        logger.info('#' * 80)
        pickle_file = find_pickle(WORK_DIR)
        if pickle_file is None:
            logger.info('No *.putio.pickle file in %s' % WORK_DIR)
            break

        logger.info('Loading pickle file: %s' % pickle_file)
        torrent = pickle.load(open(pickle_file, 'rb'))

        downloadName = torrent['torrent']

        #upload torrent to put.io
        if 'show' in torrent:
            s = 'Starting show: "%s season %s episode %s" on put.io' % (torrent['show'], torrent['season'], torrent['episode'])
            downloadName = '%s (s%se%s)' % (torrent['show'], torrent['season'], torrent['episode'])
        elif 'movie' in torrent:
            s = 'Starting movie: "%s" on put.io' % (torrent['movie'])
            downloadName = torrent['movie']
        elif 'music' in torrent:
            s = 'Starting music: "%s" on put.io' % (torrent['torrent'])
        else:
            s = 'Starting generic: %s on put.io' % (torrent['torrent'])

        logger.info(s)

        if 'mqtt' in torrent:
            logger.info('MQTT notification ON')
            publish.single('myflexget/download', str(s), hostname=MQTT_SERVER)

        currenttime = datetime.datetime.now()
        logger.info('Datetime: %s' % currenttime)
        transfer = client.Transfer.add_url(torrent['torrent'])

        if not transfer:
            if not 'transfer_failed' in torrent:
                s = 'Failed to add torrent to put.io, will retry!'
                logger.error(s)
                if 'mqtt' in torrent:
                    publish.single('myflexget/download', s, hostname=MQTT_SERVER)

                torrent['transfer_failed'] = True
                pickle.dump(torrent, open(pickle_file, "wb"))
                sleep(15)
                continue
            else:
                s = 'Failed to add torrent to put.io second time, giving up!'
                logger.error(s)
                if 'mqtt' in torrent:
                    publish.single('myflexget/download', s, hostname=MQTT_SERVER)

                logger.info('Disabling pickle file: "%s"' % pickle_file)
                ret = os.rename(pickle_file, pickle_file+'.transfer_failed')
                if not ret:
                    logger.info('Deleting pickle file: "%s"' % pickle_file)
                    os.unlink(pickle_file)
                continue

        #Give put.io some time, then check on it
        sleep(5)

        #Wait until downloaded on put.io:
        while(True):
            transfer = client.Transfer.get(transfer['id'])
            if not transfer:
                logger.warning('Did not find transfer. Continuing')
                break
            if transfer['status'] in ('COMPLETED', 'SEEDING'):
                logger.info('Torrent is completed')
                logger.info('Download time on put.io: %s' % (datetime.datetime.now() - currenttime))
                currenttime = datetime.datetime.now()
                break
            elif transfer['status'] in ('ERROR'):
                logger.warning('Transfer failed')
                if 'status_message' in transfer:
                    logger.warning(transfer['status_message'])
                client.Transfer.cancel(transfer['id'])
                logger.info('Will retry transfer')
                break

            s = 'Torrent is : %s - %s/%s' % (transfer['status'], transfer['percent_done'], transfer['availability'])
            logger.info(s)
            if 'mqtt' in torrent:
                publish.single('myflexget/download/progress', str(s), hostname=MQTT_SERVER)
            sleep(60)

        #If we did not find transfer, continue main loop
        if not transfer:
            continue

        #Download locally from put.io:
        logger.info('Downloading zipped file "%s" to: %s' % (transfer['name'], WORK_DIR))
        filename = client.File.download_zip(transfer['file_id'], dest=WORK_DIR, progress_callback=progress_callback)
        if not filename:
            if 'download_failed' in torrent:
                s = 'Download failed second time, giving up!'
                logger.error(s)
                pickle.dump(torrent, open(pickle_file+'.putio.pickle.download_failed', "wb"))
                if 'mqtt' in torrent:
                    publish.single('myflexget/download', s, hostname=MQTT_SERVER)
            else:
                logger.warning('Download failed! Will try again')
                torrent['download_failed'] = True
                #dump failed download as a new pickle
                pickle.dump(torrent, open(pickle_file+'.SECOND_TRY.putio.pickle', "wb"))
        else:
            logger.info('Download done')

            #Set up target folder
            if 'show' in torrent:
                download_folder = os.path.join(SHOW_DIR, torrent['show'])
                if not os.path.isdir(download_folder):
                    logger.info('Creating directory for new show: "%s"' % download_folder)
                    os.makedirs(download_folder)
            elif 'movie' in torrent:
                download_folder = MOVIE_DIR
            elif 'music' in torrent:
                download_folder = MUSIC_DIR
            else:
                download_folder = GENERIC_DIR

            #Extract zip file
            if os.path.isdir(download_folder) and os.access(download_folder, os.W_OK):
                show_zip = os.path.join(WORK_DIR, filename)
                logger.info('Extracting file: "%s" to "%s"' % (show_zip, download_folder))

                if unzip(show_zip, download_folder):
                    logger.info('Done extracting, removing zip file: "%s"' % (show_zip))
                    os.unlink(show_zip)
                    if 'mqtt' in torrent:
                        s = str('Download done: %s' % (transfer['name']))
                        publish.single('myflexget/download', s, hostname=MQTT_SERVER)
                else:
                    s = 'Extracting failed, check %s for partially extracted files' % (download_folder)
                    logger.error(s)
                    logger.error('zip folder was NOT deleted: %s' % show_zip)

                    if 'mqtt' in torrent:
                        publish.single('myflexget/download', s, hostname=MQTT_SERVER)
            else:
                s = 'No access to "%s", file will not be extracted' % (download_folder)
                logger.warning(s)
                if 'mqtt' in torrent:
                    publish.single('myflexget/download', s, hostname=MQTT_SERVER)


        #Always delete file on put.io, even on failed downloads.
        logger.info('Deleting file on put.io with id: %s' % transfer['file_id'])
        client.File.delete(transfer['file_id'])

        if os.path.isfile(pickle_file):
            #Delete old pickle file
            logger.info('Deleting pickle file: "%s"' % pickle_file)
            os.unlink(pickle_file)

    logger.info('No more files to start, exiting')
    os.unlink(pidfile)
    return True


def pickle_dump():
    parser = argparse.ArgumentParser(description='Download torrent/magnet using put.io')
    parser.add_argument('url', help='URL to torrent or magnet')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-m', '--moviename', help='Name of movie, triggers download to movie folder')
    group.add_argument('-t', '--showname', help='Name of the TV show, used to match on myepisodes')

    parser.add_argument('-s', '--season', help='TV show season', type=int)
    parser.add_argument('-e', '--episode', help='TV show episode', type=int)
    parser.add_argument('-q', '--mqtt', help='Send mqtt on done', action='store_true')
    parser.add_argument('-M', '--music', help='Download to music folder', action='store_true')

    args = parser.parse_args()

    if args.showname and args.season and args.episode:
        download = {
            'torrent': args.url,
            'show': args.showname,
            'season': args.season,
            'episode': args.episode
        }
    #If it is a movie
    elif args.moviename:
        download = {
            'torrent': args.url,
            'movie': args.moviename,
        }
    #If it is a music
    elif args.music:
        download = {
            'torrent': args.url,
            'music': True
        }
    #If it is a generic download
    else:
        download = {
            'torrent': args.url
        }

    if args.mqtt:
         download['mqtt'] = True

    #logger.debug('Dumping pickle: %s to folder: %s' % (download, WORK_DIR))
    if 'show' in download:
        pickle_file = "%s.%sx%s" % ("".join(x for x in download['show'] if x.isalnum()), download['season'], download['episode'])
    elif 'movie' in download and download['movie'] != 'auto':
        pickle_file = "%s" % ("".join(x for x in download['movie'] if x.isalnum()))
    else:
        # Magnet title:
        start = sys.argv[1].find('dn=')
        if start > 0:
            end = sys.argv[1].find('&', start)
            if end > 0:
                file_name = sys.argv[1][start + 3:end]
            else:
                file_name = sys.argv[1][start + 3:]
        else:
            # Torrent title:
            start = sys.argv[1].find('?title=')
            if start > 0:
                end = sys.argv[1].find('&')
                if end > 0:
                    file_name = sys.argv[1][start + 7:end]
                else:
                    file_name = sys.argv[1][start + 7:]
            else:
                # Generic name:
                file_name = sys.argv[1]

        pickle_file = "".join(x for x in file_name if x.isalnum())  # safe filename
        if len(pickle_file) > 150:  # Not too long
            pickle_file = pickle_file[:150]

    if 'movie' in download and download['movie'] == 'auto':
        download['movie'] = pickle_file


    timestamp = str(int(time()))
    pickle_file = timestamp + '.' + pickle_file

    pickle.dump(download, open('%s.putio.pickle' % os.path.join(WORK_DIR, pickle_file), "wb"))

    #Launch self as new process without parameter, so flexget stop waiting.
    my_file_name = os.path.abspath(__file__)
    output = open(LOG_FILE, 'a')
    logger.debug('Starting myself as subprocess: %s' % my_file_name)
    subprocess.Popen(my_file_name, stdout=output, stderr=output, close_fds=True)
    output.close()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        pickle_dump()
    else:
        if LOG_FILE:
            logging.basicConfig(filename=LOG_FILE)
            logging.getLogger('putio').setLevel(logging.INFO)
            logging.getLogger(__name__).setLevel(logging.INFO)
        main()
