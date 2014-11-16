#!/usr/bin/env python

# Author: Junjun Zhang

import sys
import os
import glob
import subprocess
import shutil
import xmltodict
import yaml
import requests
import logging
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
import time


logger = logging.getLogger('gnos parser')
# create console handler with a higher log level
ch = logging.StreamHandler()


def initialize_output_dir(output_dir, current_time):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    mani_output_dir = output_dir + '/' + current_time
    if os.path.exists(mani_output_dir):
        shutil.rmtree(mani_output_dir)
    os.makedirs(mani_output_dir)

    return mani_output_dir


def download_manifest(gnos_repo, mani_output_dir, use_previous=False):
    if use_previous:  # set to true only for quick testing
        return 0

    manifest_file = mani_output_dir + '/manifest.' + gnos_repo.get('repo_code') + '.xml'

    download_command = 'cgquery -s ' + gnos_repo.get('base_url'
                          ) + ' --all-states ' + ' -o ' + manifest_file + ' "' + gnos_repo.get('cgquery_para') + '"'

    if subprocess.call(download_command, shell=True) == 0:
        logger.info('downloading manifest succeeded at: {}'.format(gnos_repo.get('repo_code')))
        return manifest_file
    else:
        logger.warning('downloading manifest failed at: {}'.format(gnos_repo.get('repo_code')))
        return False


def use_previous_manifest(gnos_repo, output_dir, mani_output_dir):
    # we can use naming convention to find the directory in which the previous manifest directory was kept
    manifest_dirs = glob.glob(output_dir + '/[0-9]*_*_*[A-Z]') # this is not the safest, but should be good for now
    manifest_file = '/manifest.' + gnos_repo.get('repo_code') + '.xml'

    if len(manifest_dirs) > 1 and os.path.isfile(manifest_dirs[-2] + manifest_file):
        logger.warning('using previously donwloaded manifest for: {}'.format(gnos_repo.get('repo_code')))
        os.symlink((manifest_dirs[-2] + manifest_file).replace(output_dir, '..', 1), mani_output_dir + manifest_file)
        return mani_output_dir + manifest_file
    else:
        logger.warning('no previously donwloaded manifest found, skipping repo: {}'.format(gnos_repo.get('repo_code')))
        return False


def get_ao_from_manifest(manifest_file):
    with open (manifest_file, 'r') as x:
        xml_str = x.read()
    return xmltodict.parse(xml_str).get('ResultSet').get('Result')


def download_metadata_xml(gnos_repo, ao_uuid, metadata_xml_file):
    # let's take a shortcut here getting metadata from local files
    # THIS IS JUST A ONE-TIME THING
    #if (os.path.isfile('xmls_nov_8/all_xml/data_' + ao_uuid + '.xml')):
    #    shutil.copyfile('xmls_nov_8/all_xml/data_' + ao_uuid + '.xml', metadata_xml_file)
    #    return

    logger.info('download metadata xml from GNOS repo: {} for analysis object: {}'.format(gnos_repo.get('repo_code'), ao_uuid))
    
    url = gnos_repo.get('base_url') + '/cghub/metadata/analysisFull/' + ao_uuid
    response = requests.get(url, stream=True)

    if not response.ok:
        logger.warning('unable to download metadata for: {} from {}'.format(ao_uuid, url))
        return
    else:
        with open(metadata_xml_file, 'wb') as fh:
            for block in response.iter_content(1024):
                if not block:
                    break
                fh.write(block)


def sync_metadata_xml(gnos_repo, output_dir, manifest_file):
    logger.info('synchronize metadata xml with GNOS repo: {}'.format(gnos_repo.get('repo_code')))

    metadata_xml_dir = output_dir + '/__all_metadata_xml/' + gnos_repo.get('repo_code')
    if not os.path.exists(metadata_xml_dir):
        os.makedirs(metadata_xml_dir)

    for gnos_ao in get_ao_from_manifest(manifest_file):
        ao_uuid = gnos_ao.get('analysis_id')
        ao_state = gnos_ao.get('state')
        ao_updated = gnos_ao.get('last_modified')
        metadata_xml_file = metadata_xml_dir + '/' + ao_uuid + '__' + ao_state + '__' + ao_updated + '.xml'

        if not os.path.isfile(metadata_xml_file):  # do not have it locally, donwload from GNOS repo
            download_metadata_xml(gnos_repo, ao_uuid, metadata_xml_file)


def process_gnos_repo(gnos_repo, output_dir, mani_output_dir):
    logger.info('processing GNOS repo: {}'.format(gnos_repo.get('repo_code')))

    manifest_file = download_manifest(gnos_repo, mani_output_dir, False)  # only set last param to True for testing
    if not manifest_file:
        manifest_file = use_previous_manifest(gnos_repo, output_dir, mani_output_dir)

    if manifest_file:
        sync_metadata_xml(gnos_repo, output_dir, manifest_file)


def main(argv=None):
    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    current_time = time.strftime("%Y-%m-%d_%H-%M-%S_%Z")

    parser = ArgumentParser(description="PCAWG GNOS Metadata Downloader",
             formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-c", "--config", dest="config",
             help="Configuration file for GNOS repositories", required=True)

    args = parser.parse_args()
    conf_file = args.config

    with open(conf_file) as f:
        conf = yaml.safe_load(f)

    # output_dir
    output_dir = conf.get('output_dir')

    # initialize output directory
    mani_output_dir = initialize_output_dir(output_dir, current_time)

    logger.setLevel(logging.INFO)
    ch.setLevel(logging.WARN)

    log_file = output_dir + '/' + current_time + '.downloader.log'

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)

    for g in conf.get('gnos_repos'):
        process_gnos_repo(g, output_dir, mani_output_dir)

    return 0


if __name__ == "__main__":
    sys.exit(main())


