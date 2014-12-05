#!/usr/bin/env python

# Author: Junjun Zhang

import sys
import os
import re
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


def find_second_last_metadata_dir(output_dir):
    dir_pattern = re.compile(u'^[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}-[0-9]{2}-[0-9]{2}_[A-Z]{3}$')
    metadata_dirs = []
    for dir in os.listdir(output_dir):
        if not os.path.isdir(output_dir + '/' + dir):
            continue
        if dir_pattern.search(dir):
            metadata_dirs.append(output_dir + '/' + dir)

    return False if len(metadata_dirs) < 2 else sorted(metadata_dirs)[-2]


def use_previous_manifest(gnos_repo, output_dir, mani_output_dir):
    # we can use naming convention to find the directory in which the previous manifest directory was kept
    manifest_file = '/manifest.' + gnos_repo.get('repo_code') + '.xml'

    previous_metadata_dir = find_second_last_metadata_dir(output_dir)
    previous_manifest_file = (previous_metadata_dir + manifest_file).replace(output_dir, '..', 1)

    if previous_metadata_dir and os.path.isfile(previous_manifest_file):
        logger.warning('using previously donwloaded manifest for: {}'.format(gnos_repo.get('repo_code')))
        os.symlink(previous_manifest_file, mani_output_dir + manifest_file)
        return mani_output_dir + manifest_file
    else:
        logger.warning('no previously donwloaded manifest found, skipping repo: {}'.format(gnos_repo.get('repo_code')))
        return False


def get_ao_from_manifest(manifest_file):
    with open (manifest_file, 'r') as x:
        xml_str = x.read()
    return xmltodict.parse(xml_str).get('ResultSet').get('Result')

def is_blacklisted_ao_uuid(ao_uuid):
    blacklist = ["CF6A2220-8D48-11E3-884A-AA2401209DA7"]
    return ao_uuid in blacklist


def download_metadata_xml(gnos_repo, ao_uuid, metadata_xml_dir, ao_list_file_handler):

    if is_blacklisted_ao_uuid(ao_uuid):
        logger.warning('skip blacklisted item for: {} from {}'.format(ao_uuid, gnos_repo.get('base_url')))
        return

    logger.info('download metadata xml from GNOS repo: {} for analysis object: {}'.format(gnos_repo.get('repo_code'), ao_uuid))
    
    url = gnos_repo.get('base_url') + '/cghub/metadata/analysisFull/' + ao_uuid
    response = requests.get(url, stream=True)

    if not response.ok:
        logger.warning('unable to download metadata for: {} from {}'.format(ao_uuid, url))
        return
    else:
        metadata_xml_str = response.text
        gnos_ao = xmltodict.parse(metadata_xml_str).get('ResultSet').get('Result')
        ao_uuid = gnos_ao.get('analysis_id')
        ao_state = gnos_ao.get('state')
        ao_updated = gnos_ao.get('last_modified')
        ao_list_file_handler.write(ao_uuid + '\t' + ao_state + '\t' + ao_updated + '\n')

        metadata_xml_file = metadata_xml_dir + '/' + ao_uuid + '__' + ao_state + '__' + ao_updated + '.xml'
        with open(metadata_xml_file, 'w') as f:  # write to metadata xml file now
            f.write(metadata_xml_str)


def sync_metadata_xml(gnos_repo, output_dir, manifest_file):
    logger.info('synchronize metadata xml with GNOS repo: {}'.format(gnos_repo.get('repo_code')))

    metadata_xml_dir = output_dir + '/__all_metadata_xml/' + gnos_repo.get('repo_code')
    if not os.path.exists(metadata_xml_dir):
        os.makedirs(metadata_xml_dir)

    ao_list_file = manifest_file.replace('manifest.', 'analysis_objects.').replace('.xml', '.tsv')
    fh = open(ao_list_file, 'w')  # file for list of gnos analysis objects

    for gnos_ao in get_ao_from_manifest(manifest_file):
        ao_uuid = gnos_ao.get('analysis_id')
        ao_state = gnos_ao.get('state')
        ao_updated = gnos_ao.get('last_modified')
        metadata_xml_file = metadata_xml_dir + '/' + ao_uuid + '__' + ao_state + '__' + ao_updated + '.xml'

        if os.path.isfile(metadata_xml_file):
            fh.write(ao_uuid + '\t' + ao_state + '\t' + ao_updated + '\n')
        else:  # do not have it locally, donwload from GNOS repo
            download_metadata_xml(gnos_repo, ao_uuid, metadata_xml_dir, fh)

    fh.close()


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


