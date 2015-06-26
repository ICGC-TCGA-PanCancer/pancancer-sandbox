#!/usr/bin/env python

import sys
import os
import re
import glob
import xmltodict
import json
import yaml
import copy
import logging
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from elasticsearch import Elasticsearch
from collections import OrderedDict
import datetime
import dateutil.parser
from itertools import izip
from distutils.version import LooseVersion
from xml.etree import ElementTree as ET
import csv


logger = logging.getLogger('gnos metadata updater')
# create console handler with a higher log level
ch = logging.StreamHandler()

webservice = False

def find_latest_metadata_dir(output_dir):
    dir_pattern = re.compile(u'^[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}-[0-9]{2}-[0-9]{2}_[A-Z]{3}$')
    metadata_dirs = []
    for dir in os.listdir(output_dir):
        if not os.path.isdir(output_dir + '/' + dir):
            continue
        if dir_pattern.search(dir):
            metadata_dirs.append(output_dir + '/' + dir)

    return sorted(metadata_dirs)[-1]

def get_xml_files( metadata_dir, conf, repo ):
    xml_files = []
    #ao_seen = {}
    for r in conf.get('gnos_repos'):
        if repo and not r.get('repo_code') == repo:
            continue
        gnos_ao_list_file = metadata_dir + '/analysis_objects.' + r.get('repo_code') + '.tsv'
        if not os.path.isfile(gnos_ao_list_file):
            logger.warning('gnos analsysi object list file does not exist: {}'.format(gnos_ao_list_file))
            continue
        with open(gnos_ao_list_file, 'r') as list:
            for ao in list:
                ao_uuid, ao_state = str.split(ao, '\t')[0:2]
                if not ao_state == 'live': continue  # skip ao that is not live
                #if (ao_seen.get(ao_uuid)): continue  # skip ao if already added
                #ao_seen[ao_uuid] = 1  # include this one
                xml_files.append(r.get('repo_code') + '/' + ao.replace('\t', '__').replace('\n','') + '.xml')

    return xml_files


def get_gnos_analysis(f):
    with open (f, 'r') as x:
        xml_str = x.read()
    return xmltodict.parse(xml_str).get('ResultSet').get('Result')   


def get_analysis_attrib(gnos_analysis):
    analysis_attrib = {}
    if (not gnos_analysis['analysis_xml']['ANALYSIS_SET'].get('ANALYSIS')
          or not gnos_analysis['analysis_xml']['ANALYSIS_SET']['ANALYSIS'].get('ANALYSIS_ATTRIBUTES')
          or not gnos_analysis['analysis_xml']['ANALYSIS_SET']['ANALYSIS']['ANALYSIS_ATTRIBUTES'].get('ANALYSIS_ATTRIBUTE')
       ):
        return None
    for a in gnos_analysis['analysis_xml']['ANALYSIS_SET']['ANALYSIS']['ANALYSIS_ATTRIBUTES']['ANALYSIS_ATTRIBUTE']:
        if not analysis_attrib.get(a['TAG']):
            analysis_attrib[a['TAG']] = a['VALUE']
        else:
            logger.warning('duplicated analysis attribute key: {}'.format(a['TAG']))
    return analysis_attrib


def read_id_mapping(id_mapping_file, id_mapping):

    id_domain = 'icgc' if 'icgc' in id_mapping_file else 'tcga' 

    with open(id_mapping_file, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        mapping = {}
        for row in reader:
            if not mapping.get(row['project_code']):
                mapping[row['project_code']] = {
                    'donor': {},
                    'specimen': {},
                    'sample': {}
                }
            if row.get('submitted_donor_id') and row.get(id_domain + '_donor_id'):
                mapping.get(row['project_code'])['donor'].update({row['submitted_donor_id']: row[id_domain + '_donor_id']})
            if row.get('submitted_specimen_id') and row.get(id_domain + '_specimen_id'):
                mapping.get(row['project_code'])['specimen'].update({row['submitted_specimen_id']: row[id_domain + '_specimen_id']})
            if row.get('submitted_sample_id') and row.get(id_domain + '_sample_id'):
                mapping.get(row['project_code'])['sample'].update({row['submitted_sample_id']: row[id_domain + '_sample_id']})

    id_mapping[id_domain] = mapping

    return id_mapping


def id_mapping_from_webservice(query_id):
    ## TO be modified
    url = gnos_repo.get('base_url') + '/cghub/metadata/analysisFull/' + query_id
    response = None
    try:
        response = requests.get(url, stream=True, timeout=5)
    except: # download failed, no need to do anything
        pass

    if not response or not response.ok:
        logger.warning('unable to download metadata for: {} from {}'.format(ao_uuid, url))
        return None


def get_id_mapping(gnos_analysis, analysis_attrib, id_domain, id_type, id_mapping):
    project = analysis_attrib.get('dcc_project_code')
    if analysis_attrib.get('submitter_'+id_type+'_id'):
        if webservice:
            id_mapped = id_mapping_from_webservice(analysis_attrib.get('submitter_'+id_type+'_id'))
        else:
            id_mapped = id_mapping.get(id_domain).get(project).get(id_type).get(analysis_attrib.get('submitter_'+id_type+'_id'))
        
        id_in_xml = analysis_attrib.get(id_domain + '_' + id_type + '_id')
        if not id_in_xml:
            if not id_mapped:
                logger.warning( 'No id mapping info exists of submitter_{}_id: {} for donor: {}::{}'
                    .format( id_type, analysis_attrib.get('submitter_' + id_type + '_id'), project, analysis_attrib.get('submitter_donor_id')))
                return None
            else:
                return id_mapped
        
        else:
            if not id_mapped:
                logger.warning( 'Invalid updated xml: No mapping info exists for submitter_{}_id:{}, GNOS entry {}'
                    .format( id_type, analysis_attrib.get('submitter_' + id_type + '_id'), gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')))
            else:
                if id_in_xml == id_mapped:
                    logger.warning( 'Valid xml is already updated: {}_{}_id: {} has been correctly added for submitter_{}_id:{}, GNOS entry {}'
                        .format( id_domain, id_type, id_mapped, id_type, analysis_attrib.get('submitter_' + id_type + '_id'), gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')))
                    return None
                else:
                    logger.warning( 'Invalid updated xml: {}_{}_id: {} in xml is different with id_mapping for submitter_{}_id:{}, GNOS entry {}'
                        .format( id_domain, id_type, id_mapped, id_type, analysis_attrib.get('submitter_' + id_type + '_id'), gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')))
                    return None
    else:
        logger.warning( 'The entry does not have submitter_{}_id information for donor: {}::{}, GNOS entry {}'
                        .format( id_type, project, analysis_attrib.get('submitter_donor_id'), gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')))
        return None


def get_id_to_insert( gnos_analysis, analysis_attrib, id_mapping):

    id_to_insert = {}
    
    for id_domain, v_domain in id_mapping.iteritems():
        if v_domain.get(analysis_attrib.get('dcc_project_code')):
            for id_type, v_type in v_domain.get(analysis_attrib.get('dcc_project_code')).iteritems():
                if analysis_attrib.get('variant_workflow_name') and not id_type == 'donor': # vcf entry only have donor ids for mapping 
                    continue 
                id_mapped = get_id_mapping(gnos_analysis, analysis_attrib, id_domain, id_type, id_mapping)  
                if id_mapped is not None:
                    id_to_insert.update({id_domain + '_' + id_type + '_id': id_mapped})
        else:
            logger.warning( 'Project: {} does not have id_mapping info ...'
                    .format( analysis_attrib.get('dcc_project_code')))
    
    return id_to_insert


def write_to_xml(fname, element):      

    if not os.path.exists(os.path.dirname(fname)):
        os.makedirs(os.path.dirname(fname))
    with open(fname, "w") as f:
        f.write(ET.tostring(element))


def update_gnos_xml(xml_tree, gnos_analysis, analysis_attrib, output_dir, id_mapping):

    id_to_insert = get_id_to_insert(gnos_analysis, analysis_attrib, id_mapping)
    if not id_to_insert:
        logger.warning( 'No update id fields info are ready for donor: {}::{}, GNOS entry {}'
                        .format( analysis_attrib.get('dcc_project_code'), analysis_attrib.get('submitter_donor_id'), gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')))

    else:
        if xml_tree.find('Result/analysis_xml/ANALYSIS_SET') is not None:
            analysis_xml = xml_tree.find('Result/analysis_xml/ANALYSIS_SET')
            analysis_attribs = analysis_xml.find('ANALYSIS/ANALYSIS_ATTRIBUTES')
            for k,v in id_to_insert.iteritems():
                analysis_attrib = ET.Element('ANALYSIS_ATTRIBUTE')
                tag = ET.Element('TAG')
                tag.text = k
                analysis_attrib.append(tag)
                value = ET.Element('VALUE')
                value.text = v
                analysis_attrib.append(value)
                analysis_attribs.append(analysis_attrib)

            # write the xml metadata files 
            fname = output_dir + 'analysis.xml'
            write_to_xml(fname, analysis_xml)
        

        if xml_tree.find('Result/experiment_xml/EXPERIMENT_SET') is not None:
            experiment_xml = xml_tree.find('Result/experiment_xml/EXPERIMENT_SET')     
            fname = output_dir + 'experiment.xml'
            write_to_xml(fname, experiment_xml)
        else:
            logger.warning( 'The entry does not have experiment_xml content for donor: {}::{}, GNOS entry {}'
                        .format( analysis_attrib.get('dcc_project_code'), analysis_attrib.get('submitter_donor_id'), gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')))


        if xml_tree.find('Result/run_xml/RUN_SET') is not None:
            fname = output_dir + 'run.xml'
            run_xml = xml_tree.find('Result/run_xml/RUN_SET')
            write_to_xml(fname, run_xml)
        else:
            logger.warning( 'The entry does not have run_xml content for donor: {}::{}, GNOS entry {}'
                        .format( analysis_attrib.get('dcc_project_code'), analysis_attrib.get('submitter_donor_id'), gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')))


def update_gnos_repo(metadata_dir, conf, repo, exclude_gnos_id_lists, id_mapping):

    # pre-exclude gnos entries when this option is chosen
    gnos_ids_to_be_excluded = set()
    if exclude_gnos_id_lists:
        files = glob.glob(exclude_gnos_id_lists)
        for fname in files:
            with open(fname) as f:
                for d in f: gnos_ids_to_be_excluded.add(d.rstrip())

    
    for f in get_xml_files( metadata_dir, conf, repo ):
        output_dir = metadata_dir.rstrip('/') + '_updated/' + str.split(f, '__')[0] + '/'
        f_update = conf.get('output_dir') + '/__all_update_metadata_xml/' + f

        f = conf.get('output_dir') + '/__all_update_metadata_xml/' + f
         
        gnos_analysis = get_gnos_analysis(f)

        #print (json.dumps(gnos_analysis)) # debug
        if gnos_analysis:
            logger.info( 'updating xml file: {} ...'.format(f) )

            analysis_attrib = get_analysis_attrib(gnos_analysis)
            if gnos_analysis.get('analysis_id') and gnos_analysis.get('analysis_id') in gnos_ids_to_be_excluded:
                logger.warning( 'skipping xml file: {} with analysis_id: {}, as it\'s in the list to be excluded' \
                    .format(f, gnos_analysis.get('analysis_id')) )
                continue

            if gnos_analysis.get('dcc_project_code') and gnos_analysis.get('dcc_project_code').upper() == 'TEST':
                logger.warning('ignore entry with dcc_project_code being TEST, GNOS entry: {}'
                         .format(gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull') ))
                continue

            if not analysis_attrib:
                logger.warning('ignore entry does not have ANALYSIS information, GNOS entry: {}'
                         .format(gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull') ))
                continue

            if not analysis_attrib.get('dcc_project_code') or not analysis_attrib.get('submitter_donor_id'):
                logger.warning('ignore entry does not have dcc_project_code or submitter_donor_id, GNOS entry: {}'
                         .format(gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull') ))
                continue


            xml_tree = ET.parse(f)
            update_gnos_xml( xml_tree, gnos_analysis, analysis_attrib, output_dir, id_mapping)
            
            # if not os.path.exists(os.path.dirname(f_update)): 
            #     os.makedirs(os.path.dirname(f_update))
            # xml_tree.write(f_update, encoding='utf-8', xml_declaration=True, default_namespace=None, method="xml")

        else:
            logger.warning( 'skipping invalid xml file: {}'.format(f) )  


def main(argv=None):
    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    parser = ArgumentParser(description="PCAWG GNOS Metadata Updater",
             formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-c", "--config", dest="config",
             help="Configuration file for GNOS repositories", required=True)
    parser.add_argument("-m", "--metadata_dir", dest="metadata_dir",
             help="Directory containing metadata manifest files", required=False)
    parser.add_argument("-r", "--gnos_repo", dest="repo",
             help="Specify which GNOS repo to process, process all repos if none specified", required=False)
    parser.add_argument("-x", "--exclude_gnos_id_lists", dest="exclude_gnos_id_lists", # don't use this option for daily cron job
             help="File(s) containing GNOS IDs to be excluded, use filename pattern to specify the file(s)", required=False)


    args = parser.parse_args()
    metadata_dir = args.metadata_dir
    conf_file = args.config
    repo = args.repo
    exclude_gnos_id_lists = args.exclude_gnos_id_lists

    with open(conf_file) as f:
        conf = yaml.safe_load(f)
        for r in conf.get('gnos_repos'):
            conf[r.get('base_url')] = r.get('repo_code')

    # output_dir
    output_dir = conf.get('output_dir')
    if metadata_dir:
        if not os.path.isdir(metadata_dir):  # TODO: should add more directory name check to make sure it's right
            sys.exit('Error: specified metadata directory does not exist!')
    else:
        metadata_dir = find_latest_metadata_dir(output_dir)  # sorted(glob.glob(output_dir + '/[0-9]*_*_*[A-Z]'))[-1] # find the directory for latest metadata list
    timestamp = str.split(metadata_dir, '/')[-1]

    logger.setLevel(logging.INFO)
    ch.setLevel(logging.WARN)

    log_file = metadata_dir + '.metadata_updater' + ('' if not repo else '.'+repo) + '.log'
    # delete old log first if exists
    if os.path.isfile(log_file): os.remove(log_file)

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)

    id_mapping = {}
    if not webservice:
        # read the id_mapping file into a dict 
        read_id_mapping('pc_id_mapping-icgc.tsv', id_mapping) 

    update_gnos_repo(metadata_dir, conf, repo, exclude_gnos_id_lists, id_mapping)

    return 0


if __name__ == "__main__":
    sys.exit(main())