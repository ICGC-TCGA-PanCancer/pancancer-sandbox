#!/usr/bin/env python

import sys
import os
import re
import glob
import xmltodict
import simplejson as json
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


def generate_id_mapping_from_tsv(id_mapping_file, id_mapping, id_mapping_gdc, source_type, target_type):

    logger.info('Processing id_mapping file: {}'.format(id_mapping_file))

    with open(id_mapping_file, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            if row.get('project_code') and not id_mapping.get(row['project_code']): # initialize the non-US projects
                id_mapping[row['project_code']] = {
                    'donor': {},
                    'specimen': {},
                    'sample': {}
                }
                id_mapping_gdc[row['project_code']] = {
                    'donor': {},
                    'specimen': {},
                    'sample': {}
                }
            if row.get('project_code').endswith('-US'): #TCGA projects: use tcga_barcode to find the pcawg_id first
                for id_type in ['donor', 'specimen', 'sample']:
                    if row.get('submitted_' + id_type + '_id') and row.get('icgc_' + id_type + '_id'): 
                        id_mapping_type = id_mapping.get(row['project_code']).get(id_type)
                        id_mapping_gdc_type = id_mapping_gdc.get(row['project_code']).get(id_type)
                        if id_mapping_type and id_mapping_gdc_type:                                
                            if id_mapping_gdc_type.get(row.get('submitted_' + id_type + '_id')) and id_mapping_type.get(id_mapping_gdc_type.get(row.get('submitted_' + id_type + '_id')).get('pcawg')):
                                id_source = id_mapping_gdc_type.get(row.get('submitted_' + id_type + '_id')).get(source_type)
                                id_target = row.get('icgc_' + id_type + '_id')
                                update_id_mapping_with_check_duplicate(id_source, id_target, source_type, target_type, id_mapping, id_type, row.get('project_code'))   
                            
                            else:
                                logger.warning('No PCAWG_id mapped to TCGA_id: {} in project {} '.format(row.get('submitted_' + id_type + '_id'), row.get('project_code')))

                        else:
                            logger.warning('No id_mapping information for {} in project: {} '.format(id_type, row.get('project_code')))

                    else:
                        logger.warning('Not enough information to do the {} id mapping'.format(id_type))

            else:
                for id_type in ['donor', 'specimen', 'sample']: 
                    if row.get('submitted_'+id_type+'_id') and row.get('icgc_'+id_type+'_id'):
                        update_id_mapping_with_check_duplicate(row.get('submitted_'+id_type+'_id'), row.get('icgc_'+id_type+'_id'), source_type, target_type, id_mapping, id_type, row.get('project_code'))
                                        
    

def generate_id_mapping_from_json(id_mapping_file, id_mapping, source_type, target_type):

    logger.info('Processing id_mapping file: {}'.format(id_mapping_file))
    
    fields_map = {
        'pcawg':{
            'donor': 'participant_id',
            'specimen': 'sample_ids',
            'sample': 'aliquot_ids'
        },
        'tcga':{
            'donor': 'submitter_id',
            'specimen': 'submitter_sample_ids',
            'sample': 'submitter_aliquot_ids'
        }
    }
    with open(id_mapping_file, 'r') as f:
        for l in f:
            row = json.loads(l)
            if row.get('project.project_id')[0]:
                project = str.split(row['project.project_id'][0], '-')[1] + '-US'
                if not id_mapping.get(project):
                    id_mapping[project] = {
                        'donor': {},
                        'specimen': {},
                        'sample': {}
                    }
                                
                for id_type in ['donor', 'specimen', 'sample']:
                    if row.get(fields_map.get(source_type).get(id_type)) and row.get(fields_map.get(target_type).get(id_type)):
                        if len(row.get(fields_map.get(source_type).get(id_type))) == len(row.get(fields_map.get(target_type).get(id_type))):
                            for l in range(len(row.get(fields_map.get(source_type).get(id_type)))):
                                id_source = row.get(fields_map.get(source_type).get(id_type))[l].lower() if source_type == 'pcawg' else row.get(fields_map.get(source_type).get(id_type))[l]
                                id_target = row.get(fields_map.get(target_type).get(id_type))[l] if target_type == 'tcga' else row.get(fields_map.get(target_type).get(id_type))[l].lower()
                                update_id_mapping_with_check_duplicate(id_source, id_target, source_type, target_type, id_mapping, id_type, project)
                                
                        else: # sample id mapping length are different
                            logger.warning('Donor: {} has mismatch number of mapping for {} from source to target, in file: {}' \
                                .format(row.get(fields_map.get(source_type).get('donor')[0]), id_type, id_mapping_file))


            else:
                logger.warning('The project_code information is missing in the row: {}, in file: {}'.format(row, id_mapping_file))


def update_id_mapping_with_check_duplicate(id_source, id_target, source_type, target_type, id_mapping, id_type, project):
    if not id_mapping.get(project).get(id_type).get(id_source):
        id_mapping.get(project).get(id_type).update({id_source: {target_type: id_target}})
    elif not id_mapping.get(project).get(id_type).get(id_source).get(target_type):
        id_mapping.get(project).get(id_type).get(id_source).update({target_type: id_target})
    elif id_mapping.get(project).get(id_type).get(id_source).get(target_type) and not id_mapping.get(project).get(id_type).get(id_source).get(target_type) == id_target:
        logger.warning('The {} {}_id: {} has duplicated mapping {}_id: {}, in use is: {}' \
            .format(id_type, source_type, id_source, target_type, id_target, id_mapping.get(project).get(id_type).get(id_source).get(target_type)))



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


def get_id_to_insert( gnos_analysis, analysis_attrib, id_mapping):

    id_to_insert = {}

    field_map = {
        'donor': 'participant',
        'specimen': 'sample',
        'sample': 'aliquot',
        'id': 'barcode'
    }
    
    project = analysis_attrib.get('dcc_project_code')
    if id_mapping.get(project):
        for id_type in ['donor', 'specimen', 'sample']:
            if analysis_attrib.get('submitter_'+id_type+'_id'):
                if project.endswith('-US'): # uuid of tcga submitted to pcawg are case insensitive and transfer to lower case
                    pcawg_id = analysis_attrib.get('submitter_'+id_type+'_id').lower()
                else:
                    pcawg_id = analysis_attrib.get('submitter_'+id_type+'_id')   
                id_to_map = id_mapping.get(project).get(id_type).get(pcawg_id)
                if id_to_map:
                    for k, id_mapped in id_to_map.iteritems():
                        tag = k + '_' + id_type + '_id' if k == 'icgc' else k + '_' + field_map[id_type] + '_' + field_map['id']                    
                        id_in_xml = analysis_attrib.get(tag) # icgc_id and tcga_barcode are case sensitive

                        if not id_in_xml: # No mapping id exist
                            id_to_insert.update({tag: id_mapped})
                        else:
                            if id_in_xml == id_mapped:
                                logger.info( 'Valid xml is already updated: {}: {} has been correctly added for submitter_{}_id:{}, GNOS entry {}'
                                    .format( tag, id_mapped, id_type, analysis_attrib.get('submitter_' + id_type + '_id'), gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')))                            
                            else:
                                logger.warning( 'Invalid updated xml: {}: {} in xml is different with id_mapping for submitter_{}_id:{}, GNOS entry {}'
                                    .format( tag, id_mapped, id_type, analysis_attrib.get('submitter_' + id_type + '_id'), gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')))
                else:                            
                    logger.warning( 'No id mapping info exists of submitter_{}_id: {} for donor: {}::{}'
                        .format( id_type, analysis_attrib.get('submitter_' + id_type + '_id'), project, analysis_attrib.get('submitter_donor_id')))

    else:
        logger.warning( 'Project: {} does not have id_mapping info, GNOS entry: {}'.format(project, gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')))
    
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
                analysis_attrib_element = ET.Element('ANALYSIS_ATTRIBUTE')
                tag = ET.Element('TAG')
                tag.text = k
                analysis_attrib_element.append(tag)
                value = ET.Element('VALUE')
                value.text = v
                analysis_attrib_element.append(value)
                analysis_attribs.append(analysis_attrib_element)

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

        f = conf.get('output_dir') + '/__all_metadata_xml/' + f
         
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
            
            if is_test(analysis_attrib, gnos_analysis):
                logger.warning('ignore test entry: {}'.format(gnos_analysis.get('analysis_detail_uri')))
                continue # completely ignore test gnos entries for now, this is the quickest way to avoid test interferes real data 

            if analysis_attrib.get('dcc_project_code').endswith('-US') and analysis_attrib.get('submitter_donor_id').startswith('TCGA-'):
                logger.warning('ignore TCGA entry submitted with barcode, GNOS entry: {}'
                         .format(gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull') ))
                continue

            if gnos_analysis.get('refassem_short_name') != 'unaligned' and gnos_analysis.get('refassem_short_name') != 'GRCh37':
                logger.warning('ignore entry that is aligned but not aligned to GRCh37: {}'
                         .format(gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull') ))
                continue # completely ignore test gnos entries for now, this is the quickest way to avoid test interferes real data 

            if gnos_analysis.get('library_strategy') == 'RNA-Seq' and not analysis_attrib.get('workflow_name') in ('RNA-Seq_Alignment_SOP_STAR', 'Workflow_Bundle_TOPHAT2'):
                logger.warning('ignore RNA-Seq entry that is not STAR or TOPHAT2 aligned, entry: {}'.format(gnos_analysis.get('analysis_detail_uri')))
                continue

            donor_unique_id = analysis_attrib.get('dcc_project_code') + '::' + analysis_attrib.get('submitter_donor_id')

            if is_in_donor_blacklist(donor_unique_id):
                logger.warning('ignore blacklisted donor: {} GNOS entry: {}'
                                 .format(donor_unique_id, gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull') ))
                continue

            xml_tree = ET.parse(f)
            update_gnos_xml( xml_tree, gnos_analysis, analysis_attrib, output_dir, id_mapping)
            
            # if not os.path.exists(os.path.dirname(f_update)): 
            #     os.makedirs(os.path.dirname(f_update))
            # xml_tree.write(f_update, encoding='utf-8', xml_declaration=True, default_namespace=None, method="xml")

        else:
            logger.warning( 'skipping invalid xml file: {}'.format(f) )  


def is_test(analysis_attrib, gnos_analysis):
    if (gnos_analysis.get('aliquot_id') == '85098796-a2c1-11e3-a743-6c6c38d06053'
          or gnos_analysis.get('study') == 'CGTEST'
          or gnos_analysis.get('study') == 'icgc_pancancer_vcf_test'
          or 'test' in gnos_analysis.get('study').lower()
        ):
        return True
    elif (analysis_attrib.get('dcc_project_code') == 'None-US'
          and analysis_attrib.get('submitter_donor_id') == 'None'
          and analysis_attrib.get('submitter_specimen_id') == 'None'
          and analysis_attrib.get('dcc_specimen_type') == 'unknown'
        ):
        return True
    # TODO: what's the criteria for determining *test* entries

    return False


def is_in_donor_blacklist(donor_unique_id):
    donor_blacklist = set([
            "PACA-CA::PCSI_0449",
            "PACA-CA::PCSI_0309",
            "LIHC-US::G1551",
            "LIHC-US::G15512",
            "TCGA_MUT_BENCHMARK_4::G15511",
            "TCGA_MUT_BENCHMARK_4::G15512",
            "PBCA-DE::SNV_CALLING_TEST"
        ])
    if donor_blacklist.intersection([donor_unique_id]):
        return True
    else:
        return False


def set_default(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


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

    id_mapping = {} # generate the mapping dict between {pcawg_id(key): {'icgc': icgc_id, 'tcga': tcga_id}}
    id_mapping_gdc = {} # generate the mapping dict between tcga_barcode(key) with pcawg_id(value) {tcga_barcode: pcawg_id}
    if not webservice:
        # read the id_mapping file into dict, the sequence for reading files are important.
        generate_id_mapping_from_json('gdc_id_mapping.jsonl', id_mapping, 'pcawg', 'tcga')
        generate_id_mapping_from_json('gdc_id_mapping.jsonl', id_mapping_gdc, 'tcga', 'pcawg')

        generate_id_mapping_from_tsv('pc_id_mapping-icgc.tsv', id_mapping, id_mapping_gdc, 'pcawg', 'icgc')
    
    # write id_mapping to text file for use in parse_gnos_xml.py
    with open('id_mapping_gdc.json', 'w') as f:
        json.dump(id_mapping_gdc, f, indent=4)

    with open('id_mapping.json', 'w') as f:
        json.dump(id_mapping, f, indent=4)


    update_gnos_repo(metadata_dir, conf, repo, exclude_gnos_id_lists, id_mapping)

    return 0


if __name__ == "__main__":
    sys.exit(main())