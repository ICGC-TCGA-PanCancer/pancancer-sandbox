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




es_queries = [
  # query 0: PCAWG_full_list_donors 
    {
     "fields": "donor_unique_id", 
 
    "filter": {
                    "bool": {
                      "must": [
                        {
                          "type": {
                            "value": "donor"
                          }
                        }                   
                      ],
                      "must_not": [
                        {
                          "terms": {
                            "flags.is_manual_qc_failed": [
                              "T"
                            ]
                          }
                        },
                        {
                          "terms": {
                            "flags.is_donor_blacklisted": [
                              "T"
                            ]
                          }
                        }
                      ]
                    }
                },
      "size": 10000
    }
]

def init_report_dir(metadata_dir, report_name):
    report_dir = metadata_dir + '/reports/' + report_name
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)
    return report_dir

def create_specimen_info(donor_unique_id, es_json, compute_sites):
    specimen_info_list = []

    specimen_info = OrderedDict()
    specimen_info['donor_unique_id'] = donor_unique_id
    specimen_info['submitter_donor_id'] = es_json['submitter_donor_id']
    specimen_info['dcc_project_code'] = es_json['dcc_project_code']
    
    add_wgs_specimens(specimen_info_list, specimen_info, es_json, compute_sites)

    #comment it for now not considering the rna-seq alignments 
    #add_rna_seq_specimens(specimen_info_list, specimen_info, es_json)

    return specimen_info_list


def add_wgs_specimens(specimen_info_list, specimen_info, es_json, compute_sites):

    if es_json.get('normal_alignment_status'):
        aliquot = es_json.get('normal_alignment_status')
        get_wgs_aliquot_fields(aliquot, specimen_info, compute_sites, specimen_info_list)

    if es_json.get('tumor_alignment_status'):
        for aliquot in es_json.get('tumor_alignment_status'):
            get_wgs_aliquot_fields(aliquot, specimen_info, compute_sites, specimen_info_list)

    return specimen_info_list

def get_wgs_aliquot_fields(aliquot, specimen_info, compute_sites, specimen_info_list):
    specimen_info['aliquot_id'] = aliquot.get('aliquot_id')
    specimen_info['submitter_specimen_id'] = aliquot.get('submitter_specimen_id')
    specimen_info['submitter_sample_id'] = aliquot.get('submitter_sample_id')
    specimen_info['dcc_specimen_type'] = aliquot.get('dcc_specimen_type')
    specimen_info['library_strategy'] = 'WGS'
    specimen_info['aligned'] = aliquot.get('aligned')
    specimen_info['workflow_type'] = 'BWA'
    specimen_info['has_bam_been_transferred'] = aliquot.get('has_bwa_bam_been_transferred')
    specimen_info['bam_gnos_id'] = aliquot.get('aligned_bam').get('gnos_id') if specimen_info['aligned'] else None
    specimen_info['computer_site'] = []    
    if get_compute_site(specimen_info['donor_unique_id'], compute_sites):
        specimen_info['computer_site'] = get_compute_site(specimen_info['donor_unique_id'], compute_sites)
    #leave the compute site to be blank if the donor is not in the whitelist whether it is aligned or not
    #since we do not have the history info for the aligned ones
    #else:
    #    specimen_info['computer_site'] = [get_formal_repo_name(aliquot.get('aligned_bam').get('gnos_repo')[0])] if specimen_info['aligned'] else []

    specimen_info_list.append(copy.deepcopy(specimen_info))
    return specimen_info_list


def compute_site_report(metadata_dir, report_dir, report_name, site_counts, unassigned_unaligned_donors):

    # today's counts
    with open(report_dir + '/summary_site_counts.json', 'w') as o: o.write(json.dumps(site_counts))

    with open(report_dir + '/unassigned_unaligned_donors.txt', 'w') as o: 
        o.write('# Unassigned and unaligned donors\n')
        o.write('# dcc_project_code' + '\t' + 'submitter_donor_id' + '\n')
        o.write('\n'.join(unassigned_unaligned_donors) + '\n')

    # get all previous days counts
    [dates, metadata_dirs] = get_metadata_dirs(metadata_dir, '2015-05-26')

    site_summary_report = []
    for i, md in reversed(list(enumerate(metadata_dirs))):
        summary_site_count_file = md + '/reports/' + report_name + '/summary_site_counts.json'
        if not os.path.isfile(summary_site_count_file): continue

        site_counts_tmp = json.load(open(summary_site_count_file))
        site_summary_report.append([dates[i], site_counts_tmp])
    with open(report_dir + '/hist_summary_site_counts.json', 'w') as o: o.write(json.dumps(site_summary_report))


def get_compute_site(donor_unique_id, compute_sites):
    compute_site = []

    for c in compute_sites:
        for d in compute_sites:
            if c == d: continue
            if compute_sites.get(c).intersection(compute_sites.get(d)):
                # log overlap donors issue
                print "WARN: overlap donors found between " + c + " and " + d \
                      + ": " + ", ".join(compute_sites.get(c).intersection(compute_sites.get(d)))
        if donor_unique_id.replace('::', '\t') in compute_sites.get(c):
            compute_site.append(c)
    
    return(compute_site)

    
def get_whitelists(compute_sites):
    whitelist_dir = '../pcawg-operations/bwa_alignment/'

    for c in compute_sites:
        files = glob.glob(whitelist_dir + '/' + c + '/' + c + '.*.txt')
        for f in files: compute_sites.get(c).update(get_donors(f))


def get_donors(fname):
    donors = []
    with open(fname) as f:
        for d in f:
            if d.rstrip(): donors.append(d.rstrip())
    return donors


def get_formal_repo_name(repo):
    repo_url_to_repo = {
      "https://gtrepo-bsc.annailabs.com/": "bsc",
      "bsc": "bsc",
      "https://gtrepo-ebi.annailabs.com/": "ebi",
      "ebi": "ebi",
      "https://cghub.ucsc.edu/": "cghub",
      "cghub": "cghub",
      "https://gtrepo-dkfz.annailabs.com/": "dkfz",
      "dkfz": "dkfz",
      "https://gtrepo-riken.annailabs.com/": "riken",
      "riken": "riken",
      "https://gtrepo-osdc-icgc.annailabs.com/": "osdc-icgc",
      "osdc-icgc": "osdc-icgc",
      "https://gtrepo-osdc-tcga.annailabs.com/": "osdc-tcga",
      "osdc-tcga": "osdc-tcga",
      "https://gtrepo-etri.annailabs.com/": "etri",
      "etri": "etri"
    }

    return repo_url_to_repo.get(repo)


def filter_liri_jp(project, gnos_repo):
    if not project == 'LIRI-JP':
        return gnos_repo
    elif "https://gtrepo-riken.annailabs.com/" in gnos_repo:
        return ["https://gtrepo-riken.annailabs.com/"]
    else:
        print "This should never happen: alignment for LIRI-JP is not available at Riken repo"
        sys.exit(1)


def add_rna_seq_specimens(specimen_info_list, specimen_info, es_json):
    # to build pcawg santa cruz pilot dataset, this is a temporary walkaround to exclude the 130 RNA-Seq bad
    # entries from MALY-DE and CLLE-ES projects
    #if es_json.get('dcc_project_code') in ('MALY-DE', 'CLLE-ES'): return

    rna_seq_info = es_json.get('rna_seq').get('alignment')
    for specimen_type in rna_seq_info.keys():
        if not rna_seq_info.get(specimen_type): # the specimen_type has no alignment result
		    continue
        if 'normal' in specimen_type:
            aliquot = rna_seq_info.get(specimen_type)
            get_rna_seq_aliquot_fields(aliquot, specimen_info, specimen_info_list)

        else:
            for aliquot in rna_seq_info.get(specimen_type):
                get_rna_seq_aliquot_fields(aliquot, specimen_info, specimen_info_list)

    return specimen_info_list

def get_rna_seq_aliquot_fields(aliquot, specimen_info, specimen_info_list):
    specimen_info['aliquot_id'] = set()
    specimen_info['submitter_specimen_id'] = set()
    specimen_info['submitter_sample_id'] = set()
    specimen_info['dcc_specimen_type'] = set()
    specimen_info['library_strategy'] = 'RNA-Seq'
    specimen_info['aligned'] = True
    for workflow_type in aliquot.keys():
        specimen_info['aliquot_id'].add(aliquot.get(workflow_type).get('aliquot_id'))
        specimen_info['submitter_specimen_id'].add(aliquot.get(workflow_type).get('submitter_specimen_id'))
        specimen_info['submitter_sample_id'].add(aliquot.get(workflow_type).get('submitter_sample_id'))
        specimen_info['dcc_specimen_type'].add(aliquot.get(workflow_type).get('dcc_specimen_type'))
        specimen_info['workflow_type'] = workflow_type.upper()
        specimen_info['has_bam_been_transferred'] = False
        #for later possible use
        #specimen_info['has_bam_been_transferred'] = aliquot.get(workflow_type).get('has_' + workflow_type.lower() + '_bam_been_transferred')       
        specimen_info['bam_gnos_id'] = aliquot.get(workflow_type).get('gnos_info').get('gnos_id')
        specimen_info['computer_site'] = []
        specimen_info_list.append(copy.deepcopy(specimen_info))

def get_donor_json(es, es_index, donor_unique_id):
    es_query_donor = {
        "filter": {
            "term": {
                "donor_unique_id": donor_unique_id
            }
        }
    }
    response = es.search(index=es_index, body=es_query_donor)

    es_json = response['hits']['hits'][0]['_source']
 
    return es_json


def get_donors_list(es, es_index, es_queries):
    q_index = 0
    response = es.search(index=es_index, body=es_queries[q_index])
    
    donors_list = []
    for p in response['hits']['hits']:
    	donors_list.append(p.get('fields').get('donor_unique_id')[0])

    return donors_list 


def set_default(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, set):
        return list(obj)
    raise TypeError

def compute_site_count(specimen, site_counts, unassigned_unaligned_donors):
    c = specimen.get('computer_site')
    if len(c) == 1:
        c = c[0]
        if not site_counts.get(c):
            site_counts[c] = {
                'total': 0,
                'aligned': 0,
                'unaligned': 0
            }
        site_counts[c]['total'] += 1
        if specimen.get('aligned'):
            site_counts[c]['aligned'] += 1 
        else:
            site_counts[c]['unaligned'] += 1
    elif not c:
        site_counts['unassigned']['total'] +=1
        if specimen.get('aligned'):
            site_counts['unassigned']['aligned'] += 1 
        else:
            site_counts['unassigned']['unaligned'] += 1
            unassigned_unaligned_donors.add(specimen.get('donor_unique_id').replace('::', '\t'))        
        

    
def get_metadata_dirs(metadata_dir, start_date='2015-01-11'):
    dirs = sorted(glob.glob(metadata_dir + '/../20*_???'))
    dir_name = os.path.basename(metadata_dir)
    ret_dirs = []
    ret_dates = []
    start = False
    for d in dirs:
        if '../' + start_date in d: start = True
        if not start: continue
        ret_dates.append( str.split(os.path.basename(d),'_')[0] )
        ret_dirs.append(d)
        if dir_name == os.path.basename(d): break

    return [ret_dates, ret_dirs]

def main(argv=None):

    parser = ArgumentParser(description="PCAWG Full List of Specimen Level Alignment Info Generator",
             formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-m", "--metadata_dir", dest="metadata_dir",
             help="Directory containing metadata manifest files", required=True)
    parser.add_argument("-r", "--gnos_repo", dest="repo",
             help="Specify which GNOS repo to process, process all repos if none specified", required=False)

    args = parser.parse_args()
    metadata_dir = args.metadata_dir  # this dir contains gnos manifest files, will also host all reports
    repo = args.repo

    if not os.path.isdir(metadata_dir):  # TODO: should add more directory name check to make sure it's right
        sys.exit('Error: specified metadata directory does not exist!')

    timestamp = str.split(metadata_dir, '/')[-1]
    es_index = 'p_' + ('' if not repo else repo+'_') + re.sub(r'\D', '', timestamp).replace('20','',1)
    es_type = "donor"
    es_host = 'localhost:9200'

    es = Elasticsearch([es_host],
                        # sniff before doing anything
                        sniff_on_start=True,
                        # refresh nodes after a node fails to respond
                        sniff_on_connection_fail=True,
                        # and also every 60 seconds
                        sniffer_timeout=60)

    report_name = re.sub(r'^generate_pcawg_', '', os.path.basename(__file__))
    report_name = re.sub(r'\.py$', '', report_name)

    report_dir = init_report_dir(metadata_dir, report_name)

    #report_jsonl_fh = open(report_dir + '/' + report_name + '.jsonl', 'w')

    report_tsv_fh = open(report_dir + '/' + report_name + '.tsv', 'w')
    
    # read the tsv fields file and write to the pilot donor tsv file
    tsv_fields = ["donor_unique_id", "submitter_donor_id", "dcc_project_code", "aliquot_id", "submitter_specimen_id", \
    "submitter_sample_id", "dcc_specimen_type", "library_strategy", "aligned", "workflow_type", "has_bam_been_transferred",\
    "gnos_id", "computer_site" 
    ]
    report_tsv_fh.write('\t'.join(tsv_fields) + '\n')

	# get the full list of donors in PCAWG
    donors_list = get_donors_list(es, es_index, es_queries)
    
    # get the computer sites info
    compute_sites = {
        "aws_ireland": set(),
        "aws_oregon": set(),
        "bsc": set(),
        "dkfz": set(),
        "ebi": set(),
        "etri": set(),
        "oicr": set(),
        "pdc1_1": set(),
        "pdc2_0": set(),
        "tokyo": set(),
        "ucsc": set(),
        "sanger": set(),
        "idash": set(),
        "riken": set()
    }

    get_whitelists(compute_sites)

    site_counts = {}
    site_counts['unassigned'] = {
                    'total': 0,
                    'aligned': 0,
                    'unaligned': 0
    }
    
    unassigned_unaligned_donors = set()

    # get json doc for each donor 
    for donor_unique_id in donors_list:     
        
    	es_json = get_donor_json(es, es_index, donor_unique_id)
        
        specimen_info_list = create_specimen_info(donor_unique_id, es_json, compute_sites)
        
        for specimen in specimen_info_list: 
            #report_jsonl_fh.write(json.dumps(specimen, default=set_default) + '\n')
            # write to the tsv file
            line = ''
            for p in specimen.keys():
                if isinstance(specimen.get(p), list):
                    line += '|'.join(specimen.get(p))
                elif isinstance(specimen.get(p), set):
                    line += '|'.join(list(specimen.get(p)))
                else:
                    line += str(specimen.get(p)) if specimen.get(p) is not None else ''
                line += '\t'
            line = line[:-1]
            report_tsv_fh.write(line + '\n')

            # report for computer site
            compute_site_count(specimen, site_counts, unassigned_unaligned_donors)
        
    report_tsv_fh.close()
    #report_jsonl_fh.close()

    # report for each computer site and history
    compute_site_report(metadata_dir, report_dir, report_name, site_counts, unassigned_unaligned_donors)

    return 0


if __name__ == "__main__":
    sys.exit(main())
