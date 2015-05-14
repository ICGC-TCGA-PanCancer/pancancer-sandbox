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

def create_gnos_entity_info(donor_unique_id, es_json):
    gnos_entity_info_list = []

    gnos_entity_info = OrderedDict()
    gnos_entity_info['donor_unique_id'] = donor_unique_id
    gnos_entity_info['submitter_donor_id'] = es_json['submitter_donor_id']
    gnos_entity_info['dcc_project_code'] = es_json['dcc_project_code']
    
    add_wgs_gnos_entity(gnos_entity_info_list, gnos_entity_info, es_json)

    add_vcf_gnos_entity(gnos_entity_info_list, gnos_entity_info, es_json)

    add_rna_seq_gnos_entity(gnos_entity_info_list, gnos_entity_info, es_json)

    return gnos_entity_info_list


def add_wgs_gnos_entity(gnos_entity_info_list, gnos_entity_info, es_json):

    if es_json.get('normal_alignment_status'):
        add_wgs_aliquot_gnos_entity(es_json.get('normal_alignment_status'), gnos_entity_info, gnos_entity_info_list)

    if es_json.get('tumor_alignment_status'):
        for aliquot in es_json.get('tumor_alignment_status'):
            add_wgs_aliquot_gnos_entity(aliquot, gnos_entity_info, gnos_entity_info_list)

    return gnos_entity_info_list


def add_wgs_aliquot_gnos_entity(aliquot, gnos_entity_info, gnos_entity_info_list):
    gnos_entity_info['library_strategy'] = 'WGS'
    gnos_entity_info['aliquot_id'] = aliquot.get('aliquot_id')
    gnos_entity_info['submitter_specimen_id'] = aliquot.get('submitter_specimen_id')
    gnos_entity_info['submitter_sample_id'] = aliquot.get('submitter_sample_id')
    gnos_entity_info['dcc_specimen_type'] = aliquot.get('dcc_specimen_type')

    if aliquot.get('aligned_bam'):
        gnos_entity_info['entity_type'] = 'Aligned_bam'
        gnos_entity_info['gnos_id'] = aliquot.get('aligned_bam').get('gnos_id')
        for gnos_repo in aliquot.get('aligned_bam').get('gnos_repo'):
            gnos_entity_info['gnos_repo'] = gnos_repo
            gnos_entity_info['gnos_metadata_url'] = gnos_repo + 'cghub/metadata/analysisFull/' + gnos_entity_info['gnos_id']
            gnos_entity_info_list.append(copy.deepcopy(gnos_entity_info))

    if aliquot.get('bam_with_unmappable_reads'):
        gnos_entity_info['entity_type'] = 'Bam_with_unmappable_reads'
        gnos_entity_info['gnos_id'] = aliquot.get('bam_with_unmappable_reads').get('gnos_id')
        for gnos_repo in aliquot.get('bam_with_unmappable_reads').get('gnos_repo'):
            gnos_entity_info['gnos_repo'] = gnos_repo
            gnos_entity_info['gnos_metadata_url'] = gnos_repo + 'cghub/metadata/analysisFull/' + gnos_entity_info['gnos_id']
            gnos_entity_info_list.append(copy.deepcopy(gnos_entity_info))            

    if aliquot.get('unaligned_bams'):
        gnos_entity_info['entity_type'] = 'Unaligned_bams'
        for unaligned_bams in aliquot.get('unaligned_bams'):
            gnos_entity_info['gnos_id'] = unaligned_bams.get('gnos_id')
            for gnos_repo in unaligned_bams.get('gnos_repo'):
                gnos_entity_info['gnos_repo'] = gnos_repo
                gnos_entity_info['gnos_metadata_url'] = gnos_repo + 'cghub/metadata/analysisFull/' + gnos_entity_info['gnos_id']
                gnos_entity_info_list.append(copy.deepcopy(gnos_entity_info))  

    return gnos_entity_info_list

def add_vcf_gnos_entity(gnos_entity_info_list, gnos_entity_info, es_json):
    if es_json.get('variant_calling_results'):
        gnos_entity_info['library_strategy'] = 'WGS'
        gnos_entity_info['aliquot_id'] = None
        gnos_entity_info['submitter_specimen_id'] = None
        gnos_entity_info['submitter_sample_id'] = None
        gnos_entity_info['dcc_specimen_type'] = None
        for vcf_type in es_json.get('variant_calling_results').keys():
            gnos_entity_info['entity_type'] = vcf_type.capitalize()
            gnos_entity_info['gnos_id'] = es_json.get('variant_calling_results').get(vcf_type).get('gnos_id')    
            for gnos_repo in es_json.get('variant_calling_results').get(vcf_type).get('gnos_repo'):
                gnos_entity_info['gnos_repo'] = gnos_repo
                gnos_entity_info['gnos_metadata_url'] = gnos_repo + 'cghub/metadata/analysisFull/' + gnos_entity_info['gnos_id']
                gnos_entity_info_list.append(copy.deepcopy(gnos_entity_info))

    return gnos_entity_info_list


def filter_liri_jp(project, gnos_repo):
    if not project == 'LIRI-JP':
        return gnos_repo
    elif "https://gtrepo-riken.annailabs.com/" in gnos_repo:
        return ["https://gtrepo-riken.annailabs.com/"]
    else:
        print "This should never happen: alignment for LIRI-JP is not available at Riken repo"
        sys.exit(1)


def add_rna_seq_gnos_entity(gnos_entity_info_list, gnos_entity_info, es_json):
    rna_seq_info = es_json.get('rna_seq').get('alignment')
    for specimen_type in rna_seq_info.keys():
        if not rna_seq_info.get(specimen_type): # the specimen_type has no alignment result
		    continue
        if 'normal' in specimen_type:
            aliquot = rna_seq_info.get(specimen_type)
            add_rna_seq_aliquot_gnos_entity(aliquot, gnos_entity_info, gnos_entity_info_list)

        else:
            for aliquot in rna_seq_info.get(specimen_type):
                add_rna_seq_aliquot_gnos_entity(aliquot, gnos_entity_info, gnos_entity_info_list)

    return gnos_entity_info_list

def add_rna_seq_aliquot_gnos_entity(aliquot, gnos_entity_info, gnos_entity_info_list):
    gnos_entity_info['library_strategy'] = 'RNA-Seq'
    gnos_entity_info['aliquot_id'] = set()
    gnos_entity_info['submitter_specimen_id'] = set()
    gnos_entity_info['submitter_sample_id'] = set()
    gnos_entity_info['dcc_specimen_type'] = set()
    for workflow_type in aliquot.keys():
        gnos_entity_info['aliquot_id'].add(aliquot.get(workflow_type).get('aliquot_id'))
        gnos_entity_info['submitter_specimen_id'].add(aliquot.get(workflow_type).get('submitter_specimen_id'))
        gnos_entity_info['submitter_sample_id'].add(aliquot.get(workflow_type).get('submitter_sample_id'))
        gnos_entity_info['dcc_specimen_type'].add(aliquot.get(workflow_type).get('dcc_specimen_type'))

    for workflow_type in aliquot.keys():
        gnos_entity_info['entity_type'] = workflow_type.upper()
        gnos_entity_info['gnos_id'] = aliquot.get(workflow_type).get('gnos_info').get('gnos_id')
        for gnos_repo in aliquot.get(workflow_type).get('gnos_info').get('gnos_repo'):
            gnos_entity_info['gnos_repo'] = gnos_repo
            gnos_entity_info['gnos_metadata_url'] = gnos_repo + 'cghub/metadata/analysisFull/' + gnos_entity_info['gnos_id']
            gnos_entity_info_list.append(copy.deepcopy(gnos_entity_info))            

    return gnos_entity_info_list

def get_donor_json(es, es_index, donor_unique_id):
    es_query_donor = {
        "query": {
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


def main(argv=None):

    parser = ArgumentParser(description="PCAWG Full List of GNOS entities Info Generator",
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

    es = Elasticsearch([es_host])

    PCAWG_GNOS_entity_fh = open(metadata_dir+'/PCAWG_Full_List_GNOS_entities_'+es_index+'.jsonl', 'w')

    PCAWG_GNOS_entity_tsv_fh = open(metadata_dir + '/PCAWG_Full_List_GNOS_entities_' + es_index + '.tsv', 'w')
    
    # read the tsv fields file and write to the pilot donor tsv file
    # tsv_fields = 'PCAWG_Full_List_GNOS_entities_tsv_fields.txt'
    tsv_fields = ["donor_unique_id", "submitter_donor_id", "dcc_project_code", "library_strategy", "aliquot_id", \
    "submitter_specimen_id", "submitter_sample_id", "dcc_specimen_type", "entity_type", "gnos_id" , "gnos_repo", \
    "gnos_metadata_url"
    ]
    PCAWG_GNOS_entity_tsv_fh.write('\t'.join(tsv_fields) + '\n')


	# get the full list of donors in PCAWG
    donors_list = get_donors_list(es, es_index, es_queries)
    
    # get json doc for each donor and reorganize it 
    for donor_unique_id in donors_list:     
        
    	es_json = get_donor_json(es, es_index, donor_unique_id)
        
        gnos_entity_info_list = create_gnos_entity_info(donor_unique_id, es_json)
        
        for gnos_entity in gnos_entity_info_list: 
            PCAWG_GNOS_entity_fh.write(json.dumps(gnos_entity, default=set_default) + '\n')
            # write to the tsv file
            for p in gnos_entity.keys():
                if isinstance(gnos_entity.get(p), set):
                    PCAWG_GNOS_entity_tsv_fh.write('|'.join(list(gnos_entity.get(p))) + '\t')
                elif not gnos_entity.get(p):
                    PCAWG_GNOS_entity_tsv_fh.write('\t')
                else:
                    PCAWG_GNOS_entity_tsv_fh.write(str(gnos_entity.get(p)) + '\t')
            PCAWG_GNOS_entity_tsv_fh.write('\n')
        
    PCAWG_GNOS_entity_tsv_fh.close()

    PCAWG_GNOS_entity_fh.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())