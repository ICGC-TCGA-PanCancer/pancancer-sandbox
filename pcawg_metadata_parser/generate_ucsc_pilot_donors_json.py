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
  # query 0: donors_sanger_vcf_without_missing_bams 
    {
     "fields": "donor_unique_id", 
 
    "filter": {
                    "bool": {
                      "must": [
                        {
                          "type": {
                            "value": "donor"
                          }
                        },
                        {
                          "terms":{
                            "flags.is_sanger_variant_calling_performed":[
                              "T"
                            ]
                          }
                        },
                        {
                          "terms": {
                            "variant_calling_results.sanger_variant_calling.is_bam_used_by_sanger_missing": [
                              "F"
                            ]
                          }
                        },
                        {
                          "terms":{
                            "flags.is_normal_specimen_aligned":[
                              "T"
                            ]
                          }
                        },
                        {
                          "terms":{
                            "flags.are_all_tumor_specimens_aligned":[
                              "T"
                            ]
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

def create_reorganized_donor(donor_unique_id, es_json):
    reorganized_donor = {
        'donor_unique_id': donor_unique_id,
        'submitter_donor_id': es_json['submitter_donor_id'],
        'dcc_project_code': es_json['dcc_project_code'],
        'data_train': 'train2' if es_json.get('flags').get('is_train2_donor') else 'train3',
        'wgs': {
            'normal_specimen': {
                'bwa_alignment': {
                    'submitter_specimen_id': es_json.get('normal_alignment_status').get('submitter_specimen_id'),
                    'submitter_sample_id': es_json.get('normal_alignment_status').get('submitter_sample_id'),
                    'specimen_type': es_json.get('normal_alignment_status').get('dcc_specimen_type'),
                    'aliquot_id': es_json.get('normal_alignment_status').get('aliquot_id'),
                    'gnos_repo': es_json.get('normal_alignment_status').get('aligned_bam').get('gnos_repo'),
                    'gnos_id': es_json.get('normal_alignment_status').get('aligned_bam').get('gnos_id'),
                    'files': [
                        {
	                        'bam_file_name': es_json.get('normal_alignment_status').get('aligned_bam').get('bam_file_name'),
	                        'bam_file_size': es_json.get('normal_alignment_status').get('aligned_bam').get('bam_file_size')                        
	                    }
                    ]
                }
            },
            'tumor_specimens': []
        },
        'rna_seq': {
             'normal_specimens': [],
             'tumor_specimens': []
        }
    }

    add_wgs_tumor_specimens(reorganized_donor, es_json)

    add_rna_seq_info(reorganized_donor, es_json)

    return reorganized_donor


def add_wgs_tumor_specimens(reorganized_donor, es_json):
    wgs_tumor_alignment_info = es_json.get('tumor_alignment_status')
    wgs_tumor_sanger_vcf_info = es_json.get('variant_calling_results').get('sanger_variant_calling')
    sanger_vcf_files = wgs_tumor_sanger_vcf_info.get('files')

    tumor_wgs_specimen_count = 0
    aliquot_info = {}
    for aliquot in wgs_tumor_alignment_info:
        tumor_wgs_specimen_count += 1
    	aliquot_id = aliquot.get('aliquot_id')

    	aliquot_info = {
    	    'bwa_alignment':{
                'submitter_specimen_id': aliquot.get('submitter_specimen_id'),
                'submitter_sample_id': aliquot.get('submitter_sample_id'),
                'specimen_type': aliquot.get('dcc_specimen_type'),
                'aliquot_id': aliquot.get('aliquot_id'),
                'gnos_repo': aliquot.get('aligned_bam').get('gnos_repo'),
                'gnos_id': aliquot.get('aligned_bam').get('gnos_id'),
                'files':[
                    {
                        'bam_file_name': aliquot.get('aligned_bam').get('bam_file_name'),
                        'bam_file_size': aliquot.get('aligned_bam').get('bam_file_size')
                    }
                ]
    	    },
    	    'sanger_variant_calling':{
                'submitter_specimen_id': aliquot.get('submitter_specimen_id'),
                'submitter_sample_id': aliquot.get('submitter_sample_id'),
                'specimen_type': aliquot.get('dcc_specimen_type'),
                'aliquot_id': aliquot.get('aliquot_id'),
                'gnos_repo': wgs_tumor_sanger_vcf_info.get('gnos_repo'),
                'gnos_id': wgs_tumor_sanger_vcf_info.get('gnos_id'),
                'files':[]
    	    }
    	}

        if sanger_vcf_files:
            for f in sanger_vcf_files:
                if aliquot_id in f.get('file_name'):
                    aliquot_info.get('sanger_variant_calling').get('files').append(f)
        
        reorganized_donor.get('wgs').get('tumor_specimens').append(aliquot_info) 

    reorganized_donor['tumor_wgs_specimen_count'] = tumor_wgs_specimen_count


def add_rna_seq_info(reorganized_donor, es_json):
    rna_seq_info = es_json.get('rna_seq').get('alignment')
    for specimen_type in rna_seq_info.keys():
        if not rna_seq_info.get(specimen_type): # the specimen_type has no alignment result
		    continue
        else:
            for aliquot in rna_seq_info.get(specimen_type):
                alignment_info = {}
                for workflow_type in aliquot.keys():
                    alignment_info[workflow_type] = {
			    	    'submitter_specimen_id': aliquot.get(workflow_type).get('submitter_specimen_id'),
			    	    'submitter_sample_id': aliquot.get(workflow_type).get('submitter_sample_id'),
                        'specimen_type': aliquot.get(workflow_type).get('dcc_specimen_type'),
			    	    'aliquot_id': aliquot.get(workflow_type).get('aliquot_id'),
			    	    'gnos_repo': aliquot.get(workflow_type).get('gnos_info').get('gnos_repo'),
			    	    'gnos_id': aliquot.get(workflow_type).get('gnos_info').get('gnos_id'),
			    	    'files': [
			    	        {
				    	        'bam_file_name': aliquot.get(workflow_type).get('gnos_info').get('bam_file_name'),
			                    'bam_file_size': aliquot.get(workflow_type).get('gnos_info').get('bam_file_size')			    	        
			                }
			    	    ]
			    	}
                reorganized_donor.get('rna_seq')[specimen_type + '_specimens'].append(alignment_info) 


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

def init_es(es_host, es_index):
    es = Elasticsearch([ es_host ])

    es.indices.create( es_index, ignore=400 )

    # create mappings
    es_mapping = open('pancan.reorganized.donor.mapping.json')
    es.indices.put_mapping(index=es_index, doc_type='donor', body=es_mapping.read())
    es_mapping.close()

    return es


def set_default(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


def main(argv=None):

    parser = ArgumentParser(description="PCAWG Reorganized Json Donors Info Generator",
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
    es_index_reorganize = 'r_' + ('' if not repo else repo+'_') + re.sub(r'\D', '', timestamp).replace('20','',1)
    es_type = "donor"
    es_host = 'localhost:9200'

    es = Elasticsearch([es_host])
    #es_reorganized = init_es(es_host, es_index_reorganize)

    donor_fh = open(metadata_dir+'/ucsc_polit_donor_'+es_index_reorganize+'.jsonl', 'w')

	# get the list of donors whose sanger_vcf without missing bams
    donors_list = get_donors_list(es, es_index, es_queries)
    
    # get json doc for each donor and reorganize it 
    for donor_unique_id in donors_list:     
        
    	es_json = get_donor_json(es, es_index, donor_unique_id)
        
        reorganized_donor = create_reorganized_donor(donor_unique_id, es_json)


        # DO NOT NEED THIS YET: push to Elasticsearch
        #es_reorganized.index(index=es_index_reorganize, doc_type='donor', id=reorganized_donor['donor_unique_id'], \
        #    body=json.loads(json.dumps(reorganized_donor, default=set_default)), timeout=90 )

        donor_fh.write(json.dumps(reorganized_donor, default=set_default) + '\n')

    donor_fh.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())