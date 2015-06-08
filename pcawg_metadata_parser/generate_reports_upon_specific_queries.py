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
# query 0: PCAWGDATA-45_Sanger GNOS entries with study field ends with _test
{ 
    "name": "sanger_vcf_with_study_field_ends_with_test",
    "content":
              {
                 "fields":[
                     "donor_unique_id"
                     ],
                 "query":{
                    "wildcard":{
                            "variant_calling_results.sanger_variant_calling.study": "*_test"
                             }
                      }, 
                  "filter": {
                            "bool": {
                              "must": [
                                {
                                  "type": {
                                    "value": "donor"
                                  }
                                }                   
                              ]
                              # "must_not": [
                              #   {
                              #     "terms": {
                              #       "flags.is_manual_qc_failed": [
                              #         "T"
                              #       ]
                              #     }
                              #   },
                              #   {
                              #     "terms": {
                              #       "flags.is_donor_blacklisted": [
                              #         "T"
                              #       ]
                              #     }
                              #   }
                              # ]
                            }
                      },
                 "size": 10000
                }
},
# query 1: PCAWGDATA-47_donors with mismatch lane count
{
    "name": "specimens_with_mismatch_lane_count",
    "content":
            {
               "fields":[
                     "donor_unique_id"
                     ],  
               "filter":{
                  "bool":{
                     "must": [
                        {
                           "type":{
                              "value":"donor"
                           }
                        }
                      ],
                      "should": [
                        {
                           "terms":{
                              "normal_alignment_status.do_lane_count_and_bam_count_match":[
                                 "F"
                              ]
                           }
                        },
                        {
                           "terms":{
                              "normal_alignment_status.do_lane_counts_in_every_bam_entry_match":[
                                 "F"
                              ]
                           }
                        },
                        {
                          "nested": {
                            "path": "tumor_alignment_status",
                            "filter":{
                              "bool": {
                                "should": [
                                  {
                                     "terms":{
                                        "tumor_alignment_status.do_lane_count_and_bam_count_match":[
                                           "F"
                                        ]
                                     }
                                  },
                                  {
                                     "terms":{
                                        "tumor_alignment_status.do_lane_counts_in_every_bam_entry_match":[
                                           "F"
                                        ]
                                     }
                                  }                         
                                ]
                              }
                            }
                          }
                        }                                   
                      ],
                      "must_not": [
                      {
                        "terms": {
                          "gnos_repos_with_alignment_result":[
                            "https://cghub.ucsc.edu/"
                          ]
                        }
                      }
                     ]          
                    }
                  },
                  "size": 10000
        }
},
# query 3: sanger vcf missing input
{
      "name": "sanger_vcf_missing_input",
      "content":{
          "fields":[
                "donor_unique_id"
          ],
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
}
]

report_fields = [
["donor_unique_id", "submitter_donor_id", "dcc_project_code", "sanger_vcf_gnos_id", "study", "sanger_vcf_gnos_repo","sanger_vcf_metadata_url"],
["donor_unique_id", "submitter_donor_id", "dcc_project_code", "aliquot_id", "submitter_specimen_id", "submitter_sample_id", \
"dcc_specimen_type", "aligned", "number_of_bams", "total_lanes"],
["donor_unique_id", "submitter_donor_id", "dcc_project_code", "sanger_vcf_gnos_id", \
"normal_aliquot_id", "normal_submitter_specimen", "normal_bam_gnos_id", "is_normal_bam_used_by_sanger_missing", \
"tumor_aliquot_id", "tumor_submitter_specimen", "tumor_bam_gnos_id", "is_tumor_bam_used_by_sanger_missing"]
]

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


def get_donors_list(es, es_index, es_queries, q_index):
    response = es.search(index=es_index, body=es_queries[q_index].get('content'))
    
    donors_list = []
    for p in response['hits']['hits']:
      donors_list.append(p.get('fields').get('donor_unique_id')[0])

    return donors_list 

def create_report_info(donor_unique_id, es_json, q_index):
    report_info_list = []

    report_info = OrderedDict()
    report_info['donor_unique_id'] = donor_unique_id
    report_info['submitter_donor_id'] = es_json['submitter_donor_id']
    report_info['dcc_project_code'] = es_json['dcc_project_code']
    
    if q_index == 0:
      add_report_info_0(report_info, report_info_list, es_json)

    if q_index == 1:
      add_report_info_1(report_info, report_info_list, es_json)

    if q_index == 2:
      add_report_info_2(report_info, report_info_list, es_json)

    return report_info_list

def add_report_info_0(report_info, report_info_list, es_json):
    report_info['gnos_id'] = es_json.get('variant_calling_results').get('sanger_variant_calling').get('gnos_id')
    report_info['study'] = es_json.get('variant_calling_results').get('sanger_variant_calling').get('study')
    for gnos_repo in es_json.get('variant_calling_results').get('sanger_variant_calling').get('gnos_repo'):
        report_info['gnos_repo'] = gnos_repo
        report_info['gnos_metadata_url'] = gnos_repo + 'cghub/metadata/analysisFull/' + report_info['gnos_id']
        report_info_list.append(copy.deepcopy(report_info))

def add_report_info_1(report_info, report_info_list, es_json):
    if es_json.get('normal_alignment_status'):
        add_report_info_1_aliquot(es_json.get('normal_alignment_status'), report_info, report_info_list)

    if es_json.get('tumor_alignment_status'):
        for aliquot in es_json.get('tumor_alignment_status'):
            add_report_info_1_aliquot(aliquot, report_info, report_info_list)

    return report_info_list

def add_report_info_1_aliquot(aliquot, report_info, report_info_list):
    report_info['aliquot_id'] = aliquot.get('aliquot_id')
    report_info['submitter_specimen_id'] = aliquot.get('submitter_specimen_id')
    report_info['submitter_sample_id'] = aliquot.get('submitter_sample_id')
    report_info['dcc_specimen_type'] = aliquot.get('dcc_specimen_type')   
    report_info['aligned'] = True if aliquot.get('aligned') else False

    if not aliquot.get('do_lane_count_and_bam_count_match') or not aliquot.get('do_lane_counts_in_every_bam_entry_match'):
        report_info['number_of_bams'] = len(aliquot.get('unaligned_bams'))
        report_info['total_lanes'] = aliquot.get('lane_count')
        report_info_list.append(copy.deepcopy(report_info))

    return report_info_list

def add_report_info_2(report_info, report_info_list, es_json):
    sanger_vcf = es_json.get('variant_calling_results').get('sanger_variant_calling')
    report_info['sanger_vcf_gnos_id'] = sanger_vcf.get('gnos_id')
    report_info['normal_aliquot_id'] = None
    report_info['normal_submitter_specimen'] = None
    report_info['normal_bam_gnos_id'] = None
    report_info['is_normal_bam_used_by_sanger_missing'] = sanger_vcf.get('is_normal_bam_used_by_sanger_missing')
    report_info['tumor_aliquot_id'] = []
    report_info['tumor_submitter_specimen'] = []
    report_info['tumor_bam_gnos_id'] = []
    report_info['is_tumor_bam_used_by_sanger_missing'] = sanger_vcf.get('is_tumor_bam_used_by_sanger_missing')    
    for vcf_input in sanger_vcf.get('workflow_details').get('variant_pipeline_input_info'):
        if 'normal' in vcf_input.get('attributes').get('dcc_specimen_type').lower():
            report_info['normal_aliquot_id'] = vcf_input.get('specimen')
            report_info['normal_submitter_specimen'] = vcf_input.get('attributes').get('submitter_specimen_id')
            report_info['normal_bam_gnos_id'] = vcf_input.get('attributes').get('analysis_id')
        elif 'tumour' in vcf_input.get('attributes').get('dcc_specimen_type').lower():
            report_info['tumor_aliquot_id'].append(vcf_input.get('specimen'))
            report_info['tumor_submitter_specimen'].append(vcf_input.get('attributes').get('submitter_specimen_id'))
            report_info['tumor_bam_gnos_id'].append(vcf_input.get('attributes').get('analysis_id'))
    report_info_list.append(copy.deepcopy(report_info))


def init_report_dir(metadata_dir, report_name, repo):
    report_dir = metadata_dir + '/reports/' + report_name if not repo else metadata_dir + '/reports/' + report_name + '/' + repo
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)
    return report_dir


def main(argv=None):

    parser = ArgumentParser(description="Get Donor Info For Specific Query",
             formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-m", "--metadata_dir", dest="metadata_dir",
             help="Directory containing metadata manifest files", required=True)
    parser.add_argument("-r", "--gnos_repo", dest="repo",
             help="Specify which GNOS repo to process, process all repos if none specified", required=False)
    parser.add_argument("-q", "--ES_query", dest="q_index",
             help="Specify which ES_query to be used", required=False)


    args = parser.parse_args()
    metadata_dir = args.metadata_dir  # this dir contains gnos manifest files, will also host all reports
    repo = args.repo
    q_index = args.q_index

    if not os.path.isdir(metadata_dir):  # TODO: should add more directory name check to make sure it's right
        sys.exit('Error: specified metadata directory does not exist!')

    q_index = range(len(report_fields)) if not q_index else [int(q_index)] 

    timestamp = str.split(metadata_dir, '/')[-1]
    es_index = 'p_' + ('' if not repo else repo+'_') + re.sub(r'\D', '', timestamp).replace('20','',1)
    es_type = "donor"
    es_host = 'localhost:9200'

    es = Elasticsearch([es_host])
  
    # output result
    report_name = re.sub(r'^generate_', '', os.path.basename(__file__))
    report_name = re.sub(r'\.py$', '', report_name)
    report_dir = init_report_dir(metadata_dir, report_name, repo)

    for q in q_index:
        report_tsv_fh = open(report_dir + '/' + es_queries[q].get('name') + '.tsv', 'w')  
        report_tsv_fh.write('\t'.join(report_fields[q]) + '\n')
        # get the list of donors
        donors_list = get_donors_list(es, es_index, es_queries, q)
        # get json doc for each donor
        for donor_unique_id in donors_list:                 
            es_json = get_donor_json(es, es_index, donor_unique_id)
            
            report_info_list = create_report_info(donor_unique_id, es_json, q)
            
            for r in report_info_list: 
                # make the list of output from dict
                line = []
                for p in r.keys():
                    if isinstance(r.get(p), list):
                        line.append('|'.join(r.get(p)))
                    elif isinstance(r.get(p), set):
                        line.append('|'.join(list(r.get(p))))
                    else:
                        line.append(str(r.get(p)))
                report_tsv_fh.write('\t'.join(line) + '\n') 
        
        report_tsv_fh.close()            


    return 0


if __name__ == "__main__":
    sys.exit(main())