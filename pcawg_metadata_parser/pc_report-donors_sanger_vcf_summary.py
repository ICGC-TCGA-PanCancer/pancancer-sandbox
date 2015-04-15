#!/usr/bin/env python

import sys
import os
import re
import json
from collections import OrderedDict
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from elasticsearch import Elasticsearch
from operator import itemgetter


es_host = 'localhost:9200'
es_type = "donor"
es = Elasticsearch([es_host])

es_queries = [
  # order of the queries is important
  # query 0: bwa_bams_in_normal_used_by_sanger 
    {
     "fields":["dcc_project_code",
                "donor_unique_id",
                "variant_calling_results.sanger_variant_calling.gnos_id",
                "variant_calling_results.sanger_variant_calling.is_bam_used_by_sanger_missing",
                "variant_calling_results.sanger_variant_calling.workflow_details.variant_pipeline_input_info.specimen",
                "variant_calling_results.sanger_variant_calling.workflow_details.variant_pipeline_input_info.attributes.dcc_specimen_type",
                "variant_calling_results.sanger_variant_calling.workflow_details.variant_pipeline_input_info.attributes.submitter_specimen_id",
                "variant_calling_results.sanger_variant_calling.workflow_details.variant_pipeline_input_info.attributes.analysis_id",
                "variant_calling_results.sanger_variant_calling.is_normal_bam_used_by_sanger_missing",
                "variant_calling_results.sanger_variant_calling.is_tumor_bam_used_by_sanger_missing"], 
 
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
#                        {
#                          "terms": {
#                            "flags.are_all_tumor_specimens_aligned": [
#                              "T"
#                            ]
#                          }
#                        }
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



def init_report_dir(metadata_dir, report_name, repo):
    report_dir = metadata_dir + '/reports/' + report_name if not repo else metadata_dir + '/reports/' + report_name + '/' + repo
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)
    return report_dir



def generate_report(es_index, es_queries, metadata_dir, report_name, timestamp, repo):
    # we need to run several queries to get facet counts for different type of donors
#    report = OrderedDict()
#    count_types = [
#        "mismatch_bwa_bams_in_normal",
#        "mismatch_bwa_bams_in_tumor"
#    ]

#    for q_index in range(len(count_types)):
    q_index = 0
    report = []
    response = es.search(index=es_index, body=es_queries[q_index])
    #print(json.dumps(response['aggregations']['project_f']))  # for debugging
    for p in response['hits']['hits']:
        summary=OrderedDict()
        summary['dcc_project_code'] = p.get('fields').get('dcc_project_code')[0]
        summary['donor_unique_id'] = p.get('fields').get('donor_unique_id')[0]
        summary['sanger_vcf_gnos_id'] = p.get('fields').get('variant_calling_results.sanger_variant_calling.gnos_id')[0]
        summary['is_bam_used_by_sanger_missing'] = p.get('fields').get('variant_calling_results.sanger_variant_calling.is_bam_used_by_sanger_missing')[0]
        # find the index for normal
        specimen_type = p.get('fields').get('variant_calling_results.sanger_variant_calling.workflow_details.variant_pipeline_input_info.attributes.dcc_specimen_type')
        index = [item for item in range(len(specimen_type)) if 'normal' in specimen_type[item].lower()]
        aliquot_id = p.get('fields').get('variant_calling_results.sanger_variant_calling.workflow_details.variant_pipeline_input_info.specimen')
        submitter_specimen = p.get('fields').get('variant_calling_results.sanger_variant_calling.workflow_details.variant_pipeline_input_info.attributes.submitter_specimen_id')
        bam_gnos_id = p.get('fields').get('variant_calling_results.sanger_variant_calling.workflow_details.variant_pipeline_input_info.attributes.analysis_id')

        summary['normal_aliquot_id'] = [aliquot_id[i] for i in index] 
        summary['normal_submitter_specimen'] = [submitter_specimen[i] for i in index]
        summary['normal_bam_gnos_id'] = [bam_gnos_id[i] for i in index]
        summary['is_normal_bam_used_by_sanger_missing'] = p.get('fields').get('variant_calling_results.sanger_variant_calling.is_normal_bam_used_by_sanger_missing')[0]

        # find the index for tumor
        index = [item for item in range(len(specimen_type)) if 'tumour' in specimen_type[item].lower()]
         
        summary['tumor_aliquot_id'] = [aliquot_id[i] for i in index] 
        summary['tumor_submitter_specimen'] = [submitter_specimen[i] for i in index]
        summary['tumor_bam_gnos_id'] = [bam_gnos_id[i] for i in index]
        summary['is_tumor_bam_used_by_sanger_missing'] = p.get('fields').get('variant_calling_results.sanger_variant_calling.is_tumor_bam_used_by_sanger_missing')[0]

        report.append(summary) 

    report_dir = init_report_dir(metadata_dir, report_name, repo)

    # sort the report_in_donor_order
    report_in_donor_order = sorted(report, key=itemgetter('donor_unique_id'))    

    # report the results in donor_unique_id order
    with open(report_dir + '/' + report_name + 'bam_missing.sort_by_donor_id.txt', 'w') as a:
        a.write('# ' + report_name + '\n')
        a.write('# dcc_project_code' + '\t' + 'donor_unique_id' + '\t' + 'sanger_vcf_gnos_id' + '\t' +
            'is_bam_used_by_sanger_missing' + '\t' + 'normal_aliquot_id' + '\t' +
            'normal_submitter_specimen' + '\t' + 'normal_bam_gnos_id' + '\t' + 'is_normal_bam_used_by_sanger_missing' + '\t' +
            'tumor_aliquot_id' + '\t' + 'tumor_submitter_specimen' + '\t' + 'tumor_bam_gnos_id' + '\t' + 
            'is_tumor_bam_used_by_sanger_missing' + '\n')

        for l in report_in_donor_order:
            for p in l.keys():
                if isinstance(l.get(p), list):
                    for q in l.get(p):
                        a.write(str(q) + ' ')
                    a.write('\t')
                else:
                    a.write(str(l.get(p)) + '\t')
            a.write('\n')

#    with open(report_dir + '/donor.json', 'w') as o:
#        o.write(json.dumps(summary_table))


def get_donors(donor_buckets):
    donors = []
    for d in donor_buckets:
        donors.append(d.get('key'))
    return donors


def main(argv=None):
    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    parser = ArgumentParser(description="PCAWG Report Generator Using ES Backend",
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

    report_name = re.sub(r'^pc_report-', '', os.path.basename(__file__))
    report_name = re.sub(r'\.py$', '', report_name)

    generate_report(es_index, es_queries, metadata_dir, report_name, timestamp, repo)

    return 0


if __name__ == "__main__":
    sys.exit(main()) 
