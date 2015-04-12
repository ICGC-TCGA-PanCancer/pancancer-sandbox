#!/usr/bin/env python

# Author: Junjun Zhang

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


logger = logging.getLogger('gnos parser')
# create console handler with a higher log level
ch = logging.StreamHandler()


def init_es(es_host, es_index):
    es = Elasticsearch([ es_host ])
    #es.indices.delete( es_index, ignore=[400, 404] )
    es.indices.create( es_index, ignore=400 )

    # create mappings
    es_mapping = open('pancan.donor.mapping.json')
    es.indices.put_mapping(index=es_index, doc_type='donor', body=es_mapping.read())
    es_mapping.close()

    es_mapping = open('pancan.file.mapping.json')
    es.indices.put_mapping(index=es_index, doc_type='bam_file', body=es_mapping.read())
    es_mapping.close()

    return es


def process_gnos_analysis(gnos_analysis, donors, vcf_entries, es_index, es, bam_output_fh, annotations):
  analysis_attrib = get_analysis_attrib(gnos_analysis)

  if analysis_attrib and analysis_attrib.get('variant_workflow_name'):  # variant call gnos entry

    if analysis_attrib.get('variant_workflow_name') == 'SangerPancancerCgpCnIndelSnvStr' \
        and (
                (analysis_attrib.get('variant_workflow_version').startswith('1.0.')
                or analysis_attrib.get('variant_workflow_version').startswith('1.1.'))
                and not analysis_attrib.get('variant_workflow_version') in ['1.0.0', '1.0.1']
            ):
        donor_unique_id = analysis_attrib.get('dcc_project_code') + '::' + analysis_attrib.get('submitter_donor_id')

        logger.info('process Sanger variant call for donor: {}, in entry {}'
            .format(donor_unique_id, gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')))

        current_vcf_entry = create_vcf_entry(analysis_attrib, gnos_analysis)

        if annotations.get('sanger_vcf_in_jamboree').get(donor_unique_id): # the current donor has sanger variant calling result in jamboree
            if annotations.get('sanger_vcf_in_jamboree').get(donor_unique_id) == current_vcf_entry.get('gnos_id'): # this is the one expected
                vcf_entries[donor_unique_id] = {'sanger_variant_calling': current_vcf_entry}
                logger.info('Sanger variant calling result for donor: {}. It is already saved in Jamboree, GNOS entry is {}'
                    .format(donor_unique_id, gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')))
            else: # this is not the one expected, likely duplications
                logger.warning('Sanger variant calling result for donor: {}. Ignored as it not the one saved in Jamboree, ignoring entry {}'
                    .format(donor_unique_id, gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')))

        elif vcf_entries.get(donor_unique_id) and vcf_entries.get(donor_unique_id).get('sanger_variant_calling'):
            # let's see whether they have the same GNOS ID first, if yes, it's a copy at different GNOS repo
            # if not the same GNOS ID, we will see which one is newer, will keep the newer one
            # this can be complicated as the GNOS entries coming as a ramdon order, it's not possible to decide 
            # which ones to keep when there are multiple GNOS IDs and one or all of them have replicates in different GNOS repo
            # worry about this later.
            # If there is no synchronization or redundant calling/uploading it would be much easier.
            # The other way of handling this is to keep all VCF call entries and sort them out at
            # the end when all entries are at hand

            workflow_version_current = current_vcf_entry.get('workflow_details').get('variant_workflow_version')
            workflow_version_previous = vcf_entries.get(donor_unique_id).get('sanger_variant_calling').get('workflow_details').get('variant_workflow_version')
            gnos_updated_current = current_vcf_entry.get('gnos_last_modified')[0]
            gnos_updated_previous = vcf_entries.get(donor_unique_id).get('sanger_variant_calling').get('gnos_last_modified')[0]

            if LooseVersion(workflow_version_current) > LooseVersion(workflow_version_previous): # current is newer version
                logger.info('Newer Sanger variant calling result with version: {} for donor: {}, in entry: {} replacing older GNOS entry {} in {}'
                    .format(workflow_version_current, donor_unique_id, \
                        gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull'), \
                        vcf_entries.get(donor_unique_id).get('sanger_variant_calling').get('gnos_id'), \
                        vcf_entries.get(donor_unique_id).get('sanger_variant_calling').get('gnos_repo')[0]))
                vcf_entries.get(donor_unique_id)['sanger_variant_calling'] = current_vcf_entry
            elif LooseVersion(workflow_version_current) == LooseVersion(workflow_version_previous) \
                 and gnos_updated_current > gnos_updated_previous: # current is newer
                logger.info('Newer Sanger variant calling result with last modified date: {} for donor: {}, in entry: {} replacing older GNOS entry {} in {}'
                    .format(gnos_updated_current, donor_unique_id, \
                        gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull'), \
                        vcf_entries.get(donor_unique_id).get('sanger_variant_calling').get('gnos_id'), \
                        vcf_entries.get(donor_unique_id).get('sanger_variant_calling').get('gnos_repo')[0]))
                vcf_entries.get(donor_unique_id)['sanger_variant_calling'] = current_vcf_entry
            else: # no need to replace
                logger.warning('Sanger variant calling result already exist and is latest for donor: {}, ignoring entry {}'
                    .format(donor_unique_id, gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')))

        else:
            vcf_entries[donor_unique_id] = {'sanger_variant_calling': current_vcf_entry}

    else:  # this is test for VCF upload
        logger.warning('ignore entry that is variant calling but likely is test entry, GNOS entry: {}'
                         .format(gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull') ))
        return

  else:  # BAM entry
    if gnos_analysis.get('dcc_project_code') and gnos_analysis.get('dcc_project_code').upper() == 'TEST':
        logger.warning('ignore entry with dcc_project_code being TEST, GNOS entry: {}'
                         .format(gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull') ))
        return

    if gnos_analysis.get('library_strategy') and gnos_analysis.get('library_strategy') == 'RNA-Seq':
        logger.warning('ignore entry with library_strategy being RNA-Seq for now, GNOS entry: {}'
                         .format(gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull') ))
        return

    if not gnos_analysis.get('aliquot_id'):
        logger.warning('ignore entry does not have aliquot_id, GNOS entry: {}'
                         .format(gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull') ))
        return

    if gnos_analysis.get('refassem_short_name') != 'unaligned' and gnos_analysis.get('refassem_short_name') != 'GRCh37':
        logger.warning('ignore entry that is aligned but not aligned to GRCh37: {}'
                         .format(gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull') ))
        return # completely ignore test gnos entries for now, this is the quickest way to avoid test interferes real data 

    if not analysis_attrib:
        logger.warning('ignore entry does not have ANALYSIS information, GNOS entry: {}'
                         .format(gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull') ))
        return

    if not analysis_attrib.get('dcc_project_code') or not analysis_attrib.get('submitter_donor_id'):
        logger.warning('ignore entry does not have dcc_project_code or submitter_donor_id, GNOS entry: {}'
                         .format(gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull') ))
        return

    donor_unique_id = analysis_attrib.get('dcc_project_code') + '::' + analysis_attrib.get('submitter_donor_id')
    if is_in_donor_blacklist(donor_unique_id):
        logger.warning('ignore blacklisted donor: {} GNOS entry: {}'
                         .format(donor_unique_id, gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull') ))
        return

    if is_test(analysis_attrib, gnos_analysis):
        logger.warning('ignore test entry: {}'.format(gnos_analysis.get('analysis_detail_uri')))
        return # completely ignore test gnos entries for now, this is the quickest way to avoid test interferes real data 

    if (gnos_analysis.get('refassem_short_name') != 'unaligned'
          and not is_train_2_aligned(analysis_attrib, gnos_analysis)
        ):
        # TODO: we may create another ES index for obsoleted BAM entries
        # TODO: we will need more sophisticated check for handling BAMs that are flagged as aligned but
        #       treated as unaligned (this is actually the case for TCGA input BAM entries, maybe need a full
        #       TCGA spciment list from Marc?)
        logger.warning('ignore entry that is aligned but not by train 2 protocol: {}'
                         .format( gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull') ))
        return

    if is_corrupted_train_2_alignment(analysis_attrib, gnos_analysis):
        logger.warning('ignore entry that is aligned by train 2 protocol but seems corrupted: {}'
                         .format( gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull') ))
        return

    #TODO: put things above into one function

    # temporary hack here to skip any BAM entries from odsc-tcga repo as it's supposed not contain
    # any BAM data, but it does, and those aligned BAMs it has overlap with what in CGHub hence causes problems
    if 'osdc-tcga' in gnos_analysis.get('analysis_detail_uri'):
        logger.warning('ignore BAM entry in osdc-tcga repo: {}'
                         .format( gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull') ))
        return

    if not donors.get(donor_unique_id):
        # create a new donor if not exist
        donors[ donor_unique_id ] = create_donor(donor_unique_id, analysis_attrib, gnos_analysis)

    else: # the donor this bam entry belongs to already exists
        # perform some comparison between existing donor and the info in the current bam entry
        if (donors[donor_unique_id].get('gnos_study') != gnos_analysis.get('study')):
            logger.warning( 'existing donor {} has study {}, but study in current gnos ao is {}'.
                            format( donor_unique_id,
                                    donors[donor_unique_id].get('gnos_study'),
                                    gnos_analysis.get('study') ) )
        # more such check may be added, no time for this now

    # now parse out gnos analysis object info to build bam_file doc
    bam_file = create_bam_file_entry(donor_unique_id, analysis_attrib, gnos_analysis)

    if 'normal' in bam_file.get('dcc_specimen_type').lower(): # normal
        if donors.get(donor_unique_id).get('normal_specimen'): # normal specimen exists
            if donors.get(donor_unique_id).get('normal_specimen').get('aliquot_id') == gnos_analysis.get('aliquot_id'):
                if bam_file.get('is_aligned'):
                    if donors.get(donor_unique_id)['normal_specimen'].get('is_aligned'):
                        logger.info('more than one normal aligned bam for donor: {}, entry in use: {}, additional entry found in: {}'
                              .format(donor_unique_id,
                                  donors.get(donor_unique_id).get('normal_specimen').get('gnos_metadata_url'),
                                  gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')
                              )
                        )
                        if (not donors.get(donor_unique_id).get('normal_specimen').get('gnos_metadata_url').split('/')[-1]
                                == gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull').split('/')[-1]):
                            logger.warning('Two aligned BAM entries for the same normal specimen from donor: {} have different GNOS UUIDs: {} and {}'
                                .format(donor_unique_id,
                                    donors.get(donor_unique_id).get('normal_specimen').get('gnos_metadata_url'),
                                    gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')
                                )
                            )
                        if donors.get(donor_unique_id).get('normal_specimen').get('upload_date') < bam_file.get(
                                'upload_date'):  # the current one is newer
                            donors.get(donor_unique_id)['normal_specimen'].update(
                                prepare_aggregated_specimen_level_info(copy.deepcopy(bam_file))
                            )
                            donors.get(donor_unique_id)['gnos_repo'] = bam_file.get('gnos_repo')
                    else:
                        donors.get(donor_unique_id)['normal_specimen'].update(
                            prepare_aggregated_specimen_level_info(copy.deepcopy(bam_file))
                        )
                        donors.get(donor_unique_id)['gnos_repo'] = bam_file.get('gnos_repo')
            else:
                logger.warning('same donor: {} has different aliquot_id: {}, {} for normal specimen, entry in use: {}, additional entry found in {}'
                  .format(donor_unique_id,
                      donors.get(donor_unique_id).get('normal_specimen').get('aliquot_id'),
                      gnos_analysis.get('aliquot_id'),
                      donors.get(donor_unique_id).get('normal_specimen').get('gnos_metadata_url'),
                      gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')
                  )
                )                
        else:
            # add normal_specimen
            donors.get(donor_unique_id)['normal_specimen'].update(
                prepare_aggregated_specimen_level_info(copy.deepcopy(bam_file))
            )
            # update donor's 'gnos_repo' field with normal aligned specimen
            donors.get(donor_unique_id)['gnos_repo'] = bam_file.get('gnos_repo')

    else: # not normal
        donors.get(donor_unique_id).get('all_tumor_specimen_aliquots').add(bam_file.get('aliquot_id'))
        donors.get(donor_unique_id).get('flags')['all_tumor_specimen_aliquot_counts'] = len(donors.get(donor_unique_id).get('all_tumor_specimen_aliquots'))
        if bam_file.get('is_aligned'):
            if donors.get(donor_unique_id).get('aligned_tumor_specimens'):
                if donors.get(donor_unique_id).get('aligned_tumor_specimen_aliquots').intersection(
                        [ bam_file.get('aliquot_id') ]
                    ): # multiple alignments for the same tumor aliquot_id
                    logger.warning('more than one tumor aligned bam for donor: {} with aliquot_id: {}, additional entry found in: {}'
                          .format(donor_unique_id,
                              bam_file.get('aliquot_id'),
                              gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull')
                        )
                    )
                else:
                    donors.get(donor_unique_id).get('aligned_tumor_specimens').append( copy.deepcopy(bam_file) )
                    donors.get(donor_unique_id).get('aligned_tumor_specimen_aliquots').add(bam_file.get('aliquot_id'))
                    donors.get(donor_unique_id).get('flags')['aligned_tumor_specimen_aliquot_counts'] = len(donors.get(donor_unique_id).get('aligned_tumor_specimen_aliquots'))
            else:  # create the first element of the list
                donors.get(donor_unique_id)['aligned_tumor_specimens'] = [copy.deepcopy(bam_file)]
                donors.get(donor_unique_id).get('aligned_tumor_specimen_aliquots').add(bam_file.get('aliquot_id'))  # set of aliquot_id
                donors.get(donor_unique_id).get('flags')['aligned_tumor_specimen_aliquot_counts'] = 1
                donors.get(donor_unique_id).get('flags')['has_aligned_tumor_specimen'] = True

    original_gnos = bam_file['gnos_repo']
    bam_file.update( donors[ donor_unique_id ] )
    bam_file['gnos_repo'] = original_gnos
    del bam_file['bam_files']
    del bam_file['normal_specimen']
    del bam_file['aligned_tumor_specimens']
    del bam_file['aligned_tumor_specimen_aliquots']
    del bam_file['all_tumor_specimen_aliquots']
    del bam_file['flags']
    donors[donor_unique_id]['bam_files'].append( copy.deepcopy(bam_file) )

    # push to Elasticsearch
    # Let's not worry about this index type, it seems not that useful
    #es.index(index=es_index, doc_type='bam_file', id=bam_file['bam_gnos_ao_id'], body=json.loads( json.dumps(bam_file, default=set_default) ), timeout=90)
    bam_output_fh.write(json.dumps(bam_file, default=set_default) + '\n')


def create_vcf_entry(analysis_attrib, gnos_analysis):
    vcf_entry = {
        #'analysis_attrib': analysis_attrib, # remove this later
        #'gnos_analysis': gnos_analysis, # remove this later
        "gnos_id": gnos_analysis.get('analysis_id'),
        "gnos_repo": [gnos_analysis.get('analysis_detail_uri').split('/cghub/')[0] + '/'],
        "gnos_last_modified": [dateutil.parser.parse(gnos_analysis.get('last_modified'))],
        "files": [],
        "study": gnos_analysis.get('study'),
        "variant_calling_performed_at": gnos_analysis.get('analysis_xml').get('ANALYSIS_SET').get('ANALYSIS').get('@center_name'),
        "workflow_details": {
            "variant_workflow_name": analysis_attrib.get('variant_workflow_name'),
            "variant_workflow_version": analysis_attrib.get('variant_workflow_version'),
            "variant_pipeline_input_info": json.loads( analysis_attrib.get('variant_pipeline_input_info') ).get('workflow_inputs') if analysis_attrib.get('variant_pipeline_input_info') else [],
            "variant_pipeline_output_info": json.loads( analysis_attrib.get('variant_pipeline_output_info') ).get('workflow_outputs') if analysis_attrib.get('variant_pipeline_output_info') else [],
            "variant_qc_metrics": json.loads( analysis_attrib.get('variant_qc_metrics') ).get('qc_metrics') if analysis_attrib.get('variant_qc_metrics') else [],
            "variant_timing_metrics": json.loads( analysis_attrib.get('variant_timing_metrics') ).get('timing_metrics') if analysis_attrib.get('variant_timing_metrics') else [],
        }
    }

    #print json.dumps(vcf_entry)  # debugging only
    return vcf_entry


def set_default(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


def prepare_aggregated_specimen_level_info(bam_file):
    specimen = copy.deepcopy(bam_file)
    # TODO: actual aggregation to be completed
    return specimen


def is_in_donor_blacklist(donor_unique_id):
    donor_blacklist = set([
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


def create_bam_file_entry(donor_unique_id, analysis_attrib, gnos_analysis):
    file_info = parse_bam_file_info(gnos_analysis.get('files').get('file'))
    bam_file = {
        "dcc_specimen_type": analysis_attrib.get('dcc_specimen_type'),
        "submitter_specimen_id": analysis_attrib.get('submitter_specimen_id'),
        "submitter_sample_id": analysis_attrib.get('submitter_sample_id'),
        "aliquot_id": gnos_analysis.get('aliquot_id'),
        "use_cntl": analysis_attrib.get('use_cntl'),
        "total_lanes": analysis_attrib.get('total_lanes'),

        "gnos_repo": gnos_analysis.get('analysis_detail_uri').split('/cghub/')[0] + '/',
        "gnos_metadata_url": gnos_analysis.get('analysis_detail_uri').replace('analysisDetail', 'analysisFull'),
        "refassem_short_name": gnos_analysis.get('refassem_short_name'),
        "bam_gnos_ao_id": gnos_analysis.get('analysis_id'),
        "upload_date": dateutil.parser.parse(gnos_analysis.get('upload_date')),
        "published_date": dateutil.parser.parse(gnos_analysis.get('published_date')),
        "last_modified": dateutil.parser.parse(gnos_analysis.get('last_modified')),

        "bam_file_name": file_info.get('file_name'),
        "bam_file_size": file_info.get('file_size'),
        "md5sum": file_info.get('md5sum'),
    }

    # much more TODO for bam file info and alignment details
    if bam_file.get('refassem_short_name') == 'unaligned':
        bam_file['is_aligned'] = False
        bam_file['bam_type'] = 'Unaligned BAM'
        bam_file['alignment'] = None  # or initiate as empty object {}, depending on how ES searches it
    elif (analysis_attrib.get('workflow_output_bam_contents') == 'unaligned'
            or gnos_analysis['analysis_xml']['ANALYSIS_SET']['ANALYSIS']['DESCRIPTION'].startswith('The BAM file includes unmapped reads extracted from specimen-level BAM with the reference alignment')
         ): # this is actually BAM with unmapped reads
        bam_file['is_aligned'] = False
        bam_file['bam_type'] = 'Specimen level unmapped reads after BWA alignment'
        bam_file['alignment'] = None
    elif gnos_analysis['analysis_xml']['ANALYSIS_SET']['ANALYSIS']['DESCRIPTION'].startswith('Specimen-level BAM from the reference alignment'):
        bam_file['is_aligned'] = True
        bam_file['bam_type'] = 'Specimen level aligned BAM'
        bam_file['alignment'] = get_alignment_detail(analysis_attrib, gnos_analysis)
    else:
        bam_file['is_aligned'] = False
        bam_file['bam_type'] = 'Unknown'
        bam_file['alignment'] = None

    return bam_file


def get_alignment_detail(analysis_attrib, gnos_analysis):
    alignment = {
        "data_train": "Train 2",
        "workflow_name": analysis_attrib.get('workflow_name'),
        "workflow_version": analysis_attrib.get('workflow_version'),
        "workflow_bundle_url": analysis_attrib.get('workflow_bundle_url'),
        "workflow_source_url": analysis_attrib.get('workflow_source_url'),

        "pipeline_input_info": json.loads( analysis_attrib.get('pipeline_input_info') ).get('pipeline_input_info') if analysis_attrib.get('pipeline_input_info') else [],
        "qc_metrics": json.loads( analysis_attrib.get('qc_metrics').replace('"not_collected"', 'null') ).get('qc_metrics') if analysis_attrib.get('qc_metrics') else [],
        "markduplicates_metrics": json.loads( analysis_attrib.get('markduplicates_metrics') ).get('markduplicates_metrics') if analysis_attrib.get('markduplicates_metrics') else [],
        "timing_metrics": json.loads( analysis_attrib.get('timing_metrics').replace('"not_collected"', 'null') ).get('timing_metrics') if analysis_attrib.get('timing_metrics') else [],
    }

    alignment['input_bam_summary'] = {} # TODO: do this in a function
    
    return alignment


def parse_bam_file_info(file_fragment):
    file_info = {}
    if (type(file_fragment) != list): file_fragment = [file_fragment]

    for f in file_fragment:
        f = dict(f)
        if f.get('filename').endswith('.bam'): # assume there is only one BAM file
            file_info['file_name'] = f.get('filename')
            file_info['file_size'] = int(f.get('filesize'))
            file_info['md5sum'] = f.get('checksum').get('#text')

    return file_info


def is_train_2_aligned(analysis_attrib, gnos_analysis):
    if ( gnos_analysis.get('refassem_short_name') == 'GRCh37'
           and analysis_attrib.get('workflow_version')
           and analysis_attrib.get('workflow_version').startswith('2.6.')
       ):
        return True
    else:
        return False


def is_corrupted_train_2_alignment(analysis_attrib, gnos_analysis):
    if ( is_train_2_aligned(analysis_attrib, gnos_analysis)
           and not gnos_analysis['analysis_xml']['ANALYSIS_SET']['ANALYSIS']['DESCRIPTION'].startswith('The BAM file includes unmapped reads extracted from specimen-level BAM with the reference alignment')
           and (not analysis_attrib.get('qc_metrics') or not analysis_attrib.get('markduplicates_metrics'))
       ):
        return True
    else:
        return False


def create_donor(donor_unique_id, analysis_attrib, gnos_analysis):
    donor = {
        'donor_unique_id': donor_unique_id,
        'submitter_donor_id': analysis_attrib['submitter_donor_id'],
        'dcc_project_code': analysis_attrib['dcc_project_code'],
        'gnos_study': gnos_analysis.get('study'),
        'gnos_repo': gnos_analysis.get('analysis_detail_uri').split('/cghub/')[0] + '/', # can be better
        'flags': {
            'is_test': is_test(analysis_attrib, gnos_analysis),
            'is_cell_line': is_cell_line(analysis_attrib, gnos_analysis),
            'is_train2_donor': False,
            'is_train2_pilot': False,
            'is_normal_specimen_aligned': False,
            'are_all_tumor_specimens_aligned': False,
            'has_aligned_tumor_specimen': False,
            'aligned_tumor_specimen_aliquot_counts': 0,
            'all_tumor_specimen_aliquot_counts': 0,
            'is_sanger_variant_calling_performed': False,
            'variant_calling_performed': [],
            'vcf_in_jamboree': []
        },
        'normal_specimen': {},
        'aligned_tumor_specimens': [],
        'aligned_tumor_specimen_aliquots': set(),
        'all_tumor_specimen_aliquots': set(),
        'bam_files': [],
    }
    try:
        if type(gnos_analysis.get('experiment_xml').get('EXPERIMENT_SET').get('EXPERIMENT')) == list:
            donor['sequencing_center'] = gnos_analysis.get('experiment_xml').get('EXPERIMENT_SET').get('EXPERIMENT')[0].get('@center_name')
        else:
            donor['sequencing_center'] = gnos_analysis.get('experiment_xml').get('EXPERIMENT_SET').get('EXPERIMENT').get('@center_name')
    except:
        logger.warning('analysis object has no sequencing_center information: {}'.format(gnos_analysis.get('analysis_detail_uri')))

    return donor

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


def is_cell_line(analysis_attrib, gnos_analysis):
    is_cell_line = False
    if analysis_attrib.get('dcc_project_code') == 'TCGA_MUT_BENCHMARK_4':
        is_cell_line = True

    return is_cell_line


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


def get_gnos_analysis(f):
    with open (f, 'r') as x:
        xml_str = x.read()
    return xmltodict.parse(xml_str).get('ResultSet').get('Result')


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


def process(metadata_dir, conf, es_index, es, donor_output_jsonl_file, bam_output_jsonl_file, repo, exclude_gnos_id_lists):
    donors = {}
    vcf_entries = {}

    # update the pc_annotation-sanger_vcf_in_jamboree files using the jamboree subdirectory files
    vcf_in_jamboree_dir = '../pcawg-operations/variant_calling/sanger_workflow/jamboree/'
    
    infiles = glob.glob(vcf_in_jamboree_dir+'/Sanger_jamboree_batch*.txt')
    outfile = 'pc_annotation-sanger_vcf_in_jamboree.tsv' # hard-code file name
    update_vcf_jamboree(infiles, outfile)    

    annotations = {}
    read_annotations(annotations, 'gnos_assignment', 'pc_annotation-gnos_assignment.yml')  # hard-code file name for now
    read_annotations(annotations, 'train2_donors', 'pc_annotation-train2_donors.tsv')  # hard-code file name for now
    read_annotations(annotations, 'train2_pilot', 'pc_annotation-train2_pilot.tsv')  # hard-code file name for now
    read_annotations(annotations, 'donor_blacklist', 'pc_annotation-donor_blacklist.tsv')  # hard-code file name for now
    read_annotations(annotations, 'manual_qc_failed', 'pc_annotation-manual_qc_failed.tsv')  # hard-code file name for now
    read_annotations(annotations, 'sanger_vcf_in_jamboree', 'pc_annotation-sanger_vcf_in_jamboree.tsv')  # hard-code file name for now

    # hard-code the file name for now    
    train2_freeze_bams = read_train2_bams('../pcawg-operations/variant_calling/train2-lists/Data_Freeze_Train_2.0_GoogleDocs__2015_03_26_1637.tsv')

    # pre-exclude gnos entries when this option is chosen
    gnos_ids_to_be_excluded = set()
    if exclude_gnos_id_lists:
        files = glob.glob(exclude_gnos_id_lists)
        for fname in files:
            with open(fname) as f:
                for d in f: gnos_ids_to_be_excluded.add(d.rstrip())

    donor_fh = open(donor_output_jsonl_file, 'w')
    bam_fh = open(bam_output_jsonl_file, 'w')
    
    for f in get_xml_files( metadata_dir, conf, repo ):
        f = conf.get('output_dir') + '/__all_metadata_xml/' + f
        gnos_analysis = get_gnos_analysis(f)
        #print (json.dumps(gnos_analysis)) # debug
        if gnos_analysis:
            logger.info( 'processing xml file: {} ...'.format(f) )
            if gnos_analysis.get('analysis_id') and gnos_analysis.get('analysis_id') in gnos_ids_to_be_excluded:
                logger.warning( 'skipping xml file: {} with analysis_id: {}, as it\'s in the list to be excluded' \
                    .format(f, gnos_analysis.get('analysis_id')) )
                continue

            process_gnos_analysis( gnos_analysis, donors, vcf_entries, es_index, es, bam_fh, annotations )
        else:
            logger.warning( 'skipping invalid xml file: {}'.format(f) )

    for donor_id in donors.keys():
        donor = donors[donor_id]

        process_donor(donor, annotations, vcf_entries, conf, train2_freeze_bams)

        # push to Elasticsearch
        es.index(index=es_index, doc_type='donor', id=donor['donor_unique_id'], \
            body=json.loads(json.dumps(donor, default=set_default)), timeout=90 )
        del donor['bam_files']  # prune this before dumping JSON for Keiran
        donor_fh.write(json.dumps(donor, default=set_default) + '\n')

    donor_fh.close()
    bam_fh.close()


def update_vcf_jamboree(infilenames, outfilename):
    seen = set() # just for checking in case there are duplicated lines in jamboree files

    with open(outfilename, 'w') as fout:
        for f_index in infilenames:
            with open(f_index,'r') as fin:
                for line in fin:
                    if len(line.rstrip()) == 0: continue
                    if line in seen:
                        pass
                    else:
                        donor_unique_id, gnos_metadata_url, aliquot_id = str.split(line.rstrip(), '\t')
                        repo, gnos_id = str.split(gnos_metadata_url, 'cghub/metadata/analysisFull/')
                        fout. write(donor_unique_id+'\t'+gnos_id+'\n')
                        seen.add(line)



def read_train2_bams(filename):
    train2_bams = {}

    with open(filename, 'r') as r:
        for line in r:
            if line.startswith('dcc_project_code'): continue
            if len(line.rstrip()) == 0: continue
            dcc_project_code, donor_submitter_id, normal_aligned_bam_gnos_url,\
                num_tumor_samples, tumour_aligned_bam_gnos_url = str.split(line.rstrip(), '\t')

            normal_repo, normal_gnos_id = str.split(normal_aligned_bam_gnos_url, 'cghub/metadata/analysisFull/')

            train2_bams[dcc_project_code + "::" + donor_submitter_id] = {}
            train2_bams.get(dcc_project_code + "::" + donor_submitter_id)[normal_gnos_id] = \
                {"repo": normal_repo, "specimen_type": "normal"}

            for tumor_url in str.split(tumour_aligned_bam_gnos_url, ','):
                tumor_repo, tumor_gnos_id = str.split(tumor_url, 'cghub/metadata/analysisFull/')
                train2_bams.get(dcc_project_code + "::" + donor_submitter_id)[tumor_gnos_id] = \
                    {"repo": tumor_repo, "specimen_type": "tumor"}

    return train2_bams


def read_annotations(annotations, type, file_name):
    with open(file_name, 'r') as r:
        if annotations.get(type): # reset annotation if exists
            del annotations[type]

        if type == 'gnos_assignment':
            annotations['gnos_assignment'] = {}
            assignment = yaml.safe_load(r)
            for repo, project_donors in assignment.iteritems():
                for p_d in project_donors:
                    annotations['gnos_assignment'][p_d] = repo  # key is project or donor unique id, value is repo

        elif type == 'sanger_vcf_in_jamboree':
            annotations['sanger_vcf_in_jamboree'] = {}
            for line in r:
                if line.startswith('#'): continue
                if len(line.rstrip()) == 0: continue
                donor_id, ao_id = str.split(line.rstrip(), '\t')
                annotations[type][donor_id] = ao_id
                
        elif type in ['train2_donors', 'train2_pilot', 'donor_blacklist', 'manual_qc_failed']:
            annotations[type] = set()
            for line in r:
                if line.startswith('#'): continue
                if len(line.rstrip()) == 0: continue
                annotations[type].add(line.rstrip())

        else:
            logger.warning('unknown annotation type: {}'.format(type))


def process_donor(donor, annotations, vcf_entries, conf, train2_freeze_bams):
    logger.info( 'processing donor: {} ...'.format(donor.get('donor_unique_id')) )

    # check whether all tumor specimen(s) aligned
    if (donor.get('flags').get('aligned_tumor_specimen_aliquot_counts') 
            and donor.get('flags').get('aligned_tumor_specimen_aliquot_counts') == donor.get('flags').get('all_tumor_specimen_aliquot_counts')):
        donor.get('flags')['are_all_tumor_specimens_aligned'] = True

    # now build easy-to-use, specimen-level, gnos_repo-aware summary of bwa alignment status by iterating all collected bams
    aggregated_bam_info = bam_aggregation(donor['bam_files'])
    #print (json.dumps(aggregated_bam_info, default=set_default))  # debug only

    # let's add this aggregated alignment information to donor object
    add_alignment_status_to_donor(donor, aggregated_bam_info)
    #print json.dumps(donor.get('tumor_alignment_status'), default=set_default)  # debug only

    if donor.get('normal_alignment_status') and donor.get('normal_alignment_status').get('aligned'):
        donor.get('flags')['is_normal_specimen_aligned'] = True
    
    # add gnos repos where complete alignments for the current donor are available
    add_gnos_repos_with_complete_alignment_set(donor)

    # add gnos repos where one alignment or all alignments for the current donor are available
    add_gnos_repos_with_alignment_result(donor)

    # add original gnos repo assignment, this is based on a manually maintained yaml file
    add_original_gnos_repo(donor, annotations['gnos_assignment'])
    if donor.get('flags').get('is_normal_specimen_aligned') and not donor.get('original_gnos_assignment'):
        logger.warning('donor with normal aligned but gnos_for_originally_aligned_at is empty, please update gnos assignment annotation for donor: {} with {}'
            .format(donor.get('donor_unique_id'), conf.get(donor.get('normal_alignment_status').get('aligned_bam').get('gnos_repo')[0])))
        # it should be pretty safe to assign it automatically for this freshly aligned normal specimen
        donor['original_gnos_assignment'] = conf.get(donor.get('normal_alignment_status').get('aligned_bam').get('gnos_repo')[0])
    add_train2_donor_flag(donor, annotations['train2_donors'])
    add_train2_pilot_flag(donor, annotations['train2_pilot'])
    add_donor_blacklist_flag(donor, annotations['donor_blacklist'])
    add_manual_qc_failed_flag(donor, annotations['manual_qc_failed'])
    
    donor.get('flags')['is_sanger_vcf_in_jamboree'] = False
    if donor.get('donor_unique_id') in annotations.get('sanger_vcf_in_jamboree'):
        donor.get('flags')['is_sanger_vcf_in_jamboree'] = True
        donor.get('flags').get('vcf_in_jamboree').append('sanger')

    add_vcf_entry(donor, vcf_entries.get(donor.get('donor_unique_id')))

    check_bwa_duplicates(donor, train2_freeze_bams)


def check_bwa_duplicates(donor, train2_freeze_bams):
    duplicated_bwa_alignment_summary = {
        'exists_mismatch_bwa_bams': False,
        'exists_mismatch_bwa_bams_in_normal': False,
        'exists_mismatch_bwa_bams_in_tumor': False,
        'exists_gnos_id_mismatch': False,
        'exists_gnos_id_mismatch_in_normal': False,
        'exists_gnos_id_mismatch_in_tumor': False,
        'exists_md5sum_mismatch': False,
        'exists_md5sum_mismatch_in_normal': False,
        'exists_md5sum_mismatch_in_tumor': False,
        'exists_version_mismatch': False,
        'exists_version_mismatch_in_normal': False,
        'exists_version_mismatch_in_tumor': False,
        'exists_md5sum_mismatch_between_train2_marked_and_sanger_used': False,
        'exists_version_mismatch_between_train2_marked_and_sanger_used': False,
        'is_train2_freeze_bam_missing': False,
        'is_train2_freeze_normal_bam_missing': False,
        'is_train2_freeze_tumor_bam_missing': False,
        'is_bam_used_by_sanger_missing': False,
        'is_normal_bam_used_by_sanger_missing': False,
        'is_tumor_bam_used_by_sanger_missing': False,
        'normal': {},
        '_tmp_tumor': {},
        'tumor': []
    }
    aliquots = {}
    duplicated_bwa = False

    for bam_file in donor.get('bam_files'):
        if not bam_file.get('is_aligned'): continue

        if aliquots.get(bam_file.get('aliquot_id')): # exists already
            duplicated_bwa = True
            aliquots.get(bam_file.get('aliquot_id')).append(bam_file)
        else:
            aliquots[bam_file.get('aliquot_id')] = [bam_file]

    if True or duplicated_bwa:  # Let's do this for all donors
        for aliquot in aliquots:
          for bam_file in aliquots.get(aliquot):
            if 'normal' in bam_file.get('dcc_specimen_type').lower():
                if duplicated_bwa_alignment_summary.get('normal'):
                    duplicated_bwa_alignment_summary.get('normal').get('aligned_bam').append(
                            {
                                'gnos_id': bam_file.get('bam_gnos_ao_id'),
                                'gnos_repo': bam_file.get('gnos_repo'),
                                'md5sum': bam_file.get('md5sum'),
                                'upload_date': bam_file.get('upload_date'),
                                'published_date': bam_file.get('published_date'),
                                'last_modified': bam_file.get('last_modified'),
                                'bwa_workflow_version': bam_file.get('alignment').get('workflow_version'),
                                'is_train2_bam': is_train2_bam(donor, train2_freeze_bams, bam_file.get('bam_gnos_ao_id'), 'normal'),
                                'is_used_in_sanger_variant_call': is_used_in_sanger_variant_call(donor,
                                        bam_file.get('bam_gnos_ao_id'))
                            }
                        )
                else:
                    duplicated_bwa_alignment_summary['normal'] = {
                        'aliquot_id': aliquot,
                        'dcc_specimen_type': bam_file.get('dcc_specimen_type'),
                        'aligned_bam': [
                            {
                                'gnos_id': bam_file.get('bam_gnos_ao_id'),
                                'gnos_repo': bam_file.get('gnos_repo'),
                                'md5sum': bam_file.get('md5sum'),
                                'upload_date': bam_file.get('upload_date'),
                                'published_date': bam_file.get('published_date'),
                                'last_modified': bam_file.get('last_modified'),
                                'bwa_workflow_version': bam_file.get('alignment').get('workflow_version'),
                                'is_train2_bam': is_train2_bam(donor, train2_freeze_bams, bam_file.get('bam_gnos_ao_id'), 'normal'),
                                'is_used_in_sanger_variant_call': is_used_in_sanger_variant_call(donor,
                                        bam_file.get('bam_gnos_ao_id'))
                            }
                        ]
                    }

            else: # tumor
                if not duplicated_bwa_alignment_summary.get('_tmp_tumor').get(aliquot):
                    duplicated_bwa_alignment_summary.get('_tmp_tumor')[aliquot] = {
                        'aliquot_id': aliquot,
                        'dcc_specimen_type': bam_file.get('dcc_specimen_type'),
                        'aligned_bam': []
                    }

                duplicated_bwa_alignment_summary.get('_tmp_tumor').get(aliquot).get('aligned_bam').append(
                        {
                            'gnos_id': bam_file.get('bam_gnos_ao_id'),
                            'gnos_repo': bam_file.get('gnos_repo'),
                            'md5sum': bam_file.get('md5sum'),
                            'upload_date': bam_file.get('upload_date'),
                            'published_date': bam_file.get('published_date'),
                            'last_modified': bam_file.get('last_modified'),
                            'bwa_workflow_version': bam_file.get('alignment').get('workflow_version'),
                            'is_train2_bam': is_train2_bam(donor, train2_freeze_bams, bam_file.get('bam_gnos_ao_id'), 'tumor'),
                            'is_used_in_sanger_variant_call': is_used_in_sanger_variant_call(donor,
                                    bam_file.get('bam_gnos_ao_id'))
                        }
                    )

        for aliquot in duplicated_bwa_alignment_summary.get('_tmp_tumor'):
            duplicated_bwa_alignment_summary.get('tumor').append(duplicated_bwa_alignment_summary.get('_tmp_tumor').get(aliquot))

        del duplicated_bwa_alignment_summary['_tmp_tumor']

        # scan normal BAMs
        if duplicated_bwa_alignment_summary.get('normal'):
            b_gnos_id = None
            b_md5sum = None
            b_version = None
            has_train2_n_bam = False
            has_sanger_n_bam = False
            count_is_train2_not_sanger = 0
            count_not_train2_is_sanger = 0
            count_is_train2_is_sanger = 0
            duplicated_bwa_alignment_summary.get('normal')['exists_mismatch_bwa_bams'] = False
            duplicated_bwa_alignment_summary.get('normal')['exists_gnos_id_mismatch'] = False
            duplicated_bwa_alignment_summary.get('normal')['exists_md5sum_mismatch'] = False
            duplicated_bwa_alignment_summary.get('normal')['exists_version_mismatch'] = False

            for bam in duplicated_bwa_alignment_summary.get('normal').get('aligned_bam'):
                is_train2_n_bam = bam.get('is_train2_bam')
                if is_train2_n_bam: has_train2_n_bam = True
                is_sanger_n_bam = bam.get('is_used_in_sanger_variant_call')
                if is_sanger_n_bam: has_sanger_n_bam = True

                if is_train2_n_bam and not is_sanger_n_bam: count_is_train2_not_sanger += 1
                if not is_train2_n_bam and is_sanger_n_bam: count_not_train2_is_sanger += 1
                if is_train2_n_bam and is_sanger_n_bam: count_is_train2_is_sanger += 1

                if not b_gnos_id: b_gnos_id = bam.get('gnos_id')
                if b_gnos_id and not b_gnos_id == bam.get('gnos_id'):
                    duplicated_bwa_alignment_summary['exists_gnos_id_mismatch'] = True
                    duplicated_bwa_alignment_summary['exists_gnos_id_mismatch_in_normal'] = True
                    duplicated_bwa_alignment_summary['exists_mismatch_bwa_bams'] = True
                    duplicated_bwa_alignment_summary['exists_mismatch_bwa_bams_in_normal'] = True

                    duplicated_bwa_alignment_summary.get('normal')['exists_mismatch_bwa_bams'] = True
                    duplicated_bwa_alignment_summary.get('normal')['exists_gnos_id_mismatch'] = True

                if not b_md5sum: b_md5sum = bam.get('md5sum')
                if b_md5sum and not b_md5sum == bam.get('md5sum'):
                    duplicated_bwa_alignment_summary['exists_md5sum_mismatch'] = True
                    duplicated_bwa_alignment_summary['exists_md5sum_mismatch_in_normal'] = True
                    duplicated_bwa_alignment_summary['exists_mismatch_bwa_bams'] = True
                    duplicated_bwa_alignment_summary['exists_mismatch_bwa_bams_in_normal'] = True

                    duplicated_bwa_alignment_summary.get('normal')['exists_mismatch_bwa_bams'] = True
                    duplicated_bwa_alignment_summary.get('normal')['exists_md5sum_mismatch'] = True

                if not b_version: b_version = bam.get('bwa_workflow_version')
                if b_version and not b_version == bam.get('bwa_workflow_version'):
                    duplicated_bwa_alignment_summary['exists_version_mismatch'] = True
                    duplicated_bwa_alignment_summary['exists_version_mismatch_in_normal'] = True
                    duplicated_bwa_alignment_summary['exists_mismatch_bwa_bams'] = True
                    duplicated_bwa_alignment_summary['exists_mismatch_bwa_bams_in_normal'] = True

                    duplicated_bwa_alignment_summary.get('normal')['exists_mismatch_bwa_bams'] = True
                    duplicated_bwa_alignment_summary.get('normal')['exists_version_mismatch'] = True

            if donor.get('flags').get('is_train2_donor') and not has_train2_n_bam:
                duplicated_bwa_alignment_summary['is_train2_freeze_bam_missing'] = True
                duplicated_bwa_alignment_summary['is_train2_freeze_normal_bam_missing'] = True

            if donor.get('flags').get('is_sanger_variant_calling_performed') and not has_sanger_n_bam:
                duplicated_bwa_alignment_summary['is_bam_used_by_sanger_missing'] = True
                duplicated_bwa_alignment_summary['is_normal_bam_used_by_sanger_missing'] = True

            if donor.get('flags').get('is_train2_donor') and \
                    donor.get('flags').get('is_sanger_variant_calling_performed') and \
                    not count_is_train2_is_sanger and \
                    count_is_train2_not_sanger and count_not_train2_is_sanger:
                if duplicated_bwa_alignment_summary['exists_md5sum_mismatch']:
                    duplicated_bwa_alignment_summary['exists_md5sum_mismatch_between_train2_marked_and_sanger_used'] = True
                if duplicated_bwa_alignment_summary['exists_version_mismatch']:
                    duplicated_bwa_alignment_summary['exists_version_mismatch_between_train2_marked_and_sanger_used'] = True

        # scan tumor BAMs
        if duplicated_bwa_alignment_summary.get('tumor'):
            for aliquot in duplicated_bwa_alignment_summary.get('tumor'):
                b_gnos_id = None
                b_md5sum = None
                b_version = None
                has_train2_t_bam = False
                has_sanger_t_bam = False
                count_is_train2_not_sanger = 0
                count_not_train2_is_sanger = 0
                count_is_train2_is_sanger = 0
                aliquot['exists_mismatch_bwa_bams'] = False
                aliquot['exists_gnos_id_mismatch'] = False
                aliquot['exists_md5sum_mismatch'] = False
                aliquot['exists_version_mismatch'] = False

                for bam in aliquot.get('aligned_bam'):
                    is_train2_t_bam = bam.get('is_train2_bam')
                    if is_train2_t_bam: has_train2_t_bam = True
                    is_sanger_t_bam = bam.get('is_used_in_sanger_variant_call')
                    if is_sanger_t_bam: has_sanger_t_bam = True

                    if is_train2_t_bam and not is_sanger_t_bam: count_is_train2_not_sanger += 1
                    if not is_train2_t_bam and is_sanger_t_bam: count_not_train2_is_sanger += 1
                    if is_train2_t_bam and is_sanger_t_bam: count_is_train2_is_sanger += 1

                    if not b_gnos_id: b_gnos_id = bam.get('gnos_id')
                    if b_gnos_id and not b_gnos_id == bam.get('gnos_id'):
                        duplicated_bwa_alignment_summary['exists_gnos_id_mismatch'] = True
                        duplicated_bwa_alignment_summary['exists_gnos_id_mismatch_in_tumor'] = True
                        duplicated_bwa_alignment_summary['exists_mismatch_bwa_bams'] = True
                        duplicated_bwa_alignment_summary['exists_mismatch_bwa_bams_in_tumor'] = True

                        aliquot['exists_mismatch_bwa_bams'] = True
                        aliquot['exists_gnos_id_mismatch'] = True

                    if not b_md5sum: b_md5sum = bam.get('md5sum')
                    if b_md5sum and not b_md5sum == bam.get('md5sum'):
                        duplicated_bwa_alignment_summary['exists_md5sum_mismatch'] = True
                        duplicated_bwa_alignment_summary['exists_md5sum_mismatch_in_tumor'] = True
                        duplicated_bwa_alignment_summary['exists_mismatch_bwa_bams'] = True
                        duplicated_bwa_alignment_summary['exists_mismatch_bwa_bams_in_tumor'] = True

                        aliquot['exists_mismatch_bwa_bams'] = True
                        aliquot['exists_md5sum_mismatch'] = True

                    if not b_version: b_version = bam.get('bwa_workflow_version')
                    if b_version and not b_version == bam.get('bwa_workflow_version'):
                        duplicated_bwa_alignment_summary['exists_version_mismatch'] = True
                        duplicated_bwa_alignment_summary['exists_version_mismatch_in_tumor'] = True
                        duplicated_bwa_alignment_summary['exists_mismatch_bwa_bams'] = True
                        duplicated_bwa_alignment_summary['exists_mismatch_bwa_bams_in_tumor'] = True

                        aliquot['exists_version_mismatch'] = True
                        aliquot['exists_mismatch_bwa_bams'] = True

                if donor.get('flags').get('is_train2_donor') and not has_train2_t_bam:
                    duplicated_bwa_alignment_summary['is_train2_freeze_bam_missing'] = True
                    duplicated_bwa_alignment_summary['is_train2_freeze_tumor_bam_missing'] = True

                if donor.get('flags').get('is_sanger_variant_calling_performed') and not has_sanger_t_bam:
                    duplicated_bwa_alignment_summary['is_bam_used_by_sanger_missing'] = True
                    duplicated_bwa_alignment_summary['is_tumor_bam_used_by_sanger_missing'] = True

                if donor.get('flags').get('is_train2_donor') and \
                        donor.get('flags').get('is_sanger_variant_calling_performed') and \
                        not count_is_train2_is_sanger and \
                        count_is_train2_not_sanger and count_not_train2_is_sanger:
                    if duplicated_bwa_alignment_summary['exists_md5sum_mismatch']:
                        duplicated_bwa_alignment_summary['exists_md5sum_mismatch_between_train2_marked_and_sanger_used'] = True
                    if duplicated_bwa_alignment_summary['exists_version_mismatch']:
                        duplicated_bwa_alignment_summary['exists_version_mismatch_between_train2_marked_and_sanger_used'] = True

        donor['duplicated_bwa_alignment_summary'] = duplicated_bwa_alignment_summary


def is_used_in_sanger_variant_call(donor, gnos_id):
    if donor.get('variant_calling_results') and donor.get('variant_calling_results').get('sanger_variant_calling'):
        for input_gnos_entry in donor.get('variant_calling_results').get('sanger_variant_calling') \
                .get('workflow_details').get('variant_pipeline_input_info'):
            if gnos_id == input_gnos_entry.get('attributes').get('analysis_id'): return True

    return False


def is_train2_bam(donor, train2_freeze_bams, gnos_id, specimen_type):
    if donor.get('donor_unique_id') and train2_freeze_bams.get(donor.get('donor_unique_id')) \
            and train2_freeze_bams.get(donor.get('donor_unique_id')).get(gnos_id):
        if not specimen_type == train2_freeze_bams.get(donor.get('donor_unique_id')).get(gnos_id).get('specimen_type'):
            logger.warning('This should never happen: specimen type mismatch in train2 list in donor {}'
                    .format(donor.get('donor_unique_id')))
        return True
    return False


def add_vcf_entry(donor, vcf_entry):
    if not vcf_entry:
        return
    else:
        if not donor.get('flags').get('all_tumor_specimen_aliquot_counts') + 1 == \
            len(vcf_entry.get('sanger_variant_calling').get('workflow_details').get('variant_pipeline_output_info')):
            # not to add this variant call result since it missed tumor specimen(s)
            logger.warning('sanger variant calling workflow may have missed tumour specimen for donor: {}, ignore variant calling result.'
                    .format(donor.get('donor_unique_id')))
            return

    if not donor.get('variant_calling_results'):
        donor['variant_calling_results'] = {}
    donor.get('variant_calling_results').update(vcf_entry)

    if donor.get('variant_calling_results').get('sanger_variant_calling'):
        donor.get('flags')['is_sanger_variant_calling_performed'] = True
        donor.get('flags').get('variant_calling_performed').append('sanger')
        if not donor.get('flags').get('all_tumor_specimen_aliquot_counts') + 1 == \
                len(donor.get('variant_calling_results').get('sanger_variant_calling').get('workflow_details').get('variant_pipeline_output_info')):
            logger.warning('sanger variant calling workflow may have missed tumour specimen for donor: {}'
                    .format(donor.get('donor_unique_id')))
            donor.get('variant_calling_results').get('sanger_variant_calling')['is_output_and_tumour_specimen_counts_mismatch'] = True
        else:
            donor.get('variant_calling_results').get('sanger_variant_calling')['is_output_and_tumour_specimen_counts_mismatch'] = False


def add_original_gnos_repo(donor, annotation):
    if donor.get('gnos_repo'):
        del donor['gnos_repo']  # get rid of this rather confusing old flag

    if annotation.get(donor.get('donor_unique_id')):
        donor['original_gnos_assignment'] = annotation.get(donor.get('donor_unique_id'))
    elif annotation.get(donor.get('dcc_project_code')):
        donor['original_gnos_assignment'] = annotation.get(donor.get('dcc_project_code'))
    else:
        donor['original_gnos_assignment'] = None


def add_train2_donor_flag(donor, annotation):
    if donor.get('donor_unique_id') in annotation:
        donor.get('flags')['is_train2_donor'] = True
    else:
        donor.get('flags')['is_train2_donor'] = False


def add_train2_pilot_flag(donor, annotation):
    if donor.get('donor_unique_id') in annotation:
        donor.get('flags')['is_train2_pilot'] = True
    else:
        donor.get('flags')['is_train2_pilot'] = False


def add_donor_blacklist_flag(donor, annotation):
    if donor.get('donor_unique_id') in annotation:
        donor.get('flags')['is_donor_blacklisted'] = True
    else:
        donor.get('flags')['is_donor_blacklisted'] = False


def add_manual_qc_failed_flag(donor, annotation):
    if donor.get('donor_unique_id') in annotation:
        donor.get('flags')['is_manual_qc_failed'] = True
    else:
        donor.get('flags')['is_manual_qc_failed'] = False

def add_gnos_repos_with_alignment_result(donor):
    repos = set()

    if (donor.get('normal_alignment_status')
            and donor.get('normal_alignment_status').get('aligned_bam')):
        repos = set(donor.get('normal_alignment_status').get('aligned_bam').get('gnos_repo'))

    if donor.get('tumor_alignment_status'):
        for t in donor.get('tumor_alignment_status'):
            if t.get('aligned_bam'):
                repos.update(set(t.get('aligned_bam').get('gnos_repo')))

    donor['gnos_repos_with_alignment_result'] = repos


def add_gnos_repos_with_complete_alignment_set(donor):
    repos = set()

    if (donor.get('normal_alignment_status')
            and donor.get('normal_alignment_status').get('aligned_bam')):
        repos = set(donor.get('normal_alignment_status').get('aligned_bam').get('gnos_repo'))

    if repos and donor.get('tumor_alignment_status'):
        for t in donor.get('tumor_alignment_status'):
            if t.get('aligned_bam'):
                repos = set.intersection(repos, set(t.get('aligned_bam').get('gnos_repo')))
            else:
                repos = set()
    else:
        repos = set()

    donor['gnos_repos_with_complete_alignment_set'] = repos
    '''
    # this flag is not entirely accurate, disable it for now
    if repos:
        donor['is_alignment_completed'] = True
    else:
        donor['is_alignment_completed'] = False
    '''


def add_alignment_status_to_donor(donor, aggregated_bam_info):
    for aliquot_id in aggregated_bam_info.keys():
        alignment_status = aggregated_bam_info.get(aliquot_id)
        if 'normal' in alignment_status.get('dcc_specimen_type').lower(): # normal specimen
            if not donor.get('normal_alignment_status'): # no normal yet in this donor, this is good
                donor['normal_alignment_status'] = reorganize_unaligned_bam_info(alignment_status)
            else: # another normal with different aliquot_id! this is no good
                logger.warning('donor: {} has more than one normal, in use aliquot_id: {}, additional aliquot_id found: {}'
                        .format(donor.get('donor_unique_id'),
                                donor.get('normal_alignment_status').get('aliquot_id'),
                                aliquot_id)
                    )
        elif 'tumour' in alignment_status.get('dcc_specimen_type').lower(): # tumour specimen
            if not donor.get('tumor_alignment_status'):
                donor['tumor_alignment_status'] = []
            donor['tumor_alignment_status'].append(reorganize_unaligned_bam_info(alignment_status))
        else:
            logger.warning('invalid specimen type: {} in donor: {} with aliquot_id: {}'
                    .format(alignment_status.get('dcc_specimen_type'), donor.get('donor_unique_id'), aliquot_id)
                )


def reorganize_unaligned_bam_info(alignment_status):
    unaligned_bams = []
    for gnos_id in alignment_status.get('unaligned_bams').keys():
        unaligned_bams.append(
            {
                "gnos_id": gnos_id,
                "bam_file_name": alignment_status.get('unaligned_bams').get(gnos_id).get('bam_file_name'),
                "gnos_repo": alignment_status.get('unaligned_bams').get(gnos_id).get('gnos_repo'),
            }
        )
    alignment_status['unaligned_bams'] = unaligned_bams
    return alignment_status


def bam_aggregation(bam_files):
    aggregated_bam_info = {}

    for bam in bam_files:  # check aligned BAM(s) first
        if not bam['bam_type'] == 'Specimen level aligned BAM':
            continue

        if not aggregated_bam_info.get(bam['aliquot_id']): # new aliquot
            aggregated_bam_info[bam['aliquot_id']] = {
                "aliquot_id": bam['aliquot_id'],
                "submitter_specimen_id": bam['submitter_specimen_id'],
                "submitter_sample_id": bam['submitter_sample_id'],
                "dcc_specimen_type": bam['dcc_specimen_type'],
                "aligned": True,
                "aligned_bam": {
                    "gnos_id": bam['bam_gnos_ao_id'],
                    "bam_file_name": bam['bam_file_name'],
                    "bam_file_size": bam['bam_file_size'],
                    "gnos_last_modified": [bam['last_modified']],
                    "gnos_repo": [bam['gnos_repo']]
                 },
                 "bam_with_unmappable_reads": {},
                 "unaligned_bams": {}
            }
        else:
            alignment_status = aggregated_bam_info.get(bam['aliquot_id'])
            if alignment_status.get('aligned_bam').get('gnos_id') == bam['bam_gnos_ao_id']:
                if bam['gnos_repo'] in alignment_status.get('aligned_bam').get('gnos_repo'):
                    logger.warning( 'Same aliquot: {}, same GNOS ID: {} in the same GNOS repo: {} more than once. This should never be possible.'
                                    .format(
                                        bam['aliquot_id'],
                                        alignment_status.get('aligned_bam').get('gnos_id'),
                                        bam['gnos_repo']) 
                              )
                else:
                    alignment_status.get('aligned_bam').get('gnos_repo').append(bam['gnos_repo'])
                    alignment_status.get('aligned_bam').get('gnos_last_modified').append(bam['last_modified'])
            else:
                logger.warning( 'Same aliquot: {} from donor: {} has different aligned GNOS BAM entries, in use: {}, additional: {}'
                                    .format(
                                        bam['aliquot_id'],
                                        bam['donor_unique_id'],
                                        alignment_status.get('aligned_bam').get('gnos_id'),
                                        bam['gnos_metadata_url'])
                              )

    sort_repos_by_time(aggregated_bam_info)

    for bam in bam_files:  # now check BAM with unmappable reads that were derived from aligned BAM
        if not bam['bam_type'] == 'Specimen level unmapped reads after BWA alignment':
            continue

        if not aggregated_bam_info.get(bam['aliquot_id']): # new aliquot, too bad this is an orphaned unmapped read BAM the main aligned BAM is missing
            logger.warning('aliquot: {} has GNOS BAM entry for unmapped reads found: {}, however the main aligned BAM entry is missing'
                    .format(bam['aliquot_id'], bam['bam_gnos_ao_id'])
                )
        else:
            alignment_status = aggregated_bam_info.get(bam['aliquot_id'])
            if not alignment_status.get('bam_with_unmappable_reads'):
                alignment_status['bam_with_unmappable_reads'] = {
                    "gnos_id": bam['bam_gnos_ao_id'],
                    "bam_file_name": bam['bam_file_name'],
                    "bam_file_size": bam['bam_file_size'],
                    "gnos_repo": set([bam['gnos_repo']])
                }
            elif alignment_status.get('bam_with_unmappable_reads').get('gnos_id') == bam['bam_gnos_ao_id']:
                alignment_status.get('bam_with_unmappable_reads').get('gnos_repo').add(bam['gnos_repo'])
            else:
                logger.warning( 'same aliquot: {} has different unmappable reads GNOS BAM entries, in use: {}, additional: {}'
                                    .format(
                                        bam['aliquot_id'],
                                        alignment_status.get('bam_with_unmappable_reads').get('gnos_id'),
                                        bam['bam_gnos_ao_id']) 
                              )

    for bam in bam_files:  # last check original (submitted) unaligned BAM(s)
        if not bam['bam_type'] == 'Unaligned BAM':
            continue

        if not aggregated_bam_info.get(bam['aliquot_id']): # new aliquot with no aligned BAM yet
            aggregated_bam_info[bam['aliquot_id']] = {
                "aliquot_id": bam['aliquot_id'],
                "submitter_specimen_id": bam['submitter_specimen_id'],
                "submitter_sample_id": bam['submitter_sample_id'],
                "dcc_specimen_type": bam['dcc_specimen_type'],
                "aligned": False,
                "aligned_bam": {},
                "bam_with_unmappable_reads": {},
                "unaligned_bams": {
                    bam['bam_gnos_ao_id']: {
                        "bam_file_name": bam['bam_file_name'],
                        "gnos_repo": set([bam['gnos_repo']])
                    }
                }
            }
        else: # aliquot already exists
            alignment_status = aggregated_bam_info.get(bam['aliquot_id'])
            if alignment_status.get('unaligned_bams').get(bam['bam_gnos_ao_id']): # this unaligned bam was encountered before
                alignment_status.get('unaligned_bams').get(bam['bam_gnos_ao_id']).get('gnos_repo').add(bam['gnos_repo'])
            else:
                alignment_status.get('unaligned_bams')[bam['bam_gnos_ao_id']] = {
                        "bam_file_name": bam['bam_file_name'],
                        "gnos_repo": set([bam['gnos_repo']])
                }

    return aggregated_bam_info


def sort_repos_by_time(aggregated_bam_info):
    for aliquot in aggregated_bam_info:
        agg_bam = aggregated_bam_info.get(aliquot)
        if not agg_bam.get('aligned_bam'):
            continue
        modified_dates = agg_bam.get('aligned_bam').get('gnos_last_modified')
        gnos_repos = agg_bam.get('aligned_bam').get('gnos_repo')
        agg_bam.get('aligned_bam')['gnos_last_modified'], agg_bam.get('aligned_bam')['gnos_repo'] = \
            izip(*sorted(izip(modified_dates, gnos_repos), key=lambda x: x[0]))


def find_latest_metadata_dir(output_dir):
    dir_pattern = re.compile(u'^[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}-[0-9]{2}-[0-9]{2}_[A-Z]{3}$')
    metadata_dirs = []
    for dir in os.listdir(output_dir):
        if not os.path.isdir(output_dir + '/' + dir):
            continue
        if dir_pattern.search(dir):
            metadata_dirs.append(output_dir + '/' + dir)

    return sorted(metadata_dirs)[-1]


def main(argv=None):
    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    parser = ArgumentParser(description="PCAWG GNOS Metadata Parser",
             formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-c", "--config", dest="config",
             help="Configuration file for GNOS repositories", required=True)
    parser.add_argument("-m", "--metadata_dir", dest="metadata_dir",
             help="Directory containing metadata manifest files", required=False)
    parser.add_argument("-r", "--gnos_repo", dest="repo",
             help="Specify which GNOS repo to process, process all repos if none specified", required=False)
    parser.add_argument("-x", "--exclude_gnos_id_lists", dest="exclude_gnos_id_lists", # don't use this option for daily cron job
             help="File(s) containing GNOS IDs to be excluded, use filename pattern to specify the file(s)", required=False)
    parser.add_argument("-s", "--es_index_suffix", dest="es_index_suffix", # don't use this option for daily cron job
             help="Single letter suffix for ES index name", required=False)

    args = parser.parse_args()
    metadata_dir = args.metadata_dir
    conf_file = args.config
    repo = args.repo
    exclude_gnos_id_lists = args.exclude_gnos_id_lists
    es_index_suffix = args.es_index_suffix
    if not es_index_suffix: es_index_suffix = ''

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

    log_file = metadata_dir + '.metadata_parser' + ('' if not repo else '.'+repo) + '.log'
    # delete old log first if exists
    if os.path.isfile(log_file): os.remove(log_file)

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)

    es_host = 'localhost:9200'
    es_index = 'p_' + ('' if not repo else repo+'_') + re.sub(r'\D', '', timestamp).replace('20','',1) + es_index_suffix
    es = init_es(es_host, es_index)

    logger.info('processing metadata list files in {} to build es index {}'.format(metadata_dir, es_index))
    process(metadata_dir, conf, es_index, es, metadata_dir+'/donor_'+es_index+'.jsonl', metadata_dir+'/bam_'+es_index+'.jsonl', repo, exclude_gnos_id_lists)

    # now update kibana dashboard
    # donor
    dashboard_name = ' ['+repo+']' if repo else ''
    with open('kibana-donor.json', 'r') as d:
        donor_dashboard = json.loads(d.read())
    donor_dashboard['index']['default'] = es_index + '/donor'
    title = 'PCAWG Donors' + dashboard_name + ' (beta)'
    donor_dashboard['title'] = title
    body = {
        'dashboard': json.dumps(donor_dashboard),
        'user': 'guest',
        'group': 'guest',
        'title': title
    }
    es.index(index='kibana-int', doc_type='dashboard', id='PCAWG Donors' + dashboard_name, body=body)

    # bam search, no need this for now, not very useful
    '''
    with open('kibana-bam.json', 'r') as d:
        bam_dashboard = json.loads(d.read())
    bam_dashboard['index']['default'] = es_index + '/bam_file'
    title = 'PCAWG BAMs' + dashboard_name + ' (beta)'
    bam_dashboard['title'] = title
    body = {
        'dashboard': json.dumps(bam_dashboard),
        'user': 'guest',
        'group': 'guest',
        'title': title
    }
    es.index(index='kibana-int', doc_type='dashboard', id='PCAWG BAMs' + dashboard_name, body=body)
    '''

    return 0


if __name__ == "__main__":
    sys.exit(main())


