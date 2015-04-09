#!/bin/bash

curl -XGET "http://pancancer.info/elasticsearch/pcawg_es/donor/_search?size=300&pretty" -d '
{
   "fields":[
      "dcc_project_code",
      "donor_unique_id",
      "duplicated_bwa_alignment_summary.exists_mismatch_bwa_bams",
      "duplicated_bwa_alignment_summary.normal.exists_gnos_id_mismatch",
      "duplicated_bwa_alignment_summary.normal.aliquot_id",
      "duplicated_bwa_alignment_summary.normal.dcc_specimen_type",
      "duplicated_bwa_alignment_summary.normal.exists_md5sum_mismatch",
      "duplicated_bwa_alignment_summary.normal.exists_version_mismatch",
      "duplicated_bwa_alignment_summary.tumor.exists_gnos_id_mismatch",
      "duplicated_bwa_alignment_summary.tumor.aliquot_id",
      "duplicated_bwa_alignment_summary.tumor.dcc_specimen_type",
      "duplicated_bwa_alignment_summary.tumor.exists_md5sum_mismatch",
      "duplicated_bwa_alignment_summary.tumor.exists_version_mismatch"
   ],
   "filter":{
      "bool":{
         "must":[
            {
               "type":{
                  "value":"donor"
               }
            },
            {
               "terms":{
                  "duplicated_bwa_alignment_summary.exists_mismatch_bwa_bams":[
                     "T"
                  ]
               }
            }
         ],
         "must_not":[
            {
               "terms":{
                  "flags.is_manual_qc_failed":[
                     "T"
                  ]
               }
            },
            {
               "terms":{
                  "flags.is_donor_blacklisted":[
                     "T"
                  ]
               }
            }
         ]
      }
   }
}' |less
