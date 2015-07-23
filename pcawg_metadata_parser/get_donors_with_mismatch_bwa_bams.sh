#!/bin/bash

curl -XGET "http://pancancer.info/elasticsearch/pcawg_es/donor/_search?pretty=1" -d '
    {
      "aggs": {
        "gnos_f": {
          "aggs": {
            "exists_mismatch_bwa_bams": {
              "terms": {
                "field": "duplicated_bwa_alignment_summary.exists_mismatch_bwa_bams",
                "size": 10
              },
              "aggs": {
                "donors": {
                  "terms": {
                    "field": "donor_unique_id",
                    "size": 200
                  },
                  "aggs": {
                    "exist_in_gnos_repo": {
                      "terms": {
                        "field": "gnos_repos_with_complete_alignment_set",
                        "size": 20
                      }
                    }
                  }
                }
              }
            }
          },
          "filter": {
            "fquery": {
              "query": {
                "filtered": {
                  "query": {
                    "bool": {
                      "should": [
                        {
                          "query_string": {
                            "query": "*"
                          }
                        }
                      ]
                    }
                  },
                  "filter": {
                    "bool": {
                      "must": [
                        {
                          "type": {
                            "value": "donor"
                          }
                        },
                        {
                          "terms": {
                            "duplicated_bwa_alignment_summary.exists_mismatch_bwa_bams": [
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
                  }
                }
              }
            }
          }
        }
      },
      "size": 0
    }
' | less
