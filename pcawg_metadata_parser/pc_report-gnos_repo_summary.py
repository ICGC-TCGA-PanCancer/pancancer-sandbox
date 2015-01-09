#!/usr/bin/env python

import sys
import os
import re
import json
from collections import OrderedDict
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from elasticsearch import Elasticsearch

es_host = 'localhost:9200'
es_type = "donor"
es = Elasticsearch([es_host])

es_queries = [
  # order of the queries is important
  # query 0: live_alignment_completed_donors
  [
    # es_query for donor counts
    {
      "aggs": {
        "gnos_f": {
          "aggs": {
            "gnos_assignment": {
              "terms": {
                "field": "original_gnos_assignment",
                "size": 100
              },
              "aggs": {
                "exist_in_gnos_repo": {
                  "terms": {
                    "field": "gnos_repos_with_complete_alignment_set",
                    "size": 100
                  },
                  "aggs": {
                    "donors": {
                      "terms": {
                        "field": "donor_unique_id",
                        "size": 50000
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
                            "flags.is_normal_specimen_aligned": [
                              "T"
                            ]
                          }
                        },
                        {
                          "terms": {
                            "flags.are_all_tumor_specimens_aligned": [
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
    },
    # es_query for specimen counts
    {
      "aggs": {
        "gnos_f": {
          "aggs": {
            "gnos_assignment": {
              "terms": {
                "field": "original_gnos_assignment",
                "size": 100
              },
              "aggs": {
                "normal_exists_in_gnos_repo": {
                  "terms": {
                    "field": "normal_alignment_status.aligned_bam.gnos_repo",
                    "size": 100
                  }
                },
                "tumor_specimens": {
                  "nested": {
                    "path": "tumor_alignment_status",
                  },
                  "aggs":{
                    "tumor_exists_in_gnos_repo":{
                      "terms": {
                        "field": "tumor_alignment_status.aligned_bam.gnos_repo",
                        "size": 100
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
                            "flags.is_normal_specimen_aligned": [
                              "T"
                            ]
                          }
                        },
                        {
                          "terms": {
                            "flags.are_all_tumor_specimens_aligned": [
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
    },
  ],
  # query 1: live_aligned_sanger_not_called_donors
  [
    # es_query for donor counts
    {
      "aggs": {
        "gnos_f": {
          "aggs": {
            "gnos_assignment": {
              "terms": {
                "field": "original_gnos_assignment",
                "size": 100
              },
              "aggs": {
                "exist_in_gnos_repo": {
                  "terms": {
                    "field": "gnos_repos_with_complete_alignment_set",
                    "size": 100
                  },
                  "aggs": {
                    "donors": {
                      "terms": {
                        "field": "donor_unique_id",
                        "size": 50000
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
                            "flags.is_normal_specimen_aligned": [
                              "T"
                            ]
                          }
                        },
                        {
                          "terms": {
                            "flags.are_all_tumor_specimens_aligned": [
                              "T"
                            ]
                          }
                        },
                        {
                          "terms": {
                            "flags.is_sanger_variant_calling_performed": [
                              "F"
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
    },
    # es_query for specimen counts
    {
      "aggs": {
        "gnos_f": {
          "aggs": {
            "gnos_assignment": {
              "terms": {
                "field": "original_gnos_assignment",
                "size": 100
              },
              "aggs": {
                "normal_exists_in_gnos_repo": {
                  "terms": {
                    "field": "normal_alignment_status.aligned_bam.gnos_repo",
                    "size": 100
                  }
                },
                "tumor_specimens": {
                  "nested": {
                    "path": "tumor_alignment_status",
                  },
                  "aggs":{
                    "tumor_exists_in_gnos_repo":{
                      "terms": {
                        "field": "tumor_alignment_status.aligned_bam.gnos_repo",
                        "size": 100
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
                            "flags.is_normal_specimen_aligned": [
                              "T"
                            ]
                          }
                        },
                        {
                          "terms": {
                            "flags.are_all_tumor_specimens_aligned": [
                              "T"
                            ]
                          }
                        },
                        {
                          "terms": {
                            "flags.is_sanger_variant_calling_performed": [
                              "F"
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
    },
  ],
  # query 2: train2_donors
  # query 3: train2_pilot_donors

]



def init_report_dir(metadata_dir, report_name, repo):
    report_dir = metadata_dir + '/reports/' + report_name if not repo else metadata_dir + '/reports/' + report_name + '/' + repo
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)
    return report_dir


def generate_report(es_index, es_queries, metadata_dir, report_name, timestamp, repo):
    # we need to run several queries to get facet counts for different type of donors
    report = OrderedDict()
    donors_per_repo = {}
    count_types = [
        "live_alignment_completed_donors",
        "live_aligned_sanger_not_called_donors",
        #"train2_donors",
        #"train2_pilot_donors"
    ]

    for q_index in range(len(count_types)):
        # get donor counts
        response = es.search(index=es_index, body=es_queries[q_index][0])
        #print json.dumps(response['aggregations']['gnos_f']) + '\n'  # for debugging
    
        donors_per_repo[count_types[q_index]] = {}

        for p in response['aggregations']['gnos_f']['gnos_assignment'].get('buckets'):
            count = p.get('doc_count')
            original_gnos_repo = p.get('key')
            donors_per_repo[count_types[q_index]][original_gnos_repo] = {}

            repos = get_donors_per_repo(p.get('exist_in_gnos_repo').get('buckets'), donors_per_repo[count_types[q_index]][original_gnos_repo])

            if not report.get(original_gnos_repo):
                report[original_gnos_repo] = {}
            if not report[original_gnos_repo].get(count_types[q_index]):
                report[original_gnos_repo][count_types[q_index]] = {}

            report[original_gnos_repo][count_types[q_index]]['count'] = [count] # first count is donor
            report[original_gnos_repo][count_types[q_index]]['repos'] = repos

        #print json.dumps(donors_per_repo) # for debugging

        # get specimen counts
        response = es.search(index=es_index, body=es_queries[q_index][1])
        #print json.dumps(response['aggregations']['gnos_f']) + '\n'  # for debugging

        for p in response['aggregations']['gnos_f']['gnos_assignment'].get('buckets'):
            count_normal = p.get('doc_count')
            count_tumor = p.get('tumor_specimens').get('doc_count')
            original_gnos_repo = p.get('key')
            repos = add_specimen_counts_per_repo(
                        report[original_gnos_repo][count_types[q_index]]['repos'],
                        p.get('normal_exists_in_gnos_repo').get('buckets'),
                        p.get('tumor_specimens').get('tumor_exists_in_gnos_repo').get('buckets'),
                    )

            report[original_gnos_repo][count_types[q_index]]['count'].extend([count_normal, count_tumor]) # second count is specimen
            report[original_gnos_repo][count_types[q_index]]['repos'] = repos

    #print json.dumps(report)  # for debug

    report_dir = init_report_dir(metadata_dir, report_name, repo)

    for ctype in count_types:
        for ori_repo in donors_per_repo[ctype]:
            for repo in donors_per_repo[ctype][ori_repo]:
                with open(report_dir + '/' + ctype + '.' + ori_repo + '.' + repo + '.aligned_donors.txt', 'w') as o:
                    o.write('\n'.join(donors_per_repo[ctype][ori_repo][repo]))

        repos = {}
        for original_repo in report.keys():
            repos[get_formal_repo_name(original_repo)] = {
                "_ori_count": report[original_repo][ctype]['count']
            }
            for repo, count in report[original_repo][ctype]['repos'].iteritems():
                repos[get_formal_repo_name(original_repo)][get_formal_repo_name(repo)] = count

        with open(report_dir + '/' + ctype + '.repos.json', 'w') as o:
            o.write(json.dumps(repos))


def get_formal_repo_name(repo):
    repo_url_to_repo = {
      "https://gtrepo-bsc.annailabs.com/": "Barcelona",
      "bsc": "Barcelona",
      "https://gtrepo-ebi.annailabs.com/": "London",
      "ebi": "London",
      "https://cghub.ucsc.edu/": "Santa Cruz",
      "cghub": "Santa Cruz",
      "https://gtrepo-dkfz.annailabs.com/": "Heidelberg",
      "dkfz": "Heidelberg",
      "https://gtrepo-riken.annailabs.com/": "Tokyo",
      "riken": "Tokyo",
      "https://gtrepo-osdc-icgc.annailabs.com/": "Chicago",
      "osdc-icgc": "Chicago",
      "https://gtrepo-etri.annailabs.com/": "Seoul",
      "etri": "Seoul",
    }

    return repo_url_to_repo.get(repo)


def get_short_repo_name(url):
    repo_url_to_repo = {
      "https://gtrepo-bsc.annailabs.com/": "bsc",
      "https://gtrepo-ebi.annailabs.com/": "ebi",
      "https://cghub.ucsc.edu/": "cghub",
      "https://gtrepo-dkfz.annailabs.com/": "dkfz",
      "https://gtrepo-riken.annailabs.com/": "riken",
      "https://gtrepo-osdc-icgc.annailabs.com/": "osdc-icgc",
      "https://gtrepo-etri.annailabs.com/": "etri",
    }

    return repo_url_to_repo.get(url)


def add_specimen_counts_per_repo(repos, repo_buckets_normal, repo_buckets_tumor):
    for s in repo_buckets_normal:
      if repos.get(s.get('key')):
        repos[s.get('key')].append(s.get('doc_count') if s.get('doc_count') else 0)

    for s in repo_buckets_tumor:
      if repos.get(s.get('key')):
        repos[s.get('key')].append(s.get('doc_count') if s.get('doc_count') else 0)

    return repos


def get_donors_per_repo(repo_buckets, donors):
    repos = {}
    for d in repo_buckets:
        repos[d.get('key')] = [d.get('doc_count')]
        donors[get_short_repo_name(d.get('key'))] = [ item.get('key') for item in d.get('donors').get('buckets') ]
    return repos


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
