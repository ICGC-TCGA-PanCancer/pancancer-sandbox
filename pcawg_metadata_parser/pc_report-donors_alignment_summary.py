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
  # query 0: both aligned
  {
    "aggs": {
      "project_f": {
        "aggs": {
          "project": {
            "terms": {
              "field": "dcc_project_code",
              "size": 1000
            },
            "aggs": {
              "donor_id": {
                "terms": {
                  "field": "donor_unique_id",
                  "size": 10000
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
                          "value": es_type
                        }
                      },
                      {
                        "terms": {
                          "normal_specimen.is_aligned": [
                            "T"
                          ]
                        }
                      },
                      {
                        "terms": {
                          "are_all_tumor_specimens_aligned": [
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
  # query 1: normal aligned, tumor not
  {
    "aggs": {
      "project_f": {
        "aggs": {
          "project": {
            "terms": {
              "field": "dcc_project_code",
              "size": 1000
            },
            "aggs": {
              "donor_id": {
                "terms": {
                  "field": "donor_unique_id",
                  "size": 10000
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
                          "value": es_type
                        }
                      },
                      {
                        "terms": {
                          "normal_specimen.is_aligned": [
                            "T"
                          ]
                        }
                      },
                      { "not": 
                        { "filter":
                          {
                            "terms": {
                              "all_tumor_specimen_aliquot_counts": [
                                "0"
                              ]
                            }
                          }
                        }
                      },
                      {
                        "terms": {
                          "are_all_tumor_specimens_aligned": [
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
  # query 2: tumor aligned, normal not
  {
    "aggs": {
      "project_f": {
        "aggs": {
          "project": {
            "terms": {
              "field": "dcc_project_code",
              "size": 1000
            },
            "aggs": {
              "donor_id": {
                "terms": {
                  "field": "donor_unique_id",
                  "size": 10000
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
                          "value": es_type
                        }
                      },
                      {
                        "terms": {
                          "normal_specimen.is_aligned": [
                            "F"
                          ]
                        }
                      },
                      {
                        "terms": {
                          "are_all_tumor_specimens_aligned": [
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
  # query 3: both unaligned
  {
    "aggs": {
      "project_f": {
        "aggs": {
          "project": {
            "terms": {
              "field": "dcc_project_code",
              "size": 1000
            },
            "aggs": {
              "donor_id": {
                "terms": {
                  "field": "donor_unique_id",
                  "size": 10000
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
                          "value": es_type
                        }
                      },
                      {
                        "terms": {
                          "normal_specimen.is_aligned": [
                            "F"
                          ]
                        }
                      },
                      { "not": 
                        { "filter":
                          {
                            "terms": {
                              "all_tumor_specimen_aliquot_counts": [
                                "0"
                              ]
                            }
                          }
                        }
                      },
                      {
                        "terms": {
                          "are_all_tumor_specimens_aligned": [
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
  # query 4: normal aligned, tumor missing
  {
    "aggs": {
      "project_f": {
        "aggs": {
          "project": {
            "terms": {
              "field": "dcc_project_code",
              "size": 1000
            },
            "aggs": {
              "donor_id": {
                "terms": {
                  "field": "donor_unique_id",
                  "size": 10000
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
                          "value": es_type
                        }
                      },
                      {
                        "terms": {
                          "normal_specimen.is_aligned": [
                            "T"
                          ]
                        }
                      },
                      {
                        "terms": {
                          "all_tumor_specimen_aliquot_counts": [
                            "0"
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
  # query 5: normal unaligned, tumor missing
  {
    "aggs": {
      "project_f": {
        "aggs": {
          "project": {
            "terms": {
              "field": "dcc_project_code",
              "size": 1000
            },
            "aggs": {
              "donor_id": {
                "terms": {
                  "field": "donor_unique_id",
                  "size": 10000
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
                          "value": es_type
                        }
                      },
                      {
                        "terms": {
                          "normal_specimen.is_aligned": [
                            "F"
                          ]
                        }
                      },
                      {
                        "terms": {
                          "all_tumor_specimen_aliquot_counts": [
                            "0"
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
  # query 6: tumor aligned, normal missing
  {
    "aggs": {
      "project_f": {
        "aggs": {
          "project": {
            "terms": {
              "field": "dcc_project_code",
              "size": 1000
            },
            "aggs": {
              "donor_id": {
                "terms": {
                  "field": "donor_unique_id",
                  "size": 10000
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
                          "value": es_type
                        }
                      },
                      { "not": 
                        { "filter":
                          {
                            "exists": {
                              "field": "normal_specimen.is_aligned"
                            }
                          }
                        }
                      },
                      {
                        "terms": {
                          "are_all_tumor_specimens_aligned": [
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
  # query 7: tumor unaligned, normal missing
  {
    "aggs": {
      "project_f": {
        "aggs": {
          "project": {
            "terms": {
              "field": "dcc_project_code",
              "size": 1000
            },
            "aggs": {
              "donor_id": {
                "terms": {
                  "field": "donor_unique_id",
                  "size": 10000
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
                          "value": es_type
                        }
                      },
                      { "not": 
                        { "filter":
                          {
                            "exists": {
                              "field": "normal_specimen.is_aligned"
                            }
                          }
                        }
                      },
                      {
                        "terms": {
                          "are_all_tumor_specimens_aligned": [
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
]


def init_report_dir(metadata_dir, report_name, repo):
    report_dir = metadata_dir + '/reports/' + report_name if not repo else metadata_dir + '/reports/' + report_name + '/' + repo
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)
    return report_dir


def generate_report(es_index, es_queries, metadata_dir, report_name, repo):
    # we need to run several queries to get facet counts for different type of donors
    report = OrderedDict()
    count_types = [
        "both_aligned",
        "normal_aligned_tumor_not",
        "tumor_aligned_normal_not",
        "both_not",
        "normal_aligned_tumor_missing",
        "normal_unaligned_tumor_missing",
        "tumor_aligned_normal_missing",
        "tumor_unaligned_normal_missing"
    ]

    for q_index in range(len(count_types)):
        response = es.search(index=es_index, body=es_queries[q_index])
        #print(json.dumps(response['aggregations']['project_f']))  # for debugging
    
        for p in response['aggregations']['project_f']['project'].get('buckets'):
            count = p.get('doc_count')
            donors = get_donors(p.get('donor_id').get('buckets'))
            project = p.get('key')
            if not report.get(project):
                report[project] = {}
            if not report[project].get(count_types[q_index]):
                report[project][count_types[q_index]] = {}
            report[project][count_types[q_index]]['count'] = count
            report[project][count_types[q_index]]['donors'] = donors

    report_dir = init_report_dir(metadata_dir, report_name, repo)

    summary_table = []
    for p in report.keys():
        summary = {"project": p}
        for ctype in count_types:
            summary[ctype] = report.get(p).get(ctype).get('count') if report.get(p).get(ctype) else 0
            donors = report.get(p).get(ctype).get('donors') if report.get(p).get(ctype) else []
            if donors:
                with open(report_dir + '/' + p + '.' + ctype + '.donors.tsv', 'w') as o:
                    for d in donors:
                        o.write(d + '\n')

        summary_table.append(summary)

    with open(report_dir + '/donor.json', 'w') as o:
        o.write(json.dumps(summary_table))


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

    generate_report(es_index, es_queries, metadata_dir, report_name, repo)

    return 0


if __name__ == "__main__":
    sys.exit(main())
