#!/usr/bin/env python

import sys
import os
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


def generate_report(es_index, es_queries):
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

    #print(report)  # for debugging only
    print ('project', count_types)
    for p in report.keys():
        counts = []
        donors = []
        for ctype in count_types:
            counts.append( report.get(p).get(ctype).get('count') if report.get(p).get(ctype) else 0 )
            donors.append( report.get(p).get(ctype).get('donors') if report.get(p).get(ctype) else '' )
        print(p, counts, donors)


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
    parser.add_argument("-i", "--index", dest="es_index",
             help="Elasticsearch index to be queried", required=True)

    args = parser.parse_args()
    es_index = args.es_index

    generate_report(es_index, es_queries)

    return 0


if __name__ == "__main__":
    sys.exit(main())
