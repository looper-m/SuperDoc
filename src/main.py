import os
import json
import xmltodict
import requests
import csv

from flask import Flask, request
from flask_cors import CORS, cross_origin
from shutil import copyfile

app = Flask(__name__)
CORS(app)
DOCUMENT_SRC_FOLDER = './documents'


def generate_data():
    json_data = ""
    with open(os.path.join(DOCUMENT_SRC_FOLDER, 'mplus_topics.xml')) as dataset:
        dataset_dict = xmltodict.parse(dataset.read())

        for element_dict in dataset_dict["health-topics"]["health-topic"]:
            doc_id = element_dict["@id"]
            index_string = '{"index": {"_id": "' + doc_id + '"}}'

            doc = dict(element_dict)
            doc.pop("@id")
            json_data += index_string + "\n" + json.dumps(doc) + "\n"

    # with open(os.path.join(DOCUMENT_SRC_FOLDER, "data.json"), "w") as document:
    #     document.write(json_data)

    return json_data


def nGrams(query):
    grams = list()
    words = query.lower().split()
    n = len(words)

    for k in range(n, 0, -1):
        for i in range(n - k + 1):
            grams.append(' '.join(words[i:i + k]))
    return grams


def read_synonyms():
    synonyms = dict()

    with open(os.path.join(DOCUMENT_SRC_FOLDER, "synonyms.csv")) as syn_file:
        for row in csv.reader(syn_file):
            synonyms.setdefault(row[0], list())
            synonyms[row[0]].append(row[1])

    with open(os.path.join(DOCUMENT_SRC_FOLDER, "solr_synonyms.txt"), "w") as solr_syn_file:
        for row in synonyms.values():
            solr_string = ""
            for word in row:
                solr_string += word + ", "
            solr_syn_file.write(solr_string[:-2] + "\n")


def create_index():
    copyfile(os.path.join(DOCUMENT_SRC_FOLDER, "stoplist.txt"),
             "/Users/mo/Desktop/elasticsearch-7.11.1/config/stoplist.txt")
    copyfile(os.path.join(DOCUMENT_SRC_FOLDER, "solr_synonyms.txt"),
             "/Users/mo/Desktop/elasticsearch-7.11.1/config/solr_synonyms.txt")

    url = "http://localhost:9200/mayoc-index?pretty"
    payload = {
        "settings": {
            "index.mapping.ignore_malformed": "true",
            "analysis": {
                "analyzer": {
                    "custom_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase"
                            # "snow_stemmer"
                        ]
                    },
                    "custom_search_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "synonym_graph"
                            # "snow_stemmer"
                        ]
                    },
                    "custom_search_stop_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "synonym_graph",
                            "custom_stop"
                            # "snow_stemmer"
                        ]
                    }
                },
                "filter": {
                    "custom_stop": {
                        "type": "stop",
                        "ignore_case": "true",
                        "stopwords_path": "./stoplist.txt"
                    },
                    "synonym_graph": {
                        "type": "synonym_graph",
                        "expand": "true",
                        "synonyms_path": "./solr_synonyms.txt"
                        # "synonyms": ["ball, testicular"]
                    },
                    "snow_stemmer": {
                        "type": "snowball",
                        "language": "English"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "@date-created": {
                    "type": "date",
                    "format": "MM/dd/yyyy",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                        }
                    }
                },
                "@language": {
                    "type": "text",
                    "index": "false"
                },
                "@meta-desc": {
                    "type": "text",
                    "index_phrases": "true",
                    "analyzer": "custom_analyzer",
                    "search_analyzer": "custom_search_stop_analyzer",
                    "search_quote_analyzer": "custom_search_analyzer",
                    "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                    }
                },
                "@title": {
                    "type": "text",
                    "index_phrases": "true",
                    "analyzer": "custom_analyzer",
                    "search_analyzer": "custom_search_stop_analyzer",
                    "search_quote_analyzer": "custom_search_analyzer",
                    "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                    }
                },
                "@url": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                    }
                },
                "also-called": {
                    "type": "text",
                    "analyzer": "custom_analyzer",
                    "search_analyzer": "custom_search_stop_analyzer",
                    "search_quote_analyzer": "custom_search_analyzer",
                    "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                    }
                },
                "full-summary": {
                    "type": "text",
                    "index_phrases": "true",
                    "analyzer": "custom_analyzer",
                    "search_analyzer": "custom_search_stop_analyzer",
                    "search_quote_analyzer": "custom_search_analyzer",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "group": {
                    "properties": {
                        "#text": {
                            "type": "text",
                            "analyzer": "custom_analyzer",
                            "search_analyzer": "custom_search_stop_analyzer",
                            "search_quote_analyzer": "custom_search_analyzer",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "@id": {
                            "type": "text",
                            "index": "false"
                        },
                        "@url": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        }
                    }
                },
                "language-mapped-topic": {
                    "properties": {
                        "#text": {
                            "type": "text",
                            "index_phrases": "true",
                            "analyzer": "custom_analyzer",
                            "search_analyzer": "custom_search_stop_analyzer",
                            "search_quote_analyzer": "custom_search_analyzer",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "@id": {
                            "type": "text",
                            "index": "false"
                        },
                        "@language": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "@url": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        }
                    }
                },
                "mesh-heading": {
                    "properties": {
                        "descriptor": {
                            "properties": {
                                "#text": {
                                    "type": "text",
                                    "analyzer": "custom_analyzer",
                                    "search_analyzer": "custom_search_stop_analyzer",
                                    "search_quote_analyzer": "custom_search_analyzer",
                                    "fields": {
                                        "keyword": {
                                            "type": "keyword"
                                        }
                                    }
                                },
                                "@id": {
                                    "type": "text",
                                    "index": "false"
                                }
                            }
                        },
                        "qualifier": {
                            "properties": {
                                "#text": {
                                    "type": "text",
                                    "analyzer": "custom_analyzer",
                                    "search_analyzer": "custom_search_stop_analyzer",
                                    "search_quote_analyzer": "custom_search_analyzer",
                                    "fields": {
                                        "keyword": {
                                            "type": "keyword"
                                        }
                                    }
                                },
                                "@id": {
                                    "type": "text",
                                    "index": "false"
                                }
                            }
                        }
                    }
                },
                "other-language": {
                    "properties": {
                        "#text": {
                            "type": "text",
                            "index": "false"
                        },
                        "@url": {
                            "type": "text",
                            "index": "false"
                        },
                        "@vernacular-name": {
                            "type": "text",
                            "index": "false"
                        }
                    }
                },
                "primary-institute": {
                    "properties": {
                        "#text": {
                            "type": "text",
                            "index_phrases": "true",
                            "analyzer": "custom_analyzer",
                            "search_analyzer": "custom_search_stop_analyzer",
                            "search_quote_analyzer": "custom_search_analyzer",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "@url": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        }
                    }
                },
                "related-topic": {
                    "properties": {
                        "#text": {
                            "type": "text",
                            "analyzer": "custom_analyzer",
                            "search_analyzer": "custom_search_stop_analyzer",
                            "search_quote_analyzer": "custom_search_analyzer",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "@id": {
                            "type": "text",
                            "index": "false"
                        },
                        "@url": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        }
                    }
                },
                "see-reference": {
                    "type": "text",
                    "analyzer": "custom_analyzer",
                    "search_analyzer": "custom_search_stop_analyzer",
                    "search_quote_analyzer": "custom_search_analyzer",
                    "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                    }
                },
                "site": {
                    "properties": {
                        "@language-mapped-url": {
                            "type": "text",
                            "index": "false"
                        },
                        "@title": {
                            "type": "text",
                            "analyzer": "custom_analyzer",
                            "search_analyzer": "custom_search_stop_analyzer",
                            "search_quote_analyzer": "custom_search_analyzer"
                        },
                        "@url": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "information-category": {
                            "type": "text",
                            "analyzer": "custom_analyzer",
                            "search_analyzer": "custom_search_stop_analyzer",
                            "search_quote_analyzer": "custom_search_analyzer"
                        },
                        "organization": {
                            "type": "text",
                            "analyzer": "custom_analyzer",
                            "search_analyzer": "custom_search_stop_analyzer",
                            "search_quote_analyzer": "custom_search_analyzer",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "standard-description": {
                            "type": "text",
                            "analyzer": "custom_analyzer",
                            "search_analyzer": "custom_search_stop_analyzer",
                            "search_quote_analyzer": "custom_search_analyzer"
                        }
                    }
                }
            }
        }
    }

    requests.delete(url)
    r = requests.put(url, json=payload)
    print("create_index(): " + str(r.status_code))
    print(r.text)


def index_bulk_data(json_payload):
    url = "http://localhost:9200/mayoc-index/_bulk?pretty&refresh"
    headers = {"Content-Type": "application/json"}

    r = requests.post(url, headers=headers, data=json_payload)
    print("index_bulk_data(): " + str(r.status_code))
    print(r.text)


def parse_int(value):
    try:
        return int(value), True
    except ValueError:
        return value, False


@app.route('/document/<doc_id>', methods=['GET'])
@cross_origin()
def get_document_by_id(doc_id):
    url = "http://localhost:9200/mayoc-index/_doc/" + doc_id + "?pretty"

    r = requests.get(url)
    print("get_document_by_id(): " + str(r.status_code))
    print(r.text)
    return r.text


@app.route('/search', methods=['POST', 'GET'])
@cross_origin()
def evaluate_query():
    query_param = request.args.get('q')
    from_param = request.args.get('from')

    if query_param is None:
        return

    search_from = 0
    if from_param is not None and parse_int(from_param)[1]:
        search_from = parse_int(from_param)[0]

    search_query = query_param.strip()
    if len(search_query) == 0:
        return

    split_query = search_query.split(":", 1)
    if len(split_query) == 1:
        query_kind = ""
        query = split_query[0].strip()
    else:
        query_kind = split_query[0].strip().lower()
        query = split_query[1].strip()

    print(query)

    url = "http://localhost:9200/mayoc-index/_search?pretty"
    if len(query_kind) > 0 and query_kind == "condition" or query_kind == "illness":
        payload = {
            "_source": ["*"],
            "from": search_from,
            "size": "15",
            "query": {
                "query_string": {
                    "fields": [
                        "@title^4",
                        "mesh-heading^4",
                        "also-called^4"
                    ],
                    "query": query
                }
            },
            "highlight": {
                "require_field_match": "false",
                "pre_tags": ["<strong>"],
                "post_tags": ["</strong>"],
                "fields": {
                    "@meta-desc": {},
                    "full-summary": {}
                },
                "type": "unified"
            }
        }
    elif len(query_kind) > 0 and query_kind == "symptom":
        payload = {
            "_source": ["*"],
            "from": search_from,
            "size": "15",
            "query": {
                "query_string": {
                    "fields": [
                        "@meta-desc^4",
                        "full-summary^5",
                        "site.information-category^5",
                        "site.title^5",
                        "related-topic.#text^3",
                        "*"
                    ],
                    "query": query
                }
            },
            "highlight": {
                "require_field_match": "false",
                "pre_tags": ["<strong>"],
                "post_tags": ["</strong>"],
                "fields": {
                    "@meta-desc": {},
                    "full-summary": {}
                },
                "type": "unified"
            }
        }
    else:
        payload = {
            "_source": ["*"],
            "from": search_from,
            "size": "15",
            "query": {
                "query_string": {
                    "fields": [
                        "@title^8",
                        "mesh-heading^8",
                        "also-called^8",
                        "see-reference^8",
                        "@meta-desc^7",
                        "full-summary^7",
                        "site.information-category^5",
                        "site.title^5",
                        "related-topic.#text^2",
                        "*"
                    ],
                    "query": query,
                    "analyze_wildcard": "true"
                }
            },
            "highlight": {
                "require_field_match": "false",
                "pre_tags": ["<strong>"],
                "post_tags": ["</strong>"],
                "fields": {
                    "@meta-desc": {},
                    "full-summary": {}
                },
                "type": "unified"
            }
        }

    # print(payload)
    r = requests.get(url, json=payload)
    print("evaluate_query_simple(): " + str(r.status_code))
    # print(r.text)
    return r.text


def analyze(input_text):
    url = "http://localhost:9200/mayoc-index/_analyze?pretty"
    payload = {
        # "analyzer": "custom_search_stop_analyzer",
        "field": "@title",
        "text": input_text
    }

    r = requests.get(url, json=payload)
    print("analyze(): " + str(r.status_code))
    print(r.text)


if __name__ == "__main__":
    read_synonyms()
    create_index()
    index_bulk_data(generate_data())

    app.run(host="localhost", port=4001)

# phrase = "Quick brown fox's   jumps"
# print(nGrams(phrase))
# print("-----")

# read_synonyms()
# create_index()
# index_bulk_data(generate_data())

# qry = 'Symptom:"ball cancer" OR (ball cancer)'
# qry = '"ball cancer" OR (ball cancer)'
# qry = '"ball cancer"'
# evaluate_query(qry)
