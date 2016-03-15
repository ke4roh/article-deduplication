"""
Copyright 2016 Ronald J. Nowling

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import argparse
from collections import defaultdict
import json
import sys

def extract_duplicates(query_results):
    # Every record entry has a field "tm_field_dupe_of" which points
    # to the "entity_id" field of another article. We need to group
    # the articles by their parent

    duplicate_sets = defaultdict(set)
    
    for doc in query_results["response"]["docs"]:
        id = str(doc["entity_id"])
        
        duplicate_sets[id].add(id)
        for duplicate_of in doc["tm_field_dupe_of"]:
            duplicate_sets[id].add(str(duplicate_of))

    return duplicate_sets.values()

def merge_duplicates(duplicate_sets):
    merged_sets = []

    for duplicate_set in duplicate_sets:
        was_merged = False
        for other_duplicate_set in merged_sets:
            if len(duplicate_set.intersection(other_duplicate_set)) > 0:
                other_duplicate_set.update(duplicate_set)
                was_merged = True
        if not was_merged:
            merged_sets.append(duplicate_set)

    return merged_sets

def run(args):
    input_fl = open(args["input"])
    query_results = json.load(input_fl)
    input_fl.close()
    
    duplicate_sets = extract_duplicates(query_results)

    print len(duplicate_sets), "duplicate sets"

    while True:
        merged_duplicate_sets = merge_duplicates(duplicate_sets)
        if len(duplicate_sets) == len(merged_duplicate_sets):
            break
        print len(merged_duplicate_sets), "duplicate sets after merge"
        duplicate_sets = merged_duplicate_sets

    duplicate_lists = []
    for duplicate_set in duplicate_sets:
        duplicate_lists.append(list(duplicate_set))

    output_fl = open(args["output"], "w")
    json.dump(duplicate_lists, output_fl)
    output_fl.close()
        

def parseargs():
    parser = argparse.ArgumentParser(description="Extract sets of duplicate articles from SOLR query dump")

    parser.add_argument("--input", type=str, required=True, help="SOLR query JSON dump")
    parser.add_argument("--output", type=str, required=True, help="Duplicate set JSON output")

    return vars(parser.parse_args())

if __name__ == "__main__":
    args = parseargs()

    run(args)

