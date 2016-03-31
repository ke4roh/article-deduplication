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
import csv
import math
import random

import numpy as np

def run(args):
    bin_width = args["bin_width"]
    
    with open(args["distances"]) as distances_fl:
        dist_triplets = [(left, right, float(dist)) for left, right, dist in csv.reader(distances_fl, delimiter="\t")]

    with open(args["test_set"]) as testset_fl:
        testset = dict()
        for left, right, duplicate_text in csv.reader(testset_fl, delimiter="\t"):
            testset[frozenset((left, right))] = duplicate_text == "YES"
                

    
    distances = [dist for left, right, dist in dist_triplets]
    n_bins = int(math.floor(max(distances) / bin_width) + 1)
    bins_duplicates = [0] * n_bins
    bins_non_duplicates = [0] * n_bins

    for left, right, dist in dist_triplets:
        bin_idx = int(math.floor(dist / bin_width))
        if frozenset((left, right)) in testset:
            if testset[frozenset((left, right))]:
                bins_duplicates[bin_idx] += 1
            else:
                bins_non_duplicates[bin_idx] += 1

    cum_duplicates = np.cumsum(bins_duplicates)
    cum_non_duplicates = np.cumsum(bins_non_duplicates)

    total_duplicates = 0
    for value in testset.values():
        if value:
            total_duplicates += 1

    cum_precision = []
    cum_recall = []

    for dups, nondups in zip(cum_duplicates, cum_non_duplicates):
        prec = float(dups) / float(dups + nondups)
        recall = float(dups) / float(total_duplicates)

        cum_precision.append(prec)
        cum_recall.append(recall)
        

    if args["output"] is not None:
        with open(args["output"], "w") as output_fl:
            writer = csv.writer(output_fl, delimiter="\t")
            for bin_idx, (prec, recall) in enumerate(zip(cum_precision, cum_recall)):
                lowerbound = float(bin_idx) * bin_width
                writer.writerow([lowerbound, prec, recall])
                 

def parseargs():
    parser = argparse.ArgumentParser(description="Sample predicted duplicate pairs.")

    parser.add_argument("--distances", type=str, required=True, help="Distances file")
    parser.add_argument("--bin-width", type=float, required=True, help="Bin width")
    parser.add_argument("--test-set", type=str, required=True, help="Test set file")
    parser.add_argument("--output", type=str, help="Output file")

    return vars(parser.parse_args())

if __name__ == "__main__":
    args = parseargs()

    run(args)
