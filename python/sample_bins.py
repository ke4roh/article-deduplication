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

def run(args):
    with open(args["distances"]) as distances_fl:
        triplets = [(left, right, float(dist)) for left, right, dist in csv.reader(distances_fl, delimiter="\t")]

    bin_width = args["bin_width"]
    distances = [dist for left, right, dist in triplets]
    n_bins = int(math.floor(max(distances) / bin_width) + 1)
    bins = [[] for i in xrange(n_bins)]

    for left, right, dist in triplets:
        bin_idx = int(math.floor(dist / bin_width))
        bins[bin_idx].append((left, right, dist))

    bin_samples = []
    n_samples = args["n_samples"]
    for bin in bins:
        samples = random.sample(bin, n_samples)
        bin_samples.append(samples)

    with open(args["output"], "w") as output_fl:
        writer = csv.writer(output_fl, delimiter="\t")
        for bin_idx, bin in enumerate(bin_samples):
            for left, right, dist in bin:
                lowerbound = float(bin_idx) * bin_width
                writer.writerow([lowerbound, left, right, dist])
                 

def parseargs():
    parser = argparse.ArgumentParser(description="Sample predicted duplicate pairs.")

    parser.add_argument("--distances", type=str, required=True, help="Distances file")
    parser.add_argument("--bin-width", type=float, required=True, help="Bin width")
    parser.add_argument("--n-samples", type=int, required=True, help="Number of samples per bin")
    parser.add_argument("--output", type=str, required=True, help="Output file")

    return vars(parser.parse_args())

if __name__ == "__main__":
    args = parseargs()

    run(args)
