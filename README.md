# Article Deduplication App

A simple app that reports near duplicate articles using distance in a vector-space model.

Currently, the repository is missing example articles as well as examples of the filter and replacement word lists necessary for running the code. However, I hope that the code will serve as a useful example.

# Instructions

Get Apache Spark - last tested with [2.4.3 (May 07 2019) Pre-build for Apache Hadoop 2.7 and later](https://spark.apache.org/downloads.html)

Build:

    $ ./sbt assembly

A jar (`dedup-assembly-0.1.jar`) will be created under `target/scala_2.11`.

Get a stopword list to use:

     wget https://raw.githubusercontent.com/moewe-io/stopwords/master/dist/en/en.json

Get a replacement word list to use, a json dict:

    echo "{}" > replacement-words.json

Format the input documents:

     jq -c '.[] | {article_id: ."solution.id" | tonumber, text: .body | join(" ")}' input.json > docs.json

Import data:

Create a directory called `workdir` for intermediate files.

    spark-submit --master "local[*]" --class com.redhat.et.dedup.DedupApp target/scala_2.11/dedup-assembly-0.1.jar --work-dir workdir import-data --articles docs.json --filter-words en.json --replacement-words replacement-words.json --min-word-count 5


Compute Histogram:
Histogram document pairs based on distances.  The histogram may appear bimodal, suggesting a natural threshold for predictions.

    spark-submit --master "local[*]" --class com.redhat.et.dedup.DedupApp target/scala_2.11/dedup-assembly-0.1.jar --work-dir workdir histogram --bin-width 0.05 --histogram-file histogram.txt --tfidf --normalize

Predict duplicate pairs:

    spark-submit --master "local[*]" --class com.redhat.et.dedup.DedupApp target/scala_2.11/dedup-assembly-0.1.jar --work-dir workdir rankings --rankings-file rankings.txt --tfidf --normalize --threshold 0.5

For large data sets, you may need to add --executor-memory 200G and --driver-memory 200G to the spark-submit lines.
