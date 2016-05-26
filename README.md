# Article Deduplication App

A simple app that reports near duplicate articles using distance in a vector-space model.

Currently, the repository is missing example articles as well as examples of the filter and replacement word lists necessary for running the code. However, I hope that the code will serve as a useful example.

# Instructions

Build:

    $ sbt assembly

A jar (`dedup-assembly-0.1.jar`) will be created under `target/scala_2.10`.

Import data:

Create a directory called `workdir` for intermediate files.

    $ spark-submit --master local[20] \
    --executor-memory 200G \
    --driver-memory 200G \
    --class com.redhat.et.DedupApp
    target/scala_2.1.0/dedup-assembly-0.1.jar \
    --work-dir workdir \
    import-data \
    --articles /path/to/article.json \
    --filter-words /path/to/filter-words.txt \
    --replacement-words /path/to/replacement-words.txt \
    --min-word-count 5


Compute Histogram:
Histogram document pairs based on distances.  The histogram may appear bimodal, suggesting a natural threshold for predictions.

    $ spark-submit --master local[20] \
    --executor-memory 200G \
    --driver-memory 200G \
    --class com.redhat.et.DedupApp
    target/scala_2.1.0/dedup-assembly-0.1.jar \
    --work-dir workdir \
    histogram \
    --bin-width 0.05
    --histogram-file histogram.txt \
    --tfidf \
    --normalize

Predict duplicate pairs:

    $ spark-submit --master local[20] \
    --executor-memory 200G \
    --driver-memory 200G \
    --class com.redhat.et.DedupApp
    target/scala_2.1.0/dedup-assembly-0.1.jar \
    --work-dir workdir \
    rankings \
    --rankings-file rankings.txt \
    --tfidf \
    --normalize \
    --threshold 0.5
    
    
