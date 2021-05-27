#!/bin/bash

# Remove previous output data

hdfs dfs -rm -r $5 $6 $8

hadoop com.sun.tools.javac.Main src/*.java
jar cf "$1".jar -C src .

# shellcheck disable=SC1037
# shellcheck disable=SC1037
hadoop jar "$1".jar "$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9"

# imdb $1
#0 imdb/titleratings.tsv $2
#1 imdb/titlebasics.tsv $3
#2 imdb/titlecrew.tsv $4
#3 imdb/titleoutput $5
#4 imdb/combinedoutput $6
#5 imdb/namebasics.tsv $7
#6 imdb/getdirectorsnameoutput $8




#7 imdb/tmp/tmmp/part-r-00000 $9
#8 imdb/tmp/tmp2/part-r-00000 $10