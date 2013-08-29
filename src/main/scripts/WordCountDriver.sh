#!/bin/sh

export BASE_DIR=~/workspace/related-search
export JARFILE=hadoop-${version}-job.jar
export mc=hadoop.WordCount

echo $mc is selected

echo $r run hadoop script
hadoop fs -rmr output_tmp
hadoop jar target/$JARFILE $mc input output_tmp

echo $b
hadoop fs -cat output_tmp/part-*

echo $g job completed

