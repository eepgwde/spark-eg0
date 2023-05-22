#!/bin/bash

eval "$@"

if test -f ~hadoop/etc/x-hadoop.env
then
  set -a
  source ~hadoop/etc/x-hadoop.env
  set +a
fi

if ! which spark-submit
then
  exit 1
fi > /dev/null 2>&1

$nodo spark-submit --class org.apache.spark.examples.SparkPi \
    --master yarn \
    --deploy-mode client \
    ${SPARK_HOME}/examples/jars/spark-examples*.jar \
    10
