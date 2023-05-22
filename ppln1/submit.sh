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

: ${job0:=state}
: ${dmode:=client}

: ${X_JAR:=${PWD}/target/scala-2.12/ppln1-fatjar-0.0.2-SNAPSHOT.jar}

test -f "${X_JAR}" || exit 2

$nodo spark-submit --class artikus.spark.ctl.LDA \
 --master yarn --deploy-mode ${dmode} \
 -c spark.sql.catalogImplementation=hive \
 -c spark.sql.warehouse.dir=file:///home/hadoop/data/hive \
 -c spark.hadoop.fs.permissions.umask-mode=002 \
 --name LDA-${job0} \
 --jars "/usr/share/java/postgresql.jar" \
 --packages "com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.2,ch.qos.logback:logback-classic:1.2.10,com.typesafe.scala-logging:scala-logging_2.12:3.9.5" \
${X_JAR} ${job0} ${xargs}



