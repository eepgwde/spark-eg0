## weaves
#
# An example from 
#
# https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-common/SingleNodeSetup.html#Pseudo-Distributed_Operation
#

# This runs in local mode by default. Pass X_MODE=cluster to use the cluster

# For the default local mode, you may need to run
#  hd_ sys local start
# Although it works well enough without it.

# You will need a copy of the HADOOP_CONF_DIR in a local 'conf' directory
# A soft link to the JAR file.

HELPER_ ?= hd_

X_RUN ?= hadoop
X_PI ?= 10

## Extract the JAR file of examples
X_JAR := $(shell hadoop classpath | ( IFS=:; read i; printf "%s\n" $$i ) | grep 'mapreduce-examples-[0-9.]*\.jar')

X_JAR1 := $(shell find -L $(SP0) -type f -name '*examples*.jar')

# By default use local mode.

ifeq ($(X_MODE),)
X_MODE = client
X_MASTER = local[*]
else
X_MODE = cluster
X_MASTER = yarn
endif

X_APP ?= SimpleApp.py

# local[*]
# client

all: show0 status0 grep0 spark0

view: 
	@echo $(X_JAR)
	@echo $(X_JAR1)
	@echo $(HADOOP_CONF_DIR)

clean: clear0 clear1

status0:
	$(HELPER_) hdfs dfs -ls -R /

clear0:
	$(HELPER_) hdfs dfs -rm -R -f /weaves/output

clear1:
	$(HELPER_) hdfs dfs -rm -R -f /weaves/input


put0:
	-$(HELPER_) hdfs dfs -mkdir -p /weaves/input
	-$(HELPER_) hdfs dfs -put conf /weaves/input

## Using a link back to the distribution

# This is a HADOOP and Map-Reduce job.
grep0: clear0 put0
	 $(X_RUN) jar $(X_JAR) grep /weaves/input/conf /weaves/output 'dfs[a-z.]+'

output: grep0
	rm -rf $@
	-$(HELPER_) hdfs dfs -get /weaves/output $@

show0: output
	cat $(wildcard output/*)

## Using spark

spark0:
	spark-submit --class org.apache.spark.examples.SparkPi --master $(X_MASTER) --deploy-mode $(X_MODE) $(X_JAR1) $(X_PI)

spark1:
	spark-submit --class org.apache.spark.examples.JavaSparkPi --master $(X_MASTER) --deploy-mode $(X_MODE) $(X_JAR1) $(X_PI)

spark-shell:
	spark-shell --master $(X_MASTER) --deploy-mode $(X_MODE)

spark-python0:
	spark-submit --master $(X_MASTER) --deploy-mode $(X_MODE) $(X_APP) $(X_APP_ARGS)
