Some examples how to use the hd_ script from a user account

Running locally

 hd_ sys run pyspark --help
 hd_ sys run spark-shell --help

 hd_ sys run spark-submit --master 'local[*]' --cluster client SimpleApp.py

Using the Makefile 

 hd_ sys run make spark0
 hd_ sys run make spark-python0

 # interactively under the Makefile
 hd_ sys run make spark-shell

The cluster can be used with

 hd_ sys run make X_MODE=cluster spark0

