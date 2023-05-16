# Preamble

weaves

Revised for Spark 3. Scala version changed to align with Apache Spark.

# Spark development

## Spylon Kernel

Accessing Spark through a notebook.

Spark is now built with Scala 2.12. The SBT build should use the same.

### Installing for Jupyter

This works with the systemd-user service: jupyter. It is configured to use j1.

#### Jupyter client to Spark install with Conda to the toot environment

Within your Anaconda, install these.

 - findspark (PySpark)
 - spylon (Spylon - Scala)
 
#### Cluster - Hadoop administrator

Machine k1 will start the new Spark/Hadoop installation via the
systemd-system services hadoop and hive.

#### Client configuration

The Spark/Hadoop environment is set in the environment files used by the
Jupyter systemd-user service. /home/hadoop/root/etc/x-hadoop.env

#### Client Application

The command

    systemctl --user start jupyter
    
will start the jupyter notebook application. Use the spark notebooks to
verify it is working.

# Scala : spylon kernel

Use the notebook spark-util.ipynb as a guide. Note the use of
/misc/build/0/classes. If you change the SBT build environment you need to
keep this link up to date to access artikus.spark

There is now a Latent Dirichlet Allocation method.

# Python : findspark kernel

spark0.ipynb spark1.ipynb

Then spark0.ipynb will load and run.

spark0 is a Word2Vec demo.

# Postamble

This file's Emacs file variables

[  Local Variables: ]
[  mode:markdown ]
[  mode:outline-minor ]
[  mode:auto-fill ]
[  fill-column: 75 ]
[  coding: utf-8 ]
[  comment-column:50 ]
[  comment-start: "[  "  ]
[  comment-end:"]" ]
[  End: ]

