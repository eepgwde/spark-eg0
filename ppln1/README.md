# Preamble

weaves

Revised for Spark 3. Scala version changed to align with Apache Spark.

# Topic Modelling - Latent Dirichlet Allocation

This follows the [article](https://medium.com/analytics-vidhya/distributed-topic-modelling-using-spark-nlp-and-spark-mllib-lda-6db3f06a4da3)
with the title "Distributed Topic Modelling using Spark NLP and Spark MLLib(LDA)".

The Spark LDA implementation is given in their M[Llib](https://spark.apache.org/docs/latest/ml-guide.html) guide.
It appears in the *Clustering* section.

This makes use of the [John Snow NLP components](https://github.com/JohnSnowLabs/spark-nlp).
These can be loaded by Ivy by Spark if the `spark.jars.packages` configuration parameter set to
`"com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.2"`.

# System

## Design

It is hoped that a configurable package that can be run repeatedly on the same corpus of texts.

The corpus can be stored within the cluster on HDFS or within Hive.

A series of Spark jobs can then be submitted to analyze the texts under different configurations. This would use a
some cross-validation resampling to find some robust topics.

### Technology Issues

Using Hive to capture the data frames is very slow. 

### Pre-processing 

The Natural Language Processing pipeline converts texts to counts of words.

### Count Vectorization

This processes the tokens from the texts and assigns counts for each of the words. The output is a set of counts for
each message. This is the input to the LDA model. And it should be similar to that given by Spark in their *Clustering*
example.

The output of this stage is problematic. It is stored as a sparse array, but it is presented as a summary array.
Ordinarily, it is not necessary to process the `features`, but it was discovered, that when the tokens are reduced 
to speed up the testing.

### LDA

This derives Topics. These are defined as clusters of words that partition the messages in some near optimal way.

The model's output is a Topic matrix and a transformer that assigns a Topic to a message.

## Implementation

### Testing with Classes and Jar files

Jupyter notebook Spylon and Python can access the classes by /misc/build/0/classes. This should be set to be a soft 
link like so `classes -> spark-eg0/ppln1/target/scala-2.12/classes`. Unfortunately, the classes trick does not work.
It is better to build a Fat Jar and add that to the launcher.jars array.

[sbt-assembly](https://github.com/sbt/sbt-assembly)  as directed in [Baeldung](https://www.baeldung.com/scala/sbt-fat-jar)

### Stages

`Stage1Test` demonstrates how to run `pipeline0` and store its results as `stage0`.

### Serialization

The key class is `UserLDA`. It can serialize itself, but not the data frames within it. These are stored on Hive.

This is very slow, so a state machine is used internally to manage which tables are archived and restored.

### Hive

It is useful to store the intermediate table in Hive. Hive now runs in multi-user mode. It uses PostgreSQL on 
another server and Hive runs on the Hadoop server.

The schema of tables stored on Hive is different from when their representation within Spark. It differs in the
representation of vectors. This means there are specialized archive and unarchive methods for the Count Vectorisation
data frame.

Hive maintenance can be done with Jupyter and using Spark SQL, but Hive has a command-line client called Beeline. 
Invoked as simply `beeline`, it supports a ReadLine interface. There is a default username and password of scott 
and tiger.

    jdbc:hive2://k1:10000> !connect jdbc:hive2://k1:10000 scott tiger

### Logging

Spark uses log4j. I'm using ch.qos.logback, and it reports there are two logging implementations. I cannot use 
just one.

### Encoder

Some issues with the Encoder for an object. In the end, it wasn'nt needed. Here is an example found somewhere. 

    implicit val encodeEmployee: Encoder[Employee] = new Encoder[Employee] {
       final def apply(a: Employee): Json = Json.obj(
          ("name", Json.fromString(a.name)),
          ("password", Json.fromString("[REDACTED]")),
       )
    }

# Postamble

Markdown reference.
https://daringfireball.net/projects/markdown/syntax

Spark Configuration
https://spark.apache.org/docs/latest/configuration.html#spark-properties

Spark ScalaDoc API reference
https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html

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

