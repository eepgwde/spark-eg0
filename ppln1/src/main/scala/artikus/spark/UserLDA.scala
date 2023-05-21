package artikus.spark

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.functions.{array_to_vector, vector_to_array}
import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotators.Tokenizer
import com.johnsnowlabs.nlp.annotators.Normalizer
import com.johnsnowlabs.nlp.annotators.StopWordsCleaner
import com.johnsnowlabs.nlp.annotators.Stemmer
import com.johnsnowlabs.nlp.Finisher
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}
import com.typesafe.scalalogging.Logger

import java.io.{FileInputStream, FileNotFoundException, FileOutputStream, IOException, ObjectInputStream, ObjectOutputStream}
import scala.collection.mutable
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.functions.{array_max, col, sum}
import org.apache.spark.sql.types.{ArrayType, ByteType, DoubleType, IntegerType, StringType, StructType}

import java.net.URI
import scala.math.Ordered.orderingToOrdered
import scala.math.Ordering.Implicits.infixOrderingOps

case class Scores0(id: Int, indices: mutable.WrappedArray[Int], scores: mutable.WrappedArray[Double])

case class Scores1(word: String, score: Double)

/**
 * Allows for archival of the whole object.
 */
object UserLDA {
  val logger: Logger = Logger("UserLDA")

  val basePath = new URI(Session0.hadoop0.get("fs.defaultFS"))
  val userBase =  new Path(new URI(Session0.hadoop0.get("fs.defaultFS") + "/user/" + Session0.getUsername() ))
    // s"${basePath}/user/${Session0.getUsername()}"

  def prefix0(path: String) = {
    var tpath = path
    if (!tpath.startsWith("/")) tpath = "/" + tpath
    new Path(tpath)
  }

  val serialName = "UserLDA.ser"

  def serialize(in0: UserLDA, path: String = serialName) = {
    logger.info(s"modeller: in0: ${in0.hashCode()}")
    val p0 = Path.mergePaths(UserLDA.userBase, prefix0(in0.initial0))
    val p1 = Path.mergePaths(p0, prefix0(path))

    logger.info(s"serialize: ${p1}")
    val d0 = p1.getParent()

    // try and make the directory
    try {
      Session0.fs.mkdirs(d0)
    } catch {
      case e0: FileNotFoundException => logger.info("directory not present: " + d0)
      case _: Throwable => logger.warn(s"serialize: directory may exist")
    } finally {
      Session0.logFileStatus(d0)
    }

    val p2 = Session0.fs.create(p1)
    val oos = new ObjectOutputStream(p2)
    oos.writeObject(in0)
    oos.close
    Session0.logFileStatus(p1)
    ()
  }

  def unserialize(path: Option[Path] = None)  = {
    var t0: Option[UserLDA] = None
    var tpath = path

    if (path.isEmpty) {
      // get the most recent
      // Get all the directories
      val g0 = Session0.fs.globStatus(new Path(s"${UserLDA.userBase}/*T*Z"))
      val gmax = g0.filter(_.isDirectory).map(_.getModificationTime).max
      logger.info(s"gmax: ${gmax}")
      val first = g0.filter(_.getModificationTime == gmax)
      logger.info(s"first: length: ${first.length}; first: ${first.head}; path: ${first.head.getPath}")
      tpath = Some(Path.mergePaths(first.head.getPath, prefix0(serialName)))
    }
    val r0 = Session0.logFileStatus(tpath.get)
    if (!r0.isEmpty) {
      val ois = new ObjectInputStream(Session0.fs.open(tpath.get)) {
        override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
          try {
            Class.forName(desc.getName, false, getClass.getClassLoader)
          }
          catch {
            case ex: ClassNotFoundException => super.resolveClass(desc)
          }
        }
      }
      t0 = Some(ois.readObject.asInstanceOf[UserLDA])
     }
    t0
  }
}

/**
 * Natural Language Processing pipeline.
 *
 * This is prototyped in the Jupyter notebook scala-lda.
 *
 * The input and model parameters for this class make for very fast processing, but very poor performance.
 * It only takes 30 seconds in this default configuration and returns no messages assigned a topic classification.
 *
 * This method [[quality0]] shows the hit-rate - it should return a non-empty array.
 * The test class [[UserLDATest]] has some settings on the demonstration data (ABC news headlines) that can give
 * a hit of 3, but takes nearly 10 minutes on
 *
 * @groupprio 10 Input
 * @groupname Input Input Parameters
 * @roupdesc Input Parameters to be set for input processing
 * @groupprio 20 Model
 * @groupname Model Model Parameters
 * @roupdesc Model Parameters to be set for model fitting
 * @groupprio 30 Output
 * @groupname Output Output Metrics
 * @roupdesc Output Values to be used for output validation
 *
 */
class UserLDA extends Serializable {
  val logger: Logger = UserLDA.logger

  /**
   * Unique timestamp and identity for this object.
   */
  val initial0 = DateID.initial0

  // collates the input.
  val document_assembler = new DocumentAssembler().setInputCol("headline_text").setOutputCol("document").setCleanupMode("shrink")

  // clean unwanted characters and garbage
  val tokenizer = new Tokenizer().setInputCols(Array("document")).setOutputCol("token")

  val normalizer = new Normalizer().setInputCols(Array("token")).setOutputCol("normalized")

  // remove stopwords
  val stopwords_cleaner = new StopWordsCleaner().setInputCols("normalized").setOutputCol("cleanTokens").setCaseSensitive(false)

  // stem the words to bring them to the root form.
  val stemmer = new Stemmer().setInputCols(Array("cleanTokens")).setOutputCol("stem")

  // Finisher is the most important annotator.
  // Spark NLP adds its own structure when we convert each row in the dataframe to document.
  // Finisher helps us to bring back the expected structure viz. array of tokens.
  val finisher = new Finisher().setInputCols(Array("stem")).setOutputCols(Array("tokens"))
    .setOutputAsArray(true).setCleanAnnotations(false)

  val stages = Array(document_assembler, tokenizer, normalizer, stopwords_cleaner, stemmer, finisher)
  val nlp_pipeline = new Pipeline().setStages(stages)

  /**
   * Transform the text to produce tokens.
   *
   * This is the text pre-processing process. All of the intermediate stages are contained within it.
   *
   * @param df0 input text, loaded from somewhere, has a column called headline_text
   * @return a table containing a column for each transformation.
   */
  def pipeline0(df0: DataFrame): DataFrame = {
    val nlp_model = nlp_pipeline.fit(df0)
    val df1 = nlp_model.transform(df0)
    stage0 = Some(df1)
    df1
  }

  @transient var stage0: Option[DataFrame] = None

  /**
   * The vocabulary size.
   *
   * Constructor parameter for [[cv]]
   * Used by the [[org.apache.spark.ml.feature.CountVectorizer]]
   * @group Input
   */
  var vocabN: Int = 500

  /**
   * Removal of infrequent items.
   *
   * For example: minDF = 0.01 means "ignore terms that appear in less than 1% of the documents".
   * minDF = 5 means "ignore terms that appear in less than 5 documents".
   *
   * The default minDF is 1, which means "ignore terms that appear in less than 1 document".
   * Thus, the default setting does not ignore any terms.
   *
   * Constructor parameter for [[cv]]
   * Used by the [[org.apache.spark.ml.feature.CountVectorizer]]
   *
   * @group Input
   */
  var minTF: Double = 3.0

  /**
   * The word count vectorizer pipeline component.
   *
   */
  def countVectorizer() = {
    val cv = new CountVectorizer().setInputCol("tokens").setOutputCol("features").setVocabSize(vocabN).setMinTF(minTF)
    cv
  }

  /**
   * The word count vectorizer model
   *
   * This will serialize.
   */
  var cv_model: Option[CountVectorizerModel] = None

  /**
   * The number of tokens to process from the text.
   *
   * For testing the processing, use a value of 100. For statistical analysis, use a very large number 100000 or more.
   * It has a maximal value of size of the set of unique tokens.
   *
   * @group Model
   */
  var tokensN: Int = 10000

  /**
   * Contains the vocabulary - all the stemmed tokens
   */
  var vocab: Option[Array[String]] = None

  /*
  root
   |-- publish_date: integer (nullable = true)
   |-- tokens: array (nullable = true)
   |    |-- element: string (containsNull = true)
   |-- features: struct (nullable = true)
   |    |-- type: byte (nullable = true)
   |    |-- size: integer (nullable = true)
   |    |-- indices: array (nullable = true)
   |    |    |-- element: integer (containsNull = true)
   |    |-- values: array (nullable = true)
   |    |    |-- element: double (containsNull = true)
   */

  val featuresSchema = new StructType()
    .add("type", ByteType, false)
    .add("size", IntegerType, false)
    .add("indices", ArrayType(IntegerType), true)
    .add("values", ArrayType(DoubleType), true)


  val recordSchema = new StructType()
    .add("publish_date", IntegerType, false)
    .add("tokens", ArrayType(StringType), false)
    .add("features", featuresSchema, false)

  /**
   * Calculates the sum of the maxima of each row of features.
   *
   * This should be the sum of the counts, but only the max is implemented.
   * @param df0
   * @return sum of the maximum of each of the rows
   */
  def sumArray(df0: DataFrame) = {
    val df2 = df0.withColumn("f1", array_max(vector_to_array(col("features"))))
    val total0 = df2.select(sum(col("f1"))).first.get(0).asInstanceOf[Double]
    total0
  }

  /**
   * Count vectorization of the tokens.
   *
   * This can produce useless results if all the counts are null. There is an internal test for this.
   *
   * @param df0 just the tokens from pipeline0
   * @return
   */
  def pipeline1(df0: DataFrame): Option[DataFrame] = {
    // vectorized tokens
    val cv = countVectorizer()

    val df1 = if (tokensN > 0)
      df0.select("publish_date","tokens").limit(tokensN)
      else df0.select("publish_date","tokens")

    stage1 = None
    vocab = None
    val cvModel = cv.fit(df1)
    val df2 = cvModel.transform(df1)

    val cnt0 = sumArray(df2)
    logger.info(s"pipeline1: features-sum: ${cnt0}")
    // this isn't right, the schema has a Vector in it.
    // val df3 = Session0.instance.createDataFrame(df2.rdd, recordSchema)
    try {
      stage1 = if (cnt0 > 0) Some(df2) else None
      vocab = Some(cvModel.vocabulary)
    } catch {
      case e0: Throwable => logger.debug(s"pipeline1: failure: ${e0.getStackTrace}")
    }

    stage1
  }

  @transient var stage1: Option[DataFrame] = None

  /**
   * The number of topics cluster around
   *
   * A low integer, typically 3 or 5.
   *
   * @group Model
   */
  var topicsN = 5

  /**
   * The number of iterations.
   *
   * Low value of 100, and can go up to 100000.
   *
   * @group Model
   */
  var itersN: Int = 10

  /**
   * This stores the log likelihood and the log perplexity of the model.
   *
   * The log likelihood is usually negative and it should be maximized.
   * The perplexity is an indicator of how well the topics are clustering.
   *
   *  `bounds = ( model.logLikelihood(vdf0), model.logPerplexity(vdf0) )`
   *  [log likelihood](https://en.wikipedia.org/wiki/Likelihood_function)
   *  [log perplexity](https://en.wikipedia.org/wiki/Perplexity)
   *
   * @group Output
   */
  var bounds = ( 0.0, 0.0 )

  /**
   * The LDA model.
   *
   * This the model used for topic fitting. It can be stored to disk and re-used to transform new messages.
   *
   * This can be serialized.
   *
   * @group Output
   */
  var model0 : Option[LDAModel] = None

  /**
   * Applies LDA for topic-modelling.
   *
   * It sets [[model0]] [[bounds]] and [[transformed]]
   *
   * This produces an RDD that only has as many rows as there are topics, so it has been converted to a list.
   */
  def pipeline2(vdf0: DataFrame): List[Scores0] = {
    val lda = new LDA().setK(topicsN).setMaxIter(itersN)
    val model = lda.fit(vdf0)
    model0 = Some(model)
    bounds = ( model.logLikelihood(vdf0), model.logPerplexity(vdf0) )
    val topics = model.describeTopics(topicsN)
    transformed = Option(model.transform(vdf0))
    topics.rdd.collect().toList.map(x => new Scores0(x.getInt(0),
      x.getAs[mutable.WrappedArray[Int]](1),
      x.getAs[mutable.WrappedArray[Double]](2)) )
  }

  /**
   * The transformed messages.
   *
   * These will have a column of topic scores, from which one can deduce which message falls within which topic.
   *
   * These are the topics. They cannot be serialized.
   *
   * @group Output
   */
  @transient var transformed: Option[DataFrame] = None

  // var ds1 : Option[Serializable] = None
  var desc : Option[List[List[Scores1]]] = None

  /**
   * Topic and Vocabulary scores
   *
   * @param byTopic the output of [[pipeline2]]
   * @return
   */
  def pipeline3(byTopic: List[Scores0]) : Option[ List[List[Scores1]] ] = {

    val desc0 = byTopic.map(x => x.indices.map(vocab.get).zip(x.scores))
    val desc1 = desc0.map( _.map(x => new Scores1(x._1, x._2)).toList )

    desc = Some(desc1)
    desc
  }

  /**
   * Write the output of a pipeline to Hive.
   *
   * This is for [[pipeline0]] and its output [[stage0]]. It is written to a final table called `stage0`.
   */
  def archiver(df1: Option[DataFrame], tname: String, reload: Boolean = false) {
    if (df1.isEmpty) throw new IllegalStateException(s"data frame is empty for: ${tname}")
    val df0 = df1.get

    val aname = s"${tname}_${initial0}"
    val spark = df0.sqlContext

    // backup to one tagged with initial0
    spark.sql(s"drop table if exists ${aname}")
    if (Session0.instance.catalog.tableExists(tname)) {
      spark.sql(s"create table ${aname} as select * from ${tname}")
      spark.sql(s"drop table if exists ${tname}")
    }

    df0.createTempView(s"t${aname}")
    spark.sql(s"create table ${tname} AS select * from t${aname}")
    spark.sql(s"drop table if exists t${aname}")

    logger.info(s"table-names: ${spark.tableNames()}")
    ()
  }

  def archive0() = archiver(stage0, "stage0")

  def archive1() = archiver(stage1, "stage1")

  def archive2() = archiver(transformed, "transformed")


  // Fast sum for an array.
  def adSum(ad: Array[Double]) = {
    var sum = 0.0
    var i = 0
    while (i < ad.length) {
      sum += ad(i);
      i += 1
    }
    sum
  }

  /**
   * Count the messages that have had topics assigned.
   *
   * @return those records in (transformed) that are non-zero for one topic.
   */
  def quality0() : Array[Double] = {
    if (transformed.isEmpty) return Array[Double]();

    val tr0 = transformed.get.select("topicDistribution").collect()
    val tr1 = tr0.map(_(0)).map(_.asInstanceOf[org.apache.spark.ml.linalg.DenseVector].toArray)
    tr1.map(x => adSum(x) ).filter(_ > 0)
  }

  /**
   * Print some details to the logger.
   *
   * There's a topics matrix.
   * https://spark.apache.org/docs/latest/mllib-clustering.html#latent-dirichlet-allocation-lda
   */
  def display() {
    logger.info(s"topics: ${topicsN} " + s"vocabulary: size: ${model0.get.vocabSize}")

    val topics = model0.get.topicsMatrix
    for (topic <- Range(0, topicsN)) {
      var s0 = s"Topic: $topic :"
      for (word <- Range(0, model0.get.vocabSize)) {
        s0 += s"${topics(word, topic)} "
      }
      logger.info(s0)
    }

    val x0 = desc.get.zipWithIndex.foreach {
      case (y, i) => {
        logger.info("::" + i);
        y.map(x => logger.info(x.word + " :: " + x.score));
      }
    }
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject
    // transformed is transient
    transformed = None; stage0 = None
  }

}
