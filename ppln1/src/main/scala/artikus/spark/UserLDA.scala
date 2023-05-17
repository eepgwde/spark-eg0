package artikus.spark

import com.amazonaws.services.quicksight.model.DataSet
import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotators.Tokenizer
import com.johnsnowlabs.nlp.annotators.Normalizer
import com.johnsnowlabs.nlp.annotators.StopWordsCleaner
import com.johnsnowlabs.nlp.annotators.Stemmer
import com.johnsnowlabs.nlp.Finisher
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.types._
import com.typesafe.scalalogging.Logger

import scala.collection.mutable

class UserLDA {
  val logger: Logger = Logger("UserLDA")

  // Split sentence to tokens(array)
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
   * Produces tokens from the text.
   * @param df0
   * @return
   */
  def pipeline0(df0: DataFrame): DataFrame = {
    val nlp_model = nlp_pipeline.fit(df0)
    nlp_model.transform(df0)
  }

  val cv = new CountVectorizer().setInputCol("tokens").setOutputCol("features").setVocabSize(500).setMinTF(3.0)

  var cv_model: Option[org.apache.spark.ml.feature.CountVectorizerModel] = None

  var tokensN: Int = 100000
  var vocab: Option[Array[String]] = None

  /**
   * Count vectorization of the tokens.
   *
   * @param df0
   * @return
   */
  def pipeline1(df0: DataFrame): DataFrame = {
    // vectorized tokens
    val df1 = df0.select("publish_date","tokens").limit(tokensN)
    val cv_model = cv.fit(df1)
    vocab = Some(cv_model.vocabulary)
    cv_model.transform(df1)
  }

  var topicsN = 5
  var transformed: Option[DataFrame] = None
  var itersN: Int = 100

  var bounds = ( 0.0, 0.0 )

  /**
   * Applies LDA for topic-modelling.
   */
  def pipeline2(vdf0: DataFrame): RDD[Row] = {
    val lda = new LDA().setK(topicsN).setMaxIter(itersN)
    val model = lda.fit(vdf0)
    bounds = ( model.logLikelihood(vdf0), model.logPerplexity(vdf0) )
    val topics = model.describeTopics(topicsN)
    transformed = Option(model.transform(vdf0))
    topics.rdd
  }

  /**
   * Description for a row
   * @param id
   * @param indices
   * @param scores
   */
  case class Table1(id: Int, indices: mutable.WrappedArray[Int], scores: mutable.WrappedArray[Double])
  case class Table2(scores: mutable.WrappedArray[(String, Double)])

  var df2 : Option[Dataset[this.Table1]] = None
  var desc : Option[List[mutable.WrappedArray[(String, Double)]]] = None

  /**
   * Topic and Vocabulary scores
   * @param df1
   * @param spark
   * @return
   */
  def pipeline3(rdd3: RDD[Row]) : List[mutable.WrappedArray[(String, Double)]]  = {
    val spark = SparkSession.builder().getOrCreate()
    logger.debug("pipeline3: spark")
    implicit val scoreEncoder = Encoders.bean[Table1](classOf[Table1])

    val df1 = spark.createDataFrame(rdd3, scoreEncoder.schema)

    logger.debug("pipeline3: df1")
    df2 = Some(df1.as(scoreEncoder))

    implicit val scoreEncoder2 =
      Encoders.bean[mutable.WrappedArray[(String, Double)]](classOf[mutable.WrappedArray[(String, Double)]])

    desc = Some(df2.get.map(x => x.indices.map(vocab.get).zip(x.scores)).collect.toList.map {
      _.map(x => (x._1, x._2))
    })
    desc.get
  }

  /**
   * Print the topics with vocabulary and scores.
   * @param df0
   * @param spark
   */
  def display() {
    desc.get.map(y => { println("::"); y.map(x => logger.info(x._1 + " :: " + x._2) ) } );
  }

}
