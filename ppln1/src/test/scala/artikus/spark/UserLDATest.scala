package artikus.spark
/** This is the Scaladoc for the package. */

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession}
import org.scalatest.funspec.AnyFunSpec

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.FileSystems
import scala.collection.mutable

/**
 * Prototyping test.
 *
 * This checks the whole pipeline.
 */
class UserLDATest extends AnyFunSpec with org.scalatest.Inspectors
    with org.scalatest.matchers.should.Matchers {

  val logger: Logger = Logger("UserLDATest")

  val modeller = new UserLDA()
  val path = FileSystems.getDefault.getPath("target", "modeller.ser")

  val statTest = false
  val isArchiving = false

  // case class Table1(id: Int, indices: mutable.WrappedArray[Int], scores: mutable.WrappedArray[Double])

  var df1: Option[DataFrame] = None
  var df2: Option[DataFrame] = None
  var topics: Option[List[Scores0]] = None
  var words: Option[List[List[Scores1]]] = None
  var desc: Option[List[mutable.WrappedArray[(String, Double)]]] = None

  var modeller1: Option[UserLDA] = None

  describe ("LDA processing") {
    it("properties") {
      val props = Session0.props()
      logger.info(props.toString())
      props should not be null
      assert(props.size() > 0)
    }
    it("configuration") {
      val conf0 = Session0.configure()
      conf0 should not be null
      logger.info(conf0.toDebugString)
    }
    it("pipeline0 - load and simplify") {
      val session = Session0.instance
      session should not be null

      logger.info(session.version)

      val url = "file:///a/l/X-image/cache/data/abcnews-date-text.csv"

      val type0 = "csv"
      val infer_schema = "true"
      val first_row_is_header = "true"
      val delimiter = ","

      val df0 = session.read.format(type0)
        .option("inferSchema", infer_schema)
        .option("header", first_row_is_header)
        .option("sep", delimiter)
        .load(url)

      df1 = Some(modeller.pipeline0(df0))
      df1 should not be (None)
    }
    it("archive0 - write a table") {
      if (isArchiving) modeller.archive0()
    }
    it("pipeline1 - count vectorization") {
      // set some parameters here
      // this matches the settings in scala-lda and usually hits 3 in just over 10 minutes
      // 4 cores and 4 g
      if (statTest) {
        modeller.itersN = 100
        modeller.tokensN = 100000
        modeller.vocabN = 500
        modeller.minTF = 3.0
      }

      df1 should not be (None)
      df2 = Some(modeller.pipeline1(df1.get))
      df2 should not be (None)
    }
    it("pipeline2 - LDA fit - topics with words then scores") {
      df2 should not be (None)
      topics = Some(modeller.pipeline2(df2.get))
      topics should not be (None)
    }
    it("pipeline3 - topic each word with score") {
      topics should not be (None)

      words = modeller.pipeline3(topics.get)
      words should not be (None)
    }
    it("display - topics") {
      words should not be (None)
      modeller.display()
    }
    it("quality - messages classified") {
      val tr0 = modeller.quality0()
      logger.info(s"quality: ${tr0}")
      // only if we are using a statistical test configuration should we expect any topics.
      if (statTest) {
        assert(tr0.length > 0)
      } else {
        assert(!statTest)
      }
    }
    it("serialization - out") {
      logger.info(s"modeller: out: ${modeller.hashCode()}")
      val oos = new ObjectOutputStream(new FileOutputStream(path.toFile))
      oos.writeObject(modeller)
      oos.close
    }
    it("serialization - in") {
      // this needs some help to resolve classes
      // https://stackoverflow.com/questions/16386252/scala-deserialization-class-not-found
      val ois = new ObjectInputStream(new FileInputStream(path.toFile)) {
        override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
          try {
            Class.forName(desc.getName, false, getClass.getClassLoader)
          }
          catch {
            case ex: ClassNotFoundException => super.resolveClass(desc)
          }
        }
      }
      modeller1 = Some(ois.readObject.asInstanceOf[UserLDA])
      ois.close
    }
    it("use - serialized") {
      modeller1 should not be (None)

      logger.info(s"modeller1: in: ${modeller1.get.hashCode()}")
      val tr0 = modeller1.get.quality0()
      logger.info(s"modeller1: quality: ${tr0}")

      logger.info(s"modeller1: log likelihood: ${modeller1.get.bounds._1}")

      modeller1.get.display()
    }
    it("use - out - serialized2") {
      modeller1 should not be (None)
      UserLDA.serialize(modeller)
    }
    it("use - in - serialized2") {
      modeller1 should not be (None)
      UserLDA.serialize(modeller)
    }
    it("close") {
      Session0.instance.close()
    }
  }
}
