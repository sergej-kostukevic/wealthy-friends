package wealthy_friends.tools

import munit.FunSuite
import org.apache.spark.sql.SparkSession
import wealthy_friends.Processor

class ProcessorSuite extends FunSuite {

  val spark = SparkSession
    .builder
    .appName("Wealthy Friends")
    .config("spark.master", "local")
    .getOrCreate()

  val DATA_DIR: String = getClass.getResource("/data").getPath
  val INPUT_DIR: String = s"$DATA_DIR/input"
  val DESIRED_ANSWER = "andrea"
  println(INPUT_DIR)

  test("Processor methods") {
    val processor = Processor(spark, INPUT_DIR)

    assert(DESIRED_ANSWER == processor.findUser_ScalaWay())
    assert(DESIRED_ANSWER == processor.findUser_SqlWay())

  }

}
