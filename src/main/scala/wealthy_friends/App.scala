package wealthy_friends

import org.apache.spark.sql.SparkSession
import scopt.OParser
import better.files.File

object App {
  def main(args: Array[String]): Unit = {
    println(fansi.Color.Blue("Greetings! It's 'Wealthy Friends' application"))

    val cliArgsBuilder = OParser.builder[CliArgs]

    val cliArgsParser = {
      import cliArgsBuilder._
      OParser.sequence(
        programName("Wealthy Friends"),
        head("To find that lucky guy!"),
        opt[String]('i', "input-dir")
          .action((dir, c) => c.copy(inputDir = Some(dir)))
          .validate(dir => {
            val file = File(dir)
            if (file.exists && file.isDirectory) Right(()) else Left(s"$dir does not exist or is not a directory!")
          })
          .text("An absolute path to directory with input data"),
      )
    }

    OParser.parse(cliArgsParser, args, CliArgs()).toRight("CLI arguments fail") match {
      case Left(err) =>
        println(fansi.Color.Red(err))
        System.exit(1)

      case Right(cliArgs) =>
        cliArgs.inputDir match {
          case Some(inputDir) =>
            val spark = SparkSession
              .builder
              .appName("Wealthy Friends")
              .config("spark.master", "local")
              .getOrCreate()

            val processor = Processor(spark, inputDir)

            println(s"${fansi.Color.Blue("findUser_ScalaWay: ")}${fansi.Color.Green(processor.findUser_ScalaWay())}")
            println(s"${fansi.Color.Blue("findUser_SqlWay  : ")}${fansi.Color.Green(processor.findUser_SqlWay())}")

            spark.stop()

          case None => println(fansi.Color.Blue("No inputDir has been provided. Exit."))
        }
    }
  }
}
