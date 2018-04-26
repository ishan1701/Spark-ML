package kumar

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.ml.classification._
import org.apache.spark.sql.cassandra
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.linalg.{ Vector, Vectors }
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.DoubleType

object MainObject {
  def coverttoFeatures(df: org.apache.spark.sql.DataFrame) = {
    val label = df.select("Result_of_Treatment")

  }
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(MainObject.getClass)
    logger.setLevel(Level.ERROR)
    logger.info("Staring the Spark application")
    val conf = new SparkConf().setMaster("local").setAppName("aaa")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val options = Map("inferSchema" -> "true", "header" -> "true")
    logger.info("Uploading the csv file for feature extraction")
    val df = spark.read.options(options).csv("G:\\workspace\\kumar\\src\\main\\resources\\myNew.csv")
    val parsedDF = df.select("sex", "age", "Time", "Number_of_Warts", "Type", "Area", "Result_of_Treatment")
    val assembler = new VectorAssembler().setInputCols(Array("sex", "age", "Time", "Number_of_Warts", "Type", "Area"))
      .setOutputCol("features")
    val output = assembler.transform(parsedDF).select($"Result_of_Treatment" as "label", $"features")
    val Array(training, test) = output.randomSplit(Array(0.7, 0.3))
    //training.show(false)
    val estimator = new LogisticRegression()
    val model = estimator.fit(training)
    val prediction = model.transform(test).select("label", "prediction")
    prediction.show(100, false)

  }
}

