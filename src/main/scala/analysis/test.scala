//package analysis
//
//import core.SparkCore
//import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
//import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
//import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.{concat_ws, count, sum}
//import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
//
//import scala.::
//
//object test {
//  private val InputPath: String = "input/ETL_data.csv"
//  private val OutputPath: String = "output/1.5"
//
//
//
//  def main(args: Array[String]): Unit = {
//    val spark = SparkCore.create("test", "local[*]")
//    import spark.implicits._
//    val df = spark.read.option("header", true).option("inferSchema", true).csv(InputPath)
//    val (assembler, logisticRegressionModelLoaded) = getModel(spark)
//    val df_new = df.withColumn("行业", concat_ws(String.valueOf("_"), df.col("一级行业"), df.col("二级行业"))).drop("一级行业", "二级行业")
//
//    val frame = df_new.groupBy("行业").sum("行业投入")
//    // 计算续约率
//    val dfRe = df_new.groupBy("行业").agg(sum("续约标识") / count("续约标识") as ("续约率"))
//    //  筛选出小于70%的续约率
//    val df_low = dfRe.filter(dfRe("续约率") < 0.7)
//    val frame1 = df_low.join(frame, Seq("行业"), "left")
//    val df1 = frame1.select(frame1("续约率"), frame1("sum(行业投入)") as ("sum_行业投入"))
//
//    val df2 = assembler.transform(df1)
//    val df3 = logisticRegressionModelLoaded.transform(df2)
//    df3.show()
//  }
//
//   def getModel(session: SparkSession):(VectorAssembler,LogisticRegressionModel)={
//
//     val schema = StructType(
//       StructField("行业", StringType, nullable = true) ::
//       StructField("续约率", DoubleType, nullable = true) ::
//         StructField("sum_行业投入", DoubleType, nullable = true) ::
//
//         StructField("result", DoubleType, nullable = true) ::
//         Nil
//     )
//     val spark = SparkCore.create("test", "local[*]")
//     import spark.implicits._
//     val df = spark.read.option("header", true).option("inferSchema", true).csv(InputPath)
//
//     val df_new = df.withColumn("行业", concat_ws(String.valueOf("_"), df.col("一级行业"), df.col("二级行业"))).drop("一级行业", "二级行业")
//
//     val frame = df_new.groupBy("行业").sum("行业投入")
//     // 计算续约率
//     val dfRe = df_new.groupBy("行业").agg(sum("续约标识") / count("续约标识") as ("续约率"))
//     //  筛选出小于70%的续约率
//     val df_low = dfRe.filter(dfRe("续约率") < 0.7)
//     val frame1 = df_low.join(frame, Seq("行业"), "left")
//
//     val cols = Array( "续约率", "sum(行业投入)")
//     val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("预测续约率")
//     val featureDf = assembler.transform(frame1)
//
//     val indexer = new StringIndexer().setInputCols(cols).setOutputCol("label")
//     val labelDf = indexer.fit(featureDf).transform(featureDf)
//     val seed = 5043
//
//     val Array(trainingData,testData) = labelDf.randomSplit(Array(0.7, 0.3))
//     val logisticRegression = new LogisticRegression()
//       .setMaxIter(100)
//       .setRegParam(0.02)
//       .setElasticNetParam(0.8)
//
//     val logisticRegressionModel = logisticRegression.fit(trainingData)
//     val predictionDf = logisticRegressionModel.transform(testData)
//     val evaluator = new BinaryClassificationEvaluator()
//       .setLabelCol("label")
//       .setRawPredictionCol("prediction")
//       .setMetricName("areaUnderROC")
//     val accuracy = evaluator.evaluate(predictionDf)
//     println("预测精度"+accuracy)
//     logisticRegressionModel.write.overwrite().save("model")
//
//     val logisticRegressionModelLoade = LogisticRegressionModel.load("s-model")
//     (assembler,logisticRegressionModelLoade)
//
//
//   }
//}
