package analysis

import analysis.Ratio.OutputPath
import core.SparkCore
import org.apache.spark.sql.functions.concat_ws

object InvestmentSum {
  private val InputPath: String = "input/ETL_data.csv"
  private val OutputPath: String = "output/1.2"

  def main(args: Array[String]): Unit = {

    val spark = SparkCore.create("InvestmentSum", "local[*]")
    import spark.implicits._
    val df = spark.read.option("header", true).option("inferSchema", true).csv(InputPath)
    val df_new = df.withColumn("行业", concat_ws(String.valueOf("_"), df.col("一级行业"), df.col("二级行业"))).drop("一级行业", "二级行业")

    val frame = df_new.groupBy("行业").sum("行业投入")

    val sum_I = frame.select(frame("行业"), frame("sum(行业投入)") as ("sum_行业投入"))

    sum_I.repartition(1).write
      .option("header", "true")
      .csv(OutputPath)

    spark.stop()
  }

}
