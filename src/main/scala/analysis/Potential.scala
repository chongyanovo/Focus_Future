package analysis

import analysis.Ratio.OutputPath
import core.SparkCore
import org.apache.spark.sql.functions.{avg, concat_ws, sum}

object Potential {
  private val InputPath: String = "input/ETL_data.csv"
  private val OutputPath: String = "output/1.3"

  def main(args: Array[String]): Unit = {
    val spark = SparkCore.create("Potential", "local[*]")
    import spark.implicits._
    val df = spark.read.option("header", true).option("inferSchema", true).csv(InputPath)
    val df_new = df.withColumn("行业", concat_ws(String.valueOf("_"), df.col("一级行业"), df.col("二级行业"))).drop("一级行业", "二级行业")

    val frame = df_new.groupBy("行业").agg(avg(df_new("行业投入")) / avg(df_new("询盘人数")))



    val df_p1 = frame.select(frame("行业"), frame("(avg(行业投入) / avg(询盘人数))") as ("二级潜力比率"))
    val value = df_p1.filter(df_p1("二级潜力比率").isNotNull).orderBy("二级潜力比率").limit(10)

    value.repartition(1).write
      .option("header", "true")
      .csv(OutputPath)
    spark.stop()
  }
}
