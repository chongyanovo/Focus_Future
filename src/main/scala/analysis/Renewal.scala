package analysis

import analysis.Ratio.InputPath
import core.SparkCore
//import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.functions.{avg, concat_ws, count, lit, sum}
import org.apache.spark.ml.classification.LogisticRegression

object Renewal {
  private val InputPath: String = "input/ETL_data.csv"
  private val OutputPath: String = "output/1.4"

  def main(args: Array[String]): Unit = {
    val spark = SparkCore.create("Renewal", "local[*]")
    import spark.implicits._
    val df = spark.read.option("header", true).option("inferSchema", true).csv(InputPath)
    val df_new = df.withColumn("行业", concat_ws(String.valueOf("_"), df.col("一级行业"), df.col("二级行业"))).drop("一级行业", "二级行业")
    // 计算续约率
    val dfRe = df_new.groupBy("行业").agg(sum("续约标识") / count("续约标识") as ("续约率"))
    //  筛选出小于70%的续约率
    var df_low = dfRe.filter(dfRe("续约率") < 0.7)
    //  筛选出大于70%的续约率
    val df_hight = dfRe.filter(dfRe("续约率") >= 0.7)

    val df1 = df_new.groupBy("行业").sum("行业投入")
    val df_low1 = df_low.join(df1, Seq("行业"), "left")
    val df_hight1 = df_hight.join(df1, Seq("行业"), "left")
    val df_HS = df_hight1.select(df_hight1("行业"), df_hight1("sum(行业投入)") * df_hight1("续约率") as ("投资率"))

    //    526388
    val df_Havg = df_HS.agg(avg("投资率"))

    val df_LS = df_hight1.select(df_low1("行业"), df_low1("sum(行业投入)") * df_low1("续约率") as ("投资率"))
    val value = df_LS.select(df_LS("行业"), df_LS("投资率") - 526388 as ("投资差额")).orderBy("投资差额")
    value.repartition(1).write
      .option("header", "true")
      .csv(OutputPath)
    spark.stop()
  }
}
