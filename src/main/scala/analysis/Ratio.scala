package analysis


import core.SparkCore
import org.apache.spark.sql.functions.{concat_ws, sum}

object Ratio {
  private val InputPath: String = "input/ETL_data.csv"
  private val OutputPath: String = "output/1.1"
  def main(args: Array[String]): Unit = {
    val spark = SparkCore.create("Ratio","local[*]")
    import spark.implicits._
    val df = spark.read.option("header", true).option("inferSchema", true).csv(InputPath)
    val df_new = df.withColumn("行业", concat_ws(String.valueOf("_"), df.col("一级行业"), df.col("二级行业"))).drop("一级行业", "二级行业")

//    df_new.show()
    val df_01 = df_new.groupBy("行业").agg( (sum(df_new("曝光人数"))/sum(df_new("曝光数量"))) as("曝光比率"))
    val df_02 = df_new.groupBy("行业").agg( (sum(df_new("访问人数"))/sum(df_new("访问数量"))) as("访问比率"))
   val df_03 = df_new.groupBy("行业").agg( (sum(df_new("询盘人数"))/sum(df_new("询盘数量"))) as("询盘比率"))
   val df_04 = df_new.groupBy("行业").agg( (sum(df_new("行业投入"))/sum(df_new("曝光数量"))) as("投入比率"))
   val df_05 = df_new.groupBy("行业").agg( (sum(df_new("访问人数"))/sum(df_new("曝光人数"))) as("访问转换率"))
   val df_06 = df_new.groupBy("行业").agg( (sum(df_new("询盘人数"))/sum(df_new("访问人数"))) as("询盘转化比率"))



    val dfJ1 = df_01.join(df_02, Seq("行业"), "left")
    val dfJ2 = dfJ1.join(df_03, Seq("行业"), "left")
    val dfJ3 = dfJ2.join(df_04, Seq("行业"), "left")
    val dfJ4 = dfJ3.join(df_05, Seq("行业"), "left")
    val dfJ5 = dfJ4.join(df_06, Seq("行业"), "left")
//dfJ5.show()
    dfJ5.repartition(1).write
      .option("header", "true")
      .csv(OutputPath)
    spark.stop()
  }
}
