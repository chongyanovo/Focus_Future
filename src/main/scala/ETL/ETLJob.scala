package ETL

import core.SparkCore
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

object ETLJob {

  private val InputPath: String = "input/数据分析方向-数据集文件-data.csv"
  private val OutputPath: String = "output/.csv"


  def main(args: Array[String]): Unit = {
    val spark = SparkCore.create("ETL","local[*]")

    val df = spark.read.option("header", true).option("inferSchema", true).csv(InputPath)
    // 统计行数
    println("总共行数: " + df.count())

    // 过滤出行业投入不为null的数据
    val df2 = df.filter(df("行业投入").isNotNull)

    val df3 = df2.filter(df2("行业投入") =!= 0)

    // 发现不为NULL的数据少 ->  清洗数据
    println("去除行业投入为Null或者为0的数据剩余总行数: " + df3.count())

    // 统计有行业投入但是 曝光数量为0的数据  这部分数据我们认为是脏数据 总计1988条
    val df4 = df3.filter(df3("曝光数量").isNotNull)
    val df5 = df4.filter(df4("曝光数量") =!= 0)

    // 把行业编码列转为String以便后面拆分列操作
    val df6 = df5.withColumn("行业编码", col("行业编码").cast(StringType))

    // 创建表
    df6.createTempView("IS")

    // 编写UDF
    spark.udf.register("Industry1", (x: String) => x.substring(0, 2))
    spark.udf.register("Industry2", (x: String) => x.substring(2, 4))

    // 通过UDF函数来创建新列一级行业和二级行业
    val frame = spark.sql("select `公司ID` , `行业编码` , `行业投入` , `曝光数量` , `曝光人数` , `访问数量` , `访问人数` , `询盘数量` , `询盘人数` , `续约标识` , Industry1(`行业编码`) as `一级行业`,Industry2(`行业编码`) as `二级行业` from IS")


    frame.repartition(1).write
      .option("header", "true")
      .csv(OutputPath)


    spark.stop()
  }
}
