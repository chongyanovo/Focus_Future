package core

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Spark 相关方法
 *
 * @author chongyan
 */
object SparkCore {
  /**
   * 创建 SparkSession
   *
   * @param appName 任务名称
   * @param master  任务提交地址
   * @return SparkSession
   */
  def create(appName: String, master: String): SparkSession = {
    // 创建 SparkConf 配置文件
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    conf.set("spark.ui.port", "7777")
    // 创建一个 SparkSession 类型的 spark 对象
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark
  }
}
