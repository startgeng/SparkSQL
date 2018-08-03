package com.imooc.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
/**
  * Created by Administrator on 2018-08-03.
  * IDEA在本地测试数据在服务器上
  */
object SQLContextApp {

  def main(args: Array[String]) {

    val path = args(0)
    //创建相对应的Context

    val sparkConf = new SparkConf()
    //在测试或者生成中AppName和Master我们是通过脚本进行指定
    //sparkConf.setAppName("SQLContextApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //相关的处理
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    //关闭资源
    sc.stop()
  }
}
