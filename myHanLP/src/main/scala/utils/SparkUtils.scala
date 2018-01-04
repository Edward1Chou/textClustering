package utils


import org.apache.spark.{SparkContext,SparkConf}
/**
  * 封装SparkContext和SQLContext
  * Created by zcy on 18-1-4.
  */
object SparkUtils {
  def getSparkContext(appName:String , testOrNot:Boolean):SparkContext = {
    val conf = if(testOrNot){
      new SparkConf().setAppName(appName).setMaster("local[4]") //只开4个进程模拟集群
    }else{
      new SparkConf().setAppName(appName) // 火力全开
    }
    new SparkContext(conf)
  }

}
