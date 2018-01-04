package TFIDF_Kmeans

import com.hankcs.hanlp.dictionary.CustomDictionary
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import org.apache.spark.{Partitioner, Partition}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{IDF, HashingTF}
import utils.SparkUtils
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
/**
  *tf-idf表征文本特征，k-means实现文本聚类
  *Spark版本
  * Created by zcy on 18-1-4.
  */
object TFIDFKmeansSparkVesion {
  val data = "data/data.txt"
  def main(args: Array[String]) {
//    if(args.length!=4){
//      println("Usage: demo03_tf_idf_kmeans.TFIDFKmeansInOneFile <input_data> <numFeatures> <k> <testOrNot>")
//      System.exit(-1)
//    }

    val (input_data,numFeatures,k) =(data,2000,10)
//    val (input_data,numFeatures,k,testOrNot) =(args(0),args(1).toInt,args(2).toInt,args(3).toBoolean)

//    val sc = SparkUtils.getSparkContext("Kmeans use tf-idf",testOrNot)
    val sqlContext = SparkSession.builder().appName("tf-idf").master("local[4]").getOrCreate()
    // 1. 添加自定义词典
    CustomDictionary.add("日  期")
    CustomDictionary.add("版  号")
    CustomDictionary.add("标  题")
    CustomDictionary.add("作  者")
    CustomDictionary.add("正  文")

    // 2. 读取数据并分词
    println("segment sentences ...")
    import sqlContext.implicits._ // 使用toDF,隐式转换
    val docs = sqlContext.sparkContext.textFile(input_data).map{x => val t = x.split(".txt\t");(t(0),transform(t(1)))}
      .toDF("fileName", "sentence_words")

    // 3. 求TF
    println("calculating TF ...")
    val hashingTF = new HashingTF()
      .setInputCol("sentence_words").setOutputCol("rawFeatures").setNumFeatures(numFeatures)
    val featurizedData = hashingTF.transform(docs)

    // 4. 求IDF
    println("calculating IDF ...")
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData).cache()

//    rescaledData.show(3)

    // 4. kmeans 聚类
    // Trains a k-means model.
    println("creating kmeans model ...")
    val kmeans = new KMeans().setK(k).setSeed(1L)
    val model = kmeans.fit(rescaledData)
    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    println("calculating wssse ...")
    val WSSSE = model.computeCost(rescaledData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    val output = model.transform(rescaledData)
//    output.show(3)

    // 5. 计算正确率
    // 正确率计算方式：
    // 1） 由于实际每个类别和其文件名的第一个字符有关；
    // 2) 每个群组中使用模型预测的大部分都是正确的，所以分组正确的第一个字符减去预测值，应该是一个固定的值；
    //    其他不相等的就是分组错的了；
    // DataFrame不好处理，采用RDD处理
    // |fileName|      sentence_words|         rawFeatures|            features|prediction|
    val outputRdd = output.rdd.map(row =>(row.getString(0),row.getInt(4)))

    val fileNameFirstCharMap = outputRdd.map(_._1.charAt(0)).distinct().zipWithIndex().collect().toMap

    val partitionData = outputRdd.partitionBy(FileNamePartitioner(fileNameFirstCharMap) )

    //      fileNameFirstCharMap.foreach(println(_))
    // firstCharInFileName , firstCharInFileName - predictType
    val combined = partitionData.map(x =>( (x._1.charAt(0), Integer.parseInt(x._1.charAt(0)+"") - x._2),1) )
      .mapPartitions{f => var aMap = Map[(Char,Int),Int]();
        for(t <- f){
          if (aMap.contains(t._1)){
            aMap = aMap.updated(t._1,aMap.getOrElse(t._1,0)+1)
          }else{
            aMap = aMap + t
          }
        }
        val aList = aMap.toList
        val total= aList.map(_._2).sum
        val total_right = aList.map(_._2).max
        List((aList.head._1._1,total,total_right)).toIterator
        //       aMap.toIterator //打印各个partition的总结
      }
    val result = combined.collect()
    for(re <- result ){
      println("文档"+re._1+"开头的 文档总数："+ re._2+",分类正确的有："+re._3+",分类正确率是："+(re._3*100.0/re._2)+"%")
    }
    val averageRate = result.map(_._3).sum *100.0 / result.map(_._2).sum
    println("平均正确率为："+averageRate+"%")
  }
  /**
    * String 分词
    * @param sentense
    * @return
    */
  def transform(sentense:String):List[String] ={
    val list = StandardTokenizer.segment(sentense)
    CoreStopWordDictionary.apply(list)
    val my_list = list.asScala
    my_list.map(x => x.word.replaceAll(" ","")).toList
  }
}


/**
  * 根据文件名的第一个字符来分区
  * @param fileNameFirstCharMap
  */
case class FileNamePartitioner(fileNameFirstCharMap:Map[Char,Long]) extends Partitioner{
  override def getPartition(key: Any): Int = key match {
    case _ => fileNameFirstCharMap.getOrElse(key.toString.charAt(0),0L).toInt
  }
  override def numPartitions: Int = fileNameFirstCharMap.size
}