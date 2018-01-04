package test_tfidf

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession
//import utils.SparkUtils
/**
  *测试Spark MLlib的tf-idf
  * Created by zcy on 18-1-4.
  */
object TFIDFDemo {
  def main(args: Array[String]) {
    val spark_session = SparkSession.builder().appName("tf-idf").master("local[4]").getOrCreate()
    import spark_session.implicits._ // 隐式转换
    val sentenceData = spark_session.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    // 分词
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    println("wordsData----------------")
    val wordsData = tokenizer.transform(sentenceData)
    wordsData.show(3)
    // 求TF
    println("featurizedData----------------")
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(2000) // 设置哈希表的桶数为2000，即特征维度
    val featurizedData = hashingTF.transform(wordsData)
    featurizedData.show(3)
    // 求IDF
    println("recaledData----------------")
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.show(3)
    println("----------------")
    rescaledData.select("features", "label").take(3).foreach(println)
  }
}
