package test_scala

import com.hankcs.hanlp.dictionary.CustomDictionary
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import scala.collection.JavaConverters._
/**
  * Created by zcy on 18-1-4.
  */
object TestSegment {
  def main(args: Array[String]): Unit ={
    val sentense = "41,【 日  期 】19960104 【 版  号 】1 【 标  题 】合巢芜高速公路巢芜段竣工 【 作  者 】彭建中 【 正  文 】     安徽合（肥）巢（湖）芜（湖）高速公路巢芜段日前竣工通车并投入营运。合巢芜 高速公路是国家规划的京福综合运输网的重要干线路段，是交通部确定１９９５年建成 的全国１０条重点公路之一。该条高速公路正线长８８公里。（彭建中）"
    CustomDictionary.add("日  期")
    CustomDictionary.add("版  号")
    CustomDictionary.add("标  题")
    CustomDictionary.add("作  者")
    CustomDictionary.add("正  文")
    val list = StandardTokenizer.segment(sentense)
    CoreStopWordDictionary.apply(list)
    val my_list = list.asScala
    println(my_list.map(x => x.word.replaceAll(" ","")).mkString(","))
  }
}
