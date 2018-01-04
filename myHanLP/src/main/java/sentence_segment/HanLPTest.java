package sentence_segment;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.tokenizer.SpeedTokenizer;
import com.hankcs.hanlp.dictionary.CustomDictionary;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import com.hankcs.hanlp.tokenizer.StandardTokenizer;
import com.hankcs.hanlp.tokenizer.IndexTokenizer;

import java.util.List;
/**
 * Created by zcy on 18-1-3.
 */

public class HanLPTest {
    public static void main(String[] args){
        // 添加用户自定义词典， HanLP
        CustomDictionary.add("北邮");
        CustomDictionary.add("科研大楼");
        System.out.println("UerDict + HanLP: ");
        System.out.println(HanLP.segment("我走出北邮科研大楼，夜色已黑，繁星点点。"));
        // SpeedTokenizer
        String text = "江西鄱阳湖干枯，中国最大淡水湖变成大草原";
        System.out.println("SpeedTokenizer: ");
        System.out.println(SpeedTokenizer.segment(text));
        // NLPTokenizer
        List<Term> termList = NLPTokenizer.segment("中国科学院计算技术研究所的宗成庆教授正在教授自然语言处理课程");
        System.out.println("NLPTokenizer: ");
        System.out.println(termList);
        // StandardTokenizer
        List<Term> termList3 = StandardTokenizer.segment("商品和服务");
        System.out.println("StandardTokenizer");
        System.out.println(termList3);
        // IndexTokenizer
        List<Term> termList2 = IndexTokenizer.segment("主副食品");
        System.out.println("IndexTokenizer: ");
        for (Term term : termList2)
        {
            System.out.println(term + " [" + term.offset + ":" + (term.offset + term.word.length()) + "]");
        }

    }
}
