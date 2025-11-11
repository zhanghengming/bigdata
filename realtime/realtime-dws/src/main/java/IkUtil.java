import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author ：zhm
 * @version ：1.0
 * @since ：2025/11/8 10:44
 */
// 分词工具类
public class IkUtil {

    public static Set<String> split(String s) {
        Set<String> result = new HashSet<>();
        // String => Reader

        StringReader reader = new StringReader(s);
        // 智能分词
        // max_word
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        try {
            Lexeme next = ikSegmenter.next();
            while (next != null) {
                String word = next.getLexemeText();
                result.add(word);
                next = ikSegmenter.next();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        return result;
    }

    public static void main(String[] args) {

        System.out.println(split("今天天气真好hhhh"));
    }
}
