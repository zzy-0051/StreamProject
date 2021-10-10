package utils;



import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
    public static List<String> spiltKeyword(String keyWord) {

        StringReader reader = new StringReader(keyWord);

        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        ArrayList<String> words = new ArrayList<>();

        Lexeme next = null;
        try {
            next = ikSegmenter.next();

            while (next != null) {

                words.add(next.getLexemeText());

                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return words;
    }
}
