package core.func;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import utils.KeywordUtil;

import java.util.List;


@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitKeywordUDTF extends TableFunction<Row> {
    public void eval(String keyword) {

        List<String> spiltKeyword = KeywordUtil.spiltKeyword(keyword);

        for (String word : spiltKeyword) {
            collect(Row.of(word));
        }
    }
}
