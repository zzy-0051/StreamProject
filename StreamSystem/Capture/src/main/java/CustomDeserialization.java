import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @Author : zzy
 * @Date : 2021/09/27
 */

public class CustomDeserialization implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        JSONObject result = new JSONObject();

        String topic = sourceRecord.topic();
        String db_name = topic.split("\\.")[1];
        String tb_name = topic.split("\\.")[2];

        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        getJson(before, beforeJson);
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        getJson(after, afterJson);
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if (type.equals("create")) {
            type = "insert";
        }
        result.put("database",db_name);
        result.put("tableName",tb_name);
        result.put("before",beforeJson);
        result.put("after",afterJson);
        result.put("type",type);
        collector.collect(result.toString());
    }

    private void getJson(Struct struct, JSONObject jsonObject) {
        if (struct != null) {
            Schema schema = struct.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                Object value = struct.get(field);
                jsonObject.put(field.name(),value);
            }
        }
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
