package core.func;

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
 * @Date : 2021/09/28
 */

public class CustomDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {

        JSONObject result = new JSONObject();

        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String database = split[1];
        String tableName = split[2];

        Struct value = (Struct) sourceRecord.value();
        Struct after = value.getStruct("after");
        JSONObject afterData = new JSONObject();
        if (after != null) {
            Schema schema = after.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                afterData.put(field.name(), after.get(field));
            }
        }

        Struct before = value.getStruct("before");
        JSONObject beforeData = new JSONObject();
        if (before != null) {
            Schema schema = before.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                beforeData.put(field.name(), before.get(field));
            }
        }

        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        result.put("database", database);
        result.put("tableName", tableName);
        result.put("data", afterData);
        result.put("before", beforeData);
        result.put("type", type);

        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
