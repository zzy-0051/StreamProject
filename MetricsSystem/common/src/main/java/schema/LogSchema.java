package schema;

import com.google.gson.Gson;
import model.LogEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @Author : zzy
 * @Date : 2021/10/07
 */

public class LogSchema implements DeserializationSchema<LogEvent>, SerializationSchema<LogEvent> {

    private static final long serialVersionUID = 1153233025069175658L;

    private static final Gson gson = new Gson();

    @Override
    public LogEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), LogEvent.class);
    }

    @Override
    public boolean isEndOfStream(LogEvent logEvent) {
        return false;
    }

    @Override
    public byte[] serialize(LogEvent logEvent) {
        return gson.toJson(logEvent).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TypeInformation<LogEvent> getProducedType() {
        return TypeInformation.of(LogEvent.class);
    }

}
