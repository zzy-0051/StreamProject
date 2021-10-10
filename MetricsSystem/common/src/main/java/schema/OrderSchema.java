package schema;

import com.google.gson.Gson;
import model.OrderEvent;
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

public class OrderSchema implements DeserializationSchema<OrderEvent>, SerializationSchema<OrderEvent> {

    private static final long serialVersionUID = 3311984680934264194L;

    private static final Gson gson = new Gson();

    @Override
    public OrderEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), OrderEvent.class);
    }

    @Override
    public boolean isEndOfStream(OrderEvent orderEvent) {
        return false;
    }

    @Override
    public byte[] serialize(OrderEvent orderEvent) {
        return gson.toJson(orderEvent).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TypeInformation<OrderEvent> getProducedType() {
        return TypeInformation.of(OrderEvent.class);
    }

}
