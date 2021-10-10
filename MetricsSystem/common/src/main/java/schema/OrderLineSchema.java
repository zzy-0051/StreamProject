package schema;

import com.google.gson.Gson;
import model.OrderLineEvent;
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

public class OrderLineSchema implements DeserializationSchema<OrderLineEvent>, SerializationSchema<OrderLineEvent> {

    private static final long serialVersionUID = 8758082222966376096L;

    private static final Gson gson = new Gson();

    @Override
    public OrderLineEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), OrderLineEvent.class);
    }

    @Override
    public boolean isEndOfStream(OrderLineEvent orderLineEvent) {
        return false;
    }

    @Override
    public byte[] serialize(OrderLineEvent orderLineEvent) {
        return gson.toJson(orderLineEvent).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TypeInformation<OrderLineEvent> getProducedType() {
        return TypeInformation.of(OrderLineEvent.class);
    }

}
