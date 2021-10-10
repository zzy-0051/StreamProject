package schema;

import com.google.gson.Gson;
import model.ShopEvent;
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

public class ShopSchema implements DeserializationSchema<ShopEvent>, SerializationSchema<ShopEvent> {

    private static final long serialVersionUID = -5324277748002380102L;

    private static final Gson gson = new Gson();

    @Override
    public ShopEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), ShopEvent.class);
    }

    @Override
    public boolean isEndOfStream(ShopEvent shopEvent) {
        return false;
    }

    @Override
    public byte[] serialize(ShopEvent shopEvent) {
        return gson.toJson(shopEvent).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TypeInformation<ShopEvent> getProducedType() {
        return TypeInformation.of(ShopEvent.class);
    }

}
