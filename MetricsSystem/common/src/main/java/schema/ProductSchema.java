package schema;

import com.google.gson.Gson;
import model.ProductEvent;
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

public class ProductSchema implements DeserializationSchema<ProductEvent>, SerializationSchema<ProductEvent> {

    private static final long serialVersionUID = 2346517377398394085L;

    private static final Gson gson = new Gson();

    @Override
    public ProductEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), ProductEvent.class);
    }

    @Override
    public boolean isEndOfStream(ProductEvent productEvent) {
        return false;
    }

    @Override
    public byte[] serialize(ProductEvent productEvent) {
        return gson.toJson(productEvent).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TypeInformation<ProductEvent> getProducedType() {
        return TypeInformation.of(ProductEvent.class);
    }

}
