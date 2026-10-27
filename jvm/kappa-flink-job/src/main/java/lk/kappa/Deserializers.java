package lk.kappa;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public final class Deserializers {
    private Deserializers() {}

    public static class MerchantSchema implements DeserializationSchema<Merchant> {
        private transient ObjectMapper mapper;

        @Override
        public Merchant deserialize(byte[] message) throws IOException {
            if (mapper == null) mapper = new ObjectMapper();
            JsonNode n = mapper.readTree(message);
            return new Merchant(n.get("merchant_id").asText(), n.get("category").asText());
        }

        @Override
        public boolean isEndOfStream(Merchant next) {
            return false;
        }

        @Override
        public TypeInformation<Merchant> getProducedType() {
            return TypeInformation.of(Merchant.class);
        }
    }
}
