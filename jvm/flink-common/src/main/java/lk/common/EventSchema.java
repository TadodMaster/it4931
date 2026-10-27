package lk.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/** JSON (shared/schemas/transaction.schema.json) -> Event. Amounts are parsed as integers, never doubles. */
public class EventSchema implements DeserializationSchema<Event> {
    private transient ObjectMapper mapper;

    @Override
    public Event deserialize(byte[] message) throws IOException {
        if (mapper == null) mapper = new ObjectMapper();
        JsonNode n = mapper.readTree(message);
        Event e = new Event();
        e.eventId = n.get("event_id").asText();
        e.accountId = n.get("account_id").asText();
        e.eventTimeIso = n.get("event_time").asText();
        e.eventTimeMs = Event.parseMs(e.eventTimeIso);
        e.producedAt = n.hasNonNull("produced_at") ? n.get("produced_at").asText() : null;
        e.channel = n.get("channel").asText();
        e.type = n.get("type").asText();
        e.amount = n.get("amount").asLong();
        e.merchantId = n.hasNonNull("merchant_id") ? n.get("merchant_id").asText() : null;
        e.isSelfTransfer = n.get("is_self_transfer").asBoolean();
        e.posOffline = n.get("pos_offline").asBoolean();
        return e;
    }

    @Override
    public boolean isEndOfStream(Event next) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
