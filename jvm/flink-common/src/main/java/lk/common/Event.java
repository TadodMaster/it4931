package lk.common;

import java.time.Instant;

/** Transaction event (see shared/schemas/transaction.schema.json). Public fields so Flink treats it as a POJO. */
public class Event {
    public String eventId;
    public String accountId;
    public long eventTimeMs;
    public String eventTimeIso;
    public String producedAt;     // null for backfill
    public String channel;
    public String type;
    public long amount;           // integer VND
    public String merchantId;     // null unless PAYMENT/REFUND
    public boolean isSelfTransfer;
    public boolean posOffline;

    public Event() {}

    public static long parseMs(String iso) {
        return Instant.parse(iso).toEpochMilli();
    }
}
