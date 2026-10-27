package lk.lambda;

import java.util.HashMap;
import java.util.Map;
import lk.common.Event;
import lk.rules.SpendRules;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.params.SetParams;

/**
 * Lambda speed layer: increments Redis day buckets for events with event_time >= t_k (the batch boundary).
 * Redis layout: rt:KIND:DAY (hash; KIND = bal | spend | cnt | cat) and rt:tr:DAY:ACCOUNT (list of trace json).
 * Older events are ignored here (the next nightly batch includes them): classic Lambda.
 * Dedup is SET NX with a TTL. It is NOT transactional with the increments: a crash between the SET and the HINCRBY
 * loses that event (the speed layer undercounts until the next batch). This order was chosen so that a replay after
 * a crash can never double count.
 */
public class SpeedFn extends RichMapFunction<Event, Long> {
    private final String redisHost;
    private final String rule;
    private final int dedupTtlSeconds;
    private transient JedisPool pool;
    private transient Map<String, String> merchants;
    private transient long tk;
    private transient long tkReadAt;

    public SpeedFn(String redisHost, String rule, int dedupTtlSeconds) {
        this.redisHost = redisHost;
        this.rule = rule;
        this.dedupTtlSeconds = dedupTtlSeconds;
    }

    @Override
    public void open(Configuration parameters) {
        JedisPoolConfig cfg = new JedisPoolConfig();
        cfg.setMaxTotal(4);
        pool = new JedisPool(cfg, redisHost, 6379);
        merchants = new HashMap<>();
    }

    @Override
    public void close() {
        if (pool != null) pool.close();
    }

    private long currentTk(Jedis j) {
        long now = System.currentTimeMillis();
        if (now - tkReadAt > 1000) {
            String v = j.get("lambda:tk");
            tk = v == null ? Long.MIN_VALUE : Long.parseLong(v);
            tkReadAt = now;
        }
        return tk;
    }

    @Override
    public Long map(Event e) {
        try (Jedis j = pool.getResource()) {
            if (e.eventTimeMs < currentTk(j)) {
                j.incr("stat:old");
                return 0L;
            }
            String cat = null;
            if (SpendRules.needsMerchant(e.type)) {
                cat = merchants.get(e.merchantId);
                if (cat == null) {
                    cat = j.hget("merchants", e.merchantId);
                    if (cat == null) throw new IllegalStateException("merchant dimension missing in Redis: " + e.merchantId);
                    merchants.put(e.merchantId, cat);
                }
            }
            if (!"OK".equals(j.set("dedup:" + e.eventId, "1", SetParams.setParams().nx().ex(dedupTtlSeconds)))) {
                j.incr("stat:dup");
                return 0L;
            }
            String day = e.eventTimeIso.substring(0, 10);
            String month = e.eventTimeIso.substring(0, 7);
            Long delta = SpendRules.spendDelta(rule, e.type, e.amount, e.isSelfTransfer);
            Pipeline p = j.pipelined();
            p.hincrBy("rt:bal:" + day, e.accountId, SpendRules.signedAmount(e.type, e.amount));
            if (delta != null) {
                p.hincrBy("rt:spend:" + day, e.accountId, delta);
                p.hincrBy("rt:cnt:" + day, e.accountId, 1);
                p.hincrBy("rt:cat:" + day, e.accountId + "|" + SpendRules.category(e.type, cat) + "|" + month, delta);
            }
            p.rpush("rt:tr:" + day + ":" + e.accountId, "{\"event_id\":\"" + e.eventId + "\",\"event_time\":\""
                + e.eventTimeIso + "\",\"type\":\"" + e.type + "\",\"amount\":" + e.amount + ",\"merchant_id\":"
                + (e.merchantId == null ? "null" : "\"" + e.merchantId + "\"") + ",\"is_self_transfer\":"
                + e.isSelfTransfer + ",\"pos_offline\":" + e.posOffline + ",\"spend_contribution\":"
                + (delta == null ? "null" : delta.toString()) + "}");
            p.incr("stat:accepted");
            p.sync();
            return 1L;
        }
    }
}
