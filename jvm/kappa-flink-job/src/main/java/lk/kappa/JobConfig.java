package lk.kappa;

import java.io.FileReader;
import java.io.Reader;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;

/**
 * Job settings: command line (--key value) on top of shared/config/demo.yaml, the single place where
 * simulated-time parameters are defined. Sim durations are converted to event-time millis here
 * (event time already is simulated time, so the speedup factor is not needed inside the job).
 */
public class JobConfig implements Serializable {
    public final String rule;
    public final String version;
    public final String group;
    public final String kafka;
    public final String clickhouse;
    public final String database;
    public final String checkpointDir;
    public final long checkpointIntervalMs;
    public final int parallelism;
    public final long allowedLatenessMs;
    public final long oooMs;
    public final long dedupRetentionMs;
    public final String topicTransactions, topicMerchants, topicLate;
    public final boolean trace;
    public final String startFrom;           // earliest | latest | committed
    public final long sinkFlushMs;
    public final int sinkBatchRows;

    @SuppressWarnings("unchecked")
    public JobConfig(String[] args) throws Exception {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) a.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        Map<String, Object> y;
        try (Reader r = new FileReader(a.getOrDefault("config", "/config/demo.yaml"))) {
            y = new Yaml().load(r);
        }
        Map<String, Object> time = (Map<String, Object>) y.get("time");
        Map<String, Object> kafkaCfg = (Map<String, Object>) y.get("kafka");
        Map<String, Object> ch = (Map<String, Object>) y.get("clickhouse");
        rule = a.getOrDefault("rule", "v1");
        version = a.getOrDefault("version", rule);
        group = a.getOrDefault("group", "kappa-" + version);
        kafka = a.getOrDefault("kafka", (String) kafkaCfg.get("bootstrap"));
        clickhouse = a.getOrDefault("clickhouse", "http://" + ch.get("host") + ":" + ch.get("http_port"));
        database = a.getOrDefault("database", (String) ch.get("database"));
        checkpointDir = a.getOrDefault("checkpoint-dir", "s3://checkpoints/kappa-" + version);
        checkpointIntervalMs = Long.parseLong(a.getOrDefault("checkpoint-interval-ms", "10000"));
        parallelism = Integer.parseInt(a.getOrDefault("parallelism", "2"));
        long lateHours = ((Number) time.get("pos_max_lateness_sim_hours")).longValue();
        long marginMin = ((Number) time.get("allowed_lateness_margin_sim_minutes")).longValue();
        long oooSec = ((Number) time.get("watermark_out_of_orderness_sim_seconds")).longValue();
        long limitMs = lateHours * 3_600_000L + marginMin * 60_000L;
        allowedLatenessMs = Long.parseLong(a.getOrDefault("allowed-lateness-ms", String.valueOf(limitMs)));
        oooMs = Long.parseLong(a.getOrDefault("ooo-ms", String.valueOf(oooSec * 1000L)));
        // dedup entries must outlive the lateness limit plus the longest producer-retry delay (10 min) plus margin
        dedupRetentionMs = Long.parseLong(a.getOrDefault("dedup-retention-ms", String.valueOf(allowedLatenessMs + 30 * 60_000L)));
        topicTransactions = (String) kafkaCfg.get("topic_transactions");
        topicMerchants = (String) kafkaCfg.get("topic_merchants");
        // one late-events topic per job version: lateness depends on watermark progress, so a replay can classify differently
        String lateBase = (String) kafkaCfg.get("topic_late_events");
        topicLate = "v1".equals(version) ? lateBase : lateBase + "-" + version;
        trace = Boolean.parseBoolean(a.getOrDefault("trace", "true"));
        startFrom = a.getOrDefault("start-from", "committed");
        sinkFlushMs = Long.parseLong(a.getOrDefault("sink-flush-ms", "200"));
        sinkBatchRows = Integer.parseInt(a.getOrDefault("sink-batch-rows", "5000"));
    }

    public String table(String suffix) {
        return database + ".kappa_" + version + "_" + suffix;
    }
}
