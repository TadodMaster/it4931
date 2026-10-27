package lk.kappa;

import java.net.http.HttpClient;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * At-least-once delivery + idempotent rows = exactly-once result.
 * Rows are buffered, coalesced per (table, key) keeping the highest version, and flushed every N ms / M rows and,
 * synchronously, on every checkpoint barrier (so everything before a completed checkpoint is already in ClickHouse).
 */
public class ClickHouseSink extends RichSinkFunction<Upsert> implements CheckpointedFunction {
    private final JobConfig cfg;
    private transient HttpClient http;
    private transient ScheduledExecutorService flusher;
    private transient Object lock;                    // guards buffer
    private transient Object flushLock;               // serialises flushes: a checkpoint must wait for in-flight sends
    private transient Map<String, Map<String, Upsert>> buffer;
    private transient int rows;
    private transient volatile Throwable failure;
    private transient long watermark;

    public ClickHouseSink(JobConfig cfg) {
        this.cfg = cfg;
    }

    @Override
    public void open(Configuration parameters) {
        lock = new Object();
        flushLock = new Object();
        buffer = new HashMap<>();
        watermark = Long.MIN_VALUE;
        http = ClickHouse.client();
        flusher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "clickhouse-flusher");
            t.setDaemon(true);
            return t;
        });
        flusher.scheduleWithFixedDelay(() -> {
            try {
                flush();
            } catch (Throwable t) {
                failure = t;
            }
        }, cfg.sinkFlushMs, cfg.sinkFlushMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void invoke(Upsert u, Context ctx) throws Exception {
        check();
        boolean full;
        synchronized (lock) {
            Map<String, Upsert> t = buffer.computeIfAbsent(u.table, k -> new LinkedHashMap<>());
            Upsert old = t.get(u.key);
            if (old == null || old.version <= u.version) t.put(u.key, u);
            watermark = Math.max(watermark, ctx.currentWatermark());
            full = ++rows >= cfg.sinkBatchRows;
        }
        if (full) flush();
    }

    private void check() {
        if (failure != null) throw new IllegalStateException("sink flusher failed", failure);
    }

    void flush() throws Exception {
        synchronized (flushLock) {
            flushLocked();
        }
    }

    private static final java.time.format.DateTimeFormatter TS =
        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(java.time.ZoneOffset.UTC);

    private static String sec(long ms) {
        return "\"" + TS.format(java.time.Instant.ofEpochMilli(ms)) + "\"";
    }

    private void flushLocked() throws Exception {
        Map<String, Map<String, Upsert>> batch;
        long wm;
        synchronized (lock) {
            if (rows == 0 && watermark == Long.MIN_VALUE) return;
            batch = buffer;
            buffer = new HashMap<>();
            rows = 0;
            wm = watermark;
        }
        for (Map.Entry<String, Map<String, Upsert>> en : batch.entrySet()) {
            StringBuilder sb = new StringBuilder();
            for (Upsert u : en.getValue().values()) sb.append(u.json).append('\n');
            send(cfg.table(en.getKey()), sb.toString());
        }
        if (wm != Long.MIN_VALUE && wm != Long.MAX_VALUE && !batch.isEmpty()) {
            long now = System.currentTimeMillis();
            send(cfg.table("progress"), "{\"subtask\":" + getRuntimeContext().getIndexOfThisSubtask()
                + ",\"watermark\":" + sec(wm) + ",\"wall_time\":" + sec(now) + "}\n");
        }
    }

    private void send(String table, String body) throws Exception {
        for (int attempt = 1; ; attempt++) {
            try {
                ClickHouse.insert(http, cfg.clickhouse, table, body);
                return;
            } catch (Exception e) {
                if (attempt >= 5) throw e;
                Thread.sleep(500L * attempt);
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        check();
        flush();                       // everything emitted before the barrier is durable in ClickHouse
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {}

    @Override
    public void close() throws Exception {
        if (flusher != null) flusher.shutdownNow();
        try {
            flush();
        } catch (Exception ignored) {
            // job is going down; unflushed rows are re-emitted from the last checkpoint on restart
        }
    }
}
