package lk.kappa;

import lk.common.Event;
import lk.common.EventSchema;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Kappa architecture: ONE streaming job is the whole system.
 * Kafka(transactions) -> keyBy(account) -> dedup + lateness + rule + state -> ClickHouse upserts.
 * Reprocessing = start this job again with a new --version/--group from offset 0 (see bench reprocess).
 */
public class KappaJob {
    public static void main(String[] args) throws Exception {
        JobConfig cfg = new JobConfig(args);
        System.out.printf("[kappa] version=%s rule=%s group=%s allowedLatenessMs=%d oooMs=%d dedupRetentionMs=%d start=%s%n",
            cfg.version, cfg.rule, cfg.group, cfg.allowedLatenessMs, cfg.oooMs, cfg.dedupRetentionMs, cfg.startFrom);
        ClickHouse.createTables(cfg);
        ensureLateTopic(cfg);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(cfg.parallelism);
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));                 // incremental checkpoints
        env.enableCheckpointing(cfg.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cc = env.getCheckpointConfig();
        cc.setCheckpointStorage(cfg.checkpointDir);
        cc.setMinPauseBetweenCheckpoints(Math.min(2000, cfg.checkpointIntervalMs / 2));
        cc.setCheckpointTimeout(10 * 60_000L);
        cc.setTolerableCheckpointFailureNumber(3);
        cc.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 5000));

        OffsetsInitializer start = "earliest".equals(cfg.startFrom) ? OffsetsInitializer.earliest()
            : "latest".equals(cfg.startFrom) ? OffsetsInitializer.latest()
            : OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);

        KafkaSource<Event> txSource = KafkaSource.<Event>builder()
            .setBootstrapServers(cfg.kafka).setTopics(cfg.topicTransactions).setGroupId(cfg.group)
            .setStartingOffsets(start).setValueOnlyDeserializer(new EventSchema())
            .setProperty("commit.offsets.on.checkpoint", "true")                    // consumer-lag visibility
            .build();
        DataStream<Event> events = env.fromSource(txSource,
            WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(cfg.oooMs))
                .withTimestampAssigner((e, ts) -> e.eventTimeMs)
                .withIdleness(Duration.ofSeconds(30)),
            "transactions").uid("src-transactions");

        KafkaSource<Merchant> mSource = KafkaSource.<Merchant>builder()
            .setBootstrapServers(cfg.kafka).setTopics(cfg.topicMerchants).setGroupId(cfg.group + "-merchants")
            .setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new Deserializers.MerchantSchema())
            .build();
        // The dimension carries no event time: mark it idle so it never holds back the combined watermark.
        BroadcastStream<Merchant> merchants = env.fromSource(mSource,
                WatermarkStrategy.<Merchant>noWatermarks().withIdleness(Duration.ofSeconds(1)), "merchants")
            .uid("src-merchants").broadcast(AccountFn.MERCHANTS);

        SingleOutputStreamOperator<Upsert> upserts = events.keyBy(e -> e.accountId)
            .connect(merchants).process(new AccountFn(cfg)).uid("account-fn").name("account-state-" + cfg.version);

        upserts.addSink(new ClickHouseSink(cfg)).uid("sink-clickhouse").name("clickhouse-upserts-" + cfg.version);

        KafkaSink<String> lateSink = KafkaSink.<String>builder().setBootstrapServers(cfg.kafka)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(cfg.topicLate)
                .setValueSerializationSchema(new SimpleStringSchema()).build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
        upserts.getSideOutput(AccountFn.LATE).sinkTo(lateSink).uid("sink-late").name("late-events");

        env.execute("kappa-" + cfg.version);
    }

    /**
     * The per-version late-events topic must keep its data forever: event timestamps are simulated (months older than the wall clock),
     * so Kafka's default 7-day time retention would delete late events as soon as they are written.
     */
    private static void ensureLateTopic(JobConfig cfg) throws Exception {
        java.util.Properties p = new java.util.Properties();
        p.put("bootstrap.servers", cfg.kafka);
        try (org.apache.kafka.clients.admin.AdminClient admin = org.apache.kafka.clients.admin.AdminClient.create(p)) {
            org.apache.kafka.clients.admin.NewTopic t = new org.apache.kafka.clients.admin.NewTopic(cfg.topicLate, 1, (short) 1)
                .configs(java.util.Map.of("retention.ms", "-1"));
            try {
                admin.createTopics(java.util.List.of(t)).all().get();
            } catch (java.util.concurrent.ExecutionException e) {
                if (!(e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) throw e;
            }
        }
    }
}
