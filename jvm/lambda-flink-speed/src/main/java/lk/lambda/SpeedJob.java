package lk.lambda;

import java.io.FileReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import lk.common.Event;
import lk.common.EventSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.yaml.snakeyaml.Yaml;

/** Lambda speed layer (Java implementation of the rule via rules-java). Latest offsets, Redis day buckets. */
public class SpeedJob {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) a.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        Map<String, Object> y;
        try (Reader r = new FileReader(a.getOrDefault("config", "/config/demo.yaml"))) {
            y = new Yaml().load(r);
        }
        Map<String, Object> time = (Map<String, Object>) y.get("time");
        Map<String, Object> kafka = (Map<String, Object>) y.get("kafka");
        String rule = a.getOrDefault("rule", "v1");
        double speedup = ((Number) time.get("sim_speedup")).doubleValue();
        double retentionSimSec = ((Number) time.get("pos_max_lateness_sim_hours")).doubleValue() * 3600
            + ((Number) time.get("allowed_lateness_margin_sim_minutes")).doubleValue() * 60 + 30 * 60;
        int ttl = (int) Math.max(300, Math.ceil(retentionSimSec / speedup));   // real seconds
        String group = a.getOrDefault("group", "lambda-speed");
        String startFrom = a.getOrDefault("start-from", "committed");
        System.out.printf("[lambda-speed] rule=%s group=%s dedupTtl=%ds start=%s%n", rule, group, ttl, startFrom);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Integer.parseInt(a.getOrDefault("parallelism", "2")));
        env.enableCheckpointing(Long.parseLong(a.getOrDefault("checkpoint-interval-ms", "10000")), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(a.getOrDefault("checkpoint-dir", "s3://checkpoints/lambda-speed"));
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 5000));

        OffsetsInitializer start = "earliest".equals(startFrom) ? OffsetsInitializer.earliest()
            : "latest".equals(startFrom) ? OffsetsInitializer.latest()
            : OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST);
        KafkaSource<Event> src = KafkaSource.<Event>builder()
            .setBootstrapServers(a.getOrDefault("kafka", (String) kafka.get("bootstrap")))
            .setTopics((String) kafka.get("topic_transactions")).setGroupId(group).setStartingOffsets(start)
            .setValueOnlyDeserializer(new EventSchema()).build();
        DataStream<Event> events = env.fromSource(src, WatermarkStrategy.noWatermarks(), "transactions");
        events.map(new SpeedFn(a.getOrDefault("redis", "redis"), rule, ttl)).name("speed-layer-" + rule)
            .addSink(new DiscardingSink<>()).name("redis-side-effects");
        env.execute("lambda-speed");
    }
}
