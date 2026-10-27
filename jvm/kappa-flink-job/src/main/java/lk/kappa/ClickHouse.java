package lk.kappa;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/** Minimal ClickHouse HTTP client + DDL of the versioned Kappa serving tables. */
public final class ClickHouse {
    private ClickHouse() {}

    public static HttpClient client() {
        return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    public static void exec(HttpClient http, String base, String sql) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(base + "/"))
            .timeout(Duration.ofSeconds(60)).POST(HttpRequest.BodyPublishers.ofString(sql)).build();
        HttpResponse<String> r = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() != 200) throw new IllegalStateException("ClickHouse " + r.statusCode() + ": " + r.body());
    }

    public static void insert(HttpClient http, String base, String table, String body) throws Exception {
        String q = URLEncoder.encode("INSERT INTO " + table + " FORMAT JSONEachRow", StandardCharsets.UTF_8);
        HttpRequest req = HttpRequest.newBuilder(URI.create(base + "/?date_time_input_format=best_effort&query=" + q))
            .timeout(Duration.ofSeconds(120)).POST(HttpRequest.BodyPublishers.ofString(body)).build();
        HttpResponse<String> r = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() != 200) throw new IllegalStateException("ClickHouse insert " + r.statusCode() + ": " + r.body());
    }

    /** Idempotent: tables are keyed by the view key and replaced by the highest version. */
    public static void createTables(JobConfig c) throws Exception {
        HttpClient http = client();
        String[] ddl = {
            "CREATE DATABASE IF NOT EXISTS " + c.database,
            "CREATE TABLE IF NOT EXISTS " + c.table("account") + " (account_id String, balance Int64, accepted UInt64,"
                + " duplicates UInt64, late UInt64, version UInt64) ENGINE = ReplacingMergeTree(version) ORDER BY account_id",
            "CREATE TABLE IF NOT EXISTS " + c.table("spend_day") + " (account_id String, day Date, spend Int64,"
                + " tx_count UInt64, version UInt64) ENGINE = ReplacingMergeTree(version) ORDER BY (account_id, day)",
            "CREATE TABLE IF NOT EXISTS " + c.table("spend_cat_month") + " (account_id String,"
                + " category LowCardinality(String), month String, spend Int64, version UInt64)"
                + " ENGINE = ReplacingMergeTree(version) ORDER BY (account_id, category, month)",
            "CREATE TABLE IF NOT EXISTS " + c.table("trace") + " (event_id String, account_id String, day Date,"
                + " event_time DateTime64(3, 'UTC'), type LowCardinality(String), amount Int64, merchant_id Nullable(String),"
                + " is_self_transfer Bool, pos_offline Bool, spend_contribution Nullable(Int64), version UInt64)"
                + " ENGINE = ReplacingMergeTree(version) ORDER BY (account_id, day, event_id)",
            "CREATE TABLE IF NOT EXISTS " + c.table("progress") + " (subtask UInt32, watermark DateTime64(3, 'UTC'),"
                + " wall_time DateTime64(3, 'UTC')) ENGINE = ReplacingMergeTree(wall_time) ORDER BY subtask",
        };
        for (String s : ddl) exec(http, c.clickhouse, s);
    }
}
