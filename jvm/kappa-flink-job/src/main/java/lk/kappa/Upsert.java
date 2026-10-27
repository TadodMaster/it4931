package lk.kappa;

/**
 * One absolute-value row for a ClickHouse ReplacingMergeTree(version) table.
 * Re-delivering the same (table, key, version) row, or an older version, can never change the final result.
 */
public class Upsert {
    public String table;     // suffix: account | spend_day | spend_cat_month | trace
    public String key;       // natural key inside the table, used to coalesce rows inside one insert batch
    public long version;     // per-account monotonically increasing sequence kept in Flink keyed state
    public String json;      // JSONEachRow line

    public Upsert() {}

    public Upsert(String table, String key, long version, String json) {
        this.table = table;
        this.key = key;
        this.version = version;
        this.json = json;
    }
}
