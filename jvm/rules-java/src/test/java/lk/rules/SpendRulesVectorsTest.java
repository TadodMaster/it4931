package lk.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.FileReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

/** Runs the shared test vectors (shared/rules/spend_rules.yaml) against the Java implementation. */
class SpendRulesVectorsTest {

    @SuppressWarnings("unchecked")
    @Test
    void sharedVectors() throws Exception {
        Map<String, Object> spec;
        try (Reader r = new FileReader(System.getProperty("spec.path", "../../shared/rules/spend_rules.yaml"))) {
            spec = new Yaml().load(r);
        }
        List<Map<String, Object>> vectors = (List<Map<String, Object>>) spec.get("test_vectors");
        assertEquals(4, vectors.size(), "unexpected vector count - spec changed?");
        for (Map<String, Object> v : vectors) {
            Map<String, String> merchants = (Map<String, String>) v.get("merchants");
            for (String rule : new String[] {"v1", "v2"}) {
                Map<String, Object> exp = (Map<String, Object>) ((Map<String, Object>) v.get("expect")).get(rule);
                Map<String, Long> balance = new HashMap<>();
                Map<String, long[]> day = new HashMap<>();
                Map<String, Long> cat = new HashMap<>();
                for (Map<String, Object> e : (List<Map<String, Object>>) v.get("events")) {
                    String acct = (String) e.get("account"), type = (String) e.get("type"), time = (String) e.get("time");
                    long amount = ((Number) e.get("amount")).longValue();
                    boolean self = (Boolean) e.get("self");
                    balance.merge(acct, SpendRules.signedAmount(type, amount), Long::sum);
                    Long d = SpendRules.spendDelta(rule, type, amount, self);
                    if (d == null) continue;
                    long[] acc = day.computeIfAbsent(acct + "|" + time.substring(0, 10), k -> new long[2]);
                    acc[0] += d;
                    acc[1] += 1;
                    String m = (String) e.get("merchant");
                    String c = SpendRules.category(type, m == null ? null : merchants.get(m));
                    cat.merge(acct + "|" + c + "|" + time.substring(0, 7), d, Long::sum);
                }
                String ctx = v.get("name") + "/" + rule;
                Map<String, Number> expBal = (Map<String, Number>) exp.get("balance");
                assertEquals(expBal.size(), balance.size(), ctx + " balance size");
                expBal.forEach((k, x) -> assertEquals(x.longValue(), balance.get(k), ctx + " balance " + k));
                Map<String, Map<String, Number>> expDay = (Map<String, Map<String, Number>>) exp.get("spend_day");
                assertEquals(expDay.size(), day.size(), ctx + " day size");
                expDay.forEach((k, x) -> {
                    assertEquals(x.get("spend").longValue(), day.get(k)[0], ctx + " spend " + k);
                    assertEquals(x.get("tx_count").longValue(), day.get(k)[1], ctx + " count " + k);
                });
                Map<String, Number> expCat = (Map<String, Number>) exp.get("spend_cat_month");
                assertEquals(expCat.size(), cat.size(), ctx + " cat size");
                expCat.forEach((k, x) -> assertEquals(x.longValue(), cat.get(k), ctx + " cat " + k));
            }
        }
    }
}
