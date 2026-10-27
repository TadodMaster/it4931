package lk.lambda;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** t_k boundary cases of the Lambda split: batch < t_k <= speed layer. */
class BoundaryTest {
    private static final long TK = 1_767_225_600_000L; // 2026-01-01T00:00:00Z

    @Test
    void eventExactlyAtTkBelongsToTheSpeedLayer() {
        assertTrue(SpeedFn.ownedBySpeedLayer(TK, TK));
    }

    @Test
    void oneMillisecondBeforeTkBelongsToTheBatch() {
        assertFalse(SpeedFn.ownedBySpeedLayer(TK - 1, TK));
    }

    @Test
    void lateOfflinePosEventBeforeTkIsIgnoredByTheSpeedLayer() {
        assertFalse(SpeedFn.ownedBySpeedLayer(TK - 90 * 60_000L, TK));   // 90 minutes late: only the next batch fixes it
    }

    @Test
    void laterEventsAreRealtime() {
        assertTrue(SpeedFn.ownedBySpeedLayer(TK + 1, TK));
    }
}
