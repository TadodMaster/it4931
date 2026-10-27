package lk.rules;

/**
 * Rule implementation #2 (Java, shared by the Lambda speed layer and the Kappa job; never by Spark).
 * Semantics are defined by shared/rules/spend_rules.yaml; the vectors there are run in SpendRulesVectorsTest.
 */
public final class SpendRules {
    public static final String TRANSFER_CATEGORY = "TRANSFER";

    private SpendRules() {}

    public static boolean isPositive(String type) {
        switch (type) {
            case "TOPUP": case "REFUND": case "TRANSFER_IN": case "CASHBACK": return true;
            case "PAYMENT": case "TRANSFER_OUT": return false;
            default: throw new IllegalArgumentException("unknown type " + type);
        }
    }

    /** Effect on balance; independent of the spending rule. */
    public static long signedAmount(String type, long amount) {
        return isPositive(type) ? amount : -amount;
    }

    /** Contribution to spend, or null when the rule does not touch the event. */
    public static Long spendDelta(String rule, String type, long amount, boolean isSelfTransfer) {
        switch (rule) {
            case "v1":
                return ("PAYMENT".equals(type) || "TRANSFER_OUT".equals(type)) ? Long.valueOf(amount) : null;
            case "v2":
                if ("PAYMENT".equals(type)) return amount;
                if ("REFUND".equals(type)) return -amount;
                if ("TRANSFER_OUT".equals(type) && !isSelfTransfer) return amount;
                return null;
            default:
                throw new IllegalArgumentException("unknown rule " + rule);
        }
    }

    public static boolean needsMerchant(String type) {
        return "PAYMENT".equals(type) || "REFUND".equals(type);
    }

    /** Spend-by-category key: merchant category for PAYMENT/REFUND, pseudo category TRANSFER for TRANSFER_OUT. */
    public static String category(String type, String merchantCategory) {
        if (needsMerchant(type)) return merchantCategory;
        if ("TRANSFER_OUT".equals(type)) return TRANSFER_CATEGORY;
        return null;
    }
}
