package ecnu.dbhammer.constant;

public class FilterExpressionOperator {
    public static final String[] comparisonOperatorForNoEquality = {">", ">=", "<", "<=", "between and"};
    public static final String[] comparisonOperatorMustEquality = {">=","<=", "="};
    public static final String[] comparisonOperatorForEquality = {"=", "!=", "in", "not in", "like", "not like"};
}
