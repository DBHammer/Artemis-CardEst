package ecnu.dbhammer.query.type;

/**
 * @author xiangzhaokun
 * @ClassName ExpressionComplexType.java
 * @Description TODO
 * @createTime 2022年03月11日 10:51:00
 */
public enum ExpressionComplexType {
    PURESINGLE, // 只有单个属性 比如a.col, benchmark中一般都是这种
    SINGLE, // 单个属性上可能有运算，比如5*a.col+1
    MULTI, //多个属性，且可能有运算, 如5*a.col_1 + 4*A.col_2 + 5
}
