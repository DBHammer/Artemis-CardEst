package ecnu.dbhammer.schema.genefunc;

/**
 * @author xiangzhaokun
 * @ClassName GeneFuncType.java
 * @Description 生成函数的类型、目前有一次、二次、向下取整、分段函数
 * @createTime 2021年11月29日 14:54:00
 */
public enum GeneFuncType {
    LINEAR,
    ROUND,
    QUADRATIC,
    PIECEWISE;
}
