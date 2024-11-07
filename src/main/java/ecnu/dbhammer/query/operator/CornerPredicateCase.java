package ecnu.dbhammer.query.operator;

/**
 * @author xiangzhaokun
 * @ClassName CornerPredicateCase.java
 * @Description 可扩展的一些Bug特征，可以从Github上搜集
 */
public class CornerPredicateCase {
    public static String[] antiStandardPredicateExp = {
            "!(1 and not(1))",
            "0.1 != 0.10000000000000000000000000000000000001",
            "0.001",
            //"char(233443)",
            "1 or \"1\"",
            "False or 0.5",
            "1 % \"1e1\"",
            "NOT NOT 1"
    };

    public static String randomChoose(){
        int index = (int) (Math.random() * antiStandardPredicateExp.length);
        return "("+antiStandardPredicateExp[index]+")";
    }
}
