package ecnu.dbhammer.query.type;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xiangzhaokun
 * @ClassName ComparisonOperatorType.java
 * @Description TODO
 * @createTime 2021年12月04日 10:56:00
 */
public enum ComparisonOperatorType {
    Greater(">"),
    GreaterEqual(">="),
    Less("<"),
    LessEqual("<="),
    BetweenAnd("between and"),
    Equal("="),
    NoEqual("!="),
    In("in"),
    NotIn("not in"),
    Like("like"),
    NotLike("not like"),
    Is("is"),
    IsNot("is not");

    private static Map<String,ComparisonOperatorType> mp = new HashMap<>();

    //加快从name检索
    static{
        mp.put(">",ComparisonOperatorType.Greater);
        mp.put(">=",ComparisonOperatorType.GreaterEqual);
        mp.put("<",ComparisonOperatorType.Less);
        mp.put("<=",ComparisonOperatorType.LessEqual);
        mp.put("between and",ComparisonOperatorType.BetweenAnd);
        mp.put("=",ComparisonOperatorType.Equal);
        mp.put("!=",ComparisonOperatorType.NoEqual);
        mp.put("in",ComparisonOperatorType.In);
        mp.put("not in",ComparisonOperatorType.NotIn);
        mp.put("like",ComparisonOperatorType.Like);
        mp.put("not like",ComparisonOperatorType.NotLike);
        mp.put("is",ComparisonOperatorType.Is);
        mp.put("is not", ComparisonOperatorType.IsNot);
    }

    private final String name;

    private ComparisonOperatorType(String name){
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public static ComparisonOperatorType getTypeByName(String name){
        return mp.get(name);
    }

}
