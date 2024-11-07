package ecnu.dbhammer.query.operator;

import ecnu.dbhammer.utils.Randomly;

/**
 * @author xiangzhaokun
 * @ClassName TokenGen.java
 * @Description TODO
 * @createTime 2022年05月15日 21:34:00
 */
public enum TokenGen {
    CAST,
    CONCAT;

    public static TokenGen getRandom(){
        return Randomly.fromOptions(values());
    }
}
