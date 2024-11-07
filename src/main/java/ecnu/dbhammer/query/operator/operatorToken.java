package ecnu.dbhammer.query.operator;

import java.math.BigDecimal;

/**
 * @author xiangzhaokun
 * @ClassName operatorToken.java
 * @Description TODO
 * @createTime 2022年05月15日 16:27:00
 */
public interface operatorToken {
    String getExpressionText();

    String getResult(BigDecimal bigDecimal);
}
