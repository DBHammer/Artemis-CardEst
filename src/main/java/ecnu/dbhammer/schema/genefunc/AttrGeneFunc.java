package ecnu.dbhammer.schema.genefunc;

import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName ColumnGeneExpression.java
 * @Description 生成函数类，分为外键生成函数、普通属性生成函数，有二次项函数、一次项函数，分段函数等
 * @createTime 5:38 下午 2021/3/23
 */
public interface AttrGeneFunc {

    Integer getPKByParam(BigDecimal param);

    /**
     * 给一个result,给一个运算符，给一个取值范围[]，求该范围中满足条件的集合大小
     * @param result
     * @param operator
     * @param lowerBound
     * @param upperBound
     * @return
     */
    Pair<Integer, Integer> getSolveBound(BigDecimal result, String operator, int lowerBound, int upperBound);

    /**
     * 给一个result,给一个运算符，给一个取值范围[]，求该范围中满足条件的集合
     * @param result
     * @param operator
     * @param lowerBound
     * @param upperBound
     * @return
     */
    List<Integer> getSolveResult(BigDecimal result, String operator, int lowerBound, int upperBound);

    /**
     * 给一个自变量x，函数求出Y
     * @param k
     * @return
     */
    BigDecimal evaluate(int k);

    /**
     * 得到函数的字符串形式
     * @return
     */
    String getExpression();

    /**
     * 给出一个范围[]，求得函数在该范围中的最大值
     * @param lowerBound
     * @param upperBound
     * @return
     */
    BigDecimal getMaxValue(int lowerBound, int upperBound);

    /**
     * 给出一个范围[],求的函数在该范围的最小值
     * @param lowerBound
     * @param upperBound
     * @return
     */
    BigDecimal getMinValue(int lowerBound, int upperBound);

    /**
     * 与BigDecimal evaluate(int k)互为逆函数，给出一个y,求的y=f(x)中所有满足条件的x，其中lowbound和upperBound为取反得到的x必须要在的集合内
     * @param result
     * @param lowBound
     * @param upperBound
     * @return
     */
    List<Integer> getReverse(BigDecimal result, int lowBound, int upperBound);

    AttrGeneFunc copyMyself();

    Pair<BigDecimal,BigDecimal> getMinValue(List<Integer> keyRange);



}
