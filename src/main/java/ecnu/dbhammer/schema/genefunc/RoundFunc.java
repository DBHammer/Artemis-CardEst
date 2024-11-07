package ecnu.dbhammer.schema.genefunc;

import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName RoundFunc.java
 * @Description 向下取整函数
 * @createTime 2021年11月28日 22:35:00
 */
public class RoundFunc implements AttrGeneFunc{
    private int base;

    public RoundFunc(int base){
        this.base = base;
    }


    @Override
    public Pair<Integer, Integer> getSolveBound(BigDecimal result, String operator, int lowerBound, int upperBound) {
        return null;
    }

    @Override
    public List<Integer> getSolveResult(BigDecimal result, String operator, int lowerBound, int upperBound) {
        return null;
    }

    @Override
    public Integer getPKByParam(BigDecimal param) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public BigDecimal evaluate(int k) {
        return null;
    }

    @Override
    public String getExpression() {
        return null;
    }

    @Override
    public BigDecimal getMaxValue(int lowerBound, int upperBound) {
        return null;
    }

    @Override
    public BigDecimal getMinValue(int lowerBound, int upperBound) {
        return null;
    }

    @Override
    public List<Integer> getReverse(BigDecimal result, int lowBound, int upperBound) {
        return null;
    }


    @Override
    public AttrGeneFunc copyMyself() {
        return null;
    }

    @Override
    public Pair<BigDecimal, BigDecimal> getMinValue(List<Integer> keyRange) {
        return null;
    }


}
