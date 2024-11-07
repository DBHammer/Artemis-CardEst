package ecnu.dbhammer.schema.genefunc;

import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName QuadraticFunc.java
 * @Description TODO
 * @createTime 2021年11月28日 22:38:00
 */
public class QuadraticFunc implements AttrGeneFunc {
    private BigDecimal a;
    private BigDecimal b;
    private BigDecimal c;
    private String expression;

    public QuadraticFunc(BigDecimal a, BigDecimal b, BigDecimal c){
        this.a = a;
        this.b = b;
        this.c = c;
        this.expression = this.a.toPlainString() + " * k^2 + " +
                this.b.toPlainString() + " * k + " + this.c.toPlainString();
    }
    @Override
    public Integer getPKByParam(BigDecimal param) {
        // TODO Auto-generated method stub
        return null;
    }
    public QuadraticFunc(){

    }


    @Override
    public Pair<Integer, Integer> getSolveBound(BigDecimal result, String operator, int lowerBound, int upperBound) {
        return null;
    }

    @Override
    public List<Integer> getSolveResult(BigDecimal result, String operator, int lowerBound, int upperBound) {
        return null;
    }

    public BigDecimal evaluate(int k){
        BigDecimal bigDecimalK = new BigDecimal(k);
        return a.multiply(bigDecimalK).multiply(bigDecimalK).add(b.multiply(bigDecimalK)).add(c);
    }

    @Override
    public String getExpression() {
        return this.expression;
    }

    @Override
    public BigDecimal getMaxValue(int lowerBound, int upperBound) {
        BigDecimal axis = this.b.divide(this.a.multiply(new BigDecimal(-2)), 3,BigDecimal.ROUND_HALF_UP);
        if(this.a.compareTo(new BigDecimal(0)) > 0) {
            if(axis.compareTo(new BigDecimal(lowerBound)) < 0) {
                return this.evaluate(upperBound);
            } else if(axis.compareTo(new BigDecimal(upperBound)) > 0) {
                return this.evaluate(lowerBound);
            } else {
                BigDecimal mid = new BigDecimal(upperBound - lowerBound).divide(new BigDecimal(2)).add(new BigDecimal(lowerBound));
                if(axis.compareTo(mid) <= 0) {
                    return this.evaluate(upperBound);
                } else {
                    return this.evaluate(lowerBound);
                }
            }
        } else {
            if(axis.compareTo(new BigDecimal(lowerBound)) < 0) {
                return this.evaluate(lowerBound);
            } else if(axis.compareTo(new BigDecimal(upperBound)) > 0) {
                return this.evaluate(upperBound);
            } else {
                BigDecimal mid = new BigDecimal(upperBound - lowerBound).divide(new BigDecimal(2)).add(new BigDecimal(lowerBound));
                if(axis.compareTo(mid) <= 0) {
                    return this.evaluate(lowerBound);
                } else {
                    return this.evaluate(upperBound);
                }
            }
        }
    }

    @Override
    public BigDecimal getMinValue(int lowerBound, int upperBound) {
        BigDecimal axis = this.b.divide(this.a.multiply(new BigDecimal(-2)), 3,BigDecimal.ROUND_HALF_UP);
        if(this.a.compareTo(new BigDecimal(0)) > 0) {
            if(axis.compareTo(new BigDecimal(lowerBound)) < 0) {
                return this.evaluate(lowerBound);
            } else if(axis.compareTo(new BigDecimal(upperBound)) > 0) {
                return this.evaluate(upperBound);
            } else {
                BigDecimal mid = new BigDecimal(upperBound - lowerBound).divide(new BigDecimal(2), 3,BigDecimal.ROUND_HALF_UP).add(new BigDecimal(lowerBound));
                if(axis.compareTo(mid) <= 0) {
                    return this.evaluate(lowerBound);
                } else {
                    return this.evaluate(upperBound);
                }
            }
        } else {
            if(axis.compareTo(new BigDecimal(lowerBound)) < 0) {
                return this.evaluate(upperBound);
            } else if(axis.compareTo(new BigDecimal(upperBound)) > 0) {
                return this.evaluate(lowerBound);
            } else {
                BigDecimal mid = new BigDecimal(upperBound - lowerBound).divide(new BigDecimal(2), 3, BigDecimal.ROUND_HALF_UP).add(new BigDecimal(lowerBound));
                if(axis.compareTo(mid) <= 0) {
                    return this.evaluate(upperBound);
                } else {
                    return this.evaluate(lowerBound);
                }
            }
        }
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


//    public int getSolveSize(AttrValue d, String comp, int lowerBound, int upperBound){
//        int ans=0;
//        if(d.type==DataType.INTEGER || d.type==DataType.LONG || d.type == DataType.FLOAT
//        || d.type == DataType.DOUBLE || d.type == DataType.DECIMAL){
//            for(int i = lowerBound; i<=upperBound; i++){
//
//            }
//        }
//        return ans;
//
//    }

    public BigDecimal getCoefficient2() {
        return a;
    }

    public void setCoefficient2(BigDecimal a) {
        this.a = a;
    }

    public BigDecimal getCoefficient1() {
        return b;
    }

    public void setCoefficient1(BigDecimal b) {
        this.b = b;
    }

    public BigDecimal getCoefficient0() {
        return c;
    }

    public void setCoefficient0(BigDecimal c) {
        this.c = c;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }
}
