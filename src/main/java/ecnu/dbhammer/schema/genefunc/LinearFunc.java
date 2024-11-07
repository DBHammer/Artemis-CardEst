package ecnu.dbhammer.schema.genefunc;

import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName LinearFunc
 * @Description y = kx+b，一次函数
 * @createTime 2021年11月28日 22:36:00
 */
public class LinearFunc implements AttrGeneFunc, Cloneable, Serializable {
    // (-452809.00 * k + -3441.00) >= -1703018091
    private BigDecimal a;
    private BigDecimal b;

    private String expression;

    public void setCoefficient1(BigDecimal a) {
        this.a = a;
    }

    public void setCoefficient0(BigDecimal b) {
        this.b = b;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public LinearFunc(BigDecimal a, BigDecimal b) {
        this.a = simplifyCoefficient(a);
        this.b = simplifyCoefficient(b);
    }

    public LinearFunc() {

    }

    public LinearFunc scale(BigDecimal factor) {
        return new LinearFunc(this.a.multiply(factor), this.b.multiply(factor));
    }

    // 生成函数系数的精度控制，具体策略为：如果本来就是整数，则不用后面加0，否则，绝对值超过1的小数，保留两位小数；否则，保留两位有效数字
    private BigDecimal simplifyCoefficient(BigDecimal coefficient) {
        if (new BigDecimal(coefficient.intValue()).compareTo(coefficient) == 0) {
            return coefficient;
        } else {

            if (coefficient.abs().compareTo(new BigDecimal("1")) != -1) { // 绝对值超过1的数
                return coefficient.setScale(3, BigDecimal.ROUND_DOWN);
            } else {
                int scale = coefficient.scale();
                int unscaledValueLength = coefficient.unscaledValue().toString().length();
                return coefficient.setScale(scale - unscaledValueLength + 2, BigDecimal.ROUND_DOWN);
            }
        }
    }

    @Override
    public Integer getPKByParam(BigDecimal param) {
        return param.subtract(b).divide(a).intValue();
    }

    public Pair<Integer, Integer> getSolveBound(BigDecimal result, String operator, int lowerBound, int upperBound) {

        if (a.compareTo(new BigDecimal(0)) == 0) {
            if (result.compareTo(b) != 0) {
                return null;
            } else {
                if (operator.equals(">=") || operator.equals("<=") || operator.equals("="))
                    return Pair.of(lowerBound, upperBound);
                else
                    return null;
            }
        }

        int point = result.subtract(b).divide(a, 4).intValue();

        System.out.println("point为" + point);

        if (point < lowerBound || point > upperBound) {
            if (point < lowerBound) {
                if (operator.equals(">")) {
                    if (this.a.compareTo(new BigDecimal(0)) > 0) {
                        return Pair.of(lowerBound, upperBound);
                    } else {
                        return Pair.of(lowerBound, upperBound);
                    }
                } else if (operator.equals("<")) {
                    if (this.a.compareTo(new BigDecimal(0)) < 0) {
                        return Pair.of(point + 1, upperBound);
                    } else {
                        return Pair.of(lowerBound, point - 1);
                    }
                } else if (operator.equals(">=")) {
                    if (this.a.compareTo(new BigDecimal(0)) > 0) {
                        return Pair.of(point, upperBound);
                    } else {
                        return Pair.of(lowerBound, point);
                    }
                } else if (operator.equals("<=")) {
                    if (this.a.compareTo(new BigDecimal(0)) < 0) {
                        return Pair.of(point, upperBound);
                    } else {
                        return Pair.of(lowerBound, point);
                    }
                }
            }
        } else {
            if (operator.equals(">")) {
                if (this.a.compareTo(new BigDecimal(0)) > 0) {
                    return Pair.of(point + 1, upperBound);
                } else {
                    return Pair.of(lowerBound, point - 1);
                }
            } else if (operator.equals("<")) {
                if (this.a.compareTo(new BigDecimal(0)) < 0) {
                    return Pair.of(point + 1, upperBound);
                } else {
                    return Pair.of(lowerBound, point - 1);
                }
            } else if (operator.equals(">=")) {
                if (this.a.compareTo(new BigDecimal(0)) > 0) {
                    return Pair.of(point, upperBound);
                } else {
                    return Pair.of(lowerBound, point);
                }
            } else if (operator.equals("<=")) {
                if (this.a.compareTo(new BigDecimal(0)) < 0) {
                    return Pair.of(point, upperBound);
                } else {
                    return Pair.of(lowerBound, point);
                }
            }
        }

        return null;
    }

    @Override
    public List<Integer> getSolveResult(BigDecimal result, String operator, int lowerBound, int upperBound) {
        List<Integer> resultSet = new ArrayList<>();
        for (int i = lowerBound; i <= upperBound; i++) {
            if (operator.equals(">")) {
                if (this.evaluate(i).compareTo(result) > 0) {
                    resultSet.add(i);
                }
            } else if (operator.equals("<")) {
                if (this.evaluate(i).compareTo(result) < 0) {
                    resultSet.add(i);
                }
            } else if (operator.equals(">=")) {
                if (this.evaluate(i).compareTo(result) >= 0) {
                    resultSet.add(i);
                }
            } else if (operator.equals("<=")) {
                if (this.evaluate(i).compareTo(result) <= 0) {
                    resultSet.add(i);
                }
            }
        }
        return resultSet;
    }

    public BigDecimal evaluate(int k) {
        BigDecimal bigDecimalK = new BigDecimal(k);
        return a.multiply(bigDecimalK).add(b);
    }

    @Override
    public String getExpression() {
        this.expression = this.a.toPlainString() + " * k + " + this.b.toPlainString();
        return this.expression;
    }

    // 求最大值和最小值仅仅用于Filter填合理的参数，不会用于约束链的计算
    @Override
    public BigDecimal getMaxValue(int lowerBound, int upperBound) {
        if (this.a.compareTo(new BigDecimal(0)) > 0) {
            return this.evaluate(upperBound);
        } else if (this.a.compareTo(new BigDecimal(0)) < 0) {
            return this.evaluate(lowerBound);
        } else {
            return this.evaluate(lowerBound);
        }
    }

    @Override
    public BigDecimal getMinValue(int lowerBound, int upperBound) {
        if (this.a.compareTo(new BigDecimal(0)) > 0) {
            return this.evaluate(lowerBound);
        } else if (this.a.compareTo(new BigDecimal(0)) < 0) {
            return this.evaluate(upperBound);
        } else {
            return this.evaluate(lowerBound);
        }
    }

    // 线性函数求反函数只用于主外键之间的运算，直接用整数计算
    @Override
    public List<Integer> getReverse(BigDecimal result, int lowBound, int upperBound) {
        List<Integer> ans = new ArrayList<>();
        int x = result.subtract(this.b).divide(this.a, 3, BigDecimal.ROUND_HALF_UP).intValue();
        if (x >= lowBound && x <= upperBound) {
            ans.add(x);
        }
        return ans;
    }

    public BigDecimal getCoefficient1() {
        return a;
    }

    public BigDecimal getCoefficient0() {
        return b;
    }

    public LinearFunc addLinearFunc(LinearFunc otherLineraFunc) {
        BigDecimal a = this.a.add(otherLineraFunc.getCoefficient1());

        BigDecimal b = this.b.add(otherLineraFunc.getCoefficient0());

        return new LinearFunc(a, b);
    }

    @Override
    public AttrGeneFunc copyMyself() {
        return new LinearFunc(this.a, this.b);
    }

    @Override
    public Pair<BigDecimal, BigDecimal> getMinValue(List<Integer> keyRange) {
        return null;
    }

    public String toString() {

        return this.a.toString() + " * k + " + this.b.toString();

    }

    @Override
    public LinearFunc clone() {
        try {
            LinearFunc clone = (LinearFunc) super.clone();
            // TODO: copy mutable state here, so the clone can't change the internals of the
            // original
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
