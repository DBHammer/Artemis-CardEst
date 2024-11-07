package ecnu.dbhammer.schema.genefunc;

import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @author xiangzhaokun
 * @ClassName PiecewiseFunc.java
 * @Description TODO
 * @createTime 2021年11月28日 22:39:00
 */
public class PiecewiseFunc implements AttrGeneFunc{
    //f[x_] := Piecewise[{{15, 0<= x <6},{87, 6<= x <12},{45, 12<= x <54},{73, 54<= x <81},{95, 81<= x <84},{88, 84<= x <88},{99, 88<= x <89},{26, 89<= x <99},{62, 99<= x <142},{23, 142<= x <155},{84, 155<= x <163},{17, 163<= x <173},{46, 173<= x <216},{36, 216<= x <253},{71, 253<= x <284},{78, 284<= x <295},{89, 295<= x <305},{43, 305<= x <350},{42, 350<= x <394},{25, 394<= x <401},{59, 401<= x <449},{68, 449<= x <478},{20, 478<= x <486},{28, 486<= x <500},{58, 500<= x <540},{32, 540<= x <554},{66, 554<= x <595},{39, 595<= x <633},{54, 633<= x <669},{79, 669<= x <683},{63, 683<= x <720},{14, 720<= x <723},{77, 723<= x <733},{24, 733<= x <743},{76, 743<= x <758},{21, 758<= x <764},{94, 764<= x <767},{56, 767<= x <814},{72, 814<= x <834},{50, 834<= x <885},{97, 885<= x <885},{61, 885<= x <934},{91, 934<= x <934},{18, 934<= x <938},{67, 938<= x <970},{47, 970<= x <1020},{83, 1020<= x <1026},{70, 1026<= x <1046},{49, 1046<= x <1094},{85, 1094<= x <1099},{27, 1099<= x <1115},{51, 1115<= x <1174},{55, 1174<= x <1231},{60, 1231<= x <1269},{37, 1269<= x <1298},{65, 1298<= x <1336},{6, 1336<= x <1337},{31, 1337<= x <1356},{86, 1356<= x <1360},{92, 1360<= x <1361},{41, 1361<= x <1401},{35, 1401<= x <1425},{90, 1425<= x <1429},{40, 1429<= x <1463},{81, 1463<= x <1472},{7, 1472<= x <1472},{53, 1472<= x <1518},{30, 1518<= x <1535},{75, 1535<= x <1559},{93, 1559<= x <1560},{11, 1560<= x <1560},{9, 1560<= x <1561},{33, 1561<= x <1586},{80, 1586<= x <1600},{69, 1600<= x <1623},{34, 1623<= x <1660},{74, 1660<= x <1680},{38, 1680<= x <1706},{57, 1706<= x <1747},{96, 1747<= x <1752},{22, 1752<= x <1759},{82, 1759<= x <1768},{44, 1768<= x <1803},{98, 1803<= x <1803},{8, 1803<= x <1806},{52, 1806<= x <1860},{16, 1860<= x <1864},{10, 1864<= x <1866},{29, 1866<= x <1887},{12, 1887<= x <1889},{48, 1889<= x <1943},{13, 1943<= x <1944},{19, 1944<= x <1953},{64, 1953<= x <1998},{5, 1998<= x <2000}}]

    //TODO 想想如何表达分段函数
    Map<Pair<Integer, Integer>,Integer> map;


    @Override
    public Pair<Integer, Integer> getSolveBound(BigDecimal result, String operator, int lowerBound, int upperBound) {
        return null;
    }

    @Override
    public List<Integer> getSolveResult(BigDecimal result, String operator, int lowerBound, int upperBound) {
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

    @Override
    public Integer getPKByParam(BigDecimal param) {
        // TODO Auto-generated method stub
        return null;
    }


}
