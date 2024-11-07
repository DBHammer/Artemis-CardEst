package ecnu.dbhammer.schema.genefunc;

import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName ModFunc.java
 * @Description 取模函数
 * @createTime 2021年12月01日 21:09:00
 */
public class ModFunc implements AttrGeneFunc, Serializable {
    private int base;
    private String expression;

    public ModFunc(int base){
        this.base = base;
    }

    public ModFunc(){

    }
    public int getBase() {
        return base;
    }

    public void setBase(int base) {
        this.base = base;
    }

    public void setExpression(String expression){ this.expression = expression; }

    @Override
    public Pair<Integer, Integer> getSolveBound(BigDecimal result, String operator, int lowerBound, int upperBound) {
//        int ans=0;
//        for(int i = lowerBound; i<=upperBound; i++){
//            if(operator.equals(">")){
//                if(this.evaluate(i).compareTo(result)>0){
//                    ans++;
//                }
//            }else if(operator.equals("<")){
//                if(this.evaluate(i).compareTo(result)<0){
//                    ans++;
//                }
//            }else if(operator.equals(">=")){
//                if(this.evaluate(i).compareTo(result)>=0){
//                    ans++;
//                }
//            }else if(operator.equals("<=")){
//                if(this.evaluate(i).compareTo(result)<=0){
//                    ans++;
//                }
//            }
//        }
        return null;
    }

    @Override
    public Integer getPKByParam(BigDecimal param) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Integer> getSolveResult(BigDecimal result, String operator, int lowerBound, int upperBound) {
        List<Integer> resultSet = new ArrayList<>();
        for(int i = lowerBound; i<=upperBound; i++){
            if(operator.equals(">")){
                if(this.evaluate(i).compareTo(result)>0){
                    resultSet.add(i);
                }
            }else if(operator.equals("<")){
                if(this.evaluate(i).compareTo(result)<0){
                    resultSet.add(i);
                }
            }else if(operator.equals(">=")){
                if(this.evaluate(i).compareTo(result)>=0){
                    resultSet.add(i);
                }
            }else if(operator.equals("<=")){
                if(this.evaluate(i).compareTo(result)<=0){
                    resultSet.add(i);
                }
            }
        }
        return resultSet;
    }

    @Override
    public BigDecimal evaluate(int k) {
        return new BigDecimal(k%this.base);
    }

    @Override
    public String getExpression() {
        this.expression = "k mod " + this.base;
        return this.expression;
    }

    @Override
    public BigDecimal getMaxValue(int lowerBound, int upperBound) {
        return new BigDecimal(this.base-1);
    }

    @Override
    public BigDecimal getMinValue(int lowerBound, int upperBound) {
        return new BigDecimal(0);
    }

    @Override
    public List<Integer> getReverse(BigDecimal result, int lowBound, int upperBound) {
        //在lowerBound和upperBound之间的X，满足x % base = result的x
        List<Integer> ans = new ArrayList<>();
        int x = result.intValue();
        while(x<=upperBound && x>=lowBound){
            ans.add(x);
            x += this.base;
        }
        return ans;

    }

    @Override
    public AttrGeneFunc copyMyself() {
        return new ModFunc(this.base);
    }

    @Override
    public Pair<BigDecimal, BigDecimal> getMinValue(List<Integer> keyRange) {
        return null;
    }


}
