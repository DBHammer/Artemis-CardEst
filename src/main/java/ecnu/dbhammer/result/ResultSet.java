package ecnu.dbhammer.result;

import java.util.*;

/**
 * @author xiangzhaokun
 * @ClassName ResultSet.java
 * @Description 用于保存符合条件的Key值
 * @createTime 2021年12月09日 21:07:00
 */
public class ResultSet {
    private int lowerBound;
    private int upperBound;
    private List<Integer> keyList;
    private int minValue;
    private int maxValue;

    private boolean onlyBound;//只需要获得上界和下界即可，不需要获得具体的list

    public ResultSet(){
        this.keyList = new ArrayList<>();
        this.lowerBound = -1;
        this.upperBound = -1;
        this.maxValue = -1;
        this.minValue = Integer.MAX_VALUE;
    }


    public int getSize(){
        if(this.judge())
            return upperBound-lowerBound+1;
        else
            return this.keyList.size();
    }


    public void setOnlyBound(boolean onlyBound){
        this.onlyBound = onlyBound;
    }

    public boolean isOnlyBound() {
        return onlyBound;
    }

    public void setLowerBound(int lowerBound){
        this.lowerBound = lowerBound;
    }

    public void setUpperBound(int upperBound){
        this.upperBound = upperBound;
    }
    public void add(int a){
        keyList.add(a);
        if(a>this.maxValue){
            this.maxValue = a;
        }
        if(a<this.minValue){
            this.minValue = a;
        }
    }

    public int getLowerBound() {
        if(lowerBound!=-1) {
            return lowerBound;
        }else{
            return keyList.get(0);
        }
    }

    public int getUpperBound() {
        if(upperBound!=-1)
        return upperBound;
        else
            return keyList.get(keyList.size()-1);
    }

    public boolean judge(){
        if(this.getKeyList() == null){
            if(this.lowerBound!=-1 && this.upperBound!=-1){
                return true;
            }
        }
        return  false;
    }

    public List<Integer> getKeyList(){
        return keyList;
    }

    public int size(){
        return keyList.size();
    }
    public int getMin(){
        return this.minValue;
    }
    public int getMax(){
        return this.maxValue;
    }



}
