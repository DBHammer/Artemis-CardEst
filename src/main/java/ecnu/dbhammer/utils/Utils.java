package ecnu.dbhammer.utils;

import ecnu.dbhammer.configuration.Configurations;

/**
 * @author xiangzhaokun
 * @ClassName Utils.java
 * @Description 一些常用的方法，比如给出表的String类型名字，求的Table对象等
 * @createTime 2021年03月11日 14:42:00
 */
public class Utils {
    //返回最大值为upperBound，最接近于mod(modValue)=expectValue的值
    public static int getMaxValue(int upperBound, int modValue, int expectValue) {
        int maxValue;
        if (upperBound < modValue) {
            maxValue = upperBound;
        } else {
            int nowModValue = upperBound % modValue;
            if (expectValue < nowModValue) {
                maxValue = upperBound - (nowModValue - expectValue);
            } else {
                maxValue = upperBound - (modValue - (expectValue - nowModValue));
            }
        }
        return maxValue;
    }

    //单元测试
    public static void main(String[] args) {
        System.out.println(Utils.getMaxValue(11, 7, 3));//最接近10的mod5=2的值

    }

}
