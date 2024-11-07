package ecnu.dbhammer.utils;

import java.math.BigDecimal;
import java.util.Random;

//随机数生成器
public class RandomDataGenerator {

    private static final char[] chars =
            ("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz").toCharArray();

    //生成给定长度的随机字符串
    public static String geneString(int length) {
        char[] buffer = new char[length];
        for (int i = 0; i < length; i++) {
            buffer[i] = chars[(int)(Math.random() * 62)];
        }
        return new String(buffer);
    }

    public static BigDecimal geneRandomDecimal(int lowerBound, int upperBound){
        Random random = new Random();
        return new BigDecimal(random.nextInt(upperBound-lowerBound+1)+lowerBound);
    }

    public static int geneRandomInt(int lowerBound,int upperBound){
        Random random = new Random();
        return random.nextInt(upperBound-lowerBound+1)+lowerBound;
    }

    public static int geneIndex(int vectorSize){
        Random random = new Random();
        return random.nextInt(vectorSize);
    }
}
