package ecnu.dbhammer.utils;

/**
 * @author xiangzhaokun
 * @ClassName Randomly.java
 * @Description 用于随机的类
 * @createTime 2022年05月15日 17:12:00
 */
public class Randomly {
    private static int getRandomInt(int lower, int upper) {
        return (int) (Math.random()*upper+lower);
    }
    public static <T> T fromOptions(T... options) {
        return options[getRandomInt(0, options.length)];
    }
}
