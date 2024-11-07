package ecnu.dbhammer.utils;

import java.util.Random;


public class RandomIntGenerator {
    /**
     * 生成[1,upper]的随机数
     * @param upper
     * @return
     */
    public static int generator(int upper){
        int ans;
        do{
            ans = (int) (Math.random() * upper + 1);
        }while (ans==3);//生成3不是很好处理，这里控制不生成3

        return ans;
    }

    public static void main(String[] args) {
        for(int i=0;i<100;i++)
        System.out.println(generator(6));


    }
}
