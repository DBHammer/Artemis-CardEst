package ecnu.dbhammer.test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName CompareWithCardEst.java
 * @Description TODO
 * @createTime 2021年12月28日 15:44:00
 */
public class CompareWithCardEst {
    public static void main(String[] args) throws IOException {
        String filePath = "CompareWorks/stats_CEB/stats_CEB.sql";

        String dirPath = "CompareWorks/stats_CEB/Single";


        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(filePath)),"UTF-8"));
        List<String> queryList = new ArrayList<>();
        String lineTxt = null;
        while ((lineTxt = br.readLine())!= null) {
            System.out.println(lineTxt);


            System.out.println(lineTxt.split("\\|\\|")[1]);


            queryList.add(lineTxt.split("\\|\\|")[1]);
        }

        File file = new File(dirPath);
        if(!file.exists()){
            file.mkdir();
        }
        for(int i=0;i< queryList.size();i++){

            File file1 = new File(dirPath,"query"+i+".sql");
            if(!file1.exists()){
                file1.createNewFile();
            }
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file1));
            bufferedWriter.write(queryList.get(i));
            bufferedWriter.close();
        }


    }
}
