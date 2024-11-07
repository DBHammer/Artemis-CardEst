package ecnu.dbhammer.verification;


import com.opencsv.CSVReader;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.log.RecordLog;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
/**
 * //Vertify函数通过mathematica执行的结果和数据库执行的结果进行比较
 * //一行一行的比较Query执行的结果和Mathematics执行的结果
 * //TODO 自己实现字符串的比较-x
 */
public class Verify {

    public static Set<Integer> verifyResult(String calculateDir, String executeDir) throws Exception {
        File[] files1 = new File(calculateDir).listFiles();
        File[] files2 = new File(executeDir).listFiles();
        for(File file : files1)
        System.out.println(file.getName());
        for(File file : files2)
            System.out.println(file.getName());


        if(files1.length!= files2.length){
            System.out.println(files1.length);
            System.out.println(files2.length);



            throw new Exception("执行Query数量和生成Query数量不一致");
        }
        int n = files1.length;
        List<String[]> calResult = new ArrayList<>();
        List<String[]> execResult = new ArrayList<>();
        int errorCount = 0;
        Set<Integer> ErrorQuerySet = new HashSet<>();
        for(int i=0;i<n;i++) {
            String calResultPath = calculateDir + File.separator + "calculateResult_"+(i+1)+".csv";
            String execResultPath = executeDir + File.separator + "executeResult_"+(i+1)+".csv";
            int singleQueryError = 0;
            calResult = readFromCsv(calResultPath);
            execResult = readFromCsv(execResultPath);
            //对query结果一行一行的对比
            if(calResult.size()!=execResult.size()){
                errorCount++;
                singleQueryError++;
                RecordLog.recordLog(LogLevelConstant.ERROR,"第" + (i+1) + "个查询出现错误！");
                ErrorQuerySet.add(i);
                RecordLog.recordLog(LogLevelConstant.ERROR,"计算结果集大小："+calResult.size());
                RecordLog.recordLog(LogLevelConstant.ERROR,"执行结果集大小："+execResult.size());
            }else{
                //结果集大小一致，开始每一行每一列的对比
                int row = calResult.size();
                for(int k=0;k<row;k++){
                    String[] singleRow1 = calResult.get(k);
                    String[] singelRow2 = execResult.get(k);
                    if(singleRow1.length != singelRow2.length){
                        errorCount++;
                        singleQueryError++;
                        RecordLog.recordLog(LogLevelConstant.ERROR,"第" + (i+1) + "个查询出现错误！");
                        ErrorQuerySet.add(i);
                        RecordLog.recordLog(LogLevelConstant.ERROR,"原因是第"+(k)+"行的属性列数不一致");
                    }else{
                        //当该行的属性列一致了，开始一列一列的对比
                        int col = singleRow1.length;
                        for(int q=0;q<col;q++){
                            String position1 = singleRow1[q];
                            String position2 = singelRow2[q];
                            if(position1.indexOf(".") > 0){
                                position1 = position1.replaceAll("0+?$", "");//去掉多余的0
                                position1 = position1.replaceAll("[.]$", "");//如最后一位是.则去掉
                            }

                            if(position2.indexOf(".") > 0){
                                position2 = position2.replaceAll("0+?$", "");//去掉多余的0
                                position2 = position2.replaceAll("[.]$", "");//如最后一位是.则去掉
                            }
                            if (!position1.equals(position2)) {
                                errorCount++;
                                singleQueryError++;
                                RecordLog.recordLog(LogLevelConstant.ERROR,"第" + (i+1) + "个查询出现错误！");
                                ErrorQuerySet.add(i);
                                RecordLog.recordLog(LogLevelConstant.ERROR,"原因是第"+k+"行第"+q+"列不一致！");
                                RecordLog.recordLog(LogLevelConstant.ERROR,"计算结果："+position1);
                                RecordLog.recordLog(LogLevelConstant.ERROR,"执行结果："+position2);
                            }
                        }
                    }
                }
            }

            if(singleQueryError==0){
                RecordLog.recordLog(LogLevelConstant.INFO,"第" + (i+1) + "个查询执行正确！");
            }
        }
        if(errorCount == 0){
            RecordLog.recordLog(LogLevelConstant.INFO,"所有Query均执行正确！此次测试无问题");
        }
        return ErrorQuerySet;
    }

    public static List<String[]> readFromCsv(String fileName) throws IOException {
        List<String[]> ans = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(fileName);
             InputStreamReader isr = new InputStreamReader(fis,
                     StandardCharsets.UTF_8);
             CSVReader reader = new CSVReader(isr)) {
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                ans.add(nextLine);
            }
        }
        return ans;
    }
}
