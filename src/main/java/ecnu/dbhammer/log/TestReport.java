package ecnu.dbhammer.log;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * 生成报告
 * 报告包含数据库连接、生成的所有Query、所有计算结果和执行结果
 */
public class TestReport
{
    public static void recordReport(String readQueryPath, String queryCalculateResultPath
            , String queryExecuteResultPath, Set<Integer> errorQuerySet)
    {
        String filename = "test_report_";

        SimpleDateFormat sdf = new SimpleDateFormat();// 格式化时间
        sdf.applyPattern("MM-dd-HH-mm-ss");// a为am/pm的标记
        Date date = new Date();// 获取当前时间

        String nowDate = sdf.format(date);
        filename = filename + nowDate +".md";

        //把数据、Query.txt和建立表的语句都保存到Archive文件夹中
        String archiveName = "archive_"+nowDate;

        archiveSave(archiveName);

        //读取Query
        List<String> query = new ArrayList<String>();
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(readQueryPath)), "UTF-8"));
            String lineTxt = null;
            while ((lineTxt = br.readLine()) != null) {
                query.add(lineTxt);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        //读取计算结果
        List<String> calResult = new ArrayList<>();
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(queryCalculateResultPath)), "UTF-8"));
            String lineTxt = null;
            while ((lineTxt = br.readLine()) != null) {
                calResult.add(lineTxt);
            }
        }catch (IOException e){
            e.printStackTrace();
        }

        //读取执行结果
        List<String> execResult = new ArrayList<>();
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(queryExecuteResultPath)), "UTF-8"));
            String lineTxt = null;
            while ((lineTxt = br.readLine()) != null) {
                execResult.add(lineTxt);
            }
        }catch (IOException e){
            e.printStackTrace();
        }

        File file = new File(Configurations.getReportOutputDir(), filename);

        if (!file.exists())
        {
            try
            {
                file.createNewFile();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }


        write("## 生成的Query\r\n", filename);
        write("```\r\n", filename);

        for (String singleQuery : query ){
            write(singleQuery+"\r\n",filename);
        }
        write("```\r\n", filename);


        write("## 计算结果\r\n", filename);

        write("```\r\n", filename);
        for (String singleCalResult : calResult ){
            write(singleCalResult+"\r\n",filename);
        }

        write("```\r\n", filename);

        write("## 数据库执行的结果\r\n", filename);
        write("```\r\n", filename);

        for(String singleExecResult : execResult ){
            write(singleExecResult+"\r\n",filename);
        }
        write("```\r\n", filename);


        write("## 执行错误的Query\r\n", filename);

        if (errorQuerySet.size() == 0){
            write("\r\n",filename);
            write("此次测试没有执行错误的Query"+"\r\n",filename);

        }
        for (Integer singleErrorQueryNum : errorQuerySet ){
            write("Query #"+singleErrorQueryNum+"\r\n\n",filename);
            write(query.get(singleErrorQueryNum)+"\n",filename);
            write("计算结果："+calResult.get(singleErrorQueryNum.intValue())+"\n",filename);
            write("执行结果："+execResult.get(singleErrorQueryNum.intValue())+"\n\n",filename);
            write("------"+"\r\n",filename);
        }

    }
    /**
     * 写入报告文件
     *
     * @param aa 写入信息
     */
    private static void write(String aa, String filename)
    {
        File file = new File(Configurations.getReportOutputDir(), filename);
        try
        {
            FileWriter fileWritter = new FileWriter(Configurations.getReportOutputDir()+File.separator+file.getName(), true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            bufferWritter.write(aa);
            bufferWritter.flush();
            bufferWritter.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private static void archiveSave(String archiveDirName){

        File archiveDir = new File("./Archive"+File.separator+archiveDirName);
        if (!archiveDir.exists())
        {
            archiveDir.mkdir();
        }

        RecordLog.recordLog(LogLevelConstant.INFO,"测试数据、Query、理想结果、数据库执行结果、日志保存到目录："+archiveDir.getName());

        File archiveCalLogDir = new File("./Archive"+File.separator+archiveDirName+File.separator+"calculateLog");
        archiveCalLogDir.mkdir();

        File archiveCalResultDir = new File("./Archive"+File.separator+archiveDirName+File.separator+"calculateResult");
        archiveCalResultDir.mkdir();

        File archiveDataDir = new File("./Archive"+File.separator+archiveDirName+File.separator+"data");
        archiveDataDir.mkdir();

        File archiveExecResultDir = new File("./Archive"+File.separator+archiveDirName+File.separator+"executeResult");
        archiveExecResultDir.mkdir();

        File archiveQuerytDir = new File("./Archive"+File.separator+archiveDirName+File.separator+"query");
        archiveQuerytDir.mkdir();

        File archiveSchemaDir = new File("./Archive"+File.separator+archiveDirName+File.separator+"schema");
        archiveSchemaDir.mkdir();


        File calResultDir = new File(Configurations.getCalculateResultOutputDir());
        File dataDir = new File(Configurations.getDataoutputDir());
        File execResultDir = new File(Configurations.getExecuteResultOutputDir());
        File queryDir = new File(Configurations.getQueryOutputDir());
        File schemaDir = new File(Configurations.getSchemaOutputDir());
        try {
            FileUtils.copyDirectory(calResultDir, archiveCalResultDir);
            FileUtils.copyDirectory(dataDir, archiveDataDir);
            FileUtils.copyDirectory(execResultDir, archiveExecResultDir);
            FileUtils.copyDirectory(queryDir, archiveQuerytDir);
            FileUtils.copyDirectory(schemaDir, archiveSchemaDir);
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
