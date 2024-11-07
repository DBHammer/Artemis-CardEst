package ecnu.dbhammer.main;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.PrintConversionEvent;

import org.apache.commons.io.FileUtils;

import ecnu.dbhammer.configuration.Configurations;

/**
 * 测试数据保存类，可以自命名测试任务，然后把相关的数据保存下来
 */
public class archive {

    public static void archive(String dir,String userDir){

        File archiveDir = new File(userDir+File.separator+dir);
        if (!archiveDir.exists())
        {
            System.out.println("不存在，创建"+archiveDir.getName());
            archiveDir.mkdir();
        }else{
            System.out.println("已存在"+archiveDir.getName()+"请修改本次测试任务的Index");
            System.exit(1);
        }

        
        File targetDataDir = new File(userDir+File.separator+dir+File.separator+"data");
        targetDataDir.mkdir();

        File targetQueryDir = new File(userDir+File.separator+dir+File.separator+"query");
        targetQueryDir.mkdir();

        File targetSchemaDir = new File(userDir+File.separator+dir+File.separator+"schema");
        targetSchemaDir.mkdir();

        File dataDir = new File("./data");
        File queryDir = new File("./query");
        File schemaDir = new File("./schema");
        try {
            System.out.println("开始复制Schema文件，从"+schemaDir.getCanonicalPath()+"复制到"+targetSchemaDir.getCanonicalPath());
            FileUtils.copyDirectory(schemaDir,targetSchemaDir);
            System.out.println("开始复制数据文件，从"+dataDir.getCanonicalPath()+"复制到"+targetDataDir.getCanonicalPath());
            FileUtils.copyDirectory(dataDir, targetDataDir);
            System.out.println("开始复制Query文件，从"+queryDir.getCanonicalPath()+"复制到"+targetQueryDir.getCanonicalPath());
            FileUtils.copyDirectory(queryDir, targetQueryDir);
        }catch (IOException e){
            e.printStackTrace();
        } 
    }

    public static void main(String[] args) {
        int nowTaskIndex = 35;
        //每次测试修改这里
        List<String> testTask = new ArrayList<>();
        testTask.add("single10000uniform");//0
        testTask.add("single50000uniform");//1
        testTask.add("single100000uniform");//2
        testTask.add("single10000skew1");//3
        testTask.add("single50000skew1");//4
        testTask.add("single100000skew1");//5
        testTask.add("single10000skew3");//6
        testTask.add("single50000skew3");//7
        testTask.add("single100000skew3");//8

        testTask.add("3join10000uniform");//9
        testTask.add("3join50000uniform");//10
        testTask.add("3join100000uniform");//11
        testTask.add("3join10000skew1");//12
        testTask.add("3join50000skew1");//13
        testTask.add("3join100000skew1");//14
        testTask.add("3join10000skew3");//15
        testTask.add("3join50000skew3");//16
        testTask.add("3join100000skew3");//17

        testTask.add("5join10000uniform");//18
        testTask.add("5join50000uniform");//19
        testTask.add("5join100000uniform");//20
        testTask.add("5join10000skew1");//21
        testTask.add("5join50000skew1");//22
        testTask.add("5join100000skew1");//23
        testTask.add("5join10000skew3");//24
        testTask.add("5join50000skew3");//25
        testTask.add("5join100000skew3");//26


        testTask.add("7join10000uniform");//27
        testTask.add("7join50000uniform");//28
        testTask.add("7join100000uniform");//29
        testTask.add("7join10000skew1");//30
        testTask.add("7join50000skew1");//31
        testTask.add("7join100000skew1");//32
        testTask.add("7join10000skew3");//33
        testTask.add("7join50000skew3");//34
        testTask.add("7join100000skew3");//35


        String userDir = System.getProperty("user.home") + File.separator + "CardinalityBenchmarking";
        System.out.println("目前存在的文件");
        archive(testTask.get(nowTaskIndex),userDir);
        for(File file : new File(userDir).listFiles()){
            System.out.println(file.getAbsolutePath());
        }

    }
    
}
