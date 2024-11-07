package ecnu.dbhammer.data;


import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.schema.DBSchema;
import ecnu.dbhammer.schema.SchemaGenerator;
import ecnu.dbhammer.schema.Table;
import ecnu.dbhammer.utils.ClearDirectory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * @author mikaiming,xiangzhaokun
 * @ClassName DataGenerator.java
 * @Description 数据生成器，有了Schema信息以及所有属性的数据映射后，进行确定性的数据生成，会调用DataGenerationThread进行数据生成
 */
public class DataGenerator {

    // 节点Id
    private int nodeId;
    // 总的节点数目
    private int nodeNum;
    // 每个节点上的数据生成线程个数
    private int threadNumPerNode;

    // 待生成的测试数据库schema
    private DBSchema dbSchema;

    // 生成数据输出目录
    private String outputDir;
    // 输出文本文件的编码格式
    private String encodeType;


    public DataGenerator(int nodeId, int nodeNum, int threadNumPerNode, DBSchema dbSchema, String outputDir,
                         String encodeType) {
        super();
        this.nodeId = nodeId;
        this.nodeNum = nodeNum;
        this.threadNumPerNode = threadNumPerNode;
        this.dbSchema = dbSchema;
        this.outputDir = outputDir;
        this.encodeType = encodeType;
    }

    public void startAllDataGeneThreads() {

        // 用于展示数据生成进度，不断刷新数据生成进度，如：数据表1生成完成，数据表2生成完成，...
        List<CountDownLatch> cdl4Tables = new ArrayList<>();
        for (int i = 0; i < dbSchema.getTableList().size(); i++) {
            cdl4Tables.add(new CountDownLatch(threadNumPerNode));
        }

        //RecordLog.recordLog(LogLevelConstant.INFO,"开始启动节点" + nodeId + "上的数据生成线程...");

        for (int i = 0; i < threadNumPerNode; i++) {
            int threadId = nodeId * threadNumPerNode + i;
            int threadNum = nodeNum * threadNumPerNode;
            // 注意这里需要对dbSchema进行深拷贝，最初是为了解决Column中SimpleDateFormat非线程安全的问题
            // TODO 使用clone之后会改变columnGeneExpression 造成前后不一致
            new Thread(new DataGenerationThread(dbSchema, threadId, threadNum,
                    outputDir, encodeType, cdl4Tables)).start();
        }
        //RecordLog.recordLog(LogLevelConstant.INFO,"节点" + nodeId + "上的" + threadNumPerNode + "个数据生成线程全部启动成功！");

        //RecordLog.recordLog(LogLevelConstant.INFO,"开始生成测试数据");

        for (int i = 0; i < cdl4Tables.size(); i++) {
            try {
                cdl4Tables.get(i).await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //RecordLog.recordLog(LogLevelConstant.INFO,"数据表[" + dbSchema.getTableList().get(i).getTableName() + "]生成完成！大小"+dbSchema.getTableList().get(i).getTableSize());
        }
    }

}
