package ecnu.dbhammer.data;


import ecnu.dbhammer.schema.DBSchema;
import ecnu.dbhammer.schema.Table;

import java.io.*;
import java.util.List;
import java.util.concurrent.CountDownLatch;
/**
 * @author xiangzhaokun
 * @ClassName DataGenerationThread.java
 * @Description 数据生成线程，其中第60行的table.geneTuple(primaryKey)为生成每一行的数据
 */
public class DataGenerationThread extends Thread {

    // 待生成的测试数据库schema
    private DBSchema dbSchema;

    // 当前数据生成线程的id以及总的数据生成线程个数
    private int threadId;
    private int threadNum;

    // 生成数据输出目录
    private String outputDir;
    // 输出文本文件的编码格式
    private String encodeType;

    // 为提高用户体验，数据生成线程通过CountDownLatch告知主线程当前数据生成到第几个数据表了
    private List<CountDownLatch> cdl4Tables;

    public DataGenerationThread(DBSchema dbSchema, int threadId, int threadNum, String outputDir, String encodeType,
                                List<CountDownLatch> cdl4Tables) {
        super();
        this.dbSchema = dbSchema;
        this.threadId = threadId;
        this.threadNum = threadNum;
        this.outputDir = outputDir;
        this.encodeType = encodeType;
        this.cdl4Tables = cdl4Tables;
    }

    public void run() {

        List<Table> tables = dbSchema.getTableList();

        for (int i = 0; i < tables.size(); i++) {
            Table table = tables.get(i);
            int primaryKey = threadId;
            int tableSize = table.getTableSize();
            String createDataDir = outputDir;
            //System.out.println(createDataDir);
            File file_Data = new File(createDataDir);
            if (!file_Data.exists()) {
                file_Data.mkdirs();
            }
            String outputFileName = outputDir + File.separator + table.getTableName() + "_" + threadId + ".txt";
            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFileName), encodeType));) {
                while (primaryKey < tableSize) {
                    bw.write(table.geneTuple(primaryKey) + "\r\n");
                    //生成每一行生成的数据!!!
                    primaryKey += threadNum;
                }
                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }

            cdl4Tables.get(i).countDown();
        }
    }

}

