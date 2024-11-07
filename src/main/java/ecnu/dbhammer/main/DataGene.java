package ecnu.dbhammer.main;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.data.DataGenerator;
import ecnu.dbhammer.databaseAdapter.CreateDatabase;
import ecnu.dbhammer.databaseAdapter.DBConnection;
import ecnu.dbhammer.databaseAdapter.DatabaseConnection;
import ecnu.dbhammer.utils.SFTPConnection;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.schema.DBSchema;
import ecnu.dbhammer.utils.ClearDirectory;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;


/**
 * @author xiangzhaokun
 * @ClassName DataGene.java
 * @Description 数据生成
 * @createTime 2021年11月27日 15:30:00
 */
public class DataGene {
    public static void dataGene(DBSchema dbSchema) {

        long startTime = System.currentTimeMillis();
        int nodeNum = Configurations.getNodeNum();
        //RecordLog.recordLog(LogLevelConstant.INFO, "数据生成节点个数:" + nodeNum);
        int threadNumPerNode = Configurations.getThreadNumPerNode();
        String outputDir = Configurations.getDataoutputDir();
        String encodeType = Configurations.getEncodeType();
        //RecordLog.recordLog(LogLevelConstant.INFO, "数据生成输出文件:" + outputDir);

        //RecordLog.recordLog(LogLevelConstant.INFO, "清空" + outputDir + "下的数据");
        // 清空data/dbInstance下的数据
        ClearDirectory.deleteDir(outputDir);

        for (int i = 0; i < nodeNum; i++) {
            int nodeId = i;
            DataGenerator dataGenerator = new DataGenerator(nodeId, nodeNum, threadNumPerNode,
                    dbSchema, outputDir, encodeType);
            dataGenerator.startAllDataGeneThreads();
        } // 生成数据，并写入到文件中

        long endTime = System.currentTimeMillis();
        RecordLog.recordLog(LogLevelConstant.INFO, "数据生成消耗时间：" + (endTime - startTime)  + "ms");
        RecordLog.recordLog(LogLevelConstant.INFO, "一共生成了" + dbSchema.getTableList().size() + "个数据表");
        RecordLog.recordLog(LogLevelConstant.INFO, "数据生成结束");

    }

    public static void loadIntoDB(DBSchema dbSchema) {

        String createSql;
        RecordLog.recordLog(LogLevelConstant.INFO, "连接数据库......");

        List<DBConnection> dbConnections = Configurations.getDBConnections();

        for(DBConnection dbConnection : dbConnections) {
            //如果数据库是Ob的话，先将数据文件传输到对应的机器上
            String dbBrand = dbConnection.getDatabaseBrand();
            try {
                if (dbBrand.equalsIgnoreCase("oceanbase")) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "OceanBase数据库需要将数据文件传到RootServer上");
                    File fileDir = new File(Configurations.getDataoutputDir());
                    RecordLog.recordLog(LogLevelConstant.INFO, "一共需要传输" + fileDir.list().length + "个数据文件");
//                    for (File file : fileDir.listFiles()) {
//                        String uploadDataFile = Configurations.getDataoutputDir() + "/" + file.getName();
//                        RecordLog.recordLog(LogLevelConstant.INFO, "开始传输" + uploadDataFile);
//                        String cmd = "docker cp " + uploadDataFile + " obxzk:/home/admin/obData";
//                        System.out.println(cmd);
//                        Runtime.getRuntime().exec(cmd);
//                        Thread.sleep(2000);
//                    }

                    SFTPConnection sftpConnection = new SFTPConnection();
                    sftpConnection.connect(dbConnection.getServerUser(),dbConnection.getServerPassword(),
                            dbConnection.getServerHost(),Integer.parseInt(dbConnection.getServerPort()));
                    sftpConnection.upload(dbConnection.getUploadDir(),Configurations.getDataoutputDir());
                    sftpConnection.close();
                    System.out.println("OceanBase数据传输完成");

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            List<Connection> conns = DatabaseConnection.getDatabaseConnectionForFirst();
            System.out.println("here 2");
            for (int m = 0; m < conns.size(); m++) {
                RecordLog.recordLog(LogLevelConstant.INFO, "连接数据库" + (m + 1));
                Connection con = conns.get(m);
                String dbBrand = Configurations.getDBConnections().get(m).getDatabaseBrand();
                if (!dbBrand.equalsIgnoreCase("sqlite")) {
                    CreateDatabase.dropAndCreateDatabase(con);
                }
                RecordLog.recordLog(LogLevelConstant.INFO, "开始创建Schema中的表");
                Statement statement;

                if (dbBrand.equalsIgnoreCase("postgresql") || dbBrand.equalsIgnoreCase("gaussdb")) {
                    con = DatabaseConnection.changeDatabaseConnectionByDatabaseName(Configurations.getDBConnections().get(m));
                    statement = con.createStatement();
                } else {
                    statement = con.createStatement();
                    statement.execute("use " + Configurations.getTestDatabaseName());
                    statement.execute("set FOREIGN_KEY_CHECKS=0;");
                }

                try {
                    String createTableSqlDir = Configurations.getSchemaOutputDir() + File.separator + "createSchemaSQL"
                            + ".txt";
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(new FileInputStream(new File(createTableSqlDir)), "UTF-8"));
                    String lineTxt;
                    while ((lineTxt = br.readLine()) != null) {
                        createSql = lineTxt;
                        System.out.println(createSql);
                        statement.execute(createSql);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                RecordLog.recordLog(LogLevelConstant.INFO, "Schema中所有表创建成功！");

                RecordLog.recordLog(LogLevelConstant.INFO, "开始导入数据！");
                String tableName = null;
                String txtName = null;
                String loadStatement = null;



                // 多少张表
                for (int i = 0; i < dbSchema.getTableList().size(); i++) {
                    tableName = "table_" + i;
                    // 多少个线程
                    for (int j = 0; j < Configurations.getThreadNumPerNode(); j++) {
                        txtName = tableName + "_" + j + ".txt";

                        if (dbBrand.equalsIgnoreCase("tidb") || dbBrand.equalsIgnoreCase("mysql") ) {
                            loadStatement = "load data LOCAL infile '" + Configurations.getDataoutputDir()
                                    + "/" + txtName + "' into table " + tableName
                                    + " fields terminated by ',' lines terminated by '\r\n'";
                            System.out.println(loadStatement);
                            statement.execute(loadStatement);
                        } else if (dbBrand.equalsIgnoreCase("oceanbase")) { // oceanbase不支持从远程客户端加载数据,因此要把数据文件传到OB对应的rootserver上
                            loadStatement = "load data infile '" + Configurations.getDBConnections().get(m).getUploadDir()
                                    + "/" + txtName + "' into table " + tableName
                                    + " fields terminated by ',' lines terminated by '\r\n'";
                            System.out.println(loadStatement);
                            PreparedStatement pstmt = con.prepareStatement(loadStatement);
                            //statement.execute(loadStatement);
                            pstmt.execute();

                        }else if(dbBrand.equalsIgnoreCase("postgresql")){
                            CopyManager copyManager = new CopyManager((BaseConnection) con);
                            FileInputStream fileInputStream = new FileInputStream(Configurations.getDataoutputDir()+"/"+txtName);
                            loadStatement = "copy "+ tableName + " from stdin WITH DELIMITER ','";
                            System.out.println(loadStatement);
                            copyManager.copyIn(loadStatement, fileInputStream);
                        }
                        else if(dbBrand.equalsIgnoreCase("gaussdb")) {
                            CopyManager copyManager = new CopyManager((BaseConnection) con);
                            FileInputStream fileInputStream = new FileInputStream(Configurations.getDataoutputDir()+"/"+txtName);
                            loadStatement = "copy " + tableName + " from stdin WITH DELIMITER ','";
                            System.out.println(loadStatement);
                            copyManager.copyIn(loadStatement, fileInputStream);
                        }
                        else if (dbBrand.equalsIgnoreCase("sqlite")) {
                            // sqlite3导入数据命令 sqlite3 Achilles.db ".import ./data/dbInstance/table_0_2_0.txt
                            // --csv table_0_2" ".quit"
                            String cmdList = "cmd.list";
                            File file = new File(cmdList);
                            if (!file.exists()) {
                                file.createNewFile();
                            }

                            FileWriter fw = null;
                            BufferedWriter bw = null;
                            try {
                                fw = new FileWriter(file);
                                bw = new BufferedWriter(fw);
                                for (int k = 0; k < 3; k++) {
                                    if (k == 0) {
                                        bw.write(".mode csv" + "\n");
                                    } else if (k == 1) {
                                        bw.write(".import ./data/dbInstance/" + txtName + " --csv " + tableName + "\n");
                                    } else {
                                        bw.write(".quit");
                                    }
                                }

                            } catch (IOException e) {
                                e.printStackTrace();
                            } finally {
                                bw.close();
                                fw.close();
                            }
                            String[] cmd = { "sqlite3", "Achilles.db", ".read cmd.list" };
                            System.out.println(cmd[0] + " " + cmd[1] + " " + cmd[2]);
                            Runtime.getRuntime().exec(cmd);
                            Thread.sleep(3000);
                        }
                        // statement.execute("set FOREIGN_KEY_CHECKS=1;");
                        System.out.println(txtName + "数据导入成功！");
                    }
                    RecordLog.recordLog(LogLevelConstant.INFO, tableName + "表数据导入成功！");
                }

                RecordLog.recordLog(LogLevelConstant.INFO, "开始创建外键");
                try {
                    String addForeignKeySqlDir = Configurations.getSchemaOutputDir() + File.separator + "addForeignKey"
                            + ".txt";
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(new FileInputStream(new File(addForeignKeySqlDir)), "UTF-8"));
                    String addForeignKey;
                    while ((addForeignKey = br.readLine()) != null) {
                        System.out.println(addForeignKey);
                        statement.execute(addForeignKey);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                statement.close();
                con.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
