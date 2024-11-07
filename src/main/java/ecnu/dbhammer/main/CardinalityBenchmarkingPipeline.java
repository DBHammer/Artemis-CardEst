package ecnu.dbhammer.main;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.databaseAdapter.CreateDatabase;
import ecnu.dbhammer.databaseAdapter.DBConnection;
import ecnu.dbhammer.databaseAdapter.DatabaseConnection;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.schema.DBSchema;
import ecnu.dbhammer.utils.SFTPConnection;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName CardinalityBenchmarkingPipeline.java
 * @Description     //首先生成数据
 *     //再将数据load到多个数据库上
 *     //再不停地explain每个query，获得基数值，然后删除数据库重新导入，获得下个基数值
 * @createTime 2022年04月22日 23:21:00
 */
public class CardinalityBenchmarkingPipeline {



    public static void loadIntoMultipleDB() {

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

                    SFTPConnection sftpConnection = new SFTPConnection();
                    sftpConnection.connect(dbConnection.getServerUser(),dbConnection.getServerPassword(),
                            dbConnection.getServerHost(),Integer.parseInt(dbConnection.getServerPort()));
                    sftpConnection.upload(dbConnection.getUploadDir(),Configurations.getDataoutputDir());
                    sftpConnection.close();

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            List<Connection> conns = DatabaseConnection.getDatabaseConnectionForFirst();
            for (int m = 0; m < conns.size(); m++) {
                RecordLog.recordLog(LogLevelConstant.INFO, "连接数据库" + (m + 1));
                Connection con = conns.get(m);
                String dbBrand = Configurations.getDBConnections().get(m).getDatabaseBrand();
                if (!dbBrand.equalsIgnoreCase("sqlite")) {
                    CreateDatabase.dropAndCreateDatabase(con);
                }
                RecordLog.recordLog(LogLevelConstant.INFO, "开始创建Schema中的表");
                Statement statement = con.createStatement();
                statement.execute("use " + Configurations.getTestDatabaseName());
                statement.execute("set FOREIGN_KEY_CHECKS=0;");
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


                File file1 = new File(Configurations.getDataoutputDir());
                String[] datafiles = file1.list();

                // 多少张表
                for (int i = 0; i < datafiles.length; i++) {
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

                        } else if (dbBrand.equalsIgnoreCase("sqlite")) {
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

                con.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        File file = new File(Configurations.getDataoutputDir()+File.separator+".DS_Store");
        if(file.exists()){
            System.out.println("Mac系统Data文件夹存在ds_store，删除");
            file.delete();
        }

        // Main.main(null);

        System.out.println("开始测试基数预估!");
        TestCardinalityEstimation.testCard();

        System.out.println("开始AnalyzeTable");
        TestCardinalityEstimation.analyzeTable();

        System.out.println("开始测试analytable后基数预估!");
        TestCardinalityEstimation.testCardAnalyzeTable();

        //将所有数据、schema和query保存下来
        //archive.main(null);

    }
}
