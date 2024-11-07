package ecnu.dbhammer.JOBBenchmark;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.databaseAdapter.CreateDatabase;
import ecnu.dbhammer.databaseAdapter.DBConnection;
import ecnu.dbhammer.databaseAdapter.DatabaseConnection;
import ecnu.dbhammer.log.RecordLog;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

public class JOBBenchmark {

    public void loadData(String schemaFile, String tableDataFilesDir, String indexFile) {
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
                    //建表

                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(new FileInputStream(new File(schemaFile)), "UTF-8"));
                    String lineTxt;
                    while ((lineTxt = br.readLine()) != null) {
                        if (lineTxt.matches("[\\s]*") || lineTxt.matches("[ ]*##[\\s\\S]*")) {
                            continue;
                        }
                        statement.execute(lineTxt);
                    }

                    //导入数据
                    RecordLog.recordLog(LogLevelConstant.INFO, "开始导入数据");
                    File TablesDir = new File(tableDataFilesDir);
                    File[] tableDataFiles = TablesDir.listFiles();
                    for (int i = 0; i < tableDataFiles.length; i++) {
                        if(!tableDataFiles[i].getName().contains(".csv")) continue;
                        int index = tableDataFiles[i].getName().indexOf("#");
                        if(index == -1) index = tableDataFiles[i].getName().indexOf(".");
                        String tableName = tableDataFiles[i].getName().substring(0, index);
                        RecordLog.recordLog(LogLevelConstant.INFO, "开始导入" + tableName);
                        System.out.println(tableDataFiles[i].getCanonicalPath());
                        if (dbBrand.equalsIgnoreCase("mysql") || dbBrand.equalsIgnoreCase("tidb")) {
                            statement.execute("LOAD DATA LOCAL INFILE '" + tableDataFiles[i].getCanonicalPath() + "' INTO TABLE "
                                    + tableName + " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'");
                        } else if (dbBrand.equals("oceanbase")) {
                            String loadStatement = "load data infile '" + "/home/admin/obData/"
                                    + tableDataFiles[i].getName() + "' into table " + tableName
                                    + " fields terminated by ',' lines terminated by '\n'";
                            System.out.println(loadStatement);
                            PreparedStatement pstmt = con.prepareStatement(loadStatement);
                            //statement.execute(loadStatement);
                            pstmt.execute();
                        }
                    }
                    System.out.println("数据导入成功！");

                } catch (IOException e) {
                    e.printStackTrace();
                }

                //创建索引
                try {

                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(new FileInputStream(new File(indexFile)), "UTF-8"));
                    String addIndex;
                    while ((addIndex = br.readLine()) != null) {
                        if (addIndex.matches("[\\s]*") || addIndex.matches("[ ]*##[\\s\\S]*")) {
                            continue;
                        }
                        System.out.println(addIndex);
                        statement.execute(addIndex);
                    }
                    System.out.println("创建索引成功！");
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

    public void runSQL(String jobSQLFilesDir){
        try {
            List<DBConnection> dbConnections = DatabaseConnection.getDatabaseConnection(false);
            for (int i = 0; i < dbConnections.size(); i++) {
                RecordLog.recordLog(LogLevelConstant.INFO, "连接数据库" + (i + 1));
                Connection con = dbConnections.get(i).getDbConn();
                String dbBrand = Configurations.getDBConnections().get(i).getDatabaseBrand();
                Statement statement = con.createStatement();
                File SQLDir = new File(jobSQLFilesDir);
                File[] SQLFiles = SQLDir.listFiles();
                for (int n = 0; n < SQLFiles.length; n++) {
                    try {
                        BufferedReader br = new BufferedReader(
                                new InputStreamReader(new FileInputStream(SQLFiles[i]), "UTF-8"));
                        String line;
                        StringBuilder jobSQL = new StringBuilder();
                        while ((line = br.readLine()) != null) {
                            if (line.matches("[\\s]*") || line.matches("[ ]*##[\\s\\S]*")) {
                                continue;
                            }
                            jobSQL.append(line);
                        }
                        System.out.println(n + " "+jobSQL.toString());
                        long startTime = System.currentTimeMillis();
                        statement.execute(jobSQL.toString());
                        long endTime = System.currentTimeMillis();
                        long executeTime = endTime - startTime;
                        System.out.println("Time: " + executeTime);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String schemaFile = "/home/zr/tingc/JoinOrderBenchmark/imdb_data_csv/schematext.sql";
        String tableDataFilesDir = "/home/zr/tingc/JoinOrderBenchmark/imdb_data_csv";
        String indexFile = "/home/zr/tingc/JoinOrderBenchmark/imdb_data_csv/createindex.sql";
        String jobSQLFilesDir = "/home/zr/tingc/JoinOrderBenchmark/job";
        JOBBenchmark JOBBenchmark = new JOBBenchmark();
        //joBenchmark.loadData(schemaFile,tableDataFilesDir,indexFile);
        JOBBenchmark.runSQL(jobSQLFilesDir);
   }
}
