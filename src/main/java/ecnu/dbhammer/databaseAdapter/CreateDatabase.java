package ecnu.dbhammer.databaseAdapter;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.log.RecordLog;

import java.io.*;
import java.sql.*;
import java.util.List;

/**
 * 通过连接的数据库系统，创建用于测试的数据库实例
 */
public class CreateDatabase {

    public static void dropAndCreateDatabase(Connection conn) {
        // 从文件逐行读取查询语句，依次执行
        String testDBName = Configurations.getTestDatabaseName();
        try {
            Statement statement = conn.createStatement();

            String dropSQL = "drop database if exists " + testDBName;

            String createSQL = "create database " + testDBName;

            statement.execute(dropSQL);
            statement.execute(createSQL);
            
            statement.close();
            RecordLog.recordLog(LogLevelConstant.INFO,"删除并新建数据库"+testDBName+"成功!");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void reloadData(DBConnection dbConnection) throws Exception {
        RecordLog.recordLog(LogLevelConstant.INFO, "开始进行数据重新导入");
        Connection con = dbConnection.getDbConn();
        Statement statement = con.createStatement();

        // statement.execute("use " + Configurations.getTestDatabaseName());
        // statement.execute("set FOREIGN_KEY_CHECKS=0;");
        try {
            String createTableSqlDir = Configurations.getSchemaOutputDir() + File.separator + "createSchemaSQL"
                    + ".txt";
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(new File(createTableSqlDir)), "UTF-8"));
            String lineTxt;
            while ((lineTxt = br.readLine()) != null) {
                String createSql = lineTxt;
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

        String dbBrand = dbConnection.getDatabaseBrand();
        // 多少张表
        for (int i = 0; i < datafiles.length; i++) {
            tableName = "table_" + i;
            // 多少个线程
            for (int j = 0; j < Configurations.getThreadNumPerNode(); j++) {
                txtName = tableName + "_" + j + ".txt";
                Thread.sleep(10000);

                if (dbBrand.equalsIgnoreCase("tidb") || dbBrand.equalsIgnoreCase("mysql") ) {
                    loadStatement = "load data LOCAL infile '" + dbConnection.getUploadDir()
                            + "/" + txtName + "' into table " + tableName
                            + " fields terminated by ',' lines terminated by '\r\n'";
                    System.out.println(loadStatement);
                    statement.execute(loadStatement);
                } else if (dbBrand.equalsIgnoreCase("oceanbase")) { // oceanbase不支持从远程客户端加载数据,因此要把数据文件传到OB对应的rootserver上
                    loadStatement = "load data infile '" + dbConnection.getUploadDir()
                            + "/" + txtName + "' into table " + tableName
                            + " fields terminated by ',' lines terminated by '\r\n'";
                    System.out.println(loadStatement);
        
                    PreparedStatement pstmt = con.prepareStatement(loadStatement);
                    pstmt.execute();

                }
                 else if (dbBrand.equalsIgnoreCase("postgresql")) {
                    txtName = tableName + "_" + j + ".csv";
                    loadStatement = "\\copy " + tableName + " from " + "'" + dbConnection.getUploadDir() + "/" + txtName + "'"  + " delimiter ','";
                    System.out.println(loadStatement);
                    statement.execute(loadStatement);
                }
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
                Thread.sleep(1000);
                statement.execute(addForeignKey);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //con.close();
    }

    public static void reloadData4Tidb(DBConnection dbConnection) throws Exception {
        RecordLog.recordLog(LogLevelConstant.INFO, "开始进行外键导入");
        Connection con = dbConnection.getDbConn();
        Statement statement = con.createStatement();

        try {
            String addForeignKeySqlDir = Configurations.getSchemaOutputDir() + File.separator + "addForeignKey"
                    + ".txt";
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(new File(addForeignKeySqlDir)), "UTF-8"));
            String addForeignKey;
            while ((addForeignKey = br.readLine()) != null) {
                System.out.println(addForeignKey);
                Thread.sleep(1000);
                statement.execute(addForeignKey);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //con.close();
    }

}
