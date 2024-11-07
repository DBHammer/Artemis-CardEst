package ecnu.dbhammer.databaseAdapter;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.log.RecordLog;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
/**
 * 数据库连接类，可以配置mysql、oceanbase、postgresql等
 */
public class DatabaseConnection {

    /**
     * 获得所有数据库的连接，不带数据库名称
     * @return
     * @throws Exception
     */
    public static List<Connection> getDatabaseConnectionForFirst() throws Exception{
        List<Connection> conns = new ArrayList<>();
        List<DBConnection> dbConnConfig = Configurations.getDBConnections();

        for(DBConnection db : dbConnConfig) {
            Connection conn;
            String dbBrand = db.getDatabaseBrand();
            String dbIP = db.getDatabaseIP();
            String port = db.getDatabasePort();
            String dbUsername = db.getDbUsername();
            String dbPassword = db.getDbPassword();
            String dbURL="";

            if (dbBrand.equalsIgnoreCase("TiDB") || dbBrand.equalsIgnoreCase("mysql")
                    || dbBrand.equalsIgnoreCase("oceanbase") || dbBrand.equalsIgnoreCase("matrixone")) {
                if (dbBrand.equalsIgnoreCase("TiDB")) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "本次测试数据库为TiDB");
                    dbURL = "jdbc:mysql://" + dbIP + ":" + port + "/" + "?useSSL=false&allowLoadLocalInfile=true";
                }
                if (dbBrand.equalsIgnoreCase("mysql")) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "本次测试数据库为MySQL");
                    dbURL = "jdbc:mysql://" + dbIP + ":" + port + "/mysql" + "?useSSL=false";
                }
                if (dbBrand.equalsIgnoreCase("oceanbase")) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "本次测试数据库为OceanBase");
                    dbURL = "jdbc:mysql://" + dbIP + ":" + port + "/oceanbase" + "?useSSL=false";
                }
                if (dbBrand.equalsIgnoreCase("matrixone")) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "本次测试数据库为MatrixOne");
                    dbURL = "jdbc:mysql://" + dbIP + ":" + port + "/" + "?useSSL=false";
                }
                String driver = "com.mysql.jdbc.Driver";

                Class.forName(driver);
                conn = DriverManager.getConnection(dbURL, dbUsername, dbPassword);
                if (!conn.isClosed()) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "成功连接数据库！IP:" + dbIP + "端口:" + port);
                }
                conns.add(conn);
            } else if (dbBrand.equalsIgnoreCase("sqlite")) {
                RecordLog.recordLog(LogLevelConstant.INFO, "本次测试数据库为sqlite");
                String driver = "org.sqlite.JDBC";
                dbURL = "jdbc:sqlite:Achilles.db";
                Class.forName(driver);
                conn = DriverManager.getConnection(dbURL);
                if (!conn.isClosed()) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "成功连接数据库sqlite");
                    RecordLog.recordLog(LogLevelConstant.INFO, "开始删除表");
                    try {
                        Statement stmt = conn.createStatement();
                        List<String> preDelTables = new ArrayList<String>();
                        ResultSet rs = conn.getMetaData().getTables(null, null, null, null);
                        while (rs.next()) {
                            preDelTables.add(rs.getString("TABLE_NAME"));
                        }
                        for (String pre : preDelTables) {
                            RecordLog.recordLog(LogLevelConstant.INFO, "开始删除表" + pre);
                            stmt.executeUpdate("drop table " + pre);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }


                }
                conns.add(conn);
            }else if (dbBrand.equalsIgnoreCase("postgresql") || dbBrand.equalsIgnoreCase("gaussdb")){
                if (dbBrand.equalsIgnoreCase("postgresql")) {
                   RecordLog.recordLog(LogLevelConstant.INFO, "本次测试数据库为postgresql"); 
                   dbURL = "jdbc:postgresql://" + dbIP + ":" + port + "/postgres" + "?useSSL=false";
                }
                if (dbBrand.equalsIgnoreCase("gaussdb")) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "本次测试数据库为gaussdb");
                    dbURL = "jdbc:postgresql://" + dbIP + ":" + port + "/gaussdb" + "?useSSL=false";
                }
                String driver = "org.postgresql.Driver";
                Class.forName(driver);
                conn = DriverManager.getConnection(dbURL, dbUsername, dbPassword);
                if (!conn.isClosed()) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "成功连接数据库！IP:" + dbIP + "端口:" + port);
                }
                conns.add(conn);
            }
        }
        //可以配置更多数据库连接
        return conns;
    }
    public static List<DBConnection> getDatabaseConnection(boolean computing) throws Exception{
        List<DBConnection> dbConnConfig = Configurations.getDBConnections();
        for(DBConnection db : dbConnConfig) {
            Connection conn = null;
            String dbBrand = db.getDatabaseBrand();
            String dbIP = db.getDatabaseIP();
            String port = db.getDatabasePort();
            String testDBName = Configurations.getTestDatabaseName();
            String dbUsername = db.getDbUsername();
            String dbPassword = db.getDbPassword();

            if (dbBrand.equalsIgnoreCase("TiDB") || dbBrand.equalsIgnoreCase("mysql")
                    || dbBrand.equalsIgnoreCase("oceanbase")|| dbBrand.equalsIgnoreCase("matrixone")) {
                if (dbBrand.equalsIgnoreCase("TiDB")) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "本次测试数据库为TiDB");
                }
                if (dbBrand.equalsIgnoreCase("mysql")) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "本次测试数据库为MySQL");
                }
                if (dbBrand.equalsIgnoreCase("oceanbase")) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "本次测试数据库为OceanBase");
                }
                if (dbBrand.equalsIgnoreCase("matrixone")) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "本次测试数据库为MatrixOne");
                }
                String driver = "com.mysql.cj.jdbc.Driver";
                String dbURL = "jdbc:mysql://" + dbIP + ":" + port + "/" +testDBName+ "?useSSL=false";
                if(dbBrand.equalsIgnoreCase("mysql"))
                    dbURL += "&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
                Class.forName(driver);
                conn = DriverManager.getConnection(dbURL, dbUsername, dbPassword);
                if (!conn.isClosed()) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "成功连接数据库！IP:" + dbIP + "端口:" + port);
                }
            } else if (dbBrand.equalsIgnoreCase("sqlite")) {
                RecordLog.recordLog(LogLevelConstant.INFO, "本次测试数据库为sqlite");
                String driver = "org.sqlite.JDBC";
                String dbURL = "jdbc:sqlite:Achilles.db";
                Class.forName(driver);
                conn = DriverManager.getConnection(dbURL);
                if (!conn.isClosed()) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "成功连接数据库sqlite");
                    if (!computing) {//如果不是计算的话
                        RecordLog.recordLog(LogLevelConstant.INFO, "开始删除表");
                        try {
                            Statement stmt = conn.createStatement();
                            List<String> preDelTables = new ArrayList<String>();
                            ResultSet rs = conn.getMetaData().getTables(null, null, null, null);
                            while (rs.next()) {
                                preDelTables.add(rs.getString("TABLE_NAME"));
                            }
                            for (String pre : preDelTables) {
                                RecordLog.recordLog(LogLevelConstant.INFO, "开始删除表" + pre);
                                stmt.executeUpdate("drop table " + pre);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                }
            } else if (dbBrand.equalsIgnoreCase("postgresql") || dbBrand.equalsIgnoreCase("gaussdb")){
                String dbURL = "";
                if (dbBrand.equalsIgnoreCase("postgresql")) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "本次测试数据库为postgresql"); 
                    dbURL = "jdbc:postgresql://" + dbIP + ":" + port + "/" +testDBName.toLowerCase()+ "?useSSL=false";
                }
                if (dbBrand.equalsIgnoreCase("gaussdb")) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "本次测试数据库为gaussdb");
                    dbURL = "jdbc:postgresql://" + dbIP + ":" + port + "/" + testDBName.toLowerCase() + "?useSSL=false";
                }
                
                String driver = "org.postgresql.Driver";
                Class.forName(driver);
                conn = DriverManager.getConnection(dbURL, dbUsername, dbPassword);
                if (!conn.isClosed()) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "成功连接数据库！IP:" + dbIP + "端口:" + port);
                }
            }
            db.setDbConn(conn);
        }
        //可以配置更多数据库连接
        return dbConnConfig;
    }

    public static Connection changeDatabaseConnectionByDatabaseName(DBConnection dbConnection) throws Exception{
        String driver = "";
        String dbURL = "";
        String dbName = "";
        if(dbConnection.getDatabaseBrand().equalsIgnoreCase("tidb")||dbConnection.getDatabaseBrand().equalsIgnoreCase("oceanbase")
        ||dbConnection.getDatabaseBrand().equalsIgnoreCase("mysql")){
            driver = "com.mysql.cj.jdbc.Driver";
            dbURL = "jdbc:mysql://" ;
            dbName = Configurations.getTestDatabaseName();
        }else if(dbConnection.getDatabaseBrand().equalsIgnoreCase("postgresql") || dbConnection.getDatabaseBrand().equalsIgnoreCase("gaussdb")) {
            driver = "org.postgresql.Driver";
            dbURL = "jdbc:postgresql://";
            dbName = Configurations.getTestDatabaseName().toLowerCase();
        }
        dbURL += dbConnection.getDatabaseIP() + ":" + dbConnection.getDatabasePort() +
                "/" + dbName + "?useSSL=false";
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(dbURL, dbConnection.getDbUsername(), dbConnection.getDbPassword());
        return conn;
    }
}
