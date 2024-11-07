package ecnu.dbhammer.databaseAdapter;

import java.sql.Connection;

/**
 * @author tingc
 * @ClassName DBConnection.java
 * @Description 用于记录数据库连接时的信息
 * @createTime 2022年1月18日 14:10:00
 */

public class DBConnection {

    private String databaseBrand;
    private String databaseIP;
    private String databasePort;
    private String dbUsername;
    private String dbPassword;
    private Connection dbConn;
    private String serverHost;
    private String serverUser;
    private String serverPort;
    private String serverPassword;
    private String uploadDir;

    public DBConnection(String databaseBrand, String databaseIP, String databasePort, String dbUsername, String dbPassword,
                        String serverHost, String serverPort, String serverUser, String serverPassword, String uploadDir){
        super();
        this.databaseBrand = databaseBrand;
        this.databaseIP = databaseIP;
        this.databasePort = databasePort;
        this.dbUsername = dbUsername;
        this.dbPassword = dbPassword;
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.serverUser = serverUser;
        this.serverPassword = serverPassword;
        this.uploadDir = uploadDir;
    }

    public String getDatabaseBrand() { return this.databaseBrand; }

    public String getDatabaseIP() { return this.databaseIP; }

    public String getDatabasePort() { return this.databasePort; }

    public String getDbUsername() { return this.dbUsername; }

    public String getDbPassword() { return this.dbPassword; }

    public void setDbConn(Connection conn){
        this.dbConn = conn;
    }

    public Connection getDbConn() {
        return dbConn;
    }

    public String getServerHost() {
        return serverHost;
    }

    public String getServerPassword() {
        return serverPassword;
    }

    public String getServerPort() {
        return serverPort;
    }

    public String getServerUser() {
        return serverUser;
    }

    public String getUploadDir() {
        return uploadDir;
    }
}
