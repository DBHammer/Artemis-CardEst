package ecnu.dbhammer.utils;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import java.sql.*;

public class JDBCSSHChannel {
    /**
     *
     * @param localPort  本地host 建议mysql 3306 redis 6379
     * @param sshHost   ssh host
     * @param sshPort   ssh port
     * @param sshUserName   ssh 用户名
     * @param sshPassWord   ssh密码
     * @param remotoHost   远程机器地址
     * @param remotoPort	远程机器端口
     */
    public static void goSSH(int localPort, String sshHost, int sshPort,
                             String sshUserName, String sshPassWord,
                             String remotoHost, int remotoPort) throws SQLException{
        String driverName="com.mysql.jdbc.Driver";
        Connection conn = null;
        String dbuserName = "mysql";
        String dbpassword = "mysql123";
        String url = "jdbc:mysql://localhost:"+localPort+"/test";
        Session session= null;
        try {
            JSch jsch = new JSch();
            //登陆跳板机
            session = jsch.getSession(sshUserName, sshHost, sshPort);
            session.setPassword(sshPassWord);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
            System.out.println("连接成功");
            //通过ssh连接到mysql机器
            int assinged_port = session.setPortForwardingL(localPort, remotoHost, remotoPort);
            System.out.println("localhost:"+assinged_port+" -> "+remotoHost+":"+remotoPort);
            System.out.println("Port Forwarded");
            Class.forName(driverName).newInstance();
            conn = DriverManager.getConnection (url, dbuserName, dbpassword);
            System.out.println ("Database connection established");
            System.out.println("DONE");
        } catch (Exception e) {
            e.printStackTrace();
    }finally{
        if(conn != null && !conn.isClosed()){
            System.out.println("Closing Database Connection");
            conn.close();
        }
        if(session !=null && session.isConnected()){
            System.out.println("Closing SSH Connection");
            session.disconnect();
        }
    }
    }
}
