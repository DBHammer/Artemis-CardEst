package ecnu.dbhammer.test;

import ecnu.dbhammer.databaseAdapter.DatabaseConnection;
import ecnu.dbhammer.utils.JDBCSSHChannel;
import java.sql.*;
import java.util.List;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import ecnu.dbhammer.utils.SFTPConnection;

public class testssh {
    public static void main(String[] args) throws SQLException {
        try {
//            SFTPConnection sftpConnection = new SFTPConnection();
//            sftpConnection.connect("lyqu", "lyqu", "10.11.6.117", 22);
//            sftpConnection.upload("/home/lyqu/tingc","/home/dase/test/test");
//            sftpConnection.close();
            List<Connection> conns = DatabaseConnection.getDatabaseConnectionForFirst();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

}
