package ecnu.dbhammer.test;

import java.sql.Connection;
import java.util.List;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.databaseAdapter.CreateDatabase;
import ecnu.dbhammer.databaseAdapter.DBConnection;
import ecnu.dbhammer.databaseAdapter.DatabaseConnection;

public class cardEstimate {

    public static void main(String[] args) throws Exception {
        // List<Connection> conns = DatabaseConnection.getDatabaseConnectionForFirst();
        
        // for (Connection conn : conns) {
        //     CreateDatabase.dropAndCreateDatabase(conn);
        // }
        
        List<DBConnection> dbConnConfig = Configurations.getDBConnections();
        
        for (DBConnection connection : dbConnConfig) {
            CreateDatabase.reloadData(connection);
        }

        
    }

}
