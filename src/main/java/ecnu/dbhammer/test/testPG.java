package ecnu.dbhammer.test;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.databaseAdapter.DBConnection;
import ecnu.dbhammer.databaseAdapter.DatabaseConnection;
import ecnu.dbhammer.queryExplain.QueryExplain;
import ecnu.dbhammer.schema.Table;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class testPG {
    public static void main(String[] args){
        //Configurations.loadConfigurations();
        try {
            List<DBConnection> conns = DatabaseConnection.getDatabaseConnection(true);
            for (DBConnection dbConnection : conns) {
                Connection con = dbConnection.getDbConn();
                List<List<Long>> executeTimesPerQuery = new ArrayList<>();
                Statement statement = con.createStatement();
                statement.execute("set join_collapse_limit = 1");
                List<Table> tables = new ArrayList<>();
                Table t0 = new Table("table_1");
                Table t1 = new Table("table_2");
                Table t2 = new Table("table_3");
                Table t3 = new Table("table_4");
                tables.add(t0);
                tables.add(t1);
                tables.add(t2);
                String query = "select   count(*) as result from (table_3 cross join ((table_4 cross join table_0) cross join table_1)) where  table_0.col_19 < 245182563.11246586956343640 and table_4.col_15 < -24038001.0353708461185920 and table_1.col_14 > 5900410.0599084356253056 and table_3.col_28 <= -103644041.8977529763040986 and table_4.fk_0 = table_0.primaryKey and table_4.fk_3 = table_1.primaryKey and table_1.fk_1 = table_3.primaryKey   ";
                statement.execute("set join_collapse_limit = 1");
                long startTime = System.currentTimeMillis();
                statement.executeQuery(query);
                long endTime = System.currentTimeMillis();
                System.out.println(endTime-startTime);
                statement.close();
                Statement statement0 = con.createStatement();
                startTime = System.currentTimeMillis();
                statement0.executeQuery(query);
                endTime = System.currentTimeMillis();
                System.out.println(endTime-startTime);
                query = "select   count(*) as result from ((table_3 cross join table_4) cross join (table_1 cross join table_0)) where  table_0.col_19 < 245182563.11246586956343640 and table_4.col_15 < -24038001.0353708461185920 and table_1.col_14 > 5900410.0599084356253056 and table_3.col_28 <= -103644041.8977529763040986 and table_4.fk_0 = table_0.primaryKey and table_4.fk_3 = table_1.primaryKey and table_1.fk_1 = table_3.primaryKey   ";
                Statement statement00 = con.createStatement();
                startTime = System.currentTimeMillis();
                statement00.executeQuery(query);
                endTime = System.currentTimeMillis();
                System.out.println(endTime-startTime);
                Statement statement000 = con.createStatement();
                startTime = System.currentTimeMillis();
                statement000.executeQuery(query);
                endTime = System.currentTimeMillis();
                System.out.println(endTime-startTime);
                con.close();

                System.out.println();
                con = DatabaseConnection.changeDatabaseConnectionByDatabaseName(dbConnection);
                Statement statement1 = con.createStatement();
                statement1.execute("set join_collapse_limit = 1");
                startTime = System.currentTimeMillis();
                statement1.executeQuery(query);
                endTime = System.currentTimeMillis();
                System.out.println(endTime-startTime);
                statement1.close();
                con.close();

                con = DatabaseConnection.changeDatabaseConnectionByDatabaseName(dbConnection);
                Statement statement2 = con.createStatement();
                statement2.execute("set join_collapse_limit = 1");
                startTime = System.currentTimeMillis();
                statement2.executeQuery(query);
                endTime = System.currentTimeMillis();
                System.out.println(endTime-startTime);
                statement2.close();
                con.close();

                con = DatabaseConnection.changeDatabaseConnectionByDatabaseName(dbConnection);
                Statement statement3 = con.createStatement();
                statement3.execute("set join_collapse_limit = 1");
                startTime = System.currentTimeMillis();
                statement3.executeQuery(query);
                endTime = System.currentTimeMillis();
                System.out.println(endTime-startTime);
                statement3.close();
                con.close();

                System.out.println();
                query = "select   count(*) as result from ((table_3 cross join table_4) cross join (table_1 cross join table_0)) where  table_0.col_19 < 245182563.11246586956343640 and table_4.col_15 < -24038001.0353708461185920 and table_1.col_14 > 5900410.0599084356253056 and table_3.col_28 <= -103644041.8977529763040986 and table_4.fk_0 = table_0.primaryKey and table_4.fk_3 = table_1.primaryKey and table_1.fk_1 = table_3.primaryKey   ";
                con = DatabaseConnection.changeDatabaseConnectionByDatabaseName(dbConnection);
                Statement statement4 = con.createStatement();
                statement4.execute("set join_collapse_limit = 1");
                startTime = System.currentTimeMillis();
                statement4.executeQuery(query);
                endTime = System.currentTimeMillis();
                System.out.println(endTime-startTime);
                statement4.close();
                con.close();

                con = DatabaseConnection.changeDatabaseConnectionByDatabaseName(dbConnection);
                Statement statement5 = con.createStatement();
                statement5.execute("set join_collapse_limit = 1");
                startTime = System.currentTimeMillis();
                statement5.executeQuery(query);
                endTime = System.currentTimeMillis();
                System.out.println(endTime-startTime);
                statement5.close();
                con.close();

                System.out.println();
                query = "select   count(*) as result from ((table_4 cross join (table_3 cross join table_0)) cross join table_1) where  table_0.col_19 < 245182563.11246586956343640 and table_4.col_15 < -24038001.0353708461185920 and table_1.col_14 > 5900410.0599084356253056 and table_3.col_28 <= -103644041.8977529763040986 and table_4.fk_0 = table_0.primaryKey and table_4.fk_3 = table_1.primaryKey and table_1.fk_1 = table_3.primaryKey   ";
                con = DatabaseConnection.changeDatabaseConnectionByDatabaseName(dbConnection);
                Statement statement6 = con.createStatement();
                statement6.execute("set join_collapse_limit = 1");
                startTime = System.currentTimeMillis();
                statement6.executeQuery(query);
                endTime = System.currentTimeMillis();
                System.out.println(endTime-startTime);
                statement6.close();
                con.close();

                con = DatabaseConnection.changeDatabaseConnectionByDatabaseName(dbConnection);
                Statement statement7 = con.createStatement();
                statement7.execute("set join_collapse_limit = 1");
                startTime = System.currentTimeMillis();
                statement7.executeQuery(query);
                endTime = System.currentTimeMillis();
                System.out.println(endTime-startTime);
                statement7.close();
                con.close();
                //                QueryExplain queryExplain = new QueryExplain(dbConnection, statement, query, tables);
//                String originalJoinOrderStr = queryExplain.getJoinedTableOrderStr();
//                System.out.println(originalJoinOrderStr);
//                query = "select   count(*) as result from ((table_4 cross join table_3) cross join (table_2 cross join table_1))where  table_3.col_22 > 1102933824.6778400958064064 and table_1.col_9 >= -1181469110.45485790441600 and table_4.col_24 >= -1099893864.176296764761397824 and table_2.col_17 > 432274141.64361833431121048 and table_3.fk_2 = table_1.primaryKey and table_4.fk_0 = table_3.primaryKey and table_4.fk_3 = table_2.primaryKey   ";
//                QueryExplain queryExplain2 = new QueryExplain(dbConnection, statement, query, tables);
//                String originalJoinOrderStr2 = queryExplain2.getJoinedTableOrderStr();
//                System.out.println(originalJoinOrderStr2);

            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
