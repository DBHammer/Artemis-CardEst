package ecnu.dbhammer.main;
import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.databaseAdapter.CreateDatabase;
import ecnu.dbhammer.databaseAdapter.DBConnection;
import ecnu.dbhammer.log.RecordLog;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 连接多个数据库，进行AnalyzeTable
 */
public class AnalyzeTableAlone {
    public static void Analyze(DBConnection dbConnection) throws SQLException {

        Connection con = dbConnection.getDbConn();
        try {
            
            System.out.println("connect Database...");
            try {

                Statement statement = con.createStatement();
                
                System.out.println("开始AnalyzeTable");
                String tableName = null;
                String txtName = null;
                String loadStatement = null;

                File dirFile = new File("./data");
                int n = dirFile.listFiles().length;

                


                int numofDataFile = 0;
                for (int i = 0; i  < n; i++) {
                    System.out.println(dirFile.listFiles()[i].getName());
                    if(!dirFile.listFiles()[i].getName().contains(".txt")){
                        continue;
                    }
                    numofDataFile++;
                }
                System.out.println("一共"+numofDataFile+"张表");

                if(dbConnection.getDatabaseBrand().equals("tidb")){

                    // 多少张表
                    for (int i = 0; i  < numofDataFile; i++) {
                        System.out.println(dirFile.listFiles()[i].getName());
                        if(!dirFile.listFiles()[i].getName().contains(".txt")){
                            continue;
                        }
                        Thread.sleep(1000);
                        tableName = "table_" + i;
                        // 多少个线程

                        txtName = tableName + "_0" + ".txt";
                        loadStatement = "Analyze Table "+tableName;
                        System.out.println(loadStatement);
                        statement.execute(loadStatement);
                        System.out.println(tableName + "Analyze成功");
                    }
                }else if(dbConnection.getDatabaseBrand().equals("oceanbase")){
                    // loadStatement = "alter system major freeze";
                    // statement.execute(loadStatement);
                    // Thread.sleep(3000);
                    // System.out.println("开始判断合并是否成功！");
                    // List<Integer> values = new ArrayList<>(); 
                    // List<Integer> uniqueList = null;
                    // do{
                    //     String judgement = "select name,value from oceanbase.__all_zone where name='frozen_version' or name='last_merged_version'";
                    //     ResultSet rs = statement.executeQuery(judgement);
                    //     values.clear();
                    //     System.out.println("合并中......");
                    //     while(rs.next()){
                    //         values.add(rs.getInt("value"));
                    //         System.out.println(rs.getInt("value"));
                    //     }
                    //     uniqueList = values.stream().distinct().collect(Collectors.toList());
                    //     if(uniqueList.size()==1){
                    //         System.out.println("合并成功！");
                    //     }
                    //     Thread.sleep(500);
                    // }while(uniqueList.size()!=1);

                    for (int i = 0; i  < numofDataFile; i++) {
                        System.out.println(dirFile.listFiles()[i].getName());
                        Thread.sleep(1000);
                        if(!dirFile.listFiles()[i].getName().contains(".txt")){
                            continue;
                        }
                        tableName = "table_" + i;
                        // 多少个线程

                        txtName = tableName + "_0" + ".txt";
                        String majorStatement = "alter system major freeze";
                        loadStatement = "ANALYZE TABLE "+tableName + " COMPUTE STATISTICS FOR ALL COLUMNS SIZE AUTO";

                        System.out.println(loadStatement);
                        statement.execute(majorStatement);
                        statement.execute(loadStatement);
                        System.out.println(tableName + "Analyze成功");
                    }

                } else if (dbConnection.getDatabaseBrand().equals("postgresql")) {
                    for (int i = 0; i  < numofDataFile; i++) {
                        System.out.println(dirFile.listFiles()[i].getName());
                        Thread.sleep(1000);
                        if(!dirFile.listFiles()[i].getName().contains(".txt")){
                            continue;
                        }
                        tableName = "table_" + i;
                        // 多少个线程

                        txtName = tableName + "_0" + ".txt";
                        loadStatement = "ANALYZE "+tableName;

                        System.out.println(loadStatement);
                        statement.execute(loadStatement);
                        System.out.println(tableName + "Analyze成功");
                    }
                }
                statement.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }finally{
            con.close();
        }
    }
    public static void main(String[] args) throws Exception {
        TestCardinalityEstimation.analyzeTable();
    }   
}
