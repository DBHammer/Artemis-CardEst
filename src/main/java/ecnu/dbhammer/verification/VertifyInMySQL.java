package ecnu.dbhammer.verification;

import ecnu.dbhammer.configuration.Configurations;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 主要用于待测试数据库执行结果和理想结果不一致时，使用Mysql数据库做一个对比，用于Bug的调试
 */
public class VertifyInMySQL {
    public static void loadData(){
        Connection con = null;
        String url = "jdbc:mysql://127.0.0.1:4000/artemis";
        url = url + "?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true";
        String userName = "root";
        String password = "";
        // 从文件逐行读取建表和建索引语句，依次执行
        String createSql = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("connect MySQL...");
            try {
                con = DriverManager.getConnection(url, userName, password);
                if (!con.isClosed())
                    System.out.println("Succeeded connecting to the MySQL!");
                Statement statement = con.createStatement();
                try {
                    String createTableSqlDir = Configurations.getSchemaOutputDir() + File.separator + "createSchemaSQL" + "_" + "0" + ".txt";
                    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(createTableSqlDir)),"UTF-8"));
                    String lineTxt = null;
                    while ((lineTxt = br.readLine())!= null) {
                        createSql = lineTxt;
                        statement.execute(createSql);
                        System.out.println(createSql + "建Schema语句成功！！！！");
                    }
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("----------------------------------------");
                System.out.println("----------------------------------------");
                System.out.println("----------------------------------------");
                System.out.println("Schema中所有表创建成功！！！！！");

                System.out.println("----------------------------------------");
                System.out.println("----------------------------------------");
                System.out.println("----------------------------------------");
                System.out.println("开始导入数据！！！！！！");
                String tableName = null;
                String txtName = null;
                String loadStatement = null;

                // 多少张表
                for (int i = 0; i  <=11; i++) {
                    tableName = "table_0_" + i;
                    // 多少个线程
                    for(int j = 0; j < Configurations.getThreadNumPerNode(); j++) {
                        txtName = tableName + "_" + j +".txt";
                        loadStatement = "load data local infile './data/dbInstance/" + txtName +"' into table " + tableName + " fields terminated by ',' lines terminated by '\r\n'";
                        System.out.println(loadStatement);
                        statement.execute(loadStatement);
                        System.out.println(txtName + "数据导入成功！！！");
                    }
                    System.out.println(tableName + "表数据导入成功！！！！！");
                }
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        VertifyInMySQL.loadData();
    }
}
