package ecnu.dbhammer.main;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.databaseAdapter.CreateDatabase;
import ecnu.dbhammer.log.RecordLog;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author xiangzhaokun
 * @ClassName LoadDataAlone.java
 * @Description 单独的导入数据
 * @createTime 2022年03月03日 14:21:00
 */
public class LoadDataAlone {
    public static void loadData(){
        Connection con = null;
        String url = "jdbc:mysql://127.0.0.1:3306/";
        url = url + "?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true";
        String userName = "root";
        String password = "12345678";
        // 从文件逐行读取建表和建索引语句，依次执行
        String createSql = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("connect Database...");
            try {
                con = DriverManager.getConnection(url, userName, password);
                if (!con.isClosed())
                    System.out.println("Succeeded connecting to the Database!");
                CreateDatabase.dropAndCreateDatabase(con);
                Statement statement = con.createStatement();
                try {
                    statement.execute("use " + Configurations.getTestDatabaseName());
                    String createTableSqlDir = Configurations.getSchemaOutputDir() + File.separator + "createSchemaSQL"  + ".txt";
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

                RecordLog.recordLog(LogLevelConstant.INFO, "开始创建外键");
                try {
                    String addForeignKeySqlDir = Configurations.getSchemaOutputDir() + File.separator + "addForeignKey"
                            + ".txt";
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(new FileInputStream(new File(addForeignKeySqlDir)), "UTF-8"));
                    String addForeignKey;
                    while ((addForeignKey = br.readLine()) != null) {
                        System.out.println(addForeignKey);
                        //statement.execute(addForeignKey);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("----------------------------------------");
                System.out.println("----------------------------------------");
                System.out.println("----------------------------------------");
                System.out.println("开始导入数据！！！！！！");
                String tableName = null;
                String txtName = null;
                String loadStatement = null;

                File dirFile = new File("./data");
                int n = dirFile.listFiles().length;

                System.out.println(n);


                // 多少张表
                for (int i = 0; i  < n; i++) {
                    System.out.println(dirFile.listFiles()[i].getName());
                    if(!dirFile.listFiles()[i].getName().contains(".txt")){
                        continue;
                    }
                    tableName = "table_" + i;
                    // 多少个线程

                        txtName = tableName + "_0" + ".txt";
                        loadStatement = "load data local infile './data/" + txtName +"' into table " + tableName + " fields terminated by ',' lines terminated by '\r\n'";
                        System.out.println(loadStatement);
                        statement.execute(loadStatement);
                        System.out.println(txtName + "数据导入成功！！！");
                        statement.execute("analyze table "+tableName);

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
        loadData();

    }
}
