package ecnu.dbhammer.verification;

import com.opencsv.CSVWriter;
import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.databaseAdapter.DBConnection;
import ecnu.dbhammer.databaseAdapter.DatabaseConnection;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.query.Aggregation;
import ecnu.dbhammer.query.QueryTree;
import ecnu.dbhammer.utils.ClearDirectory;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName ExecutingAlone.java
 * @Description 单独的执行查询
 * @createTime 2022年03月10日 15:21:00
 */
public class ExecutingAlone {
    public static List<Pair<Integer,String>> execute() {
        RecordLog.recordLog(LogLevelConstant.INFO,"------开始单独执行查询------");

        String readQueryPath = Configurations.getQueryOutputDir() + File.separator + "query" + ".txt";
        String queryExecuteResultPath = Configurations.getExecuteResultOutputDir();

        List<Pair<Integer,String>> listOfNULL = new ArrayList<>();


        // 从文件逐行读取查询语句，依次执行
        String query;
        try {
            List<DBConnection> conns = DatabaseConnection.getDatabaseConnection(true);

            for(DBConnection dbConnection : conns) {
                Connection con = dbConnection.getDbConn();
                List<Long> executeTimePerConn = new ArrayList<>();
                Statement statement = con.createStatement();
                //执行查询
                int n = 0;
                try {
                    ClearDirectory.deleteDir(queryExecuteResultPath);

                    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(readQueryPath)), "UTF-8"));
                    String lineTxt;
                    while ((lineTxt = br.readLine()) != null) {
                        n++;
                        query = lineTxt;

                        System.out.println(query);
                        long startTime = System.currentTimeMillis();
                        ResultSet resultSet = statement.executeQuery(query);//读取每一行的查询 然后执行
                        long endTime = System.currentTimeMillis();

                        executeTimePerConn.add(endTime - startTime);



                        //统计null值使用aggregation来统计 方便
                        while(resultSet.next()) {
                            String execStrResult = resultSet.getString("result");

                            BigDecimal execBdcResult=null;
                            if (execStrResult == null) {
                                execBdcResult = null;
                                listOfNULL.add(Pair.of(n, query));
                            } else if (execStrResult.equals("0")) {
                                execBdcResult = new BigDecimal(0);
                                listOfNULL.add(Pair.of(n, query));
                            } else {
                                execBdcResult = new BigDecimal(execStrResult);
                            }

                            System.out.println(execBdcResult);


                        }

                    }
                    System.out.println("Executed Queries:"+n);


                } catch (IOException e) {
                    e.printStackTrace();
                }
                con.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return listOfNULL;
    }

    public static void main(String[] args) {
        List<Pair<Integer,String>> nullList = ExecutingAlone.execute();
        System.out.println("无效负载的个数"+nullList.size());
        for(Pair<Integer, String> single : nullList){
            System.out.println("Query ID:\n"+single.getLeft());
            System.out.println("Query:\n"+single.getRight());
        }
    }
}
