package ecnu.dbhammer.main;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.databaseAdapter.DBConnection;
import ecnu.dbhammer.databaseAdapter.DatabaseConnection;
import ecnu.dbhammer.planParser.QueryPlanParser;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName TestCardAfterAnalyze.java
 * @Description Analyze Table后进行基数预估准确度的测试
 * @createTime 2022年04月23日 15:20:00
 */
public class TestCardAfterAnalyze {
    public static void testCardAnalyzeTable() throws Exception {

        String targetQueryEstimatedCard = Configurations.getGenerateExactCard();
        List<DBConnection> conns = DatabaseConnection.getDatabaseConnection(true);
        for(DBConnection dbConnection : conns){
            Connection con = dbConnection.getDbConn();
            Statement statement = con.createStatement();


            System.out.println("开始ExplainQuery");
            File dirFile = new File("./query/targetQuery");
            int n = dirFile.listFiles().length;
            System.out.println("一共"+n+"个Query");

            for (int i = 0; i  < n; i++) {

                for(int j=0;j<5;j++) {
                    Thread.sleep(500);
                    String readQueryPath = "./query" + File.separator + "targetQuery" + File.separator + "targetQuery_" + i + ".sql";
                    String query = FileUtils.readFileToString(new File(readQueryPath), "utf-8");
                    System.out.println(query);
                    QueryPlanParser queryExplain = new QueryPlanParser(dbConnection, statement, query);

                    System.out.println(readQueryPath + " Analyze成功");

                    
                    System.out.println((j==4 ? "多次执行":"")+"顶点的基数为" + queryExplain.getEstimatedCard());
                    String name =null;
                    if(j==0 || j==4) {
                        if (j != 4) {
                            name = targetQueryEstimatedCard + File.separator + "estimatedCard_" + dbConnection.getDatabaseBrand() + "_afterAnalyzeTable.txt";
                        } else {
                            name = targetQueryEstimatedCard + File.separator + "estimatedCard_" + dbConnection.getDatabaseBrand() + "_multipleExecute.txt";
                        }
                        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(name, true), Configurations.getEncodeType()))) {
                            bw.write(queryExplain.getEstimatedCard() + "\r\n");
                            bw.flush();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }

            }
            statement.close();
            con.close();
        }

    }

    public static void main(String[] args) throws Exception {
        testCardAnalyzeTable();
    }
}
