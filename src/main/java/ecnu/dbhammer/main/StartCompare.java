package ecnu.dbhammer.main;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.log.TestReport;
import ecnu.dbhammer.query.QueryTree;
import ecnu.dbhammer.verification.QueryResultExecution;
import ecnu.dbhammer.verification.Verify;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.util.*;

/**
 * @author tingc
 * @ClassName StartCompare.java
 * @Description TODO
 * @createTime 2022年1月18日 11:49:00
 */
public class StartCompare {

    public static void StartCompare(List<QueryTree> queryTrees){
        String queryExecuteTimePath = Configurations.getExecuteTimeOutputDir() + File.separator + "compareResult" + ".txt";

        List<Pair<Integer,String>> queryListOfNull = QueryResultExecution.execute(true,queryTrees,"executeTime");

        List<List<Long>> executeTimeList = QueryResultExecution.getExecuteTimeList();


        if(executeTimeList.size()>=2) {
            //对比查询执行时间,目前只对比tidb和ob
            int cnt = 0;
            Map<Integer, Long> slowQuery = new TreeMap<>();
            for (int queryIndex = 0; queryIndex < executeTimeList.get(0).size(); queryIndex++) {
                if (executeTimeList.get(0).get(queryIndex) < executeTimeList.get(1).get(queryIndex)) {
                    cnt++;
                    slowQuery.put(queryIndex, executeTimeList.get(1).get(queryIndex) - executeTimeList.get(0).get(queryIndex));
                }
            }
            double percent = cnt * 1.0 / executeTimeList.get(0).size() * 100.0;

            //排序：得出OB top10的 慢查询
            List<Map.Entry<Integer, Long>> sortedSlowQuery = sortResults(slowQuery);
            //写入文件，记录结果
            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(queryExecuteTimePath), "utf-8"))) {
                if(slowQuery.size() == 0)
                    bw.write(Configurations.getDBConnections().get(1).getDatabaseBrand() + " 的查询执行时间都快于 "
                            + Configurations.getDBConnections().get(0).getDatabaseBrand() + "！\n");
                else {
                    bw.write(Configurations.getDBConnections().get(1).getDatabaseBrand() + " " + percent + "% 的Query执行慢于"
                            + Configurations.getDBConnections().get(0).getDatabaseBrand() + "\nTop10:\n");
                    for(int i=0; i < 10 ;i++){
                        if(i >= sortedSlowQuery.size())
                            break;
                        bw.write(sortedSlowQuery.get(i).getValue() + " ms\t query " + sortedSlowQuery.get(i).getKey() + "\n");
                    }
                }

                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static void StartCompare4OB(List<QueryTree> queryTrees){
        String queryExecuteTimePath = Configurations.getExecuteTimeOutputDir() + File.separator + "compareResult" + ".txt";

        List<Pair<Integer,String>> queryListOfNull = QueryResultExecution.execute(true, queryTrees,"executeTime");

        List<List<Long>> avgExecuteTimeList = QueryResultExecution.getExecuteTimeList();


        if(avgExecuteTimeList.size()>=2) {
            //对比查询执行时间,目前只对比tidb和ob
            int cnt = 0;
            Map<Integer, Long> slowQuery = new TreeMap<>();
            for (int queryIndex = 0; queryIndex < avgExecuteTimeList.get(0).size(); queryIndex++) {
                if (avgExecuteTimeList.get(0).get(queryIndex) < avgExecuteTimeList.get(1).get(queryIndex)) {
                    cnt++;
                    slowQuery.put(queryIndex, avgExecuteTimeList.get(1).get(queryIndex) - avgExecuteTimeList.get(0).get(queryIndex));
                }
            }
            double percent = cnt * 1.0 / avgExecuteTimeList.get(0).size() * 100.0;

            //排序：得出OB top10的 慢查询
            List<Map.Entry<Integer, Long>> sortedSlowQuery = sortResults(slowQuery);
            //写入文件，记录结果
            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(queryExecuteTimePath), "utf-8"))) {
                if(slowQuery.size() == 0)
                    bw.write("v3.1.2 的查询执行时间都快于 "
                            + "v3.1.1!\n");
                else {
                    bw.write("v3.1.2 " + percent + "% 的Query执行慢于"
                            + "v3.1.1\nTop10:\n");
                    for(int i=0; i < 10 ;i++){
                        if(i >= sortedSlowQuery.size())
                            break;
                        bw.write(sortedSlowQuery.get(i).getValue() + " ms\t query " + sortedSlowQuery.get(i).getKey() + "\n");
                    }
                }

                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static List<Map.Entry<Integer, Long>> sortResults(Map<Integer, Long> results){
        List<Map.Entry<Integer, Long>> list = new ArrayList<>(results.entrySet());
        //降序排序
        Collections.sort(list, (o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        return list;
    }
}
