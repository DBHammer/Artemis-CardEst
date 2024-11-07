package ecnu.dbhammer.verification;

//import com.mysql.jdbc.exceptions.MySQLTimeoutException;
import com.opencsv.CSVWriter;
import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.databaseAdapter.DBConnection;
import ecnu.dbhammer.databaseAdapter.DatabaseConnection;
import ecnu.dbhammer.graphviz.DrawQueryTree;
import ecnu.dbhammer.joinorder.JoinOrderEvaluation;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.main.StartCaluate;
import ecnu.dbhammer.query.Aggregation;
import ecnu.dbhammer.query.QueryTree;
import ecnu.dbhammer.queryExplain.QueryExplain;
import ecnu.dbhammer.schema.Table;
import ecnu.dbhammer.utils.ClearDirectory;

import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

/**
 * //通过QueryResultExecution来执行查询,写入文件
 * //再执行结果生成，写入文件
 */
public class QueryResultExecution {

    private static List<List<Long>> avgExecuteTimeList;

    public static List<Pair<Integer, String>> execute(boolean executeTimeOnly, List<QueryTree> queryTrees,
            String type) {
        RecordLog.recordLog(LogLevelConstant.INFO, "------开始执行查询------");

        // String readQueryPath = Configurations.getQueryOutputDir() + File.separator +
        // "query" + ".txt";
        String queryExecuteResultPath = Configurations.getExecuteResultOutputDir();

        List<Pair<Integer, String>> listOfNULL = new ArrayList<>();

        if (executeTimeOnly) {
            RecordLog.recordLog(LogLevelConstant.INFO, "只需统计查询时间，无需记录查询结果");
            avgExecuteTimeList = new ArrayList<>();

            // 从文件逐行读取查询语句，依次执行
            String query;
            try {
                List<DBConnection> conns = DatabaseConnection.getDatabaseConnection(true);

                for (DBConnection dbConnection : conns) {
                    Connection con = dbConnection.getDbConn();
                    List<List<Long>> executeTimesPerQuery = new ArrayList<>();
                    Statement statement = con.createStatement();
                    List<String> querys = new ArrayList<>();
                    // 执行查询

                    ClearDirectory.deleteDir(queryExecuteResultPath);

                    for (QueryTree queryTree : queryTrees) {
                        // 过滤空行

                        // 针对ob不同版本的merge join、hash join性能对比实验
                        // query = hintMergeHash(lineTxt);
                        query = queryTree.getSqlText();
                        // System.out.println(query);

                        querys.add(query);
                        // 每个query执行7次，去掉最大最小值，取平均
                        List<Long> executeTimes = new ArrayList<>();
                        int cnt = 7;
                        while (cnt > 0) {
                            long startTime = System.currentTimeMillis();
                            ResultSet resultSet = statement.executeQuery(query);// 读取每一行的查询 然后执行
                            long endTime = System.currentTimeMillis();
                            executeTimes.add(endTime - startTime);
                            cnt--;
                            // System.out.println(endTime - startTime + " ms");
                        }
                        executeTimesPerQuery.add(executeTimes);
                    }
                    statement.close();
                    con.close();
                    List<Long> avgExecuteTimePerQuery = calculateAvgTime(executeTimesPerQuery);
                    avgExecuteTimeList.add(avgExecuteTimePerQuery);

                    // 将执行时间写入文件

                    List<String[]> outTimeList = new ArrayList<>();

                    // 先写标题:query,time1,...,avg time
                    String[] csvHeaders = new String[9];
                    csvHeaders[0] = "query";
                    for (int i = 1; i < 8; i++) {
                        csvHeaders[i] = "time" + Integer.toString(i);
                    }
                    csvHeaders[8] = "avg time";
                    outTimeList.add(csvHeaders);

                    // 再写时间
                    for (int i = 0; i < querys.size(); i++) {
                        String[] line = new String[9];
                        line[0] = querys.get(i);
                        for (int col = 1; col < 8; col++) {
                            line[col] = executeTimesPerQuery.get(i).get(col - 1).toString();
                        }
                        line[8] = avgExecuteTimePerQuery.get(i).toString();
                        outTimeList.add(line);
                    }
                    String queryExecuteTimeFile = Configurations.getExecuteTimeOutputDir() + File.separator + type + "_"
                            + dbConnection.getDatabaseBrand() + ".csv";
                    try (FileOutputStream fos = new FileOutputStream(queryExecuteTimeFile);
                            OutputStreamWriter osw = new OutputStreamWriter(fos,
                                    StandardCharsets.UTF_8);
                            CSVWriter writer = new CSVWriter(osw)) {
                        writer.writeAll(outTimeList);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            RecordLog.recordLog(LogLevelConstant.INFO, "记录查询结果，用于正确性验证，并统计负载有效性");
            String query;
            try {
                List<DBConnection> conns = DatabaseConnection.getDatabaseConnection(true);

                for (DBConnection dbConnection : conns) {
                    Connection con = dbConnection.getDbConn();
                    Statement statement = con.createStatement();
                    // 执行查询
                    int n = 0;

                    ClearDirectory.deleteDir(queryExecuteResultPath);
                    for (QueryTree queryTree : queryTrees) {
                        n++;
                        query = queryTree.getSqlText();
                        System.out.println(query);
                        ResultSet resultSet = statement.executeQuery(query);// 读取每一行的查询 然后执行
                        String queryExecuteResultFile = Configurations.getExecuteResultOutputDir() + File.separator
                                + "executeResult_" + n + ".csv";
                        try (FileOutputStream fos = new FileOutputStream(queryExecuteResultFile);
                                OutputStreamWriter osw = new OutputStreamWriter(fos,
                                        StandardCharsets.UTF_8);
                                CSVWriter writer = new CSVWriter(osw)) {
                            Boolean includeHeaders = true;
                            writer.writeAll(resultSet, includeHeaders);
                        }

                        // 统计空值，把Query统一修改成select count(*),方便
                        String countQuery = "select count(*) as result from " + query.split(" from ")[1];
                        ResultSet resultSet1 = statement.executeQuery(countQuery);
                        while (resultSet1.next()) {
                            String execStrResult = resultSet1.getString("result");
                            System.out.println("执行Count结果" + execStrResult);
                            if (execStrResult.equals("0")) {
                                listOfNULL.add(Pair.of(n, query));
                            }
                        }

                    }

                    con.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return listOfNULL;
    }

    public static List<Long> calculateAvgTime(List<List<Long>> executeTimePerConn) {
        List<Long> avgTimeList = new ArrayList<>();
        for (int i = 0; i < executeTimePerConn.size(); i++) {
            Collections.sort(executeTimePerConn.get(i));
            long sum = 0;
            for (int j = 1; j < executeTimePerConn.get(i).size() - 1; j++)
                sum += executeTimePerConn.get(i).get(j);
            avgTimeList.add(sum / (executeTimePerConn.get(i).size() - 2));
        }
        return avgTimeList;
    }

    public static String hintMergeHash(String query) {
        int index1 = query.indexOf("from");
        int index2 = query.indexOf("where");
        String tableNameStr = query.substring(index1 + 4, index2).trim();
        StringBuilder queryStr = new StringBuilder();
        queryStr.append(" select /*+USE_PLAN_CACHE(NONE) USE_MERGE(").append(tableNameStr).append(")*/ ")
                .append(query.substring(6));
        System.out.println(queryStr);
        return queryStr.toString();
    }

    public static void executeForJoinOrder(List<QueryTree> queryTrees) {
        RecordLog.recordLog(LogLevelConstant.INFO, "------开始执行查询评估JoinOrder------");

        // 从文件逐行读取查询语句，依次执行

        try {
            Thread.sleep(5000);
            List<DBConnection> conns = DatabaseConnection.getDatabaseConnection(true);
            for (DBConnection dbConnection : conns) {
                RecordLog.recordLog(LogLevelConstant.INFO, "本次评估数据库为" + dbConnection.getDatabaseBrand());
                Connection con = dbConnection.getDbConn();
                List<Long> executeTimePerConn = new ArrayList<>();
                Statement statement = con.createStatement();
                // 执行查询
                for (QueryTree queryTree : queryTrees) {

                    String originalQuery = queryTree.getSqlText();
                    System.out.println(originalQuery);
                    RecordLog.recordLog(LogLevelConstant.INFO, "执行原始查询");
                    long startTime = System.currentTimeMillis();
                    ResultSet resultSet = statement.executeQuery(originalQuery);
                    long endTime = System.currentTimeMillis();
                    long originalTime = endTime - startTime;
                    RecordLog.recordLog(LogLevelConstant.INFO, "时间：" + (endTime - startTime));
                    executeTimePerConn.add(endTime - startTime);

                    RecordLog.recordLog(LogLevelConstant.INFO, "再执行原始查询");
                    long startTime2 = System.currentTimeMillis();
                    ResultSet resultSet2 = statement.executeQuery(originalQuery);
                    long endTime2 = System.currentTimeMillis();
                    RecordLog.recordLog(LogLevelConstant.INFO, "时间" + (endTime2 - startTime2));
                    RecordLog.recordLog(LogLevelConstant.INFO, "执行JoinReorder查询");

                    String joinReorderQuery = queryTree.geneCardOptimialJoinOrderQuery(dbConnection.getDatabaseBrand());
                    System.out.println(joinReorderQuery);

                    long startTimeJoin = System.currentTimeMillis();
                    ResultSet resultSetOptimialJoin = statement.executeQuery(joinReorderQuery);
                    long endTimeJoin = System.currentTimeMillis();
                    long joinReorderTime = endTimeJoin - startTimeJoin;
                    RecordLog.recordLog(LogLevelConstant.INFO, "时间：" + joinReorderTime);

                    RecordLog.recordLog(LogLevelConstant.INFO, "再执行JoinReorder查询");
                    long startTimeJoinAgain = System.currentTimeMillis();
                    ResultSet resultSetOptimialJoinAgain = statement.executeQuery(joinReorderQuery);
                    long endTimeJoinAgain = System.currentTimeMillis();
                    long joinReorderTimeAgain = endTimeJoinAgain - startTimeJoinAgain;
                    RecordLog.recordLog(LogLevelConstant.INFO, "时间：" + joinReorderTimeAgain);

                    RecordLog.recordLog(LogLevelConstant.INFO,
                            "JoinReorder后比Reorder前快" + 1.0 * originalTime / joinReorderTime);

                }
                // 对于每一个连接，都会执行所有的查询，执行完后关掉
                con.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void recordOriginalJoinOrder(String originalJoinOrderStr) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("original_join_order.txt", true)); // true表示追加写入
            writer.write(originalJoinOrderStr);
            writer.newLine(); // 换行
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("记录 originalJoinOrderStr 到文件时出错！");
        }
    }

    // 将不同数据库的执行计划丢给其他数据库执行
    public static void shuffleJoin(List<QueryTree> queryTrees) {
        RecordLog.recordLog(LogLevelConstant.INFO, "------开始执行查询评估JoinOrderShuffle------");
        ClearDirectory.deleteDir(Configurations.getJoinOrderRankDir());

        try {
            List<DBConnection> conns = DatabaseConnection.getDatabaseConnection(true);
            Map<String, String[]> map = new HashMap<>();
            for (DBConnection dbConnection : conns) {
                Connection con = dbConnection.getDbConn();
                String[] s = new String[queryTrees.size()];
                int index = 0;
                for (QueryTree queryTree : queryTrees) {

                    Statement statement = con.createStatement();
                    String originalQuery = queryTree.getSqlText();
                    // hint不使用查询计划缓存,设置执行超时
                    if (dbConnection.getDatabaseBrand().equalsIgnoreCase("oceanbase")) {
                        originalQuery = "select /*+query_timeout(40000000) USE_PLAN_CACHE(NONE)*/ "
                                + originalQuery.substring(6);
                        // statement.execute("set ob_query_timeout=20000000;");
                    } else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("tidb"))
                        originalQuery = "select /*+ ignore_plan_cache() */ " + originalQuery.substring(6);
                    else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("postgresql")
                            || dbConnection.getDatabaseBrand().equalsIgnoreCase("gaussdb")) {
                        statement.execute("SET statement_timeout = 100000");
                    } else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("mysql")) {
                        statement.execute("set max_execution_time = 40000");
                    }
                    System.out.println(originalQuery);
                    RecordLog.recordLog(LogLevelConstant.INFO, "执行原始查询");

                    try {
                        statement.executeQuery(originalQuery);// 跑两遍，算第二次的执行时间；第一次执行cache住所有数据表的数据
                        long startTime = System.currentTimeMillis();
                        statement.executeQuery(originalQuery);
                        long endTime = System.currentTimeMillis();
                        RecordLog.recordLog(LogLevelConstant.INFO, "时间：" + (endTime - startTime));

                        // explain查询，得到数据库选择的连接顺序

                        QueryExplain queryExplain = new QueryExplain(dbConnection, statement, originalQuery,
                                queryTree.getTables());

                        String originalJoinOrderStr = queryExplain.getJoinedTableOrderStr();
                        s[index++] = originalJoinOrderStr;
                        recordOriginalJoinOrder(originalJoinOrderStr);

                        statement.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("原始查询出错！");
                    }
                }
                map.put(dbConnection.getDatabaseBrand().toLowerCase(), s);
            }

            for (DBConnection dbConnection : conns) {
                String dbMSBrand =  dbConnection.getDatabaseBrand();
                if (dbMSBrand.equalsIgnoreCase("tidb")) {
                    String[] ob = map.get("oceanbase");
                    String[] pg = map.get("postgresql");

                } else if (dbMSBrand.equalsIgnoreCase("oceanbase")) {
                    String[] tidb = map.get("tidb");
                    String[] pg = map.get("postgresql");
                } else if (dbMSBrand.equalsIgnoreCase("postgresql")) {
                    String[] ob = map.get("oceanbase");
                    String[] tidb = map.get("tidb");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // update by ct for join order rank
    public static void executeForJoinRank(List<QueryTree> queryTrees) {
        RecordLog.recordLog(LogLevelConstant.INFO, "------开始执行查询评估JoinOrderRank------");
        ClearDirectory.deleteDir(Configurations.getJoinOrderRankDir());

        try {
            List<DBConnection> conns = DatabaseConnection.getDatabaseConnection(true);
            // List<List<Integer>> rankListPerDB = new ArrayList<>();
            // List<List<Long>> timeDistanceListPerDB = new ArrayList<>();
            List<Double> MRRperDB = new ArrayList<>();
            List<Double> avgTimePerDB = new ArrayList<>();

            for (DBConnection dbConnection : conns) {
                Connection con = dbConnection.getDbConn();

                List<Integer> rankList = new ArrayList<>();
                List<Long> originalTimeList = new ArrayList<>();
                List<Long> timeDistanceList = new ArrayList<>();

                // 实验用
                List<Long> explainTimeList = new ArrayList<>();
                List<Long> enumTimeList = new ArrayList<>();

                // 执行查询
                int fileIndex = 1;

                // 记录随机生成的查询是否覆盖原始查询
                boolean isCover = false;
                for (QueryTree queryTree : queryTrees) {
                    List<Pair<String, Long>> pairs = new ArrayList<>();

                    Statement statement = con.createStatement();
                    String originalQuery = queryTree.getSqlText();
                    // hint不使用查询计划缓存,设置执行超时
                    if (dbConnection.getDatabaseBrand().equalsIgnoreCase("oceanbase")) {
                        originalQuery = "select /*+query_timeout(40000000) USE_PLAN_CACHE(NONE)*/ "
                                + originalQuery.substring(6);
                        // statement.execute("set ob_query_timeout=20000000;");
                    } else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("tidb"))
                        originalQuery = "select /*+ ignore_plan_cache() */ " + originalQuery.substring(6);
                    else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("postgresql")
                            || dbConnection.getDatabaseBrand().equalsIgnoreCase("gaussdb")) {
                        statement.execute("SET statement_timeout = 100000");
                    } else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("mysql")) {
                        statement.execute("set max_execution_time = 40000");
                    }
                    System.out.println(originalQuery);
                    RecordLog.recordLog(LogLevelConstant.INFO, "执行原始查询");

                    try {
                        statement.executeQuery(originalQuery);// 跑两遍，算第二次的执行时间；第一次执行cache住所有数据表的数据
                        long startTime = System.currentTimeMillis();
                        statement.executeQuery(originalQuery);
                        long endTime = System.currentTimeMillis();
                        long originalTime = endTime - startTime;
                        RecordLog.recordLog(LogLevelConstant.INFO, "时间：" + (endTime - startTime));

                        // explain查询，得到数据库选择的连接顺序
                        // long startExplainTime = System.currentTimeMillis();
                        // long startExplainTime = System.nanoTime();
                        QueryExplain queryExplain = new QueryExplain(dbConnection, statement, originalQuery,
                                queryTree.getTables());
                        // long endExplainTime = System.currentTimeMillis();
                        // long endExplainTime = System.nanoTime();
                        // long explainTime = endExplainTime-startExplainTime;
                        // explainTimeList.add(explainTime);
                        // System.out.println("explain: " + explainTime);

                        String originalJoinOrderStr = queryExplain.getJoinedTableOrderStr();

                        // System.out.println("######" + originalJoinOrderStr);

                        pairs.add(Pair.of("original", originalTime));
                        originalTimeList.add(originalTime);
                        statement.close();
                        // 枚举Join order
                        JoinOrderEvaluation joinOrderEvaluation = new JoinOrderEvaluation(queryTree);
                        // long startEnumTime = System.currentTimeMillis();
                        // long startEnumTime = System.nanoTime();
                        List<String> allJoinOrderQuery = joinOrderEvaluation
                                .QueryHintEnum(dbConnection.getDatabaseBrand(), originalJoinOrderStr, originalTime);
                        // long endEnumTime = System.currentTimeMillis();
                        // long endEnumTime = System.nanoTime();
                        // long enumTime = endEnumTime - startEnumTime;
                        // enumTimeList.add(enumTime);
                        // System.out.println("enum: " + enumTime);
                        // bushy tree的本来就是随机的，左深树的要在这里随机一下
                        List<String> randomJoinOrderQuery = new LinkedList<>();
                        if (allJoinOrderQuery.size() > 385) {
                            Collections.shuffle(allJoinOrderQuery);
                            randomJoinOrderQuery = allJoinOrderQuery.subList(0, 385);
                        } else
                            randomJoinOrderQuery = allJoinOrderQuery;

                        for (String joinOrder : randomJoinOrderQuery) {
                            if (joinOrder.contains(originalJoinOrderStr)) {
                                isCover = true;
                            }
                        }

                        for (int i = 0; i < randomJoinOrderQuery.size(); i++) {
                            // 中断执行时间过长的join顺序，加hint: query_timeout()
                            // System.out.println(randomJoinOrderQuery.get(i));
                            // 对于PG：执行计划缓存只在会话级别，因此重新连接数据库执行同一查询的不同连接顺序
                            // TODO 对于OB和TIDB：需要看一下是否也只在会话级别 主要问题是换了一个链接之后，是不是重新warm up
                            // if (dbConnection.getDatabaseBrand().equalsIgnoreCase("postgresql")) {
                            // con.close();
                            // con =
                            // DatabaseConnection.changeDatabaseConnectionByDatabaseName(dbConnection);
                            // }
                            Statement statement1 = con.createStatement();
                            try {
                                // statement1.setQueryTimeout(2);
                                // pg可设置会话级别的查询时间超时设置：SET statement_timeout = 10000;单位是毫秒
                                if (dbConnection.getDatabaseBrand().equalsIgnoreCase("postgresql")) {
                                    long upperBound = originalTime + 1000;
                                    statement1.execute("SET statement_timeout = " + upperBound);
                                    statement1.execute("set join_collapse_limit = 1");// 强制执行join顺序
                                } else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("mysql")) {
                                    long upperBound = originalTime + 1000;
                                    statement1.execute("set max_execution_time = " + upperBound);
                                } else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("gaussdb")) {
                                    long upperBound = originalTime + 1000;
                                    statement1.execute("SET statement_timeout = " + upperBound);
                                    statement1.execute("set join_collapse_limit = 1");// 强制执行join顺序
                                }
                                long startTime1 = System.currentTimeMillis();
                                statement1.executeQuery(randomJoinOrderQuery.get(i));
                                long endTime1 = System.currentTimeMillis();
                                long reorderTime1 = endTime1 - startTime1;
                                pairs.add(Pair.of(Integer.toString(i), reorderTime1));
                                System.out.println(i + " " + reorderTime1 + "ms");
                            } catch (Exception e) {
                                System.out.println(i + " Timeout");
                                pairs.add(Pair.of(Integer.toString(i), originalTime * 2));
                            }
                            statement1.close();
                        }

                        // 排序
                        // 使用匿名内部类
                        pairs.sort(new Comparator() {
                            @Override
                            public int compare(Object o1, Object o2) {
                                Pair<String, Long> s1 = (Pair) o1;
                                Pair<String, Long> s2 = (Pair) o2;
                                return s1.getRight().compareTo(s2.getRight());
                            }
                        });

                        // 各joinOrder与执行时间写入文件，方便后面分析
                        String[] csvHeader = { "Rank", "SQL", "Time(ms)" };
                        List<String[]> lineList = new ArrayList<>();
                        lineList.add(csvHeader);
                        int rank = 0;
                        long timeDistance = 0;
                        for (int i = 0; i < pairs.size(); i++) {
                            String[] line = new String[csvHeader.length];
                            line[0] = Integer.toString(i + 1);
                            if (pairs.get(i).getLeft().equalsIgnoreCase("original")) {
                                rank = i + 1;
                                timeDistance = pairs.get(i).getRight() - pairs.get(0).getRight();
                                // break;
                                line[1] = originalJoinOrderStr;
                            } else {
                                line[1] = randomJoinOrderQuery.get(Integer.parseInt(pairs.get(i).getLeft()));
                            }
                            line[2] = Long.toString(pairs.get(i).getRight());
                            lineList.add(line);
                        }
                        rankList.add(rank);
                        timeDistanceList.add(timeDistance);

                        // 写入文件
                        String joinOrderTimeFile = Configurations.getJoinOrderRankDir() + File.separator + "query_"
                                + fileIndex + ".csv";
                        try (FileOutputStream fos = new FileOutputStream(joinOrderTimeFile);
                                OutputStreamWriter osw = new OutputStreamWriter(fos,
                                        StandardCharsets.UTF_8);
                                CSVWriter writer = new CSVWriter(osw)) {
                            writer.writeAll(lineList);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("原始查询出错！");
                    }
                    fileIndex++;
                    isCover = false;
                }
                // 对于每一个连接，都会执行所有的查询，执行完后关掉
                // statement.close();
                con.close();
                // 输出结果
                System.out.println("total " + rankList.size());
                for (int i = 0; i < rankList.size(); i++) {
                    System.out.println(rankList.get(i) + " " + timeDistanceList.get(i) + "ms");
                }
                // 计算MRR值
                double mrr = JoinOrderEvaluation.calculateMRR(rankList);
                System.out.println("mrr: " + mrr);

                // rankListPerDB.add(rankList);
                // timeDistanceListPerDB.add(timeDistanceList);
                MRRperDB.add(mrr);

                // 写入文件
                List<String[]> outRankList = new ArrayList<>();

                // 先写标题
                String[] csvHeaders = { "Query", "Rank", "Time (ms)", "Best Time (ms)", "Time Gap (ms)", "isCover" };
                outRankList.add(csvHeaders);

                // 再写结果
                long sum = 0;
                for (int i = 0; i < rankList.size(); i++) {
                    String[] line = new String[csvHeaders.length];
                    line[0] = Integer.toString(i + 1);
                    line[1] = Integer.toString(rankList.get(i));
                    line[2] = Long.toString(originalTimeList.get(i));
                    line[3] = Long.toString(originalTimeList.get(i) - timeDistanceList.get(i));
                    line[4] = Long.toString(timeDistanceList.get(i));
                    line[5] = Boolean.toString(isCover);
                    outRankList.add(line);
                    sum += timeDistanceList.get(i);
                }
                avgTimePerDB.add(sum * 1.0 / rankList.size());

                String joinOrderRankFile = Configurations.getJoinOrderRankDir() + File.separator + "joinOrderRank_"
                        + dbConnection.getDatabaseBrand() + ".csv";
                try (FileOutputStream fos = new FileOutputStream(joinOrderRankFile);
                        OutputStreamWriter osw = new OutputStreamWriter(fos,
                                StandardCharsets.UTF_8);
                        CSVWriter writer = new CSVWriter(osw)) {
                    writer.writeAll(outRankList);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // 实验用
                // String expfile = Configurations.getJoinOrderRankDir() + File.separator +
                // "exp" + ".txt";
                // long timesum = 0;
                // for(Long i : explainTimeList){
                // timesum += i;
                // }
                // long avgExplainTime = timesum / explainTimeList.size();
                // timesum = 0;
                // for(Long i : enumTimeList){
                // timesum += i;
                // }
                // long avgEnumTime = timesum / enumTimeList.size();
                // try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new
                // FileOutputStream(expfile), "utf-8"))) {
                // String line = "avgExplainTime: " + avgExplainTime + "\n" + "avgEnumTime:
                // "+avgEnumTime;
                // bw.write(line);
                // bw.flush();
                // } catch (IOException e) {
                // e.printStackTrace();
                // }
            }
            // 写MRR和平均时延差
            String MRRfile = Configurations.getJoinOrderRankDir() + File.separator + "MRRresult" + ".csv";
            try (BufferedWriter bw = new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(MRRfile), "utf-8"))) {
                for (int i = 0; i < MRRperDB.size(); i++) {
                    String line = conns.get(i).getDatabaseBrand() + ": MRR = "
                            + MRRperDB.get(i) + "\t" + "Avg Latency Gap = " + avgTimePerDB.get(i) + " ms";
                    bw.write(line);
                }

                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<List<Long>> getExecuteTimeList() {
        return avgExecuteTimeList;
    }

    /*
     * 使用数据库执行Query，执行结果写入文件
     */
    // public static void main(String[] args) {
    // String readQueryPath = Configurations.getQueryOutputDir() + File.separator +
    // "query" + ".txt";
    // String queryCalculateResultPath =
    // Configurations.getCalculateResultOutputDir() + File.separator +
    // "calculateResult" + ".txt";
    // String queryExecuteResultPath = Configurations.getExecuteResultOutputDir() +
    // File.separator + "executeResult" + ".txt";
    //
    // QueryResultExecution.execute(Configurations.getExecuteTimeOnly());
    //
    // Set<Integer> ErrList = null;
    // try {
    // ErrList = Verify.verifyResult(queryCalculateResultPath,
    // queryExecuteResultPath);
    // } catch (Exception e) {
    // e.printStackTrace();
    // }
    // }
}
