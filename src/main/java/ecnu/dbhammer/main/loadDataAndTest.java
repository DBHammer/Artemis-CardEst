package ecnu.dbhammer.main;

//import com.mysql.jdbc.exceptions.MySQLTimeoutException;
import com.opencsv.CSVWriter;
import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.databaseAdapter.CreateDatabase;
import ecnu.dbhammer.databaseAdapter.DBConnection;
import ecnu.dbhammer.databaseAdapter.DatabaseConnection;
import ecnu.dbhammer.joinorder.JoinOrderEvaluation;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.planParser.QueryPlanParser;
import ecnu.dbhammer.query.QueryTree;
import ecnu.dbhammer.query.type.QueryGraph;
import ecnu.dbhammer.queryExplain.QueryExplain;
import ecnu.dbhammer.schema.Table;
import ecnu.dbhammer.test.RawNode;
import ecnu.dbhammer.utils.SFTPConnection;
import ecnu.dbhammer.verification.QueryResultExecution;
import org.apache.commons.lang3.tuple.Pair;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author tingc
 * @ClassName loadDataAndTest.java
 * @Description 用以前生成的scheme,data及query去测试数据库
 * @createTime 2022年2月24日 14:31:00
 */
public class loadDataAndTest {
    private static List<String> midJoinSubQueries;

    private static final Pattern ROW_COUNTS = Pattern.compile("rows:[0-9]+");
    private static final Pattern PLAN_ID = Pattern.compile("([a-zA-Z]+_[0-9]+)");
    private static final Pattern TABLE_NAME = Pattern.compile("table:([a-zA-Z0-9_$]+),");
    private static String[] sqlInfoColumns = { "id", "operator info", "estRows", "access object" };

    public static void main(String[] args) {
        // 读配置文件
        if (args.length > 0)
            Configurations.loadConfigurations(args[0]);
        else
            Configurations.loadConfigurations(null);
        String type = "string";
        if (args.length > 1)
            type = args[1];
        // 读取schema文件，创建数据库及表;读取data文件，导入数据
        // createSchemaAndLoadData();
        // 读取query及queryTree文件，获取各查询
        execQuerys(type);
        // execQuerys("cyclic");
        // execQuerys("clique");
        // execQuerysTest();
        // StartCompare.StartCompare4OB();
        // execQuerys4OtherDB(type);
        // test();
        // executeQueryForAnaylze(type);
        // ttest(type);
        // filterQuerys(type);
        // execQuerysMoreTimes(type);
    }

    public static void createSchemaAndLoadData() {
        String createSql;
        try {
            List<Connection> conns = DatabaseConnection.getDatabaseConnectionForFirst();
            for (int m = 0; m < conns.size(); m++) {
                RecordLog.recordLog(LogLevelConstant.INFO, "连接数据库" + (m + 1));
                Connection con = conns.get(m);
                String dbBrand = Configurations.getDBConnections().get(m).getDatabaseBrand();
                if (!dbBrand.equalsIgnoreCase("sqlite")) {
                    CreateDatabase.dropAndCreateDatabase(con);
                }
                RecordLog.recordLog(LogLevelConstant.INFO, "开始创建Schema中的表");
                Statement statement = con.createStatement();
                statement.execute("use " + Configurations.getTestDatabaseName());
                statement.execute("set FOREIGN_KEY_CHECKS=0;");
                try {
                    String createTableSqlDir = Configurations.getSchemaOutputDir() + File.separator + "createSchemaSQL"
                            + ".txt";
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(new FileInputStream(new File(createTableSqlDir)),
                                    StandardCharsets.UTF_8));
                    String lineTxt;
                    while ((lineTxt = br.readLine()) != null) {
                        createSql = lineTxt;
                        System.out.println(createSql);
                        statement.execute(createSql);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                RecordLog.recordLog(LogLevelConstant.INFO, "Schema中所有表创建成功！");

                RecordLog.recordLog(LogLevelConstant.INFO, "开始导入数据！");

                String tableName = null;
                String txtName = null;
                String loadStatement = null;

                // 从文件中读取data
                File file = new File(Configurations.getDataoutputDir());
                String[] datafiles = file.list();
                for (int i = 0; i < datafiles.length; i++) {
                    tableName = "table_" + i;
                    // 多少个线程
                    for (int j = 0; j < Configurations.getThreadNumPerNode(); j++) {
                        txtName = tableName + "_" + j + ".txt";

                        if (dbBrand.equalsIgnoreCase("tidb")) {
                            loadStatement = "load data LOCAL infile '" + Configurations.getDataoutputDir()
                                    + "/" + txtName + "' into table " + tableName
                                    + " fields terminated by ',' lines terminated by '\r\n'";
                            System.out.println(loadStatement);
                            statement.execute(loadStatement);
                        } else if (dbBrand.equalsIgnoreCase("oceanbase")) { // oceanbase不支持从远程客户端加载数据,因此要把数据文件传到OB对应的rootserver上
                            loadStatement = "load data infile '"
                                    + Configurations.getDBConnections().get(m).getUploadDir()
                                    + "/" + txtName + "' into table " + tableName
                                    + " fields terminated by ',' lines terminated by '\r\n'";
                            System.out.println(loadStatement);
                            PreparedStatement pstmt = con.prepareStatement(loadStatement);
                            // statement.execute(loadStatement);
                            pstmt.execute();

                        } else if (dbBrand.equalsIgnoreCase("mysql")) {
                            loadStatement = "load data LOCAL infile '" + Configurations.getDataoutputDir()
                                    + "/" + txtName + "' into table " + tableName
                                    + " fields terminated by ',' lines terminated by '\r\n'";
                            System.out.println(loadStatement);
                            statement.execute(loadStatement);
                        } else if (dbBrand.equalsIgnoreCase("postgresql") || dbBrand.equalsIgnoreCase("gaussdb")) {
                            CopyManager copyManager = new CopyManager((BaseConnection) con);
                            FileInputStream fileInputStream = new FileInputStream(
                                    Configurations.getDataoutputDir() + "/" + txtName);
                            loadStatement = "copy " + tableName + " from stdin WITH DELIMITER ','";
                            System.out.println(loadStatement);
                            copyManager.copyIn(loadStatement, fileInputStream);
                        }
                        // statement.execute("set FOREIGN_KEY_CHECKS=1;");
                        System.out.println(txtName + "数据导入成功！");
                    }
                    RecordLog.recordLog(LogLevelConstant.INFO, tableName + "表数据导入成功！");
                }

                RecordLog.recordLog(LogLevelConstant.INFO, "开始创建外键");
                try {
                    String addForeignKeySqlDir = Configurations.getSchemaOutputDir() + File.separator + "addForeignKey"
                            + ".txt";
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(new FileInputStream(new File(addForeignKeySqlDir)),
                                    StandardCharsets.UTF_8));
                    String addForeignKey;
                    while ((addForeignKey = br.readLine()) != null) {
                        System.out.println(addForeignKey);
                        statement.execute(addForeignKey);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                con.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void execQuerys(String type) {
        try {
            // 读取所有query，放入queryTree
            List<QueryTree> queryTrees = new ArrayList<>();
            try {
                String queryTXT = Configurations.getQueryOutputDir() + File.separator + type
                        + ".txt";
                BufferedReader br = new BufferedReader(
                        new InputStreamReader(new FileInputStream(new File(queryTXT)), StandardCharsets.UTF_8));
                String lineTxt;
                while ((lineTxt = br.readLine()) != null) {
                    // 过滤空行
                    if (lineTxt.matches("[\\s]*") || lineTxt.matches("[ ]*##[\\s\\S]*")) {
                        continue;
                    }
                    // 找出query中的表
                    int index1 = lineTxt.indexOf("from");
                    int index2 = lineTxt.indexOf("where");
                    String[] tableNames = lineTxt.substring(index1 + 4, index2).split(",");
                    List<Table> tables = new ArrayList<>();
                    for (int i = 0; i < tableNames.length; i++) {
                        Table table = new Table(tableNames[i].trim());
                        tables.add(table);
                    }
                    QueryTree queryTree = new QueryTree(lineTxt, tables);
                    queryTrees.add(queryTree);

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            // 执行
            // QueryResultExecution.executeForJoinRank(queryTrees);
            QueryResultExecution.shuffleJoin(queryTrees);
            // QueryResultExecution.execute(true, queryTrees, type);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void execQuerys4OtherDB(String type) {
        // 读.csv文件，得到joinorder
        File file = new File(Configurations.getJoinOrderRankDir());
        String output = "";
        String output_type = "";
        String[] queryFiles = file.list();
        Map<Integer, String> queryFilesMap = new HashMap<>();
        for (String queryFile : queryFiles) {
            if (!queryFile.contains("query"))
                continue;
            int index1 = queryFile.indexOf("_");
            int index2 = queryFile.indexOf(".");
            int queryNum = Integer.parseInt(queryFile.substring(index1 + 1, index2));
            queryFilesMap.put(queryNum, queryFile);
        }
        try {
            List<DBConnection> conns = DatabaseConnection.getDatabaseConnection(true);
            // List<List<Integer>> rankListPerDB = new ArrayList<>();
            // List<List<Long>> timeDistanceListPerDB = new ArrayList<>();
            List<Double> MRRperDB = new ArrayList<>();
            List<Double> avgTimePerDB = new ArrayList<>();
            List<Integer> sameNumPerDB = new ArrayList<>();

            for (DBConnection dbConnection : conns) {
                Connection con = dbConnection.getDbConn();

                List<Integer> rankList = new ArrayList<>();
                List<Long> originalTimeList = new ArrayList<>();
                List<Long> timeDistanceList = new ArrayList<>();

                String queryTXT = Configurations.getQueryOutputDir() + File.separator + type
                        + ".txt";
                BufferedReader br = new BufferedReader(
                        new InputStreamReader(new FileInputStream(new File(queryTXT)), "UTF-8"));
                String lineTxt;
                int fileIndex = 0;
                int sameNum = 0;
                while ((lineTxt = br.readLine()) != null) {
                    // 过滤空行
                    if (lineTxt.matches("[\\s]*") || lineTxt.matches("[ ]*##[\\s\\S]*")) {
                        continue;
                    }
                    fileIndex++;
                    if (!queryFilesMap.containsKey(fileIndex))
                        continue;
                    // 找出query中的表
                    int index1 = lineTxt.indexOf("from");
                    int index2 = lineTxt.indexOf("where");
                    String[] tableNames = lineTxt.substring(index1 + 4, index2).split(",");
                    List<Table> tables = new ArrayList<>();
                    for (int i = 0; i < tableNames.length; i++) {
                        Table table = new Table(tableNames[i].trim());
                        tables.add(table);
                    }

                    List<Pair<String, Long>> pairs = new ArrayList<>();

                    Statement statement = con.createStatement();
                    String originalQuery = lineTxt;
                    // hint不使用查询计划缓存
                    try {
                        if (dbConnection.getDatabaseBrand().equalsIgnoreCase("oceanbase"))
                            originalQuery = "select /*+USE_PLAN_CACHE(NONE)*/ " + originalQuery.substring(6);
                        else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("tidb"))
                            originalQuery = "select /*+ ignore_plan_cache() */ " + originalQuery.substring(6);
                        else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("postgresql")
                                || dbConnection.getDatabaseBrand().equalsIgnoreCase("gaussdb")) {
                            statement.execute("SET statement_timeout = 100000");
                        } else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("mysql")) {
                            statement.execute("set max_execution_time = 30000");// 超过30s的不要了
                        }
                        System.out.println(originalQuery);
                        RecordLog.recordLog(LogLevelConstant.INFO, "执行原始查询");

                        statement.executeQuery(originalQuery);// 跑两遍，算第二次的执行时间；第一次执行cache住所有数据表的数据
                        long startTime = System.currentTimeMillis();
                        statement.executeQuery(originalQuery);
                        long endTime = System.currentTimeMillis();
                        long originalTime = endTime - startTime;
                        RecordLog.recordLog(LogLevelConstant.INFO, "时间：" + (endTime - startTime));

                        // explain查询，得到数据库选择的连接顺序
                        QueryExplain queryExplain = new QueryExplain(dbConnection, statement, originalQuery, tables);
                        String originalJoinOrderStr = queryExplain.getJoinedTableOrderStr();

                        System.out.println("######" + originalJoinOrderStr);
                        pairs.add(Pair.of("original", originalTime));
                        originalTimeList.add(originalTime);
                        statement.close();

                        BufferedReader reader = new BufferedReader(new FileReader(
                                Configurations.getJoinOrderRankDir() + File.separator + queryFilesMap.get(fileIndex)));// 打开对应query的csv文件
                        reader.readLine();// 第一行信息，为标题信息，不用
                        int queryIndex = 0;
                        List<String> joinOrderQuerys = new LinkedList<>();

                        while ((lineTxt = reader.readLine()) != null) {
                            // 过滤空行
                            if (lineTxt.matches("[\\s]*") || lineTxt.matches("[ ]*##[\\s\\S]*")) {
                                continue;
                            }
                            long upperBound;
                            if (dbConnection.getDatabaseBrand().equalsIgnoreCase("oceanbase")) {
                                upperBound = originalTime * 1000 + 1000000;
                            } else {
                                upperBound = originalTime + 1000;
                            }
                            String[] item = lineTxt.split("\",\"");// CSV格式文件为逗号分隔符文件，这里根据逗号切分
                            String query = item[1];
                            String hintQuery = "";
                            boolean flag = false;
                            if (!item[1].contains("select")) {
                                flag = true;
                                String thisJoin = item[1];
                                // System.out.println(ob);
                                // System.out.println(oriOrderStr);
                                if (thisJoin.trim().equals(originalJoinOrderStr.trim())) {
                                    sameNum++;
                                    continue;
                                } else {
                                    // 如果是tidb，只跑左深树的
                                    // if(dbConnection.getDatabaseBrand().equalsIgnoreCase("tidb")
                                    // && !isLeftDeepTree(thisJoin,tables))
                                    // continue;
                                    hintQuery = changeToFullQuery(thisJoin, originalQuery,
                                            dbConnection.getDatabaseBrand(), upperBound);
                                    // System.out.println("%%%%%%%" + hintQuery);

                                }
                            } else {
                                String jo = "";
                                if (item[1].contains("LEADING")) {
                                    index1 = item[1].indexOf("LEADING");
                                    index2 = item[1].indexOf("*/");
                                    jo = item[1].substring(index1 + 7, index2).trim();
                                } else {
                                    index1 = item[1].indexOf("from");
                                    index2 = item[1].indexOf("where");
                                    jo = item[1].substring(index1 + 4, index2).trim();
                                }

                                // 如果是tidb，只跑左深树的
                                // if(dbConnection.getDatabaseBrand().equalsIgnoreCase("tidb")
                                // && !isLeftDeepTree(jo,tables))
                                // continue;
                                if (jo.equals(originalJoinOrderStr.trim()))
                                    continue;
                                else {
                                    hintQuery = changeToFullQuery(jo, originalQuery, dbConnection.getDatabaseBrand(),
                                            upperBound);
                                    // System.out.println("%%%%%%%" + hintQuery);
                                }
                            }
                            joinOrderQuerys.add(hintQuery);
                            // System.out.println(hintQuery);

                            // 对于PG：执行计划缓存只在会话级别，因此重新连接数据库执行同一查询的不同连接顺序
                            // TODO 对于OB和TIDB：需要看一下是否也只在会话级别 主要问题是换了一个链接之后，是不是重新warm up
                            // TODO pg可能没有计划缓存？
                            // if(dbConnection.getDatabaseBrand().equalsIgnoreCase("postgresql")) {
                            // con.close();
                            // con =
                            // DatabaseConnection.changeDatabaseConnectionByDatabaseName(dbConnection);
                            // }
                            Statement statement1 = con.createStatement();
                            try {
                                if (dbConnection.getDatabaseBrand().equalsIgnoreCase("postgresql")
                                        || dbConnection.getDatabaseBrand().equalsIgnoreCase("gaussdb")) {
                                    upperBound = originalTime + 1000;
                                    statement1.execute("SET statement_timeout = " + upperBound);
                                    statement1.execute("set join_collapse_limit = 1");// 强制执行join顺序
                                } else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("mysql")) {
                                    upperBound = originalTime + 1000;
                                    statement1.execute("set max_execution_time = " + upperBound);
                                    // 这里看一下8.0的，据说8.0没有这个了
                                    // statement1.execute("set global query_cache_size = 0");
                                }
                                // statement1.setQueryTimeout(1);
                                long startTime2 = System.currentTimeMillis();
                                statement1.executeQuery(hintQuery);
                                long endTime2 = System.currentTimeMillis();
                                long reorderTime2 = endTime2 - startTime2;
                                pairs.add(Pair.of(Integer.toString(queryIndex), reorderTime2));
                                System.out.println(queryIndex + " " + reorderTime2 + "ms");
                            } catch (Exception e) {
                                System.out.println(queryIndex + " Timeout");
                                pairs.add(Pair.of(Integer.toString(queryIndex), upperBound));
                                // 实验trick：Mysql跑的太慢，但是mysql和tidb选的计划其实差不多，mysql用tidb的计划们跑，后面的其实都timeout了
                                if (flag && queryIndex > 50)
                                    break;
                            }
                            statement1.close();
                            queryIndex++;
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
                                line[1] = joinOrderQuerys.get(Integer.parseInt(pairs.get(i).getLeft()));
                            }
                            line[2] = Long.toString(pairs.get(i).getRight());
                            lineList.add(line);
                        }
                        rankList.add(rank);
                        timeDistanceList.add(timeDistance);

                        // 写入文件
                        output = Configurations.getJoinOrderRankDir() + File.separator + "joinOrderRank_"
                                + dbConnection.getDatabaseBrand();
                        File dir = new File(output);
                        if (!dir.exists()) {
                            dir.mkdir();
                        }
                        output_type = output + File.separator + type;
                        File dir2 = new File(output_type);
                        if (!dir2.exists()) {
                            dir2.mkdir();
                        }
                        String joinOrderTimeFile = output_type + File.separator + "query_" + fileIndex + ".csv";
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
                        System.out.println("原始查询出错");
                    }

                }
                con.close();
                sameNumPerDB.add(sameNum);
                System.out.println("sameNum: " + sameNum);
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
                String[] csvHeaders = { "Query", "Rank", "Time (ms)", "Best Time (ms)", "Time Gap (ms)" };
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
                    outRankList.add(line);
                    sum += timeDistanceList.get(i);
                }
                avgTimePerDB.add(sum * 1.0 / rankList.size());

                String joinOrderRankFile = output_type + File.separator + "joinOrderRank_"
                        + dbConnection.getDatabaseBrand() + ".csv";
                try (FileOutputStream fos = new FileOutputStream(joinOrderRankFile);
                        OutputStreamWriter osw = new OutputStreamWriter(fos,
                                StandardCharsets.UTF_8);
                        CSVWriter writer = new CSVWriter(osw)) {
                    writer.writeAll(outRankList);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // 写MRR和平均时延差
            String MRRfile = output_type + File.separator + "MRRresult" + ".csv";
            try (BufferedWriter bw = new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(MRRfile), StandardCharsets.UTF_8))) {
                for (int i = 0; i < MRRperDB.size(); i++) {
                    String line = conns.get(i).getDatabaseBrand() + ": MRR = "
                            + MRRperDB.get(i) + "\t" + "Avg Latency Gap = " + avgTimePerDB.get(i) + " ms"
                            + "\n" + "num of same join orders = " + sameNumPerDB.get(i);
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

    public static String changeToFullQuery(String joinOrder, String oriQuery, String dbMSBrand, long upperBound) {
        // 先删除原来的hint
        if (oriQuery.contains("/*")) {
            int index1 = oriQuery.indexOf("count(*)");
            oriQuery = "select " + oriQuery.substring(index1);
        }
        // System.out.println(oriQuery);
        // from ... where中间的表的顺序
        String[] splitedSQLSelect = oriQuery.split("select");
        String[] splitedSQLFrom = splitedSQLSelect[1].split("from");
        String[] splitedSQLWhere = splitedSQLFrom[1].split("where");
        String fullSQL = "";
        // 这里默认ob和pg互相转换，tidb和mysql互相转换
        if (dbMSBrand.equalsIgnoreCase("tidb")) {
            // joinOrder = joinOrder.replaceAll("\\( | \\)","");
            fullSQL = "select /*+ MAX_EXECUTION_TIME(" + upperBound + ") ignore_plan_cache() */ STRAIGHT_JOIN "
                    + splitedSQLFrom[0] + "from " + joinOrder + " where " + splitedSQLWhere[1];
        } else if (dbMSBrand.equalsIgnoreCase("oceanbase")) {
            joinOrder = joinOrder.replaceAll(" cross join ", ",");
            fullSQL = "select /*+query_timeout(" + upperBound + ") USE_PLAN_CACHE(NONE) LEADING" + joinOrder + "*/ "
                    + splitedSQLFrom[0] + "from " + splitedSQLFrom[1];
        } else if (dbMSBrand.equalsIgnoreCase("postgresql")) {
            joinOrder = joinOrder.replaceAll(",", " cross join ");
            fullSQL = "select " + splitedSQLFrom[0] + "from " + joinOrder + " where " + splitedSQLWhere[1];
        } else if (dbMSBrand.equalsIgnoreCase("mysql")) {
            // long upperBound = originalTime + 1000;
            fullSQL = "select /*! STRAIGHT_JOIN */ " + splitedSQLFrom[0] + "from " + joinOrder + " where "
                    + splitedSQLWhere[1];
        } else if (dbMSBrand.equalsIgnoreCase("gaussdb")) {
            fullSQL = "select /*+ leading" + joinOrder + "*/ " + splitedSQLFrom[0] + "from " + splitedSQLFrom[1];
        }
        return fullSQL;
    }

    // 把原始查询和排名第一的顺序重新多跑几遍
    public static void execQuerysMoreTimes(String type) {
        // 读.csv文件，得到joinorder
        File file = new File(Configurations.getJoinOrderRankDir());
        String output = "";
        String output_type = "";
        String[] queryFiles = file.list();
        Map<Integer, String> queryFilesMap = new HashMap<>();
        for (String queryFile : queryFiles) {
            if (!queryFile.contains("query"))
                continue;
            int index1 = queryFile.indexOf("_");
            int index2 = queryFile.indexOf(".");
            int queryNum = Integer.parseInt(queryFile.substring(index1 + 1, index2));
            queryFilesMap.put(queryNum, queryFile);
        }
        try {
            List<DBConnection> conns = DatabaseConnection.getDatabaseConnection(true);

            for (DBConnection dbConnection : conns) {
                Connection con = dbConnection.getDbConn();
                Statement statement = con.createStatement();
                Map<Integer, List<Long>> executeTimesPerQuery = new HashMap<>();
                Map<Integer, List<Long>> executeTimesPerQuery1 = new HashMap<>();
                Map<Integer, String> querys = new HashMap<>();
                Map<Integer, String> topquerys = new HashMap<>();

                int cnt = 7;
                while (cnt > 0) {
                    String queryTXT = Configurations.getQueryOutputDir() + File.separator + type
                            + ".txt";
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(new FileInputStream(new File(queryTXT)), "UTF-8"));
                    String lineTxt;
                    int fileIndex = 0;

                    while ((lineTxt = br.readLine()) != null) {
                        // 过滤空行
                        if (lineTxt.matches("[\\s]*") || lineTxt.matches("[ ]*##[\\s\\S]*")) {
                            continue;
                        }
                        fileIndex++;
                        if (!queryFilesMap.containsKey(fileIndex))
                            continue;

                        String originalQuery = lineTxt;

                        BufferedReader reader = new BufferedReader(new FileReader(
                                Configurations.getJoinOrderRankDir() + File.separator + queryFilesMap.get(fileIndex)));// 打开对应query的csv文件
                        reader.readLine();// 第一行信息，为标题信息，不用

                        lineTxt = reader.readLine(); // 第二行 即排名第一的
                        if (!lineTxt.contains("select"))
                            continue;// 第一的是原始查询，则下一个

                        if (cnt == 7)
                            querys.put(fileIndex, originalQuery);
                        // hint不使用查询计划缓存
                        try {
                            if (dbConnection.getDatabaseBrand().equalsIgnoreCase("oceanbase"))
                                originalQuery = "select /*+USE_PLAN_CACHE(NONE)*/ " + originalQuery.substring(6);
                            else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("tidb"))
                                originalQuery = "select /*+ ignore_plan_cache() */ " + originalQuery.substring(6);
                            else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("postgresql")
                                    || dbConnection.getDatabaseBrand().equalsIgnoreCase("gaussdb")) {
                                statement.execute("SET statement_timeout = 100000");
                            } else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("mysql")) {
                                statement.execute("set max_execution_time = 30000");// 超过30s的不要了
                            }
                            System.out.println(originalQuery);
                            RecordLog.recordLog(LogLevelConstant.INFO, "执行原始查询");

                            long startTime = System.currentTimeMillis();
                            statement.executeQuery(originalQuery);// 读取每一行的查询 然后执行
                            long endTime = System.currentTimeMillis();
                            long exeTime = endTime - startTime;
                            System.out.println(exeTime);
                            if (cnt == 7) {
                                executeTimesPerQuery.put(fileIndex, new ArrayList<>());
                                executeTimesPerQuery.get(fileIndex).add(exeTime);
                            } else
                                executeTimesPerQuery.get(fileIndex).add(exeTime);

                            String[] item = lineTxt.split("\",\"");// CSV格式文件为逗号分隔符文件，这里根据逗号切分
                            String topquery = item[1];

                            if (dbConnection.getDatabaseBrand().equalsIgnoreCase("tidb")) {
                                int index1 = topquery.indexOf("MAX");
                                int index2 = topquery.indexOf("ignore");
                                topquery = topquery.substring(0, index1) + "MAX_EXECUTION_TIME(30000) "
                                        + topquery.substring(index2);
                            } else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("oceanbase")) {
                                int index1 = topquery.indexOf("USE_PLAN");
                                topquery = "select /*+" + topquery.substring(index1);
                            }
                            System.out.println(topquery);

                            if (cnt == 7)
                                topquerys.put(fileIndex, topquery);

                            try {
                                long startTime1 = System.currentTimeMillis();
                                statement.executeQuery(topquery);// 读取每一行的查询 然后执行
                                long endTime1 = System.currentTimeMillis();
                                long exeTime1 = endTime1 - startTime1;
                                System.out.println(exeTime1);
                                if (cnt == 7) {
                                    executeTimesPerQuery1.put(fileIndex, new ArrayList<>());
                                    executeTimesPerQuery1.get(fileIndex).add(exeTime1);
                                } else
                                    executeTimesPerQuery1.get(fileIndex).add(exeTime1);
                            } catch (Exception e) {
                                System.out.println("timeout");
                                long timeout = 30000;
                                if (cnt == 7) {

                                    executeTimesPerQuery1.put(fileIndex, new ArrayList<>());
                                    executeTimesPerQuery1.get(fileIndex).add(timeout);
                                } else
                                    executeTimesPerQuery1.get(fileIndex).add(timeout);
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                    cnt--;
                }

                // 将执行时间写入文件

                List<String[]> outTimeList = new ArrayList<>();

                // 先写标题:query,time1,...,avg time
                String[] csvHeaders = new String[4];
                csvHeaders[0] = "ori query";
                csvHeaders[1] = "top query";
                csvHeaders[2] = "ori avg";
                csvHeaders[3] = "top avg";
                outTimeList.add(csvHeaders);

                // 再写时间
                for (Map.Entry<Integer, String> entry : querys.entrySet()) {
                    String[] line = new String[4];
                    int i = entry.getKey();
                    line[0] = querys.get(i);
                    line[1] = topquerys.get(i);
                    line[2] = calculateAvgTime(executeTimesPerQuery.get(i)).toString();
                    line[3] = calculateAvgTime(executeTimesPerQuery1.get(i)).toString();
                    outTimeList.add(line);
                }
                String queryExecuteTimeFile = Configurations.getQueryOutputDir() + File.separator + type + "_time"
                        + ".csv";
                try (FileOutputStream fos = new FileOutputStream(queryExecuteTimeFile);
                        OutputStreamWriter osw = new OutputStreamWriter(fos,
                                StandardCharsets.UTF_8);
                        CSVWriter writer = new CSVWriter(osw)) {
                    writer.writeAll(outTimeList);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                con.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Long calculateAvgTime(List<Long> executeTime) {

        Collections.sort(executeTime);
        long sum = 0;
        for (int j = 1; j < executeTime.size() - 1; j++)
            sum += executeTime.get(j);
        return sum / (executeTime.size() - 2);

    }

    public static void test() {
        // String sql = "select count(*) as result from table_4 ,table_9, table_16 where
        // table_9.col_20 >= 662513367.9897921342210549 and table_4.col_3 >=
        // 1091570067.79213611997066401 and table_16.col_5 <=
        // 1148546756.7839221488678496 and table_9.fk_6 = table_4.primaryKey and
        // table_9.fk_4 = table_16.primaryKey ";
        // String jo = "";
        // int index1, index2;
        // if (sql.contains("LEADING")) {
        // index1 = sql.indexOf("LEADING");
        // index2 = sql.indexOf("*/");
        // jo = sql.substring(index1 + 7, index2).trim();
        // } else {
        // index1 = sql.indexOf("from");
        // index2 = sql.indexOf("where");
        // jo = sql.substring(index1 + 4, index2).trim();
        // }
        // String thisJO = "(table_15,(table_4,table_0))";
        // String hint = changeToFullQuery(thisJO,sql,"postgresql",1000);
        // System.out.println(hint);
        List<String> randomJoinOrderQuery = new LinkedList<>();
        List<String> allJoinOrderQuery = new ArrayList<>();
        allJoinOrderQuery.add("one");
        allJoinOrderQuery.add("two");
        allJoinOrderQuery.add("three");
        allJoinOrderQuery.add("four");
        allJoinOrderQuery.add("five");
        Collections.shuffle(allJoinOrderQuery);
        randomJoinOrderQuery = allJoinOrderQuery.subList(0, 3);
        System.out.println(allJoinOrderQuery);
        System.out.println(randomJoinOrderQuery);
    }

    // 用于分析 ob tidb 查询计划的每个算子的基数预估结果
    public static void executeQueryForAnaylzeCard(String type) {
        String targetQueryEstimatedCard = Configurations.getGenerateExactCard();
        try {
            List<Connection> conns = DatabaseConnection.getDatabaseConnectionForFirst();
            // for(Connection con : conns) {
            // CreateDatabase.dropAndCreateDatabase(con);
            // }

            List<DBConnection> dbConns = DatabaseConnection.getDatabaseConnection(true);

            for (DBConnection dbConnection : dbConns) {
                Connection con = dbConnection.getDbConn();
                Statement statement = con.createStatement();
                try {
                    String queryTXT = Configurations.getQueryOutputDir() + File.separator + type
                            + ".txt";
                    List<String> queries = new ArrayList<>();
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(new FileInputStream(new File(queryTXT)), StandardCharsets.UTF_8));
                    String lineTxt;
                    // 拿到所有 query
                    while ((lineTxt = br.readLine()) != null) {
                        // 过滤空行
                        if (lineTxt.matches("[\\s]*") || lineTxt.matches("[ ]*##[\\s\\S]*")) {
                            continue;
                        }

                        String modifiedQuery = lineTxt;

                        // Use regex to find numeric values in the between clause
                        Pattern pattern = Pattern.compile("(\\d+\\.\\d+) and (\\d+\\.\\d+)");
                        Matcher matcher = pattern.matcher(modifiedQuery);

                        if (matcher.find()) {
                            // Extract and convert numeric values to integers
                            int lowerValue = (int) Double.parseDouble(matcher.group(1));
                            int upperValue = (int) Double.parseDouble(matcher.group(2));

                            // Construct the modified SQL query
                            modifiedQuery = modifiedQuery
                                    .replaceFirst("(?<=between )\\d+\\.\\d+", String.valueOf(lowerValue))
                                    .replaceFirst("and \\d+\\.\\d+", "and " + upperValue);

                        }
                        // System.out.println(modifiedQuery);

                        lineTxt = "explain " + lineTxt;
                        queries.add(lineTxt);
                    }
                    // 导入数据
                    // CreateDatabase.reloadData(dbConnection);
                    // CreateDatabase.reloadData4Tidb(dbConnection);

                    // System.out.println("记录数据库执行真实基数");
                    // for (String query : queries) {
                    //     ResultSet rs = statement.executeQuery(query);
                    //     int count = 0;

                    //     if (rs.next()) {
                    //         count = rs.getInt("result");
                    //     }
                    //     try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                    //             targetQueryEstimatedCard + File.separator + "trueCard_"
                    //                     + dbConnection.getDatabaseBrand() + ".csv",
                    //             true), Configurations.getEncodeType()))) {
                    //         bw.write(count + "\r\n");
                    //         bw.flush();
                    //     } catch (IOException e) {
                    //         e.printStackTrace();
                    //     }
                    // }

                    // 运行 sql
                    System.out.println("开始测试基数预估!");
                    for (String query : queries) {
                        QueryPlanParser queryExplain = new QueryPlanParser(dbConnection, statement, query);

                        System.out.println("最终顶点的基数为" + queryExplain.getEstimatedCard());
                        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                                new FileOutputStream(targetQueryEstimatedCard + File.separator + "estimatedCard_"
                                        + dbConnection.getDatabaseBrand() + ".csv", true),
                                Configurations.getEncodeType()))) {
                            bw.write(queryExplain.getEstimatedCard() + "\r\n");
                            bw.flush();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    // AnalyzeTableAlone.Analyze(dbConnection);

                    // System.out.println("analyze 后测试基数预估!");
                    // for (String query : queries) {
                    // QueryPlanParser queryExplain = new QueryPlanParser(dbConnection, statement,
                    // query);

                    // System.out.println("最终顶点的基数为"+queryExplain.getEstimatedCard());
                    // try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new
                    // FileOutputStream(targetQueryEstimatedCard+File.separator+"analyze_estimatedCard_"+dbConnection.getDatabaseBrand()+".csv",
                    // true), Configurations.getEncodeType()))) {
                    // bw.write(queryExplain.getEstimatedCard() + "\r\n");
                    // bw.flush();
                    // } catch (IOException e) {
                    // e.printStackTrace();
                    // }
                    // }

                    // System.out.println("create index 后测试基数预估!");
                    // for (String query : queries) {
                    // QueryPlanParser queryExplain = new QueryPlanParser(dbConnection, statement,
                    // query);

                    // System.out.println("最终顶点的基数为"+queryExplain.getEstimatedCard());
                    // try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new
                    // FileOutputStream(targetQueryEstimatedCard+File.separator+"index_estimatedCard_"+dbConnection.getDatabaseBrand()+".csv",
                    // true), Configurations.getEncodeType()))) {
                    // bw.write(queryExplain.getEstimatedCard() + "\r\n");
                    // bw.flush();
                    // } catch (IOException e) {
                    // e.printStackTrace();
                    // }
                    // }

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void executeQueryForAnaylze(String type) {
        // 读.csv文件，得到joinorder
        File file = new File(Configurations.getJoinOrderRankDir());
        String output = "";
        String output_type = "";
        String[] queryFiles = file.list();
        Map<Integer, String> queryFilesMap = new HashMap<>();
        for (String queryFile : queryFiles) {
            if (!queryFile.contains("query"))
                continue;
            int index1 = queryFile.indexOf("_");
            int index2 = queryFile.indexOf(".");
            int queryNum = Integer.parseInt(queryFile.substring(index1 + 1, index2));
            queryFilesMap.put(queryNum, queryFile);
        }
        try {
            List<DBConnection> conns = DatabaseConnection.getDatabaseConnection(true);

            for (DBConnection dbConnection : conns) {
                Connection con = dbConnection.getDbConn();
                Statement statement = con.createStatement();
                try {
                    String queryTXT = Configurations.getQueryOutputDir() + File.separator + type
                            + ".txt";

                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(new FileInputStream(new File(queryTXT)), StandardCharsets.UTF_8));
                    String lineTxt;
                    int queryIndex = 0;
                    while ((lineTxt = br.readLine()) != null) {
                        // 过滤空行
                        if (lineTxt.matches("[\\s]*") || lineTxt.matches("[ ]*##[\\s\\S]*")) {
                            continue;
                        }
                        queryIndex++;
                        if (!queryFilesMap.containsKey(queryIndex))
                            continue;

                        List<String> queries = new ArrayList<>();
                        queries.add(lineTxt);

                        BufferedReader reader = new BufferedReader(new FileReader(
                                Configurations.getJoinOrderRankDir() + File.separator + queryFilesMap.get(queryIndex)));// 打开对应query的csv文件
                        reader.readLine();// 第一行信息，为标题信息，不用
                        String lineTxt2 = reader.readLine(); // 第二行 即排名第一的
                        if (!lineTxt2.contains("select"))
                            continue;// 第一的是原始查询，则下一个查询
                        String[] item = lineTxt2.split("\",\"");// CSV格式文件为逗号分隔符文件，这里根据逗号切分
                        String topquery = item[1];
                        if (dbConnection.getDatabaseBrand().equalsIgnoreCase("tidb")) {
                            int index_ignore = topquery.indexOf("ignore");
                            topquery = "select /*+ " + topquery.substring(index_ignore);// 去掉MAX_EXECUTION
                        } else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("oceanbase")) {
                            int index_use = topquery.indexOf("USE_PLAN");
                            topquery = "select /*+" + topquery.substring(index_use);
                        } else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("mysql")) {
                            statement.execute("set max_execution_time = 100000");
                            int end_hint = topquery.indexOf("count(*)");
                            topquery = "select /*! STRAIGHT_JOIN */ " + topquery.substring(end_hint);
                        }
                        queries.add(topquery);

                        List<Long> times = new ArrayList<>();
                        for (int n = 0; n < queries.size(); n++) {

                            String query = queries.get(n);
                            midJoinSubQueries = new ArrayList<>();
                            String outputFile = "";
                            if (n == 0)
                                outputFile = Configurations.getJoinOrderRankDir() + File.separator + "query_"
                                        + queryIndex
                                        + "_ori.txt";
                            else
                                outputFile = Configurations.getJoinOrderRankDir() + File.separator + "query_"
                                        + queryIndex
                                        + "_top.txt";
                            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                                    new FileOutputStream(outputFile, true), StandardCharsets.UTF_8));
                            System.out.println(query);
                            bw.write(query + "\r\n");
                            bw.flush();
                            try {
                                if (dbConnection.getDatabaseBrand().equalsIgnoreCase("postgresql")
                                        && query.contains("cross join"))
                                    statement.execute("set join_collapse_limit = 1");
                                long startTime = System.currentTimeMillis();
                                ResultSet rs = statement.executeQuery(query);
                                long endTime = System.currentTimeMillis();
                                long reorderTime = endTime - startTime;
                                times.add(reorderTime);
                                System.out.println("time: " + reorderTime);
                                bw.write("time: " + reorderTime + "\r\n");
                                bw.flush();
                                while (rs.next()) {
                                    System.out.println("result: " + rs.getString(1));
                                    bw.write("result: " + rs.getString(1) + "\r\n");
                                    bw.write("\r\n");
                                    bw.flush();
                                }

                                StringBuffer execquery = new StringBuffer(query);
                                execquery = new StringBuffer("explain ").append(execquery);

                                String info = "";
                                rs = statement.executeQuery(execquery.toString());
                                while (rs.next()) {
                                    if (dbConnection.getDatabaseBrand().equalsIgnoreCase("tidb"))
                                        info += rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3)
                                                + "\t" + rs.getString(4) + "\t" + rs.getString(5) + "\n";
                                    else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("oceanbase")
                                            || dbConnection.getDatabaseBrand().equalsIgnoreCase("postgresql"))
                                        info += rs.getString(1) + "\n";
                                    else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("mysql")) {
                                        for (int i = 1; i <= 12; i++)
                                            info += rs.getString(i) + "\t";
                                        info += "\n";
                                    }

                                }
                                bw.write(info + "\r\n");
                                bw.write("\r\n");
                                bw.flush();

                                int index1 = query.indexOf("from");
                                int index2 = query.indexOf("where");
                                String[] tables = query.substring(index1 + 4, index2).split(",");
                                String[] filters = query.substring(index2 + 5).split("and");
                                List<String> filterSQLs = new ArrayList<>();
                                for (int j = 0; j < tables.length; j++) {
                                    if (tables[j].contains("table")) {
                                        String tableName = tables[j].trim();
                                        for (int k = 0; k < filters.length; k++) {
                                            String str = tableName + ".col";
                                            if (filters[k].contains(str)) {
                                                String sql = "select count(*) from " + tableName + " where "
                                                        + filters[k].trim();
                                                filterSQLs.add(sql);
                                            }

                                        }
                                    }
                                }
                                for (int j = 0; j < filterSQLs.size(); j++) {
                                    System.out.println(filterSQLs.get(j));
                                    bw.write(filterSQLs.get(j) + "\r\n");
                                    bw.flush();
                                    long begin = System.currentTimeMillis();
                                    rs = statement.executeQuery(filterSQLs.get(j));
                                    long end = System.currentTimeMillis();
                                    long time = end - begin;
                                    while (rs.next()) {
                                        System.out.println("result: " + rs.getString(1) + " time: " + time);
                                        bw.write("result: " + rs.getString(1) + " time: " + time + "\r\n");
                                        bw.write("\r\n");
                                        bw.flush();
                                    }
                                }

                                if (dbConnection.getDatabaseBrand().equalsIgnoreCase("oceanbase")) {
                                    // 处理ob输出结果
                                    String[] temp = info.split("Outputs & filters");
                                    String[] half1 = temp[0].split("\n");
                                    String[] half2 = temp[1].split("  [0-9]+ - ");
                                    ArrayList<String[]> queryPlan = new ArrayList<>();
                                    int half2_index = 1;
                                    for (int i = 3; i < half1.length - 1; i++) {
                                        String[] columns = half1[i].split("\\|");
                                        String[] infos = new String[5];
                                        for (int j = 1; j < columns.length - 1; j++) {
                                            infos[j - 1] = columns[j];
                                        }
                                        infos[4] = half2[half2_index];
                                        half2_index++;
                                        queryPlan.add(infos);
                                    }
                                    RawNode root = buildRawNodeTree_Oceanbase(queryPlan);
                                    findMidJoin2(root, tables, filters); // 所有中间join的结果
                                } else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("tidb")) {
                                    StringBuffer analyzequery = new StringBuffer(query);
                                    analyzequery = new StringBuffer("explain analyze ").append(analyzequery);
                                    System.out.println(analyzequery);
                                    bw.write(analyzequery + "\r\n");
                                    bw.flush();
                                    rs = statement.executeQuery(analyzequery.toString());
                                    info = "";
                                    while (rs.next()) {
                                        for (int i = 1; i <= 9; i++)
                                            info += rs.getString(i) + "\t";
                                        info += "\n";
                                    }
                                    bw.write(info + "\r\n");
                                    bw.write("\r\n");
                                    bw.flush();

                                } else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("mysql")) {
                                    StringBuffer explaintree = new StringBuffer(query);
                                    explaintree = new StringBuffer("explain format=tree ").append(explaintree);
                                    System.out.println(explaintree);
                                    bw.write(explaintree + "\r\n");
                                    bw.flush();
                                    String info2 = "";
                                    rs = statement.executeQuery(explaintree.toString());
                                    while (rs.next()) {
                                        info2 += rs.getString(1) + "\n";
                                    }
                                    bw.write(info2 + "\r\n");
                                    bw.write("\r\n");
                                    bw.flush();

                                    StringBuffer analyzequery = new StringBuffer(query);
                                    analyzequery = new StringBuffer("explain analyze ").append(analyzequery);
                                    System.out.println(analyzequery);
                                    bw.write(analyzequery + "\r\n");
                                    bw.flush();
                                    String info3 = "";
                                    rs = statement.executeQuery(analyzequery.toString());
                                    while (rs.next()) {
                                        info3 += rs.getString(1) + "\n";
                                    }
                                    bw.write(info3 + "\r\n");
                                    bw.write("\r\n");
                                    bw.flush();

                                    // QueryExplain queryExplain = new QueryExplain(dbConnection, statement, query,
                                    // null);
                                    // String JoinOrderStr = queryExplain.getJoinedTableOrderStr();
                                    // String[] tmp = JoinOrderStr.split(",");
                                    // String whereSQL = "";
                                    // String fromSQL = "";
                                    // String firstTable = tmp[0].trim();
                                    // for (int k = 0; k < filters.length; k++) {
                                    // String str = firstTable + ".col";
                                    // if (filters[k].contains(str)) {
                                    // whereSQL += filters[k];
                                    // fromSQL += firstTable;
                                    // filters[k] = "";
                                    // }
                                    // }
                                    // for (int j = 1; j < tmp.length; j++) {
                                    // String newTable = tmp[j].trim();
                                    // for (int k = 0; k < filters.length; k++) {
                                    // String str = newTable + ".col";
                                    // if (filters[k].contains(str)) {
                                    // whereSQL += " and " + filters[k];
                                    // fromSQL += "," + newTable;
                                    // filters[k] = "";
                                    // }
                                    // }
                                    // for (int m = 0; m < j; m++) {
                                    // String oldTable = tmp[m].trim();
                                    // for (int k = 0; k < filters.length; k++) {
                                    // String s1 = newTable + ".";
                                    // String s2 = oldTable + ".";
                                    // if (filters[k].contains(s1) && filters[k].contains(s2)) {
                                    //
                                    // whereSQL += " and " + filters[k];
                                    // filters[k] = "";
                                    // }
                                    // }
                                    // }
                                    // String fullSql = "select count(*) from " + fromSQL + " where " + whereSQL;
                                    // midJoinSubQueries.add(fullSql);

                                    // }

                                } else if (dbConnection.getDatabaseBrand().equalsIgnoreCase("postgresql")) {
                                    StringBuffer analyzequery = new StringBuffer(query);
                                    analyzequery = new StringBuffer("explain analyze ").append(analyzequery);
                                    System.out.println(analyzequery);
                                    bw.write(analyzequery + "\r\n");
                                    bw.flush();
                                    rs = statement.executeQuery(analyzequery.toString());
                                    info = "";
                                    while (rs.next()) {
                                        info += rs.getString(1) + "\n";
                                    }
                                    bw.write(info + "\r\n");
                                    bw.write("\r\n");
                                    bw.flush();
                                }
                                for (int j = 0; j < midJoinSubQueries.size(); j++) {
                                    System.out.println(midJoinSubQueries.get(j));
                                    bw.write(midJoinSubQueries.get(j) + "\r\n");
                                    bw.flush();
                                    long begin = System.currentTimeMillis();
                                    rs = statement.executeQuery(midJoinSubQueries.get(j));
                                    long end = System.currentTimeMillis();
                                    long time = end - begin;
                                    while (rs.next()) {
                                        System.out.println("result: " + rs.getString(1) + " time: " + time);
                                        bw.write("result: " + rs.getString(1) + " time: " + time + "\r\n");
                                        bw.write("\r\n");
                                        bw.flush();
                                    }
                                }
                                if (times.size() == 2) {
                                    double deviation = (times.get(0) * 1.0 - times.get(1) * 1.0) / times.get(1) * 1.0
                                            * 100;
                                    bw.write("deviation: " + deviation + "\r\n");
                                    bw.flush();
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                bw.write(e.getMessage());
                                bw.flush();
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                statement.close();
                con.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void findMidJoin2(RawNode root, String[] tables, String[] filters) {
        if (root != null) {
            if (root.left != null)
                findMidJoin2(root.left, tables, filters);
            if (root.right != null)
                findMidJoin2(root.right, tables, filters);
            if (root.nodeType.toLowerCase().contains("join") || root.nodeType.toLowerCase().contains("loop")) {
                String s1 = root.left.bracketsStr;
                String s2 = root.right.bracketsStr;

                Set<String> tableSetLeft = new HashSet<>();
                Set<String> tableSetRight = new HashSet<>();
                int index1 = -1;
                int index2 = 0;
                while (index2 < s1.length()) {
                    index1 = s1.indexOf("table_", index2);
                    if (index1 == -1)
                        break;
                    index2 = s1.indexOf(".", index1);
                    tableSetLeft.add(s1.substring(index1, index2));
                }
                index1 = -1;
                index2 = 0;
                while (index2 < s2.length()) {
                    index1 = s2.indexOf("table_", index2);
                    if (index1 == -1)
                        break;
                    index2 = s2.indexOf(".", index1);
                    tableSetRight.add(s2.substring(index1, index2));
                }
                List<String> joinFilters = new ArrayList<>();
                for (String sl : tableSetLeft) {
                    for (String sr : tableSetRight) {
                        for (int k = 0; k < filters.length; k++) {
                            String sl2 = sl + ".";// table_1. 避免table_1 和table_10都匹配到table_1
                            String sr2 = sr + ".";
                            if (filters[k].contains(sl2) && filters[k].contains(sr2))
                                joinFilters.add(filters[k]);
                        }
                    }
                }
                String joinFilter = "";
                for (int i = 0; i < joinFilters.size(); i++) {
                    if (i == 0)
                        joinFilter += joinFilters.get(i);
                    else
                        joinFilter += " and " + joinFilters.get(i);
                }
                String newFilters = "";
                if (joinFilter.length() > 0)
                    newFilters = s1 + " and " + s2 + " and " + joinFilter;
                else
                    newFilters = s1 + " and " + s2;

                String tableNames = "";
                for (String s : tableSetLeft) {
                    tableNames += "," + s;
                }
                for (String s : tableSetRight) {
                    tableNames += "," + s;
                }

                root.setBracketsStr(newFilters);
                String sql = "select count(*) from " + tableNames.substring(1) + " where " + newFilters;
                midJoinSubQueries.add(sql);

            } else if (root.nodeType.toLowerCase().contains("scan") || root.nodeType.toLowerCase().contains("get")) {
                String tableName = root.tableName;
                for (int k = 0; k < filters.length; k++) {
                    String str = tableName + ".col";
                    if (filters[k].contains(str)) {
                        root.setBracketsStr(filters[k]);
                    }
                }
            } else {
                if (root.left != null)
                    root.setBracketsStr(root.left.bracketsStr);
                else
                    root.setBracketsStr(root.right.bracketsStr);

            }
        }
    }

    // tidb一开始用的这个。。。脑子抽抽的产物，写这么复杂干什么
    public static void findMidJoin(RawNode root, String[] tables, String[] filters, String dbType) {
        if (root != null) {
            if (root.left != null)
                findMidJoin(root.left, tables, filters, dbType);
            if (root.right != null)
                findMidJoin(root.right, tables, filters, dbType);
            if (root.nodeType.toLowerCase().contains("join") || root.nodeType.toLowerCase().contains("loop")) {
                String info = root.operatorInfo;

                int index1 = -1;
                int index2 = 0;
                String joinFilter = "";
                List<String> eqs = new ArrayList<>();
                if (dbType.equalsIgnoreCase("tidb")) {
                    while (index2 < info.length()) {
                        index1 = info.indexOf("eq(", index2);
                        if (index1 == -1)
                            break;
                        index2 = info.indexOf(")", index1);
                        String eq = info.substring(index1 + 3, index2);
                        eqs.add(eq.replace(",", "="));
                    }
                } else if (dbType.equalsIgnoreCase("oceanbase")) {
                    index1 = info.indexOf("equal_conds([");// 看一下多个连接条件怎么表示的
                    index2 = info.indexOf("]", index1);
                    info.substring(index1 + 13, index2);
                }
                for (int i = 0; i < eqs.size(); i++) {
                    if (i == 0)
                        joinFilter += eqs.get(i);
                    else
                        joinFilter += " and " + eqs.get(i);
                }

                String oldFilters = "";
                String newTable1 = "new1";
                String newTable2 = "new2";
                if (root.left != null && root.left.nodeType.toLowerCase().contains("join")) {
                    oldFilters = root.left.bracketsStr;
                    RawNode now = root.right;
                    while (now != null) {
                        if (now.nodeType.toLowerCase().contains("scan")) {
                            List<List<String>> matches = matchPattern(TABLE_NAME, now.operatorInfo);
                            newTable1 = matches.get(0).get(1);
                        }
                        now = now.left;
                    }
                } else if (root.right != null && root.right.nodeType.toLowerCase().contains("join")) {
                    oldFilters = root.right.bracketsStr;
                    RawNode now = root.left;
                    while (now != null) {
                        if (now.nodeType.toLowerCase().contains("scan")) {
                            List<List<String>> matches = matchPattern(TABLE_NAME, now.operatorInfo);
                            newTable1 = matches.get(0).get(1);
                        }
                        now = now.left;
                    }
                } else {
                    RawNode now = root.left;
                    while (now != null) {
                        // System.out.println("left: " + now.nodeType);
                        if (now.nodeType.toLowerCase().contains("scan")) {
                            List<List<String>> matches = matchPattern(TABLE_NAME, now.operatorInfo);
                            newTable1 = matches.get(0).get(1);
                        }
                        now = now.left;
                    }
                    now = root.right;
                    while (now != null) {
                        // System.out.println("right: " + now.nodeType);
                        if (now.nodeType.toLowerCase().contains("scan")) {
                            List<List<String>> matches = matchPattern(TABLE_NAME, now.operatorInfo);
                            newTable2 = matches.get(0).get(1);
                        }
                        now = now.left;
                    }
                }

                String tableNames = "";
                Set<String> tableSet = new HashSet<>();
                index1 = -1;
                index2 = 0;
                while (index2 < oldFilters.length()) {
                    index1 = oldFilters.indexOf("table_", index2);
                    if (index1 == -1)
                        break;
                    index2 = oldFilters.indexOf(".", index1);
                    tableSet.add(oldFilters.substring(index1, index2));
                }
                tableSet.add(newTable1);
                if (!newTable2.equals("new2"))
                    tableSet.add(newTable2);
                String tablefilter = "";
                for (String s : tableSet) {
                    tableNames += "," + s;
                }
                for (int k = 0; k < filters.length; k++) {
                    String str1 = newTable1 + ".col";
                    String str2 = newTable2 + ".col";
                    if (filters[k].contains(str1) || filters[k].contains(str2)) {
                        tablefilter += " and " + filters[k];
                    }
                }
                if (tablefilter.length() > 0)
                    tablefilter = tablefilter.substring(4);
                String newFilters = null;
                if (oldFilters.length() > 0)
                    newFilters = oldFilters + " and " + joinFilter + " and " + tablefilter;
                else if (joinFilter.length() > 0)
                    newFilters = joinFilter + " and " + tablefilter;
                else
                    newFilters = tablefilter;
                root.setBracketsStr(newFilters);
                String sql = "select count(*) from " + tableNames.substring(1) + " where " + newFilters;
                midJoinSubQueries.add(sql);
            }

        }
    }

    public static RawNode buildRawNodeTree_Tidb(List<String[]> queryPlan) {
        Deque<Pair<Integer, RawNode>> pStack = new ArrayDeque<>();
        List<List<String>> matches = matchPattern(PLAN_ID, queryPlan.get(0)[0]);// 匹配出HashAgg_156
        String nodeType = matches.get(0).get(0).split("_")[0];
        String[] subQueryPlanInfo = extractSubQueryPlanInfo(queryPlan.get(0));
        // String[]{"id", "operator info", "estRows", "access object"}
        String planId = matches.get(0).get(0), operatorInfo = subQueryPlanInfo[1], executionInfo = subQueryPlanInfo[2];
        // if access object == null subqueryplan[1]=operator info else
        // subqueryplan[1]=access object+operator info subQueryPlanInfo[2]=rows:estrows
        Matcher matcher;
        int rowCount = (matcher = ROW_COUNTS.matcher(executionInfo)).find()
                ? Integer.parseInt(matcher.group(0).split(":")[1])
                : 0;// 得到rows的行数
        RawNode rawNodeRoot = new RawNode(planId, null, null, nodeType, operatorInfo, rowCount, 0, "");
        RawNode rawNode;
        pStack.push(Pair.of(0, rawNodeRoot));// 把根节点放入双端队列
        for (String[] subQueryPlan : queryPlan.subList(1, queryPlan.size())) {
            subQueryPlanInfo = extractSubQueryPlanInfo(subQueryPlan);
            matches = matchPattern(PLAN_ID, subQueryPlanInfo[0]);
            planId = matches.get(0).get(0);
            operatorInfo = subQueryPlanInfo[1];
            executionInfo = subQueryPlanInfo[2];
            nodeType = matches.get(0).get(0).split("_")[0];
            try {
                rowCount = (matcher = ROW_COUNTS.matcher(executionInfo)).find()
                        ? Integer.parseInt(matcher.group(0).split(":")[1])
                        : 0;
            } catch (ArrayIndexOutOfBoundsException e) {
                System.out.println("错误！");
            }
            rawNode = new RawNode(planId, null, null, nodeType, operatorInfo, rowCount, 0, "");
            int level = (subQueryPlan[0].split("─")[0].length() + 1) / 2;// 获取层级
            while (!pStack.isEmpty() && pStack.peek().getKey() > level) {
                pStack.pop(); // pop直到找到同一个层级的节点
            }
            if (pStack.isEmpty()) {
                System.out.println("pStack不应为空");
            }

            if (pStack.peek().getKey().equals(level)) {
                pStack.pop();
                if (pStack.isEmpty()) {
                    System.out.println("pStack不应为空");
                }
                pStack.peek().getValue().right = rawNode;
            } else {
                pStack.peek().getValue().left = rawNode;
            }
            pStack.push(Pair.of(level, rawNode));// 放入双端队列
        }
        return rawNodeRoot;
    }

    public static RawNode buildRawNodeTree_Oceanbase(List<String[]> queryPlan) {
        Deque<Pair<Integer, RawNode>> pStack = new ArrayDeque<>();
        // String[]:id nodeType table est_rows operator_info
        String nodeType = queryPlan.get(0)[1].trim();
        String planId = queryPlan.get(0)[1].trim() + "_" + queryPlan.get(0)[0].trim();
        String operatorInfo = queryPlan.get(0)[4].trim();
        String tableName = queryPlan.get(0)[2].trim();
        long rows = Long.parseLong(queryPlan.get(0)[3].trim());
        RawNode rawNodeRoot = new RawNode(planId, null, null, nodeType, operatorInfo, rows, 0, tableName);
        RawNode rawNode;
        pStack.push(Pair.of(0, rawNodeRoot));// 把根节点放入双端队列
        for (String[] subQueryPlan : queryPlan.subList(1, queryPlan.size())) {
            nodeType = subQueryPlan[1].trim();
            planId = subQueryPlan[1].trim() + "_" + subQueryPlan[0].trim();
            operatorInfo = subQueryPlan[4].trim();
            rows = Long.parseLong(subQueryPlan[3].trim());
            tableName = subQueryPlan[2].trim();

            rawNode = new RawNode(planId, null, null, nodeType, operatorInfo, rows, 0, tableName);
            // if(nodeType.toLowerCase().contains("scan") ||
            // nodeType.toLowerCase().contains("get")) {
            // String selectId = "SELECTION_" + subQueryPlan[0].trim();
            // RawNode selectNode = new RawNode(selectId,
            // rawNode,null,"selection",operatorInfo,rows,0,tableName);
            // rawNode = selectNode;
            // }
            int index = 0;
            for (int i = 0; i < subQueryPlan[1].length(); i++) {
                if (subQueryPlan[1].charAt(i) != ' ') {
                    index = i;
                    break;
                }
            }
            int level = index;// 获取层级
            while (!pStack.isEmpty() && pStack.peek().getKey() > level) {
                pStack.pop(); // pop直到找到同一个层级的节点
            }
            if (pStack.isEmpty()) {
                System.out.println("pStack不应为空");
            }

            if (pStack.peek().getKey().equals(level)) {
                pStack.pop();
                if (pStack.isEmpty()) {
                    System.out.println("pStack不应为空");
                }
                pStack.peek().getValue().right = rawNode;
            } else {
                pStack.peek().getValue().left = rawNode;
            }
            pStack.push(Pair.of(level, rawNode));// 放入双端队列
        }
        return rawNodeRoot;
    }

    // PG过滤100ms一下的查询;OB过滤小于500ms和大于30s的查询;tidb过滤<300ms >30s;mysql<500ms,30s
    public static void filterQuerys(String type) {
        try {
            List<DBConnection> conns = DatabaseConnection.getDatabaseConnection(true);

            for (DBConnection dbConnection : conns) {
                Connection con = dbConnection.getDbConn();
                Statement statement = con.createStatement();
                try {
                    String queryTXT = Configurations.getQueryOutputDir() + File.separator + type
                            + ".txt";
                    String outputFileName = Configurations.getQueryOutputDir() + File.separator + type
                            + "_1.txt";
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(new FileInputStream(new File(queryTXT)), StandardCharsets.UTF_8));
                    String lineTxt;
                    while ((lineTxt = br.readLine()) != null) {
                        // 过滤空行
                        if (lineTxt.matches("[\\s]*") || lineTxt.matches("[ ]*##[\\s\\S]*")) {
                            continue;
                        }
                        // lineTxt = "select /*+query_timeout(30000000) */ " + lineTxt.substring(6);
                        try {
                            statement.execute("set max_execution_time = 30000");
                            long startTime = System.currentTimeMillis();
                            statement.executeQuery(lineTxt);
                            long endTime = System.currentTimeMillis();
                            long reorderTime = endTime - startTime;
                            System.out.println("time: " + reorderTime);

                            if (reorderTime < 500 || reorderTime > 30000)
                                continue;

                            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                                    new FileOutputStream(outputFileName, true), Configurations.getEncodeType()))) {
                                bw.write(lineTxt + "\r\n");
                                bw.flush();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        } catch (Exception e) {
                            System.out.println("time: filter");
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void ttest(String type) {
        try {
            String queryTXT = Configurations.getQueryOutputDir() + File.separator + type
                    + ".txt";
            String outputFileName = Configurations.getQueryOutputDir() + File.separator + type
                    + "_1.txt";
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(new File(queryTXT)), StandardCharsets.UTF_8));
            String lineTxt;
            while ((lineTxt = br.readLine()) != null) {
                // 过滤空行
                if (lineTxt.matches("[\\s]*") || lineTxt.matches("[ ]*##[\\s\\S]*")) {
                    continue;
                }
                System.out.println(lineTxt);
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(outputFileName, true), Configurations.getEncodeType()))) {
                    bw.write(lineTxt + "\r\n");
                    bw.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean isLeftDeepTree(String joinOrder, List<Table> tables) {
        int num = tables.size() - 1;
        for (int i = 0; i < num; i++) {
            if (joinOrder.charAt(i) != '(')
                return false;
        }
        return true;
    }

    public static void execQuerysTest() {
        String sql1 = "select /*+USE_PLAN_CACHE(NONE) LEADING(table_0, table_9, table_4, table_5)*/   count(*) as result from table_0, table_9, table_4, table_5 where  table_4.col_8 <= 1922008581 and table_5.col_0 < -348809115.7006844609905468 and table_0.col_7 > -4255518.1398595209726152 and table_9.col_5 < -211145821.32382347996192376 and table_4.fk_3 = table_5.primaryKey and table_4.fk_11 = table_0.primaryKey and table_9.fk_2 = table_0.primaryKey   ";
        String sql2 = "select /*+USE_PLAN_CACHE(NONE) LEADING(table_4, table_5, table_0, table_9)*/   count(*) as result from table_0, table_9, table_4, table_5 where  table_4.col_8 <= 1922008581 and table_5.col_0 < -348809115.7006844609905468 and table_0.col_7 > -4255518.1398595209726152 and table_9.col_5 < -211145821.32382347996192376 and table_4.fk_3 = table_5.primaryKey and table_4.fk_11 = table_0.primaryKey and table_9.fk_2 = table_0.primaryKey   ";
        String sql3 = "select /*+ USE_PLAN_CACHE(NONE) LEADING(table_15, table_8, table_1, table_11, table_12)*/   count(*) as result from table_15, table_8, table_1, table_11, table_12 where  table_8.col_7 > -9394 and table_15.col_17 >= 108012815.6454908348582152 and table_12.col_10 > -391930542.693070381306296 and table_1.col_11 <= 521088377.6015854040982113 and table_11.col_10 <= 1324348041.7947881134823990 and table_8.fk_3 = table_15.primaryKey and table_12.fk_11 = table_15.primaryKey and table_1.fk_1 = table_8.primaryKey and table_11.fk_1 = table_15.primaryKey   ";
        String sql4 = "select /*+ USE_PLAN_CACHE(NONE) LEADING(table_8, table_15, table_1, table_11, table_12)*/   count(*) as result from table_15, table_8, table_1, table_11, table_12 where  table_8.col_7 > -9394 and table_15.col_17 >= 108012815.6454908348582152 and table_12.col_10 > -391930542.693070381306296 and table_1.col_11 <= 521088377.6015854040982113 and table_11.col_10 <= 1324348041.7947881134823990 and table_8.fk_3 = table_15.primaryKey and table_12.fk_11 = table_15.primaryKey and table_1.fk_1 = table_8.primaryKey and table_11.fk_1 = table_15.primaryKey   ";
        String sql5 = "select /*+USE_PLAN_CACHE(NONE) LEADING(table_6, table_5, table_1, table_10, table_0)*/  table_10.col_2%5431 as result_0, table_5.col_6+-2666 as result_1, table_10.fk_2+-8486 as result_2, table_0.col_8+8137 as result_3, table_6.col_7--6813 as result_4, table_0.col_13-6238 as result_5, table_10.fk_3 as result_6, table_1.col_9 as result_7, table_5.col_0*-7698 as result_8, table_5.col_9*-4881 as result_9, table_10.col_9%-9905 as result_10, table_6.col_4-2836 as result_11, table_5.col_9 as result_12, table_5.col_0+3946 as result_13, table_10.col_7-4757 as result_14, table_1.col_15-8901 as result_15, table_1.fk_1-6901 as result_16, table_0.col_5--6313 as result_17, table_0.col_14*156 as result_18, table_1.primaryKey%7211 as result_19, table_10.col_13+-6562 as result_20, table_5.col_13-1186 as result_21, table_0.col_13*7668 as result_22, table_6.col_9 as result_23, table_1.col_2*-3017 as result_24, table_0.col_1*4648 as result_25, table_1.col_13+-6867 as result_26, table_1.col_7-2221 as result_27, table_10.col_8-3992 as result_28, table_10.col_5 as result_29, table_1.col_12+8033 as result_30, table_6.fk_2%-8295 as result_31, table_5.col_7%6068 as result_32, table_10.col_11+-8649 as result_33, table_10.col_9+5619 as result_34, table_0.fk_0 as result_35, table_1.col_11+6685 as result_36, table_10.col_5 as result_37, table_0.col_4+-1670 as result_38, table_5.col_5 as result_39, table_0.col_0-9720 as result_40, table_1.col_0%-4827 as result_41, table_0.col_10%-880 as result_42, table_6.col_4--8616 as result_43, table_6.fk_1*8835 as result_44, table_1.fk_2 as result_45, table_10.fk_6 as result_46, table_6.col_0+821 as result_47, table_5.col_16*-2202 as result_48, table_0.col_6--1484 as result_49, table_1.col_5 as result_50, table_5.col_1%8792 as result_51, table_10.col_7%-8367 as result_52, table_0.fk_0%-1811 as result_53, table_10.col_8 as result_54, table_10.fk_3 as result_55, table_1.col_14%-6828 as result_56, table_6.col_8 as result_57, table_1.fk_0+-4961 as result_58, table_10.col_8-440 as result_59, table_1.col_11*-8993 as result_60, table_0.col_15+492 as result_61, table_10.col_7*-3335 as result_62, table_6.col_4%4703 as result_63, table_10.col_9%5870 as result_64, table_6.fk_3 as result_65, table_5.col_16+-736 as result_66, table_10.col_3+4510 as result_67, table_10.col_3*-5424 as result_68, table_0.fk_1*-1367 as result_69, table_10.col_6 as result_70, table_5.col_7*-1552 as result_71, table_10.col_4+-7786 as result_72, table_0.col_14+-7157 as result_73  from table_1, table_5, table_6, table_10, table_0 where  table_6.col_3 >= -1107 and table_5.col_13 >= 260508053.11191856139954140 and table_10.col_7 > 220432403.23232200518001280 and table_0.col_11 > -582437089.2063137679778940 and table_1.col_12 > -66865252.44736897726868296 and table_6.fk_1 = table_5.primaryKey and table_10.fk_1 = table_5.primaryKey and table_0.fk_0 = table_5.primaryKey and table_1.fk_1 = table_5.primaryKey   ";
        String sql6 = "explain select /*+USE_PLAN_CACHE(NONE) LEADING(table_6, table_5, table_1, table_10, table_0)*/  table_10.col_2%5431 as result_0, table_5.col_6+-2666 as result_1, table_10.fk_2+-8486 as result_2, table_0.col_8+8137 as result_3, table_6.col_7--6813 as result_4, table_0.col_13-6238 as result_5, table_10.fk_3 as result_6, table_1.col_9 as result_7, table_5.col_0*-7698 as result_8, table_5.col_9*-4881 as result_9, table_10.col_9%-9905 as result_10, table_6.col_4-2836 as result_11, table_5.col_9 as result_12, table_5.col_0+3946 as result_13, table_10.col_7-4757 as result_14, table_1.col_15-8901 as result_15, table_1.fk_1-6901 as result_16, table_0.col_5--6313 as result_17, table_0.col_14*156 as result_18, table_1.primaryKey%7211 as result_19, table_10.col_13+-6562 as result_20, table_5.col_13-1186 as result_21, table_0.col_13*7668 as result_22, table_6.col_9 as result_23, table_1.col_2*-3017 as result_24, table_0.col_1*4648 as result_25, table_1.col_13+-6867 as result_26, table_1.col_7-2221 as result_27, table_10.col_8-3992 as result_28, table_10.col_5 as result_29, table_1.col_12+8033 as result_30, table_6.fk_2%-8295 as result_31, table_5.col_7%6068 as result_32, table_10.col_11+-8649 as result_33, table_10.col_9+5619 as result_34, table_0.fk_0 as result_35, table_1.col_11+6685 as result_36, table_10.col_5 as result_37, table_0.col_4+-1670 as result_38, table_5.col_5 as result_39, table_0.col_0-9720 as result_40, table_1.col_0%-4827 as result_41, table_0.col_10%-880 as result_42, table_6.col_4--8616 as result_43, table_6.fk_1*8835 as result_44, table_1.fk_2 as result_45, table_10.fk_6 as result_46, table_6.col_0+821 as result_47, table_5.col_16*-2202 as result_48, table_0.col_6--1484 as result_49, table_1.col_5 as result_50, table_5.col_1%8792 as result_51, table_10.col_7%-8367 as result_52, table_0.fk_0%-1811 as result_53, table_10.col_8 as result_54, table_10.fk_3 as result_55, table_1.col_14%-6828 as result_56, table_6.col_8 as result_57, table_1.fk_0+-4961 as result_58, table_10.col_8-440 as result_59, table_1.col_11*-8993 as result_60, table_0.col_15+492 as result_61, table_10.col_7*-3335 as result_62, table_6.col_4%4703 as result_63, table_10.col_9%5870 as result_64, table_6.fk_3 as result_65, table_5.col_16+-736 as result_66, table_10.col_3+4510 as result_67, table_10.col_3*-5424 as result_68, table_0.fk_1*-1367 as result_69, table_10.col_6 as result_70, table_5.col_7*-1552 as result_71, table_10.col_4+-7786 as result_72, table_0.col_14+-7157 as result_73  from table_1, table_5, table_6, table_10, table_0 where  table_6.col_3 >= -1107 and table_5.col_13 >= 260508053.11191856139954140 and table_10.col_7 > 220432403.23232200518001280 and table_0.col_11 > -582437089.2063137679778940 and table_1.col_12 > -66865252.44736897726868296 and table_6.fk_1 = table_5.primaryKey and table_10.fk_1 = table_5.primaryKey and table_0.fk_0 = table_5.primaryKey and table_1.fk_1 = table_5.primaryKey   ";
        String[] sqls = { sql1, sql2, sql3, sql4, sql5, sql6 };

        try {
            List<DBConnection> dbConnections = DatabaseConnection.getDatabaseConnection(false);
            for (int m = 0; m < dbConnections.size(); m++) {
                RecordLog.recordLog(LogLevelConstant.INFO, "连接数据库" + (m + 1));
                Connection con = dbConnections.get(m).getDbConn();
                for (int i = 0; i < 2; i++) {
                    Statement statement1 = con.createStatement();
                    List<Long> times = new LinkedList<>();
                    try {
                        // System.out.println(sqls[i*2]);
                        // statement1.setQueryTimeout(1);

                        for (int j = 0; j < 7; j++) {
                            long startTime = System.currentTimeMillis();
                            ResultSet resultSet = statement1.executeQuery(sqls[i]);
                            long endTime = System.currentTimeMillis();
                            long reorderTime = endTime - startTime;
                            // pairs.add(Pair.of(Integer.toString(i), reorderTime));
                            times.add(reorderTime);
                            System.out.println(i + " " + reorderTime + "ms");
                        }
                        // System.out.println(sqls[i]);
                        // ResultSet resultSet1 = statement1.executeQuery(sqls[i]);
                        // String info = "";
                        // while (resultSet1.next()) {
                        // info += resultSet1.getString(1);
                        // }
                        // System.out.println(info);

                    } catch (Exception e) {
                        System.out.println(i + " Timeout");
                    }
                    Collections.sort(times);
                    long sum = 0;
                    for (int k = 1; k < 6; k++)
                        sum += times.get(k);
                    System.out.println("平均：" + sum / 5);
                    statement1.close();
                }
                con.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String[] extractSubQueryPlanInfo(String[] data) {//

        // String[]{"id", "operator info", "estRows", "access object"}
        String[] ret = new String[3];
        ret[0] = data[0];// id
        ret[1] = data[3].isEmpty() ? data[1] : String.format("%s,%s", data[3], data[1]);// 判断access object是否为空
        ret[2] = "rows:" + data[2];
        return ret;

    }

    public static List<List<String>> matchPattern(Pattern pattern, String str) {
        Matcher matcher = pattern.matcher(str);
        List<List<String>> ret = new ArrayList<>();
        while (matcher.find()) {
            List<String> groups = new ArrayList<>();
            for (int i = 0; i <= matcher.groupCount(); i++) {
                groups.add(matcher.group(i));
            }
            ret.add(groups);
        }

        return ret;
    }
}
