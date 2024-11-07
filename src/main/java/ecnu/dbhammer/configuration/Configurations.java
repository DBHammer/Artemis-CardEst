package ecnu.dbhammer.configuration;


import com.google.ortools.Loader;
import ecnu.dbhammer.databaseAdapter.DBConnection;
import ecnu.dbhammer.utils.Histogram;
import ecnu.dbhammer.utils.HistogramItem;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
/**
 * @author xiangzhaokun
 * @ClassName Configurations.java
 * @Description 整个工具的配置文件类，用于用户自定义配置文件，提供测试的灵活性
 * @createTime 2020年10月01日 12:26:00
 */
public class Configurations {

    // 数据库实例个数，针对一个数据库实例会生成一组测试案例
    // 考虑到每个测试数据库实例可能会很大，测试数据准备时间往往很长，故针对每个测试数据库，我们会生成一组测试案例
    // 针对AP负载来说，我们的测试任务是验证复杂查询负载返回结果集的正确性，一个测试案例指一个查询SQL。

    // 针对每个数据库实例，生成的测试案例个数
    private static int queryNumPerSchema;//一个测试案例指的是一个查询SQL

    private static int nodeNum;//分布式环境下节点数,目前用不到这个

    private static int solverThread; //求解连接结果时所用的线程数

    private static double sumProbability;

    private static double avgProbability;

    private static double countProbability;

    private static double maxProbability;

    private static double minProbability;

    private static boolean savaArchive;
    private static boolean isGenerateOnly;


    private static boolean drawQuery;
    private static double pureColumnProbability;
    private static double singleColumnProbability;
    private static double multiColumnProbability;

    private static double subQueryProbability;


    private static String testDatabaseName;

    private static List<DBConnection> DBConnections; //连接的多个数据库

    private static int tableNumPerQuery;//生成每个Query的涉及的表数目

    private static int threadNumPerNode;//分布式环境下，每个节点生成数据的线程

    // 每个数据库实例中的数据表个数，这是一个直方图配置信息
    //比如10～20的概率为0.6 20~30的概率为0.4
    private static Histogram tableNumHistogram = null;

    // 数据表的大小配置信息，通过概率随机确定
    private static Histogram tableSizeHistogram = null;

    // 单个数据表中的外键个数配置信息，通过概率随机确定
    private static Histogram foreignKeyNumHistogram = null;

    // 每个数据表中的属性个数，该属性个数不包含主键和外键
    private static Histogram columnNumHistogram = null;

    // 每种数据类型出现的概率
    // 目前主持的数据类型有：整型、实型、小数、字符串、布尔型、日期
    private static Map<String, Float> dataType2OccurProbability = null;

    private static Map<String, Map<String,Float>> dataDistributionProbability = null;

    private static double zipSkewness = 0.0;

    // 如果是小数类型，其精度p、s值的范围
    private static int maxP4Decimal;
    private static int maxS4Decimal;

    // 字符串属性长度的配置信息
    private static Histogram varcharLengthHistogram = null;


    // 每个字符串属性在生成时，需要一个种子字符串集合，下面配置项为种子字符串的个数
    private static int seedStringNum;

    // 支持二级索引的数据类型
    private static Set<String> supportIndexDataTypes = null;

    // 目前暂不考虑二级索引类型，如BTree、Hash、etc
    // 每个属性上建立二级索引的概率，不同数据类型的属性相应的概率可能有差异
    private static Map<String, Float> dataType2CreateIndexProbability = null;

    // 在一个属性组上建立二级索引。针对一个数据表，建立多属性二级索引的个数
    private static Histogram multiColumnIndexNumHistogram = null;
    private static int indexMaxColumnNum;

    // 是否需要显示地在外键上建立二级索引
    private static boolean isCreateIndex4ForeignKey;

    // TODO 数据生成相关配置项还需要补充...
    // 如：生成数据输出目录，文件编码格式等
    private static String dataoutputDir = null;

    private static String queryOutputDir = null;

    private static String schemaOutputDir = null;

    private static String reportOutputDir = null;

    private static String calculateResultOutputDir = null;

    private static String executeResultOutputDir = null;

    private static String executeTimeOutputDir = null;

    private static String joinOrderRankDir = null;

    private static String intermediateCardDir = null;

    private static String generateExactCard = null;


    private static String encodeType = null;


    private static boolean cardOptimalJoinOrderEval;

    private static boolean joinOrderRank;

    private static boolean getAllCard;

    private static boolean dataAndQueryGen;

    private static boolean isGeneSchema;

    private static boolean isGeneData;

    private static boolean isGeneQuery;

    private static boolean isLoadData;

    private static String schemaFile;

    private static int cardLowerBound;

    //  query生成需要的配置项



    // 每条查询中最多包含的表数目
    private static int maxTableNum;

    // 查询中用做group by列的最大数目
    private static int maxGroupNum;

    // 查询中用作order by列的最大数目
    private static int maxOrderNum;

    // 查询中包含的最大聚合数目
    private static int maxAggreNum;
    //private static Histogram aggregationNumHistogram = null;
    //private static Map<String,Float> aggregationType2OccourProbability = null;

    private static boolean correctnessVarity;

    private static boolean executeTimeCompare;

    private static boolean randomGeneQueryGraphShape;

    private static Map<String, Float> queryGraphShape2OccurProbability;

    public static ExecutorService service;

    public static void loadConfigurations(String fileName) {
        SAXReader reader = new SAXReader();
        try {
            Document document;
            if(fileName == null)
                document = reader.read(new File("./src/main/resources/config/apConfig.xml"));
            else
                document = reader.read(new File(fileName));
            Configurations.queryNumPerSchema = Integer.parseInt(document.valueOf("testCaseGenerator/queryNumPerSchema").trim());
            Configurations.tableNumPerQuery = Integer.parseInt(document.valueOf("testCaseGenerator/tableNumPerQuery").trim());
            Configurations.threadNumPerNode = Integer.parseInt(document.valueOf("testCaseGenerator/threadNumPerNode").trim());
            Configurations.nodeNum = Integer.parseInt(document.valueOf("testCaseGenerator/nodeNum").trim());

            Configurations.solverThread = Integer.parseInt(document.valueOf("testCaseGenerator/solverThread").trim());
            //数据库配置

            Configurations.testDatabaseName = document.valueOf("testCaseGenerator/testDatabaseName").trim();

            List<Node> nodes = null;
            nodes = document.selectNodes("testCaseGenerator/databaseConfig");
            DBConnections = new ArrayList<>();
            for (Node node : nodes) {
                if(node.valueOf("databaseBrand").trim().equalsIgnoreCase("oceanbase")){
                    DBConnections.add(new DBConnection(
                            node.valueOf("databaseBrand").trim(),
                            node.valueOf("databaseIP").trim(),
                            node.valueOf("databasePort").trim(),
                            node.valueOf("dbUsername").trim(),
                            node.valueOf("dbPassword").trim(),
                            node.valueOf("serverHost").trim(),
                            node.valueOf("serverPort").trim(),
                            node.valueOf("serverUser").trim(),
                            node.valueOf("serverPassword").trim(),
                            node.valueOf("uploadDir").trim()));
                }else {
                    DBConnections.add(new DBConnection(
                            node.valueOf("databaseBrand").trim(),
                            node.valueOf("databaseIP").trim(),
                            node.valueOf("databasePort").trim(),
                            node.valueOf("dbUsername").trim(),
                            node.valueOf("dbPassword").trim(),
                            "","","","",node.valueOf("uploadDir").trim()));
                }
            }

            //聚集函数概率配置
            Configurations.sumProbability = Double.parseDouble(document.valueOf("testCaseGenerator/sumProbability").trim());
            Configurations.avgProbability = Double.parseDouble(document.valueOf("testCaseGenerator/avgProbability").trim());
            Configurations.countProbability = Double.parseDouble(document.valueOf("testCaseGenerator/countProbability").trim());
            Configurations.maxProbability = Double.parseDouble(document.valueOf("testCaseGenerator/maxProbability").trim());
            Configurations.minProbability = Double.parseDouble(document.valueOf("testCaseGenerator/minProbability").trim());

            //生成Query的Filter配置，配置生成一个列、还是多个列运算
            Configurations.pureColumnProbability = Double.parseDouble(document.valueOf("testCaseGenerator/pureColumnProbability").trim());
            Configurations.singleColumnProbability = Double.parseDouble(document.valueOf("testCaseGenerator/singleColumnProbability").trim());
            Configurations.multiColumnProbability = Double.parseDouble(document.valueOf("testCaseGenerator/multiColumnProbability").trim());
            Configurations.subQueryProbability = Double.parseDouble(document.valueOf("testCaseGenerator/subQueryProbability").trim());


            if (Configurations.sumProbability + Configurations.avgProbability + Configurations.countProbability
                    + Configurations.maxProbability + Configurations.minProbability > 1.0 ){
                throw new Exception("聚合操作中的概率配置有误，大于1！");
            }

            Configurations.savaArchive = Boolean.parseBoolean(document.valueOf("testCaseGenerator/saveArchive").trim());

            Configurations.isGenerateOnly = Boolean.parseBoolean(document.valueOf("testCaseGenerator/generateOnly").trim());

            Configurations.drawQuery = Boolean.parseBoolean(document.valueOf("testCaseGenerator/drawQuery").trim());

            Configurations.getAllCard = Boolean.parseBoolean(document.valueOf("testCaseGenerator/getIntermediateCard").trim());


            List<HistogramItem> items = null;

            nodes = document.selectNodes("testCaseGenerator/tableNumHistogram/HistogramItem");
            items = new ArrayList<>();
            for (Node node : nodes) {
                items.add(new HistogramItem(
                        Double.parseDouble(node.valueOf("minValue")),
                        Double.parseDouble(node.valueOf("maxValue")),
                        Double.parseDouble(node.valueOf("ratio"))));
            }
            Configurations.tableNumHistogram = new Histogram(items);
            if (!(Configurations.tableNumHistogram.getProbabilitySum() > 0.999999999
                    && Configurations.tableNumHistogram.getProbabilitySum() < 1.000000001)) {
                throw new Exception("tableNumHistogram配置项有误，概率和不为1！");
            }

            nodes = document.selectNodes("testCaseGenerator/tableSizeHistogram/HistogramItem");
            items = new ArrayList<>();
            for (Node node : nodes) {
                items.add(new HistogramItem(
                        Double.parseDouble(node.valueOf("minValue")),
                        Double.parseDouble(node.valueOf("maxValue")),
                        Double.parseDouble(node.valueOf("ratio"))));
            }
            Configurations.tableSizeHistogram = new Histogram(items);
            if (!(Configurations.tableSizeHistogram.getProbabilitySum() > 0.999999999
                    && Configurations.tableSizeHistogram.getProbabilitySum() < 1.000000001)) {
                throw new Exception("tableSizeHistogram配置项有误，概率和不为1！");
            }



            nodes = document.selectNodes("testCaseGenerator/columnNumHistogram/HistogramItem");
            items = new ArrayList<>();
            for (Node node : nodes) {
                items.add(new HistogramItem(
                        Double.parseDouble(node.valueOf("minValue")),
                        Double.parseDouble(node.valueOf("maxValue")),
                        Double.parseDouble(node.valueOf("ratio"))));
            }
            Configurations.columnNumHistogram = new Histogram(items);
            if (!(Configurations.columnNumHistogram.getProbabilitySum() > 0.999999999
                    && Configurations.columnNumHistogram.getProbabilitySum() < 1.000000001)) {
                throw new Exception("columnNumHistogram配置项有误，概率和不为1！");
            }



            nodes = document.selectNodes("testCaseGenerator/dataType2OccurProbability/item");
            Configurations.dataType2OccurProbability = new HashMap<>();
            double dataType2OccurProbabilitySum = 0;
            for (Node node : nodes) {
                dataType2OccurProbability.put(node.valueOf("dataType").trim(), Float.parseFloat(node.valueOf("probability")));
                dataType2OccurProbabilitySum += Double.parseDouble(node.valueOf("probability"));
            }
            if (!(dataType2OccurProbabilitySum > 0.999999999 && dataType2OccurProbabilitySum < 1.000000001)) {
                throw new Exception("dataType2OccurProbability配置项有误，概率和不为1！");
            }

            Configurations.zipSkewness = Double.parseDouble(document.valueOf("testCaseGenerator/zipSkewness").trim());

            Configurations.randomGeneQueryGraphShape = Boolean.parseBoolean(document.valueOf("testCaseGenerator/randomGeneQueryGraphShape").trim());
            if(!Configurations.randomGeneQueryGraphShape) {
                nodes = document.selectNodes("testCaseGenerator/queryGraphShape2OccurProbability/item");
                Configurations.queryGraphShape2OccurProbability = new HashMap<>();
                double queryGraphShape2OccurProbabilitySum = 0;
                for (Node node : nodes) {
                    queryGraphShape2OccurProbability.put(node.valueOf("queryGraphShape").trim(), Float.parseFloat(node.valueOf("probability")));
                    queryGraphShape2OccurProbabilitySum += Double.parseDouble(node.valueOf("probability"));
                }
                if (!(queryGraphShape2OccurProbabilitySum > 0.999999999 && queryGraphShape2OccurProbabilitySum < 1.000000001)) {
                    throw new Exception("queryGraphShape2OccurProbability配置项有误，概率和不为1！");
                }
            }

            nodes = document.selectNodes("testCaseGenerator/dataDistributionProbability/item");
            Configurations.dataDistributionProbability = new HashMap<>();
            for (Node node : nodes) {

                HashMap<String, Float> singleDistributionProbability = new HashMap<>();
                singleDistributionProbability.put("uniform",Float.parseFloat(node.valueOf("uniformProbability")));
                singleDistributionProbability.put("zipfian",Float.parseFloat(node.valueOf("zipfianProbability")));
                double sum = 0;
                sum = Float.parseFloat(node.valueOf("uniformProbability")) + Float.parseFloat(node.valueOf("zipfianProbability"));
                if (!(sum > 0.999999999 && sum < 1.000000001)){
                    throw new Exception(node.valueOf("dataType")+"的数据分布配置项有误，概率和不为1！");
                }
                dataDistributionProbability.put(node.valueOf("dataType").trim(),singleDistributionProbability);
            }



            Configurations.maxP4Decimal = Integer.parseInt(document.valueOf("testCaseGenerator/maxP4Decimal").trim());
            Configurations.maxS4Decimal = Integer.parseInt(document.valueOf("testCaseGenerator/maxS4Decimal").trim());

            nodes = document.selectNodes("testCaseGenerator/varcharLengthHistogram/HistogramItem");
            items = new ArrayList<>();
            for (Node node : nodes) {
                items.add(new HistogramItem(
                        Double.parseDouble(node.valueOf("minValue")),
                        Double.parseDouble(node.valueOf("maxValue")),
                        Double.parseDouble(node.valueOf("ratio"))));
            }
            Configurations.varcharLengthHistogram = new Histogram(items);
            if (!(Configurations.varcharLengthHistogram.getProbabilitySum() > 0.999999999
                    && Configurations.varcharLengthHistogram.getProbabilitySum() < 1.000000001)) {
                throw new Exception("varcharLengthHistogram配置项有误，概率和不为1！");
            }

            Configurations.seedStringNum = Integer.parseInt(document.valueOf("testCaseGenerator/seedStringNum").trim());

            nodes = document.selectNodes("testCaseGenerator/supportIndexDataTypes/dataType");
            Configurations.supportIndexDataTypes = new HashSet<>();
            for (Node node : nodes) {
                Configurations.supportIndexDataTypes.add(node.getText().trim());
            }

            nodes = document.selectNodes("testCaseGenerator/dataType2CreateIndexProbability/item");
            Configurations.dataType2CreateIndexProbability = new HashMap<>();
            for (Node node : nodes) {
                dataType2CreateIndexProbability.put(node.valueOf("dataType").trim(), Float.parseFloat(node.valueOf("probability")));
            }

            nodes = document.selectNodes("testCaseGenerator/multiColumnIndexNumHistogram/HistogramItem");
            items = new ArrayList<>();
            for (Node node : nodes) {
                items.add(new HistogramItem(
                        Double.parseDouble(node.valueOf("minValue")),
                        Double.parseDouble(node.valueOf("maxValue")),
                        Double.parseDouble(node.valueOf("ratio"))));
            }
            Configurations.multiColumnIndexNumHistogram = new Histogram(items);

            Configurations.indexMaxColumnNum = Integer.parseInt(document.valueOf("testCaseGenerator/indexMaxColumnNum").trim());
            Configurations.isCreateIndex4ForeignKey = Boolean.parseBoolean(document.valueOf("testCaseGenerator/isCreateIndex4ForeignKey").trim());

            Configurations.dataoutputDir = document.valueOf("testCaseGenerator/dataoutputDir").trim();

            Configurations.queryOutputDir = document.valueOf("testCaseGenerator/queryoutputDir").trim();

            Configurations.schemaOutputDir = document.valueOf("testCaseGenerator/schemaoutputDir").trim();

            Configurations.reportOutputDir = document.valueOf("testCaseGenerator/reportoutputDir").trim();

            Configurations.calculateResultOutputDir = document.valueOf("testCaseGenerator/calculateResultOutputDir").trim();

            Configurations.executeResultOutputDir = document.valueOf("testCaseGenerator/executeResultOutputDir").trim();

            Configurations.executeTimeOutputDir = document.valueOf("testCaseGenerator/executeTimeOutputDir").trim();

            Configurations.joinOrderRankDir = document.valueOf("testCaseGenerator/joinOrderRankOutputDir").trim();

            Configurations.intermediateCardDir = document.valueOf("testCaseGenerator/intermediateCardDir").trim();

            Configurations.encodeType = document.valueOf("testCaseGenerator/encodeType").trim();

            Configurations.generateExactCard  = document.valueOf("testCaseGenerator/generatedExactCardinality").trim();

            Configurations.maxTableNum = Integer.parseInt(document.valueOf("testCaseGenerator/maxTableNum").trim());
            Configurations.maxGroupNum = Integer.parseInt(document.valueOf("testCaseGenerator/maxGroupNum").trim());
            Configurations.maxOrderNum = Integer.parseInt(document.valueOf("testCaseGenerator/maxOrderNum").trim());
            Configurations.maxAggreNum = Integer.parseInt(document.valueOf("testCaseGenerator/maxAggreNum").trim());


            Configurations.cardOptimalJoinOrderEval = Boolean.parseBoolean(document.valueOf("testCaseGenerator/joinOrderEvaluatioin/cardOptimialJoinOrder").trim());
            Configurations.joinOrderRank = Boolean.parseBoolean(document.valueOf("testCaseGenerator/joinOrderEvaluatioin/joinOrderRank").trim());



            Configurations.isGeneSchema = Boolean.parseBoolean(document.valueOf("testCaseGenerator/isGeneSchema").trim());
            Configurations.isGeneData = Boolean.parseBoolean(document.valueOf("testCaseGenerator/isGeneData").trim());
            Configurations.isGeneQuery = Boolean.parseBoolean(document.valueOf("testCaseGenerator/isGeneQuery").trim());

            Configurations.schemaFile = document.valueOf("testCaseGenerator/schemaFile").trim();

            Configurations.isLoadData = Boolean.parseBoolean(document.valueOf("testCaseGenerator/isLoadData").trim());

            Configurations.cardLowerBound = Integer.parseInt(document.valueOf("testCaseGenerator/cardLowerBound").trim());

            Configurations.service = Executors.newFixedThreadPool(solverThread);

            Configurations.executeTimeCompare = Boolean.parseBoolean(document.valueOf("testCaseGenerator/executeTimeCompare").trim());

            Configurations.correctnessVarity = Boolean.parseBoolean(document.valueOf("testCaseGenerator/correctnessVarity").trim());

            Configurations.dataAndQueryGen = Boolean.parseBoolean(document.valueOf("testCaseGenerator/isCalcuate").trim());
        } catch (Exception e) {
            e.printStackTrace();
        }
        //加载的时候就直接执行
        //Configurations.loadConfigurations();
        Loader.loadNativeLibraries();//Google OR-Tools求解需要载入的东西
        //然后创建好所有的需要的文件夹
        List<String> requireDir = new ArrayList<>();
        requireDir.add(Configurations.getSchemaOutputDir());
        requireDir.add(Configurations.getDataoutputDir());
        requireDir.add(Configurations.getCalculateResultOutputDir());
        requireDir.add(Configurations.getQueryOutputDir());
        requireDir.add(Configurations.getExecuteResultOutputDir());
        requireDir.add(Configurations.getExecuteTimeOutputDir());
        requireDir.add(Configurations.getJoinOrderRankDir());
        requireDir.add(Configurations.getIntermediateCardDir());
        requireDir.add(Configurations.getReportOutputDir());
        requireDir.add("Archive");
        requireDir.add("geneExpressions");
        requireDir.add("TreeGraph");
        requireDir.add("TreeGraphDot");
        requireDir.add("ExactCardinalityGeneration");
        for(String dir : requireDir){
            File file = new File(dir);
            if (!file.exists()) {
                file.mkdir();
            }
        }
    }

    public static void disPlayConfig(){

        System.out.println("每个模式生成Query(queryNumPerSchema)数量：\n"+Configurations.queryNumPerSchema);
        System.out.println("每个节点生成数据的线程数：\n"+Configurations.threadNumPerNode);
        System.out.println("模式中表的数量的分布(caseNumPerSchema)直方图：\n"+Configurations.tableNumHistogram);
        System.out.println("模式中表大小的分布(tableSizeHistogram)直方图：\n"+Configurations.tableSizeHistogram);
        System.out.println("每个表中外键数量分布(foreignKeyNumHistogram)直方图：\n"+Configurations.foreignKeyNumHistogram);
        System.out.println("每个表中的列的数量的分布(columnNumHistogram)直方图：\n"+Configurations.columnNumHistogram);
        System.out.println("生成数据分布的(均匀、Zipf分布)概率：\n"+Configurations.dataDistributionProbability);
        System.out.println("数据类型分布(dataType2OccurProbability)概率：\n"+Configurations.dataType2OccurProbability);
        System.out.println(Configurations.maxP4Decimal);
        System.out.println(Configurations.maxS4Decimal);
        System.out.println(Configurations.varcharLengthHistogram);
        System.out.println("生成的测试数据库名称："+Configurations.testDatabaseName);

        System.out.println("数据库配置：");
        for(int i = 0; i < DBConnections.size(); i ++) {
            System.out.println("1:");
            System.out.println("数据库类型：" + DBConnections.get(i).getDatabaseBrand());
            System.out.println("数据库连接IP：" + DBConnections.get(i).getDatabaseIP());
            System.out.println("数据库连接Port：" + DBConnections.get(i).getDatabasePort());
            System.out.println("数据库连接的用户名和密码：" + DBConnections.get(i).getDbUsername() + " " + DBConnections.get(i).getDbPassword());
        }

        System.out.println(""+Configurations.seedStringNum);
        System.out.println("支持二级索引的数据类型"+Configurations.supportIndexDataTypes);
        System.out.println(Configurations.dataType2CreateIndexProbability);
        System.out.println(Configurations.multiColumnIndexNumHistogram);
        System.out.println(Configurations.indexMaxColumnNum);
        System.out.println(Configurations.isCreateIndex4ForeignKey);
        System.out.println(Configurations.dataoutputDir);
        System.out.println("编码格式："+Configurations.encodeType);
        System.out.println(Configurations.maxTableNum);
        System.out.println(Configurations.maxGroupNum);
        System.out.println(Configurations.maxOrderNum);
        System.out.println(Configurations.maxAggreNum);
    }

    public static int getQueryNumPerSchema() {
        return queryNumPerSchema;
    }

    public static int getTableNumPerQuery() {
        return tableNumPerQuery;
    }

    public static String getQueryOutputDir() {
        return queryOutputDir;
    }

    public static String getSchemaOutputDir() {
        return schemaOutputDir;
    }

    public static String getReportOutputDir() {
        return reportOutputDir;
    }

    public static String getCalculateResultOutputDir() {
        return calculateResultOutputDir;
    }

    public static int getNodeNum() {
        return nodeNum;
    }

    public static double getSumProbability() {
        return sumProbability;
    }

    public static double getAvgProbability() {
        return avgProbability;
    }

    public static double getCountProbability() {
        return countProbability;
    }

    public static double getMaxProbability() {
        return maxProbability;
    }

    public static double getMinProbability() {
        return minProbability;
    }

    public static double getPureColumnProbability(){
        return pureColumnProbability;
    }
    public static double getSingleColumnProbability(){
        return singleColumnProbability;
    }
    public static double getMultiColumnProbability(){
        return multiColumnProbability;
    }

    public static double getSubQueryProbability(){
        return subQueryProbability;
    }

    public static boolean isSavaArchive() {
        return savaArchive;
    }
    public static boolean isGenerateOnly() {
        return isGenerateOnly;
    }

    public static boolean isdrawQuery() {
        return drawQuery;
    }

    public static String getTestDatabaseName() {
        return testDatabaseName;
    }

    public static List<DBConnection> getDBConnections() {
        return DBConnections;
    }

    public static int getThreadNumPerNode() {
        return threadNumPerNode;
    }

    public static Histogram getTableNumHistogram() {
        return tableNumHistogram;
    }

    public static Histogram getTableSizeHistogram() {
        return tableSizeHistogram;
    }

    public static Histogram getColumnNumHistogram() {
        return columnNumHistogram;
    }

    public static Map<String, Map<String,Float>> getDataDistributionProbability(){
        return dataDistributionProbability;
    }
    public static Map<String, Float> getDataType2OccurProbability() {
        return dataType2OccurProbability;
    }

    public static int getMaxP4Decimal() {
        return maxP4Decimal;
    }

    public static int getMaxS4Decimal() {
        return maxS4Decimal;
    }

    public static Histogram getVarcharLengthHistogram() {
        return varcharLengthHistogram;
    }

    public static int getSeedStringNum() {
        return seedStringNum;
    }

    public static Set<String> getSupportIndexDataTypes() {
        return supportIndexDataTypes;
    }

    public static Map<String, Float> getDataType2CreateIndexProbability() {
        return dataType2CreateIndexProbability;
    }

    public static Histogram getMultiColumnIndexNumHistogram() {
        return multiColumnIndexNumHistogram;
    }

    public static int getIndexMaxColumnNum() {
        return indexMaxColumnNum;
    }

    public static boolean isCreateIndex4ForeignKey() {
        return isCreateIndex4ForeignKey;
    }

    public static String getDataoutputDir() {return dataoutputDir; }

    public static String getEncodeType() {return encodeType; }

    public static String getExecuteResultOutputDir() {
        return executeResultOutputDir;
    }

    public static String getExecuteTimeOutputDir() { return executeTimeOutputDir; }

    public static String getJoinOrderRankDir() { return joinOrderRankDir; }



    public static String getIntermediateCardDir() {
        return intermediateCardDir;
    }

    public static String getGenerateExactCard(){
        return generateExactCard;
    }

    public static boolean isCalcuate() {
        return dataAndQueryGen;
    }

    public static int getMaxTableNum() {
        return maxTableNum;
    }

    public static int getMaxGroupNum() {
        return maxGroupNum;
    }

    public static int getMaxOrderNum() {
        return maxOrderNum;
    }

    public static int getMaxAggreNum() {
        return maxAggreNum;
    }


    public static int getSolverThread(){
        return solverThread;
    }

    public static boolean iscardOptimalJoinOrderEval(){
        return cardOptimalJoinOrderEval;
    }

    public static boolean isSortJoinOrderEval() { return joinOrderRank; }

    public static boolean isGetAllCard() {return getAllCard;}

    public static boolean isCorrectnessVarity() {return correctnessVarity;}

    public static boolean isExecuteTimeCompare() {return executeTimeCompare;}

    public static boolean isRandomGeneQueryGraphShape() { return randomGeneQueryGraphShape; }

    public static Map<String, Float> getQueryGraphShape2OccurProbability() { return queryGraphShape2OccurProbability; }

    public static boolean isGeneSchema() { return isGeneSchema; }

    public static boolean isGeneData() {return isGeneData;}

    public static boolean isGeneQuery() {return isGeneQuery;}

    public static boolean isLoadData() {return isLoadData;}

    public static String getSchemaFile() { return schemaFile; }

    public static double getZipSkewness() { return zipSkewness; }

    public static int getCardLowerBound() { return cardLowerBound;}

    // for test
    public static void main(String[] args) {
        System.out.println("配置信息：");
        Configurations.disPlayConfig();
    }

}
