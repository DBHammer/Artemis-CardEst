package ecnu.dbhammer.query;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.genetic.GeneticAlgorithmQueryParams;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.main.StartCaluate;
import ecnu.dbhammer.query.operator.CornerPredicateCase;
import ecnu.dbhammer.query.type.QueryGraph;
import ecnu.dbhammer.result.ResultGenerator;
import ecnu.dbhammer.schema.*;
import ecnu.dbhammer.solver.JoinCondition;

import org.apache.commons.math3.genetics.GeneticAlgorithm;
import org.jgrapht.graph.DirectedPseudograph;
import org.jgrapht.graph.Pseudograph;
import org.apache.commons.lang3.tuple.Pair;
import com.google.ortools.algorithms.main;
import java.io.*;
import java.sql.ResultSet;
import java.util.*;

/**
 * 负载生成器，生成所有的负载
 */
public class QueryGenerator {

    private DBSchema dbSchema;
    // 需要生成测试query的个数
    private int queryNum;
    // 所有query的平均生成时间
    private double queryGeneAvgTime;

    // 实验用
    private double filterGeneAvgTime;
    private double joinPatternGeneAvgTime;
    // 含有标准join数量的query的平均生成时间
    private double standardNumAvgTime;

    private QueryGraph queryGraph;

    private List<Table> joinedTableOrder; // 进行参数实例化用的Join Order，从某个FK表开始

    private List<Table> joinedTableOrder4Union;

    private Table firstTable;

    private Table firstTable4Union;

    private List<List<Pair<Table, Table>>> joinPairList;

    private List<List<Pair<Table, Table>>> joinPairList4Union;

    // 这个joinRelationPairList是用来构建左深树的，每个Pair代表左边提供的属性，右边提供的属性
    private List<List<Pair<Attribute, Attribute>>> joinRelationPairList;

    private List<List<Pair<Attribute, Attribute>>> joinRelationPairList4Union;

    private List<Table> cardOptimialJoinOrder;

    private List<Table> cardOptimialJoinOrder4Union;

    private double[] queryGraphShapeComulativeProbabilities;

    private QueryGraph[] queryGraphs;

    // 保存生成的queryNum个query的queryTree表示形式
    private List<QueryTree> queryTrees = new ArrayList<>();

    public QueryGenerator(DBSchema dbSchema, int queryNum) {
        super();
        this.dbSchema = dbSchema;
        this.queryNum = queryNum;
    }

    // 随机选取一种连接形状
    QueryGraph getRandomGraphType() {
        QueryGraph[] queryGraphs = QueryGraph.values();
        int i = new Random().nextInt(queryGraphs.length);
        return queryGraphs[i];

    }

    // 决定当前Query的QueryGraph形状
    private void initInfo4queryGraphShapeRandomGene(Map<String, Float> queryGraphShape2OccurProbability) {
        queryGraphShapeComulativeProbabilities = new double[queryGraphShape2OccurProbability.size()];
        queryGraphs = new QueryGraph[queryGraphShape2OccurProbability.size()];
        Iterator<Map.Entry<String, Float>> iter = queryGraphShape2OccurProbability.entrySet().iterator();
        int idx = 0;
        int numOfTable = Configurations.getTableNumPerQuery();
        while (iter.hasNext()) {
            Map.Entry<String, Float> entry = iter.next();
            String name = entry.getKey().toUpperCase();
            // 表数量<3时，只能生成chain
            // cyclic和grid的表数量>=4
            if (name.equals("CYCLIC") && entry.getValue() == 1.0 && numOfTable < 4) {
                RecordLog.recordLog(LogLevelConstant.INFO, "Cyclic至少需要4张表参与连接！请更改配置文件！");
                System.exit(0);
            }
            if (name.equals("GRID") && entry.getValue() == 1.0 && (numOfTable < 4 || numOfTable % 2 == 1)) {
                RecordLog.recordLog(LogLevelConstant.INFO, "Grid至少需要4张表参与连接，且参与连接表数量为双数！请更改配置文件！");
                System.exit(0);
            }
            if (!name.equals("CHAIN") && entry.getValue() > 0.0 && numOfTable < 3) {
                RecordLog.recordLog(LogLevelConstant.INFO, "参与连接的表数量小于3时，只能生成chain类型！请更改配置文件！");
                System.exit(0);
            }
            queryGraphs[idx] = Enum.valueOf(QueryGraph.class, name);
            queryGraphShapeComulativeProbabilities[idx] = entry.getValue();
            idx++;
        }

        for (int i = 1; i < queryGraphShapeComulativeProbabilities.length; i++) {
            queryGraphShapeComulativeProbabilities[i] += queryGraphShapeComulativeProbabilities[i - 1];
        }
    }

    // update by ct: 随机得到query graph形状
    private QueryGraph getRandomQueryGraphShape(int numOfTable) {
        if (numOfTable < 3)
            return QueryGraph.CHAIN;

        if (Configurations.isRandomGeneQueryGraphShape()) {
            int randomNum = (int) (Math.random() * 8);
            return queryGraphs[randomNum];
        }
        double randomValue = Math.random();
        for (int i = 0; i < queryGraphShapeComulativeProbabilities.length; i++) {
            if (randomValue < queryGraphShapeComulativeProbabilities[i]) {
                return queryGraphs[i];
            }
        }
        return null;
    }

    // 单层查询
    public QueryTree generate_basic(String path, int numOfTable, double[] params) throws Exception {
        // 生成某个形状的QueryGraph（随机或满足自定义概率）
        long joinPatternStartTime = System.currentTimeMillis();

        queryGraph = getRandomQueryGraphShape(numOfTable);
        RecordLog.recordLog(LogLevelConstant.INFO, "预定连接形状为:" + queryGraph.getName());

        long geneSingleQueryStartTime = System.currentTimeMillis();

        int tableNum = numOfTable;

        // selectedTables中保存了参与当前连接的表
        // shuchu long selectedTableStartTime = System.currentTimeMillis();

        RecordLog.recordLog(LogLevelConstant.INFO, "开始随机选择有参照关系的表");
        Pseudograph<String, JoinCondition> joinGraph = this.getRandomTables(tableNum);// joinGraph存储了指定表数量的连接图

        List<Table> selectedTables = new ArrayList<>();
        for (String name : joinGraph.vertexSet()) {
            selectedTables.add(this.dbSchema.getTableByName(name));
        }

        for (Table selTable : selectedTables) {
            RecordLog.recordLog(LogLevelConstant.INFO, selTable.getTableName());
        }

        // 选择完表，构建TableNode
        List<TableNode> tableNodes = geneTableNodesOverAllTables(selectedTables);

        // 开始构建连接顺序,具体策略：首先随机选择一个table出来，然后不断挑一个与之前已选择table中数据表有参照关系的table出来构建Join操作
        // TODO 貌似可以改为拓扑排序

        // 下面会进行预定的JoinOrder生成，通过该JoinOrder进行过滤谓词的生成
        List<List<Pair<Attribute, Attribute>>> preJoinRelation = geneIdealJoinOrderOverAllTableNodes(tableNodes,
                joinGraph, firstTable);
        // joinRelationMap是所有表的Join Relation，但是后续生成filter的时候，会cut掉表

        long joinPatternEndTime = System.currentTimeMillis();
        long joinPatternTime = joinPatternEndTime - joinPatternStartTime;
        joinPatternGeneAvgTime += joinPatternTime;

        RecordLog.recordLog(LogLevelConstant.INFO, "开始根据Join序生成Filter");
        // 根据JoinOrder依据每个Filter的关联关系依次构建filter
        // 先针对每个数据表生成一个Filter，在随机生成测试query tree时，filter是下放在查询树的最底层的

        long geneFiltersStartTime = System.currentTimeMillis();
        Pair<Integer, Integer> targetCardinality = Pair.of(10_000, 10_00000000);
        int numbOfEachTemplate = 2;
        FilterGenerator2  filterGenerator = new FilterGenerator2(joinedTableOrder, preJoinRelation, joinGraph, params,
                targetCardinality, numbOfEachTemplate);
//        FilterGenerator filterGenerator = new FilterGenerator(joinedTableOrder, preJoinRelation, joinGraph, params,
//                10000);

        filterGenerator.GeneticAlgorithmQueryParams();
        long geneFiltersEndTime = System.currentTimeMillis();

        System.out.println("生成时间:" + (geneFiltersEndTime - geneFiltersStartTime));
        int finalTableNum = selectedTables.size();
        // 下一张可能会有多个连接条件，因此需要之前多个表的主键范围，用来帮助filter生成（保证生成不会出现null值）
        // List<List<Integer>> lastKeySet = null;
        // for (int filterID = 1; filterID <= joinedTableOrder.size(); filterID++) {
        //     if (filterID == 1) {
        //         lastKeySet = filterGenerator.GenerateFirstFilter();// 先生成第一个Filter，随机生成
        //         // 第一个Filter肯定能生成成功

        //     } else {
        //         lastKeySet = filterGenerator.GenerateMoreFilter(filterID, lastKeySet);
        //         if (lastKeySet == null) {
        //             RecordLog.recordLog(LogLevelConstant.INFO, "生成了所有表的Filter，不需要删除表。");
        //         } else if (lastKeySet.size() == 0) {
        //             RecordLog.recordLog(LogLevelConstant.INFO,
        //                     "本Query生成到这个filter时停止，本Query最多的表数量为" + (filterID - 1));
        //             finalTableNum = filterID - 1;
        //             break;

        //         }
        //     }
        // }

        // 前面可根据表的数量生成filter
        // 实现Filter和Table分离
        List<Filter> filters = filterGenerator.getFilters();// 得到所有的Filter
        // System.out.println("get filters" + filters.toString());
        // System.out.println("filters size = " + filters.size());
        List<Integer> joinCardinality = filterGenerator.getJoinCardinality();// 得到所有的基数

        // shuchu long geneFiltersEndTime = System.currentTimeMillis();
        long geneFiltersTime = geneFiltersEndTime - geneFiltersStartTime;
        // shuchu RecordLog.recordLog(LogLevelConstant.INFO, "Filters生成结束，花费时间:" +
        // geneFiltersTime);
        // shuchu RecordLog.recordLog(LogLevelConstant.INFO, "生成了" + filters.size() +
        // "个Filter");
        filterGeneAvgTime += geneFiltersTime;

        // 创建join前把joinorder selectTable、joinRelationMap的后几个去掉
        // 注意要把select 已经生成的聚合运算修改
        int deleteTableNum = selectedTables.size() - finalTableNum;
        for (int del = 0; del < deleteTableNum; del++) {
            selectedTables.remove(selectedTables.size() - 1);
            joinedTableOrder.remove(joinedTableOrder.size() - 1);
            joinPairList.remove(joinPairList.size() - 1);
            joinRelationPairList.remove(joinRelationPairList.size() - 1);

        }
        RecordLog.recordLog(LogLevelConstant.INFO, "删除无法生成Filter的表后的TableList，删除了个" + deleteTableNum + "无法生成Filter的表");

        // 开始根据JoinOrder构建Join
        QueryNode root = geneJoinsOverAllFilters(filters);

        // root节点的作用
        // 会影响SQL文本中表join的顺序
        // 先跳过group by，order by，limit

        int groupByNum = 0;// 目前没有group by
        List<GroupBy> groupBys = geneGroupBy(joinedTableOrder, groupByNum);
        RecordLog.recordLog(LogLevelConstant.INFO, "分组生成结束");

        // 有分组操作的前提下，排序操作的表需要进行一些限制

        int orderByNum = 0;// TODO 这里order by可以修改 在生成union all时候不能有order by

        if (groupByNum > 0) {
            if (orderByNum > groupByNum) {
                orderByNum = groupByNum;
            }
        }

        List<OrderBy> orderBys;
        if (groupByNum == 0) {
            orderBys = geneOrderBysNoGroup(joinedTableOrder, orderByNum);
        } else {
            orderBys = geneOrderBysWiGroup(groupBys, orderByNum);
        }

        Having having = null;

        // 构建聚合操作，支持的聚合操作有：SUM, AVG, COUNT, MAX, MIN

        // TODO Query中聚合操作的数量 该项应该通过配置信息随机生成出来
        // int aggregationNum = 1 + (int) (Math.random() * 3);

        // TODO Query中聚合操作的数量 该项应该通过配置信息随机生成出来
        Select selectThings;

        // TODO 通过修改这里，控制聚合运算和投影表达式的生成选择
        if (Math.random() < 1) {
            // 要么生成聚合运算
            long geneAggregationsStartTime = System.currentTimeMillis();

            selectThings = new Aggregation(joinedTableOrder);

            long geneAggregationsEndTime = System.currentTimeMillis();

            long geneAggTime = geneAggregationsEndTime - geneAggregationsStartTime;

            System.out.println("聚合生成结束，时间：" + geneAggTime);
        } else {
            // 要么生成select items，这里确保如果有group by,会在group by作用的属性列选择属性
            List<Attribute> attributesForSelect = new ArrayList<>();
            if (groupByNum > 0) {
                for (GroupBy groupBy : groupBys) {
                    attributesForSelect.add(groupBy.getColumnName());
                }
                // 控制生成having
                having = new Having(new Aggregation(joinedTableOrder));
            } else {
                for (Table table : joinedTableOrder) {
                    attributesForSelect.addAll(table.getAllAttrubute());
                }
            }
            selectThings = new SelectItems(attributesForSelect);

        }

        String nonstandardPredicate = null;
        if (Math.random() < 0) {
            nonstandardPredicate = CornerPredicateCase.randomChoose();
        }

        QueryTree queryTree = new QueryTree(root, joinCardinality, selectThings, groupBys, orderBys, having,
                joinedTableOrder, filters,
                nonstandardPredicate, queryGraph, joinGraph, this.cardOptimialJoinOrder,
                filterGenerator.getFilterResults(), filterGenerator.getJoinResults(),
                filterGenerator.getFinalKey4Join());

        // 计算最终基数帮助limit生成
        // StartCaluate.startCal(queryTree);
        // int resultSize = queryTree.getFinalResultSize();
        long geneSingleQueryEndTime = System.currentTimeMillis();
        long geneSingleQueryTime = geneSingleQueryEndTime - geneSingleQueryStartTime;

        // 控制是否生成union all
        int UnionNum = 0;
        if (Math.random() < 0) {
            List<QueryTree> unionSet = new LinkedList<>();
            for (int k = 0; k < UnionNum; k++) {
                QueryTree queryTree4Union = generateUnion(tableNum, queryTree);
                unionSet.add(queryTree4Union);
            }
            queryTree.setUnionSet(unionSet);
        }

        System.out.println("queryTree生成结束,时间：" + geneSingleQueryTime);
        queryGeneAvgTime += geneSingleQueryTime;

        String createQueryDir = Configurations.getQueryOutputDir();
        File file_Query = new File(createQueryDir);
        if (!file_Query.exists()) {
            file_Query.mkdir();
        }

        return queryTree;
    }

    public List<QueryTree> generate(String path, int numOfTable) throws Exception {

        queryGeneAvgTime = 0.0;
        filterGeneAvgTime = 0.0;
        joinPatternGeneAvgTime = 0.0;
        int standNum = 0;
        int cnt = 0;
        double standAllTime = 0;// 生成理想数量表的Query
        List<QueryTree> queryTrees = new ArrayList<>();

        GeneticAlgorithmQueryParams genetic = new GeneticAlgorithmQueryParams();
        ArrayList<double[]> params = genetic.combination(numOfTable);
        // 初始各shape的概率
        if (!Configurations.isRandomGeneQueryGraphShape())
            initInfo4queryGraphShapeRandomGene(Configurations.getQueryGraphShape2OccurProbability());
        for (int i = 0; i < queryNum; i++) {
            RecordLog.recordLog(LogLevelConstant.INFO, "开始生成第" + (i + 1) + "条Query SQL");
            QueryTree queryTree = generate_basic(path, numOfTable, params.get(i));
            double randomValue = Math.random();
            if (randomValue > Configurations.getSubQueryProbability()) {
                // 普通的单层查询
                queryTrees.add(queryTree);
            } else {
                // 生成子查询
                double random = Math.random();
                if (random < 0.5) {
                    // 生成非相关子查询
                    queryTree.generateSubTable();
                    List<Table> tables = new ArrayList<>();
                    Table subTable = queryTree.getSubTable();
                    tables.add(subTable);

                    int groupByNum = 0;
                    List<GroupBy> groupBys = geneGroupBy(tables, groupByNum);
                    Select selectThings;
                    Having having = null;
                    if (Math.random() < 0) {
                        selectThings = new Aggregation(tables);
                    } else {
                        List<Attribute> attributesForSelect = new ArrayList<>();
                        if (groupByNum > 0) {
                            for (GroupBy groupBy : groupBys) {
                                attributesForSelect.add(groupBy.getColumnName());
                            }
                            // 控制生成having
                            having = new Having(new Aggregation(tables));
                        } else {
                            for (Table table : tables) {
                                attributesForSelect.addAll(table.getAllAttrubute());
                            }
                        }
                        selectThings = new SelectItems(attributesForSelect);
                    }
                    QueryTree subQueryTree = new QueryTree(null, null, selectThings, groupBys, null, having, tables,
                            null, null, null, null, null, null, null, null);
                    subQueryTree.setLastSqlText(queryTree.geneSQLJoinConInWhere());
                    queryTrees.add(subQueryTree);
                } else {

                }
            }
            String createQueryDir = Configurations.getQueryOutputDir();
            File file_Query = new File(createQueryDir);
            if (!file_Query.exists()) {
                file_Query.mkdir();
            }
            String outputFileName = path;
            // 把不同形状query分开存
            String outputQueryGraphFileName = Configurations.getQueryOutputDir() + File.separator + queryGraph.getName()
                    + ".txt";
            QueryTree qt = queryTrees.get(queryTrees.size() - 1);
            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(outputFileName, true), Configurations.getEncodeType()))) {
                System.out.println(qt.geneSQLJoinConInWhere());
                bw.write(qt.geneSQLJoinConInWhere() + "\r\n");
                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(outputQueryGraphFileName, true), Configurations.getEncodeType()))) {
                bw.write(qt.geneSQLJoinConInWhere() + "\r\n");
                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // 所有的query都已生成完成，计算生成时间
        queryGeneAvgTime = queryGeneAvgTime / queryNum;
        filterGeneAvgTime = filterGeneAvgTime / queryNum;
        joinPatternGeneAvgTime = joinPatternGeneAvgTime / queryNum;

        System.out.println("不满足指定连接数目的查询数：" + cnt);
        System.out.println("实例化正常的查询数：" + standNum);

        standardNumAvgTime = standAllTime * 1.0 / standNum;

        return queryTrees;
    }

    // 从当前数据库schema中随机选择指定数目（tableNum）个数据表
    // 选择的数据表之间必须有参照依赖关系，不然无法join，从而无法构成一个query
    private Pseudograph<String, JoinCondition> getRandomTables(int tableNum) {
        QueryGraphGenerator queryGraphGenerator = new QueryGraphGenerator(dbSchema);
        // 1. 纯chain类型
        if (this.queryGraph == QueryGraph.CHAIN) {
            queryGraphGenerator.generateQueryGraph4Chain(tableNum);
        } // 2.Tree类型：结合chain和star
        else if (this.queryGraph == QueryGraph.TREE) {
            queryGraphGenerator.generateQueryGraph4Tree(tableNum);
        } // 3.纯star类型
        else if (this.queryGraph == QueryGraph.STAR) {
            queryGraphGenerator.generateQueryGraph4Star(tableNum);
        } // 4.纯cycle类型
        else if (this.queryGraph == QueryGraph.CYCLE) {
            queryGraphGenerator.generateQueryGraph4Cycle(tableNum);
        } // 5.cyclic类型：有环的（>=1个环）
        else if (this.queryGraph == QueryGraph.CYCLIC) {
            queryGraphGenerator.generateQueryGraph4Cyclic(tableNum);
        } // 6.clique类型
        else if (this.queryGraph == QueryGraph.CLIQUE) {
            queryGraphGenerator.generateQueryGraph4Clique(tableNum);
        } // 7.grid类型
        else if (this.queryGraph == QueryGraph.GRID) {
            queryGraphGenerator.generateQueryGraph4grid(tableNum);

        }
        firstTable = queryGraphGenerator.getFirstTable();
        // 注意：第一个随机加入的数据表可能会导致最终selectedTables无法含有tableNum个数据表，但因为该问题可容忍，就没有再加一层循环随机了
        return queryGraphGenerator.getJoinGraph();
    }

    // 将选择的表转化为TableNode
    private List<TableNode> geneTableNodesOverAllTables(List<Table> selectedTables) {
        List<TableNode> tableNodes = new ArrayList<>();
        for (int i = 0; i < selectedTables.size(); i++) {
            Table table = selectedTables.get(i);

            tableNodes.add(new TableNode(table));
        }
        return tableNodes;
    }

    // 基于tableNode生成理想的连接顺序
    // TODO 目前仅支持PK-FK Join
    // update by ct：增加多连接条件的情况
    private List<List<Pair<Attribute, Attribute>>> geneIdealJoinOrderOverAllTableNodes(List<TableNode> tableNodes,
            Pseudograph<String, JoinCondition> tableGraph, Table firstTable) {
        // 记录表之间的连接序
        joinedTableOrder = new ArrayList<>();
        joinPairList = new ArrayList<>();
        joinRelationPairList = new ArrayList<>();
        if (tableNodes.size() == 1) {
            joinedTableOrder.add(tableNodes.get(0).getTable());
        } else {
            // 先得到每张表参与的所有依赖关系
            Map<String, Map<String, JoinCondition>> allRelations = new HashMap<>();
            for (JoinCondition joinCondition : tableGraph.edgeSet()) {
                if (!allRelations.containsKey(joinCondition.getLeftAttribute().getTableName())) {
                    allRelations.put(joinCondition.getLeftAttribute().getTableName(), new HashMap<>());
                }
                allRelations.get(joinCondition.getLeftAttribute().getTableName())
                        .put(joinCondition.getRightAttribute().getTableName(), joinCondition);
                if (!allRelations.containsKey(joinCondition.getRightAttribute().getTableName())) {
                    allRelations.put(joinCondition.getRightAttribute().getTableName(), new HashMap<>());
                }
                allRelations.get(joinCondition.getRightAttribute().getTableName())
                        .put(joinCondition.getLeftAttribute().getTableName(), joinCondition);
            }
            // 逐步加入表及其对应依赖关系
            Set<String> joinedTableNameSet = new HashSet<>();
            ArrayList<String> tableGraphVertexTable = new ArrayList(tableGraph.vertexSet());
            // 先加入第一张表
            joinedTableNameSet.add(firstTable.getTableName());
            joinedTableOrder.add(firstTable);
            tableGraphVertexTable.remove(firstTable.getTableName());
            // 遍历后续的表
            for (int i = 0; i < tableGraphVertexTable.size(); i++) {
                // System.out.println("table is: "+tableGraphVertexTable.get(i));
                Table newTable = dbSchema.getTableByName(tableGraphVertexTable.get(i));
                // 当前要加入的表参与的所有依赖关系
                Map<String, JoinCondition> tmpMap = allRelations.get(tableGraphVertexTable.get(i));
                // 遍历所有依赖关系，是否与已连接表集合中的表有关系（joinedTableNameSet）
                Iterator<Map.Entry<String, JoinCondition>> it = tmpMap.entrySet().iterator();
                List<Pair<Table, Table>> tmpList1 = new LinkedList<>();
                List<Pair<Attribute, Attribute>> tmpList2 = new LinkedList<>();

                while (it.hasNext()) {
                    Map.Entry<String, JoinCondition> entry = it.next();
                    if (joinedTableNameSet.contains(entry.getKey())) {// 表示新表和已选择的表有连接关系
                        Table tableInSet = dbSchema.getTableByName(entry.getKey());
                        tmpList1.add(Pair.of(tableInSet, newTable));
                        // 判断新加入的表是fk表还是pk表
                        // fk表在set中，说明新加入的是pk表
                        if (entry.getValue().getLeftAttribute().getTableName().equals(entry.getKey())) {
                            // shuchu System.out.println("新加入的是pk表");
                            Attribute leftJoinAttribute = entry.getValue().getLeftAttribute();
                            Attribute rightJoinAttribute = newTable.getPrimaryKey();
                            tmpList2.add(Pair.of(leftJoinAttribute, rightJoinAttribute));
                            // tmpList3.add(Pair.of(leftJoinAttribute, rightJoinAttribute));
                            String leftJoinColumnName = leftJoinAttribute.getFullAttrName();
                            String rightJoinColumnName = rightJoinAttribute.getFullAttrName();
                            // shuchu RecordLog.recordLog(LogLevelConstant.INFO, "左表Join提供的列名" +
                            // leftJoinColumnName + "右表Join提供的列名" + rightJoinColumnName);
                        }
                        // 新加入的是fk表
                        else {
                            // shuchu System.out.println("新加入的是fk表");
                            Attribute leftJoinAttribute = tableInSet.getPrimaryKey();
                            Attribute rightJoinAttribute = entry.getValue().getLeftAttribute();
                            tmpList2.add(Pair.of(leftJoinAttribute, rightJoinAttribute));
                            String leftJoinColumnName = leftJoinAttribute.getFullAttrName();
                            String rightJoinColumnName = rightJoinAttribute.getFullAttrName();
                            // shuchu RecordLog.recordLog(LogLevelConstant.INFO, "左表Join提供的列名" +
                            // leftJoinColumnName + "右表Join提供的列名" + rightJoinColumnName);
                        }
                    }
                }
                joinedTableNameSet.add(tableGraphVertexTable.get(i));
                joinedTableOrder.add(newTable);
                joinPairList.add(tmpList1);
                joinRelationPairList.add(tmpList2);
            }
        }
        // }

        // shuchu RecordLog.recordLog(LogLevelConstant.INFO, "预期的TableJoinOrder为:");

        // shuchu for (Table tableName : joinedTableOrder) {
        // RecordLog.recordLog(LogLevelConstant.INFO, tableName.getTableName() + " 大小" +
        // tableName.getTableSize());
        // }
        return joinRelationPairList;// 返回构建好的Joinmap
    }

    // 基于Filters生成连接操作，并构建成Query Tree的形式
    // 具体策略：首先随机选择一个Filter出来，然后不断挑一个与之前已选择Filter中数据表有参照关系的Filter出来构建Join操作
    // 在构建Join算子时，需确保左边孩子节点为root节点，以方便后面将Query Tree转化成SQL
    private QueryNode geneJoinsOverAllFilters(List<Filter> filters) {
        // 先得到第一个filter,当作root
        Table firstTable;
        if (joinPairList.size() == 0) {// 表示没有找到足够的表参与连接，只选出了一张表
            firstTable = filters.get(0).getTable();
        } else {
            firstTable = joinPairList.get(0).get(0).getLeft();
        }
        QueryNode root = getFilterByTableName(filters, firstTable.getTableName());

        for (int i = 0; i < joinPairList.size(); i++) {
            // 新表的filter条件
            Filter filter = getFilterByTableName(filters, joinPairList.get(i).get(0).getRight().getTableName());
            // 跟前面的root结合成新的join
            // 可能是单个join或者复合join
            // if (joinRelationPairList.get(i).size() > 1) {
            // List<Join> joins = new LinkedList<>();
            // for (int j = 0; j < joinRelationPairList.get(i).size(); j++)
            // joins.add(new Join(root, filter,
            // joinRelationPairList.get(i).get(j).getLeft(),
            // joinRelationPairList.get(i).get(j).getRight()));
            // ComplexJoin complexJoin = new ComplexJoin(joins);
            // root = complexJoin;
            // } else
            if (joinRelationPairList.get(i).size() > 1) {
                for (int j = 0; j < joinRelationPairList.get(i).size(); j++) {
                    Join join = new Join(root, filter, joinRelationPairList.get(i).get(j).getLeft(),
                            joinRelationPairList.get(i).get(j).getRight());
                    root = join;
                }
            } else {
                if (root == null) {
                    System.out.println("root is null");
                }
                if (filter == null) {
                    System.out.println("filter is null");
                }
                if (joinRelationPairList.get(i).get(0).getLeft() == null) {
                    System.out.println("left is null");
                }
                if (joinRelationPairList.get(i).get(0).getRight() == null) {
                    System.out.println("right is null");
                }
                Join join = new Join(root, filter, joinRelationPairList.get(i).get(0).getLeft(),
                        joinRelationPairList.get(i).get(0).getRight());
                root = join;
            }
        }
        return root;
    }

    private Filter getFilterByTableName(List<Filter> filters, String tableName) {
        for (Filter filter : filters) {
            if (filter.getTable().getTableName().equals(tableName)) {
                return filter;
            }
        }
        return null;
    }

    // 在构建的连接结果上进行分组操作，随机确定分组选择的列，可以支持多列分组，使用group by之后会对select中可以指定的项进行限制
    private List<GroupBy> geneGroupBy(List<Table> selectedTables, int groupByNum) {
        List<GroupBy> groupBys = new ArrayList<>();
        for (int i = 0; i < groupByNum; i++) {
            groupBys.add(new GroupBy(selectedTables));
        }
        return groupBys;
    }

    private List<GroupBy> geneGroupBy4Union(List<Table> selectedTables, int groupByNum, List<GroupBy> firstGroupBys) {
        List<GroupBy> groupBys = new ArrayList<>();
        for (int i = 0; i < groupByNum; i++) {
            while (true) {
                GroupBy groupBy = new GroupBy(selectedTables);
                // 控制保证属性类型相同
                if (firstGroupBys.get(i).attribute.getDataType().getClass() == groupBy.attribute.getDataType()
                        .getClass()) {
                    groupBys.add(groupBy);
                    break;
                } else {
                    continue;
                }
            }
        }
        return groupBys;
    }

    // 在构建的连接结果上进行排序操作，随机确定排序选择的列
    private List<OrderBy> geneOrderBysNoGroup(List<Table> selectedTables, int orderByNum) {
        List<OrderBy> orderBys = new ArrayList<>();
        for (int i = 0; i < orderByNum; i++) {
            orderBys.add(new OrderBy(selectedTables));
        }
        return orderBys;
    }

    private List<OrderBy> geneOrderBysNoGroup4Union(List<Table> selectedTables, int orderByNum,
            List<OrderBy> firstOrderBys) {
        List<OrderBy> orderBys = new ArrayList<>();
        for (int i = 0; i < orderByNum; i++) {
            while (true) {
                OrderBy orderBy = new OrderBy(selectedTables);
                if (firstOrderBys.get(i).attribute.getDataType().getClass() == orderBy.attribute.getDataType()
                        .getClass()) {
                    orderBys.add(orderBy);
                    break;
                } else {
                    continue;
                }
            }
        }
        return orderBys;
    }

    private List<OrderBy> geneOrderBysWiGroup(List<GroupBy> groupBys, int orderByNum) {
        List<OrderBy> orderBys = new ArrayList<>();
        List<Attribute> columnNames = new ArrayList<>();
        for (int i = 0; i < groupBys.size(); i++) {
            columnNames.add(groupBys.get(i).getColumnName());
        }
        for (int i = 0; i < orderByNum; i++) {
            int index = (int) (Math.random() * columnNames.size());
            Attribute columnName = columnNames.get(index);
            orderBys.add(new OrderBy(columnName));
            columnNames.remove(index);
        }
        return orderBys;
    }

    public double getQueryGeneAvgTime() {
        return queryGeneAvgTime;
    }

    public double getFilterGeneAvgTime() {
        return filterGeneAvgTime;
    }

    public double getJoinPatternGeneAvgTime() {
        return joinPatternGeneAvgTime;
    }

    public double getStandard() {
        return standardNumAvgTime;
    }

    public List<Table> getCardOptimialJoinOrder() {
        return this.cardOptimialJoinOrder;
    }

    private boolean judge(Set<Table> alreadyJoin, Table testTable, DirectedPseudograph<String, ForeignKey> tableGraph) {
        boolean ans = false;
        for (Table table : alreadyJoin) {
            if (tableGraph.containsEdge(table.getTableName(), testTable.getTableName())
                    || tableGraph.containsEdge(testTable.getTableName(), table.getTableName())) {
                ans = true;
            }
        }
        return ans;

    }

    private QueryTree generateUnion(int tableNum, QueryTree firstQueryTree) throws Exception {
        List<Table> selectedTables4Union = new ArrayList<>();
        Pseudograph<String, JoinCondition> joinGraph4Union = this.getRandomTables4Union(tableNum);
        for (String name : joinGraph4Union.vertexSet()) {
            selectedTables4Union.add(this.dbSchema.getTableByName(name));
        }
        // 选择完表，构建TableNode
        List<TableNode> tableNodes4Union = geneTableNodesOverAllTables(selectedTables4Union);
        List<List<Pair<Attribute, Attribute>>> preJoinRelation4Union = geneIdealJoinOrderOverAllTableNodes4Union(
                tableNodes4Union, joinGraph4Union, firstTable4Union);

        FilterGenerator filterGenerator4Union = new FilterGenerator(joinedTableOrder4Union, preJoinRelation4Union,
                joinGraph4Union, new double[0], 1000);

        int finalTableNum4Union = selectedTables4Union.size();
        // 下一张可能会有多个连接条件，因此需要之前多个表的主键范围，用来帮助filter生成（保证生成不会出现null值）
        List<List<Integer>> lastKeySet4Union = null;
        for (int filterID = 1; filterID <= joinedTableOrder4Union.size(); filterID++) {
            if (filterID == 1) {
                lastKeySet4Union = filterGenerator4Union.GenerateFirstFilter();// 先生成第一个Filter，随机生成
                // 第一个Filter肯定能生成成功
            } else {
                lastKeySet4Union = filterGenerator4Union.GenerateMoreFilter(filterID, lastKeySet4Union);
                if (lastKeySet4Union == null) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "生成了所有表的Filter，不需要删除表。");
                } else if (lastKeySet4Union.size() == 0) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "本Query生成到这个filter时停止，本Query最多的表数量为" + (filterID - 1));
                    finalTableNum4Union = filterID - 1;
                    break;

                }
            }
        }
        List<Filter> filters4Union = filterGenerator4Union.getFilters();// 得到所有的Filter
        List<Integer> joinCardinality4Union = filterGenerator4Union.getJoinCardinality();// 得到所有的基数
        int deleteTableNum4Union = selectedTables4Union.size() - finalTableNum4Union;
        for (int del = 0; del < deleteTableNum4Union; del++) {
            selectedTables4Union.remove(selectedTables4Union.size() - 1);
            joinedTableOrder4Union.remove(joinedTableOrder4Union.size() - 1);
            joinPairList4Union.remove(joinPairList4Union.size() - 1);
            joinRelationPairList4Union.remove(joinRelationPairList4Union.size() - 1);

        }
        RecordLog.recordLog(LogLevelConstant.INFO,
                "删除无法生成Filter的表后的TableList，删除了个" + deleteTableNum4Union + "无法生成Filter的表");

        // 开始进行JoinReorder
        if (Configurations.iscardOptimalJoinOrderEval()) {
            // TODO joinReorder算法待修改
            // this.cardOptimialJoinOrder = joinReorder(filters, joinGraph);
        } else {
            this.cardOptimialJoinOrder4Union = joinedTableOrder4Union;
        }
        // 开始根据JoinOrder构建Join
        QueryNode root4Union = geneJoinsOverAllFilters4Union(filters4Union);
        int groupByNum = firstQueryTree.getGroupBys().size();
        List<GroupBy> groupBys4Union = geneGroupBy4Union(joinedTableOrder4Union, groupByNum,
                firstQueryTree.getGroupBys());
        List<OrderBy> orderBys4Union;
        Having having = null;
        int orderByNum = firstQueryTree.getOrderBys().size();
        if (groupByNum == 0) {
            orderBys4Union = geneOrderBysNoGroup4Union(joinedTableOrder4Union, orderByNum,
                    firstQueryTree.getOrderBys());
        } else {
            orderBys4Union = geneOrderBysWiGroup(groupBys4Union, orderByNum);
        }

        List<Attribute> attributesForSelect4Union = new ArrayList<>();
        if (groupByNum > 0) {
            for (GroupBy groupBy : groupBys4Union) {
                attributesForSelect4Union.add(groupBy.getColumnName());
            }
        } else {
            for (Table table : joinedTableOrder4Union) {
                attributesForSelect4Union.addAll(table.getAllAttrubute());
            }
        }
        Select selectThings4Union = new SelectItems(attributesForSelect4Union, firstQueryTree.getSelect());

        String nonstandardPredicate = null;
        if (Math.random() < 0) {
            nonstandardPredicate = CornerPredicateCase.randomChoose();
        }

        QueryTree queryTree4Union = new QueryTree(root4Union, joinCardinality4Union, selectThings4Union, groupBys4Union,
                orderBys4Union, having, joinedTableOrder4Union, filters4Union,
                nonstandardPredicate, queryGraph, joinGraph4Union, this.cardOptimialJoinOrder4Union,
                filterGenerator4Union.getFilterResults(), filterGenerator4Union.getJoinResults(),
                filterGenerator4Union.getFinalKey4Join());

        // 计算最终基数帮助limit生成
        StartCaluate.startCal(queryTree4Union);

        return queryTree4Union;
    }

    private Pseudograph<String, JoinCondition> getRandomTables4Union(int tableNum) {
        QueryGraphGenerator queryGraphGenerator = new QueryGraphGenerator(dbSchema);
        // 1. 纯chain类型
        if (this.queryGraph == QueryGraph.CHAIN) {
            queryGraphGenerator.generateQueryGraph4Chain(tableNum);
        } // 2.Tree类型：结合chain和star
        else if (this.queryGraph == QueryGraph.TREE) {
            queryGraphGenerator.generateQueryGraph4Tree(tableNum);
        } // 3.纯star类型
        else if (this.queryGraph == QueryGraph.STAR) {
            queryGraphGenerator.generateQueryGraph4Star(tableNum);
        } // 4.纯cycle类型
        else if (this.queryGraph == QueryGraph.CYCLE) {
            queryGraphGenerator.generateQueryGraph4Cycle(tableNum);
        } // 5.cyclic类型：有环的（>=1个环）
        else if (this.queryGraph == QueryGraph.CYCLIC) {
            queryGraphGenerator.generateQueryGraph4Cyclic(tableNum);
        } // 6.clique类型
        else if (this.queryGraph == QueryGraph.CLIQUE) {
            queryGraphGenerator.generateQueryGraph4Clique(tableNum);
        } // 7.grid类型
        else if (this.queryGraph == QueryGraph.GRID) {
            queryGraphGenerator.generateQueryGraph4grid(tableNum);

        }
        firstTable4Union = queryGraphGenerator.getFirstTable();
        // 注意：第一个随机加入的数据表可能会导致最终selectedTables无法含有tableNum个数据表，但因为该问题可容忍，就没有再加一层循环随机了
        return queryGraphGenerator.getJoinGraph();
    }

    private List<List<Pair<Attribute, Attribute>>> geneIdealJoinOrderOverAllTableNodes4Union(List<TableNode> tableNodes,
            Pseudograph<String, JoinCondition> tableGraph, Table firstTable) {
        // 记录表之间的连接序
        joinedTableOrder4Union = new ArrayList<>();
        joinPairList4Union = new ArrayList<>();
        joinRelationPairList4Union = new ArrayList<>();
        if (tableNodes.size() == 1) {
            joinedTableOrder4Union.add(tableNodes.get(0).getTable());
        } else {
            // 先得到每张表参与的所有依赖关系
            Map<String, Map<String, JoinCondition>> allRelations = new HashMap<>();
            for (JoinCondition joinCondition : tableGraph.edgeSet()) {
                if (!allRelations.containsKey(joinCondition.getLeftAttribute().getTableName())) {
                    allRelations.put(joinCondition.getLeftAttribute().getTableName(), new HashMap<>());
                }
                allRelations.get(joinCondition.getLeftAttribute().getTableName())
                        .put(joinCondition.getRightAttribute().getTableName(), joinCondition);
                if (!allRelations.containsKey(joinCondition.getRightAttribute().getTableName())) {
                    allRelations.put(joinCondition.getRightAttribute().getTableName(), new HashMap<>());
                }
                allRelations.get(joinCondition.getRightAttribute().getTableName())
                        .put(joinCondition.getLeftAttribute().getTableName(), joinCondition);
            }
            // 逐步加入表及其对应依赖关系
            Set<String> joinedTableNameSet = new HashSet<>();
            ArrayList<String> tableGraphVertexTable = new ArrayList(tableGraph.vertexSet());
            // 先加入第一张表
            joinedTableNameSet.add(firstTable.getTableName());
            joinedTableOrder4Union.add(firstTable);
            tableGraphVertexTable.remove(firstTable.getTableName());
            // 遍历后续的表
            for (int i = 0; i < tableGraphVertexTable.size(); i++) {
                // System.out.println("table is: "+tableGraphVertexTable.get(i));
                Table newTable = dbSchema.getTableByName(tableGraphVertexTable.get(i));
                // 当前要加入的表参与的所有依赖关系
                Map<String, JoinCondition> tmpMap = allRelations.get(tableGraphVertexTable.get(i));
                // 遍历所有依赖关系，是否与已连接表集合中的表有关系（joinedTableNameSet）
                Iterator<Map.Entry<String, JoinCondition>> it = tmpMap.entrySet().iterator();
                List<Pair<Table, Table>> tmpList1 = new LinkedList<>();
                List<Pair<Attribute, Attribute>> tmpList2 = new LinkedList<>();

                while (it.hasNext()) {
                    Map.Entry<String, JoinCondition> entry = it.next();
                    if (joinedTableNameSet.contains(entry.getKey())) {
                        Table tableInSet = dbSchema.getTableByName(entry.getKey());
                        tmpList1.add(Pair.of(tableInSet, newTable));
                        // 判断新加入的表是fk表还是pk表
                        // fk表在set中，说明新加入的是pk表
                        if (entry.getValue().getLeftAttribute().getTableName().equals(entry.getKey())) {
                            // shuchu System.out.println("新加入的是pk表");
                            Attribute leftJoinAttribute = entry.getValue().getLeftAttribute();
                            Attribute rightJoinAttribute = newTable.getPrimaryKey();
                            tmpList2.add(Pair.of(leftJoinAttribute, rightJoinAttribute));
                            // tmpList3.add(Pair.of(leftJoinAttribute, rightJoinAttribute));
                            String leftJoinColumnName = leftJoinAttribute.getFullAttrName();
                            String rightJoinColumnName = rightJoinAttribute.getFullAttrName();
                            // shuchu RecordLog.recordLog(LogLevelConstant.INFO, "左表Join提供的列名" +
                            // leftJoinColumnName + "右表Join提供的列名" + rightJoinColumnName);
                        }
                        // 新加入的是fk表
                        else {
                            // shuchu System.out.println("新加入的是fk表");
                            Attribute leftJoinAttribute = tableInSet.getPrimaryKey();
                            Attribute rightJoinAttribute = entry.getValue().getLeftAttribute();
                            tmpList2.add(Pair.of(leftJoinAttribute, rightJoinAttribute));
                            String leftJoinColumnName = leftJoinAttribute.getFullAttrName();
                            String rightJoinColumnName = rightJoinAttribute.getFullAttrName();
                            // shuchu RecordLog.recordLog(LogLevelConstant.INFO, "左表Join提供的列名" +
                            // leftJoinColumnName + "右表Join提供的列名" + rightJoinColumnName);
                        }
                    }
                }
                joinedTableNameSet.add(tableGraphVertexTable.get(i));
                joinedTableOrder4Union.add(newTable);
                joinPairList4Union.add(tmpList1);
                joinRelationPairList4Union.add(tmpList2);
            }
        }
        return joinRelationPairList4Union;// 返回构建好的Joinmap
    }

    private QueryNode geneJoinsOverAllFilters4Union(List<Filter> filters) {
        // 先得到第一个filter,当作root
        Table firstTable;
        if (joinPairList4Union.size() == 0) {// 表示没有找到足够的表参与连接，只选出了一张表
            firstTable = filters.get(0).getTable();
        } else {
            firstTable = joinPairList4Union.get(0).get(0).getLeft();
        }
        QueryNode root = getFilterByTableName(filters, firstTable.getTableName());
        for (int i = 0; i < joinPairList4Union.size(); i++) {
            // 新表的filter条件
            Filter filter = getFilterByTableName(filters, joinPairList4Union.get(i).get(0).getRight().getTableName());
            // 跟前面的root结合成新的join
            // 可能是单个join或者复合join
            if (joinRelationPairList4Union.get(i).size() > 1) {
                List<Join> joins = new LinkedList<>();
                for (int j = 0; j < joinRelationPairList4Union.get(i).size(); j++)
                    joins.add(new Join(root, filter, joinRelationPairList4Union.get(i).get(j).getLeft(),
                            joinRelationPairList4Union.get(i).get(j).getRight()));
                ComplexJoin complexJoin = new ComplexJoin(joins);
                root = complexJoin;
            } else {
                Join join = new Join(root, filter, joinRelationPairList4Union.get(i).get(0).getLeft(),
                        joinRelationPairList4Union.get(i).get(0).getRight());
                root = join;
            }
        }
        return root;
    }

}
