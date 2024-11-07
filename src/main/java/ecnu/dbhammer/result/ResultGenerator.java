package ecnu.dbhammer.result;


import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.constant.LogicRelation;
import ecnu.dbhammer.data.AttrValue;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.query.operator.operatorToken;
import ecnu.dbhammer.query.type.AggregatioinType;
import ecnu.dbhammer.query.type.ComparisonOperatorType;
import ecnu.dbhammer.query.type.OperatorType;
import ecnu.dbhammer.query.type.OrderByType;
import ecnu.dbhammer.schema.*;
import com.opencsv.CSVWriter;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import ecnu.dbhammer.query.*;
import ecnu.dbhammer.solver.CSPDefination;

/**
 * 结果生成类，包括准确的基数生成以及理想结果生成
 */
public class ResultGenerator {

    private QueryNode root;
    //算select后面的东西
    private Select select;
    //算groupby
    private List<GroupBy> groupBys;
    //算orderby
    private List<OrderBy> orderBys;

    private QueryTree queryTree;


    private long computationTime;

    private long singleFilterGenerateTime;

    public ResultGenerator(QueryTree queryTree) {
        super();
        this.queryTree = queryTree;
        this.root = queryTree.getRoot();
        this.select = queryTree.getSelect();
        this.groupBys = queryTree.getGroupBys();
        this.orderBys = queryTree.getOrderBys();
    }


    public void geneResult(String calculateResultOutputDir) throws Exception {


        long startGeneResult = System.currentTimeMillis();
        // 保存最终结果的所有表
        List<Table> tables = new ArrayList<>();
        // 保存最终结果的表的相对应的元组Id集合
        List<ResultRow> key4Join = new ArrayList<>();
        //这里只是Join的结果
        //还需做剩下的

        // query中只包含一个表
        if (queryTree.getTables().size() == 1) {
            System.out.println("单表查询！表大小为：" + queryTree.getTables().get(0).getTableSize());
            // 单表查询的最终结果会保存在一个FilterResult中
            FilterResult finalFilterResult = null;
            if (queryTree.getRoot() instanceof Filter) {
                finalFilterResult = queryTree.getFilterResults()[0];
            } else if (queryTree.getRoot() instanceof TableNode) {
                finalFilterResult = null;
            }

            tables.add(queryTree.getRoot().getTable());//为了求k的范围

            List<SingleConstraint> constraints = new ArrayList<>();
            if (finalFilterResult != null) {
                constraints.addAll(finalFilterResult.getResult());
            }

            ComparisonOperatorType comparisonOperatorType = this.queryTree.getFilters().get(0).getPredicateList().get(0).getComparisonOperator();

            long ourSolverStart = System.currentTimeMillis();
            ConstraintGroup ourSolver;
            if (comparisonOperatorType == ComparisonOperatorType.In) {
                ourSolver = new ConstraintGroup(tables.get(0), 0, tables.get(0).getTableSize() - 1, constraints, LogicRelation.OR, 0);
            } else {
                ourSolver = new ConstraintGroup(tables.get(0), 0, tables.get(0).getTableSize() - 1, constraints, LogicRelation.AND, 0);
            }

            ResultSet set = ourSolver.getResultSet();
            for (SingleConstraint singleTableConstraint : constraints) {
                RecordLog.recordLog(LogLevelConstant.INFO, singleTableConstraint.toString());
            }
            long filterGenTime = System.currentTimeMillis() - ourSolverStart;
            this.singleFilterGenerateTime = filterGenTime;
            System.out.println("Filter求解器花费时间" + filterGenTime);

            for (int id = set.getLowerBound(); id <= set.getUpperBound(); id++) {
                List<Integer> keyList = new ArrayList<>();
                keyList.add(id);
                key4Join.add(new ResultRow(keyList));
            }
            queryTree.setFinalResultSize(set.getKeyList().size());
        } else if (queryTree.getTables().size() > 1) {
            CSPDefination origin = queryTree.getJoinResults()[queryTree.getTableOrder().size() - 2].getCspDefination();
            CSPDefination finalCsp = new CSPDefination(origin.getBiggestTable(), origin.getVariableList(), origin.getDomainList(), origin.getJoinConditionList());
            finalCsp.solve(queryTree.getTableOrder().toArray(new String[0]));//将K按照TableOrder的顺序排序
            key4Join = finalCsp.getKey4Join();
            queryTree.setFinalResultSize(finalCsp.getCardinality());
        }


        System.out.println("全部连接之后产生的KeySet大小：" + key4Join.size());

        List<String> tableOrder = new ArrayList<>();
        //如果表的数量为1的时候，只有一个表
        if (queryTree.getTables().size() == 1) {
            tableOrder.add(queryTree.getTables().get(0).getTableName());
        } else {
            tableOrder = queryTree.getTableOrder();
        }

        System.out.println("tableOrder是:");
        for (String tableInOrder : tableOrder) {

            System.out.println(tableInOrder);
        }
        System.out.println("tableOrder展示结束————————:");


        //求group by,对key4Join进行group by运算
        Map<BigDecimal, List<ResultRow>> resultMap = new HashMap<>();
        if (this.groupBys.size() > 0) {
            List<Attribute> groupByAttr = new ArrayList<>();
            for (GroupBy groupBy : this.groupBys) {
                groupByAttr.add(groupBy.getColumnName());
            }

            String attTableName = groupByAttr.get(0).getTableName();//聚合运算中涉及到的表
            int index = 0;

            for (int k = 0; k < tableOrder.size(); k++) {
                if (tableOrder.get(k).equals(attTableName)) {
                    index = k;
                    break;
                }
            }
            //group by后跟多个属性，比较麻烦，目前先暂定一个属性
            int finalIndex = index;

            //resultMap = key4Join.stream().collect(Collectors.groupingBy(x -> x.getData(finalIndex, groupByAttr.get(0)), );
            resultMap = key4Join.stream().collect(Collectors.groupingBy(x -> x.getData(finalIndex, groupByAttr.get(0)), TreeMap::new, Collectors.toList()));

            if(this.orderBys.size()>0){
                if(this.orderBys.get(0).getOrderByType()== OrderByType.ASC){
                    resultMap = key4Join.stream().collect(Collectors.groupingBy(x -> x.getData(finalIndex, groupByAttr.get(0)), TreeMap::new, Collectors.toList()));
                }else{
                    resultMap = key4Join.stream().collect(Collectors.groupingBy(x -> x.getData(finalIndex, groupByAttr.get(0)), TreeMap::new, Collectors.toList())).descendingMap();

                }
            }
        }else{
            //没有group by算子，默认为1组
            resultMap.put(new BigDecimal(1),key4Join);
        }

        //最终只需要对resultMap操作即可
        if (this.select instanceof Aggregation) {
            //保存最终的查询结果
            RecordLog.recordLog(LogLevelConstant.INFO, "该Query为聚合运算Query");

            List<String[]> results = new ArrayList<>();
            String[] headName = new String[1];
            headName[0] = "result";
            results.add(headName);
            //group by前，为一个组
            //group by后，为多个组
            String[] singleResult;
            singleResult = new String[resultMap.size()];

            Aggregation aggregation = (Aggregation) this.select;
            //得到聚合运算的表达式
            //sum( - table_0_3.col_4 / 5 + table_0_3.col_5 / 4 - 844)
            //得到的是- table_0_3.col_4 / 5 + table_0_3.col_5 / 4 - 844
            System.out.println("聚合表达式：" + aggregation.getExpression().getText());

            List<Attribute> aggregationAttributes = aggregation.getExpression().getVariables();//eg. table_0_7.col_17
            List<BigDecimal> coffs = aggregation.getExpression().getCoffs();
            List<Boolean> isPosts = aggregation.getExpression().getIsPosts();
            List<Boolean> isMultiplys = aggregation.getExpression().getIsMultiplys();
            System.out.println("聚合表达式涉及的属性:");
            for (Attribute singleAttname : aggregationAttributes) {

                System.out.print(singleAttname.getFullAttrName() + " ");
            }
            System.out.println();

            if (aggregation.getAggregationType() == AggregatioinType.SUM || aggregation.getAggregationType() == AggregatioinType.AVG
                    || aggregation.getAggregationType() == AggregatioinType.MAX || aggregation.getAggregationType() == AggregatioinType.MIN) {
                int u=0;
                for(List<ResultRow> value : resultMap.values()){
                    if(value.size()==0) {
                        singleResult[u++] = "NULL";
                    }else{
                        BigDecimal sum = new BigDecimal("0");
                        BigDecimal max = null;
                        BigDecimal min = null;
                        // 根据聚合表达式，计算出对应所有连接结果的表达式的值，并进行求和
                        for (ResultRow resultRow : value) {
                            List<Integer> keys = resultRow.getRowData();
                            BigDecimal singleTupleAggrResult = BigDecimal.ZERO;
                            // 根据连接结果计算出每一个聚合属性的值，进而求出聚合表达式的值
                            for (int j = 0; j < aggregationAttributes.size(); j++) {
                                // 找到该属性对应的数据表在连接结果行中的位置
                                String attTableName = aggregationAttributes.get(j).getTableName();//聚合运算中涉及到的表
                                int index = 0;
                                //假如 index为3，该表就是第三个被连接的
                                for (int k = 0; k < tableOrder.size(); k++) {
                                    if (tableOrder.get(k).equals(attTableName)) {
                                        index = k;
                                        break;
                                    }
                                }

                                int attId = keys.get(index);  // 该属性对应的连接tuple主键值


                                Attribute singleAggregation = aggregationAttributes.get(j);

                                if (isPosts.get(j)) {//如果是+
                                    if (isMultiplys.get(j)) {
                                        singleTupleAggrResult = singleTupleAggrResult.add(coffs.get(j).multiply(singleAggregation.attrEvaluate(attId)));
                                    } else {
                                        singleTupleAggrResult = singleTupleAggrResult.add(singleAggregation.attrEvaluate(attId).divide(coffs.get(j), 6));
                                    }
                                } else {//如果是-
                                    if (isMultiplys.get(j)) {
                                        singleTupleAggrResult = singleTupleAggrResult.subtract(coffs.get(j).multiply(singleAggregation.attrEvaluate(attId)));
                                    } else {
                                        singleTupleAggrResult = singleTupleAggrResult.subtract(singleAggregation.attrEvaluate(attId).divide(coffs.get(j), 6));
                                    }
                                }
                            }
                            if (max == null) {
                                max = singleTupleAggrResult;
                            } else {
                                if (singleTupleAggrResult.compareTo(max) > 0) {
                                    max = singleTupleAggrResult;
                                }
                            }
                            if (min == null) {
                                min = singleTupleAggrResult;
                            } else {
                                if (singleTupleAggrResult.compareTo(min) < 0) {
                                    min = singleTupleAggrResult;
                                }
                            }
                            sum = sum.add(singleTupleAggrResult);
                        }
                        if (aggregation.getAggregationType() == AggregatioinType.SUM) {
                            System.out.println("Sum结果是");
                            System.out.println(sum);
                            singleResult[u++] = sum.toString();
                        } else if (aggregation.getAggregationType() == AggregatioinType.AVG) {
                            System.out.println("Avg结果是");
                            BigDecimal avg = sum.divide(new BigDecimal(key4Join.size()), 6, BigDecimal.ROUND_HALF_UP);
                            System.out.println(avg);
                            singleResult[u++] = avg.toString();
                        } else if (aggregation.getAggregationType() == AggregatioinType.MIN) {
                            System.out.println("Min结果是");
                            System.out.println(min);
                            singleResult[u++] = min.toString();
                        } else if (aggregation.getAggregationType() == AggregatioinType.MAX) {
                            System.out.println("Max结果是");
                            System.out.println(max);
                            singleResult[u++] = max.toString();
                        }
                    }
                }
            } else if (aggregation.getAggregationType() == AggregatioinType.COUNT) {
                System.out.println("Count值为：");
                int u=0;
                for(List<ResultRow> value : resultMap.values()){
                    singleResult[u++] = String.valueOf(value.size());
                }
            }
            //aggregation计算已经结束
            for(String single : singleResult) {
                results.add(new String[]{single});
            }
            System.out.println("计算结果为：");
            for(String[] strings : results) {
                for(String s :strings)
                System.out.print(s);

                System.out.println();
            }
            this.computationTime = System.currentTimeMillis() - startGeneResult;
            // 计算结果写入指定文件下

            try (FileOutputStream fos = new FileOutputStream(calculateResultOutputDir);
                 OutputStreamWriter osw = new OutputStreamWriter(fos,
                         StandardCharsets.UTF_8);
                 CSVWriter writer = new CSVWriter(osw)) {
                writer.writeAll(results);
            }

            String CardinalityOutputDir = Configurations.getGenerateExactCard() + File.separator + "calculateCard.txt";
            //基数结果写入统一文件下方便记录
            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(CardinalityOutputDir, true), Configurations.getEncodeType()))) {
                bw.write(singleResult[0] + "\r\n");
                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (this.select instanceof SelectItems) {
            //如果是select Item的话
            RecordLog.recordLog(LogLevelConstant.INFO, "该Query不是聚合运算Query");

            List<String[]> results = new ArrayList<>();

            SelectItems selectItems = (SelectItems) this.select;
            //这里生成复杂表达式
            List<Attribute> selectAttributes = selectItems.getVariables();//涉及的属性

            List<BigDecimal> coffs = selectItems.getCoffs();//涉及的参数

            List<OperatorType> operatorTypes = selectItems.getOperatorTypes();//涉及的运算类型

            List<Boolean> isWithOperatorToken = selectItems.getIsWithOperatorToken();//是否有数据库功能算子

            List<operatorToken> operatorTokens = selectItems.getOperatorTokens();

            String[] headName = new String[selectAttributes.size()];
            for (int j = 0; j < selectAttributes.size(); j++) {
                headName[j] = "result_" + j;
            }
            results.add(headName);
            for(List<ResultRow> value : resultMap.values()) {
                String[] singleRowResult = new String[selectAttributes.size()];
                for (ResultRow resultRow : value) {
                    List<Integer> keyRows = resultRow.getRowData();
                    //对于Join结果的每一行
                    for (int j = 0; j < selectAttributes.size(); j++) {
                        // 找到该属性对应的数据表在连接结果keylist中的位置
                        String attTableName = selectAttributes.get(j).getTableName();//涉及到的表
                        int index = 0;
                        //假如 index为3，该表就是第三个被连接的
                        for (int k = 0; k < tableOrder.size(); k++) {
                            if (tableOrder.get(k).equals(attTableName)) {
                                index = k;
                                break;
                            }
                        }
                        int attId = keyRows.get(index);

                        Attribute singleSelectAttr = selectAttributes.get(j);
                        OperatorType operatorType = operatorTypes.get(j);
                        Boolean isWithToken = isWithOperatorToken.get(j);

                        operatorToken token = operatorTokens.get(j);
                        if (operatorType == OperatorType.NULL) {
                            if(isWithToken){
                                singleRowResult[j] = token.getResult(singleSelectAttr.attrEvaluate(attId));
                            } else{
                                singleRowResult[j] = AttrValue.tranString(singleSelectAttr.geneAttrValue(attId));
                            }
                        } else {
                            BigDecimal originalResult = singleSelectAttr.attrEvaluate(attId);
                            if(isWithToken){
                                originalResult = new BigDecimal(token.getResult(originalResult));
                            }
                            BigDecimal coff = coffs.get(j);
                            if (operatorType == OperatorType.ADD) {
                                originalResult = originalResult.add(coff);
                            } else if (operatorType == OperatorType.SUB) {
                                originalResult = originalResult.subtract(coff);
                            } else if (operatorType == OperatorType.MUL) {
                                originalResult = originalResult.multiply(coff);
                            } else if (operatorType == OperatorType.MOD) {
                                originalResult = originalResult.remainder(coff);
                            }
                            singleRowResult[j] = originalResult.toString();
                        }
                    }

                }
                results.add(singleRowResult);
            }
            System.out.println("计算结果为：");
            for(String[] strings : results) {
                for(String s :strings)
                    System.out.print(s);
                System.out.println();
            }

            this.computationTime = System.currentTimeMillis() - startGeneResult;

            try (FileOutputStream fos = new FileOutputStream(calculateResultOutputDir);
                 OutputStreamWriter osw = new OutputStreamWriter(fos,
                         StandardCharsets.UTF_8);
                 CSVWriter writer = new CSVWriter(osw)) {
                writer.writeAll(results);
            }
        }

    }


    public void geneResult() throws Exception {


        long startGeneResult = System.currentTimeMillis();
        // 保存最终结果的所有表
        List<Table> tables = new ArrayList<>();
        // 保存最终结果的表的相对应的元组Id集合
        List<ResultRow> key4Join = new ArrayList<>();
        //这里只是Join的结果
        //还需做剩下的

        // query中只包含一个表
        if (queryTree.getTables().size() == 1) {
            System.out.println("单表查询！表大小为：" + queryTree.getTables().get(0).getTableSize());
            // 单表查询的最终结果会保存在一个FilterResult中
            FilterResult finalFilterResult = null;
            if (queryTree.getRoot() instanceof Filter) {
                finalFilterResult = queryTree.getFilterResults()[0];
            } else if (queryTree.getRoot() instanceof TableNode) {
                finalFilterResult = null;
            }

            tables.add(queryTree.getRoot().getTable());//为了求k的范围

            List<SingleConstraint> constraints = new ArrayList<>();
            if (finalFilterResult != null) {
                constraints.addAll(finalFilterResult.getResult());
            }

            ComparisonOperatorType comparisonOperatorType = this.queryTree.getFilters().get(0).getPredicateList().get(0).getComparisonOperator();

            long ourSolverStart = System.currentTimeMillis();
            ConstraintGroup ourSolver;
            if (comparisonOperatorType == ComparisonOperatorType.In) {
                ourSolver = new ConstraintGroup(tables.get(0), 0, tables.get(0).getTableSize() - 1, constraints, LogicRelation.OR, 0);
            } else {
                ourSolver = new ConstraintGroup(tables.get(0), 0, tables.get(0).getTableSize() - 1, constraints, LogicRelation.AND, 0);
            }

            ResultSet set = ourSolver.getResultSet();
            for (SingleConstraint singleTableConstraint : constraints) {
                RecordLog.recordLog(LogLevelConstant.INFO, singleTableConstraint.toString());
            }
            long filterGenTime = System.currentTimeMillis() - ourSolverStart;
            this.singleFilterGenerateTime = filterGenTime;
            System.out.println("Filter求解器花费时间" + filterGenTime);

            for (int id = set.getLowerBound(); id <= set.getUpperBound(); id++) {
                List<Integer> keyList = new ArrayList<>();
                keyList.add(id);
                key4Join.add(new ResultRow(keyList));
            }
            queryTree.setFinalResultSize(set.getKeyList().size());
        } else if (queryTree.getTables().size() > 1) {
            CSPDefination origin = queryTree.getJoinResults()[queryTree.getTableOrder().size() - 2].getCspDefination();
            CSPDefination finalCsp = new CSPDefination(origin.getBiggestTable(), origin.getVariableList(), origin.getDomainList(), origin.getJoinConditionList());
            finalCsp.solve(queryTree.getTableOrder().toArray(new String[0]));//将K按照TableOrder的顺序排序
            key4Join = finalCsp.getKey4Join();
            queryTree.setFinalResultSize(finalCsp.getCardinality());
        }


        System.out.println("全部连接之后产生的KeySet大小：" + key4Join.size());

        List<String> tableOrder = new ArrayList<>();
        //如果表的数量为1的时候，只有一个表
        if (queryTree.getTables().size() == 1) {
            tableOrder.add(queryTree.getTables().get(0).getTableName());
        } else {
            tableOrder = queryTree.getTableOrder();
        }

        System.out.println("tableOrder是:");
        for (String tableInOrder : tableOrder) {

            System.out.println(tableInOrder);
        }
        System.out.println("tableOrder展示结束————————:");


        //求group by,对key4Join进行group by运算
        Map<BigDecimal, List<ResultRow>> resultMap = new HashMap<>();
        if (this.groupBys.size() > 0) {
            List<Attribute> groupByAttr = new ArrayList<>();
            for (GroupBy groupBy : this.groupBys) {
                groupByAttr.add(groupBy.getColumnName());
            }

            String attTableName = groupByAttr.get(0).getTableName();//聚合运算中涉及到的表
            int index = 0;

            for (int k = 0; k < tableOrder.size(); k++) {
                if (tableOrder.get(k).equals(attTableName)) {
                    index = k;
                    break;
                }
            }
            //group by后跟多个属性，比较麻烦，目前先暂定一个属性
            int finalIndex = index;

            //resultMap = key4Join.stream().collect(Collectors.groupingBy(x -> x.getData(finalIndex, groupByAttr.get(0)), );
            resultMap = key4Join.stream().collect(Collectors.groupingBy(x -> x.getData(finalIndex, groupByAttr.get(0)), TreeMap::new, Collectors.toList()));

            if(this.orderBys.size()>0){
                if(this.orderBys.get(0).getOrderByType()== OrderByType.ASC){
                    resultMap = key4Join.stream().collect(Collectors.groupingBy(x -> x.getData(finalIndex, groupByAttr.get(0)), TreeMap::new, Collectors.toList()));
                }else{
                    resultMap = key4Join.stream().collect(Collectors.groupingBy(x -> x.getData(finalIndex, groupByAttr.get(0)), TreeMap::new, Collectors.toList())).descendingMap();

                }
            }
        }else{
            //没有group by算子，默认为1组
            resultMap.put(new BigDecimal(1),key4Join);
        }

        //最终只需要对resultMap操作即可
        if (this.select instanceof Aggregation) {
            //保存最终的查询结果
            RecordLog.recordLog(LogLevelConstant.INFO, "该Query为聚合运算Query");

            List<String[]> results = new ArrayList<>();
            String[] headName = new String[1];
            headName[0] = "result";
            results.add(headName);
            //group by前，为一个组
            //group by后，为多个组
            String[] singleResult;
            singleResult = new String[resultMap.size()];

            Aggregation aggregation = (Aggregation) this.select;
            //得到聚合运算的表达式
            //sum( - table_0_3.col_4 / 5 + table_0_3.col_5 / 4 - 844)
            //得到的是- table_0_3.col_4 / 5 + table_0_3.col_5 / 4 - 844
            System.out.println("聚合表达式：" + aggregation.getExpression().getText());

            List<Attribute> aggregationAttributes = aggregation.getExpression().getVariables();//eg. table_0_7.col_17
            List<BigDecimal> coffs = aggregation.getExpression().getCoffs();
            List<Boolean> isPosts = aggregation.getExpression().getIsPosts();
            List<Boolean> isMultiplys = aggregation.getExpression().getIsMultiplys();
            System.out.println("聚合表达式涉及的属性:");
            for (Attribute singleAttname : aggregationAttributes) {

                System.out.print(singleAttname.getFullAttrName() + " ");
            }
            System.out.println();

            if (aggregation.getAggregationType() == AggregatioinType.SUM || aggregation.getAggregationType() == AggregatioinType.AVG
                    || aggregation.getAggregationType() == AggregatioinType.MAX || aggregation.getAggregationType() == AggregatioinType.MIN) {
                int u=0;
                for(List<ResultRow> value : resultMap.values()){
                    if(value.size()==0) {
                        singleResult[u++] = "NULL";
                    }else{
                        BigDecimal sum = new BigDecimal("0");
                        BigDecimal max = null;
                        BigDecimal min = null;
                        // 根据聚合表达式，计算出对应所有连接结果的表达式的值，并进行求和
                        for (ResultRow resultRow : value) {
                            List<Integer> keys = resultRow.getRowData();
                            BigDecimal singleTupleAggrResult = BigDecimal.ZERO;
                            // 根据连接结果计算出每一个聚合属性的值，进而求出聚合表达式的值
                            for (int j = 0; j < aggregationAttributes.size(); j++) {
                                // 找到该属性对应的数据表在连接结果行中的位置
                                String attTableName = aggregationAttributes.get(j).getTableName();//聚合运算中涉及到的表
                                int index = 0;
                                //假如 index为3，该表就是第三个被连接的
                                for (int k = 0; k < tableOrder.size(); k++) {
                                    if (tableOrder.get(k).equals(attTableName)) {
                                        index = k;
                                        break;
                                    }
                                }

                                int attId = keys.get(index);  // 该属性对应的连接tuple主键值


                                Attribute singleAggregation = aggregationAttributes.get(j);

                                if (isPosts.get(j)) {//如果是+
                                    if (isMultiplys.get(j)) {
                                        singleTupleAggrResult = singleTupleAggrResult.add(coffs.get(j).multiply(singleAggregation.attrEvaluate(attId)));
                                    } else {
                                        singleTupleAggrResult = singleTupleAggrResult.add(singleAggregation.attrEvaluate(attId).divide(coffs.get(j), 6));
                                    }
                                } else {//如果是-
                                    if (isMultiplys.get(j)) {
                                        singleTupleAggrResult = singleTupleAggrResult.subtract(coffs.get(j).multiply(singleAggregation.attrEvaluate(attId)));
                                    } else {
                                        singleTupleAggrResult = singleTupleAggrResult.subtract(singleAggregation.attrEvaluate(attId).divide(coffs.get(j), 6));
                                    }
                                }
                            }
                            if (max == null) {
                                max = singleTupleAggrResult;
                            } else {
                                if (singleTupleAggrResult.compareTo(max) > 0) {
                                    max = singleTupleAggrResult;
                                }
                            }
                            if (min == null) {
                                min = singleTupleAggrResult;
                            } else {
                                if (singleTupleAggrResult.compareTo(min) < 0) {
                                    min = singleTupleAggrResult;
                                }
                            }
                            sum = sum.add(singleTupleAggrResult);
                        }
                        if (aggregation.getAggregationType() == AggregatioinType.SUM) {
                            System.out.println("Sum结果是");
                            System.out.println(sum);
                            singleResult[u++] = sum.toString();
                        } else if (aggregation.getAggregationType() == AggregatioinType.AVG) {
                            System.out.println("Avg结果是");
                            BigDecimal avg = sum.divide(new BigDecimal(key4Join.size()), 6, BigDecimal.ROUND_HALF_UP);
                            System.out.println(avg);
                            singleResult[u++] = avg.toString();
                        } else if (aggregation.getAggregationType() == AggregatioinType.MIN) {
                            System.out.println("Min结果是");
                            System.out.println(min);
                            singleResult[u++] = min.toString();
                        } else if (aggregation.getAggregationType() == AggregatioinType.MAX) {
                            System.out.println("Max结果是");
                            System.out.println(max);
                            singleResult[u++] = max.toString();
                        }
                    }
                }
            } else if (aggregation.getAggregationType() == AggregatioinType.COUNT) {
                System.out.println("Count值为：");
                int u=0;
                for(List<ResultRow> value : resultMap.values()){
                    singleResult[u++] = String.valueOf(value.size());
                }
            }
            //aggregation计算已经结束
            for(String single : singleResult) {
                results.add(new String[]{single});
            }
            System.out.println("计算结果为：");
            for(String[] strings : results) {
                for(String s :strings)
                System.out.print(s);

                System.out.println();
            }
            this.computationTime = System.currentTimeMillis() - startGeneResult;
            // 计算结果写入指定文件下

            String CardinalityOutputDir = Configurations.getGenerateExactCard() + File.separator + "calculateCard.txt";
            //基数结果写入统一文件下方便记录
            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(CardinalityOutputDir, true), Configurations.getEncodeType()))) {
                bw.write(singleResult[0] + "\r\n");
                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (this.select instanceof SelectItems) {
            //如果是select Item的话
            RecordLog.recordLog(LogLevelConstant.INFO, "该Query不是聚合运算Query");

            List<String[]> results = new ArrayList<>();

            SelectItems selectItems = (SelectItems) this.select;
            //这里生成复杂表达式
            List<Attribute> selectAttributes = selectItems.getVariables();//涉及的属性

            List<BigDecimal> coffs = selectItems.getCoffs();//涉及的参数

            List<OperatorType> operatorTypes = selectItems.getOperatorTypes();//涉及的运算类型

            List<Boolean> isWithOperatorToken = selectItems.getIsWithOperatorToken();//是否有数据库功能算子

            List<operatorToken> operatorTokens = selectItems.getOperatorTokens();

            String[] headName = new String[selectAttributes.size()];
            for (int j = 0; j < selectAttributes.size(); j++) {
                headName[j] = "result_" + j;
            }
            results.add(headName);
            for(List<ResultRow> value : resultMap.values()) {
                String[] singleRowResult = new String[selectAttributes.size()];
                for (ResultRow resultRow : value) {
                    List<Integer> keyRows = resultRow.getRowData();
                    //对于Join结果的每一行
                    for (int j = 0; j < selectAttributes.size(); j++) {
                        // 找到该属性对应的数据表在连接结果keylist中的位置
                        String attTableName = selectAttributes.get(j).getTableName();//涉及到的表
                        int index = 0;
                        //假如 index为3，该表就是第三个被连接的
                        for (int k = 0; k < tableOrder.size(); k++) {
                            if (tableOrder.get(k).equals(attTableName)) {
                                index = k;
                                break;
                            }
                        }
                        int attId = keyRows.get(index);

                        Attribute singleSelectAttr = selectAttributes.get(j);
                        OperatorType operatorType = operatorTypes.get(j);
                        Boolean isWithToken = isWithOperatorToken.get(j);

                        operatorToken token = operatorTokens.get(j);
                        if (operatorType == OperatorType.NULL) {
                            if(isWithToken){
                                singleRowResult[j] = token.getResult(singleSelectAttr.attrEvaluate(attId));
                            } else{
                                singleRowResult[j] = AttrValue.tranString(singleSelectAttr.geneAttrValue(attId));
                            }
                        } else {
                            BigDecimal originalResult = singleSelectAttr.attrEvaluate(attId);
                            if(isWithToken){
                                originalResult = new BigDecimal(token.getResult(originalResult));
                            }
                            BigDecimal coff = coffs.get(j);
                            if (operatorType == OperatorType.ADD) {
                                originalResult = originalResult.add(coff);
                            } else if (operatorType == OperatorType.SUB) {
                                originalResult = originalResult.subtract(coff);
                            } else if (operatorType == OperatorType.MUL) {
                                originalResult = originalResult.multiply(coff);
                            } else if (operatorType == OperatorType.MOD) {
                                originalResult = originalResult.remainder(coff);
                            }
                            singleRowResult[j] = originalResult.toString();
                        }
                    }

                }
                results.add(singleRowResult);
            }
            System.out.println("计算结果为：");
            for(String[] strings : results) {
                for(String s :strings)
                    System.out.print(s);
                System.out.println();
            }

            this.computationTime = System.currentTimeMillis() - startGeneResult;
        }

    }

    public long getComputationTime() {
        return computationTime;
    }

    public long getFilterGeneTime() {
        return singleFilterGenerateTime;
    }

    public int getTableNum() {
        return this.queryTree.getTables().size();
    }
}