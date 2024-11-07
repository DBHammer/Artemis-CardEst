package ecnu.dbhammer.query;

import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.data.AttrValue;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.query.type.ComparisonOperatorType;
import ecnu.dbhammer.query.type.JoinMode;
import ecnu.dbhammer.result.FilterResult;
import ecnu.dbhammer.result.JoinResult;
import ecnu.dbhammer.result.ResultRow;
import ecnu.dbhammer.schema.*;
import ecnu.dbhammer.solver.FilterCardinalitySolver;
import ecnu.dbhammer.solver.JoinCondition;
import org.apache.commons.lang3.tuple.Pair;
import org.jgrapht.graph.Pseudograph;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
Filter生成器
第一个Filter随机生成
第二的Filter使用第一个Filter过滤出的数据，求得范围，在范围里选择参数
第三个Filter基于第一个Fliter和第二个Filter连接后的结果
依次类推
 */
public class FilterGenerator {

    private List<Table> joinedTableOrder;

    private List<List<Pair<Attribute, Attribute>>> preJoinRelation;

    private List<Filter> filters;// FilterGenerator最终生成的结果

    private List<List<Filter>> allFilters;

    private FilterResult[] filterResults;// filterResult

    private JoinResult[] joinResults;

    private List<Integer> filterCardinality;// 用来存储表filter后的结果基数

    private List<Integer> joinCardinality;// 存储join的中间结果

    private Pseudograph<String, JoinCondition> joinGraph;

    private QueryNode preRoot;

    private List<List<Integer>> finalKey4Join;

    private Map<String, Column> table2PredicateCol;// 为了表间关联记录,指定构建谓词的属性列（驱动列）

    private double[] params;

    private int targetCard;
    private List<List<Predicate>> predicateLists;
    // population[i]：表i， population[i][i]：表i上的第i个谓词，
    // population[i][i][i]：表i上的第i个谓词的第i组参数
    private List<List<List<AttrValue[]>>> population;
    private List<List<List<int[]>>> populationPKRanges;
    private double[] fitnesses;

    private static int NUM_GENERATIONS = 10;
    public static final int POP_SIZE = 14;
    private static int SELECTION_SIZE = POP_SIZE / 2;
    private static int TOURNAMENT_SIZE = 4;

    public FilterGenerator(List<Table> joinedTableOrder, List<List<Pair<Attribute, Attribute>>> preJoinRelation,
                           Pseudograph<String, JoinCondition> joinGraph, double[] params, int targetCard) {
        this.joinedTableOrder = joinedTableOrder;
        this.preJoinRelation = preJoinRelation;
        this.joinGraph = joinGraph;
        this.filters = new ArrayList<>();
        this.filterResults = new FilterResult[joinedTableOrder.size()];
        this.joinResults = new JoinResult[joinedTableOrder.size() - 1];
        this.filterCardinality = new ArrayList<>();
        this.joinCardinality = new ArrayList<>();
        this.finalKey4Join = new ArrayList<>();
        this.table2PredicateCol = new LinkedHashMap<>();
        this.params = params;
        this.targetCard = targetCard;
        this.predicateLists = new ArrayList<>();
        this.population = new ArrayList<>();
        this.populationPKRanges = new ArrayList<>();
    }

    public int getBestIndex() {
        int bestIndex = 0;
        for (int i = 1; i < fitnesses.length; i++) {
            if (fitnesses[i] < fitnesses[bestIndex]) {
                bestIndex = i;
            }
        }
        return bestIndex;
    }

    // generateFilters and init param
    public void GeneratePredicateExpressionsAndInitPopulation() {
        predicateLists.clear();
        population.clear();
        populationPKRanges.clear();
        for (int index = 0; index < joinedTableOrder.size(); index++) {
            Table table = joinedTableOrder.get(index);
            // TODO: Add more predicate for a table, also contain `and` / `or`
            List<Predicate> predicates = new ArrayList<>();
            List<List<AttrValue[]>> predicatesParams = new ArrayList<>();
            List<List<int[]>> predicatesPKRanges = new ArrayList<>();
            Predicate predicate = new Predicate(table, params[index]);
            predicates.add(predicate);
            predicatesParams.add(predicate.getInitParams());
            predicatesPKRanges.add(predicate.getPkRangesList());
            predicateLists.add(predicates);
            population.add(predicatesParams);
            populationPKRanges.add(predicatesPKRanges);
        }
    }

    public void algorithm2() throws Exception {
        GeneratePredicateExpressionsAndInitPopulation();

        for (int generation = 0; generation < NUM_GENERATIONS; generation++) {
            fitnesses = new double[POP_SIZE];
            for (int i = 0; i < POP_SIZE; i++) {
                fitnesses[i] = fitnessFunction2(i, false);
            }
            int bestIndex = getBestIndex();
            // 差距在一个数量级以内
            if (fitnesses[bestIndex] <= 10) {
                fitnessFunction2(bestIndex, false);
                break;
            }
            tournamentSelection();
            crossover();
            mutation2();
        }

        for (int i = 0; i < POP_SIZE; i++) {
            fitnesses[i] = fitnessFunction2(i, false);
            System.out.println("fit=" + fitnesses[i]);
        }

        int bestIndex = getBestIndex();
        if (fitnesses[bestIndex] == Double.MAX_VALUE) {
            System.err.println("can't find params");
        } else {
            System.out.println("best fit " + bestIndex + " =" + fitnesses[bestIndex]);
            fitnessFunction2(bestIndex, false);
        }
    }

    public void mutation2() throws Exception {
        Random random = new Random();
        for (int i = 0; i < predicateLists.size(); i++) {
            for (int j = 0; j < predicateLists.get(i).size(); j++) {
                for (int k = SELECTION_SIZE; k < POP_SIZE; k++) {
                    Predicate p = predicateLists.get(i).get(j);
                    if (p.getComparisonOperator() == ComparisonOperatorType.BetweenAnd) {
                        double param = random.nextDouble();
                        double start = random.nextDouble() * (1 - param);
                        double end = start + param;
                        p.geneParametersInGene(p.getTable(), p.getExpression(), ComparisonOperatorType.BetweenAnd,
                                start,
                                end);
                    } else {
                        double param = random.nextDouble();
                        p.geneParameterInGene(p.getTable(), p.getExpression(), p.getComparisonOperator(), param);

                    }
                    population.get(i).get(j).set(k, p.getParameters());
                    populationPKRanges.get(i).get(j).set(k,
                            p.getPkRangesList().get(p.getPkRangesList().size() - 1));
                }
            }
        }
    }

    public void crossover() throws Exception {
        int offspringSize = POP_SIZE - SELECTION_SIZE;

        int[][] indexes = new int[offspringSize][2];
        for (int k = 0; k < offspringSize; k++) {
            int parent1Idx = k % SELECTION_SIZE;
            int parent2Idx = (k + 1) % SELECTION_SIZE;
            indexes[k][0] = parent1Idx;
            indexes[k][1] = parent2Idx;
        }
        crossoverCopy(indexes);
    }

    public void tournamentSelection() throws Exception {
        Random rand = new Random();
        int[] indexes = new int[SELECTION_SIZE];
        // 保留优秀父代（目前是锦标赛法）
        for (int i = 0; i < SELECTION_SIZE; i++) {
            int bestIndex = 0;
            double bestFitness = Double.MAX_VALUE;
            for (int j = 0; j < TOURNAMENT_SIZE; j++) {
                int idx = rand.nextInt(POP_SIZE);
                if (fitnesses[idx] < bestFitness) {
                    bestFitness = fitnesses[idx];
                    bestIndex = idx;
                }
            }
            indexes[i] = bestIndex;
        }
        selectionCopy(indexes);
    }

    public void crossoverCopy(int[][] indexes) throws Exception {
        // 目前是从某张表开始切分，单点交叉
        int crossoverPoint = population.size() / 2;
        for (int i = 0; i < predicateLists.size(); i++) {
            for (int j = 0; j < predicateLists.get(i).size(); j++) {
                for (int idx = 0; idx < indexes.length; idx++) {
                    int parent1Idx = indexes[idx][0];
                    int parent2Idx = indexes[idx][1];
                    AttrValue[] parent1Param = population.get(i).get(j).get(parent1Idx);
                    AttrValue[] parent2Param = population.get(i).get(j).get(parent2Idx);
                    int[] parent1PKRange = populationPKRanges.get(i).get(j).get(parent1Idx);
                    int[] parent2PKRange = populationPKRanges.get(i).get(j).get(parent2Idx);
                    if (i < crossoverPoint) {
                        population.get(i).get(j).add(parent1Param);
                        population.get(i).get(j).add(parent2Param);
                        populationPKRanges.get(i).get(j).add(parent1PKRange);
                        populationPKRanges.get(i).get(j).add(parent2PKRange);
                    } else {
                        population.get(i).get(j).add(parent2Param);
                        population.get(i).get(j).add(parent1Param);
                        populationPKRanges.get(i).get(j).add(parent2PKRange);
                        populationPKRanges.get(i).get(j).add(parent1PKRange);
                    }
                }
            }
        }
    }

    public void selectionCopy(int[] indexes) throws Exception {
        List<List<List<AttrValue[]>>> newPopulation = new ArrayList<>();
        List<List<List<int[]>>> newPopulationPKRanges = new ArrayList<>();
        for (int i = 0; i < predicateLists.size(); i++) {
            // 每张表的每个谓词的参数
            List<List<AttrValue[]>> eachTablePredicatesParams = new ArrayList<>();
            List<List<int[]>> eachTablePredicatesPKRanges = new ArrayList<>();
            for (int j = 0; j < predicateLists.get(i).size(); j++) {
                List<AttrValue[]> eachPredicateParams = new ArrayList<>();
                List<int[]> eachPredicatePKRanges = new ArrayList<>();
                for (int index : indexes) {
                    eachPredicateParams.add(population.get(i).get(j).get(index));
                    eachPredicatePKRanges.add(populationPKRanges.get(i).get(j).get(index));
                }
                eachTablePredicatesParams.add(eachPredicateParams);
                eachTablePredicatesPKRanges.add(eachPredicatePKRanges);
            }
            newPopulation.add(eachTablePredicatesParams);
            newPopulationPKRanges.add(eachTablePredicatesPKRanges);
        }
        population = newPopulation;
        populationPKRanges = newPopulationPKRanges;
    }

    public double fitnessFunction2(int index, boolean flag) throws Exception {
        filters.clear();
        List<List<Integer>> keyRanges = new LinkedList<>();
        // each table
        for (int i = 0; i < predicateLists.size(); i++) {
            Table table = joinedTableOrder.get(i);
            int[] PKrange = new int[2];
            // each table's predicates
            for (int j = 0; j < predicateLists.get(i).size(); j++) {
                Predicate predicate = predicateLists.get(i).get(j);
                AttrValue[] param = population.get(i).get(j).get(index);
                predicate.setParameters(param);
                predicate.constructText();
                if (j == 0) {
                    PKrange = populationPKRanges.get(i).get(0).get(index);
                } else {
                    // TODO: 合并范围
                }
            }
            Filter filter = new Filter(predicateLists.get(i), table);
            filters.add(filter);

            if (i == 0) {
                for (int key = PKrange[0]; key < PKrange[1]; key++) {
                    List<Integer> row = new ArrayList<>();
                    row.add(new Integer(key));
                    keyRanges.add(row);
                }
                // System.out.println("i = 0,[" + keyRanges.get(0).get(0) + ", "
                // + keyRanges.get(keyRanges.size() - 1).get(0) + ",] size=" +
                // keyRanges.size());
            } else {
                // Begin to get 连接条件
                int joinCnt = i - 1;
                Attribute joinFkColumn = null;
                Attribute joinPkColumn = null;

                List<String> lastTables = new ArrayList<>();
                List<Integer> lastTablesIndex = new ArrayList<>();
                // 新来的表可能和已有的连接有着多个连接条件 记录上一个表是什么
                for (int j = 0; j < preJoinRelation.get(joinCnt).size(); j++) {
                    if (preJoinRelation.get(joinCnt).get(j).getKey() instanceof ForeignKey) {
                        joinFkColumn = preJoinRelation.get(joinCnt).get(j).getKey();
                        joinPkColumn = preJoinRelation.get(joinCnt).get(j).getValue();
                    } else {
                        joinFkColumn = preJoinRelation.get(joinCnt).get(j).getValue();
                        joinPkColumn = preJoinRelation.get(joinCnt).get(j).getKey();
                    }
                    // 判断是在这次join提供主键还是外键
                    if (table.getTableName().equals(joinPkColumn.getTableName())) {
                        lastTables.add(joinFkColumn.getTableName());
                    } else if (table.getTableName().equals(joinFkColumn.getTableName())) {// 新表提供fk
                        lastTables.add(joinPkColumn.getTableName());
                    }
                }
                for (String lastTable : lastTables) {
                    for (int j = 0; j < i; j++) {
                        if (joinedTableOrder.get(j).getTableName().equals(lastTable)) {
                            lastTablesIndex.add(j);
                            break;
                        }
                    }
                }

                List<List<Integer>> newKeyRanges = new ArrayList<>();
                for (List<Integer> lastResultRow : keyRanges) {
                    // 当前参加Join的表计算完所有join关系后的PK范围
                    List<Integer> finalIdealKeysEachRow = new ArrayList<>();
                    List<List<Integer>> idealKeys = new ArrayList<>();
                    for (int j = 0; j < lastTables.size(); j++) {
                        String baseTable = lastTables.get(j);
                        int baseKey = lastResultRow.get(lastTablesIndex.get(j));

                        JoinCondition joinCondition = this.joinGraph.getEdge(baseTable, table.getTableName());
                        if (joinCondition.judge() == JoinMode.PK2FK) {
                            if (joinCondition.getLeftAttribute().getTableName().equals(baseTable)) {
                                ForeignKey foreignKey = (ForeignKey) joinCondition.getLeftAttribute();
                                idealKeys.add(foreignKey.getNextKeyRange(baseKey));// 上一个表为fk表 正向求
                            } else if (joinCondition.getLeftAttribute().getTableName().equals(table.getTableName())) {
                                ForeignKey foreignKey = (ForeignKey) joinCondition.getLeftAttribute();
                                idealKeys.add(foreignKey.getReverseKeyRange(baseKey));// 上一个表为pk表 反向求
                            }
                        }
                    }

                    finalIdealKeysEachRow.addAll(idealKeys.get(0));

                    // 求解所有主键的交集得到最终结果
                    for (int j = 1; j < lastTables.size(); j++) {
                        finalIdealKeysEachRow = intersectList(finalIdealKeysEachRow, idealKeys.get(j));// 先拿到idealKeys[0],再与所有的idealKeys[]求交集即是每一行的最终idealKey
                    }

                    for (Integer row : finalIdealKeysEachRow) {
                        List<Integer> eachRowResult = new ArrayList<>(lastResultRow); // 深拷贝lastResultRow
                        // 判断此PK是否在filter后的[min, max]范围内
                        if (row.compareTo(PKrange[0]) > 0 && row.compareTo(PKrange[1]) < 0) {
                            eachRowResult.add(row);
                            newKeyRanges.add(eachRowResult);
                        }
                    }
                }
                if (newKeyRanges.size() == 0) {
                    return Double.MAX_VALUE;
                }
                keyRanges = newKeyRanges;
                // System.out.println("i != 0,[" + keyRanges.get(0).get(i) + ", "
                // + keyRanges.get(keyRanges.size() - 1).get(i) + ",] size=" +
                // keyRanges.size());
            }
        }
        int finalCard = keyRanges.size();
        if (flag) {
            allFilters.add(filters);
        }
        return Math.max(finalCard / targetCard, targetCard / finalCard);
    }


    public List<List<Integer>> GenerateFirstFilter() throws Exception {

        RecordLog.recordLog(LogLevelConstant.INFO, "开始生成第一个Filter");
        // 第一个filter可在其对应表的主键范围内随机生成，但是为了过滤出更多的数据，这里将其数据全部过滤出来
        Table table = joinedTableOrder.get(0);
        Predicate predicate = new Predicate(table, params[0]);// 传入一个数据表，然后依据随机策略生成一个该表上的选择过滤谓词
        Filter firstFilter = new Filter(predicate, table);// 表有了 predicate有了 所以filter有了

        filters.add(firstFilter);

        RecordLog.recordLog(LogLevelConstant.INFO, "谓词为：" + predicate.getText());
        // 使用FilterResult计算出符合条件的Key值集合，再根据key直接的关系推导下一个Filter
        filterResults[0] = new FilterResult(firstFilter);

        preRoot = filterResults[0].getFilter();

        List<Integer> keyRange = FilterCardinalitySolver.Compute(filterResults[0], 0);
        System.out.println("第一个Filter过滤出主键基数：" + keyRange.size());// 这里输出的为过滤后的基表基数

        // RecordLog.recordLog(LogLevelConstant.INFO, "第一个Filter过滤出主键集合范围：[" +
        // keyRange.get(0) + " " + keyRange.get(keyRange.size() - 1) + "]");

        List<List<Integer>> keyRanges = new LinkedList<>();
        for (Integer key : keyRange) {
            List<Integer> row = new ArrayList<>();
            row.add(key);
            keyRanges.add(row);
        }
        filterCardinality.add(keyRange.size());
        return keyRanges;
    }

    /**
     * @param index
     * @param lastResultKeySet
     * @return
     * @throws Exception
     */
    // 传入上次生成的Filter范围，生成本次filter并Join后提供给下一个filter的范围，上次的Filter范围可以是多个表的
    public List<List<Integer>> GenerateMoreFilter(int index, List<List<Integer>> lastResultKeySet) throws Exception {

        RecordLog.recordLog(LogLevelConstant.INFO, "开始生成第" + index + "个Filter");
        Table table = joinedTableOrder.get(index - 1);// 要生成filter的表
        Table nextTable;
        if (index < joinedTableOrder.size()) {
            nextTable = joinedTableOrder.get(index);// 下次Join参与的表
        } else {
            nextTable = null;
        }

        // update by ct: 一张表上可生成多个filter
        List<PredicateExpression> expressions = new ArrayList<>();
        PredicateExpression expression;
        System.out.println("第" + index + "个表的名称:" + table.getTableName() + " 大小:" + table.getTableSize());

        // update by ct: 该表是否被指定了表间关联 驱动列谓词
        if (!table2PredicateCol.containsKey(table.getTableName())) {// 未指定，随机生成谓词
            while (true) {
                try {
                    // shuchu System.out.println("生成一个Expression");
                    if (index < joinedTableOrder.size()) {
                        expression = new PredicateExpression(table, false);// 生成一个where后面的表达式（根据选中的表） 保证生成成功
                    } else {
                        expression = new PredicateExpression(table, true);// 第一张和最后一张表不选关联列
                    }
                    System.out.println("该谓词的表达式为：" + expression.getText() + "类型为：" + expression.getType());
                    break;
                } catch (Exception ee) {
                    ee.printStackTrace();
                }
            }
        } else {// 指定构建谓词的属性列
            expression = new PredicateExpression(table2PredicateCol.get(table.getTableName()));
        }

        // update by ct
        // 若expression在数据关联列上，那么同时生成一个驱动列的filter（为了更好测试数据关联的影响）
        // 默认单属性的谓词才会涉及数据关联
        PredicateExpression expression1;
        List<Column> cols = expression.getCols();
        // 关联列 和 驱动列 一起生成谓词
        if (cols.get(0).getCorrelationFactor() != 0
                && cols.get(0).getTableName().equals(cols.get(0).getDrivingColumn().getTableName())) { // 表内
            expression1 = new PredicateExpression(cols.get(0).getDrivingColumn()); // 指定属性列构建谓词；cols.get(0)是关联列;
            expressions.add(expression1);// 先处理驱动列
            expressions.add(expression);
        } else if (cols.get(0).getCorrelationFactor() != 0) { // 表间
            for (int i = index; i < joinedTableOrder.size(); i++) { // 驱动列是否在未连接的表中；如果在就为那张表指定一个驱动列上谓词
                if (joinedTableOrder.get(i).getTableName().equals(cols.get(0).getDrivingColumn().getTableName())) {
                    table2PredicateCol.put(joinedTableOrder.get(i).getTableName(), cols.get(0).getDrivingColumn());
                    break;
                }
            }
            expressions.add(expression);
        } else {// 无数据关联，只生成一个谓词
            expressions.add(expression);
        }

        // Begin to get 连接条件
        int joinCnt = index - 2;
        Attribute joinFkColumn = null;
        Attribute joinPkColumn = null;
        Attribute leftJoinColumn = null;
        Attribute rightJoinColumn = null;
        Attribute nextJoinFkColumn = null;
        Attribute nextJoinPKColumn = null;

        List<String> lastTables = new ArrayList<>();

        // 新来的表可能和已有的连接有着多个连接条件 记录上一个表是什么
        for (int i = 0; i < preJoinRelation.get(joinCnt).size(); i++) {
            RecordLog.recordLog(LogLevelConstant.INFO, "新来的表和之前表的第" + (i + 1) + "个连接条件");
            if (preJoinRelation.get(joinCnt).get(i).getKey() instanceof ForeignKey) {
                joinFkColumn = preJoinRelation.get(joinCnt).get(i).getKey();
                joinPkColumn = preJoinRelation.get(joinCnt).get(i).getValue();
            } else {
                joinFkColumn = preJoinRelation.get(joinCnt).get(i).getValue();
                joinPkColumn = preJoinRelation.get(joinCnt).get(i).getKey();
            }
            System.out.println("此次连接条件:" + joinFkColumn.getFullAttrName() + " " + joinPkColumn.getFullAttrName());

            // 判断是在这次join提供主键还是外键
            if (table.getTableName().equals(joinPkColumn.getTableName())) {
                RecordLog.recordLog(LogLevelConstant.INFO, "新连接的表为主键表，由已有连接的结果对应的主键正向推导！");
                ForeignKey joinForeignKey = (ForeignKey) joinFkColumn;

                lastTables.add(joinFkColumn.getTableName());
                RecordLog.recordLog(LogLevelConstant.INFO, "提供给新表连接的旧表外键为：" + joinForeignKey.getFullAttrName());

            } else if (table.getTableName().equals(joinFkColumn.getTableName())) {// 新表提供fk
                RecordLog.recordLog(LogLevelConstant.INFO, "新连接的表为外键表，由当前中间连接结果中与之相连的表的主键集反向推导！");
                lastTables.add(joinPkColumn.getTableName());
            }

        }

        System.out.println("上一个表是：" + lastTables);

        System.out.println("开始计算下一个Filter的理想取值范围，上个中间连接结果的大小为:" + lastResultKeySet.size());

        // shuchu long startParallel = System.currentTimeMillis();

        // 为连接操作后的主键范围
        List<Integer> finalIdealKeys = new ArrayList<>();

        // 根据上一个中间结果，计算此次连接后的finalIdealKeys
        for (List<Integer> lastResultRow : lastResultKeySet) {
            List<Integer> finalIdealKeysEachRow = new ArrayList<>();
            List<List<Integer>> idealKeys = new ArrayList<>();
            // lastResultRow.get(i) 相当于第i个表的某行主键
            for (int i = 0; i < lastResultRow.size(); i++) {
                // 得到第i个表及相应的主键
                String baseTable = lastTables.get(i);
                int baseKey = lastResultRow.get(i);

                JoinCondition joinCondition = this.joinGraph.getEdge(baseTable, table.getTableName());
                if (joinCondition.judge() == JoinMode.PK2FK) {
                    if (joinCondition.getLeftAttribute().getTableName().equals(baseTable)) {
                        ForeignKey foreignKey = (ForeignKey) joinCondition.getLeftAttribute();
                        idealKeys.add(foreignKey.getNextKeyRange(baseKey));// 上一个表为fk表 正向求
                    } else if (joinCondition.getLeftAttribute().getTableName().equals(table.getTableName())) {
                        ForeignKey foreignKey = (ForeignKey) joinCondition.getLeftAttribute();
                        idealKeys.add(foreignKey.getReverseKeyRange(baseKey));// 上一个表为pk表 反向求
                    }
                } else {
                    // TODO FK-FK Join待做
                }
            }
            finalIdealKeysEachRow.addAll(idealKeys.get(0));

            // 求解所有主键的交集得到最终结果
            for (int i = 1; i < lastResultRow.size(); i++) {
                // finalIdealKeysEachRow.retainAll(idealKeys.get(i));//retainAll的时间复杂度高
                finalIdealKeysEachRow = intersectList(finalIdealKeysEachRow, idealKeys.get(i));// 先拿到idealKeys[0],再与所有的idealKeys[]求交集即是每一行的最终idealKey
            }

            finalIdealKeys.addAll(finalIdealKeysEachRow);
        }

        Collections.sort(finalIdealKeys);

        // shuchu
        // System.out.println("Filter理想主键范围计算花费时间"+(System.currentTimeMillis()-startParallel));

        // shuchu RecordLog.recordLog(LogLevelConstant.INFO, "最终确定的理想主键范围为:");

        // System.out.println(finalIdealKeys);
        if (finalIdealKeys.size() == 0) {
            RecordLog.recordLog(LogLevelConstant.INFO, "多个连接条件推出的表的理想主键集交集为空，已经选不到filter了，停止生成filter!");
            return new ArrayList<>();
        }

        // finalIdealKeys 是直接对上一次的执行结果主键范围和此次表进行连接，计算最终主键范围，下面再进行 filter 生成。

        // update by ct: 多个谓词
        // shuchu System.out.println("&主键范围");
        // shuchu System.out.println(finalIdealKeys.get(0) + " " +
        // finalIdealKeys.get(finalIdealKeys.size()-1));
        List<Predicate> predicateList = new ArrayList<>();
        for (int i = 0; i < expressions.size(); i++) {
            if (i > 0) {
                List<Integer> newRange = FilterCardinalitySolver.Compute(filterResults[index - 1], 0);
                // 跟前一张表连接后的主键范围 和 第一个过滤谓词后的主键范围 求交集
                // finalIdealKeys.retainAll(newRange);//时间复杂度太大，pass
                finalIdealKeys = intersectList(finalIdealKeys, newRange);// 优化的求交集算法
                // shuchu System.out.println("关联列");
                // shuchu System.out.println("新主键范围");
                // shuchu System.out.println(finalIdealKeys.get(0) + " " +
                // finalIdealKeys.get(finalIdealKeys.size()-1));
            }
            expression = expressions.get(i);
            // 根据finalIdealKeys生成this.table的过滤谓词参数
            BigDecimal[] maxMinValue = table.calculateMaxMinValue(expression, finalIdealKeys);
            RecordLog.recordLog(LogLevelConstant.DEBUG, "最大值：" + maxMinValue[0].toPlainString());
            RecordLog.recordLog(LogLevelConstant.DEBUG, "最小值：" + maxMinValue[1].toPlainString());

            // 最大值最小值已经求出，开始在这个范围内随机选择filter的参数
            // TODO 这里先只用> >= < <= 后续和之前的模块结合

            // BigDecimal bigDecimalRandom = BigDecimal.valueOf(params[index-1]);

            // // TODO 修改参数生成
            // AttrValue[] parameter = new AttrValue[1];
            // parameter[0] = new AttrValue(DataType.DECIMAL,
            // bigDecimalRandom.multiply(maxMinValue[0].subtract(maxMinValue[1])).add(maxMinValue[1]));
            // // 可能中途会爆int 所以直接用toString
            // RecordLog.recordLog(LogLevelConstant.INFO, "生成的参数为：" +
            // AttrValue.tranString(parameter[0]));

            // boolean beEqualOperator = false;// filter中的运算符只能和=号相关，如>= <= =等
            // // 这里要注意：如果生成的参数刚好等于生成范围的最大值或最小值，filter中的运算符不能是<或者>
            // // TODO 为了将更多的值filter上去，这里可以更改如果生成的参数在范围的左半部，用>,如果在右半部，用小于

            // // 如果生成的参数刚好等于最大值和最小值，要带等号
            // if (((BigDecimal) parameter[0].value).compareTo(maxMinValue[0]) == 0
            // || ((BigDecimal) parameter[0].value).compareTo(maxMinValue[1]) == 0) {
            // beEqualOperator = true;
            // }

            // expression有了，参数有了，运算符有了 ，Filter构建完成
            Predicate predicate = new Predicate(expression, maxMinValue, table, params[index - 1]);
            predicateList.add(predicate);

            if (i == 0) {
                Filter newFilter = new Filter(predicateList.get(0), table);
                filterResults[index - 1] = new FilterResult(newFilter);
            }
        }

        Filter newFilter = null;
        if (expressions.size() == 1) { // 只有一个谓词
            newFilter = new Filter(predicateList.get(0), table);
            filters.add(newFilter);
            filterResults[index - 1] = new FilterResult(newFilter);
        } else { // 有2个谓词
            newFilter = new Filter(predicateList, table);
            filters.add(newFilter);
            filterResults[index - 1] = new FilterResult(newFilter);
        }

        // filter构建完成后，开始利用一个preJoin，计算两个filter join后的结果，为下次的Filter生成提供Join的主键范围

        System.out.println("开始生成JoinResult");
        System.out.println(
                "生成了第" + index + "个Filter，" + newFilter.getTable().getTableName() + "，需要做第" + (index - 1) + "次Join");
        // join的时候 左边就是老的表 右边就是新来的表
        QueryNode preJoin;

        List<Join> joins = new LinkedList<>();
        for (int i = 0; i < preJoinRelation.get(joinCnt).size(); i++) {
            if (preJoinRelation.get(joinCnt).get(i).getKey() instanceof ForeignKey) {
                joinFkColumn = preJoinRelation.get(joinCnt).get(i).getKey();
                joinPkColumn = preJoinRelation.get(joinCnt).get(i).getValue();
            } else {
                joinFkColumn = preJoinRelation.get(joinCnt).get(i).getValue();
                joinPkColumn = preJoinRelation.get(joinCnt).get(i).getKey();
            }
            // 判断是在这次join提供主键还是外键
            if (table.getTableName().equals(joinPkColumn.getTableName())) {
                // 新表都是右表
                rightJoinColumn = joinPkColumn;
                leftJoinColumn = joinFkColumn;
            } else if (table.getTableName().equals(joinFkColumn.getTableName())) {// 新表提供fk
                rightJoinColumn = joinFkColumn;
                leftJoinColumn = joinPkColumn;
            }
            joins.add(new Join(preRoot, filterResults[index - 1].getFilter(), leftJoinColumn, rightJoinColumn));
        }
        // 判断是单个join还是多个join；多个join合成一个complexJoin
        if (joins.size() == 1)
            preJoin = joins.get(0);
        else
            preJoin = new ComplexJoin(joins);

        preRoot = preJoin;
        if (index == 2) { // 两个filter后的表进行连接
            if (table.getTableName().equals(joinPkColumn.getTableName()))
                joinResults[index - 2] = new JoinResult(filterResults[0], filterResults[1], (Join) preJoin);// 求出joinResult[0]

            else
                joinResults[index - 2] = new JoinResult(filterResults[1], filterResults[0], (Join) preJoin);
        } else { // 已经连接的表和经过filter后的新表连接
            if (preJoin instanceof Join)
                joinResults[index - 2] = new JoinResult(joinResults[index - 3], filterResults[index - 1],
                        (Join) preJoin);
            else
                joinResults[index - 2] = new JoinResult(joinResults[index - 3], filterResults[index - 1],
                        (ComplexJoin) preJoin);
        }

        // 生成第二个filter，做第一次join，求joinResult[0]，求joinCardinality[0]
        // 生成第三个Filter，做第二次join，求joinResult[1],求joinCardinality[1]
        // 生成第index个filter，做第index-1次join，求joinResult[index-2]
        System.out.println("此次Join的JoinResult构建完成");

        List<String> provideForNextTableList = new ArrayList<>();
        // 从已连接的结果中，获得与下一张表有连接关系的表，
        List<ResultRow> keySetProvideForNext = null;
        if (nextTable != null) {
            // 下一张表的所有连接关系
            for (int i = 0; i < preJoinRelation.get(joinCnt + 1).size(); i++) {
                Table provideForNextTable = null;
                if (preJoinRelation.get(joinCnt + 1).get(i).getKey() instanceof ForeignKey) {
                    nextJoinFkColumn = preJoinRelation.get(joinCnt + 1).get(i).getKey();
                    nextJoinPKColumn = preJoinRelation.get(joinCnt + 1).get(i).getValue();
                } else {
                    nextJoinFkColumn = preJoinRelation.get(joinCnt + 1).get(i).getValue();
                    nextJoinPKColumn = preJoinRelation.get(joinCnt + 1).get(i).getKey();
                }
                if (nextTable.getTableName().equals(nextJoinFkColumn.getTableName())) {
                    provideForNextTable = DBSchema.getTableThroughName(joinedTableOrder,
                            nextJoinPKColumn.getTableName());
                } else if (nextTable.getTableName().equals(nextJoinPKColumn.getTableName())) {
                    provideForNextTable = DBSchema.getTableThroughName(joinedTableOrder,
                            nextJoinFkColumn.getTableName());
                }
                provideForNextTableList.add(provideForNextTable.getTableName());
            }

            // 注：这里需要再求一遍，原因是前面求基数是在getExecuteTimeOnly关闭的时候才求

            // shuchu long start = System.currentTimeMillis();
            joinResults[index - 2].getCspDefination()
                    .solve(provideForNextTableList.toArray(new String[provideForNextTableList.size()]));
            // shuchu long end = System.currentTimeMillis();
            // shuchu System.out.println("求解时间：" + (end - start));

            // keySetProvideForNext为连接的基数
            keySetProvideForNext = joinResults[index - 2].getCspDefination().getKey4Join();

            RecordLog.recordLog(LogLevelConstant.INFO,
                    "求解（中间）结果大小" + keySetProvideForNext.size());

            joinCardinality.add(joinResults[index - 2].getCspDefination().getCardinality());

            RecordLog.recordLog(LogLevelConstant.INFO,
                    "提供给下一个表连接的表是:" + provideForNextTableList);

            if (keySetProvideForNext.size() == 0) {
                // 理论上经过合理的参数填充，这里不会为0，如果为0，就是实现有问题
                System.out.println("无解");
                System.exit(1);
            } else {
                // joinResults[index -
                // 2].getCspDefination().solve(provideForNextTableList.toArray(new
                // String[provideForNextTableList.size()]));
                System.out.println(
                        keySetProvideForNext.get(0) + " " + keySetProvideForNext.get(keySetProvideForNext.size() - 1));
            }

        } else {
            System.out.println("已经求出了最后一个表的Filter，无须为推导下一个Filter");
        }
        if (keySetProvideForNext == null) {
            return null;
        } else {
            List<List<Integer>> nextAns = new ArrayList<>();
            for (ResultRow resultRow : keySetProvideForNext) {
                nextAns.add(resultRow.getRowData());
            }
            return nextAns;
        }
    }

    public static List<Integer> intersectList(List<Integer> list1, List<Integer> list2) {
        Map<Integer, Integer> tempMap = list2.parallelStream()
                .collect(Collectors.toMap(Function.identity(), Function.identity(), (oldData, newData) -> newData));
        return list1.parallelStream().filter(str -> tempMap.containsKey(str)).collect(Collectors.toList());
    }

    public List<Table> getJoinedTableOrder() {
        return joinedTableOrder;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public List<Integer> getJoinCardinality() {
        return joinCardinality;
    }

    public FilterResult[] getFilterResults() {
        return filterResults;
    }

    public JoinResult[] getJoinResults() {
        return joinResults;
    }

    public List<List<Integer>> getFinalKey4Join() {
        return finalKey4Join;
    }

    public List<Integer> getFilterCardinality() {
        return filterCardinality;
    }
}