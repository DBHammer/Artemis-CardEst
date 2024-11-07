package ecnu.dbhammer.query.gene;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;
import org.jgrapht.graph.Pseudograph;
import org.uma.jmetal.problem.doubleproblem.DoubleProblem;
import org.uma.jmetal.solution.doublesolution.DoubleSolution;
import org.uma.jmetal.solution.doublesolution.impl.DefaultDoubleSolution;
import org.uma.jmetal.util.bounds.Bounds;

import ecnu.dbhammer.query.Predicate;
import ecnu.dbhammer.query.type.JoinMode;
import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.schema.ForeignKey;
import ecnu.dbhammer.schema.Table;
import ecnu.dbhammer.solver.JoinCondition;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Getter
@Setter
public class ParameterInstantiationProblem implements DoubleProblem {
    private int numberOfVariables; // 参数个数
    private final int numberOfObjectives = 2; // 目标函数个数
    private final int numberOfConstraints = 0; // 约束条件个数
    private List<Bounds<Double>> variableBounds = new ArrayList<>(); // 变量的取值范围
    private Pair<Integer, Integer> targetCard; // 目标输出的基数范围
    private List<DoubleSolution> historicalSolutions = new ArrayList<>(); // 历史有效解集
    private List<HotRegion> hotRegions; // 热区列表，基于历史有效解集
    private static final int PARALLEL_THRESHOLD = 100; // 并行计算的阈值
    private List<NavigableMap<Double, Integer>> eventPointsPerVariable; // 每个变量的事件点映射

    private List<Table> joinedTableOrder; // 已连接的表顺序
    private List<List<Pair<Attribute, Attribute>>> preJoinRelation; // 预连接关系
    private Pseudograph<String, JoinCondition> joinGraph; // 连接图
    private List<List<Predicate>> predicateLists; // 谓词列表

    /**
     * 构造函数，初始化问题的参数和变量范围。
     *
     * @param numberOfVariables 参数个数
     * @param bounds            变量的取值范围
     * @param targetCard        目标输出的基数范围
     * @param joinedTableOrder  已连接的表顺序
     * @param preJoinRelation   预连接关系
     * @param joinGraph         连接图
     * @param predicateLists    谓词列表
     */
    public ParameterInstantiationProblem(int numberOfVariables, ArrayList<Pair<Double, Double>> bounds,
            Pair<Integer, Integer> targetCard, List<Table> joinedTableOrder,
            List<List<Pair<Attribute, Attribute>>> preJoinRelation,
            Pseudograph<String, JoinCondition> joinGraph,
            List<List<Predicate>> predicateLists) {
        this.numberOfVariables = numberOfVariables;
        this.joinedTableOrder = joinedTableOrder;
        this.preJoinRelation = preJoinRelation;
        this.joinGraph = joinGraph;
        this.targetCard = targetCard;
        this.predicateLists = predicateLists;
        this.hotRegions = new ArrayList<>(Collections.nCopies(numberOfVariables, null));
        this.eventPointsPerVariable = new ArrayList<>();
        for (int i = 0; i < numberOfVariables; i++) {
            eventPointsPerVariable.add(new TreeMap<>());
        }
        // 添加到 variableBounds 的逻辑
        for (Pair<Double, Double> bound : bounds) {
            variableBounds.add(Bounds.create(bound.getLeft(), bound.getRight()));
        }

        // 输出 variableBounds 中的每一项
        System.out.println("Variable Bounds:");
        for (Bounds bound : variableBounds) {
            System.out.println(bound.getLowerBound() + "," + bound.getUpperBound());
        }
    }

    /**
     * 更新主键范围，根据当前变量值更新变量的取值范围。
     *
     * @param variables 当前解的变量值列表
     */
    /**
     * 更新主键范围，根据当前变量值更新变量的取值范围。
     *
     * @param variables 当前解的变量值列表
     */
    public void updatePKRange(List<Double> variables) {
        this.variableBounds.clear();
        System.out.println(variables.toString());
        for (int i = 0; i < variables.size(); i++) {
            variableBounds.add(predicateLists.get(i).get(0).getPKRBounds(variables.get(i)));
        }
    }

    /**
     * 获取指定变量的范围，根据变量值和索引。
     *
     * @param variableValue 变量值
     * @param variableIndex 变量索引
     * @return 变量的范围
     */
    private Bounds<Double> getBoundsForVariable(double variableValue, int variableIndex) {
        return predicateLists.get(variableIndex).get(0).getPKRBounds(variableValue);
    }

    @Override
    public DoubleSolution evaluate(DoubleSolution solution) {
        int[] objectives;
        List<Double> variables = solution.variables();
        for (int i = 0; i < variables.size(); i++) {
            Double currentVariable = variables.get(i);
            if (currentVariable.isNaN()) {
                variables.set(i, (variableBounds.get(i).getLowerBound() + variableBounds.get(i).getUpperBound()) / 2);
            }
        }
        // updatePKRange(variables);
        if (historicalSolutions.isEmpty()) {
            objectives = calculateObjectives(solution);
            solution.objectives()[0] = objectives[0];
        } else {
            objectives = calculateDualObjectives(solution);
            solution.objectives()[0] = objectives[0];
            solution.objectives()[1] = objectives[1];
        }
        return solution;
    }

    @Override
    public DoubleSolution createSolution() {
        return new DefaultDoubleSolution(variableBounds, numberOfObjectives, numberOfConstraints);
    }

    @Override
    public List<Bounds<Double>> variableBounds() {
        return variableBounds;
    }

    /**
     * 计算目标函数 F1，表示基数误差。
     *
     * @param cur 当前计算的基数
     * @return 基数误差
     */
    private int F1(Integer cur) {
        return Math.max(0, targetCard.getLeft() - cur) + Math.max(0, cur - targetCard.getRight());
    }

    /**
     * 计算目标函数 F2，表示与热区的重叠程度。
     *
     * @param currentSolution 当前解
     * @return 重叠程度的百分比
     */
    private int F2(DoubleSolution currentSolution) {
        List<HotRegion> hotRegions = getHotRegionsPerTable();
        double totalOverlap;
        if (numberOfVariables >= PARALLEL_THRESHOLD) {
            totalOverlap = IntStream.range(0, numberOfVariables).parallel()
                    .mapToDouble(i -> {
                        Bounds<Double> currentBounds = getBoundsForVariable(currentSolution.variables().get(i), i);
                        HotRegion hotRegion = hotRegions.get(i);
                        if (hotRegion != null) {
                            double overlapLength = Math.max(0,
                                    Math.min(currentBounds.getUpperBound(), hotRegion.getUpperBound())
                                            - Math.max(currentBounds.getLowerBound(), hotRegion.getLowerBound()));
                            double currentRangeLength = currentBounds.getUpperBound() - currentBounds.getLowerBound();
                            if (currentRangeLength > 0) {
                                return overlapLength / currentRangeLength;
                            }
                        }
                        return 0.0;
                    })
                    .sum();
        } else {
            totalOverlap = 0;
            for (int i = 0; i < numberOfVariables; i++) {
                Bounds<Double> currentBounds = getBoundsForVariable(currentSolution.variables().get(i), i);
                HotRegion hotRegion = hotRegions.get(i);
                if (hotRegion != null) {
                    double overlapLength = Math.max(0,
                            Math.min(currentBounds.getUpperBound(), hotRegion.getUpperBound())
                                    - Math.max(currentBounds.getLowerBound(), hotRegion.getLowerBound()));
                    double currentRangeLength = currentBounds.getUpperBound() - currentBounds.getLowerBound();
                    if (currentRangeLength > 0) {
                        double overlapRatio = overlapLength / currentRangeLength;
                        totalOverlap += overlapRatio;
                    }
                }
            }
        }
        return (int) (totalOverlap / numberOfVariables * 100);
    }

    /**
     * 添加新的解到历史解集中，并增量更新热区。
     *
     * @param newSolution 新的解
     */
    public void addSolution(DoubleSolution newSolution) {
        historicalSolutions.add(newSolution);
        addSolutionToEventPoints(newSolution);
        for (int i = 0; i < numberOfVariables; i++) {
            HotRegion hotRegion = calculateHotRegionIncrementally(i);
            hotRegions.set(i, hotRegion);
        }
    }

    /**
     * 将新的解的范围添加到事件点映射中，增量更新事件点。
     *
     * @param solution 新的解
     */
    private void addSolutionToEventPoints(DoubleSolution solution) {
        for (int i = 0; i < numberOfVariables; i++) {
            Bounds<Double> bounds = getBoundsForVariable(solution.variables().get(i), i);
            NavigableMap<Double, Integer> eventPoints = eventPointsPerVariable.get(i);
            eventPoints.put(bounds.getLowerBound(), eventPoints.getOrDefault(bounds.getLowerBound(), 0) + 1);
            eventPoints.put(bounds.getUpperBound(), eventPoints.getOrDefault(bounds.getUpperBound(), 0) - 1);
        }
    }

    /**
     * 增量计算指定变量的热区。
     *
     * @param variableIndex 变量索引
     * @return 热区对象
     */
    private HotRegion calculateHotRegionIncrementally(int variableIndex) {
        NavigableMap<Double, Integer> eventPoints = eventPointsPerVariable.get(variableIndex);
        int currentFrequency = 0; // 当前频率
        int maxFrequency = 0; // 最大频率
        double maxStart = 0; // 热区起始位置
        double maxEnd = 0; // 热区结束位置
        double maxLength = 0; // 最大热区长度
        Double prevPosition = null; // 前一个事件点的位置
        for (Map.Entry<Double, Integer> entry : eventPoints.entrySet()) {
            double position = entry.getKey();
            int delta = entry.getValue();
            if (prevPosition != null) {
                if (currentFrequency == maxFrequency) {
                    double currentLength = position - prevPosition;
                    if (currentLength > maxLength) {
                        maxStart = prevPosition;
                        maxEnd = position;
                        maxLength = currentLength;
                    }
                }
            }
            currentFrequency += delta;
            if (currentFrequency > maxFrequency) {
                maxFrequency = currentFrequency;
                maxStart = position;
                maxEnd = position;
                maxLength = 0;
            }
            prevPosition = position;
        }

        return new HotRegion(variableIndex, maxStart, maxEnd, maxFrequency);
    }

    /**
     * 获取每个变量的热区列表。
     *
     * @return 热区列表
     */
    public List<HotRegion> getHotRegionsPerTable() {
        if (hotRegions != null && !hotRegions.contains(null)) {
            return hotRegions;
        }
        hotRegions = new ArrayList<>();
        for (int i = 0; i < numberOfVariables; i++) {
            List<Bounds<Double>> historicalBounds = new ArrayList<>();
            for (DoubleSolution solution : historicalSolutions) {
                historicalBounds.add(getBoundsForVariable(solution.variables().get(i), i));
            }
            HotRegion hotRegion = calculateHotRegionForTable(i, historicalBounds);
            hotRegions.add(hotRegion);
        }
        return hotRegions;
    }

    /**
     * 计算指定变量的热区，基于所有历史范围。
     *
     * @param tableIndex 变量索引
     * @param ranges     历史范围列表
     * @return 热区对象
     */
    private HotRegion calculateHotRegionForTable(int tableIndex, List<Bounds<Double>> ranges) {
        if (ranges.isEmpty()) {
            return null;
        }
        List<EventPoint> eventPoints = new ArrayList<>();
        for (Bounds<Double> range : ranges) {
            eventPoints.add(new EventPoint(range.getLowerBound(), 1)); // 区间开始，频率加1
            eventPoints.add(new EventPoint(range.getUpperBound(), -1)); // 区间结束，频率减1
        }
        eventPoints.sort(Comparator.comparingDouble(ep -> ep.position));
        int currentFrequency = 0;
        int maxFrequency = 0;
        double maxStart = 0;
        double maxEnd = 0;
        double intervalStart = 0;
        double intervalEnd;
        double maxLength = 0;
        for (EventPoint ep : eventPoints) {
            currentFrequency += ep.delta;
            if (ep.delta > 0 && currentFrequency >= maxFrequency) {
                intervalStart = ep.position;
            }
            if (currentFrequency > maxFrequency) {
                maxFrequency = currentFrequency;
                maxStart = intervalStart;
                maxEnd = ep.position;
                maxLength = maxEnd - maxStart;
            } else if (currentFrequency == maxFrequency && ep.delta < 0) {
                intervalEnd = ep.position;
                double currentLength = intervalEnd - intervalStart;
                if (currentLength > maxLength) {
                    maxStart = intervalStart;
                    maxEnd = intervalEnd;
                    maxLength = currentLength;
                }
            }
        }
        return new HotRegion(tableIndex, maxStart, maxEnd, maxFrequency);
    }

    /**
     * 计算单目标函数的值。
     *
     * @param solution 当前解
     * @return 目标函数值数组
     */
    private int[] calculateObjectives(DoubleSolution solution) {
        int curCalculatedCard = calculateByCPSolver(solution);
        // System.out.println("value is:" + curCalculatedCard);
        // System.out.println("Solution Variables:");
        // for (int i = 0; i < solution.variables().size(); i++) {
        // System.out.println("Variable " + i + ": " + solution.variables().get(i));
        // }
        int cardinalityError = F1(curCalculatedCard);
        solution.attributes().put("currentCardinality", curCalculatedCard);
        System.out.println("currentCardinality: "+ curCalculatedCard);
        return new int[] { cardinalityError };
    }

    /**
     * 计算双目标函数的值。
     *
     * @param solution 当前解
     * @return 目标函数值数组
     */
    private int[] calculateDualObjectives(DoubleSolution solution) {
        int curCalculatedCard = calculateByCPSolver(solution);
        int cardinalityError = F1(curCalculatedCard);
        int overlapping = F2(solution);
        solution.attributes().put("currentCardinality", curCalculatedCard);
        solution.attributes().put("overlapping degree", overlapping);
        System.out.println("currentCardinality: "+ curCalculatedCard + " , overlapping degree: " + overlapping);
        return new int[] { cardinalityError, overlapping };
    }

    /**
     * 使用 CP 求解器计算当前解的基数。
     *
     * @param solution 当前解
     * @return 计算得到的基数
     */
    private int calculateByCPSolver(DoubleSolution solution) {
        List<List<Integer>> keyRanges = new LinkedList<>();
        // 对每个表进行处理
        for (int i = 0; i < numberOfVariables; i++) {
            Bounds<Double> bounds = getBoundsForVariable(solution.variables().get(i), i);
            Table table = joinedTableOrder.get(i);
            if (i == 0) {
                // 第一个表，初始化键范围
                for (int key = bounds.getLowerBound().intValue(); key < bounds.getUpperBound().intValue(); key++) {
                    List<Integer> row = new ArrayList<>();
                    row.add(key);
                    keyRanges.add(row);
                }
            } else {
                // 处理连接条件
                int joinCnt = i - 1;
                Attribute joinFkColumn = null;
                Attribute joinPkColumn = null;
                List<String> lastTables = new ArrayList<>();
                List<Integer> lastTablesIndex = new ArrayList<>();
                // 可能存在多个连接条件
                for (Pair<Attribute, Attribute> pair : preJoinRelation.get(joinCnt)) {
                    if (pair.getKey() instanceof ForeignKey) {
                        joinFkColumn = pair.getKey();
                        joinPkColumn = pair.getValue();
                    } else {
                        joinFkColumn = pair.getValue();
                        joinPkColumn = pair.getKey();
                    }
                    // 判断新表提供的是主键还是外键
                    if (table.getTableName().equals(joinPkColumn.getTableName())) {
                        lastTables.add(joinFkColumn.getTableName());
                    } else if (table.getTableName().equals(joinFkColumn.getTableName())) {
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
                    // 当前表的主键范围
                    List<Integer> finalIdealKeysEachRow = new ArrayList<>();
                    List<List<Integer>> idealKeys = new ArrayList<>();
                    for (int j = 0; j < lastTables.size(); j++) {
                        String baseTable = lastTables.get(j);
                        int baseKey = lastResultRow.get(lastTablesIndex.get(j));
                        JoinCondition joinCondition = this.joinGraph.getEdge(baseTable, table.getTableName());
                        if (joinCondition.judge() == JoinMode.PK2FK) {
                            if (joinCondition.getLeftAttribute().getTableName().equals(baseTable)) {
                                ForeignKey foreignKey = (ForeignKey) joinCondition.getLeftAttribute();
                                idealKeys.add(foreignKey.getNextKeyRange(baseKey));
                            } else if (joinCondition.getLeftAttribute().getTableName().equals(table.getTableName())) {
                                ForeignKey foreignKey = (ForeignKey) joinCondition.getLeftAttribute();
                                idealKeys.add(foreignKey.getReverseKeyRange(baseKey));
                            }
                        }
                    }
                    finalIdealKeysEachRow.addAll(idealKeys.get(0));
                    // 求交集得到最终主键范围
                    for (int j = 1; j < lastTables.size(); j++) {
                        finalIdealKeysEachRow = intersectList(finalIdealKeysEachRow, idealKeys.get(j));
                    }
                    for (Integer row : finalIdealKeysEachRow) {
                        List<Integer> eachRowResult = new ArrayList<>(lastResultRow);
                        // 判断主键是否在当前范围内
                        if (row.compareTo(bounds.getLowerBound().intValue()) > 0
                                && row.compareTo(bounds.getUpperBound().intValue()) < 0) {
                            eachRowResult.add(row);
                            newKeyRanges.add(eachRowResult);
                        }
                    }
                }
                if (newKeyRanges.isEmpty()) {
                    return Integer.MAX_VALUE;
                }
                keyRanges = newKeyRanges;
            }
        }
        int finalCard = keyRanges.size();
        return finalCard;
    }

    @Override
    public int numberOfVariables() {
        return this.numberOfVariables;
    }

    @Override
    public int numberOfObjectives() {
        return this.numberOfObjectives;
    }

    @Override
    public int numberOfConstraints() {
        return this.numberOfConstraints;
    }

    @Override
    public String name() {
        return "ParameterInstantiationProblem";
    }

    /**
     * 计算两个整数列表的交集。
     *
     * @param list1 列表1
     * @param list2 列表2
     * @return 交集列表
     */
    public static List<Integer> intersectList(List<Integer> list1, List<Integer> list2) {
        Map<Integer, Integer> tempMap = list2.parallelStream()
                .collect(Collectors.toMap(Function.identity(), Function.identity(), (oldData, newData) -> newData));
        return list1.parallelStream().filter(tempMap::containsKey).collect(Collectors.toList());
    }

    /**
     * 事件点类，表示区间的起点或终点。
     */
    public static class EventPoint {
        double position; // 事件点的位置
        int delta; // 频率增量，+1 表示区间开始，-1 表示区间结束

        public EventPoint(double position, int delta) {
            this.position = position;
            this.delta = delta;
        }
    }

    /**
     * 热区类，表示访问频率最高的区间。
     */
    public static class HotRegion {
        private int tableIndex; // 表的索引
        private double lowerBound; // 热区下界
        private double upperBound; // 热区上界
        private int frequency; // 热区的访问频率

        public HotRegion(int tableIndex, double lowerBound, double upperBound, int frequency) {
            this.tableIndex = tableIndex;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.frequency = frequency;
        }

        // Getter 方法
        public int getTableIndex() {
            return tableIndex;
        }

        public double getLowerBound() {
            return lowerBound;
        }

        public double getUpperBound() {
            return upperBound;
        }

        public int getFrequency() {
            return frequency;
        }
    }
}