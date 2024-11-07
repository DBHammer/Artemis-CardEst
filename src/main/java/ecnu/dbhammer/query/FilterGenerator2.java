package ecnu.dbhammer.query;

import ecnu.dbhammer.data.AttrValue;
import ecnu.dbhammer.data.DataType;
import ecnu.dbhammer.query.gene.NSGAIIQueryOptimizer;
import ecnu.dbhammer.query.gene.ParameterInstantiationProblem;
import ecnu.dbhammer.result.FilterResult;
import ecnu.dbhammer.result.JoinResult;
import ecnu.dbhammer.schema.*;
import ecnu.dbhammer.solver.JoinCondition;
import lombok.Getter;
import lombok.Setter;

import org.apache.commons.lang3.tuple.Pair;
import org.jgrapht.graph.Pseudograph;
import org.uma.jmetal.util.bounds.Bounds;

import java.math.BigDecimal;
import java.util.*;

@Getter
@Setter
public class FilterGenerator2 {

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

    private Pair<Integer, Integer> targetCard;
    private List<List<Predicate>> predicateLists;
    // population[i]：表i， population[i][i]：表i上的第i个谓词，
    // population[i][i][i]：表i上的第i个谓词的第i组参数
    private List<List<List<AttrValue[]>>> population;
    private List<List<List<int[]>>> populationPKRanges;

    // NSGAIIQueryOptimizer
    private ArrayList<Pair<Double, Double>> bounds;
    private List<Bounds<Double>> variableBounds;
    private int numbOfEachTemplate;

    public FilterGenerator2(List<Table> joinedTableOrder, List<List<Pair<Attribute, Attribute>>> preJoinRelation,
            Pseudograph<String, JoinCondition> joinGraph, double[] params, Pair<Integer, Integer> targetCard,
            int numbOfEachTemplate) {
        this.joinedTableOrder = joinedTableOrder;
        this.preJoinRelation = preJoinRelation;
        this.joinGraph = joinGraph;
        this.params = params;
        this.targetCard = targetCard;
        this.numbOfEachTemplate = numbOfEachTemplate;
        this.filterResults = new FilterResult[joinedTableOrder.size()];
        this.joinResults = new JoinResult[joinedTableOrder.size() - 1];
        this.filters = new ArrayList<>();
        this.filterCardinality = new ArrayList<>();
        this.joinCardinality = new ArrayList<>();
        this.finalKey4Join = new ArrayList<>();
        this.table2PredicateCol = new LinkedHashMap<>();
        this.predicateLists = new ArrayList<>();
        this.population = new ArrayList<>();
        this.populationPKRanges = new ArrayList<>();
        this.bounds = new ArrayList<>();
    }

    public void GeneticAlgorithmQueryParams() {
        allFilters = new ArrayList<>();
        GeneratePredicateExpressionsAndInitPopulation();
        int totalCount = predicateLists.stream().mapToInt(List::size).sum();
        ParameterInstantiationProblem problem = new ParameterInstantiationProblem(totalCount, bounds, targetCard,
                joinedTableOrder, preJoinRelation, joinGraph, predicateLists);
        NSGAIIQueryOptimizer optimizer = new NSGAIIQueryOptimizer(problem);
        variableBounds = optimizer.getVariableBounds();
        for (int k = 0; k < numbOfEachTemplate; k++) {
            List<Double> params = optimizer.runOptimizer();
            filters.clear();
            // each table
            for (int i = 0; i < predicateLists.size(); i++) {
                Table table = joinedTableOrder.get(i);
                // each table's predicates
                for (int j = 0; j < predicateLists.get(i).size(); j++) {
                    AttrValue[] attrValues = new AttrValue[1];
                    attrValues[0] = new AttrValue(DataType.DECIMAL, BigDecimal.valueOf(params.get(i)));
                    Predicate predicate = predicateLists.get(i).get(j);
                    predicate.setParameters(attrValues);
                    predicate.constructText();
                }
                Filter filter = new Filter(predicateLists.get(i), table);
                filters.add(filter);
            }
            outputFilterAndPKRange();
            System.out.println("finish");
        }
    }

    public void outputFilterAndPKRange() {
        for (int i = 0; i < filters.size(); ++i) {
            // 目前每张表只加了一个filter，后面可以增加多个
            System.out.println(filters.get(i).getPredicateList().get(0).getText() + "->["
                    + variableBounds.get(i).getLowerBound() + "," + variableBounds.get(i).getUpperBound() + "]");
        }
    }

    // generateFilters and init param
    public void GeneratePredicateExpressionsAndInitPopulation() {
        predicateLists.clear();
        population.clear();
        populationPKRanges.clear();
        for (int index = 0; index < joinedTableOrder.size(); index++) {
            Table table = joinedTableOrder.get(index);
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
            BigDecimal a = predicate.getExpression().computePredicateValue(0);
            BigDecimal b = predicate.getExpression().computePredicateValue(table.getTableSize() - 1);
            BigDecimal lowBound = a.min(b);
            BigDecimal upperBound = a.max(b);
            bounds.add(Pair.of(lowBound.doubleValue(), upperBound.doubleValue()));
        }
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