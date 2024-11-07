package ecnu.dbhammer.query.filter;

import ecnu.dbhammer.query.type.JoinMode;
import ecnu.dbhammer.schema.ForeignKey;
import ecnu.dbhammer.schema.Table;
import ecnu.dbhammer.solver.JoinCondition;
import org.jgrapht.graph.Pseudograph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author xiangzhaokun
 * @ClassName FilterValueCalculate.java
 * @Description TODO
 * @createTime 2022年04月28日 16:31:00
 */
public class FilterValueCalculateTask implements Callable<List<Integer>> {
    private Table table;//要生成Filter的表

    private List<String> lastTables;

    private Pseudograph<String, JoinCondition> joinGraph;

    private List<List<Integer>> lastResultKeySet;

    private int left;

    private int right;

    public FilterValueCalculateTask(Table table, List<String> lastTables, List<List<Integer>> lastResultKeySet, int left, int right, Pseudograph<String, JoinCondition> joinGraph) {
        this.table = table;
        this.lastTables = lastTables;
        this.lastResultKeySet = lastResultKeySet;
        this.left = left;
        this.right = right;
        this.joinGraph = joinGraph;
    }

    @Override
    public List<Integer> call() throws Exception {

        List<Integer> finalIdealKeys = new ArrayList<>();
        //对于每一行的主键对
        System.out.println("线程内开始计算");


        List<Integer> lastResultRow = null;
        long start = System.currentTimeMillis();
        for (int q = left; q <= right; q++) {
            System.out.println(q);


            lastResultRow = lastResultKeySet.get(q);
            List<Integer> finalIdealKeysEachRow = new ArrayList<>();
            List<List<Integer>> idealKeys = new ArrayList<>();
            System.out.println("lastResultRow大小"+lastResultRow.size());

            for (int i = 0; i < lastResultRow.size(); i++) {
                String baseTable = lastTables.get(i);

                int baseKey = lastResultRow.get(i);

                JoinCondition joinCondition = this.joinGraph.getEdge(baseTable, table.getTableName());
                if (joinCondition.judge() == JoinMode.PK2FK) {
                    if (joinCondition.getLeftAttribute().getTableName().equals(baseTable)) {
                        ForeignKey foreignKey = (ForeignKey) joinCondition.getLeftAttribute();
                        idealKeys.add(foreignKey.getNextKeyRange(baseKey));
                    } else if (joinCondition.getLeftAttribute().getTableName().equals(table.getTableName())) {
                        ForeignKey foreignKey = (ForeignKey) joinCondition.getLeftAttribute();
                        idealKeys.add(foreignKey.getReverseKeyRange(baseKey));
                    }
                } else {
                    //TODO FK-FK Join待做
                }
            }
            finalIdealKeysEachRow.addAll(idealKeys.get(0));


            for (int i = 1; i < lastResultRow.size(); i++) {
                //finalIdealKeysEachRow.retainAll(idealKeys.get(i));
                finalIdealKeysEachRow = intersectList(finalIdealKeysEachRow, idealKeys.get(i));
            }


            finalIdealKeys.addAll(finalIdealKeysEachRow);
        }
        //System.out.println(System.currentTimeMillis() - start);
        Collections.sort(finalIdealKeys);
        return finalIdealKeys;
    }

    public static List<Integer> intersectList(List<Integer> list1, List<Integer> list2){
        Map<Integer, Integer> tempMap = list2.parallelStream().collect(Collectors.toMap(Function.identity(), Function.identity(), (oldData, newData) -> newData));
        return list1.parallelStream().filter(str->{
            return tempMap.containsKey(str);
        }).collect(Collectors.toList());
    }
}
