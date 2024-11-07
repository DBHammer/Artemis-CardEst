package ecnu.dbhammer.joinorder;

import ecnu.dbhammer.query.QueryTree;
import ecnu.dbhammer.schema.Table;
import ecnu.dbhammer.test.RawNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xiangzhaokun + tingc
 * @ClassName JoinOrderEvaluation.java
 * @Description 给生成的Query指定Join Order全部执行一遍，比较性能
 * @createTime 2021年10月30日 21:31:00
 */
public class JoinOrderEvaluation {
    private QueryTree queryTree;
    private String originSQL;
    private List<String> QueryHintList;

    public JoinOrderEvaluation(QueryTree queryTree) {
        this.queryTree = queryTree;
        this.originSQL = queryTree.getSqlText();
        this.QueryHintList = new ArrayList<>();
    }

    public List<String> QueryHintEnum(String dbMSBrand, String originalJoinOrder, long originalTime) {
        List<String> allJoinOrderSQL = new ArrayList<>();
        JoinOrderEnum joinOrderEnum = new JoinOrderEnum(queryTree.getTables());
        List<String> allJoinOrders;
        //只有左深树的
        if(dbMSBrand.equalsIgnoreCase("tidb") || dbMSBrand.equalsIgnoreCase("mysql")){
            allJoinOrders = joinOrderEnum.enumerateJoinOrder_LeftDeep();
        }else {//包含bushy tree的
            allJoinOrders = joinOrderEnum.enumerateJoinOrder_BushyTree();
        }

        //from  ...  where中间的表的顺序
        String[] splitedSQLSelect = originSQL.split("select");
        String[] splitedSQLFrom = splitedSQLSelect[1].split("from");
        String[] splitedSQLWhere = splitedSQLFrom[1].split("where");

        for(String joinOrder : allJoinOrders){
            String fullSQL = "";
            //排除
            if(originalJoinOrder.equalsIgnoreCase(joinOrder))
                continue;

            // System.out.println(joinOrder);
            if (dbMSBrand.equalsIgnoreCase("tidb")) {
                long upperBound = originalTime + 1000;
                fullSQL = "select /*+ MAX_EXECUTION_TIME("+upperBound +") ignore_plan_cache() */ STRAIGHT_JOIN " + splitedSQLFrom[0] + "from " + joinOrder + " where " + splitedSQLWhere[1];
            }else if (dbMSBrand.equalsIgnoreCase("oceanbase")) {
                long upperBound = originalTime * 1000 + 1000000;
                if(upperBound > 40000000) upperBound = 40000000;//避免查询执行太久
                fullSQL = "select /*+query_timeout("+upperBound+") USE_PLAN_CACHE(NONE) LEADING" + joinOrder + "*/ " + splitedSQLFrom[0] + "from " + splitedSQLFrom[1];
            }else if(dbMSBrand.equalsIgnoreCase("postgresql")){
                joinOrder = joinOrder.replaceAll(","," cross join ");
                fullSQL = "select " + splitedSQLFrom[0] + "from " + joinOrder + " where " + splitedSQLWhere[1];
            }else if(dbMSBrand.equalsIgnoreCase("mysql")){
                //long upperBound = originalTime + 1000;
                fullSQL = "select /*! STRAIGHT_JOIN */ " + splitedSQLFrom[0] + "from " + joinOrder + " where " + splitedSQLWhere[1];
            }else if(dbMSBrand.equalsIgnoreCase("gaussdb")) {
                fullSQL = "select /*+ leading" + joinOrder + "*/ " + splitedSQLFrom[0] + "from " + splitedSQLFrom[1];
            }

            allJoinOrderSQL.add(fullSQL);
        }

        this.QueryHintList = allJoinOrderSQL;
        return allJoinOrderSQL;
    }

    //计算MRR值：MRR(Mean Reciprocal Rank)：是把标准答案在被评价系统给出结果中的排序取倒数作为它的准确度，再对所有的问题取平均
    public static double calculateMRR(List<Integer> rankList) {
        double result = 0;
        double sum = 0;
        for(Integer rank : rankList) {
            sum += 1.0 / rank;
        }
        result = sum / rankList.size();

        return result;
    }

    public void JoinOrderTest(){
        for(int i=0;i<QueryHintList.size();i++){
            System.out.println(QueryHintList.get(i));
        }
    }
    public Map<String,Integer> JoinOrderBenchmarking(){
        Map<String,Integer> trueOrder = new HashMap<>();
        for(int i=0;i<QueryHintList.size();i++){

        }
        return null;
    }


}
