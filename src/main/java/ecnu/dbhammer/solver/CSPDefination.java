package ecnu.dbhammer.solver;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.query.Join;
import ecnu.dbhammer.result.ResultRow;
import ecnu.dbhammer.schema.Table;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.*;

/**
 * @author xiangzhaokun
 * @ClassName CSPDefination.java
 * @Description 约束满足问题的定义，只求join
 * @createTime 2022年04月08日 21:06:00
 */
public class CSPDefination {

    private Table biggestTable;//经过过滤后最大的表

    private List<Table> variableList;//涉及的变量

    private List<Pair<Integer, Integer>> domainList;//涉及变量经过过滤后的值域

    private List<JoinCondition> joinConditionList;//连接条件

    private int cardinality;//该连接算子的基数

    private List<ResultRow> key4Join;//该连接算子的主键对，只有是最后一个连接算子才求，否则只需求基数即可

    public CSPDefination(Table biggestTable, List<Table> variableList, List<Pair<Integer, Integer>> domainList, List<JoinCondition> joinConditionList) {
        this.biggestTable = biggestTable;
        this.variableList = variableList;
        this.domainList = domainList;
        this.joinConditionList = joinConditionList;
        this.key4Join = new LinkedList<>();
    }

    public void solve(String[] providedTable) throws InterruptedException, ExecutionException {
        RecordLog.recordLog(LogLevelConstant.INFO, "求解整体Join结果");
        System.out.println("涉及的变量：");
        int index = 0;
        for (int i=0;i<variableList.size();i++) {
            if(variableList.get(i).getTableName().equals(this.biggestTable.getTableName())){
                index = i;
            }
            System.out.print(variableList.get(i).getTableName() + " ");
        }
        System.out.println();
        System.out.println("涉及的连接条件");

        for(JoinCondition joinCondition : joinConditionList){
            System.out.println(joinCondition.toString());
        }
        System.out.println("涉及的取值范围：");
        for (Pair<Integer, Integer> pair : domainList) {
            System.out.println(pair.getLeft() + " " + pair.getRight());
        }

        System.out.println("取值范围最大的范围为:"+this.domainList.get(index).getLeft()+" "+this
            .domainList.get(index).getRight());


        int amountData = this.domainList.get(index).getRight()-this.domainList.get(index).getLeft()+1;
        int nThread = (int) Math.ceil(amountData*1.0/1000);

        if(nThread > Configurations.getSolverThread()){
            nThread = Configurations.getSolverThread();
        }
        ExecutorService service = ThreadPools.getService();

        SolveTask[] computingTasks = new SolveTask[nThread];
        int step = (int) Math.ceil(amountData * 1.0 / nThread);
        System.out.println("数据间隔："+step);
        List<Pair<Integer,Integer>> pairList = new ArrayList<>();
        for (int i = this.domainList.get(index).getLeft(); i <= this.domainList.get(index).getRight(); i += step) {
            int left = i;
            int right = Math.min((i + step - 1), this.domainList.get(index).getRight());
            pairList.add(Pair.of(left,right));
        }

        System.out.println("数据分段数："+pairList.size());
        for(int i=0;i< pairList.size();i++) {
            //TODO 这里是验证分段划分是否正确的
            //System.out.println(pairList.get(i).getLeft() +" "+pairList.get(i).getRight());
        }
        System.out.println("线程数："+nThread);

        if(pairList.size() > nThread){

            System.exit(1);
        }
        for(int i=0;i<pairList.size();i++){
            Pair<Integer,Integer> pair = pairList.get(i);
            computingTasks[i] = new SolveTask(this.biggestTable,this.getVariableList(),pair,this.domainList,this.joinConditionList,providedTable);
            //System.out.println("here 3 " + i);
        }

        Future<List<ResultRow>>[] F = new Future[pairList.size()];
        for (int i = 0; i < pairList.size(); i++) {
            F[i] = service.submit(computingTasks[i]);
            //System.out.println("here 4 " + i);
        }

        for (int i = 0; i < pairList.size(); i++) {
            this.key4Join.addAll(F[i].get());
        }
        //service.shutdown();
        //Thread.sleep(5000);
    }


    // 在加入新的连接时将原有CSP转换为新的CSP
    public CSPDefination addConstraints(Table newTable, Pair<Integer, Integer> newDomain, List<JoinCondition> newJoinConditions) {
        List<Table> newTableList = new ArrayList<>(this.variableList);
        newTableList.add(newTable);
        List<Pair<Integer, Integer>> newDomainList = new ArrayList<>(this.domainList);
        newDomainList.add(newDomain);
        List<JoinCondition> newJoinConditionList = new ArrayList<>(newJoinConditions);

        int index = 0;
        for(int i=0;i<this.variableList.size();i++){
            if(this.variableList.get(i).getTableName().equals(this.biggestTable.getTableName())){
                index = i;
            }
        }
        int nowBiggestSize = this.domainList.get(index).getRight()-this.domainList.get(index).getLeft()+1;
        if (newDomain.getRight()-newDomain.getLeft()+1 > nowBiggestSize) {
            biggestTable = newTable;
        }
        return new CSPDefination(biggestTable, newTableList, newDomainList, newJoinConditionList);
    }


    public int getCardinality() {
        this.cardinality = this.key4Join.size();
        return cardinality;
    }

    public List<ResultRow> getKey4Join() {
        return key4Join;
    }

    public List<Table> getVariableList() {
        return variableList;
    }

    public List<Pair<Integer, Integer>> getDomainList() {
        return domainList;
    }

    public List<JoinCondition> getJoinConditionList() {
        return joinConditionList;
    }

    public Table getBiggestTable() {
        return biggestTable;
    }
}
