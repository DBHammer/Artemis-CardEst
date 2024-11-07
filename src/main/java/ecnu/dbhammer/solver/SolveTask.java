package ecnu.dbhammer.solver;

import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.CpSolver;
import com.google.ortools.sat.IntVar;
import ecnu.dbhammer.result.ResultRow;
import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.schema.ForeignKey;
import ecnu.dbhammer.schema.PrimaryKey;
import ecnu.dbhammer.schema.Table;
import ecnu.dbhammer.schema.genefunc.AttrGeneFunc;
import ecnu.dbhammer.schema.genefunc.LinearFunc;
import ecnu.dbhammer.schema.genefunc.ModFunc;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @author xiangzhaokun
 * @ClassName SolveTask.java
 * @Description 求解器的求解任务，目前每个任务是以主键划分，每个任务被分配到不同的线程上求解，增加求解的效率
 * @createTime 2022年04月12日 20:13:00
 */
public class SolveTask implements Callable<List<ResultRow>> {

    private Table biggestTable;

    private List<Table> variableList;//涉及的其他变量

    private Pair<Integer,Integer> biggestTablePartialDomain;

    private List<Pair<Integer,Integer>> domainList;//涉及变量经过过滤后的值域

    private List<JoinCondition> joinConditionList;//连接条件

    private String[] providedTable;//需要求的表的主键值

    public SolveTask(Table biggestTable,List<Table> variableList, Pair<Integer,Integer> biggestTablePartialDomain,List<Pair<Integer,Integer>> domainList,List<JoinCondition> joinConditionList, String[] providedTable){
        this.biggestTable = biggestTable;
        this.variableList = variableList;
        this.biggestTablePartialDomain = biggestTablePartialDomain;
        this.domainList = domainList;
        this.joinConditionList = joinConditionList;
        this.providedTable = providedTable;
    }

    public List<ResultRow> call() throws Exception {
        //System.out.println("here 1");
        CpModel model = new CpModel();
        // Create the  variables.
        IntVar[] intVars = new IntVar[this.variableList.size()];

        // 保存每个表主键的范围，即线性规划的变量范围
        Map<String,IntVar> mp = new HashMap<>();
        for(int i=0;i<this.variableList.size();i++) {
            if(this.variableList.get(i).getTableName().equals(this.biggestTable.getTableName())){
                intVars[i]  = model.newIntVar(this.biggestTablePartialDomain.getLeft(), this.biggestTablePartialDomain.getRight(), this.biggestTable.getTableName());
            }else {
                intVars[i] = model.newIntVar(domainList.get(i).getLeft(), domainList.get(i).getRight(), variableList.get(i).getTableName());
            }
            mp.put(variableList.get(i).getTableName(),intVars[i]);
        }
        // Create the constraints.

        for(int i=0;i<this.joinConditionList.size();i++){
            //这里要注意对应变量关系
            JoinCondition joinCondition = joinConditionList.get(i);
            Attribute leftAttribute = joinCondition.getLeftAttribute();
            Attribute rightAttribute = joinCondition.getRightAttribute();

            AttrGeneFunc leftFunc = leftAttribute.getColumnGeneExpressions().get(0);
            AttrGeneFunc rightFunc = rightAttribute.getColumnGeneExpressions().get(0);

            String leftTableName = leftAttribute.getTableName();
            String rightTableName = rightAttribute.getTableName();

            IntVar leftVar = mp.get(leftTableName);
            IntVar rightVar = mp.get(rightTableName);
            if(leftAttribute instanceof PrimaryKey || rightAttribute instanceof PrimaryKey) {
                if(leftAttribute instanceof PrimaryKey && rightAttribute instanceof ForeignKey){
                    if(rightFunc instanceof LinearFunc){
                        Long cof = ((LinearFunc) rightFunc).getCoefficient1().longValue();
                        Long offset = ((LinearFunc) rightFunc).getCoefficient0().longValue();
                        LinearSolverFunc linearSolverFunc = new LinearSolverFunc(cof,rightVar,offset);
                        model.addEquality(new LinearSolverFunc(1,leftVar,0), linearSolverFunc);
                    }else if(rightFunc instanceof ModFunc){ // 目前fk均是基于modFunc
                        long base = ((ModFunc) rightFunc).getBase();
                        model.addModuloEquality(leftVar,rightVar,base);
                    }

                }else if(leftAttribute instanceof ForeignKey && rightAttribute instanceof PrimaryKey){
                    if(leftFunc instanceof LinearFunc){
                        Long cof = ((LinearFunc) leftFunc).getCoefficient1().longValue();
                        Long offset = ((LinearFunc) leftFunc).getCoefficient0().longValue();
                        LinearSolverFunc linearSolverFunc = new LinearSolverFunc(cof,rightVar,offset);
                        model.addEquality(linearSolverFunc,new LinearSolverFunc(1,rightVar,0));

                    }else if(leftFunc instanceof ModFunc){
                        long base = ((ModFunc) leftFunc).getBase();
                        model.addModuloEquality(rightVar,leftVar,base);

                    }
                }
            }else{
                //TODO  FK-FK Join待做
            }
        }

        CpSolver solver = new CpSolver();

        IntVar[] printerValues = new IntVar[providedTable.length];
        for(int k=0;k< providedTable.length;k++){
            printerValues[k] = mp.get(providedTable[k]);
        }
        SolutionPrinter cb = new SolutionPrinter(printerValues);
        // Tell the solver to enumerate all solutions.并将结果按照providedTable的顺序排列
        solver.getParameters().setEnumerateAllSolutions(true);

        // And solve.
        solver.searchAllSolutions(model, cb);
        //System.out.println("here 2");
        return cb.getResult();
    }
}
