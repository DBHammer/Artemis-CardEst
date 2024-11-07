package ecnu.dbhammer.solver;

import com.google.ortools.Loader;
import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.CpSolver;
import com.google.ortools.sat.CpSolverSolutionCallback;
import com.google.ortools.sat.IntVar;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName SolverTest.java
 * @Description TODO
 * @createTime 2022年04月12日 16:36:00
 */
public class SolverTest {
    List<String> variableList = new ArrayList<>();
    List<Pair<Integer,Integer>> domainList = new ArrayList<>();

    List<List<Integer>> result;

    public SolverTest(List<String> variableList, List<Pair<Integer,Integer>> domainList){
        this.variableList = variableList;
        this.domainList = domainList;
    }

    public static void main(String[] args) {
        Loader.loadNativeLibraries();
        long start  = System.currentTimeMillis();
        SolverTest.solveTask(0,100000);
        long end = System.currentTimeMillis();
        System.out.println((end-start));
    }


    public static List<List<Integer>> solveTask(int left, int right){


        // Create the model.
        CpModel model = new CpModel();
        IntVar t1 = model.newIntVar(left, right, "table_4");
        IntVar t2 = model.newIntVar(0, 100000, "table_0");
        IntVar t3 = model.newIntVar(0,100000,"table_1");
        model.addEquality(new LinearSolverFunc(1,t2,0),new LinearSolverFunc(1,t3,0));
        model.addModuloEquality(t2,t1,100000);
        CpSolver solver = new CpSolver();

        SolverTest.VarArraySolutionPrinterTest cb = new SolverTest.VarArraySolutionPrinterTest(new IntVar[] {t1,t2});
        // Tell the solver to enumerate all solutions.
        solver.getParameters().setEnumerateAllSolutions(true);

        // And solve.
        solver.searchAllSolutions(model, cb);
        System.out.println("解数"+cb.getEveryResult().size());


        return cb.getEveryResult();
    }
    static class VarArraySolutionPrinterTest extends CpSolverSolutionCallback {
        public VarArraySolutionPrinterTest(IntVar[] variables) {
            variableArray = variables;
        }

        @Override
        public void onSolutionCallback() {
            List<Integer> row = new ArrayList<>();
            for (IntVar v : variableArray) {
                row.add((int) value(v));
            }
            //System.out.println(row);
            everyResult.add(row);
        }

        public List<List<Integer>> getEveryResult() {
            return everyResult;
        }
        private final IntVar[] variableArray;

        private List<List<Integer>> everyResult = new ArrayList<>();
    }

}
