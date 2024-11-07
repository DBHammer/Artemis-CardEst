package ecnu.dbhammer.solver;


import com.google.ortools.Loader;
import com.google.ortools.sat.*;

import java.util.*;

/**
 * @author xiangzhaokun
 * @ClassName SearchForAllSolutionsSampleSat.java
 * @Description Google-OR Tools求解器测试程序
 * @createTime 2022年04月08日 15:38:00
 */
/** Code sample that solves a model and displays all solutions. */
public class SearchForAllSolutionsSampleSat {

    public static List<List<Integer>> key = new ArrayList<>();

    public static Map<String, Set<Integer>> table2Key = new HashMap<>();

    public static Set<Integer> set = new HashSet<>();
    public static void main(String[] args) throws Exception {
        Loader.loadNativeLibraries();
        // Create the model.
        CpModel model = new CpModel();

        // Create the variables.

        IntVar t1 = model.newIntVar(0, 13605, "table_5");
        IntVar t2 = model.newIntVar(6604, 29679, "table_3");
        //IntVar t3 = model.newIntVar(731, 23920, "table_8");
//        IntVar t4 = model.newIntVar(0, 666, "table_9");
//        IntVar t5 = model.newIntVar(215, 4514, "table_7");
//        IntVar t6 = model.newIntVar(257, 5109, "table_6");
//        IntVar t7 = model.newIntVar(345, 5437, "table_4");
//        IntVar t8 = model.newIntVar(0, 1529, "table_2");
        // Create the constraints.

        model.addModuloEquality(t1,t2,15577);
        //model.addModuloEquality(t1,t3,15577);


        // Create a solver and solve the model.
        CpSolver solver = new CpSolver();
        VarArraySolutionPrinter cb = new VarArraySolutionPrinter(new IntVar[] {t1,t2});
        // Tell the solver to enumerate all solutions.
        solver.getParameters().setEnumerateAllSolutions(true);

        // And solve.
        long startTime  = System.currentTimeMillis();
        solver.searchAllSolutions(model, cb);
        System.out.println(System.currentTimeMillis() - startTime);


        System.out.println(set);



        System.out.println(cb.getSolutionCount() + " solutions found.");
    }

    static class VarArraySolutionPrinter extends CpSolverSolutionCallback {
        public VarArraySolutionPrinter(IntVar[] variables) {
            variableArray = variables;
        }

        @Override
        public void onSolutionCallback() {

//            System.out.printf("Solution #%d: time = %.02f s%n", solutionCount, wallTime());
            List<Integer> row = new ArrayList<>();
            for (IntVar v : variableArray) {

                int value = (int) value(v);
                row.add(value);
            }
            key.add(row);
            solutionCount++;

        }

        public int getSolutionCount() {
            return solutionCount;
        }

        private int solutionCount;
        private final IntVar[] variableArray;
    }
}