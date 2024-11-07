package ecnu.dbhammer.solver;

import com.google.ortools.sat.CpSolverSolutionCallback;
import com.google.ortools.sat.IntVar;
import ecnu.dbhammer.result.ResultRow;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName SolutionPrinter.java
 * @Description 用于输出求解器的结果
 * @createTime 2022年04月12日 20:16:00
 */
public class SolutionPrinter extends CpSolverSolutionCallback {
    private final IntVar[] variableArray;

    private List<ResultRow> result = new ArrayList<>();

    public SolutionPrinter(IntVar[] variables) {
        variableArray = variables;
    }

    @Override
    public void onSolutionCallback() {
        List<Integer> row = new ArrayList<>();
        for (IntVar v : variableArray) {
            row.add((int) value(v));
        }
        result.add(new ResultRow(row));
    }

    public List<ResultRow> getResult() {
        return result;
    }
}
