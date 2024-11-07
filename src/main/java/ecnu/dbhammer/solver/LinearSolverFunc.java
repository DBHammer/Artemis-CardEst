package ecnu.dbhammer.solver;

import com.google.ortools.sat.IntVar;
import com.google.ortools.sat.LinearExpr;

/**
 * @author xiangzhaokun
 * @ClassName LinearForJoin.java
 * @Description 求解器定义求解任务的一次函数约束
 * @createTime 2022年04月08日 21:44:00
 */
public class LinearSolverFunc implements LinearExpr {

    public IntVar var;
    public long coef;
    public long offset;
    public LinearSolverFunc(long coef, IntVar var, long offset){
        this.coef = coef;
        this.var = var;
        this.offset = offset;
    }
    @Override
    public int numElements() {
        return 1;
    }

    @Override
    public IntVar getVariable(int i) {
        return this.var;
    }

    @Override
    public long getCoefficient(int i) {
        return this.coef;
    }

    @Override
    public long getOffset() {
        return this.offset;
    }
}
