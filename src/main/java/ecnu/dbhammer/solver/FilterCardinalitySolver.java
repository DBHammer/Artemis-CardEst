package ecnu.dbhammer.solver;

import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.constant.LogicRelation;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.query.type.ComparisonOperatorType;
import ecnu.dbhammer.query.type.ExpressionType;
import ecnu.dbhammer.result.ConstraintGroup;
import ecnu.dbhammer.result.FilterResult;
import ecnu.dbhammer.result.ResultSet;
import ecnu.dbhammer.result.SingleConstraint;

import java.util.*;

/**
 * 过滤运算求解器，目前是枚举
 */
public class FilterCardinalitySolver {

    public static List<Integer> Compute(FilterResult filterResult, int type) throws Exception {


        RecordLog.recordLog(LogLevelConstant.INFO,"表"+filterResult.getTable().getTableName()+"上的约束为"+filterResult.getResult());

        //solver
        List<SingleConstraint> constraints = new ArrayList<>();

        for(SingleConstraint singleConstraint : filterResult.getResult()){
            constraints.add(singleConstraint);
        }


        long ourSolverStart = System.currentTimeMillis();
        ConstraintGroup ourSolver;
        if(filterResult.getFilter()==null){
            //System.out.println("FilterResult为"+"NULL");
            ourSolver = new ConstraintGroup(filterResult.getTable(),0, filterResult.getTable().getTableSize() - 1, constraints, LogicRelation.AND,type);
        }else {
            ComparisonOperatorType comparisonOperatorType = filterResult.getFilter().getPredicateList().get(0).getComparisonOperator();
            if (comparisonOperatorType == ComparisonOperatorType.In) {
                ourSolver = new ConstraintGroup(filterResult.getTable(),0, filterResult.getTable().getTableSize() - 1, constraints, LogicRelation.OR,type);
            } else if((comparisonOperatorType == ComparisonOperatorType.Equal || comparisonOperatorType == ComparisonOperatorType.NoEqual) && 
            filterResult.getFilter().getPredicateList().get(0).getExpression().getType() == ExpressionType.VARCHAR && filterResult.getFilter().getPredicateList().get(0).isOrProbability()) {
                // (col1 = a or col1 = b)
                ourSolver = new ConstraintGroup(filterResult.getTable(),0, filterResult.getTable().getTableSize() - 1, constraints, LogicRelation.OR,type);
            }
            else {
                ourSolver = new ConstraintGroup(filterResult.getTable(),0, filterResult.getTable().getTableSize() - 1, constraints, LogicRelation.AND,type);
            }
        }

        ResultSet idSet = ourSolver.getResultSet();
        System.out.println("新求解器求解时间"+(System.currentTimeMillis() - ourSolverStart));

        if(idSet.isOnlyBound()){//目前这里不会走到（只存上下界有问题）
            System.out.println("过滤算子的求解，只需要确定上界与下界");
            List<Integer> list = new ArrayList<>();
            list.add(idSet.getLowerBound());
            list.add(idSet.getUpperBound());
            return list;
        }else{
            System.out.println("过滤算子的求解，需要获得所有的Key");
            return idSet.getKeyList();
        }
    }
}
