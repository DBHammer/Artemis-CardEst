package ecnu.dbhammer.result;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.data.AttrValue;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.query.type.ComparisonOperatorType;
import ecnu.dbhammer.query.type.ExpressionType;
import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.schema.Table;

import java.math.BigDecimal;
import java.util.*;


import ecnu.dbhammer.query.*;
import ecnu.dbhammer.schema.*;

/**
 * 通过生成的Filter，获得Filter上的约束条件，以便求解器进行求解
 */
public class FilterResult implements NodeResult{

    // 根据什么filter产生结果
    private Filter filter;
    // 在哪张表上进行filter
    private Table table;

    // Filter上的约束集合，最终转化为整数规划问题
    private List<SingleConstraint> result = new ArrayList<>();


    public FilterResult(Table table, List<SingleConstraint> result){
        this.table = table;
        this.result = result;
        this.filter = null;
    }
    public FilterResult(Filter filter) {
        super();
        this.filter = filter;
        this.table = filter.getTable();
        geneFilterResult();
    }

    public void geneFilterResult() {

        RecordLog.recordLog(LogLevelConstant.INFO,"开始生成"+this.filter.getTable().getTableName()+"的FilterResult");

        for(int i=0; i<filter.getPredicateList().size(); i++) {
            Predicate predicate = filter.getPredicateList().get(i);
            String predicateText = filter.getPredicateList().get(i).getText();//得到predicate的string形式 比如a+b>3
            RecordLog.recordLog(LogLevelConstant.INFO,"Filter: " + predicateText);
            ComparisonOperatorType comparisionOperator = predicate.getComparisonOperator();
            // in , not in , between and 进行转化 in , not in 转化为等于和不等于 between and 转化为 > 和 <
            if (predicate.getExpression().getType() == ExpressionType.DIGIT) {     //数值型表达式
                // 1: ">", 2: ">=", 3: "<", 4: "<=", 5: "between and", 6: "=", 7: "!="
                // 8: "in", 9: "not in", 10: "like", 11: "not like", 12: "is", 13: "is not"
                if (comparisionOperator == ComparisonOperatorType.BetweenAnd) {  // 比较操作符为between and
                    // 谓词转化 找到其中的两个参数 对谓词使用大于和小于进行替代
                    BigDecimal parameter1 = (BigDecimal) predicate.getParameters()[0].value;
                    BigDecimal parameter2 = (BigDecimal) predicate.getParameters()[1].value;

                    SingleConstraint[] singleConstraints = new SingleConstraint[2];
                    singleConstraints[0] = new SingleConstraint(this.table, predicate.getExpression(), parameter1, ComparisonOperatorType.Greater.getName());
                    singleConstraints[1] = new SingleConstraint(this.table, predicate.getExpression(), parameter2, ComparisonOperatorType.Less.getName());


                    convertToPkFormula(singleConstraints);
                } else if (comparisionOperator == ComparisonOperatorType.In || comparisionOperator == ComparisonOperatorType.NotIn) { //比较操作符为 in ，not in
                    //TODO in和not in记得重新做转换
                    AttrValue[] parameters = predicate.getParameters();
                    SingleConstraint[] singleConstraints = new SingleConstraint[parameters.length];
                    for (int j = 0; j < parameters.length; j++) {
                        if (comparisionOperator == ComparisonOperatorType.In) {
                            singleConstraints[j] = new SingleConstraint(this.table, predicate.getExpression(), (BigDecimal)parameters[j].value, ComparisonOperatorType.Equal.getName());
                        }
                        else {
                            singleConstraints[j] = new SingleConstraint(this.table, predicate.getExpression(), (BigDecimal)parameters[j].value, ComparisonOperatorType.NoEqual.getName());
                        }
                    }
                    convertToPkFormula(singleConstraints);
                } else {  // 比较操作符为 > , >= , < , <= , = , !=

                    SingleConstraint[] singleConstraints = new SingleConstraint[1];
                    ComparisonOperatorType operator = predicate.getComparisonOperator();
                    BigDecimal para = (BigDecimal) predicate.getParameters()[0].value;
                    singleConstraints[0] = new SingleConstraint(this.table, predicate.getExpression(), para, operator.getName());
                    convertToPkFormula(singleConstraints);
                }
            } else if (predicate.getExpression().getType() == ExpressionType.VARCHAR) {  // 字符型表达式
                if (comparisionOperator == ComparisonOperatorType.Equal || comparisionOperator == ComparisonOperatorType.NoEqual) {  // = , !=
                    // 提取数字部分 转化为对主键的约束
                    String[] split4Num = predicateText.split("#");
                    String number = split4Num[1];
                    //TODO 根据计算结果取整之后的值进行比较
                    //TODO = , !=记得重新做转换

                    SingleConstraint[] singleConstraints = new SingleConstraint[1];
                    ComparisonOperatorType operator = predicate.getComparisonOperator();

                    BigDecimal para = new BigDecimal(number);
                    singleConstraints[0] = new SingleConstraint(this.table, predicate.getExpression(), para, operator.getName());

                    if(predicate.isOrProbability()) {
                        String number2 = split4Num[5];
                        BigDecimal para2 = new BigDecimal(number2);
                        singleConstraints = new SingleConstraint[2];
                        singleConstraints[0] = new SingleConstraint(this.table, predicate.getExpression(), para, operator.getName());
                        singleConstraints[1] = new SingleConstraint(this.table, predicate.getExpression(), para2, operator.getName());
                    }


                    convertToPkFormula(singleConstraints);
                } else if (comparisionOperator == ComparisonOperatorType.In || comparisionOperator == ComparisonOperatorType.NotIn) {  // in , not in
                    //TODO  in , not in记得做转换
                    AttrValue[] parameters = predicate.getParameters();
                    String[] split4Num = null;
                    String[] numbers = new String[parameters.length];
                    for (int j = 0; j < parameters.length; j++) {
                        split4Num = ((String) parameters[j].value).split("#");
                        numbers[j] = split4Num[1];
                    }

                    SingleConstraint[] singleConstraints = new SingleConstraint[parameters.length];

                    ComparisonOperatorType operator;
                    if (predicate.getComparisonOperator() == ComparisonOperatorType.In) {
                        operator = ComparisonOperatorType.Equal;
                    } else {
                        operator = ComparisonOperatorType.NoEqual;
                    }

                    BigDecimal[] para = new BigDecimal[parameters.length];
                    for (int j = 0; j < parameters.length; j++) {
                        para[j] = new BigDecimal(numbers[j]);
                        singleConstraints[j] = new SingleConstraint(this.table, predicate.getExpression(), para[j], operator.getName());
                    }

                    convertToPkFormula(singleConstraints);
                } else {   // like , not like
                    // TODO like not like 转化
                    // varchar类型的like可能的形式为
                    // #%#varchar#int#  -----1
                    // #int#%#int#      -----2
                    // #int#varchar#%#  -----3
                    // #%#varchar#%#    -----4
                    AttrValue pm = predicate.getParameters()[0];
                    //  1、2、3三种情况只需要进行int部分的比较，可以得到唯一的结果
                    //  4这种情况可能有多个满足条件的主键值
                    
                }
            } else if (predicate.getExpression().getType() == ExpressionType.TIME) {  // 日期型表达式
                //TODO 日期型记得做转换
                if (comparisionOperator == ComparisonOperatorType.BetweenAnd) { // between and
                    AttrValue parameter1 = predicate.getParameters()[0];
                    AttrValue parameter2 = predicate.getParameters()[1];
        
                    SingleConstraint[] singleConstraints = new SingleConstraint[2];
                    singleConstraints[0] = new SingleConstraint(this.table, predicate.getExpression(), (BigDecimal)parameter1.value, ComparisonOperatorType.Greater.getName());
                    singleConstraints[1] = new SingleConstraint(this.table, predicate.getExpression(), (BigDecimal)parameter2.value, ComparisonOperatorType.Less.getName());

                    convertToPkFormula(singleConstraints);
                } else { // > , >= , < , <=
                    SingleConstraint[] oneSingleConstraint = new SingleConstraint[1];

                    oneSingleConstraint[0] = new SingleConstraint(this.table, predicate.getExpression(), (BigDecimal)predicate.getParameters()[0].value, predicate.getComparisonOperator().getName());
                    
                    convertToPkFormula(oneSingleConstraint);
                }
            } else if (predicate.getExpression().getType() == ExpressionType.BOOL) {  // 布尔型表达式
                // 查询中的bool值选择随机产生
                // 数据bool值的生成依据计算结果的奇偶性
                System.out.println("布尔类型数据的产生暂时与主键无关");//要么是0要么是1这样的列与主键没有关系
                //TODO 可以有关系

            }
        }

    }

    public void convertToPkFormula(SingleConstraint[] singleConstraints) {
        //new solver
        for (int j = 0; j < singleConstraints.length; j++) {//对于每一个谓词表达式，里面的所有参与运算的colunm

            result.add(singleConstraints[j]);
        }
    }

    public Table getTable() {
        return this.table;
    }


    public List<SingleConstraint> getResult() {
        return result;
    }

    public Filter getFilter(){
        return this.filter;
    }

}
