package ecnu.dbhammer.result;

import ecnu.dbhammer.data.AttrValue;
import ecnu.dbhammer.data.DataType;
import ecnu.dbhammer.query.PredicateExpression;
import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.schema.Table;
import ecnu.dbhammer.schema.genefunc.AttrGeneFunc;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

/**
 * @author xiangzhaokun
 * @ClassName SingleConstraint.java
 * @Description 单个约束，比如ax+b<c，求解器最关键的类
 * @createTime 2021年12月01日 17:15:00
 */
public class SingleConstraint implements Serializable {
    private Table table;//约束属于哪张表
    private PredicateExpression predicateExpression;
    private BigDecimal parameter;
    private String operator;


    public SingleConstraint(Table table,PredicateExpression predicateExpression, BigDecimal parameter, String operator){
        this.table = table;
        this.predicateExpression = predicateExpression;
        this.parameter = parameter;
        this.operator = operator;
    }


    public Pair<Integer,Integer> getReverse() throws Exception {
        AttrGeneFunc func = predicateExpression.getComUnit();
        return func.getSolveBound(parameter,operator,0,table.getTableSize()-1);
    }


    /**
     * 对于一个主键，判断该约束是否满足。3*col_1 + 4*col_2 < 100, 每一列生成函数不同，也能确保计算
     * 每一列的生成函数选择，是attribute负责的
     * 求解器最关键的函数
     * @param pk
     * @return
     */
    public boolean meetConstraint(int pk){

        List<Attribute> attributes = this.predicateExpression.getVariables();
        List<BigDecimal> coffs = this.predicateExpression.getCoff();

        BigDecimal left = BigDecimal.ZERO;

        int dk = pk;
        //update by ct: 关联列处理
        //表内关联：主键-->驱动列-->关联列
        if(this.predicateExpression.getCols().get(0).getCorrelationFactor() != 0
                && this.predicateExpression.getCols().get(0).getDrivingFK() == null){
            BigDecimal drivingKey = this.predicateExpression.getCols().get(0).getDrivingColumn().attrEvaluate(pk);
            dk = drivingKey.intValue();
        }
        //表间关联：主键-->外键-->驱动列主键-->驱动列-->关联列
        else if(this.predicateExpression.getCols().get(0).getCorrelationFactor() != 0){
            // 拿到主键对应的外键
            BigDecimal fKey = this.predicateExpression.getCols().get(0).getDrivingFK().attrEvaluate(pk);
            // 外键即驱动列的主键，根据驱动列的主键计算驱动列
            BigDecimal drivingKey = this.predicateExpression.getCols().get(0).getDrivingColumn().attrEvaluate(fKey.intValue());
            dk = drivingKey.intValue();
        }

        //对部分相关的关联数据，要根据主键找对应的生成函数-->具体实现不同在attrEvaluate和attrEvaluate2中
        for(int i=0;i<attributes.size();i++){
            if(this.predicateExpression.getCols().get(0).getCorrelationFactor() != 2)
                left = left.add(coffs.get(i).multiply(attributes.get(i).attrEvaluate(dk)));
            else
                left = left.add(coffs.get(i).multiply(attributes.get(i).attrEvaluate2(pk,dk)));
        }
        if(coffs.size() == attributes.size()+1){
            left = left.add(coffs.get(coffs.size()-1));
        }

        if(new AttrValue(DataType.DECIMAL,left).Compare(new AttrValue(DataType.DECIMAL,parameter),this.operator)){
            return true;
        }else{
            return false;
        }
    }


    public String toString(){
        return "Table " +  this.table.getTableName() +" | (" + predicateExpression.getText() + ")" + this.operator + parameter.toString();
    }



    public BigDecimal getParameter() {
        return parameter;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public void setTable(Table table){
        this.table = table;
    }

    public Table getTable(){
        return this.table;
    }

    public PredicateExpression getPredicateExpression(){
        return this.predicateExpression;
    }

}
