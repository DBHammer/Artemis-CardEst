package ecnu.dbhammer.result;

import ecnu.dbhammer.constant.LogicRelation;
import ecnu.dbhammer.schema.Table;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

/**
 * @author xiangzhaokun
 * @ClassName ConstraintExpression.java
 * @Description 约束表达式组，用于求解过滤谓词
 * @createTime 2021年11月28日 23:40:00
 */
public class ConstraintGroup {
    //约束组
    private int type;//=0属于过滤算子的主键约束组，=1属于连接算子的主键约束组

    private Table belongs; //属于哪张表
    private int lowerBound;// 下界
    private int upperBound;//上界
    private List<SingleConstraint> constraints; //约束

    private LogicRelation logicRelation;//约束之间的关系//TODO

    public ConstraintGroup(Table belongs, int lowerBound, int upperBound, List<SingleConstraint> constraints, LogicRelation logicRelation, int type) {
        this.belongs = belongs;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.constraints = constraints;
        this.logicRelation = logicRelation;
        this.type = type;
    }


    /**
     * 计算最终满足约束的基数大小
     * @return
     */
    public int getCardinalitySize() {
        int ans=0;
        if(this.logicRelation == LogicRelation.AND) {
            System.out.println("满足所有And约束");
            System.out.println("有"+this.constraints.size()+"个约束");

            for (int i = lowerBound; i <= upperBound; i++) {
                if (this.meetAndConstraint(i)) {
                    ans++;
                }
            }
            return ans;
        }else{
            for (int i = lowerBound; i <= upperBound; i++) {
                if (this.meetOrConstraint(i)) {
                    ans++;
                }
            }
            return ans;
        }
    }

    /**
     * 计算最终满足约束的主键结果集
     * @return
     * @throws Exception
     */
    public ResultSet getResultSet() throws Exception {
        ResultSet idSet = new ResultSet();

        if(this.type==0 && (1==0)){
            //均匀分布数据下过滤算子的求解，只需要求得上界与下界，目前还有一点Bug
            Pair<Integer,Integer> bound = constraints.get(0).getReverse();
            idSet.setLowerBound(bound.getLeft());
            idSet.setUpperBound(bound.getRight());

            idSet.setOnlyBound(true);

        }else {
            //TODO 非均匀分布的基数生成方法待修改
            System.out.println("进入求解过程");


            if (this.logicRelation == LogicRelation.AND) {
                for (int i = lowerBound; i <= upperBound; i++) {
                    if (this.meetAndConstraint(i)) {
                        idSet.add(i);
                    }
                }
                return idSet;
            } else {
                for (int i = lowerBound; i <= upperBound; i++) {
                    if (this.meetOrConstraint(i)) {
                        idSet.add(i);
                    }
                }
            }
        }
        return idSet;
    }

    /**
     *
     * @param pk
     * @return 对于一个pk,是否满足该约束组中的所有and约束,因为是要满足所有的约束，所以如果发现有一个不满足，直接return false
     */
    private boolean meetAndConstraint(int pk){
        for(SingleConstraint singleConstraint : this.constraints){
            if(!singleConstraint.meetConstraint(pk)){
                return false;
            }
        }
        return true;
    }

    //满足or约束
    private boolean meetOrConstraint(int pk){
        for(SingleConstraint singleConstraint : this.constraints){
            if(singleConstraint.meetConstraint(pk)){
                return true;
            }
        }
        return false;
    }
    public int getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(int upperBound) {
        this.upperBound = upperBound;
    }

    public int getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(int lowerBound) {
        this.lowerBound = lowerBound;
    }

    public List<SingleConstraint> getConstraints() {
        return constraints;
    }

    public void setConstraints(List<SingleConstraint> constraints) {
        this.constraints = constraints;
    }
}
