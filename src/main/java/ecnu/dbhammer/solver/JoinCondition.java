package ecnu.dbhammer.solver;

import ecnu.dbhammer.query.Join;
import ecnu.dbhammer.query.type.JoinMode;
import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.schema.ForeignKey;
import ecnu.dbhammer.schema.PrimaryKey;
import ecnu.dbhammer.schema.genefunc.AttrGeneFunc;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;

/**
 * @author xiangzhaokun
 * @ClassName JoinCondition.java
 * @Description 连接关系
 * @createTime 2022年04月08日 22:02:00
 */
public class JoinCondition implements Serializable {
    private Attribute leftAttribute;
    private Attribute rightAttribute;

    //连接关系的构建规则，如果是PK-FK Join，则leftAttribute为FK，rightAttribute为PK
    //如果是FK-FK Join，则两个都为FK
    public JoinCondition(Attribute leftAttribute, Attribute rightAttribute){
        this.leftAttribute = leftAttribute;
        this.rightAttribute = rightAttribute;
    }

    public Attribute getLeftAttribute() {
        return leftAttribute;
    }

    public Attribute getRightAttribute() {
        return rightAttribute;
    }

    public JoinMode judge(){
        if(leftAttribute instanceof PrimaryKey || rightAttribute instanceof PrimaryKey){
            return JoinMode.PK2FK;
        }else if(leftAttribute instanceof ForeignKey && rightAttribute instanceof ForeignKey){
            return JoinMode.FK2FK;
        }else{
            return JoinMode.CrossJoin;
        }
    }
    @Override
    public String toString() {
        return "JoinCondition{" +
                 leftAttribute.getFullAttrName() +
                "=" + rightAttribute.getFullAttrName() +
                '}';
    }
}
