package ecnu.dbhammer.query;



import ecnu.dbhammer.query.type.JoinMode;
import ecnu.dbhammer.query.type.JoinType;
import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.schema.ForeignKey;
import ecnu.dbhammer.schema.PrimaryKey;
import ecnu.dbhammer.schema.Table;
import ecnu.dbhammer.solver.JoinCondition;
import ecnu.dbhammer.utils.DeepClone;
import org.apache.commons.lang3.SerializationUtils;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 连接生成
 */
public class Join extends QueryNode {

    // --------------- 配置参数信息 ---------------
    private static double innerJoinProbability = 1.0;
    private static double fullJoinProbability = 0.0;
    private static double leftJoinProbability = 0.0;
    private static double rightJoinProbability = 0.0;
    private static double leftsemiJoinProbability = 0.0;
    private static double leftantiJoinProbability = 0.0;

    // --------------- 配置参数信息 ---------------

    private JoinType joinType;

    private JoinMode joinMode;

    // 左右孩子节点可以为：Join，Filter，Table
    //todo 加入Table类型-x
    // 目前所有数据表上都是有Filter的，故左右孩子节点仅为：Join或者Filter -x

    private QueryNode leftChildNode;
    private QueryNode rightChildNode;


    //两个Join的列，目前可以是PK-FKJoin以及FK-FKJoin
    private Attribute leftAttribute;
    private Attribute rightAttribute;


    // 表之间的joinRelation，在join构造函数赋值
    // 当queryNode的类型为Join时，记录发生连接的两张表的表名
    // 注意：key 为外键  value 为主键
    private LinkedHashMap<Attribute,Attribute> joinRelations = new LinkedHashMap<>();

    private List<JoinCondition> joinConditionList = new ArrayList<>();


    private String name;

    public Join(QueryNode leftChildNode, QueryNode rightChildNode,
                Attribute leftAttribute, Attribute rightAttribute) {
        super();
        double randomValue = Math.random();
        if (randomValue < innerJoinProbability) {
            this.joinType = JoinType.INNERJOIN;
        } else if (randomValue < innerJoinProbability + fullJoinProbability) {
            this.joinType = JoinType.FULLOUTERJOIN;
        } else if (randomValue < innerJoinProbability + fullJoinProbability
                + leftJoinProbability) {
            this.joinType = JoinType.LEFTOUTERJOIN;
        } else if (randomValue < innerJoinProbability + fullJoinProbability
                + leftJoinProbability + rightJoinProbability) {
            this.joinType = JoinType.RIGHTOUTERJOIN;
        } else if (randomValue < innerJoinProbability + fullJoinProbability
                + leftJoinProbability + rightJoinProbability + leftsemiJoinProbability) {
            this.joinType = JoinType.LEFTSEMIJOIN;   
        } else if (randomValue < innerJoinProbability + fullJoinProbability
                + leftJoinProbability + rightJoinProbability + leftsemiJoinProbability + leftantiJoinProbability) {
            this.joinType = JoinType.LEFTANTIJOIN;
        }//随机确定是什么join类型，目前只有InnerJoin,因为其他Join暂时还未想到用约束方法表示


        this.leftChildNode = leftChildNode;
        this.rightChildNode = rightChildNode;
        this.leftAttribute = leftAttribute;
        this.rightAttribute = rightAttribute;

        if(leftChildNode instanceof Filter) {//左边是filter，这里默认所有右边的表都是filter，filter和filter join
            if(leftAttribute instanceof PrimaryKey)//key 为外键 value为主键
            {
                joinRelations.put(rightAttribute,leftAttribute);//（x1,x2）x1为提供外键的column x2为提供主键的colunm
                joinConditionList.add(new JoinCondition(rightAttribute,leftAttribute));
            } else if(leftAttribute instanceof ForeignKey){
                joinRelations.put(leftAttribute,rightAttribute);
                joinConditionList.add(new JoinCondition(leftAttribute,rightAttribute));
            } else{
                joinRelations.put(null,null);
            }
            System.out.println("本次Join左边名称"+ leftChildNode.getName());
            System.out.println("本次Join右边名称"+rightChildNode.getName());

            this.name = ((Filter)leftChildNode).getName() + " " + rightChildNode.getName();
        } else if (leftChildNode instanceof Join ) {//join类型
            joinRelations = (LinkedHashMap<Attribute, Attribute>) DeepClone.deepCloneMap(leftChildNode.getJoinRelations());
            joinConditionList = DeepClone.deepCloneList(((Join) leftChildNode).getJoinConditionList());
            if(leftAttribute instanceof PrimaryKey)
            {
                joinRelations.put(rightAttribute,leftAttribute);
                joinConditionList.add(new JoinCondition(rightAttribute,leftAttribute));
            } else if(leftAttribute instanceof ForeignKey){
                joinRelations.put(leftAttribute,rightAttribute);
                joinConditionList.add(new JoinCondition(leftAttribute,rightAttribute));
            }else{
                joinRelations.put(null,null);
            }
            System.out.println("本次Join左边名称"+((Join)leftChildNode).getName());
            System.out.println("本次Join右边名称"+rightChildNode.getName());
            this.name = ((Join)leftChildNode).getName() + " " + rightChildNode.getName();
        } else if (leftChildNode instanceof ComplexJoin) { //update by ct: 复合join
            joinRelations = (LinkedHashMap<Attribute, Attribute>) DeepClone.deepCloneMap(leftChildNode.getJoinRelations());
            joinConditionList = DeepClone.deepCloneList(((ComplexJoin) leftChildNode).getJoinConditionList());

            if(leftAttribute instanceof PrimaryKey)
            {
                joinRelations.put(rightAttribute,leftAttribute);
                joinConditionList.add(new JoinCondition(rightAttribute,leftAttribute));
            } else if(leftAttribute instanceof ForeignKey){
                joinRelations.put(leftAttribute,rightAttribute);
                joinConditionList.add(new JoinCondition(leftAttribute,rightAttribute));
            }else{
                joinRelations.put(null,null);
            }
            System.out.println("本次ComplexJoin左边名称"+((ComplexJoin)leftChildNode).getName());
            System.out.println("本次ComplexJoin右边名称"+rightChildNode.getName());
            this.name = ((ComplexJoin)leftChildNode).getName() + " " + rightChildNode.getName();
        }

        if(leftAttribute instanceof ForeignKey && rightAttribute instanceof ForeignKey){
            this.joinMode = JoinMode.FK2FK;
        }else if(leftAttribute instanceof PrimaryKey || rightAttribute instanceof PrimaryKey){
            this.joinMode = JoinMode.PK2FK;
        }else if(leftAttribute == null || rightAttribute == null){
            this.joinMode = JoinMode.CrossJoin;
        }

    }

    public JoinMode getJoinMode(){
        return joinMode;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public QueryNode getLeftChildNode() {
        return leftChildNode;
    }

    public QueryNode getRightChildNode() {
        return rightChildNode;
    }

    public Attribute getLeftAttribute() {
        return leftAttribute;
    }

    public Attribute getRightAttribute() {
        return rightAttribute;
    }

    /**
     * 这里左边是FK，右边是PK
     * @return
     */
    public LinkedHashMap<Attribute,Attribute> getJoinRelations() {
        return this.joinRelations;
    }

    public List<JoinCondition> getJoinConditionList() {
        return joinConditionList;
    }

    public String getName(){
        return this.name;
    }

}
