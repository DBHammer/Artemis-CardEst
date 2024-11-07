package ecnu.dbhammer.query;

import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.schema.ForeignKey;
import ecnu.dbhammer.solver.JoinCondition;

import java.util.*;

/**
 * @author tingc
 * @ClassName ComplexJoin.java
 * @Description 复合join，包含多个join条件
 * @createTime 2022年3月16日 14:35:00
 */
public class ComplexJoin extends QueryNode {
    private List<Join> joins;
    private LinkedHashMap<Attribute,Attribute> joinRelations;
    private Set<String> joinRelationsSet;
    private List<JoinCondition> joinConditionList;

    //左右子节点
    private QueryNode leftChildNode;
    private QueryNode rightChildNode;

    private String name;
    public ComplexJoin(List<Join> joins){
        super();
        this.joins = joins;
        this.joinRelations = new LinkedHashMap<>();
        this.joinRelationsSet = new HashSet<>();
        this.joinConditionList = new ArrayList<>();
        this.name = joins.get(0).getName();
        //融合所有涉及的Join条件
        for(Join join : joins){
            LinkedHashMap<Attribute, Attribute> joinRelationsPerJoin = join.getJoinRelations();
            Iterator<Map.Entry<Attribute,Attribute>> it = joinRelationsPerJoin.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry<Attribute, Attribute> entry = it.next();
                String relationStr = ((ForeignKey)entry.getKey()).getTableName()+"."+((ForeignKey)entry.getKey()).getFkColumnName()
                        + "->" + entry.getValue().getTableName();
                if(!joinRelationsSet.contains(relationStr)) {
                    joinRelations.put(entry.getKey(), entry.getValue());
                    joinConditionList.add(new JoinCondition(entry.getKey(), entry.getValue()));
                    joinRelationsSet.add(relationStr);

                }
            }
        }
        this.leftChildNode = joins.get(0).getLeftChildNode();
        this.rightChildNode = joins.get(0).getRightChildNode();
        //一般是rightChildNode和leftChildNode中存在多个连接关系
    }

    public List<Join> getJoins(){
        return this.joins;
    }

    public LinkedHashMap<Attribute, Attribute> getJoinRelations(){
        return this.joinRelations;
    }
    public String getName(){
        return this.name;
    }
    public QueryNode getLeftChildNode() { return this.leftChildNode; }
    public QueryNode getRightChildNode() { return this.rightChildNode; }

    public List<JoinCondition> getJoinConditionList() {
        return joinConditionList;
    }
}
