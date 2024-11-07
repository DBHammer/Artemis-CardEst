package ecnu.dbhammer.test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class RawNode {
    public final String nodeType;//hashjoin IndexReader IndexRangeScan....
    public final String operatorInfo;
    public final long rowCount;
    public long middleResult;
    public String tableName;
    public final String id;
    public RawNode left;
    public RawNode right;
    //当该节点是join节点时，以下参数会用到
    public String leftJoinColummnName;
    public String rightJoinColumnName;
    public Map<String, String> preJoinRelations;
    public String bracketsStr;

    public RawNode(String id, RawNode left, RawNode right, String nodeType, String operatorInfo, long rowCount, long middleResult, String tableName) {
        this.id = id;
        this.left = left;
        this.right = right;
        this.nodeType = nodeType;
        this.operatorInfo = operatorInfo;
        this.rowCount = rowCount;
        this.middleResult = middleResult;
        this.tableName = tableName;
        leftJoinColummnName = "";
        rightJoinColumnName = "";
        preJoinRelations = new LinkedHashMap<>();
        bracketsStr = "";
    }

    @Override
    public String toString() {
        return "RawNode{id=" + id + "; nodeType=" +nodeType +"; operatorInfo="+operatorInfo+"; rowCount="+rowCount+"}";
    }

    public void setMiddleResult (long middleresult) {
        this.middleResult = middleresult;
    }

    public void setTableName (String tableName) {
        this.tableName = tableName;
    }

    public void setLeftJoinColummnName (String leftJoinColummnName) {this.leftJoinColummnName = leftJoinColummnName;}

    public void setRightJoinColumnName (String rightJoinColumnName) {this.rightJoinColumnName = rightJoinColumnName;}

    public void setPreJoinRelatons (Map<String, String> preJoinRelations) {this.preJoinRelations = preJoinRelations;}

    public Map<String, String> getPreJoinRelations () {return preJoinRelations;}

    public void setBracketsStr(String bracketsStr) {this.bracketsStr = bracketsStr;}
}

