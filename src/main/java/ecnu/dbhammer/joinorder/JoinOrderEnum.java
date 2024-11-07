package ecnu.dbhammer.joinorder;

import ecnu.dbhammer.schema.Table;
import ecnu.dbhammer.test.RawNode;
import org.apache.commons.collections.CollectionUtils;

import java.util.*;

/**
 * @author xiangzhaokun + tingc
 * @ClassName JoinOrderEnum.java
 * @Description 枚举Join Order来评估查询优化器Join Order枚举的好坏；增加对bushy tree的枚举（tingc）
 * @createTime 2021年10月30日 19:44:00
 */
public class JoinOrderEnum {
    private List<Table> singleOrder;
    private List<String> allOrder;
    private Set<String> allBushyTreeOrder;
    private Set<String> allLeftDeepTreeOrder;
    private List<Table> relatedTables;
    private int n;

    public JoinOrderEnum(List<Table> relatedTables) {
        this.relatedTables = relatedTables;
        this.n = relatedTables.size();
        singleOrder = new ArrayList<>();
        allOrder = new ArrayList<>();
        allBushyTreeOrder = new HashSet<>();
        allLeftDeepTreeOrder = new HashSet<>();
    }

    //枚举n个数的所有排列
    public List<String> enumerateJoinOrder() {
        boolean[] book = new boolean[n];
        dfs(book);
        return this.allOrder;
    }

    //update by ct: 枚举指定枚举空间大小的左深树的连接顺序，生成n个顺序数，随机打乱
    public List<String> enumerateJoinOrder_LeftDeep() {
        //定义搜索空间大下  left_deep枚举空间：N!
        int space;
        switch(n){
            case 1:
                space = 1;
                break;
            case 2:
                space = 2;
                break;
            case 3:
                space = 6;
                break;
            case 4:
                space = 24;
                break;
            default:
                space = 97;
                break;
        }
        while(allLeftDeepTreeOrder.size() < space) {
            List<Table> tables = new ArrayList<>();
            CollectionUtils.addAll(tables, new Object[n]);
            Collections.copy(tables, relatedTables);
            Collections.shuffle(tables);//通过shuffle来打乱表的顺序
            String result = "";
            for(int i=0;i<tables.size();i++){
                if(i==0) result += tables.get(i).getTableName();
                else
                    result += "," + tables.get(i).getTableName();
            }
            if(!allLeftDeepTreeOrder.contains(result)) {
                allLeftDeepTreeOrder.add(result);
                allOrder.add(result);
            }
        }
        return this.allOrder;
    }

    //update by ct: 枚举包含bushy tree的连接顺序空间
    public List<String> enumerateJoinOrder_BushyTree() {
        //定义搜索空间大下
        int space;
        switch(n){
            case 1:
                space = 1;
                break;
            case 2:
                space = 2;
                break;
            case 3:
                space = 12;
                break;
            case 4:
                space = 97;
                break;
            default:
                //五表以上连接搜索空间太大导致结果出不来  
                space = 97;
                break;
        }
        while(allBushyTreeOrder.size() < space) {
            //第一步：生成树型
            boolean[] bitSequence = geneBitSequence(n);
            //第二步：生成表的排列（放入叶节点）
            List<Table> tables = new ArrayList<>();
            CollectionUtils.addAll(tables, new Object[n]);
            Collections.copy(tables, relatedTables);
            Collections.shuffle(tables);
            String result = "";
            int cnt = 0;
            //第三步：组合树型与叶节点的表
            for (int i = 0; i < bitSequence.length; i++) {
                if (bitSequence[i] == false) {
                    result += tables.get(cnt).getTableName() + " ";
                    cnt++;
                } else
                    result += bitSequence[i] + " ";
            }
            if(!allBushyTreeOrder.contains(result)){
                allBushyTreeOrder.add(result);
                //第四步：建连接树，遍历得到括号形式的连接顺序，如 ((A,(B,C)),D)
                RawNode newRoot = buildNewJoinOrderTree(result, n);
                bushyTreeToString(newRoot);
                allOrder.add(newRoot.bracketsStr);
            }
        }
        return this.allOrder;
    }

    //生成join树第一步：生成位序列
    public static boolean[] geneBitSequence(int num) {
        int len = 2*num-1;
        boolean[] seq = new boolean[len];
        seq[0] = true;
        //String seq = "1";
        int cnt0 = 0;
        int cnt1 = 1;
        double p; //p为该位填0的概率
        int bit_num = 2 * (num -1);
        int i;
        for(i=1; i<bit_num; i++){
            if(cnt0 == num-1 || cnt1 == num-1)
                break;
            int r = cnt1 - cnt0;
            int k = bit_num - i;
            p = (r * (k + r + 2) * 1.0) / (2.0 * k * (r + 1));
            double d = Math.random();
            if(d < p && cnt0 < cnt1) {
                //seq += "0";
                seq[i] = false;
                cnt0 ++;
            }
            else {
                //seq += "1";
                seq[i] = true;
                cnt1 ++;
            }
        }
        if(i<bit_num && cnt1 == num-1){
            for(int j=0; j<bit_num-i; j++)
                //seq += "0";
                seq[i] = false;
        }
        if(i<bit_num && cnt0 == num-1){
            for(int j=0; j<bit_num-i; j++)
                //seq += "1";
                seq[i] = true;
        }
        //seq +="0";
        seq[i] = false;
        return seq;
    }

    public static RawNode buildNewJoinOrderTree (String sequence, int num){
        String temp[] = sequence.trim().split(" ");
        int join_index[] = new int[temp.length];
        int cnt = 0;
        for(int i=0; i<temp.length; i++){
            if(temp[i].trim().equals("true"))
                cnt ++;
            join_index[i] = cnt-1;
        }
        int len = 2*num-1;

        List<RawNode> nodeList = new ArrayList<>();
        //先建根节点
        RawNode rawNodeRoot = new RawNode("", null, null, "join", "", 0, 0, "");
        nodeList.add(rawNodeRoot);

        for(int i=0; i<len; i++){
            if(temp[i].trim().equals("true")){
                int left = 2*join_index[i] + 1;
                int right = 2*join_index[i] + 2;
                RawNode leftNode;
                RawNode rightNode;
                if(temp[left].trim().equals("true")){
                    leftNode = new RawNode("", null, null, "join", "",0,0,"");
                }
                else {
                    leftNode = new RawNode("", null, null, "selection", "",0,0,temp[left].trim());
                    RawNode leftLeftNode = new RawNode("", null, null, "scan", "",0,0,temp[left].trim());
                    leftNode.left = leftLeftNode;
                }
                if(temp[right].trim().equals("true")){
                    rightNode = new RawNode("", null, null, "join", "",0,0,"");
                }
                else {
                    rightNode = new RawNode("", null, null, "selection", "",0,0,temp[right].trim());
                    RawNode rightLeftNode = new RawNode("", null, null, "scan", "",0,0,temp[right].trim());
                    rightNode.left = rightLeftNode;
                }
                nodeList.get(i).left = leftNode;
                nodeList.get(i).right = rightNode;
                nodeList.add(leftNode);
                nodeList.add(rightNode);
            }

        }
        return rawNodeRoot;
    }

    public static void bushyTreeToString(RawNode root){
        if(root!=null) {
            if(root.left != null) bushyTreeToString(root.left);
            if(root.right != null) bushyTreeToString(root.right);
            if(root.nodeType.toLowerCase().contains("join")) {
                String s1 = root.left.bracketsStr;
                String s2 = root.right.bracketsStr;
                root.setBracketsStr("("+s1+","+s2+")");
            }
            else if(root.nodeType.toLowerCase().contains("scan") || root.nodeType.toLowerCase().contains("get")) {
                root.setBracketsStr(root.tableName);
            }
            else {
                if(root.left != null)
                    root.setBracketsStr(root.left.bracketsStr);
                else
                    root.setBracketsStr(root.right.bracketsStr);
            }
        }
    }

    public List<String> enumerateJoinOrdertoSQL() {
        List<String> enumerateSQLs = new ArrayList<>();

        return enumerateSQLs;
    }

    private void dfs(boolean[] book) {
        if (singleOrder.size() == this.n) {
            allOrder.add(tablesToString(singleOrder));
            return;
        }
        for (int i = 0; i < relatedTables.size(); i++) {
            if (!book[i]) {
                singleOrder.add(relatedTables.get(i));
                book[i] = true;
                dfs(book);
                singleOrder.remove(singleOrder.size() - 1);
                book[i] = false;
            }
        }
    }

    private String tablesToString(List<Table> tables){
        StringBuilder orderStr = new StringBuilder();
        for(int i=0;i<tables.size();i++){
            if(i == 0)
                orderStr.append(tables.get(i).getTableName());
            else
                orderStr.append(",").append(tables.get(i).getTableName());
        }
        return orderStr.toString();
    }
}
