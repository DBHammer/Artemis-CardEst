package ecnu.dbhammer.result;


import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName TreeNode.java
 * @Description TODO
 * @createTime 2021年12月16日 23:15:00
 */
public class TreeNode {
    public int value;
    public List<TreeNode> childrens;

    public TreeNode parent;
    public TreeNode(){

    }

    public TreeNode(int value){
        this.value = value;
        this.childrens = new ArrayList<>();
        this.parent = null;
    }

    public TreeNode(int value, List<TreeNode> childrens){
        this.value = value;
        this.childrens = childrens;
        this.parent = null;
    }

    public static List<Integer> transferToIntList(List<TreeNode> treeNodeList){
        List<Integer> ans = new ArrayList<>();
        for(TreeNode node : treeNodeList){
            ans.add(node.value);
        }
        return ans;
    }
}
