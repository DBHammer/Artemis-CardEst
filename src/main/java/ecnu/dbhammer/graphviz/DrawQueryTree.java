package ecnu.dbhammer.graphviz;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.query.Filter;
import ecnu.dbhammer.query.QueryTree;
import ecnu.dbhammer.schema.Table;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author xiangzhaokun
 * //QueryTree是通过TableOrder构建的
 * //TODO 后续改成用变换的QueryTree构建
 * //画树形图
 */

public class DrawQueryTree {
    private QueryTree queryTree;
    private int index;

    public DrawQueryTree(QueryTree queryTree, int index){
        this.queryTree = queryTree;
        this.index = index;
    }


    public void draw(){
        RecordLog.recordLog(LogLevelConstant.INFO,"开始画第"+(index+1)+"个Query的树形图");

        
        List<String> tableOrder = new ArrayList<>();
        if(queryTree.getTables().size() == 1){
            tableOrder.add(queryTree.getTables().get(0).getTableName());
        }else {
            tableOrder = queryTree.getTableOrder();
        }
        RecordLog.recordLog(LogLevelConstant.INFO,"Table连接的顺序是："+tableOrder);


        //1.table size
        //2.filter后的size
        //3.join后的size
        //TODO 目前只包括join的size，图上无表size和filter后的size
        RecordLog.recordLog(LogLevelConstant.INFO,"每次Join的中间结果集大小是："+queryTree.middleResultCount);

        int joinCount = tableOrder.size()-1;
        if (joinCount != 0) {
            StringBuilder drawStr = new StringBuilder();
            //digraph binaryTree{root->1234[dir=back];
            // root->table_0_5[dir=back];1234->123[dir=back];1234->table_0_4[dir=back];123->12[dir=back];123->table_0_3[dir=back];12->table_0_1[dir=back];12->table_0_2[dir=back];}
            drawStr.append("digraph binaryTree{");
            while (joinCount >= 2) {
                drawStr.append("\"Join" + joinCount + "_[" + queryTree.middleResultCount.get(joinCount - 1) + "]\"->" + "\"Join" + (joinCount - 1) + "_[" + queryTree.middleResultCount.get(joinCount - 2) + "]\";");
                drawStr.append("\"Join" + joinCount + "_[" + queryTree.middleResultCount.get(joinCount - 1) + "]\"->" + tableOrder.get(joinCount) + ";");
                joinCount--;
            }

            drawStr.append("\"Join" + joinCount + "_[" + queryTree.middleResultCount.get(joinCount - 1) + "]\"->" + tableOrder.get(joinCount) + ";");
            drawStr.append("\"Join" + joinCount + "_[" + queryTree.middleResultCount.get(joinCount - 1) + "]\"->" + tableOrder.get(joinCount - 1) + ";");
            drawStr.append("}");
            System.out.println(drawStr.toString());

            File file = new File("./TreeGraph");
            if (!file.exists()) {
                file.mkdir();
            }
            String graphPngName = "query_" + index + ".png";
            //dot -Tpng -o Downloads/tree.png Desktop/tree.dot


            File dotFile = new File("TreeGraphDot");
            if (!dotFile.exists()) {
                dotFile.mkdir();
            }
            String dotFileName = dotFile.getName() + File.separator + "querytree_draw_" + index + ".dot";
            //System.out.println(dotFileName);


            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dotFileName), Configurations.getEncodeType()))) {
                bw.write(drawStr.toString());
                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }

            String drawQueryTreeCommand = "dot -Tpng -o TreeGraph/" + graphPngName + " " + dotFileName;
            //System.out.println(drawQueryTreeCommand);


            try {
                Runtime.getRuntime().exec(drawQueryTreeCommand);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else{
            RecordLog.recordLog(LogLevelConstant.INFO,"只有1张表暂时不画图！");
        }
    }

}
