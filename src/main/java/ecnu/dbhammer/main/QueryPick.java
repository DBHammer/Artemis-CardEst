package ecnu.dbhammer.main;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.query.QueryTree;

import javax.management.Query;
import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author xiangzhaokun
 * @ClassName QueryPick.java
 * @Description 选出高质量的Query，这里可以根据自己的要求
 * @createTime 2022年04月17日 16:46:00
 */
public class QueryPick {

    public static List<String> filterSet = new ArrayList<>();
    /**
     * 选出符合最终基数条件的Query
     * @param queryTrees
     * @param target
     */
    public static void pickQuerySatisfy(List<QueryTree> queryTrees, int target){
        String targetQueryPath = Configurations.getQueryOutputDir() + File.separator + "targetQuery.txt";
        String targetQueryCard = Configurations.getQueryOutputDir() + File.separator + "targetCard.txt";


//        Set<Integer> set = new HashSet<>();
//        int num = 0;
        for(QueryTree queryTree : queryTrees){
            if(queryTree.getFinalResultSize()>=target ){
//                set.add(queryTree.getFinalResultSize());
//                if(num>=10){
//                    continue;
//                }
//                String singletargetQueryPath = targetQueryPath + File.separator + "targetQuery.txt";
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(targetQueryPath, true), Configurations.getEncodeType()))) {
                    bw.write(queryTree.geneSQLJoinConInWhere() + "\r\n");
                    bw.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(targetQueryCard, true), Configurations.getEncodeType()))) {
                    bw.write(queryTree.getFinalResultSize() + "\r\n");
                    bw.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
//                num++;

            }
        }
    }

}
