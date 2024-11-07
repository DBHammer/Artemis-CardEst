package ecnu.dbhammer.main;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.graphviz.DrawQueryTree;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.query.QueryTree;
import ecnu.dbhammer.result.ResultGenerator;
import ecnu.dbhammer.utils.ClearDirectory;
import ecnu.dbhammer.utils.ClearFile;

import java.io.File;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName StartCaluate.java
 * @Description 开始使用求解器生成Query的理想结果
 * @createTime 2021年11月27日 15:44:00
 */
public class StartCaluate {
    public static void startCal(List<QueryTree> queryTrees){

        //结果计算前，把对应的结果计算结果文件清空

        ClearDirectory.deleteDir(Configurations.getCalculateResultOutputDir());
        ClearDirectory.deleteDir(Configurations.getGenerateExactCard());

        long allComputionTime = 0;
        int idealTableNum = 0;
        long idealComputationTime = 0;

        long filterGeneTime = 0;
        for (int i = 0; i < queryTrees.size(); i++) {

            String calculateResultOutputDir = Configurations.getCalculateResultOutputDir() + File.separator + "calculateResult_"+ (i+1) + ".csv";
            QueryTree queryTree = queryTrees.get(i);


            RecordLog.recordLog(LogLevelConstant.INFO,"第" + (i+1) + "个结果开始计算");

            ResultGenerator resultGenerator = new ResultGenerator(queryTree);

            try {
                resultGenerator.geneResult(calculateResultOutputDir);//计算所有的结果
            }catch (Exception e){
                e.printStackTrace();
            }
            allComputionTime += resultGenerator.getComputationTime();
            filterGeneTime += resultGenerator.getFilterGeneTime();
            if(queryTree.getTables().size() == Configurations.getTableNumPerQuery()){
                idealTableNum++;
                idealComputationTime += resultGenerator.getComputationTime();
            }
            RecordLog.recordLog(LogLevelConstant.INFO,"第" + (i+1) + "个结果计算结束,消耗时间：" + resultGenerator.getComputationTime() + "ms");
        }
        RecordLog.recordLog(LogLevelConstant.INFO,"平均每个Query计算花费时间："+allComputionTime*1.0/queryTrees.size()/1000+"s");
        RecordLog.recordLog(LogLevelConstant.INFO,"一共生成了标准Join个数的Query数："+idealTableNum+"个");
        RecordLog.recordLog(LogLevelConstant.INFO,"平均每个Query(标准个数的表)计算花费时间："+idealComputationTime*1.0/idealTableNum+"ms");
        RecordLog.recordLog(LogLevelConstant.INFO,"SingleFilterGeneTime："+filterGeneTime*1.0/queryTrees.size()+"ms");
        //画图
        // ClearDirectory.deleteDir("./TreeGraph");
        // ClearDirectory.deleteDir("./TreeGraphDot");
        RecordLog.recordLog(LogLevelConstant.INFO,"删除已画树形图成功！");
        if (Configurations.isdrawQuery()){
            for (int i = 0; i < queryTrees.size(); i++) {
                QueryTree queryTree = queryTrees.get(i);
                queryTree.middleResultCount.add(queryTree.getFinalResultSize());
                DrawQueryTree drawTree = new DrawQueryTree(queryTree, i);
                drawTree.draw();//画图
            }
        }
    }


    public static void startCal(QueryTree queryTree){
        ResultGenerator resultGenerator = new ResultGenerator(queryTree);
        try {
            resultGenerator.geneResult();//计算所有的结果
        }catch (Exception e){
                e.printStackTrace();
        }
    }
}
