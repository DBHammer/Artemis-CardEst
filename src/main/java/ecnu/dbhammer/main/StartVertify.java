package ecnu.dbhammer.main;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.log.TestReport;
import ecnu.dbhammer.query.QueryTree;
import ecnu.dbhammer.verification.QueryResultExecution;
import ecnu.dbhammer.verification.Verify;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.util.List;
import java.util.Set;

/**
 * @author xiangzhaokun
 * @ClassName StartVertify.java
 * @Description 查询结果对比模块
 * @createTime 2021年11月27日 16:27:00
 */
public class StartVertify {
    public static void startVertify(List<QueryTree> queryTrees){
        String readQueryPath = Configurations.getQueryOutputDir() + File.separator + "query" + ".txt";
        String queryCalculateResultPath = Configurations.getCalculateResultOutputDir();
        String queryExecuteResultPath = Configurations.getExecuteResultOutputDir();

        List<Pair<Integer,String>> queryListOfNull = QueryResultExecution.execute(false,queryTrees,"executeTime");

        Set<Integer> errorList = null;
        try {
            errorList = Verify.verifyResult(queryCalculateResultPath, queryExecuteResultPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //如果为true，有没有错误都要保存
        if(Configurations.isSavaArchive()) {
            if (errorList.size() != 0) {
                RecordLog.recordLog(LogLevelConstant.INFO,"此次测试有" + errorList.size() + "个Error, 保存测试场景");
            }
            //TestReport.recordReport(readQueryPath, queryCalculateResultPath, queryExecuteResultPath, errorList);
        }else{//否则只有在有错误的情况下保存
            if (errorList.size() != 0) {
                RecordLog.recordLog(LogLevelConstant.INFO,"此次测试有" + errorList.size() + "个Error, 保存测试场景");
                //TestReport.recordReport(readQueryPath, queryCalculateResultPath, queryExecuteResultPath, errorList);
            }else{
                RecordLog.recordLog(LogLevelConstant.INFO,"由于设置saveArchive为false，无错误不需要保存测试场景");
            }
        }

        RecordLog.recordLog(LogLevelConstant.INFO,"此次测试有"+queryListOfNull.size()+"个NULL值");
        for (Pair<Integer,String> query : queryListOfNull){
            RecordLog.recordLog(LogLevelConstant.INFO,"第"+query.getLeft()+"个Query:");
            RecordLog.recordLog(LogLevelConstant.INFO,query.getRight());
        }
        RecordLog.recordLog(LogLevelConstant.INFO,"预期全部生成"+Configurations.getTableNumPerQuery()+"个表Join的Query");
        int[] actualNumOfQuery = new int[Configurations.getTableNumPerQuery()+1];
        for (QueryTree queryTree : queryTrees){
            int tableNum = queryTree.getTables().size();
            actualNumOfQuery[tableNum]++;
        }
        for (int i=1;i<=Configurations.getTableNumPerQuery();i++){
            RecordLog.recordLog(LogLevelConstant.INFO,i+"个表Join的Query有"+actualNumOfQuery[i]+"个");
        }

        //RecordLog.recordLog(LogLevelConstant.INFO,"结束运行整个工具！");
    }
}
