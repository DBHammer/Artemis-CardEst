package ecnu.dbhammer.main;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.databaseAdapter.DBConnection;
import ecnu.dbhammer.databaseAdapter.DatabaseConnection;
import ecnu.dbhammer.joinorder.JoinOrderEnum;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.query.Join;
import ecnu.dbhammer.query.QueryTree;
import ecnu.dbhammer.verification.QueryResultExecution;

import java.sql.Connection;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName StartJoinOrderEvaluation.java
 * @Description 开始进行连接顺序枚举的评估
 * @createTime 2022年01月26日 16:31:00
 */
public class StartJoinOrderEvaluation {
    public static void startEvaluation(List<QueryTree> queryTrees){
        if(Configurations.iscardOptimalJoinOrderEval())
            QueryResultExecution.executeForJoinOrder(queryTrees);
        else
            QueryResultExecution.executeForJoinRank(queryTrees);

        RecordLog.recordLog(LogLevelConstant.INFO, "JoinOrder Evaluation结束");
    }
}
