package ecnu.dbhammer.main;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.joinorder.JoinOrderEvaluation;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.query.QueryTree;
import ecnu.dbhammer.schema.DBSchema;
import ecnu.dbhammer.solver.ThreadPools;
import ecnu.dbhammer.test.ReadSchemaInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName Main.java
 * @Description 整个程序入口
 * @createTime 2021年11月27日 15:47:00
 */
public class Main {
    public static final String LOGO = "     ___   _____    _____   _____       ___  ___   _   _____  \n" +
            "    /   | |  _  \\  |_   _| | ____|     /   |/   | | | /  ___/ \n" +
            "   / /| | | |_| |    | |   | |__      / /|   /| | | | | |___  \n" +
            "  / / | | |  _  /    | |   |  __|    / / |__/ | | | | \\___  \\ \n" +
            " / /  | | | | \\ \\    | |   | |___   / /       | | | |  ___| | \n" +
            "/_/   |_| |_|  \\_\\   |_|   |_____| /_/        |_| |_| /_____/ ";

    public static void main(String[] args) throws Exception {
        System.out.println(LOGO);

        //读配置文件
        if(args.length > 0)
            Configurations.loadConfigurations(args[0]);
        else
            Configurations.loadConfigurations(null);
        String type = "query";
        if(args.length > 1)
            type = args[1];

        RecordLog.recordLog(LogLevelConstant.INFO, "------Artemis开始运行------");
        RecordLog.recordLog(LogLevelConstant.INFO, "开始生成Schema、数据和负载Query");

        DBSchema dbSchema;

        // if (Configurations.isGetAllCard()) {
        //         // ob tidb 对比实验，拿到查询计划的每一步中间基数
        //         RecordLog.recordLog(LogLevelConstant.INFO, "------开始获取数据库查询计划的所有基数------");
        //         loadDataAndTest.executeQueryForAnaylzeCard("query");

        // }
        //生成新的模式或者读取已有的模式
        if(Configurations.isGeneSchema()) {
            RecordLog.recordLog(LogLevelConstant.INFO, "------开始生成Schema------");
            dbSchema = SchemaGene.schemaGene();
        }else{
            RecordLog.recordLog(LogLevelConstant.INFO, "------开始读取Schema------");
            dbSchema = ReadSchemaInfo.readSchema();
        }

        //生成新的数据或不生成数据
        if(Configurations.isGeneData()) {
            RecordLog.recordLog(LogLevelConstant.INFO, "------开始生成数据------");
            // 由于varchar类型的具体信息会在第一次生成模式时候记录，而如果根据expression.txt读取模式时会丢失column的具体生成信息，故当前只能模式和数据同时生成才会不造成数据丢失
            DataGene.dataGene(dbSchema);
        }

        //是否将数据导入数据库
        if(Configurations.isLoadData()) {
            RecordLog.recordLog(LogLevelConstant.INFO, "------开始导入数据到数据库中------");
            DataGene.loadIntoDB(dbSchema);
        }

        //是否生成新的查询
        if(Configurations.isGeneQuery()) {
            RecordLog.recordLog(LogLevelConstant.INFO, "------开始生成Query------");
            // 由于varchar类型的具体信息会在第一次生成模式时候记录，而如果根据expression.txt读取模式时会丢失column的具体生成信息，故当前只能模式和数据同时生成才会不造成数据丢失
            List<QueryTree> queryTrees = QueryGene.queryGene(dbSchema);

            //是否进行结果自计算：挑选最终结果符合要求的查询，如cardLowerBound设置为5则表示挑选最终结果>=5的查询，存入targetQuery.txt中
            if (Configurations.isCalcuate()) {
                RecordLog.recordLog(LogLevelConstant.INFO, "------开始用求解器计算Query的理想结果------");
                StartCaluate.startCal(queryTrees);
                QueryPick.pickQuerySatisfy(queryTrees, Configurations.getCardLowerBound());
            }

            //测试画图
            // StartCaluate.startCal(queryTrees);

            //是否只生成查询：只生成需要的查询而不进行正确性验证、执行时间对比和连接连接顺序评估
            if (!Configurations.isGenerateOnly()) {
                //是否进行正确性验证
                if (Configurations.isCorrectnessVarity()) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "------开始结果验证并统计------");
                    StartVertify.startVertify(queryTrees);
                }
                //是否进行执行时间对比
                if (Configurations.isExecuteTimeCompare()) {
                    StartCompare.StartCompare(queryTrees);
                    //StartCompare.StartCompare4OB();
                }

                //是否进行JoinReorder评估：目前只针对星型查询，根据中间结果进行连接顺序重排
                if (Configurations.iscardOptimalJoinOrderEval()) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "------开启了JoinReorder评估------");
                    StartJoinOrderEvaluation.startEvaluation(queryTrees);
                }

                //是否进行连接顺序评估
                if (Configurations.isSortJoinOrderEval()) {
                    RecordLog.recordLog(LogLevelConstant.INFO, "------开启了JoinOrder排名评估------");
                    StartJoinOrderEvaluation.startEvaluation(queryTrees);
                }

            } else {
                RecordLog.recordLog(LogLevelConstant.INFO, "------设置了只需要生成，不需要验证------");
            }

            RecordLog.recordLog(LogLevelConstant.INFO, "预期全部生成" + Configurations.getTableNumPerQuery() + "个表Join的Query");
            int[] actualNumOfQuery = new int[Configurations.getTableNumPerQuery() + 1];
            for (QueryTree queryTree : queryTrees) {
                int tableNum = queryTree.getTables().size();
                actualNumOfQuery[tableNum]++;
            }
            for (int i = 1; i <= Configurations.getTableNumPerQuery(); i++) {
                RecordLog.recordLog(LogLevelConstant.INFO, i + "个表Join的Query有" + actualNumOfQuery[i] + "个");
            }
            ThreadPools.down();
        }else{
            //若不生成新的查询，是否使用已有的查询进行join order评估import time

            if (Configurations.isSortJoinOrderEval()) {
                RecordLog.recordLog(LogLevelConstant.INFO, "------开始JoinOrder排名评估------");
                loadDataAndTest.execQuerys(type);
            } else if (Configurations.isGetAllCard()) {
                // ob tidb 对比实验，拿到查询计划的每一步中间基数
                RecordLog.recordLog(LogLevelConstant.INFO, "------开始获取数据库查询计划的所有基数------");
                loadDataAndTest.executeQueryForAnaylzeCard("query");

            }

        }

        RecordLog.recordLog(LogLevelConstant.INFO, "------ByeBye------");
    }
}
