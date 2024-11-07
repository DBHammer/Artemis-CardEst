package ecnu.dbhammer.main;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.query.QueryGenerator;
import ecnu.dbhammer.query.QueryTree;
import ecnu.dbhammer.schema.DBSchema;
import ecnu.dbhammer.utils.ClearDirectory;

import java.io.File;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName QueryGene.java
 * @Description 负载生成，并统计负载生成的效率等
 * @createTime 2021年11月27日 15:24:00
 */
public class QueryGene {
    public static List<QueryTree> queryGene(DBSchema dbSchema) throws Exception {

        int queryNum = Configurations.getQueryNumPerSchema();
        QueryGenerator queryGenerator = new QueryGenerator(dbSchema,queryNum);
        // 生成query的同时写入指定目录的文件下
        //ClearDirectory.deleteDir(".//query");
        // RecordLog.recordLog(LogLevelConstant.INFO,"删除之前生成的Query");
        String queryPath = Configurations.getQueryOutputDir() + File.separator + "query" + ".txt";
        int numOfTable = Configurations.getTableNumPerQuery();
        //配置文件中期望的Table数
        List<QueryTree> queryTrees = queryGenerator.generate(queryPath,numOfTable);

        RecordLog.recordLog(LogLevelConstant.INFO,"每个Query生成的平均时间为"+queryGenerator.getQueryGeneAvgTime()+"ms");
        RecordLog.recordLog(LogLevelConstant.INFO, "每个Query连接模板生成的平均时间为" + queryGenerator.getJoinPatternGeneAvgTime() + "ms");
        RecordLog.recordLog(LogLevelConstant.INFO, "每个Query过滤谓词生成的平均时间为" + queryGenerator.getFilterGeneAvgTime() + "ms");
        RecordLog.recordLog(LogLevelConstant.INFO,"生成标准数量Join算子的Query的平均时间"+queryGenerator.getStandard()+"ms");

        return queryTrees;
    }

    public static void main(String[] args) throws Exception {
        DBSchema dbSchema = SchemaGene.schemaGene();
        List<QueryTree> queryTrees = QueryGene.queryGene(dbSchema);
        for(QueryTree queryTree : queryTrees){
            System.out.println(queryTree.getSqlText());
        }
    }
}
