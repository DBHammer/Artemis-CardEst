package ecnu.dbhammer.main;

import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.schema.DBSchema;

/**
 * @author xiangzhaokun
 * @ClassName DatabaseInstanceGene.java
 * @Description 数据库实例的生成，包括模式生成和确定性数据生成
 * @createTime 2022年02月20日 16:01:00
 */
public class DatabaseInstanceGene {
    public static void dbInstanceGene(){
        long startTime = System.currentTimeMillis();
        RecordLog.recordLog(LogLevelConstant.INFO, "------开始生成Schema------");
        DBSchema dbSchema = SchemaGene.schemaGene();
        RecordLog.recordLog(LogLevelConstant.INFO, "------开始生成数据------");
        DataGene.dataGene(dbSchema);
        long endTime = System.currentTimeMillis();
        System.out.println("数据库实例生成时间"+(endTime-startTime));
    }

    public static void main(String[] args) {
        dbInstanceGene();
    }
}
