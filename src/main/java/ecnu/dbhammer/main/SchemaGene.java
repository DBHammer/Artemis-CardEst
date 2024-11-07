package ecnu.dbhammer.main;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.schema.DBSchema;
import ecnu.dbhammer.schema.SchemaGenerator;
import ecnu.dbhammer.schema.Table;
import ecnu.dbhammer.test.GeneExpressionOutFile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author xiangzhaokun
 * @ClassName SchemaGene.java
 * @Description 模式生成
 * @createTime 2021年11月27日 15:06:00
 */
public class SchemaGene {
    public static DBSchema schemaGene(){
        Map<String, Float> dataType2OccurProbability = Configurations.getDataType2OccurProbability();
        SchemaGenerator schemaGenerator = new SchemaGenerator(dataType2OccurProbability);
        DBSchema dbSchema = null;
        try {

            long startTime = System.currentTimeMillis();
            dbSchema = schemaGenerator.schemaGenerate();
            long endTime = System.currentTimeMillis();
            System.out.println("schema生成的时间"+(endTime-startTime)+"ms");


            String schemaSQLDir = Configurations.getSchemaOutputDir() + File.separator + "createSchemaSQL"  + ".txt";
            String addForeginKeyDir = Configurations.getSchemaOutputDir() + File.separator + "addForeignKey.txt";
            dbSchema.writeDownSQL(schemaSQLDir,addForeginKeyDir);
            // 生成建表语句，并写入相应的目录下

            new GeneExpressionOutFile().dumpGeneExpression(dbSchema);//将生成函数保存下来

            //实验
            int dataScale=0;
            for(Table table : dbSchema.getTableList()){
                dataScale+=table.getTableSize();
            }
            System.out.println("此次数据规模量："+dataScale);


        } catch (Exception e) {
            e.printStackTrace();
        }
        return dbSchema;
    }

    
    //测试schema生成
    public static void main(String[] args) {
        DBSchema dbSchema = SchemaGene.schemaGene();
    }
}
