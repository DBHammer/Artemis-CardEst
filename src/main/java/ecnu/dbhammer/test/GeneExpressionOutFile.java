package ecnu.dbhammer.test;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.data.DataType;
import ecnu.dbhammer.schema.*;
import ecnu.dbhammer.schema.genefunc.AttrGeneFunc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class GeneExpressionOutFile {

    public void dumpGeneExpression(DBSchema dbSchemas) {
        try {
            File outfile = new File(Configurations.getSchemaOutputDir() + File.separator + "expressions.txt");
            if (!outfile.exists()) {
                outfile.createNewFile();
            }
            BufferedWriter out = new BufferedWriter(new FileWriter(outfile));
            //输出外键生成函数和普通列生成函数
            List<Table> tables = dbSchemas.getTableList();
            for (Table t : tables) {
                // 表信息
                out.write("table: "+t.getTableName()+" || "+t.getTableSize()+" || "+t.getPrimaryKeyName()+"\r\n");

                // 外键信息
                List<ForeignKey> fks = t.getForeignKeys();
                for (ForeignKey fk : fks) {
                    out.write("fk: "+t.getTableName()+"."+fk.getFkColumnName()+"->"
                            +fk.getReferencedTableName()+"->"+fk.getReferencedTablePrimaryKeyName()+"->"+fk.getReferencedTableSize()
                            +" || "+fk.getDataType());
                    for (AttrGeneFunc fkGeneExpression : fk.getColumnGeneExpressions()) {
                        out.write(" || "+fkGeneExpression.getExpression());
                    }
                    out.write("\r\n");
                }

                // 普通列信息
                List<Column> columns = t.getColumns();
                for(Column c : columns){
                    out.write("col: "+t.getTableName()+"."+c.getColumnName()+" || "+c.getDataType());
                    out.write("||corr"+c.getCorrelationFactor());
                    if(c.getCorrelationFactor() == 0)
                        out.write("||null");
                    else
                        out.write("||"+c.getDrivingColumn().getTableName()+"."+c.getDrivingColumn().getColumnName());
                    out.write("||zip"+c.getSkewness());
                    if(c.getSkewness() != 0)
                        out.write("||"+c.getIndexList());
                    else
                        out.write("||null");
                    if(c.getCorrelationFactor() == 2)
                        out.write("||"+c.getCorrColumn_PK_IndexList());
                    else
                        out.write("||null");
                    out.write("||"+c.getColumnGeneExpressions().size());
                    for(AttrGeneFunc attrGeneFunc : c.getColumnGeneExpressions()){
                        out.write(" || "+ attrGeneFunc.getExpression());
                    }
                    if(c.getDataType() == DataType.VARCHAR) {
                        out.write("||" + c.getVarcharLength());
                        out.write("||" + Arrays.toString(c.getSeedStrings()));
                    }

                    out.write("\r\n");
                }
            }
            out.close();
        }catch(IOException e) {
            e.printStackTrace();
        }
    }
}
