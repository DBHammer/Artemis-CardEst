package ecnu.dbhammer.schema;



import ecnu.dbhammer.data.DataType;
import org.jgrapht.graph.DirectedPseudograph;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
/**
 * Schema类型，里面包括所有的表以及表之间的依赖关系
 */
public class DBSchema implements Cloneable {

    // 一个数据库实例往往有多个数据表
    private List<Table> tableList;//数据库模式中的表数组
    // 外键依赖关系
    private DirectedPseudograph<Table, ForeignKey> foreignKeyDependency;
    // 表名到表的map
    private Map<String,Table> tableName2Table;
    
    //DBschema构造函数
    public DBSchema(List<Table> tableList, DirectedPseudograph<Table, ForeignKey> foreignKeyDependency) {
        super();
        this.tableList = tableList;
        this.foreignKeyDependency = foreignKeyDependency;
        this.tableName2Table = new HashMap<>();
        for(Table table : tableList){
            tableName2Table.put(table.getTableName(),table);
        }
    }

    public List<Table> getTableList() {
        return tableList;
    }

    public Table getTableByName(String tableName){
        return this.tableName2Table.get(tableName);
    }

    // 数值类型列个数
    public int getDigitColumnNum(){
        int ans=0;
        for(Table table : tableList){
            for(Attribute attribute : table.getAllAttrubute()){
                if(DataType.isDigit(attribute.getDataType())){
                    ans++;
                }
            }
        }
        return ans;
    }

    // 建表和外键约束语句
    public void writeDownSQL(String createDir, String foreignKeyDir) throws IOException {
        List<Table> tables = this.tableList;
        File file = new File(createDir);
        if (!file.exists()) {
            file.createNewFile();
        }
        File foreignKeyFile = new File(foreignKeyDir);
        if(!foreignKeyFile.exists()){
            foreignKeyFile.createNewFile();
        }
        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            fw = new FileWriter(file);
            bw = new BufferedWriter(fw);
            //写下创建表的语句
            List<String> sqls = new ArrayList<>();

            for (int j = 0; j < tables.size(); j++) {
                List<String> createSQLs = tables.get(j).geneAllCreateSQLs();

                sqls.addAll(createSQLs);
            }
            for (String sql : sqls) {
                bw.write(sql + "\r\n");
            }
            bw.flush();
            //再写添加外键的语句
            sqls.clear();
            for(int j=0;j<tables.size();j++){
                List<String> foreignKeyConstraintSQLs = tables.get(j).geneForeignKeyConstraintSQL();
                sqls.addAll(foreignKeyConstraintSQLs);
            }

            bw = new BufferedWriter(new FileWriter(foreignKeyFile));
            for (String sql : sqls) {
                bw.write(sql + "\r\n");
            }

        } catch (IOException e) {

        } finally {
            bw.close();
            fw.close();
        }
    }

    // 得到schema中表规模最大的表
    public Table getMaxSizeTable(){
        Table table = null;
        int maxSize = 0;
        for(Table t : this.tableList){
            if(t.getTableSize() > maxSize){
                maxSize = t.getTableSize();
                table = t;
            }
        }
        return table;
    }

    public static Table getTableThroughName(List<Table> tables,String tableName) {
        for(Table table : tables) {
            if (table.getTableName().equals(tableName)) {
                return table;
            }
        }
        System.out.println("表名错误,没有找到");
        return null;
    }

    public Map<String, Table> getTableName2Table() {
        return tableName2Table;
    }

    // 获取这张表参照的所有其他表
    public List<Table> getReferenceTables(Table table){
        List<Table> tables = new ArrayList<>();
        for(ForeignKey foreignKey : table.getForeignKeys()){
            tables.add(getTableThroughName(this.tableList,foreignKey.getReferencedTableName()));
        }
        return tables;
    }

    public DirectedPseudograph<Table, ForeignKey> getForeignKeyDependency() {
        return foreignKeyDependency;
    }

    @Override
    public String toString() {
        return "DBSchema [tables=\n" + Arrays.toString(tableList.toArray()) + "]\n";
    }
    //以字符串形式输出数组
}
