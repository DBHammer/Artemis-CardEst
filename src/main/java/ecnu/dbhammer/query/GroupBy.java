package ecnu.dbhammer.query;



import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.schema.Column;
import ecnu.dbhammer.schema.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * Group By生成
 */
public class GroupBy {

    Attribute attribute;

    private List<Table> tables;

    public GroupBy(List<Table> tables) {
        this.tables = tables;
        this.attribute = getColumnName4Group(tables);
    }

    // 随机选取具有倾斜分布的列作为分组属性
    private Attribute getColumnName4Group(List<Table> tables) {
        List<Column> allColumns = new ArrayList<>();
        List<Column> columns = new ArrayList<>();
        for (int i = 0; i < tables.size(); i++) {
            allColumns.addAll(tables.get(i).getColumns());
        }

        for (Column column : allColumns) {
            if (column.getSkewness() != 0) {
                columns.add(column);
            }
        }


        int index = (int)(Math.random() * columns.size());
        return columns.get(index);//随机选择table list中的一个列
    }

    public Attribute getColumnName() {
        return this.attribute;
    }

    public List<Table> getTables() {
        return tables;
    }
}
