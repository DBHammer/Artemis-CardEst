package ecnu.dbhammer.query;

import ecnu.dbhammer.schema.Table;

public class TableNode extends QueryNode{
    private Table table;
    public TableNode(Table table){
        super();
        this.table = table;
    }
    public Table getTable() {
        return table;
    }
    public String getName(){
        return this.table.getTableName();
    }
    public boolean equals(Object o) {
        if (o instanceof TableNode) {
            TableNode d = (TableNode)o;
            return d.table.getTableName().equals(this.table.getTableName());
        }
        return false;
    }
}
