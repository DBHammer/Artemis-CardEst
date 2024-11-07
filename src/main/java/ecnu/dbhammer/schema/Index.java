package ecnu.dbhammer.schema;



import java.io.Serializable;
import java.util.Arrays;

public class Index implements Cloneable, Serializable {

    private String indexType;
    private String indexName;
    private String tableName;
    private String[] columnNames;

    public Index(String indexType, String indexName, String tableName, String[] columnNames) {
        super();
        this.indexType = indexType;
        this.indexName = indexName;
        this.tableName = tableName;
        this.columnNames = columnNames;
    }

    public String getIndexType() {
        return indexType;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getTableName() {
        return tableName;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    // 深拷贝
    @Override
    public Index clone() {
        String[] columnNamesCopy = Arrays.copyOf(columnNames, columnNames.length);
        return new Index(indexType, indexName, tableName, columnNamesCopy);
    }

    @Override
    public String toString() {
        return "\n\tIndex [indexType=" + indexType + ", indexName=" + indexName + ", tableName=" + tableName
                + ", columnNames=" + Arrays.toString(columnNames) + "]";
    }

}
