package ecnu.dbhammer.schema;


import ecnu.dbhammer.data.AttrValue;
import ecnu.dbhammer.data.DataType;
import ecnu.dbhammer.schema.genefunc.AttrGeneFunc;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

/**
 * 属性类，抽象类，被Column,PrimaryKey,ForeignKey类实现
 */
public abstract class Attribute implements Serializable {

    private String tableName;//表的名称
    private int tableSize;//表的大小
    private String attName;//属性名称
    private DataType dataType;//属性的数据类型

    // 1: int; 2: long; 3: float; 4: double; 5: decimal; 6: varchar; 7: datetime; 8: bool

    public Attribute(){
    }

    public Attribute(String tableName, int tableSize, String attName, DataType dataType){
        this.tableName = tableName;
        this.tableSize = tableSize;
        this.attName = attName;
        this.dataType = dataType;
    }

    public abstract BigDecimal attrEvaluate(int pk);

    public abstract BigDecimal attrEvaluate2(int pk, int drivingCol);
    //生成真实的表中的值，比如生成Varchar的时候，先生成一个bigdecimal，再在Varchar Pool中选择一个Varchar
    public abstract AttrValue geneAttrValue(int pk);

    public String getTableName() {
        return tableName;
    }

    public int getTableSize() {
        return tableSize;
    }

    public abstract int getColumnGeneExpressionNum();

    public abstract List<AttrGeneFunc> getColumnGeneExpressions();

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setTableSize(int tableSize) {
        this.tableSize = tableSize;
    }

    public void setAttName(String attName) {
        this.attName = attName;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public DataType getDataType() {
        return dataType;
    }
    public String getAttName() {
        return attName;
    }

    public String getFullAttrName(){
        return this.tableName + "." + this.attName;
    }


    public abstract boolean judgeProperties();
}
