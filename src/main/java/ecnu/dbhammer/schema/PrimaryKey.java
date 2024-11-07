package ecnu.dbhammer.schema;



import ecnu.dbhammer.data.AttrValue;
import ecnu.dbhammer.data.DataType;
import ecnu.dbhammer.schema.genefunc.AttrGeneFunc;
import ecnu.dbhammer.schema.genefunc.LinearFunc;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
/**
 * 主键类，里面含有基于主键的数据映射
 */
public class PrimaryKey extends Attribute implements Cloneable, Serializable {

    private List<AttrGeneFunc> attrGeneFuncs;


    public PrimaryKey(String tableName, int tableSize, String primaryKeyName) {
        super(tableName,tableSize,primaryKeyName,DataType.INTEGER);
        initColumnGeneExpression();
    }

    /**
     * pk生成函数选择y=x即可
     */
    public void initColumnGeneExpression() {
        int columnGeneExpressionNum = 1;
        attrGeneFuncs = new ArrayList<>();
        for (int i = 0; i < columnGeneExpressionNum; i++) {
            attrGeneFuncs.add(new LinearFunc(new BigDecimal("1"), new BigDecimal("0")));
        }

    }

    @Override
    public BigDecimal attrEvaluate(int pk) {
        return new BigDecimal(pk);
    }

    @Override
    public BigDecimal attrEvaluate2(int pk, int drivingCol) {
        return null;
    }

    @Override
    public AttrValue geneAttrValue(int pk) {
        return new AttrValue(DataType.INTEGER,pk);
    }

    public int getColumnGeneExpressionNum() {
        return this.attrGeneFuncs.size();
    }

    public List<AttrGeneFunc> getColumnGeneExpressions() {
        return attrGeneFuncs;
    }

    public String getPrimaryKeyName() {
        return super.getAttName();
    }

    public DataType getDataType() {
        return super.getDataType();
    }

    @Override
    public boolean judgeProperties() {
        return false;
    }


    @Override  //深拷贝
    public PrimaryKey clone() {
        return new PrimaryKey(super.getTableName(), super.getTableSize(), super.getAttName());
    }

    @Override
    public String toString() {
        return "PrimaryKey{" +
                "tableName=" + super.getTableName() +
                ", tableSize=" + super.getTableSize() +
                ", primaryKeyName=" + super.getAttName() +
                '}';
    }

}
