package ecnu.dbhammer.result;

import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.schema.Table;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName ResultRow.java
 * @Description 以行的形式保存Join结果
 * @createTime 2022年05月08日 23:14:00
 */
public class ResultRow {
    private List<Integer> rowData;


    public ResultRow(List<Integer> rowData){
        this.rowData = rowData;
    }
    public List<Integer> getRowData() {
        return rowData;
    }

    public BigDecimal getData(int tableIndex, Attribute attribute){
        return attribute.attrEvaluate(rowData.get(tableIndex));
    }

    /**
     * 该函数用于没有group by时，直接按照一个true分组，把所有数据都分为一组，方便处理
     * @return
     */
    public BigDecimal getTrue(){
        return new BigDecimal(1);
    }
}
