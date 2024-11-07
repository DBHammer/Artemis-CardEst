package ecnu.dbhammer.schema;

import ecnu.dbhammer.query.PredicateExpression;
import ecnu.dbhammer.query.type.ExpressionComplexType;
import ecnu.dbhammer.schema.genefunc.AttrGeneFunc;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// 数据表生成时，默认有一个自增的整型主键，目前主键仅支持单整型属性
public class Table implements Cloneable, Serializable {
    private int tableIndex;
    private String tableName;
    private int tableSize;
    private PrimaryKey primaryKey;
    private List<ForeignKey> foreignKeys;
    // column的数量（不包含主外键）必然不为0，因为主外键是记录标识符，没有任何物理含义
    private List<Column> columns;//
    private List<Index> indexes;// 索引

    // TODO 可添加更多的表状态

    public Table(int tableIndex, String tableName, int tableSize, PrimaryKey primaryKey, List<ForeignKey> foreignKeys,
            List<Column> columns,
            List<Index> indexes) {
        super();
        this.tableIndex = tableIndex;
        this.tableName = tableName;
        this.tableSize = tableSize;
        this.primaryKey = primaryKey;
        this.foreignKeys = foreignKeys;
        this.columns = columns;
        this.indexes = indexes;
    }

    public Table() {
        super();
    }

    public Table(String name) {
        super();
        this.tableName = name;
    }

    // 返回一条记录，格式为：主键, 各个外键, 其他属性
    public String geneTuple(int primaryKey) {
        StringBuilder sb = new StringBuilder();
        sb.append(primaryKey).append(",");// 加入主键

        // 外键数据的生成
        for (ForeignKey foreignKey : foreignKeys) {
            sb.append(foreignKey.geneValue(primaryKey)).append(",");
        } // 加入外键 外键是由主键生成的

        // 普通属性的生成
        for (int i = 0; i < columns.size(); i++) {
            // 判断是否是具有数据关联的列，数据关联列以驱动列作为自变量，其他列以主键作为自变量
            if (columns.get(i).getCorrelationFactor() == 0)
                sb.append(columns.get(i).geneValue(primaryKey));
            else
                sb.append(columns.get(i).geneValue4Correlation(primaryKey));
            if (i != columns.size() - 1)
                sb.append(",");
        }

        // TODO 加入用于Fuzzing的属性生成
        return sb.toString();
    }

    // 生成该表的建表语句（不包含主外键约束）以及建索引语句
    public List<String> geneAllCreateSQLs() {
        List<String> sqls = new ArrayList<>();
        StringBuilder sb = new StringBuilder();

        // TODO 转换成相应的数据库建表语句对应的数据类型
        // 所有属性，包含主外键属性
        sb.append("create table " + tableName + "(");

        sb.append(primaryKey.getPrimaryKeyName() + " int, ");
        for (ForeignKey foreignKey : foreignKeys) {
            sb.append(foreignKey.getFkColumnName() + " int, ");
        }
        for (Column column : columns) {
            sb.append(column.getColumnName() + " " + column.getStringDataType4CreateTable() + ", ");
        }

        // 主外键约束
        sb.append(" primary key(" + primaryKey.getPrimaryKeyName() + ")");
        // for (ForeignKey foreignKey : foreignKeys) {
        // sb.append(", foreign key (" + foreignKey.getFkColumnName() + ") references "
        // + foreignKey.getReferencedTableName() + " (" +
        // foreignKey.getReferencedTablePrimaryKeyName() + ")");
        // }
        sb.append(")");
        sb.append(";");
        sqls.add(sb.toString());
        sb.setLength(0);

        // 建索引SQL
        // TODO 建索引先去掉，因为现在用不到
        // for (Index index : indexes) {
        // sb.append("create " + index.getIndexType() + " index " + index.getIndexName()
        // + " on " + index.getTableName() + " (");
        // String[] columnNames = index.getColumnNames();
        // for (int i = 0; i < columnNames.length - 1; i++) {
        // sb.append(columnNames[i] + ", ");
        // }
        // sb.append(columnNames[columnNames.length - 1] + ");");
        // sqls.add(sb.toString());
        // sb.setLength(0);
        // }
        return sqls;
    }

    public List<String> geneForeignKeyConstraintSQL() {
        List<String> sqls = new ArrayList<>();
        for (ForeignKey foreignKey : this.foreignKeys) {
            StringBuilder sb = new StringBuilder();
            sb.append("ALTER TABLE " + this.tableName + " ADD constraint " + this.tableName
                    + foreignKey.getFkColumnName() + " FOREIGN KEY (" + foreignKey.getFkColumnName() + ") REFERENCES "
                    + foreignKey.getReferencedTableName() + "(" + foreignKey.getReferencedTablePrimaryKeyName() + ");");
            sqls.add(sb.toString());
        }
        return sqls;

    }

    public int getTableSize() {
        return tableSize;
    }

    public String getTableName() {
        return tableName;
    }

    public String getPrimaryKeyName() {
        return primaryKey.getPrimaryKeyName();
    }

    public PrimaryKey getPrimaryKey() {
        return this.primaryKey;
    }

    public List<ForeignKey> getForeignKeys() {
        return foreignKeys;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Attribute getColumnThroughColumnName(String columnName) {
        for (Column column : columns) {
            if (column.getColumnName().equals(columnName.substring(columnName.indexOf(".") + 1))) {
                return column;
            }
        }
        for (ForeignKey foreignKey : foreignKeys) {
            if (foreignKey.getFkColumnName().equals(columnName.substring(columnName.indexOf(".") + 1))) {
                return foreignKey;
            }
        }
        if (primaryKey.getPrimaryKeyName().equals(columnName.substring(columnName.indexOf(".") + 1))) {
            return primaryKey;
        }
        return null;
    }

    /**
     * 为了进行合理的参数选择，我们需要计算出谓词表达式的最大和最小值
     *
     * @param expression
     * @param lowBound
     * @param upperBound
     * @return
     */
    public BigDecimal[] calculateMaxMinValue(PredicateExpression expression, int lowBound, int upperBound) {
        // TODO 目前Expression中只有一个属性，要适配多个属性，配饰多个属性还需要生成函数之间的整合，比如一次和二次之间的融合，分段函数和普通函数的融合

        BigDecimal[] maxMinValue = new BigDecimal[4];
        if (expression.getComplexType() == ExpressionComplexType.PURESINGLE) {
            maxMinValue[0] = ((Column) expression.getVariables().get(0)).getUpperBound();
            maxMinValue[1] = ((Column) expression.getVariables().get(0)).getLowerBound();
        } else {
            BigDecimal maxValue = BigDecimal.valueOf(Long.MIN_VALUE);
            BigDecimal minValue = BigDecimal.valueOf(Long.MAX_VALUE);
            for (int i = lowBound; i <= upperBound; i++) {
                BigDecimal value = expression.computePredicateValue(i);
                if (value.compareTo(maxValue) > 0) {
                    maxValue = value;
                }

                if (value.compareTo(minValue) < 0) {
                    minValue = value;
                }
            }
            maxMinValue[0] = maxValue;
            maxMinValue[1] = minValue;
        }
        return maxMinValue;
    }

    /**
     * 给出主键的范围，求filter的参数取值范围，其中keyRange是有序的
     *
     * @param expression
     * @param keyRange
     * @return
     */
    public BigDecimal[] calculateMaxMinValue(PredicateExpression expression, List<Integer> keyRange) {
        BigDecimal[] maxMinValue = new BigDecimal[2];
        // update by ct: 关联列单独处理
        if (expression.getCols().get(0).getCorrelationFactor() == 1
                || expression.getCols().get(0).getCorrelationFactor() == -1) {
            Column drvingColumn = expression.getCols().get(0).getDrivingColumn();
            // 表内关联列：驱动列在该表内
            if (drvingColumn.getTableName().equals(this.tableName)) {
                // 关联列依赖驱动列的值进行范围计算
                // 驱动列是zipfian分布的
                BigDecimal tmpMin = drvingColumn.attrEvaluate(keyRange.get(0));
                BigDecimal tmpMax = drvingColumn.attrEvaluate(keyRange.get(keyRange.size() - 1));
                // 驱动列 和 关联列 之间是 正向线性相关 或 负向线性相关
                maxMinValue[0] = expression.getVariables().get(0).getColumnGeneExpressions().get(0)
                        .getMaxValue(tmpMin.intValue(), tmpMax.intValue());
                maxMinValue[1] = expression.getVariables().get(0).getColumnGeneExpressions().get(0)
                        .getMinValue(tmpMin.intValue(), tmpMax.intValue());
            } else { // 表外关联列：驱动列在另一张表
                     // keyRange是关联列所在表的主键，先计算出外键（关联列 外键 = 驱动列 主键）
                List<Integer> drivingTableKeyRange = new ArrayList<>();
                ForeignKey fk = expression.getCols().get(0).getDrivingFK();
                for (Integer pk : keyRange) {
                    drivingTableKeyRange.add(fk.attrEvaluate(pk).intValue());
                }
                Collections.sort(drivingTableKeyRange);
                BigDecimal tmpMin = drvingColumn.attrEvaluate(drivingTableKeyRange.get(0));
                BigDecimal tmpMax = drvingColumn
                        .attrEvaluate(drivingTableKeyRange.get(drivingTableKeyRange.size() - 1));
                // 驱动列 和 关联列 之间是 正向线性相关 或 负向线性相关
                maxMinValue[0] = expression.getVariables().get(0).getColumnGeneExpressions().get(0)
                        .getMaxValue(tmpMin.intValue(), tmpMax.intValue());
                maxMinValue[1] = expression.getVariables().get(0).getColumnGeneExpressions().get(0)
                        .getMinValue(tmpMin.intValue(), tmpMax.intValue());
            }
        } else if (expression.getCols().get(0).getCorrelationFactor() == 2) {
            // 不能首尾判断了（不是递增或递减的）;所有元素都要计算一遍，再从中找最值（时间开销大）
            // 表内关联列：驱动列在该表内
            List<BigDecimal> finalValues = new ArrayList<>();
            Column drvingColumn = expression.getCols().get(0).getDrivingColumn();
            if (drvingColumn.getTableName().equals(this.tableName)) {
                for (Integer pk : keyRange) {
                    BigDecimal tmp = drvingColumn.attrEvaluate(pk);
                    finalValues.add(expression.getCols().get(0).attrEvaluate2(pk, tmp.intValue()));
                }
            } else { // 表外关联列：驱动列在另一张表
                // keyRange是关联列所在表的主键，先计算出外键（关联列 外键 = 驱动列 主键）
                ForeignKey fk = expression.getCols().get(0).getDrivingFK();
                for (Integer pk : keyRange) {
                    BigDecimal fkValue = fk.attrEvaluate(pk);
                    BigDecimal tmp = drvingColumn.attrEvaluate(fkValue.intValue());
                    finalValues.add(expression.getCols().get(0).attrEvaluate2(pk, tmp.intValue()));
                }
            }
            Collections.sort(finalValues);// 排序找最值
            maxMinValue[0] = finalValues.get(finalValues.size() - 1);
            maxMinValue[1] = finalValues.get(0);
        } else if (expression.getComplexType() == ExpressionComplexType.PURESINGLE) {
            if (expression.getVariables().get(0).judgeProperties()) {
                // 满足严格使用一个一次函数的均匀分布
                maxMinValue[0] = expression.getVariables().get(0).getColumnGeneExpressions().get(0)
                        .getMaxValue(keyRange.get(0), keyRange.get(keyRange.size() - 1));
                maxMinValue[1] = expression.getVariables().get(0).getColumnGeneExpressions().get(0)
                        .getMinValue(keyRange.get(0), keyRange.get(keyRange.size() - 1));
            } else {
                // 动态倾斜度的Zifian分布
                maxMinValue[0] = expression.getVariables().get(0).attrEvaluate(keyRange.get(keyRange.size() - 1));
                maxMinValue[1] = expression.getVariables().get(0).attrEvaluate(keyRange.get(0));
            }
        } else {
            BigDecimal maxValue = BigDecimal.valueOf(Long.MIN_VALUE);
            BigDecimal minValue = BigDecimal.valueOf(Long.MAX_VALUE);
            for (int i : keyRange) {
                BigDecimal value = expression.computePredicateValue(i);
                if (value.compareTo(maxValue) > 0) {
                    maxValue = value;
                }
                if (value.compareTo(minValue) < 0) {
                    minValue = value;
                }
            }
            maxMinValue[0] = maxValue;
            maxMinValue[1] = minValue;
        }
        return maxMinValue;
    }

    public List<Attribute> getAllAttrubute() {
        List<Attribute> attributes = new ArrayList<>();
        if (this.tableName != "subTable") {
            attributes.add(primaryKey);
            attributes.addAll(foreignKeys);
        }
        attributes.addAll(columns);
        return attributes;
    }

    // 根据pk计算表达式值
    public BigDecimal CauculateCertainValueByPK(PredicateExpression expression, int primaryKey) {
        List<Attribute> attributes = expression.getVariables();
        List<BigDecimal> coffs = expression.getCoff();

        BigDecimal left = BigDecimal.ZERO;
        for (int i = 0; i < attributes.size(); i++) {
            left = left.add(coffs.get(i).multiply(attributes.get(i).attrEvaluate(primaryKey)));
        }
        if (coffs.size() == attributes.size() + 1) {
            left = left.add(coffs.get(coffs.size() - 1));
        }
        return left;
    }

    // 根据参数计算pk, 这里的filter只可能是单列谓词
    public Integer CauculatePKByParam(PredicateExpression expression, Double param) {
        List<Attribute> attributes = expression.getVariables();
        List<BigDecimal> coffs = expression.getCoff();

        if (attributes.size() != 1) {
            System.err.println("Err, must be single attribute");
        }


        BigDecimal right = BigDecimal.valueOf(param);
        if (coffs.size() == attributes.size() + 1) {
            right = right.subtract(coffs.get(coffs.size() - 1));
        }
        right = right.divide(coffs.get(0));

        List<AttrGeneFunc> funcs = attributes.get(0).getColumnGeneExpressions();
        if (funcs.size() != 1) {
            System.err.println("Err, must be single attribute");
        }
        return funcs.get(0).getPKByParam(right);
    }

    // 随机计算一个表达式中的值
    public BigDecimal randomCauculateCertainValue(PredicateExpression expression, double param) {
        List<Attribute> attributes = expression.getVariables();
        List<BigDecimal> coffs = expression.getCoff();

        // jw
        int primaryKey = (int) (param * tableSize);
        BigDecimal left = BigDecimal.ZERO;
        for (int i = 0; i < attributes.size(); i++) {
            left = left.add(coffs.get(i).multiply(attributes.get(i).attrEvaluate(primaryKey)));
        }
        if (coffs.size() == attributes.size() + 1) {
            left = left.add(coffs.get(coffs.size() - 1));
        }
        return left;
    }

    public BigDecimal CauculateCertainValueFromRange(PredicateExpression expression, BigDecimal[] maxMinValue,
            double param) {
        List<Attribute> attributes = expression.getVariables();
        List<BigDecimal> coffs = expression.getCoff();
        BigDecimal left;
        while (true) {
            left = BigDecimal.ZERO;
            int primaryKey = (int) (param * tableSize);
            for (int i = 0; i < attributes.size(); i++) {
                left = left.add(coffs.get(i).multiply(attributes.get(i).attrEvaluate(primaryKey)));
            }
            if (coffs.size() == attributes.size() + 1) {
                left = left.add(coffs.get(coffs.size() - 1));
            }

            // 如果生成的值在范围内，则结束
            if (left.compareTo(maxMinValue[0]) < 0 && left.compareTo(maxMinValue[1]) > 0) {
                break;
            }
        }
        return left;
    }

    public int getTableIndex() {
        return this.tableIndex;
    }

    @Override
    public String toString() {
        return "Table [tableName=" + tableName + ", tableSize=" + tableSize + "]";
    }

    public void setTableIndex(int tableIndex) {
        this.tableIndex = tableIndex;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setTableSize(int tableSize) {
        this.tableSize = tableSize;
    }

    public void setPrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
    }

    public void setForeignKeys(List<ForeignKey> foreignKeys) {
        this.foreignKeys = foreignKeys;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }
}
