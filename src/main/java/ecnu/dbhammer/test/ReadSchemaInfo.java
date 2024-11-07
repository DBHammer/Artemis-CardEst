package ecnu.dbhammer.test;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.data.DataType;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.schema.*;
import ecnu.dbhammer.schema.genefunc.AttrGeneFunc;
import ecnu.dbhammer.schema.genefunc.LinearFunc;
import ecnu.dbhammer.schema.genefunc.ModFunc;
import ecnu.dbhammer.schema.genefunc.QuadraticFunc;
import org.jgrapht.graph.DirectedPseudograph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

public class ReadSchemaInfo {
    private static Map<String, Table> tableMap;// 保存各表的映射
    // private Map<String, List<String>> Column2Expressions;//保存各表各列的生成函数
    private static List<Table> tables;

    public static DBSchema readSchema() {
        // 读入文件（后续可能加入配置文件）
        File infile = new File(Configurations.getSchemaFile());

        Map<String, Map<String, Float>> dataDistributionProbability = Configurations.getDataDistributionProbability();

        BufferedReader reader = null;
        String tempString = null;
        tableMap = new LinkedHashMap<>();
        // Column2Expressions = new HashMap<>();
        tables = new ArrayList<>();
        Table table;
        Column column;
        Map<String, List<Column>> table2column = new HashMap<>();
        Map<String, List<ForeignKey>> table2fk = new HashMap<>();
        ForeignKey foreignKey;
        PrimaryKey primaryKey;
        AttrGeneFunc expression;
        AttrGeneFunc[] attrGeneFuncs;

        try {
            reader = new BufferedReader(new FileReader(infile));
            // table: table_0 || 50 || primaryKey
            // fk: table_0.fk_0->table_1->primaryKey->50 || INTEGER || k mod 50
            // col: table_0.col_0 || INTEGER||1 || 37029236 * k + 3937
            int tableID = 0;
            while ((tempString = reader.readLine()) != null) {
                // tempString = tempString.toLowerCase();
                if (tempString.startsWith("table:")) {
                    table = new Table();
                    tempString = tempString.substring(7);
                    String temp[] = tempString.split("\\|\\|");
                    String tableName = temp[0].trim();
                    int num = Integer.parseInt(temp[1].trim());
                    String pkName = temp[2].trim();

                    table.setTableIndex(tableID);
                    tableID++;
                    table.setTableName(tableName);
                    table.setTableSize(num);
                    primaryKey = new PrimaryKey(tableName, num, pkName);
                    table.setPrimaryKey(primaryKey);

                    tableMap.put(tableName, table);

                } else if (tempString.startsWith("col:")) {
                    column = new Column();
                    tempString = tempString.substring(5);
                    String temp[] = tempString.split("\\|\\|");
                    String tableName = temp[0].split("\\.")[0].trim();
                    String columnName = temp[0].split("\\.")[1].trim();

                    DataType dataType = DataType.valueOf(temp[1].trim());

                    double skewness = Double.parseDouble(temp[4].substring(3));

                    if (dataType == DataType.VARCHAR && skewness == 0) {
                        if (skewness == 0) {
                            column.setVarcharLength(Integer.parseInt(temp[9]));
                            String tmp = temp[10].substring(1, temp[10].length() - 1).replaceAll(" ", "");
                            String[] randomStrings = tmp.split(",");
                            column.setSeedStringNum(randomStrings.length);
                            column.setSeedStrings(randomStrings);
                        } else {
                            int index = Integer.parseInt(temp[7]+8);
                            column.setVarcharLength(index);
                            String tmp = temp[index+1].substring(1, temp[index+1].length() - 1).replaceAll(" ", "");
                            String[] randomStrings = tmp.split(",");
                            column.setSeedStringNum(randomStrings.length);
                            column.setSeedStrings(randomStrings);
                        }

                    } 

                    int correlationFactor = 0;
                    if (temp[2].charAt(4) == '0')
                        correlationFactor = 0;
                    else if (temp[2].charAt(4) == '1')
                        correlationFactor = 1;
                    else if (temp[2].charAt(4) == '2')
                        correlationFactor = 2;
                    else
                        correlationFactor = -1;

                    String drivingColumnName = null;
                    String drivingTableName = null;

                    if (correlationFactor != 0) {
                        drivingTableName = temp[3].split("\\.")[0].trim();
                        drivingColumnName = temp[3].split("\\.")[1].trim();
                    }

                    // 倾斜分布下的数据分段的index
                    List<Integer> indexList = new ArrayList<>();
                    if (skewness != 0.0) {
                        String tmp = temp[5].substring(1, temp[5].length() - 1);
                        String ids[] = tmp.split(",");
                        for (String id : ids) {
                            indexList.add(Integer.parseInt(id.trim()));
                        }

                    }

                    List<Integer> corrIndexList = new ArrayList<>();
                    if (correlationFactor == 2) {
                        String tmp = temp[6].substring(1, temp[6].length() - 1);
                        String ids[] = tmp.split(",");
                        for (String id : ids) {
                            if (id != "") {
                                corrIndexList.add(Integer.parseInt(id.trim()));
                            }
                        }
                    }
                    // System.out.println(indexList);
                    attrGeneFuncs = new AttrGeneFunc[Integer.parseInt(temp[7].trim())];
                    if (dataType != DataType.VARCHAR) {
                        for (int i = 8; i < temp.length; i++) {
                            String exp = temp[i].trim();
                            String exptemp[] = exp.split("\\+");
                            if (exptemp.length > 2) {
                                expression = new QuadraticFunc();
                                ((QuadraticFunc) expression).setExpression(temp[i].trim());
                                getCofficients(expression, temp[i].trim());
                            } else {
                                expression = new LinearFunc();
                                ((LinearFunc) expression).setExpression(temp[i].trim());
                                getCofficients(expression, temp[i].trim());
                            }
                            attrGeneFuncs[i - 8] = expression;
                        }
                    } else {
                        String exp = temp[8].trim();
                        String exptemp[] = exp.split("\\+");
                        if (exptemp.length > 2) {
                            expression = new QuadraticFunc();
                            ((QuadraticFunc) expression).setExpression(temp[8].trim());
                            getCofficients(expression, temp[8].trim());
                        } else {
                            expression = new LinearFunc();
                            ((LinearFunc) expression).setExpression(temp[8].trim());
                            getCofficients(expression, temp[8].trim());
                        }
                        attrGeneFuncs[0] = expression;
                    }

                    column.setTableName(tableName);
                    column.setTableSize(tableMap.get(tableName).getTableSize());
                    column.setAttName(columnName);
                    column.setDataDistributionProbability(dataDistributionProbability);
                    column.setDataType(dataType);
                    column.setCorrelationFactor(correlationFactor);
                    column.setColumnGeneExpressions(Arrays.asList(attrGeneFuncs));
                    column.setDrvingTableName(drivingTableName);
                    column.setDrvingColumnName(drivingColumnName);
                    column.setSkewness(skewness);

                    if (skewness != 0.0)
                        column.setIndexList(indexList);
                    if (correlationFactor == 2)
                        column.setCorrColumn_PK_IndexList(corrIndexList);

                    if (!table2column.containsKey(tableName)) {
                        List<Column> c = new ArrayList<>();
                        c.add(column);
                        table2column.put(tableName, c);
                    } else {
                        table2column.get(tableName).add(column);
                    }

                } else if (tempString.startsWith("fk:")) {
                    foreignKey = new ForeignKey();
                    tempString = tempString.substring(4);
                    String temp[] = tempString.split("\\|\\|");
                    String tableName = temp[0].split("\\.")[0].trim();
                    String referencedInfo[] = temp[0].split("\\.")[1].trim().split("->");

                    foreignKey.setLocalTableName(tableName);
                    foreignKey.setLocalTableSize(tableMap.get(tableName).getTableSize());
                    foreignKey.setFkColumnName(referencedInfo[0].trim());
                    PrimaryKey pk = new PrimaryKey(referencedInfo[1].trim(),
                            (int) Long.parseLong(referencedInfo[3].trim()), referencedInfo[2].trim());
                    foreignKey.setPrimaryKey(pk);
                    foreignKey.setIntDataType(DataType.INTEGER);

                    attrGeneFuncs = new AttrGeneFunc[temp.length - 2];
                    for (int i = 2; i < temp.length; i++) {
                        if (temp[i].contains("mod")) {
                            String mod[] = temp[i].split("mod");
                            expression = new ModFunc();
                            ((ModFunc) expression).setExpression(temp[i].trim());
                            ((ModFunc) expression).setBase(Integer.parseInt(mod[1].trim()));
                            attrGeneFuncs[i - 2] = expression;
                        } else {
                            expression = new LinearFunc();
                            ((LinearFunc) expression).setExpression(temp[i].trim());
                            getCofficients(expression, temp[i].trim());
                            attrGeneFuncs[i - 2] = expression;
                        }
                    }
                    foreignKey.setColumnGeneExpressions(Arrays.asList(attrGeneFuncs));

                    if (!table2fk.containsKey(tableName)) {
                        List<ForeignKey> fks = new ArrayList<>();
                        fks.add(foreignKey);
                        table2fk.put(tableName, fks);
                    } else {
                        table2fk.get(tableName).add(foreignKey);
                    }

                }

            }
            reader.close();

            // 读入query
            // reader = new BufferedReader(new FileReader(queryFile));
            // while ((tempString = reader.readLine()) != null) {
            // querys.add(tempString.trim());
            // }
            // reader.close();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        DirectedPseudograph<Table, ForeignKey> foreignKeyDependency = new DirectedPseudograph<>(ForeignKey.class);

        // 为关联列setDrivingColumn;为表间的关联列setDrivingFK
        for (Map.Entry<String, List<Column>> entry : table2column.entrySet()) {
            List<Column> columns = entry.getValue();
            for (Column column1 : columns) {
                if (column1.getCorrelationFactor() != 0) {
                    column1.setDrivingColumn(
                            findColumn(table2column, column1.getDrvingTableName(), column1.getDrvingColumnName()));
                }
                if (column1.getCorrelationFactor() != 0
                        && !column1.getDrivingColumn().getTableName().equals(entry.getKey()))
                    column1.setDrivingFK(findFK(table2fk, entry.getKey(), column1.getDrivingColumn().getTableName()));
            }
        }

        // 更新Table类
        for (Map.Entry<String, Table> entry : tableMap.entrySet()) {
            String tableName = entry.getKey();
            if (table2column.containsKey(tableName))
                entry.getValue().setColumns(table2column.get(tableName));
            else
                entry.getValue().setColumns(Arrays.asList(new Column[0]));
            if (table2fk.containsKey(tableName))
                entry.getValue().setForeignKeys(table2fk.get(tableName));
            else
                entry.getValue().setForeignKeys(Arrays.asList(new ForeignKey[0]));
            tables.add(entry.getValue());
            foreignKeyDependency.addVertex(entry.getValue());
        }

        // 构建表的参照依赖关系图
        for (Map.Entry<String, Table> entry : tableMap.entrySet()) {
            for (ForeignKey fk : entry.getValue().getForeignKeys()) {
                foreignKeyDependency.addEdge(entry.getValue(), tableMap.get(fk.getReferencedTableName()), fk);
            }
        }
        DBSchema dbSchemas = new DBSchema(tables, foreignKeyDependency);
        RecordLog.recordLog(LogLevelConstant.INFO, "数据库Schema读取结束");
        new GeneExpressionOutFile().dumpGeneExpression(dbSchemas);// 将生成函数保存下来
        return dbSchemas;
    }

    public static void getCofficients(AttrGeneFunc expression, String exp) {
        String temp[] = exp.split("\\+");
        // temp长度大于2: 有二次项
        if (temp.length > 2) {
            ((QuadraticFunc) expression)
                    .setCoefficient2(new BigDecimal(Double.parseDouble(temp[0].split("\\*")[0].trim())));
            ((QuadraticFunc) expression)
                    .setCoefficient1(new BigDecimal(Double.parseDouble(temp[1].split("\\*")[0].trim())));
            ((QuadraticFunc) expression).setCoefficient0(new BigDecimal(Double.parseDouble(temp[2].trim())));
        } else {
            ((LinearFunc) expression)
                    .setCoefficient1(new BigDecimal(Double.parseDouble(temp[0].split("\\*")[0].trim())));
            ((LinearFunc) expression).setCoefficient0(new BigDecimal(Double.parseDouble(temp[1].trim())));
        }
    }

    public static Column findColumn(Map<String, List<Column>> table2Columnn, String tableName, String ColumnName) {
        List<Column> columns = table2Columnn.get(tableName);
        for (Column column : columns) {
            if (column.getColumnName().equals(ColumnName))
                return column;
        }
        return null;
    }

    public static ForeignKey findFK(Map<String, List<ForeignKey>> table2fk, String referTable, String referedTable) {
        List<ForeignKey> fks = table2fk.get(referTable);
        for (ForeignKey fk : fks) {
            if (fk.getReferencedTableName().equals(referedTable))
                return fk;
        }
        return null;
    }

    public Map<String, Table> getTableMap() {
        return tableMap;
    }

}
