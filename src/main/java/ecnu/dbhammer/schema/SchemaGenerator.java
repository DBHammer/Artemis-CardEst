package ecnu.dbhammer.schema;


import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.data.DataType;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.schema.genefunc.AttrGeneFunc;
import ecnu.dbhammer.schema.genefunc.LinearFunc;
import ecnu.dbhammer.test.GeneExpressionOutFile;
import ecnu.dbhammer.utils.RandomDataGenerator;
import org.jgrapht.graph.DirectedPseudograph;


import java.math.BigDecimal;
import java.util.*;

/**
 * 随机模式生成器
 */
public class SchemaGenerator {

    private double[] dataTypeComulativeProbabilities = null;
    private DataType[] dataTypes = null;
    private List<Table> tables;
    private List<PrimaryKey> primaryKeyList;
    private List<List<Column>> columnsList;
    private List<List<ForeignKey>> foreignsList;
    private int[] tableSizes;
    private List<String> tableNames;

    public SchemaGenerator(Map<String, Float> dataType2OccurProbability) {
        this.initInfo4DataTypeRandomGene(dataType2OccurProbability);
    }

    public DBSchema schemaGenerate() throws Exception {
        Map<String, Map<String, Float>> dataDistributionProbability = Configurations.getDataDistributionProbability();

        int tableNum = Configurations.getTableNumHistogram().getRandomIntValue();
        //RecordLog.recordLog(LogLevelConstant.INFO, "本次测试生成" + tableNum + "个表");

        tables = new ArrayList<>(tableNum);
        tableSizes = new int[tableNum];
        tableNames = new ArrayList<>();
        primaryKeyList = new ArrayList<>();
        columnsList = new ArrayList<>();

        DirectedPseudograph<Table, ForeignKey> foreignKeyDependency = new DirectedPseudograph<>(ForeignKey.class);
        for (int i = 0; i < tableNum; i++) {
            //RecordLog.recordLog(LogLevelConstant.INFO, "开始生成第" + i +"个表");

            //这里可以自己diy表的大小
            int randomTableSize;
            randomTableSize = Configurations.getTableSizeHistogram().getRandomIntValue();
            tableSizes[i] = randomTableSize;
            String tableName = "table_" + i;
            tableNames.add(tableName);
            //RecordLog.recordLog(LogLevelConstant.INFO, "表大小为" + randomTableSize);
            //生成主键
            primaryKeyList.add(new PrimaryKey(tableName, randomTableSize, "primaryKey"));

            //RecordLog.recordLog(LogLevelConstant.INFO, "主键生成结束");
            //生成普通属性
            int columnNum = Configurations.getColumnNumHistogram().getRandomIntValue();
            Column[] columns = new Column[columnNum];//所有column
            //配置属性的生成函数组中函数数量
            //update by ct: 原先zipfian分布时，所有列都是同一个zipfian分布-->改成前50%的列满足zipfian分布，后25%的列为均匀分布，最后25%的列满足数据关联
            for (int k = 0; k < columnNum; k++) {//
                String columnName = "col_" + k;
                // 随机生成普通列的数据类型
                DataType dataType = getRandomDataType();

                if (dataType == DataType.DECIMAL) { // TODO 待修改，数据类型为decimal，需根据数据进行实际的精度制定
                    int decimalP = 65;
                    int decimalS = 30;
                    if (decimalP < decimalS) {
                        decimalP = decimalS;
                    }
                    try {
                        columns[k] = new Column(tableName, randomTableSize, columnName, dataDistributionProbability, dataType,
                                decimalP, decimalS, columnNum, k);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else if (dataType == DataType.VARCHAR) { // TODO 待修改，数据类型为varchar，如何保证diversity的字符串类型生成
                    int varcharLength = Configurations.getVarcharLengthHistogram().getRandomIntValue();
                    int seedStringNum = Configurations.getSeedStringNum();
                    try {
                        columns[k] = new Column(tableName, randomTableSize, columnName, dataDistributionProbability, dataType, varcharLength, seedStringNum, columnNum, k);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else { // int、long、float、double、datetime、bool
                    columns[k] = new Column(tableName, randomTableSize, columnName, dataDistributionProbability, dataType, columnNum, k);
                }
            }
            columnsList.add(Arrays.asList(columns));
            //普通属性生成结束
        }
        //RecordLog.recordLog(LogLevelConstant.INFO, "普通属性生成结束");

        //开始生成外键
        //RecordLog.recordLog(LogLevelConstant.INFO, "开始生成外键");
        foreignsList = new ArrayList<>();
        List<Integer> candidateTable = new LinkedList<>();
        for (int i = 0; i < tableNum; i++) {
            candidateTable.add(i);
        }

        for (int i = 0; i < tableNum; i++) {
            //RecordLog.recordLog(LogLevelConstant.INFO, "开始生成表" + tableNames.get(i) + "的外键");
            //以下二选一，第二个指生成满连接关系，第一个随机控制每个表的外键数量
            //int foreignKeyNum = new Random().nextInt(tableNum);
            //update by ct: 每张表的外键大于等于连接数目，为了clique能达到指定连接数目
            //int foreignKeyNum = (int) (Configurations.getTableNumPerQuery() + Math.random() * (tableNum-Configurations.getTableNumPerQuery()));
            int foreignKeyNum = tableNum-1;
            //每个表的外键数量不能超过表的数量
            if (foreignKeyNum >= tableNum) {
                foreignKeyNum = tableNum - 1;
            }

            List<Integer> candidateTableList = new LinkedList<>();
            candidateTableList.addAll(candidateTable);
            candidateTableList.remove(i);//排除参照自己
            List<Integer> delList = new ArrayList<>();
//            for(int tableId : candidateTableList){
//                //这里用>就是排除参照比自己大的表，就是只让大表参照小表
//                //用<就是排除参照比自己小的表，就是只让小表参照大表
//                if(tableSizes[tableId]>tableSizes[i]){
//                    delList.add(tableId);
//                }
//            }
//            for(int tableId : delList){
//                //System.out.println("删除"+tableId);
//
//                candidateTableList.remove((Integer) tableId);
//            }
//            foreignKeyNum = candidateTableList.size();

            //RecordLog.recordLog(LogLevelConstant.INFO, tableNames.get(i)+"表可参照的表的ID List:" + candidateTableList);
            //RecordLog.recordLog(LogLevelConstant.INFO, "开始生成该表的每一个外键");

            //实验用
            //foreignKeyNum = 1;
            ForeignKey[] foreignKeys = new ForeignKey[foreignKeyNum];
            //RecordLog.recordLog(LogLevelConstant.INFO, tableNames.get(i)+"表外键的最终数量:" + foreignKeyNum);

            for (int k = 0; k < foreignKeyNum; k++) {
                //生成一个表的过程中，每个外键参照的表都不一样

                int randomIndex = (int) (Math.random() * candidateTableList.size());
                int referencedTableId = candidateTableList.get(randomIndex);
                //RecordLog.recordLog(LogLevelConstant.INFO, "选择的表为:"+tableNames.get(referencedTableId) + "大小为:"+tableSizes[referencedTableId]);
                candidateTableList.remove(randomIndex);//O(1)
                String fkColumnName = "fk_" + k;
                PrimaryKey referencedPrimary = primaryKeyList.get(referencedTableId);

                foreignKeys[k] = new ForeignKey(referencedPrimary, tableNames.get(i), tableSizes[i], fkColumnName);


            }
            foreignsList.add(Arrays.asList(foreignKeys));
        }

        //update by ct: 为具有数据关联的列 选择 driving column驱动列
        chooseDrivingColumns();

        //构建所有表
        Map<String,Table> name2Table = new HashMap<>();
        for (int i=0; i<tableNum; i++){
            Table table = new Table(i,tableNames.get(i),tableSizes[i],primaryKeyList.get(i),foreignsList.get(i),columnsList.get(i),null);
            tables.add(table);
            foreignKeyDependency.addVertex(table);
            name2Table.put(tableNames.get(i),table);
        }
        //构建表的参照依赖关系图
        for(int i=0;i<tableNum;i++){
            for(ForeignKey foreignKey : tables.get(i).getForeignKeys()){
                foreignKeyDependency.addEdge(tables.get(i),name2Table.get(foreignKey.getReferencedTableName()),foreignKey);
            }
        }

        //TODO 进行更diversity的schema状态生成，比如索引

        DBSchema dbSchemas = new DBSchema(tables,foreignKeyDependency);
        RecordLog.recordLog(LogLevelConstant.INFO, "数据库Schema生成结束");
        return dbSchemas;

    }


    //为具有数据关联的列 选择 driving column驱动列 并且在此过程中为关联列构建生成函数（部分相关的情况）
    private void chooseDrivingColumns(){
        //遍历所有表的所有列
        for(int i=0; i<columnsList.size();i++) {
            for(int j=0; j<columnsList.get(i).size(); j++) {
                if (columnsList.get(i).get(j).getCorrelationFactor() != 0) {
                    //分为单表列间 和 表间
                    int type = (int) (Math.random() * 2);
                    if (foreignsList.get(i).size() == 0) type = 0;//表明当前表无外键，只能在表内找
                    List<Integer> candidateFKID = new ArrayList<>();//候选外键
                    for(int k=0; k<foreignsList.get(i).size(); k++){
                        //主键表要比外键表大或者相等，要不然外键不满足递增或递减，那么关联列也不满足递增或递减，就会对谓词过滤求主键范围造成影响
                        if(foreignsList.get(i).get(k).getReferencedTableSize() >= tableSizes[i])
                            candidateFKID.add(k);
                    }
                    if(candidateFKID.size() == 0) type = 0;

                    //实验用
                    //type = 1;

                    List<Integer> drivingColumn4Select = new ArrayList<>();

                    Column drivingCol = null;
                    if (type == 0) { //单表内：选择同一张表的列
                        //TODO 这里的0.5和下面的0.5都要根据SchemaGene里的设置改变
                        // 需保证选择的驱动列也是int类型的，且倾斜度不能为0，否则会为空
                        for (int k = 0; k < columnsList.get(i).size(); k++) {
                            if(columnsList.get(i).get(k).getDataType() == DataType.INTEGER && k != j && columnsList.get(i).get(k).getSkewness() != 0) {
                                drivingColumn4Select.add(k);
                            }
                        }

                        int num = (int) (drivingColumn4Select.size());
                        int random = drivingColumn4Select.get((int) (Math.random() * num));
                        drivingCol = columnsList.get(i).get(random);
                        columnsList.get(i).get(j).setDrivingColumn(drivingCol);
                    } else { //表间：选择另一张表的列
                        int random1 = (int) (Math.random() * candidateFKID.size());//选候选外键参照的表中的一个
                        String referTableName = foreignsList.get(i).get(candidateFKID.get(random1)).getReferencedTableName();
                        //System.out.println("referName " + referTableName);
                        int tableID = Integer.parseInt(referTableName.split("_")[1]);

                        // 初始化int类型的驱动列
                        for (int k = 0; k < columnsList.get(tableID).size(); k++) {
                            if(columnsList.get(tableID).get(k).getDataType() == DataType.INTEGER && columnsList.get(tableID).get(k).getSkewness() != 0) {
                                drivingColumn4Select.add(k);
                            }
                        }    

                        int num = (int) (drivingColumn4Select.size());//zipfian列的个数
                        int random2 = drivingColumn4Select.get((int)(Math.random() * num));//随机选择一个列
                        drivingCol = columnsList.get(tableID).get(random2);
                        columnsList.get(i).get(j).setDrivingColumn(drivingCol);
                        columnsList.get(i).get(j).setDrivingFK(foreignsList.get(i).get(candidateFKID.get(random1)));
                        //System.out.println(columnsList.get(tableID).get(random2).getTableName());
                        //System.out.println(foreignsList.get(i).get(random1));
                    }

                    //若数据关联为部分相关，则还需要在此为关联列指定生成规则（即生成函数）
                    if(columnsList.get(i).get(j).getCorrelationFactor() == 2)
                        geneAttrGeneFuncs4Correlation(columnsList.get(i).get(j), drivingCol);
                }
            }
        }
    }

    private void geneAttrGeneFuncs4Correlation(Column corrColumn, Column drivingColumn){
        //用于存储分段主键下标，同一段内的主键对应的属性列使用同一个生成函数
        List<Integer> drivingCol_PK_IndexList = drivingColumn.getIndexList();
        List<Integer> corrCol_PK_IndexList = new ArrayList<>();
        List<AttrGeneFunc> attrGeneFuncs = new ArrayList<>();

        //遍历驱动列的每一个数据项（默认驱动列为倾斜分布）
        for(int i=0;i<drivingCol_PK_IndexList.size()-1;i++){
            //关联列分段：对驱动列的每一个数据项，如“5”有20个，可设定10个“5”对应的关联列数据为“50”，10个“5”对应的关联列数据为“60”
            //随机分段
            int sum = drivingCol_PK_IndexList.get(i);
            while(sum < drivingCol_PK_IndexList.get(i+1)){
                int max = drivingCol_PK_IndexList.get(i+1) - sum;
                int subLength = new Random().nextInt(max) + 1;//段的长度
                if(attrGeneFuncs.size() == 0){ //第一个数据项，构建一个生成函数（常函数，因为别的函数不能保证老王所说的随机的数据关联）
                    attrGeneFuncs.add(new LinearFunc(new BigDecimal(0), new BigDecimal(new Random().nextInt())));
                }else{//之后的数据项，有两种方式：①随机构建一个生成函数 ②选择已有的函数
                    int type = new Random().nextInt(2);
                    if(type == 0){
                        attrGeneFuncs.add(new LinearFunc(new BigDecimal(0), new BigDecimal(new Random().nextInt())));
                    }else{
                        int k = new Random().nextInt(attrGeneFuncs.size());
                        attrGeneFuncs.add(attrGeneFuncs.get(k));
                    }
                }
                //记录关联列对应的主键下标
                corrCol_PK_IndexList.add(sum);
                sum += subLength;
            }
        }
        //生成函数信息放入关联列中
        corrColumn.setColumnGeneExpressions(attrGeneFuncs);
        corrColumn.setCorrColumn_PK_IndexList(corrCol_PK_IndexList);
    }

    private void initInfo4DataTypeRandomGene(Map<String, Float> dataType2OccurProbability) {
        dataTypeComulativeProbabilities = new double[dataType2OccurProbability.size()];
        dataTypes = new DataType[dataType2OccurProbability.size()];
        Iterator<Map.Entry<String, Float>> iter = dataType2OccurProbability.entrySet().iterator();
        int idx = 0;
        while (iter.hasNext()) {
            Map.Entry<String, Float> entry = iter.next();
            String name = entry.getKey().toUpperCase();
            if (name.equals("INT")) {
                name = "INTEGER";
            }
            dataTypes[idx] = Enum.valueOf(DataType.class, name);
            dataTypeComulativeProbabilities[idx] = entry.getValue();
            idx++;

        }
        for (int i = 1; i < dataTypeComulativeProbabilities.length; i++) {
            dataTypeComulativeProbabilities[i] += dataTypeComulativeProbabilities[i - 1];
        }
    }

    private DataType getRandomDataType() {
        double randomValue = Math.random();
        for (int i = 0; i < dataTypeComulativeProbabilities.length; i++) {
            if (randomValue < dataTypeComulativeProbabilities[i]) {
                return dataTypes[i];
            }
        }
        return null;
    }

}
