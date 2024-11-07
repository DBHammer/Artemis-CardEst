package ecnu.dbhammer.schema;


import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.data.AttrValue;
import ecnu.dbhammer.data.DataType;
import ecnu.dbhammer.schema.genefunc.AttrGeneFunc;
import ecnu.dbhammer.schema.genefunc.LinearFunc;
import ecnu.dbhammer.schema.genefunc.QuadraticFunc;
import ecnu.dbhammer.utils.RandomDataGenerator;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 普通Column类，里面含有基于主键的数据映射
 */
public class Column extends Attribute implements Cloneable, Serializable {
    private Map<String,Map<String,Float>> dataDistributionProbability;

    private double skewness;//属性列的数据分布情况 =0是均匀分布 >0是zipfian分布

    //update by ct: 增加数据关联（完全线性相关）
    private int correlationFactor;//相关系数：0表示不相关，1表示正相关，-1表示负相关，2表示部分相关
    private Column drivingColumn = null;//记录和哪个列相关
    private ForeignKey drivingFK = null;//记录和哪个外键有关
    private int columnNum;//属性列总数
    private int columnID;//属性列id

    private int lowBound;
    private int upperBound;


    private List<AttrGeneFunc> attrGeneFuncs;//生成函数的group

    private List<Integer> corrColumn_PK_IndexList = new ArrayList<>();
    private List<Integer> indexList = new ArrayList<>();
    // 只有当属性为decimal数据类型时，下面两个参数才会被启用
    private int decimalP, decimalS;

    // 只有当属性为varchar数据类型时，下面四个参数才会被启用
    // 由于随机字符串的生成规则导致生成的字符串必须包含4个'#'字符和两个整型数字，故字符串的长度必须要足够长！
    // 考虑到整型数字（int）最大长度是10位， 故varcharLength需大于10+10+4
    // 生成的随机字符串是不定长的，但其最大长度保证小于varcharLength
    private int varcharLength;
    private int seedStringNum;

    private String[] seedStrings = null;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

    private String drvingTableName = null;
    private String drvingColumnName = null;

    public Column(){
        super();
    }



    //二分寻找最后一个小于等于给定target值的下标
    private int lastLessOrEqualsBinarySearch(List<Integer> list, int target) {
        int l = 0, r = list.size() - 1;
        while (l < r) {
            int mid = l + ((r - l + 1) >> 1);
            if (list.get(mid) > target) r = mid - 1;
            else l = mid; //
        }
        if (list.get(l) <= target && (l == list.size() - 1 || list.get(l+1) > target)) {
            return l;
        } // <=
        return -1;
    }

    //找到主键对应的生成函数下标
    public int hashFunction(int pk){
        if(indexList.size()!=0){
            int index = this.lastLessOrEqualsBinarySearch(indexList,pk);
            return index;

        }else{
            //return pk % this.getColumnGeneExpressionNum();
            return 0;
        }

        //待修改，如果是均匀分布，只会有一个生成函数
        //如果是zipfian分布，使用特殊的分段规则
    }

    //针对部分相关的关联数据
    public int hashFunction2(int pk){
        if(corrColumn_PK_IndexList.size()!=0){
            int index = this.lastLessOrEqualsBinarySearch(corrColumn_PK_IndexList,pk);
            return index;
        }else{
            return 0;
        }
    }

    /**
     * 除decimal和varchar外的其他类型的列
     * @param tableName
     * @param tableSize
     * @param columnName
     * @param dataDistributionProbability
     * @param dataType
     * @throws Exception
     */
    public Column(String tableName, int tableSize, String columnName, Map<String, Map<String, Float>> dataDistributionProbability,
                  DataType dataType, int columnNum, int columnID) throws Exception {
        super(tableName,tableSize,columnName,dataType);
        this.dataDistributionProbability = dataDistributionProbability;
        this.columnNum = columnNum;
        this.columnID = columnID;
        this.skewness = 0;
        this.correlationFactor = 0;//初始化为不相关
        if(this.dataDistributionProbability != null){
            initColumnGeneExpressions(this.dataDistributionProbability);//初始化生成函数表达式
        }
        //System.out.println("生成函数组数量:"+this.attrGeneFuncs.size());
    }


    /**
     * decimal数据类型 或者 varchar数据类型
     * @param tableName
     * @param tableSize
     * @param attName
     * @param dataDistributionProbability
     * @param dataType
     * @param decimalP_varcharLength
     * @param decimalS_seedStringNum
     * @throws Exception
     */
    public Column(String tableName, int tableSize, String attName, Map<String, Map<String, Float>> dataDistributionProbability, DataType dataType,
                  int decimalP_varcharLength, int decimalS_seedStringNum, int columnNum, int columnID) throws Exception {
        super(tableName,tableSize,attName,dataType);
        this.dataDistributionProbability = dataDistributionProbability;
        this.columnNum = columnNum;
        this.columnID = columnID;
        this.skewness = 0;
        this.correlationFactor = 0;//初始化为不相关

        if (dataType == DataType.DECIMAL) {
            this.decimalP = decimalP_varcharLength;
            this.decimalS = decimalS_seedStringNum;
        } else if (dataType == DataType.VARCHAR) {
            this.varcharLength = decimalP_varcharLength;
            if (this.varcharLength < 30) {
                throw new Exception("配置信息有误，字符串的最小长度为30！" + attName + ":" + this.varcharLength);
            }
            this.seedStringNum = decimalS_seedStringNum;

            seedStrings = new String[seedStringNum];
            // 24=10+10+4，两个整型和4个'#'字符的最大长度
            int seedVarcharLength = varcharLength - 24;
            for (int i = 0; i < seedStringNum; i++) {
                seedStrings[i] = RandomDataGenerator.geneString(seedVarcharLength);
            }
        }
        if(this.dataDistributionProbability != null) {
            initColumnGeneExpressions(this.dataDistributionProbability);
        }
    }

    // 这里假设测试数据表的size在100亿以内
    private void initColumnGeneExpressions(Map<String,Map<String,Float>> dataDistributionProbability) throws Exception {
        attrGeneFuncs = new ArrayList<>();

        switch (super.getDataType()) {
            case INTEGER: // int
                if (Math.random()<dataDistributionProbability.get("int").get("uniform")) {
                    //完全的均匀分布只需要一个一次函数即可
                    attrGeneFuncs.add(randomLinearFunction());//均匀分布
                }
                else if(Math.random()<dataDistributionProbability.get("int").get("uniform")+dataDistributionProbability.get("int")
                        .get("zipfian")){
                    //update by ct: 原先zipfian分布时，所有列都是同一个zipfian分布
                    // -->改成前50%的列满足zipfian分布，后25%的列为均匀分布，最后25%的列满足数据关联

                    if (columnID < columnNum * 0.5)
                        initZipfianColumnGeneExpression4Int();//Zipf分布采用分段函数
                    else if (columnID < columnNum * 0.75)
                        attrGeneFuncs.add(randomLinearFunction());//均匀分布
                    else
                    // TODO数据关联部分存在问题
                        initCorrelationColumnGeneExpression4Int();//数据关联的列
                }
                break;
            // TODO目前除了int外，其他类型均还未支持zipfian分布
            case LONG: // long
                if (Math.random()<dataDistributionProbability.get("long").get("uniform")) {
                    attrGeneFuncs.add(initColumnGeneExpression4Long());//
                }
                else if(Math.random()<dataDistributionProbability.get("long").get("uniform")+dataDistributionProbability.get("long")
                        .get("zipfian")){
                    initZipfianColumnGeneExpression4Long();
                }

                break;
            case FLOAT: // float
                if (Math.random()<dataDistributionProbability.get("float").get("uniform")) {
                    attrGeneFuncs.add(initColumnGeneExpression4Real());//
                }
                else if(Math.random()<dataDistributionProbability.get("float").get("uniform")+dataDistributionProbability.get("float")
                        .get("zipfian")){

                    initZipfianColumnGeneExpression4Long();
                }
                break;
            case DOUBLE: // double
                if (Math.random()<dataDistributionProbability.get("double").get("uniform")) {
                    attrGeneFuncs.add(initColumnGeneExpression4Real());//
                }
                else if(Math.random()<dataDistributionProbability.get("double").get("uniform")+dataDistributionProbability.get("double")
                        .get("zipfian")){
                    initZipfianColumnGeneExpression4Long();
                }
                break;
            case DECIMAL: // decimal
                attrGeneFuncs.add(initColumnGeneExpression4Decimal());
                break;
            case VARCHAR: // varchar
                if (Math.random()<dataDistributionProbability.get("varchar").get("uniform")) {
                    attrGeneFuncs.add(initColumnGeneExpression4Varchar());//
                }
                else if(Math.random()<dataDistributionProbability.get("varchar").get("uniform")+dataDistributionProbability.get("varchar")
                        .get("zipfian")){
                    initZipfianColumnGeneExpression4Int();
                }
                break;
            case TIMESTAMP: // datetime
                attrGeneFuncs.add(initColumnGeneExpression4DateTime());
                break;
            case BOOL: // bool
                // 模2为0的即为true，其他为false
                //bool类型的只有随机生成
                attrGeneFuncs.add(initColumnGeneExpression4Int());
                break;
            case BLOB:
                //TODO
                break;
        }

    }

    private AttrGeneFunc randomLinearFunction(){
//        BigDecimal coefficient1 = new BigDecimal((int)(Math.random() * (Integer.MAX_VALUE - 100000000) /
//                super.getTableSize() * (Math.random() < 0.5 ? 1 : -1)));
        BigDecimal coefficient1 = new BigDecimal((int)(Math.random() * 2) +1);
        BigDecimal coefficient0 = new BigDecimal((int)(Math.random() * 50 * (Math.random() < 0.5 ? 1 : -1)));
        return new LinearFunc(coefficient1, coefficient0);
    }

    private AttrGeneFunc initColumnGeneExpression4Int() {
        // 小表（size小于10000的表）有50%的可能性具有二次项
        //先默认都是一次项 容易计算
        //TODO 目前生成函数是根据表的大小来选择一次和二次的
        //均匀分布使用一次函数即可
        //这里控制列的生成函数group中是否有二次项公式
        BigDecimal coefficient1 = new BigDecimal((int)(Math.random() * (Integer.MAX_VALUE - 100000000) /
                super.getTableSize() * (Math.random() < 0.5 ? 1 : -1)));
        BigDecimal coefficient0 = new BigDecimal((int)(Math.random() * 10000 * (Math.random() < 0.5 ? 1 : -1)));
//            BigDecimal coefficient1 = new BigDecimal(5);
//            BigDecimal coefficient0 = new BigDecimal(1);
        return new LinearFunc(coefficient1, coefficient0);

    }

    private void initZipfianColumnGeneExpression4Int() throws Exception {
        //P(r) = C/r^z
        //[]区间使用不同的一次函数
        this.skewness = Configurations.getZipSkewness();
        double c = 0.5;
        this.lowBound = new Random().nextInt(super.getTableSize());

        int gap = new Random().nextInt((Integer.MAX_VALUE-lowBound)/(super.getTableSize()-1));

        //首先控制数据项的个数
        int baseSize = 100;
        int itemNum;
        int baseItemNum=new Random().nextInt(10)+5;
        if(super.getTableSize()<100){
            itemNum = baseItemNum;
        }else {
            itemNum = baseItemNum + (super.getTableSize()/baseSize * new Random().nextInt(10));
            if(itemNum>1000){
                itemNum = new Random().nextInt(200)+850;
            }
        }
        //实验用
        //itemNum = 1000;

        //然后根据倾斜度计算每个数据项的概率
        double[] probablities = new double[itemNum];
        double proSum = 0;
        for(int i=0;i<itemNum;i++){
            probablities[i] = c/Math.pow(i+1,skewness);
            proSum += probablities[i];
        }
        //然后进行概率的normalization
        for(int i=0;i<itemNum;i++){
            probablities[i] = probablities[i]/proSum;
        }


        int init = 0;
        int rank = 1;
        int stage = lowBound;
        indexList.add(init);

        while(init < super.getTableSize()){

            int numofEachStage = (int) Math.round(super.getTableSize() * probablities[rank-1]);
            //还是使用一次函数，只不过一次项系数为0

            if(numofEachStage == 0){

                int offset = super.getTableSize() - init;
                if(offset > 5) {

                    // System.out.println("碰到了提前终止的，将剩下的" + offset + "分配给之前的");
                    // System.out.println("原先indexList");
                    // for(int i=0;i<indexList.size();i++){
                    //     System.out.print(indexList.get(i)+" ");
                    // }
                    // System.out.println(" ");
                    int miniInit = 0;
                    int offIndex = 0;
                    int preSum = 0;
                    while (miniInit < offset) {
                        int numOfMini = (int) Math.round(offset * probablities[offIndex]);
                        if (numOfMini == 0) {
                            numOfMini = 1;
                        }
                        indexList.set(offIndex + 1, indexList.get(offIndex+1)+preSum+numOfMini);
                        miniInit += numOfMini;
                        offIndex++;
                        preSum+=numOfMini;
                    }

                    if(preSum != offset){
                        throw new Exception("错误！");
                    }
                    //更新indexList
                    for(int i=offIndex+1;i<indexList.size();i++){
                        indexList.set(i, indexList.get(i)+preSum);
                    }


                    break;
                }else{
                    numofEachStage++;
                }
            }
            this.attrGeneFuncs.add(new LinearFunc(new BigDecimal(0), new BigDecimal(stage)));

            rank++;

            init += numofEachStage;
            if (rank > probablities.length && init < super.getTableSize()) {
                init = super.getTableSize();
            }
            indexList.add(init);

            stage += gap;

        }

        // System.out.println("倾斜度为:"+skewness);
        //System.out.println("最终Item:"+this.attrGeneFuncs.size());
        //System.out.println("最终函数路由器Size:"+indexList.size());
    }

    //update by ct: 增加数据关联，正负线性相关（完全相关）或部分相关
    private void initCorrelationColumnGeneExpression4Int(){
        //随机选择正相关或负相关或部分相关
        int random = (int) (Math.random() * 3);
        random = 2;//默认部分相关
        if(random == 0){//正相关
            correlationFactor = 1;
            BigDecimal coefficient1 = new BigDecimal((int)(Math.random() * 2) +1);
            BigDecimal coefficient0 = new BigDecimal((int)(Math.random() * 50 * (Math.random() < 0.5 ? 1 : -1)));
            attrGeneFuncs.add(new LinearFunc(coefficient1, coefficient0));
        }
        else if(random == 1){//负相关
            correlationFactor = -1;
            BigDecimal coefficient1 = new BigDecimal((int)(Math.random() * -2) -1);
            BigDecimal coefficient0 = new BigDecimal((int)(Math.random() * 50 * (Math.random() < 0.5 ? 1 : -1)));
            attrGeneFuncs.add(new LinearFunc(coefficient1, coefficient0));
        }else if(random == 2){//部分相关
            correlationFactor = 2;
            //生成函数在后续根据驱动列生成；部分相关默认生成函数为常函数
            //代码段位于SchemaGenerator.java中
        }
    }

    private AttrGeneFunc initColumnGeneExpression4Long() {
        // long型的最大值在900亿亿左右，故此时的小表可认为是小于一亿的表
        // 同样，小表有50%的可能性具有二次项
        if (super.getTableSize() < 100000000 && Math.random() < 0.5) {
            // 二次项的系数可在900以内
            BigDecimal coefficient2 = new BigDecimal((int)(Math.random() * 900 * (Math.random() < 0.5 ? 1 : -1)));
            // 一次项系数在1000以内
            BigDecimal coefficient1 = new BigDecimal((int)(Math.random() * 1000 * (Math.random() < 0.5 ? 1 : -1)));
            // 常数项在10000以内
            BigDecimal coefficient0 = new BigDecimal((int)(Math.random() * 10000 * (Math.random() < 0.5 ? 1 : -1)));
            return new QuadraticFunc(coefficient2, coefficient1, coefficient0);
        } else { // 不具有二次项
            // 这里计算中加了"% 1000"，以避免系数过大
            BigDecimal coefficient1 = new BigDecimal((int)(Math.random() * (Long.MAX_VALUE - 100000000) /
                    super.getTableSize() % 1000 * (Math.random() < 0.5 ? 1 : -1)));
            BigDecimal coefficient0 = new BigDecimal((int)(Math.random() * 10000 * (Math.random() < 0.5 ? 1 : -1)));
            return new LinearFunc(coefficient1, coefficient0);
        }
    }

    private void initZipfianColumnGeneExpression4Long() throws Exception {
        //P(r) = C/r^z
        //[]区间使用不同的一次函数
        this.skewness = Configurations.getZipSkewness();
        double c = 0.5;
        this.lowBound = new Random().nextInt(super.getTableSize());

        int gap = new Random().nextInt((Integer.MAX_VALUE-lowBound)/(super.getTableSize()-1));

        //首先控制数据项的个数
        int baseSize = 100;
        int itemNum;
        int baseItemNum=new Random().nextInt(10)+5;
        if(super.getTableSize()<100){
            itemNum = baseItemNum;
        }else {
            itemNum = baseItemNum + (super.getTableSize()/baseSize * new Random().nextInt(10));
            if(itemNum>1000){
                itemNum = new Random().nextInt(200)+850;
            }
        }
        //实验用
        //itemNum = 1000;

        //然后根据倾斜度计算每个数据项的概率
        double[] probablities = new double[itemNum];
        double proSum = 0;
        for(int i=0;i<itemNum;i++){
            probablities[i] = c/Math.pow(i+1,skewness);
            proSum += probablities[i];
        }
        //然后进行概率的normalization
        for(int i=0;i<itemNum;i++){
            probablities[i] = probablities[i]/proSum;
        }


        int init = 0;
        int rank = 1;
        double stage = lowBound + Math.random();
        indexList.add(init);

        while(init < super.getTableSize()){

            int numofEachStage = (int) Math.round(super.getTableSize() * probablities[rank-1]);
            //还是使用一次函数，只不过一次项系数为0

            if(numofEachStage == 0){

                int offset = super.getTableSize() - init;
                if(offset > 5) {

                    // System.out.println("碰到了提前终止的，将剩下的" + offset + "分配给之前的");
                    // System.out.println("原先indexList");
                    // for(int i=0;i<indexList.size();i++){
                    //     System.out.print(indexList.get(i)+" ");
                    // }
                    // System.out.println(" ");
                    int miniInit = 0;
                    int offIndex = 0;
                    int preSum = 0;
                    while (miniInit < offset) {
                        int numOfMini = (int) Math.round(offset * probablities[offIndex]);
                        if (numOfMini == 0) {
                            numOfMini = 1;
                        }
                        indexList.set(offIndex + 1, indexList.get(offIndex+1)+preSum+numOfMini);
                        miniInit += numOfMini;
                        offIndex++;
                        preSum+=numOfMini;
                    }

                    if(preSum != offset){
                        throw new Exception("错误！");
                    }
                    //更新indexList
                    for(int i=offIndex+1;i<indexList.size();i++){
                        indexList.set(i, indexList.get(i)+preSum);
                    }


                    break;
                }else{
                    numofEachStage++;
                }
            }
            this.attrGeneFuncs.add(new LinearFunc(new BigDecimal(0), new BigDecimal(stage)));

            rank++;

            init += numofEachStage;
            if (rank > probablities.length && init < super.getTableSize()) {
                init = super.getTableSize();
            }
            indexList.add(init);

            stage += gap;

        }

        //System.out.println("倾斜度为:"+skewness);
        //System.out.println("最终Item:"+this.attrGeneFuncs.size());
        //System.out.println("最终函数路由器Size:"+indexList.size());
    }

    // 该函数实现参考initColumnGeneExpression4Long()，但是日期类型的毫秒数应该不可能为负数
    private AttrGeneFunc initColumnGeneExpression4DateTime() {
        // 为使得生成的日期类型数据处于一个比较正常的范围，我们的处理机制为：选择一个最近十年内的某个时间点为基线来确定属性生成系数；
        long currentTime = System.currentTimeMillis();
        double randomTimeLength = (20L * 365 * 24 * 3600 * 1000) * Math.random();
        int tableSize = super.getTableSize();
        if (tableSize < 100000000 && Math.random() < 0.5) {
            BigDecimal coefficient2 = new BigDecimal(randomTimeLength / tableSize / tableSize);
            BigDecimal coefficient1 = new BigDecimal(randomTimeLength / tableSize);
            BigDecimal coefficient0 = new BigDecimal(currentTime - 2 * randomTimeLength);
            return new QuadraticFunc(coefficient2, coefficient1, coefficient0);
        } else {
            BigDecimal coefficient1 = new BigDecimal(randomTimeLength / tableSize);
            BigDecimal coefficient0 = new BigDecimal(currentTime - randomTimeLength);
            return new LinearFunc(coefficient1, coefficient0);
        }
    }


    private AttrGeneFunc initColumnGeneExpression4Real() {
        if (Math.random() < 0.0) {
            // float的最大值在E38左右，double的最大值在E308左右
            BigDecimal coefficient2 = new BigDecimal(Math.random() * 1000 * (Math.random() < 0.5 ? 1 : -1));
            BigDecimal coefficient1 = new BigDecimal(Math.random() * 1000 * (Math.random() < 0.5 ? 1 : -1));
            BigDecimal coefficient0 = new BigDecimal(Math.random() * 10000 * (Math.random() < 0.5 ? 1 : -1));
            return new QuadraticFunc(coefficient2, coefficient1, coefficient0);
        } else {
            BigDecimal coefficient1 = new BigDecimal(Math.random() * (Float.MAX_VALUE - 100000000) /
                    super.getTableSize() % 1000 * (Math.random() < 0.5 ? 1 : -1));
            BigDecimal coefficient0 = new BigDecimal(Math.random() * 10000 * (Math.random() < 0.5 ? 1 : -1));
            return new LinearFunc(coefficient1, coefficient0);
        }
    }



    private AttrGeneFunc initColumnGeneExpression4Decimal() {
        int integerPartLength = decimalP - decimalS;
        double maxIntegerPartValue = Math.pow(10, integerPartLength) - 1;
        if (maxIntegerPartValue > Integer.MAX_VALUE && super.getTableSize() < 10000 && Math.random() < 0.0) {
            BigDecimal coefficient2 = new BigDecimal(Math.random() * 20 * (Math.random() < 0.5 ? 1 : -1));
            BigDecimal coefficient1 = new BigDecimal(Math.random() * 1000 * (Math.random() < 0.5 ? 1 : -1));
            BigDecimal coefficient0 = new BigDecimal(Math.random() * 10000 * (Math.random() < 0.5 ? 1 : -1));
            return new QuadraticFunc(coefficient2, coefficient1, coefficient0);
        } else {
            // 每个属性的decimalP，decimalS是随机生成的，故(maxIntegerPartValue * 0.9)无需乘以Math.random()
            //TODO modify 0.9 0.05
            BigDecimal coefficient1 = new BigDecimal((maxIntegerPartValue * 0.8) / super.getTableSize() * (Math.random() < 0.5 ? 1 : -1));
            BigDecimal coefficient0 = new BigDecimal(Math.random() * maxIntegerPartValue * 0.05 * (Math.random() < 0.5 ? 1 : -1));
            // decimalP可能等于decimalS
            if (maxIntegerPartValue == 0) { // 此时decimalS必然大于1
                coefficient1 = new BigDecimal(0.8 / super.getTableSize() * (Math.random() < 0.5 ? 1 : -1));
                coefficient0 = new BigDecimal(Math.random() * 0.1 * (Math.random() < 0.5 ? 1 : -1));
            }
            return new LinearFunc(coefficient1, coefficient0);
        }
    }

    private AttrGeneFunc initColumnGeneExpression4Varchar() {
        // 控制其生成出来的整数在10位以内
        BigDecimal coefficient1 = new BigDecimal(Math.random() * (Integer.MAX_VALUE - 100000000) /
                super.getTableSize() % 1000 * (Math.random() < 0.5 ? 1 : -1));
        BigDecimal coefficient0 = new BigDecimal(Math.random() * 10000 * (Math.random() < 0.5 ? 1 : -1));
        return new LinearFunc(coefficient1, coefficient0);
    }

    

    /**
     * 判断其是否为使用一次函数的线性分布
     * @return
     */
    public boolean judgeProperties(){
        if(this.getColumnGeneExpressionNum() == 1){
            if (this.attrGeneFuncs.get(0) instanceof LinearFunc){
                return true;
            }
        }
        return false;
    }

    @Override
    public BigDecimal attrEvaluate(int pk) {
        BigDecimal value;

        // 找pk对应的生成函数
        int hashCode = hashFunction(pk);
        //System.out.println(pk+" "+hashCode);

        // 根据pk和生成函数计算出值
        // jw
        value = attrGeneFuncs.get(hashCode).evaluate(pk);

        return value;
    }

    @Override
    public BigDecimal attrEvaluate2(int pk, int drivingCol) {
        BigDecimal value;
        int hashcode = hashFunction2(pk);
        // 驱动列的选择需要满足同种类型，否则这里会出现空的情况
        value = attrGeneFuncs.get(hashcode).evaluate(drivingCol);
        return value;
    }

    public AttrValue geneAttrValue(int pk){

        BigDecimal value = this.attrEvaluate(pk);
        //进行valueTransform
        switch (super.getDataType()){
            case INTEGER:
                return new AttrValue(DataType.INTEGER,value.intValue());
            case LONG:
                return new AttrValue(DataType.LONG,value.longValue());
            case FLOAT:
                return new AttrValue(DataType.FLOAT,value.floatValue());
            case DOUBLE:
                return new AttrValue(DataType.DOUBLE,value.doubleValue());
            case DECIMAL:
                // 注意：生成的测试数据在导入数据库后可能被截取
                return new AttrValue(DataType.DECIMAL,value);
            case VARCHAR:
                String transValue = "#" + value.abs().intValue() + "#" + seedStrings[value.abs().intValue() % seedStringNum]
                        + "#" + value.abs().intValue() + "#";
                return new AttrValue(DataType.VARCHAR,transValue);
            case TIMESTAMP:
                return null;
                // return sdf.format(new Date(value.longValue()))+"";
            case BOOL:
                return null;
                //return value.intValue() % 2 == 0 ? "true" : "false";
            case BLOB:
                //TODO 添加BLOB数据类型
                return null;
            default:
                return null; // 理论上不会进入该分支
        }
    }

    // decimal类型属性的生成数据可能会被截取！
    public String geneValue(int primaryKey) {
        //System.out.println("使用的表达式：" + columnGeneExpressions[hashCode]);
        BigDecimal value = this.attrEvaluate(primaryKey);

        //datatype:1: int; 2: long; 3: float; 4: double; 5: decimal; 6: varchar; 7: datetime; 8: bool
        switch (super.getDataType()) {
            case INTEGER:
                return value.intValue() + "";
            case LONG:
                return value.longValue() + "";
            case FLOAT:
                return value.toPlainString() + "";
            case DOUBLE:
                return value.toPlainString() + "";
            case DECIMAL:
                // 注意：生成的测试数据在导入数据库后可能被截取
                return value.toPlainString();
            case VARCHAR:
                return "#" + value.abs().intValue() + "#" + seedStrings[value.abs().intValue() % seedStringNum]
                        + "#" + value.abs().intValue() + "#";
            case TIMESTAMP:
                return sdf.format(new Date(value.longValue()))+"";
            case BOOL:
                return value.intValue() % 2 == 0 ? "true" : "false";
            case BLOB:
                //TODO 添加BLOB数据类型
                return null;
            default:
                return null; // 理论上不会进入该分支
        }
    }

    //update by ct
    public String geneValue4Correlation(int primaryKey){
        BigDecimal value = null;
        //分为表内和表间
        //表内：主键-->驱动列-->关联列
        if(this.drivingColumn.getTableName().equals(this.getTableName())) {
            // value0 为驱动列的值，因为驱动列是根据pk生成的
            BigDecimal value0 = this.drivingColumn.attrEvaluate(primaryKey);
            if(this.correlationFactor != 2)
                value = this.attrEvaluate(value0.intValue());
            else
                value = this.attrEvaluate2(primaryKey, value0.intValue());
        }else{//表间，两表由主外键相连，关联列所在表主键-->关联列所在表外键-->驱动列所在表主键-->驱动列-->关联列
            BigDecimal value0 = this.drivingFK.attrEvaluate(primaryKey);//关联列所在表外键 = 驱动列所在表主键
            BigDecimal value1 = this.drivingColumn.attrEvaluate(value0.intValue());//驱动列
            if(this.correlationFactor != 2)
                value = this.attrEvaluate(value1.intValue());
            else
                value = this.attrEvaluate2(primaryKey, value1.intValue());
        }

        //datatype:1: int; 2: long; 3: float; 4: double; 5: decimal; 6: varchar; 7: datetime; 8: bool
        switch (super.getDataType()) {
            case INTEGER:
                return value.intValue() + "";
            case LONG:
                return value.longValue() + "";
            case FLOAT:
                return value.toPlainString() + "";
            case DOUBLE:
                return value.toPlainString() + "";
            case DECIMAL:
                // 注意：生成的测试数据在导入数据库后可能被截取
                return value.toPlainString();
            case VARCHAR:
                return "#" + value.abs().intValue() + "#" + seedStrings[value.abs().intValue() % seedStringNum]
                        + "#" + value.abs().intValue() + "#";
            case TIMESTAMP:
                return sdf.format(new Date(value.longValue()))+"";
            case BOOL:
                return value.intValue() % 2 == 0 ? "true" : "false";
            case BLOB:
                //TODO 添加BLOB数据类型
                return null;
            default:
                return null; // 理论上不会进入该分支
        }
    }

    public String getColumnName() {
        return super.getAttName();
    }

    public DataType getDataType() {
        return super.getDataType();
    }

    public BigDecimal getLowerBound(){
        if(this.attrGeneFuncs.size()==1){
            return this.attrGeneFuncs.get(0).getMinValue(0,super.getTableSize()-1);
        }else{
            return new BigDecimal(this.lowBound);
        }
    }

    public BigDecimal getUpperBound(){
        if(this.attrGeneFuncs.size()==1){
            return this.attrGeneFuncs.get(0).getMaxValue(0,super.getTableSize()-1);
        }else{
            return this.attrGeneFuncs.get(this.attrGeneFuncs.size()-1).evaluate(1);
        }
    }

    public int getCorrelationFactor() { return this.correlationFactor;}
    public void setDrivingColumn(Column drivingColumn){ this.drivingColumn = drivingColumn; }
    public void setDrivingFK(ForeignKey drivingFK) { this.drivingFK = drivingFK; }
    public Column getDrivingColumn(){ return this.drivingColumn;}
    public ForeignKey getDrivingFK(){ return this.drivingFK; }
    public double getSkewness() { return this.skewness;}


    public String getStringDataType4CreateTable() {
        switch (super.getDataType()) {
            case TINYINT:
                return "tinyint";
            case INTEGER:
                return "int";
            case LONG:
                return "bigint";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case DECIMAL:
                return "decimal(" + decimalP + ", " + decimalS + ")";
            case VARCHAR:
                return "varchar(" + varcharLength + ")";
            case TIMESTAMP:
                return "timestamp"; // datetime TODO
            case BOOL:
                return "bool";
            case BLOB:
                return "blob";
            default:
                return null;
        }
    }

    public int getColumnGeneExpressionNum() {
        return this.attrGeneFuncs.size();
    }

    public List<AttrGeneFunc> getColumnGeneExpressions() {
        return attrGeneFuncs;
    }

    public String[] getSeedStrings() {
        return seedStrings;
    }

    public int getSeedStringNum() {
        return seedStringNum;
    }

    public void setSeedStringNum(int seedStringNum) {
        this.seedStringNum = seedStringNum;
    }

    public void setSeedStrings(String[] seedStrings) {
        this.seedStrings = seedStrings;
    }

    public int getVarcharLength() {
        return varcharLength;
    }

    public void setVarcharLength(int varcharLength) {
        this.varcharLength = varcharLength;
    }

    public void setTableName (String tableName) {super.setTableName(tableName);}

    public void setTableSize (int tableSize) {super.setTableSize(tableSize);}

    public void setAttName (String attName) {super.setAttName(attName);}

    public void setColumnGeneExpressions (List<AttrGeneFunc> attrGeneFuncs) {this.attrGeneFuncs = attrGeneFuncs;}

    public void setDataType(DataType dataType) {super.setDataType(dataType);}

    public void setCorrelationFactor(int correlationFactor) { this.correlationFactor = correlationFactor;}

    public void setDataDistributionProbability(Map<String,Map<String,Float>> dataDistributionProbability) {
        this.dataDistributionProbability = dataDistributionProbability;
    }

    public void setDrvingTableName(String drvingTableName) {
        this.drvingTableName = drvingTableName;
    }

    public void setDrvingColumnName(String drvingColumnName) {
        this.drvingColumnName = drvingColumnName;
    }

    public String getDrvingTableName() {
        return drvingTableName;
    }

    public String getDrvingColumnName() {
        return drvingColumnName;
    }

    public void setIndexList(List<Integer> indexList) {
        this.indexList = indexList;
    }

    public List<Integer> getIndexList() {
        return indexList;
    }

    public void setSkewness(double skewness) {
        this.skewness = skewness;
    }

    public List<Integer> getCorrColumn_PK_IndexList() { return this.corrColumn_PK_IndexList;}

    public void setCorrColumn_PK_IndexList(List<Integer> corrColumn_PK_IndexList) { this.corrColumn_PK_IndexList = corrColumn_PK_IndexList;}


    // 深拷贝
    @Override
    public Column clone() {
        try {
            if(super.getDataType() == DataType.DECIMAL) { // decimal
                return new Column(super.getTableName(), super.getTableSize(), super.getAttName(), dataDistributionProbability, super.getDataType(),
                        decimalP, decimalS, columnNum, columnID);
            } else if (super.getDataType() == DataType.VARCHAR) { // varchar
                return new Column(super.getTableName(), super.getTableSize(), super.getAttName(), dataDistributionProbability, super.getDataType(),
                        varcharLength, seedStringNum, columnNum, columnID);
            } else { // 除decimal和varchar外的其他数据类型
                return new Column(super.getTableName(), super.getTableSize(), super.getAttName(), dataDistributionProbability, super.getDataType(), columnNum, columnID);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null; // 理论上不会进入该分支
    }

    @Override
    public String toString() {
        return "\n\tColumn [tableName=" + super.getTableName() + ", tableSize=" + super.getTableSize() + ", columnName=" + super.getAttName()
                + ", intDataType=" + super.getDataType() +  ", decimalP="
                + decimalP + ", decimalS=" + decimalS + ", varcharLength=" + varcharLength + ", seedStringNum="
                + seedStringNum + ", columnGeneExpressions="
                + attrGeneFuncs + "]";
    }
}
