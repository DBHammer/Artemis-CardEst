package ecnu.dbhammer.query;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.data.DataType;
import ecnu.dbhammer.exception.NotEnoughColumnException;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.main.QueryPick;
import ecnu.dbhammer.query.type.ExpressionComplexType;
import ecnu.dbhammer.query.type.ExpressionType;
import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.schema.Column;
import ecnu.dbhammer.schema.ForeignKey;
import ecnu.dbhammer.schema.Table;
import ecnu.dbhammer.schema.genefunc.AttrGeneFunc;
import ecnu.dbhammer.schema.genefunc.LinearFunc;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName PredicateExpression.java
 * @Description 选择谓词后面的表达式
 * @createTime 2021年12月03日 17:01:00
 */
public class PredicateExpression implements Serializable {

    // 生成单属性表达式的概率，且表达式中仅含该属性，属性上不作任何运算
    private static double pureColumnProbability = Configurations.getPureColumnProbability();
    // 表达式中仅有一个属性，可以有运算，目前仅支持线性运算，如：col * 5 + 7
    private static double singleColumnProbability = Configurations.getSingleColumnProbability();
    // 表达式中可含有多个属性，同样目前仅支持线性运算，如：col_1 / 4 + col_2 * 2 - 3，目前最多3个属性一起运算
    private static double multiColumnProbability = Configurations.getMultiColumnProbability();
    //TODO repair bug为何这里配置了1 还是会生成一元公式

    // 多属性表达式中最多可含有的属性的个数，多属性表达式中的属性必须是数值类型的 因为其他类型不能进行运算
    private static int multiColumnMaxNum = 3;

    // 表达式中参与计算的立即数的小数位数
    private static int exprIdataScale = 2;

    private String text;

    private List<BigDecimal> coff;

    private List<Attribute> variables;
    // 表达式类型，0: 数值型; 6: varchar; 7: datetime; 8: bool
    private ExpressionType type;

    private ExpressionComplexType complexType;

    private Boolean allTheLinear; //如果所有属性都使用一个一次函数进行推导，那么该predicate直接用O(1)的复杂度反推

    private List<Column> cols ;//记录谓词包含的列

    public PredicateExpression(Table table, boolean isFirstOrLast) throws Exception {
        List<Attribute> allAttrbutes = new ArrayList<>();
        List<Column> columns = new ArrayList<>(table.getColumns());
        List<Column> filterColumns = new ArrayList<>();
        cols = new ArrayList<>();

        //这里只限定在普通属性上生成filter
        //update by ct: zipfian分布时，三种情况：
        // 1.只在zipfian分布的列上生成filter --> 可以用来测试数据分布的影响
        // 2.在zipfian分布的列和数据关联列上生成filter --> 可以用来测试数据分布和数据关联的影响
        // 3.随机在各个普通属性上生成filter
        int attrType = 4;//这里可以自定义
        switch (attrType) {
                case 1:
                    for (Column column : columns) {// 随机在zipfian分布的列上生成
                        if(column.getSkewness() != 0) {
                            allAttrbutes.add(column);
                            filterColumns.add(column);
                        }
                    }
                    break;
                case 2:// 随机在zipfian分布或者具有关联关系的列上生成filter
                    for (Column column : columns) {
                        if(column.getSkewness() != 0 || column.getCorrelationFactor() != 0) {
                            allAttrbutes.add(column);
                            filterColumns.add(column);
                        }
                    }
                    break;
                case 3:
                    for (Column column : columns) {
                        allAttrbutes.add(column);
                        filterColumns.add(column);
                    }
                    break;
                case 4:
                    for (Column column : columns) {
                        if(column.getDataType() == DataType.INTEGER) {
                            allAttrbutes.add(column);
                            filterColumns.add(column);
                        }
                    }
                    break;
                case 5:
                    for (Column column : columns) {
                        if(column.getDataType() == DataType.FLOAT) {
                            allAttrbutes.add(column);
                            filterColumns.add(column);
                        }
                    }
                    break;
                case 6:
                    for (Column column : columns) {
                        if(column.getDataType() == DataType.VARCHAR) {
                            allAttrbutes.add(column);
                            filterColumns.add(column);
                        }
                    }
                    break;
                
                default:
                    break;
            }
        // if(isFirstOrLast) attrType = 1; //第一张表和最后一张表的谓词不选关联列
        // if(columns.get(0).getSkewness() == 0){// 如果第一列倾斜度为0，直接随机生成
        //     for (Column column : columns) {
        //         allAttrbutes.add(column);
        //         filterColumns.add(column);
        //     }
        // }
        // else {
        //     switch (attrType) {
        //         case 1:
        //             for (Column column : columns) {// 随机在zipfian分布的列上生成
        //                 if(column.getSkewness() != 0) {
        //                     allAttrbutes.add(column);
        //                     filterColumns.add(column);
        //                 }
        //             }
        //             break;
        //         case 2:// 随机在zipfian分布或者具有关联关系的列上生成filter
        //             for (Column column : columns) {
        //                 if(column.getSkewness() != 0 || column.getCorrelationFactor() != 0) {
        //                     allAttrbutes.add(column);
        //                     filterColumns.add(column);
        //                 }
        //             }
        //             break;
        //         case 3:
        //             for (Column column : columns) {
        //                 allAttrbutes.add(column);
        //                 filterColumns.add(column);
        //             }
        //             break;
        //         case 4:
        //             for (Column column : columns) {
        //                 if(column.getDataType() == DataType.INTEGER) {
        //                     allAttrbutes.add(column);
        //                     filterColumns.add(column);
        //                 }
        //             }
        //             break;
        //         case 5:
        //             for (Column column : columns) {
        //                 if(column.getDataType() == DataType.FLOAT) {
        //                     allAttrbutes.add(column);
        //                     filterColumns.add(column);
        //                 }
        //             }
        //             break;
        //         case 6:
        //             for (Column column : columns) {
        //                 if(column.getDataType() == DataType.VARCHAR) {
        //                     allAttrbutes.add(column);
        //                     filterColumns.add(column);
        //                 }
        //             }
        //             break;
                
        //         default:
        //             break;
        //     }
        // }

        // 控制表达式中参与计算的立即数的小数位数
        String scaleRegex = "#.";
        for (int i = 0; i < exprIdataScale; i++) {
            scaleRegex += "0";
        }
        DecimalFormat df = new DecimalFormat(scaleRegex);

        double randomValue = Math.random();
        if (randomValue < pureColumnProbability + singleColumnProbability) {//一元公式
            //shuchu RecordLog.recordLog(LogLevelConstant.INFO,"本次生成的PredicateExpression只有一个属性");
            //TODO hint:随机选择列，进行where表达式的构建
            Attribute randomAttr = null;
            Column randomCol = null;
            int idx = (int) (Math.random() * allAttrbutes.size());
            randomAttr = allAttrbutes.get(idx);
            randomCol = filterColumns.get(idx);
            cols.add(randomCol);

            //TODO 这里控制不选择重复的Filter
            String randomColumnName = randomAttr.getFullAttrName();
            DataType dataType = randomAttr.getDataType();

            // 1: int; 2: long; 3: float; 4: double; 5: decimal; 6: varchar; 7: datetime; 8: bool
            if (randomValue < pureColumnProbability || !DataType.isDigit(dataType)) {
                complexType = ExpressionComplexType.PURESINGLE;
                text = randomColumnName;//只包含一个属性 而不做任何运算
                coff = new ArrayList<>(1);

                coff.add(new BigDecimal(1));

                if (!DataType.isDigit(dataType)) {
                    if(dataType==DataType.VARCHAR)
                        type = ExpressionType.VARCHAR;
                    else if(dataType==DataType.TIMESTAMP){
                        type = ExpressionType.TIME;
                    }else if(dataType==DataType.BOOL){
                        type = ExpressionType.BOOL;
                    }
                } else {
                    type = ExpressionType.DIGIT;
                }
            } else {//构造一元运算公式
                // 这里直接指定了了三个数字，分别是5、0.2、1000
                // 其中5和0.2是尽量使得表达式的计算结果不能过大，1000是为了表达式能够尽量多样化
                complexType = ExpressionComplexType.SINGLE;
                boolean isPost = Math.random() < 0.5;
                BigDecimal coff1 = new BigDecimal(df.format(Math.random() * 5 + 0.2));
                BigDecimal coff2 = new BigDecimal((Math.random() < 0.5 ? "+" : "-")+df.format(Math.random() * 1000));

                text = (isPost ? " + " : " - ") + randomColumnName + " * " + coff1.toString() + " + " +  coff2.toString();

                type = ExpressionType.DIGIT;
                coff = new ArrayList<>(2);
                coff.add(new BigDecimal((isPost?"+":"-")+coff1.toString()));
                coff.add(coff2);
            }
            variables = new ArrayList<>(1);
            variables.add(randomAttr);

            // 多属性表达式情况
        } else if (randomValue < pureColumnProbability + singleColumnProbability + multiColumnProbability) {
            // 多属性表达式中只能是数值类型属性，不同属性不能在一起运算

            complexType = ExpressionComplexType.MULTI;
            //int columnNum = (int)(Math.random() * multiColumnMaxNum) + 1;//参与运算的属性个数
            int columnNum = 3;
            List<Attribute> randomColumns = new ArrayList<>();

            int whileTimes = columnNum * 2 > 100 ? columnNum * 2 : 100;
            while (true) {
                if (randomColumns.size() == columnNum ) {
                    break;
                } else if (--whileTimes < 0) {
                    throw new NotEnoughColumnException("没有足够的数值类型的列构成表达式");
                }

                int idx = (int)(Math.random() * allAttrbutes.size());
                Attribute randomAttr = allAttrbutes.get(idx);
                Column randomCol = filterColumns.get(idx);
                DataType dataType = randomAttr.getDataType();
                if (!DataType.isDigit(dataType)) {
                    continue; // 当前属性并非数值类型
                }

                //update by ct: 默认多属性谓词中不包含数据关联的列
                if(randomCol.getCorrelationFactor() != 0)
                    continue;

                randomColumns.add(randomAttr);

                cols.add(randomCol);
            }//循环找到columnNum数量的属性列来参与构造expression

            if (randomColumns.size() == 0) {
                throw new Exception("多属性谓词表达式生成时，没找到到任何数值型属性！");
            }

            variables = new ArrayList<>(randomColumns.size());
            coff = new ArrayList<>(randomColumns.size()+1);
            text = "";
            for (int i = 0; i < randomColumns.size(); i++) {
                boolean isPost = Math.random() < 0.5;
                BigDecimal coff1 = new BigDecimal(df.format(Math.random() * 5 + 0.2));
                text = text + (isPost ? " + " : " - ") + randomColumns.get(i).getFullAttrName() + (" * ") + coff1.toString();
                variables.add(randomColumns.get(i));
                coff.add(new BigDecimal((isPost?"+":"-")+coff1.toString()));
            }
            BigDecimal coff2 = new BigDecimal((Math.random() < 0.5 ? "+" : "-")+df.format(Math.random() * 1000));
            text = text + " + " + coff2.toString();
            coff.add(coff2);
            type = ExpressionType.DIGIT;
        }

        int allLinear  = 0;

        for(int i=0;i<variables.size();i++){
            if(variables.get(i).judgeProperties()){
                allLinear++;
            }
        }

        if(allLinear == variables.size()){
            this.allTheLinear = true;
        }else{
            this.allTheLinear = false;
        }
    }

    //update by ct: 为驱动列生成谓词(一元)
    public PredicateExpression(Column column){
        cols = new ArrayList<>();
        // 控制表达式中参与计算的立即数的小数位数
        String scaleRegex = "#.";
        for (int i = 0; i < exprIdataScale; i++) {
            scaleRegex += "0";
        }
        DecimalFormat df = new DecimalFormat(scaleRegex);

        complexType = ExpressionComplexType.PURESINGLE;
        text = column.getTableName() + "." +column.getColumnName();//只包含一个属性 而不做任何运算
        coff = new ArrayList<>(1);

        coff.add(new BigDecimal(1));

        DataType dataType = column.getDataType();
        if (!DataType.isDigit(dataType)) {
            if(dataType==DataType.VARCHAR)
                type = ExpressionType.VARCHAR;
            else if(dataType==DataType.TIMESTAMP){
                type = ExpressionType.TIME;
            }else if(dataType==DataType.BOOL){
                type = ExpressionType.BOOL;
            }
        } else {
            type = ExpressionType.DIGIT;
        }

        variables = new ArrayList<>(1);
        variables.add(column);

        cols.add(column);
    }

    public String getText() {
        return text;
    }

    public List<BigDecimal> getCoff(){
        return this.coff;
    }

    public List<Attribute> getVariables() {
        return variables;
    }

    public AttrGeneFunc getComUnit() throws Exception {
        if(this.allTheLinear = true) {
            LinearFunc ans = new LinearFunc(new BigDecimal(0), new BigDecimal(0));

            List<BigDecimal> coffs = this.getCoff();

            for(int i=0;i<this.variables.size();i++){
                ans = ans.addLinearFunc(((LinearFunc) this.variables.get(i).getColumnGeneExpressions().get(0)).scale(coffs.get(i)));
            }
            if(coffs.size() == this.variables.size()+1){
                ans = ans.addLinearFunc(new LinearFunc(new BigDecimal(0),coffs.get(coffs.size() - 1)));
            }
            System.out.println(ans);
            return ans;


        }else{
            throw new Exception("该谓词涉及的属性列的生成不全是一次函数，暂时无法融合");
        }
    }

    /**
     * 给一个pk，算该过滤谓词的值
     * @return
     */
    public BigDecimal computePredicateValue(int pk){
        List<Attribute> attributes = this.getVariables();
        List<BigDecimal> coffs = this.getCoff();

        BigDecimal left = BigDecimal.ZERO;
        for(int i=0;i<attributes.size();i++){
            left = left.add(coffs.get(i).multiply(attributes.get(i).attrEvaluate(pk)));
        }
        if(coffs.size() == attributes.size()+1){
            left = left.add(coffs.get(coffs.size()-1));
        }
        return left;
    }

    public ExpressionType getType() {
        return type;
    }


    public ExpressionComplexType getComplexType(){
        return complexType;
    }

    public List<Column> getCols () {return this.cols;}


    @Override
    public String toString() {
        return "PredicateExpression [text=" + text + ", variables=" + Arrays.toString(variables.toArray()) + ", type=" + type + "]";
    }
}
