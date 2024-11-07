package ecnu.dbhammer.query;

import ecnu.dbhammer.constant.FilterExpressionOperator;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.data.AttrValue;
import ecnu.dbhammer.data.DataType;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.query.type.ComparisonOperatorType;
import ecnu.dbhammer.query.type.ExpressionType;
import ecnu.dbhammer.schema.Column;
import ecnu.dbhammer.schema.Table;
import java.util.Random;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.uma.jmetal.util.bounds.Bounds;

/**
 * 过滤谓词Predicate生成，过滤谓词Predicate+表=Filter
 */
public class Predicate implements Serializable {

    // expression可以生成谓词的公式，但是不能填运算符后面充值，比如只能生成4*a+b...
    // predicate生成比较符号和值 比如>100
    // --------------- 配置参数信息 ---------------
    // 下面这三个配置参数仅针对数值型表达式
    // ">", ">=", "<", "<=" 出现的概率
    // gt: greater than; lt: less than
    private static double gtLtProbability = 1.00;
    // "between and" 出现的概率
    private static double betweenProbability = 0.00;
    // "=", "!=", "in", "not in" 出现的概率
    private static double equalInProbability = 0.00;

    // "in", "not in" 后面集合中元素的最大个数
    private static int inItemsMaxNum = 10;
    // --------------- 配置参数信息 ---------------

    // TODO 注意：目前一个表上仅生成一个谓词，即没有逻辑运算符 and, or, ()。
    private PredicateExpression expression;

    // 针对数值型表达式，可使用：">", ">=", "<", "<=", "between and", "=", "!=", "in", "not in"
    // 针对字符型表达式，可使用："=", "!=", "in", "not in", "like", "not like"
    // 针对日期类型表达式，可使用：">", ">=", "<", "<=", "between and"
    // 针对布尔型表达式，可使用："is", "is not"
    private ComparisonOperatorType comparisonOperator;

    private Table table;

    // 1: ">", 2: ">=", 3: "<", 4: "<=", 5: "between and", 6: "=", 7: "!="
    // 8: "in", 9: "not in", 10: "like", 11: "not like", 12: "is", 13: "is not"
    // 当比较运算符为"between and"时，有两个参数；当比较运算符为"in"或者"not in"时，参数可为多个~
    private AttrValue[] parameters = null;

    public void setParameters(AttrValue[] parameters) {
        this.parameters = parameters;
    }

    // 遗传算法中，针对这个谓词的初始化参数种群
    private List<AttrValue[]> initParams;

    // 每个参数对应的pk范围
    private List<int[]> pkRangesList;

    // 谓词的文本形式
    private String text = null;

    // 是否生成逻辑运算符or 这里是针对某个过滤谓词的多个判断条件 即(table1.col1 != 1 or table1.col1 != 2)
    private static boolean OrProbability = false;

    public Predicate(PredicateExpression expression,
            BigDecimal[] maxMinValue, Table table, double param) {
        this.expression = expression;
        if (expression.getType() == ExpressionType.DIGIT || expression.getType() == ExpressionType.TIME) {
            int intComparisonOperator = (int) (Math.random()
                    * FilterExpressionOperator.comparisonOperatorForNoEquality.length) + 1;
            this.comparisonOperator = ComparisonOperatorType.getTypeByName(
                    FilterExpressionOperator.comparisonOperatorForNoEquality[intComparisonOperator - 1]);
            if (comparisonOperator == ComparisonOperatorType.In
                    || comparisonOperator == ComparisonOperatorType.NotIn) {
                // "in", "not in"
                int paraNum = (int) (Math.random() * inItemsMaxNum) + 1;
                this.parameters = geneParameter(table, expression, paraNum, maxMinValue, param);
            } else if (comparisonOperator == ComparisonOperatorType.BetweenAnd) {
                // "between and"
                Random random = new Random();
                double start = random.nextDouble() * (1 - param);
                double end = start + param;
                this.parameters = new AttrValue[2];

                this.parameters[0] = geneParameter(table, expression, ComparisonOperatorType.GreaterEqual,
                        maxMinValue, start);
                this.parameters[1] = geneParameter(table, expression, ComparisonOperatorType.LessEqual,
                        maxMinValue, end);
                if (OrProbability) {
                    double start1 = random.nextDouble() * (1 - param);
                    double end1 = start + param;
                    double start2 = random.nextDouble() * (1 - param);
                    double end2 = start + param;
                    this.parameters = new AttrValue[4];
                    this.parameters[0] = geneParameter(table, expression, ComparisonOperatorType.GreaterEqual,
                            maxMinValue, start1);
                    this.parameters[1] = geneParameter(table, expression, ComparisonOperatorType.LessEqual,
                            maxMinValue, end1);
                    this.parameters[2] = geneParameter(table, expression, ComparisonOperatorType.GreaterEqual,
                            maxMinValue, start2);
                    this.parameters[3] = geneParameter(table, expression, ComparisonOperatorType.LessEqual,
                            maxMinValue, end2);
                }
            } else {
                // ">", ">=", "<", "<="
                if (comparisonOperator == ComparisonOperatorType.Greater
                        || comparisonOperator == ComparisonOperatorType.GreaterEqual) {
                    param = Math.min(param, 1 - param);
                }

                if (comparisonOperator == ComparisonOperatorType.Less
                        || comparisonOperator == ComparisonOperatorType.LessEqual) {
                    param = Math.max(param, 1 - param);
                }

                BigDecimal bigDecimalRandom = BigDecimal.valueOf(param);
                AttrValue[] parameters = new AttrValue[1];
                parameters[0] = new AttrValue(DataType.DECIMAL,
                        bigDecimalRandom.multiply(maxMinValue[0].subtract(maxMinValue[1])).add(maxMinValue[1]));

                this.parameters = parameters;
            }
        } else if (expression.getType() == ExpressionType.VARCHAR) {
            int intComparisonOperator = (int) (Math.random()
                    * FilterExpressionOperator.comparisonOperatorForEquality.length) + 1;
            this.comparisonOperator = ComparisonOperatorType
                    .getTypeByName(FilterExpressionOperator.comparisonOperatorForEquality[intComparisonOperator - 1]);
            if (comparisonOperator == ComparisonOperatorType.Equal
                    || comparisonOperator == ComparisonOperatorType.NoEqual) {
                // "=", "!="
                this.parameters = geneParameter(table, expression, 1, maxMinValue, param);
                // 添加or
                if (OrProbability) {
                    this.parameters = geneParameter(table, expression, 2, maxMinValue, param);
                }

            } else if (comparisonOperator == ComparisonOperatorType.In
                    || comparisonOperator == ComparisonOperatorType.NotIn) {
                // "in", "not in"
                int paraNum = (int) (Math.random() * inItemsMaxNum) + 1;
                this.parameters = geneParameter(table, expression, paraNum, maxMinValue, param);
            } else {
                // "like", "not like"
                AttrValue varcharParameter = geneParameter(table, expression, 1, maxMinValue, param)[0];

                String[] varcharParameterArr = ((String) varcharParameter.value).split("#", -1);
                this.parameters = new AttrValue[1];
                // 通配符可能放在左、中、右和两边，现假设四种情况出现的概率相同
                // 中间字符串相同，可能数值部分不同 数值部分相同则，整个字符串都相同
                // TODO 写到valueTransfer类中
                double tmp = Math.random();
                if (tmp < 0.25) {
                    StringBuilder stringBuilder = new StringBuilder(
                            "#%#" + varcharParameterArr[2] + "#" + varcharParameterArr[3] + "#");
                    this.parameters[0] = new AttrValue(DataType.VARCHAR, stringBuilder.toString());
                } else if (tmp < 0.5) {
                    StringBuilder stringBuilder = new StringBuilder(
                            "#" + varcharParameterArr[1] + "#%#" + varcharParameterArr[3] + "#");
                    this.parameters[0] = new AttrValue(DataType.VARCHAR, stringBuilder.toString());
                } else if (tmp < 0.75) {
                    StringBuilder stringBuilder = new StringBuilder(
                            "#" + varcharParameterArr[1] + "#" + varcharParameterArr[2] + "#%#");
                    this.parameters[0] = new AttrValue(DataType.VARCHAR, stringBuilder.toString());
                } else {
                    StringBuilder stringBuilder = new StringBuilder("%" + varcharParameter + "%");
                    this.parameters[0] = new AttrValue(DataType.VARCHAR, stringBuilder.toString());
                }
            }
        }
        constructText();
    }

    // 传入一个数据表，然后依据随机策略生成一个该表上的选择过滤谓词
    // TODO 目前只有生成第一个filter的时候用到这个函数了
    public Predicate(Table table, double param) {
        initParams = new ArrayList<>();
        pkRangesList = new ArrayList<>();
        this.table = table;
        while (true) {
            try {
                RecordLog.recordLog(LogLevelConstant.INFO, "生成一个where表达式");
                expression = new PredicateExpression(table, true);// 生成一个where后面的表达式（根据选中的表） 保证生成成功
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (expression.getType() == ExpressionType.DIGIT) { // 数值型表达式
            // 数值型表达式可选择的比较运算符有：">", ">=", "<", "<=", "between and", "=", "!=", "in", "not
            // in"
            String[] candidiate1 = { ">", ">=", "<", "<=" };
            String[] candidiate2 = { "=", "!=", "in", "not in" };

            double randomValue = Math.random();
            // System.out.println("随机数为：" + randomValue);
            if (randomValue < gtLtProbability) {// < <= > >=
                int intComparisonOperator = (int) (Math.random() * candidiate1.length);// 随机选择一个比较操作符
                comparisonOperator = ComparisonOperatorType.getTypeByName(candidiate1[intComparisonOperator]);

                parameters = new AttrValue[1];// 1个参数

                if (comparisonOperator == ComparisonOperatorType.Greater
                        || comparisonOperator == ComparisonOperatorType.GreaterEqual) {
                    param = Math.min(param, 1 - param);
                }

                if (comparisonOperator == ComparisonOperatorType.Less
                        || comparisonOperator == ComparisonOperatorType.LessEqual) {
                    param = Math.max(param, 1 - param);
                }

                parameters[0] = geneParameter(table, expression, 1, param)[0];

                Random random = new Random();
                for (int i = 0; i < FilterGenerator.POP_SIZE; i++) {
                    AttrValue[] parameter = new AttrValue[1];
                    parameter[0] = geneParameterInGene(table, expression, comparisonOperator,
                            random.nextDouble());
                    parameters = parameter;
                    initParams.add(parameter);
                }
            } else if (randomValue < gtLtProbability + betweenProbability) {
                comparisonOperator = ComparisonOperatorType.BetweenAnd;
                Random random = new Random();
                double start = random.nextDouble() * (1 - param);
                double end = start + param;
                parameters = new AttrValue[2];

                parameters[0] = geneParameter(table, expression, ComparisonOperatorType.GreaterEqual, start);
                parameters[1] = geneParameter(table, expression, ComparisonOperatorType.LessEqual, end);// 算双边谓词的参数

                // 保证双边的有效性 parameters[0] <= expression <= parameters[1]
                while (parameters[0].Compare(parameters[1], ">")) {
                    parameters[0] = geneParameter(table, expression, ComparisonOperatorType.GreaterEqual, start);
                    parameters[1] = geneParameter(table, expression, ComparisonOperatorType.LessEqual, end);
                }

                for (int i = 0; i < FilterGenerator.POP_SIZE; i++) {
                    param = random.nextDouble();
                    start = random.nextDouble() * (1 - param);
                    end = start + param;

                    AttrValue[] parameter = new AttrValue[2];
                    parameter = geneParametersInGene(table, expression, ComparisonOperatorType.BetweenAnd, start, end);
                    parameters = parameter;
                    initParams.add(parameter);
                }
            } else if (randomValue < gtLtProbability + betweenProbability + equalInProbability) {
                int intComparisonOperator = (int) (Math.random() * 4);
                comparisonOperator = ComparisonOperatorType.getTypeByName(candidiate2[intComparisonOperator]);
                if (comparisonOperator == ComparisonOperatorType.Equal
                        || comparisonOperator == ComparisonOperatorType.NoEqual) {
                    // "=", "!="
                    parameters = geneParameter(table, expression, 1, param);// = !=只能有一个参数

                    Random random = new Random();
                    for (int i = 0; i < FilterGenerator.POP_SIZE; i++) {
                        initParams.add(geneParameter(table, expression, 1, random.nextDouble()));
                    }
                } else {
                    // "in", "not in"
                    int paraNum = (int) (Math.random() * inItemsMaxNum) + 1;
                    parameters = geneParameter(table, expression, paraNum, param);// in not in为集合 可以有几个参数

                    Random random = new Random();
                    for (int i = 0; i < FilterGenerator.POP_SIZE; i++) {
                        initParams.add(geneParameter(table, expression, paraNum, random.nextDouble()));
                    }
                }
            }

        } else if (expression.getType() == ExpressionType.VARCHAR) { // 字符型表达式
            // 字符型表达式可选择的比较运算符有："=", "!=", "in", "not in", "like", "not like"
            // 假设这些比较运算符出现的概率相同
            String[] candidate = { "=", "!=", "in", "not in", "like", "not like" };
            int intComparisonOperator = (int) (Math.random() * 6) + 6;
            comparisonOperator = ComparisonOperatorType.getTypeByName(candidate[4]);// 随机确定运算符

            if (comparisonOperator == ComparisonOperatorType.Equal
                    || comparisonOperator == ComparisonOperatorType.NoEqual) {
                // "=", "!="
                parameters = geneParameter(table, expression, 1, param);
            } else if (comparisonOperator == ComparisonOperatorType.In
                    || comparisonOperator == ComparisonOperatorType.NotIn) {
                // "in", "not in"
                int paraNum = (int) (Math.random() * inItemsMaxNum) + 1;
                parameters = geneParameter(table, expression, paraNum, param);
            } else {
                // "like", "not like"
                AttrValue varcharParameter = geneParameter(table, expression, 1, param)[0];

                String[] varcharParameterArr = ((String) varcharParameter.value).split("#", -1);
                parameters = new AttrValue[1];
                // 通配符可能放在左、中、右和两边，现假设四种情况出现的概率相同
                // 中间字符串相同，可能数值部分不同 数值部分相同则，整个字符串都相同
                // TODO 写到valueTransfer类中
                double tmp = 0.8;
                if (tmp < 0.25) {
                    StringBuilder stringBuilder = new StringBuilder(
                            "#%#" + varcharParameterArr[2] + "#" + varcharParameterArr[3] + "#");
                    parameters[0] = new AttrValue(DataType.VARCHAR, stringBuilder.toString());
                } else if (tmp < 0.5) {
                    StringBuilder stringBuilder = new StringBuilder(
                            "#" + varcharParameterArr[1] + "#%#" + varcharParameterArr[3] + "#");
                    parameters[0] = new AttrValue(DataType.VARCHAR, stringBuilder.toString());
                } else if (tmp < 0.75) {
                    StringBuilder stringBuilder = new StringBuilder(
                            "#" + varcharParameterArr[1] + "#" + varcharParameterArr[2] + "#%#");
                    parameters[0] = new AttrValue(DataType.VARCHAR, stringBuilder.toString());
                } else {
                    StringBuilder stringBuilder = new StringBuilder("%" + varcharParameterArr[2] + "%");
                    parameters[0] = new AttrValue(DataType.VARCHAR, stringBuilder.toString());
                }
            }

        } else if (expression.getType() == ExpressionType.TIME) { // 日期类型
            // 日期类型表达式可选择的比较运算符有：">", ">=", "<", "<=", "between and"
            // 假设这些比较运算符出现的概率相同
            String[] candidate = { ">", ">=", "<", "<=", "between and" };
            int intComparisonOperator = (int) (Math.random() * 5) + 1;
            comparisonOperator = ComparisonOperatorType.getTypeByName(candidate[intComparisonOperator - 1]);
            if (comparisonOperator != ComparisonOperatorType.BetweenAnd) {
                // ">", ">=", "<", "<="
                parameters = new AttrValue[1];
                parameters[0] = geneParameter(table, expression, comparisonOperator, param);
            } else {
                // "between and"
                Random random = new Random();
                double start = random.nextDouble() * (1 - param);
                double end = start + param;
                parameters = new AttrValue[2];
                parameters[0] = geneParameter(table, expression, ComparisonOperatorType.GreaterEqual, start);
                parameters[1] = geneParameter(table, expression, ComparisonOperatorType.LessEqual, end);

                while (parameters[0].Compare(parameters[1], ">")) {
                    parameters[0] = geneParameter(table, expression, ComparisonOperatorType.GreaterEqual, start);
                    parameters[1] = geneParameter(table, expression, ComparisonOperatorType.LessEqual, end);
                }
            }
        } else if (expression.getType() == ExpressionType.BOOL) { // 布尔类型
            String[] candidite = { "is", "is not" };
            int intComparisonOperator = (int) (Math.random() * 2) + 1;
            comparisonOperator = ComparisonOperatorType.getTypeByName(candidite[intComparisonOperator - 1]);
            parameters = new AttrValue[1];
            parameters[0] = Math.random() < 0.5 ? new AttrValue(DataType.BOOL, true)
                    : new AttrValue(DataType.BOOL, false);
        }
        constructText();
    }

    public void setExpression(PredicateExpression expression) {
        this.expression = expression;
    }

    public void setComparisonOperator(ComparisonOperatorType comparisonOperator) {
        this.comparisonOperator = comparisonOperator;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public AttrValue[] geneParametersInGene(Table table, PredicateExpression expression,
            ComparisonOperatorType comparisonOperator, double leftParam, double rightParam) {
        AttrValue[] parameters = new AttrValue[2];
        int leftPk = (int) (leftParam * table.getTableSize() - 1);
        int rightPk = (int) (rightParam * table.getTableSize() - 1);
        BigDecimal lowBoundValue = table.CauculateCertainValueByPK(expression, 0);
        BigDecimal upperBoundValue = table.CauculateCertainValueByPK(expression, table.getTableSize() - 1);
        boolean asc = lowBoundValue.compareTo(upperBoundValue) < 0;

        if (asc) {
            pkRangesList.add(new int[] { leftPk, rightPk });
            parameters[0] = new AttrValue(DataType.DECIMAL, table.CauculateCertainValueByPK(expression, leftPk));
            parameters[1] = new AttrValue(DataType.DECIMAL, table.CauculateCertainValueByPK(expression, rightPk));
        } else {
            pkRangesList.add(new int[] { leftPk, rightPk });
            parameters[0] = new AttrValue(DataType.DECIMAL, table.CauculateCertainValueByPK(expression, rightPk));
            parameters[1] = new AttrValue(DataType.DECIMAL, table.CauculateCertainValueByPK(expression, leftPk));
        }
        return parameters;
    }

    // 遗传算法填参逻辑
    public AttrValue geneParameterInGene(Table table, PredicateExpression expression,
            ComparisonOperatorType comparisonOperator, double param) {
        if (comparisonOperator == ComparisonOperatorType.Less || comparisonOperator == ComparisonOperatorType.LessEqual
                || comparisonOperator == ComparisonOperatorType.Greater
                || comparisonOperator == ComparisonOperatorType.GreaterEqual) { // < <=

            int pk = (int) (param * table.getTableSize() - 1);
            BigDecimal lowBoundValue = table.CauculateCertainValueByPK(expression, 0);
            BigDecimal upperBoundValue = table.CauculateCertainValueByPK(expression, table.getTableSize() - 1);
            boolean asc = lowBoundValue.compareTo(upperBoundValue) < 0;
            if (comparisonOperator == ComparisonOperatorType.Less
                    || comparisonOperator == ComparisonOperatorType.LessEqual) {
                if (asc) {
                    pkRangesList.add(new int[] { 0, pk });
                } else {
                    pkRangesList.add(new int[] { pk, table.getTableSize() - 1 });
                }
            } else {
                if (asc) {
                    pkRangesList.add(new int[] { pk, table.getTableSize() - 1 });
                } else {
                    pkRangesList.add(new int[] { 0, pk });
                }
            }
            return new AttrValue(DataType.DECIMAL, table.CauculateCertainValueByPK(expression, pk));
        }
        return null;
    }

    // 1: ">", 2: ">=", 3: "<", 4: "<=", 5: "between and", 6: "=", 7: "!="
    // 8: "in", 9: "not in", 10: "like", 11: "not like", 12: "is", 13: "is not"
    // 参数生成
    public AttrValue geneParameter(Table table, PredicateExpression expression,
            ComparisonOperatorType comparisonOperator, double param) {// 非等值谓词的参数

        BigDecimal maxValue = null;
        BigDecimal minValue = null;

        if (comparisonOperator == ComparisonOperatorType.Less || comparisonOperator == ComparisonOperatorType.LessEqual
                || comparisonOperator == ComparisonOperatorType.Greater
                || comparisonOperator == ComparisonOperatorType.GreaterEqual) { // < <=
            BigDecimal[] max2min = table.calculateMaxMinValue(expression, 0,
                    table.getTableSize() - 1);

            maxValue = max2min[0].compareTo(max2min[1]) == 1 ? max2min[0] : max2min[1];
            minValue = max2min[0].compareTo(max2min[1]) == 1 ? max2min[1] : max2min[0];

            BigDecimal bigDecimalRandom = BigDecimal.valueOf(param);
            // TODO 提示： 这里控制第一个表Filter出的数据量
            // return new AttrValue(DataType.DECIMAL,maxValue.add(new
            // BigDecimal(1)));//选最大的数+1,使得所有数据都能filter出来
            // TODO 改用二分搜索
            return new AttrValue(DataType.DECIMAL,
                    bigDecimalRandom.multiply(maxValue.subtract(minValue)).add(minValue));// 随机选
        }
        return null;
    }

    public AttrValue geneParameter(Table table, PredicateExpression expression,
            ComparisonOperatorType comparisonOperator, BigDecimal[] maxMinValue, double param) {// 非等值谓词的参数

        BigDecimal maxValue = maxMinValue[0];
        BigDecimal minValue = maxMinValue[1];

        if (comparisonOperator == ComparisonOperatorType.Less
                || comparisonOperator == ComparisonOperatorType.LessEqual) { // < <=

            BigDecimal bigDecimalRandom = BigDecimal.valueOf(param);
            return new AttrValue(DataType.DECIMAL,
                    bigDecimalRandom.multiply(maxValue.subtract(minValue)).add(minValue));// 随机选

        } else if (comparisonOperator == ComparisonOperatorType.Greater
                || comparisonOperator == ComparisonOperatorType.GreaterEqual) {// >和>=

            BigDecimal bigDecimalRandom = BigDecimal.valueOf(param);
            return new AttrValue(DataType.DECIMAL,
                    bigDecimalRandom.multiply(maxValue.subtract(minValue)).add(minValue));// 随机选
        }
        return null;
    }

    public AttrValue[] geneParameter(Table table, PredicateExpression expression, int paraNum, double param) {// 等值谓词的参数
        AttrValue[] parameters = new AttrValue[paraNum];// 参数的个数
        // TODO 未处理重复
        // System.out.println("表达式类型为：" + expression.getType());
        if (expression.getType() == ExpressionType.DIGIT || expression.getType() == ExpressionType.TIME) { // 数值型表达式
            for (int i = 0; i < paraNum; i++) {
                parameters[i] = new AttrValue(DataType.DECIMAL, table.randomCauculateCertainValue(expression, param));
            }

        } else if (expression.getType() == ExpressionType.VARCHAR) { // 字符型表达式
            for (int i = 0; i < paraNum; i++) {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("#");
                BigDecimal certainValue = table.randomCauculateCertainValue(expression, param);
                stringBuilder.append(certainValue.abs().intValue());
                stringBuilder.append("#");
                // 计算出字符串 找到expression中对应的列
                String regex = "(table_\\d_\\d{1,3}.){0,1}col_\\d{1,3}";
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(expression.getText());
                String matcherString = null;
                while (matcher.find()) {
                    matcherString = matcher.group();
                }

                Column varcharColumn = (Column) table.getColumnThroughColumnName(matcherString);
                stringBuilder.append(varcharColumn.getSeedStrings()[certainValue.abs().intValue()
                        % varcharColumn.getSeedStringNum()]);
                stringBuilder.append("#");
                stringBuilder.append(certainValue.abs().intValue());
                stringBuilder.append("#");
                // TODO 记得适配varchar类型
                parameters[i] = new AttrValue(DataType.VARCHAR, stringBuilder.toString());
            }
        }
        return parameters;
    }

    public AttrValue[] geneParameter(Table table, PredicateExpression expression, int paraNum,
            BigDecimal[] maxMinValue, double param) {// 等值谓词的参数
        AttrValue[] parameters = new AttrValue[paraNum];// 参数的个数
        // TODO 未处理重复
        // System.out.println("表达式类型为：" + expression.getType());
        if (expression.getType() == ExpressionType.DIGIT || expression.getType() == ExpressionType.TIME) { // 数值型表达式
            for (int i = 0; i < paraNum; i++) {
                parameters[i] = new AttrValue(DataType.DECIMAL,
                        table.CauculateCertainValueFromRange(expression, maxMinValue, param));
            }

        } else if (expression.getType() == ExpressionType.VARCHAR) { // 字符型表达式
            for (int i = 0; i < paraNum; i++) {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("#");
                BigDecimal certainValue = table.CauculateCertainValueFromRange(expression, maxMinValue, param);
                stringBuilder.append(certainValue.abs().intValue());
                stringBuilder.append("#");
                // 计算出字符串 找到expression中对应的列
                // String[] varcharColumnName = expression.getVariables();
                String regex = "(table_\\d_\\d{1,3}.){0,1}col_\\d{1,3}";
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(expression.getText());
                String matcherString = null;
                while (matcher.find()) {
                    matcherString = matcher.group();
                }

                Column varcharColumn = (Column) table.getColumnThroughColumnName(matcherString);
                stringBuilder.append(varcharColumn.getSeedStrings()[certainValue.abs().intValue()
                        % varcharColumn.getSeedStringNum()]);
                stringBuilder.append("#");
                stringBuilder.append(certainValue.abs().intValue());
                stringBuilder.append("#");
                // TODO 记得适配varchar类型
                parameters[i] = new AttrValue(DataType.VARCHAR, stringBuilder.toString());
            }
        }
        return parameters;
    }

    public void constructText() {
        // 构造predicate表达式
        // expression生成了- table_0_4.col_4 * 2.02 + 330.50
        // predicate生成了> -11324886.87
        // 加在一起就构成了完整的predicate
        //// 1: ">", 2: ">=", 3: "<", 4: "<=", 5: "between and", 6: "=", 7: "!="
        // // 8: "in", 9: "not in", 10: "like", 11: "not like", 12: "is", 13: "is not"
        if (comparisonOperator == ComparisonOperatorType.Greater
                || comparisonOperator == ComparisonOperatorType.GreaterEqual ||
                comparisonOperator == ComparisonOperatorType.Less
                || comparisonOperator == ComparisonOperatorType.LessEqual ||
                comparisonOperator == ComparisonOperatorType.Equal
                || comparisonOperator == ComparisonOperatorType.NoEqual ||
                comparisonOperator == ComparisonOperatorType.Is || comparisonOperator == ComparisonOperatorType.IsNot) {// 单边谓词
            // TODO modify 字符型加''
            //// 1: int; 2: long; 3: float; 4: double; 5: decimal; 6: varchar; 7: datetime;
            // 8: bool
            if (expression.getType() == ExpressionType.VARCHAR) {
                text = expression.getText() + " " + comparisonOperator.getName() + " " + "'"
                        + AttrValue.tranString(parameters[0]) + "'";
                if (OrProbability) {
                    text = "(" + expression.getText() + " " + comparisonOperator.getName() + " " + "'"
                            + AttrValue.tranString(parameters[0]) + "'" + " or " +
                            expression.getText() + " " + comparisonOperator.getName() + " " + "'"
                            + AttrValue.tranString(parameters[1]) + "')";
                }
                // 如果是字符串 生成的参数要加''
            } else if (expression.getType() == ExpressionType.TIME) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
                String value = AttrValue.tranString(parameters[0]);
                BigDecimal value2 = new BigDecimal(value);
                String s = sdf.format(new Date(value2.longValue())) + "";
                text = expression.getText() + " " + comparisonOperator.getName() + " " + s;
            } else {
                text = expression.getText() + " " + comparisonOperator.getName() + " "
                        + AttrValue.tranString(parameters[0]);
            }
        } else if (comparisonOperator == ComparisonOperatorType.BetweenAnd) {
            if (expression.getType() == ExpressionType.TIME) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
                String value1 = AttrValue.tranString(parameters[0]);
                BigDecimal value11 = new BigDecimal(value1);
                String s1 = sdf.format(new Date(value11.longValue())) + "";

                String value2 = AttrValue.tranString(parameters[1]);
                BigDecimal value22 = new BigDecimal(value2);
                String s2 = sdf.format(new Date(value22.longValue())) + "";
                text = expression.getText() + " between " + s1 + " and " + s2;
                if (OrProbability) {
                    String value3 = AttrValue.tranString(parameters[2]);
                    BigDecimal value33 = new BigDecimal(value3);
                    String s3 = sdf.format(new Date(value33.longValue())) + "";

                    String value4 = AttrValue.tranString(parameters[3]);
                    BigDecimal value44 = new BigDecimal(value4);
                    String s4 = sdf.format(new Date(value22.longValue())) + "";
                    text = "(" + expression.getText() + " between " + s1 + " and " + s2 + " or " + expression.getText()
                            + " between " + s3 + " and " + s4 + ")";
                    // col1 between a and b or col1 between c and d 在生成text后，直接取最大覆盖范围，与现有求解器统一
                    AttrValue[] parametersLast = new AttrValue[2];
                    parametersLast[0] = value11.compareTo(value33) > 0 ? parameters[2] : parameters[0];
                    parametersLast[1] = value22.compareTo(value44) > 0 ? parameters[1] : parameters[3];
                    parameters = parametersLast;

                }
            } else {
                text = expression.getText() + " between " + AttrValue.tranString(parameters[0]) + " and "
                        + AttrValue.tranString(parameters[1]);
                if (OrProbability) {
                    text = "(" + expression.getText() + " between " + AttrValue.tranString(parameters[0]) + " and "
                            + AttrValue.tranString(parameters[1]) + " or " +
                            expression.getText() + " between " + AttrValue.tranString(parameters[2]) + " and "
                            + AttrValue.tranString(parameters[3]) + ")";
                    AttrValue[] parametersLast = new AttrValue[2];
                    parametersLast[0] = parameters[0].Compare(parameters[2], ">") ? parameters[2] : parameters[0];
                    parametersLast[1] = parameters[1].Compare(parameters[3], ">") ? parameters[1] : parameters[3];
                    parameters = parametersLast;
                }
            }
        } else if (comparisonOperator == ComparisonOperatorType.In
                || comparisonOperator == ComparisonOperatorType.NotIn) {
            // TODO modify in not in ()
            text = expression.getText() + " " + comparisonOperator.getName() + " (";
            for (int i = 0; i < parameters.length; i++) {
                if (expression.getType() == ExpressionType.VARCHAR) {
                    text = text + "'" + AttrValue.tranString(parameters[i]) + "'";
                } else {
                    text = text + AttrValue.tranString(parameters[i]);
                }
                if (i != parameters.length - 1) {
                    text = text + ", ";
                }
            }
            text = text + ")";
        } else if (comparisonOperator == ComparisonOperatorType.Like
                || comparisonOperator == ComparisonOperatorType.NotLike) {
            text = expression.getText() + " " + comparisonOperator.getName() + " '"
                    + AttrValue.tranString(parameters[0]) + "'";
        }
    }

    public PredicateExpression getExpression() {
        return expression;
    }

    public ComparisonOperatorType getComparisonOperator() {
        return comparisonOperator;
    }

    public AttrValue[] getParameters() {
        return parameters;
    }

    public String getText() {
        return text;
    }

    public static boolean isOrProbability() {
        return OrProbability;
    }

    public List<AttrValue[]> getInitParams() {
        return initParams;
    }

    public List<int[]> getPkRangesList() {
        return pkRangesList;
    }

    public void setPkRangesList(List<int[]> pkRangesList) {
        this.pkRangesList = pkRangesList;
    }

    public Bounds<Double> getPKRBounds(Double param) {
        BigDecimal lowBoundValue = table.CauculateCertainValueByPK(expression, 0);
        BigDecimal upperBoundValue = table.CauculateCertainValueByPK(expression, table.getTableSize() - 1);
        boolean asc = lowBoundValue.compareTo(upperBoundValue) < 0;

        Double lowBound = 0.0, upperBound = 0.0;
        if (comparisonOperator == ComparisonOperatorType.Less
                || comparisonOperator == ComparisonOperatorType.LessEqual) {
            if (asc) {
                lowBound = 0.0;
                upperBound = Double.valueOf(table.CauculatePKByParam(expression, param));
            } else {
                lowBound = Double.valueOf(table.CauculatePKByParam(expression, param));
                upperBound = Double.valueOf(table.getTableSize() - 1);
            }
        } else {
            if (asc) {
                lowBound = Double.valueOf(table.CauculatePKByParam(expression, param));
                upperBound = Double.valueOf(table.getTableSize() - 1);
            } else {
                lowBound = 0.0;
                upperBound = Double.valueOf(table.CauculatePKByParam(expression, param));
            }
        }
        return Bounds.create(lowBound, upperBound);
    }

    @Override
    public String toString() {
        return "Predicate [expression=" + expression + ", strComparisonOperator is " + comparisonOperator.getName()
                + ", parameters=" + Arrays.toString(parameters)
                + ", text=" + text + "]";
    }

}
