package ecnu.dbhammer.query;


import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.data.DataType;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.query.type.AggregatioinType;
import ecnu.dbhammer.query.type.ExpressionType;
import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.schema.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * 聚合运算生成
 */
public class Aggregation implements Select{

    //传入tables进来，然后统计数字类型的属性和非数字类型的属性，有数字类型表示可以生成数字类型，有非数字类型表示可以生成非数字类型
    //但是选到数字类型时
    //选到非数字类型时，只能生成count
    //count(*)都能生成
    //聚合操作
    // --------------- 配置参数信息 ---------------
    // 每种聚合操作出现的概率
    private static double sumProbability = Configurations.getSumProbability();
    private static double avgProbability = Configurations.getAvgProbability();
    private static double countProbability = Configurations.getCountProbability();
    private static double maxProbability = Configurations.getMaxProbability();
    private static double minProbability = Configurations.getMinProbability();

    //目前仅支持count(*)
    private static double countStarProbability = 1.0;//count(x)和count(*) 因为count(*)忽略null值
    // --------------- 配置参数信息 ---------------

    // 聚合操作的类型，目前支持的聚合操作有：sum, avg, count, max, min
    private AggregatioinType aggregatioinType;
    // 1: sum, 2: avg, 3: count, 4: max, 5: min

    // 表达式类型，0: 数值型; 6: varchar; 7: datetime; 8: bool
    private ExpressionType expressionType;

    // 若当前聚合操作为sum、avg、max、min，表达式仅可为数值型；若是count，则无所谓
    // 注意：当聚合操作为count时，expression可为空，表示 count(*)
    private AggregationExpression expression;

    private List<Table> tables;

    private List<Attribute> digitalAttributes;

    private List<Attribute> noDigitalAttributes;

    public Aggregation(List<Table> tables) {

        this.tables = tables;

        double randomValue = Math.random();
        if (sumProbability + avgProbability + countProbability
                + maxProbability + minProbability < 1) {
            RecordLog.recordLog(LogLevelConstant.ERROR, "5种聚合操作的概率和不为一！");
            System.exit(1);
        }

        //先计算数字类型的属性个数
        if (this.getDigitColumnNum() == 0) {
            sumProbability = 0;
            avgProbability = 0;
            countProbability = 1;
            maxProbability = 0;
            minProbability = 0;
            this.expressionType = ExpressionType.NODIGIT;
        }

        if (randomValue < sumProbability) {

            this.aggregatioinType = AggregatioinType.SUM;
            this.expression = new AggregationExpression(true, false, this.digitalAttributes);//聚合操作表达式生成 sum
        } else if (randomValue < sumProbability + avgProbability) {
            this.aggregatioinType = AggregatioinType.AVG;
            this.expression = new AggregationExpression(true, false, this.digitalAttributes);//聚合操作表达式生成 avg
        } else if (randomValue < sumProbability + avgProbability + countProbability) {
            this.aggregatioinType = AggregatioinType.COUNT;
            //count可以用到digit类型也可以用到非digit类型，分情况
            Boolean forDigit;
            if(expressionType == ExpressionType.DIGIT){
                forDigit = true;
            }else{
                forDigit = false;
            }
            if (Math.random() > countStarProbability) {
                this.expression = new AggregationExpression(forDigit, true, this.noDigitalAttributes);//聚合操作表达式生成 count() 数字类型和varchar类型
            } else {
                this.expression = new AggregationExpression();// count(*)
            }
        } else if (randomValue < sumProbability + avgProbability + countProbability
                + maxProbability) {
            this.aggregatioinType = AggregatioinType.MAX;
            this.expression = new AggregationExpression(true, false, this.digitalAttributes);
        } else if (randomValue < sumProbability + avgProbability + countProbability
                + maxProbability + minProbability) {
            this.aggregatioinType = AggregatioinType.MIN;
            this.expression = new AggregationExpression(true, false, this.digitalAttributes);
        }
    }

    public int getDigitColumnNum() {
        this.digitalAttributes = new ArrayList<>();
        this.noDigitalAttributes = new ArrayList<>();
        for (Table table : tables) {
            for (Attribute attribute : table.getAllAttrubute()) {
                if (DataType.isDigit(attribute.getDataType())) {
                    this.digitalAttributes.add(attribute);
                }else{
                    this.noDigitalAttributes.add(attribute);
                }
            }
        }
        return this.digitalAttributes.size();
    }

    public AggregatioinType getAggregationType() {
        return this.aggregatioinType;
    }

    public AggregationExpression getExpression() {
        return expression;
    }

    public List<Table> getTables() {
        return tables;
    }
    public String getText(){
        return this.expression.getText();
    }

}
