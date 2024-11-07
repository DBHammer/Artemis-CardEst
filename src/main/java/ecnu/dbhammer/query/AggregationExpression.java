package ecnu.dbhammer.query;



import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.utils.GetRandomList;
import ecnu.dbhammer.utils.RandomIntGenerator;

import java.math.BigDecimal;
import java.util.*;

/**
 * 聚合运算表达式
 */
public class AggregationExpression {
    private String text;
    // 表达式中的符号变量，即属性标识字符串，带有前缀的
    private List<Attribute> variables = new ArrayList<>();

    private List<BigDecimal> coffs = new ArrayList<>();
    private List<Boolean> isPosts = new ArrayList<>();
    private List<Boolean> isMultiplys = new ArrayList<>();
    // 聚合操作表达式的生成。相较选择谓词中的表达式，聚合操作表达式中可涉及多个数据表的属性
    // 如果聚合操作是sum、avg、max、min，表达式仅可为数值型，而当聚合操作为count时，则无所谓

    public AggregationExpression(){
        this.text="*";
    }
    // 聚合操作中的属性可以是多张表的组合
    // todo 目前聚合操作只支持一个属性或者两个属性，改成随机生成一个复杂表达式

    public AggregationExpression(boolean isNumeric, boolean isForCount, List<Attribute> attributeList) {
        if (isNumeric) {
            //如果是数字类型的aggregation
            int num = attributeList.size();
            //int randomNum = (int) (Math.random() * num) + 1;

            int randomNum = 1;
            List<Attribute> attributes = GetRandomList.getRandomAttributeList(attributeList,randomNum);

            int idx = (int)(Math.random() * attributeList.size());
            Attribute attributeForDistinct = attributeList.get(idx);
            String randomColumnNameForDistinct = attributeForDistinct.getFullAttrName();

            double distinctRandomValue = Math.random();
            //TODO 如果有distinct，暂时不要有count
            if (distinctRandomValue < 0){
                //如果有distinct
                text = "distinct("+randomColumnNameForDistinct+")";
                variables.add(attributeForDistinct);
            }else {
                //如果没有distinct
                //查看是否为了Count，因为Count一般是计算某个列，而且count不和distinct一起用
                if (!isForCount) {
                    StringBuilder textBuilder = new StringBuilder();
                    for(int i=0;i<attributes.size();i++){
                        boolean isPost = Math.random() < 1;
                        boolean isMultiply = Math.random() < 1;
                        isPosts.add(isPost);
                        isMultiplys.add(isMultiply);
                        BigDecimal coff = new BigDecimal(1);
                        coffs.add(coff);
                        variables.add(attributes.get(i));
                        textBuilder.append((isPost ? " + " : " - ") + attributes.get(i).getFullAttrName() + (isMultiply ? " * " : " / ") + coff);
                    }
                    text = textBuilder.toString();
                } else {
                    text = randomColumnNameForDistinct;
                    variables.add(attributeForDistinct);
                }
            }



        } else { // 字符型表达式，其实就是一个字符型属性 非数字类型
            int idx = (int)(Math.random() * attributeList.size());
            Attribute attribute = attributeList.get(idx);
            String randomColumnName = attribute.getFullAttrName();

            text = randomColumnName;
            variables.add(attribute);
        }
        System.out.println(text);
    }


    public String getText() {
        return text;
    }

    public List<Attribute> getVariables() {
        return variables;
    }

    public List<BigDecimal> getCoffs() {
        return coffs;
    }

    public List<Boolean> getIsPosts() {
        return isPosts;
    }

    public List<Boolean> getIsMultiplys() {
        return isMultiplys;
    }

    @Override
    public String toString() {
        return "AggregationExpression [text=" + text  + "]";
    }

}
