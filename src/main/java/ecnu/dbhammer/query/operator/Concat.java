package ecnu.dbhammer.query.operator;

import ecnu.dbhammer.schema.Attribute;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName Concat.java
 * @Description Concat算子，用于字符串拼接
 */
public class Concat implements operatorToken{
    private List<Attribute> items;

    public Concat(Attribute... attributes){
        this.items = new ArrayList<>();
        this.items.addAll(Arrays.asList(attributes));

    }
    public String getOracle(int k){
        StringBuilder stringBuilder = new StringBuilder();
        for(Attribute attribute : items){
            stringBuilder.append(attribute.attrEvaluate(k).toString());
        }
        return stringBuilder.toString();
    }

    @Override
    public String getExpressionText() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("concat(");
        for(int i=0;i<items.size();i++){
            stringBuilder.append(items.get(i).getFullAttrName());
            if(i!=items.size()-1){
                stringBuilder.append(",");
            }
        }
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    @Override
    public String getResult(BigDecimal bigDecimal) {
        StringBuilder stringBuilder = new StringBuilder();
        for(int i=0;i<items.size();i++){
            stringBuilder.append(bigDecimal.toString());
        }
        return stringBuilder.toString();
    }
}
