package ecnu.dbhammer.query;

import ecnu.dbhammer.data.AttrValue;
import ecnu.dbhammer.data.DataType;
import ecnu.dbhammer.query.operator.Cast;
import ecnu.dbhammer.query.operator.Concat;
import ecnu.dbhammer.query.operator.TokenGen;
import ecnu.dbhammer.query.operator.operatorToken;
import ecnu.dbhammer.query.type.OperatorType;
import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.schema.Table;
import ecnu.dbhammer.utils.GetRandomList;
import ecnu.dbhammer.utils.RandomDataGenerator;
import ecnu.dbhammer.utils.RandomIntGenerator;
import ecnu.dbhammer.utils.Randomly;
import ecnu.dbhammer.utils.ReplaceAttr;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName Select.java
 * @Description select后面的复杂投影表达式
 * @createTime 2022年01月15日 20:04:00
 */
public class SelectItems implements Select{
    private String text;

    private List<Attribute> variables;

    private List<Boolean> isWithOperatorToken = new ArrayList<>();

    private List<operatorToken> operatorTokens = new ArrayList<>();

    private List<BigDecimal> coffs = new ArrayList<>();

    private List<OperatorType> operatorTypes = new ArrayList<>();

    private List<String> isWithOperatorTokenString = new ArrayList<>();

    public SelectItems(List<Attribute> vaildVariables){
        List<Attribute> attributes = vaildVariables;

        //int randomNumber = (int)(Math.random() * attributes.size())+1;
        int randomNumber = 5;
        this.variables = GetRandomList.getRandomAttributeList(attributes,randomNumber);
        StringBuilder textBuilder = new StringBuilder();
        for(int i=0; i < this.variables.size();i++){
            Attribute attribute = this.variables.get(i);
            //对于每一个variable而言
            if(DataType.isDigit(attribute.getDataType())){
                Boolean isWithToken = Randomly.fromOptions(true,false);
                if(isWithToken){
                    //生成功能算子
                    isWithOperatorToken.add(true);
                    TokenGen tokenGen = TokenGen.getRandom();
                    if(tokenGen== TokenGen.CAST){
                        Cast cast = new Cast(attribute);
                        operatorTokens.add(cast);
                    }else if(tokenGen == TokenGen.CONCAT){
                        int concatItemNum = RandomDataGenerator.geneRandomInt(1,2);
                        Attribute[] attributes1 = new Attribute[concatItemNum];
                        for(int k=0;k<concatItemNum;k++){
                            attributes1[k] = attribute;
                        }
                        Concat concat = new Concat(attributes1);
                        operatorTokens.add((operatorToken) concat);
                    }

                }else{
                    //不生成功能算子
                    isWithOperatorToken.add(false);
                    operatorTokens.add(null);

                }

                OperatorType operatorType;
                if(operatorTokens.get(operatorTokens.size()-1) instanceof Concat){
                    operatorType = OperatorType.NULL;
                }else {
                    operatorType = OperatorType.getRandomOperator();
                }
                operatorTypes.add(operatorType);
                if(operatorType==OperatorType.NULL){
                    coffs.add(null);
                }else {
                    coffs.add(RandomDataGenerator.geneRandomDecimal(-10000, 10000));
                }
            }else{
                //非数字类型暂时不做运算
                operatorTypes.add(OperatorType.NULL);
                coffs.add(null);
            }

            if(operatorTypes.get(operatorTypes.size()-1)==OperatorType.NULL){
                if(i==this.variables.size()-1) {
                    if(isWithOperatorToken.get(i)) {
                        String text = operatorTokens.get(i).getExpressionText();
                        isWithOperatorTokenString.add(text);
                        textBuilder.append( text + " as result_" + i + " ");
                    }else{
                        isWithOperatorTokenString.add("");
                        textBuilder.append(attribute.getFullAttrName() + " as result_" + i + " ");
                    }
                }else{
                    if(isWithOperatorToken.get(i)) {
                        String text = operatorTokens.get(i).getExpressionText();
                        isWithOperatorTokenString.add(text);
                        textBuilder.append( text + " as result_" + i + ", ");
                    }else{
                        isWithOperatorTokenString.add("");
                        textBuilder.append(attribute.getFullAttrName() + " as result_" + i + ", ");
                    }
                }
            }else{
                OperatorType op = this.operatorTypes.get(this.operatorTypes.size()-1);
                BigDecimal coff = this.coffs.get(this.coffs.size()-1);
                if(i==this.variables.size()-1) {
                    if(isWithOperatorToken.get(i)) {
                        String text = operatorTokens.get(i).getExpressionText();
                        isWithOperatorTokenString.add(text);
                        textBuilder.append(text + op.getName() + coff.toString() + " as result_" + i + " ");
                    }else {
                        isWithOperatorTokenString.add("");
                        textBuilder.append(attribute.getFullAttrName() + op.getName() + coff.toString() + " as result_" + i + " ");
                    }
                }else{
                    if(isWithOperatorToken.get(i)) {
                        String text = operatorTokens.get(i).getExpressionText();
                        isWithOperatorTokenString.add(text);
                        textBuilder.append(text + op.getName() + coff.toString() + " as result_" + i + ", ");
                    }else {
                        isWithOperatorTokenString.add("");
                        textBuilder.append(attribute.getFullAttrName() + op.getName() + coff.toString() + " as result_" + i + ", ");
                    }
                }
            }
        }
        this.text = textBuilder.toString();
    }

    public SelectItems(List<Attribute> vaildVariables, Select firstSelect){
        List<Attribute> attributes = vaildVariables;
        SelectItems firstSelectItems = (SelectItems)firstSelect;


        int randomNumber = firstSelectItems.getVariables().size();

        this.variables = GetRandomList.getRandomAttributeList4Union(attributes,randomNumber, firstSelectItems.getVariables());

        this.isWithOperatorToken = firstSelectItems.getIsWithOperatorToken();
        this.coffs = firstSelectItems.getCoffs();
        this.operatorTokens = firstSelectItems.getOperatorTokens();
        this.operatorTypes = firstSelectItems.getOperatorTypes();
        this.isWithOperatorTokenString = firstSelectItems.getIsWithOperatorTokenString();

        for(int i = 0; i < this.variables.size(); i++){
            System.out.println("test---->第一个表" + firstSelectItems.variables.get(i).getFullAttrName() +  "---->第二个表" + this.variables.get(i).getFullAttrName());
            System.out.println(isWithOperatorToken.get(i) + "  " + coffs.get(i) + "  " + operatorTokens.get(i) + "  " + operatorTypes.get(i));
        }
        
        StringBuilder textBuilder = new StringBuilder();
        for(int i=0; i < this.variables.size();i++){
            Attribute attribute = this.variables.get(i);
            //对于每一个variable而言

            if(operatorTypes.get(i)==OperatorType.NULL){
                if(i==this.variables.size()-1) {
                    if(isWithOperatorToken.get(i)) {
                        String replaceText = ReplaceAttr.replaceAttr(isWithOperatorTokenString.get(i), attribute.getFullAttrName());
                        System.out.println(isWithOperatorTokenString.get(i) +"175test---->"+replaceText+ "------attr" + attribute.getFullAttrName());
                        textBuilder.append(replaceText + " as result_" + i + " ");
                    }else{
                        textBuilder.append(attribute.getFullAttrName() + " as result_" + i + " ");
                    }
                }else{
                    if(isWithOperatorToken.get(i)) {
                        String replaceText = ReplaceAttr.replaceAttr(isWithOperatorTokenString.get(i), attribute.getFullAttrName());
                        System.out.println(isWithOperatorTokenString.get(i) +"183test---->"+replaceText+ "------attr" + attribute.getFullAttrName());
                        textBuilder.append(replaceText + " as result_" + i + ", ");
                    }else{
                        textBuilder.append(attribute.getFullAttrName() + " as result_" + i + ", ");
                    }
                }
            }else{
                OperatorType op = this.operatorTypes.get(i);
                BigDecimal coff = this.coffs.get(i);
                if(i==this.variables.size()-1) {
                    if(isWithOperatorToken.get(i)) {
                        String replaceText = ReplaceAttr.replaceAttr(isWithOperatorTokenString.get(i), attribute.getFullAttrName());
                        System.out.println(isWithOperatorTokenString.get(i) +"195test---->"+replaceText+ "------attr" + attribute.getFullAttrName());
                        textBuilder.append(replaceText + op.getName() + coff.toString() + " as result_" + i + " ");
                    }else {
                        textBuilder.append(attribute.getFullAttrName() + op.getName() + coff.toString() + " as result_" + i + " ");
                    }
                }else{
                    if(isWithOperatorToken.get(i)) {
                        String replaceText = ReplaceAttr.replaceAttr(isWithOperatorTokenString.get(i), attribute.getFullAttrName());
                        System.out.println(isWithOperatorTokenString.get(i) +"203test---->"+replaceText+ "------attr" + attribute.getFullAttrName());
                        textBuilder.append(replaceText + op.getName() + coff.toString() + " as result_" + i + ", ");
                    }else {
                        textBuilder.append(attribute.getFullAttrName() + op.getName() + coff.toString() + " as result_" + i + ", ");
                    }
                }
            }
        }
        this.text = textBuilder.toString();
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

    public List<OperatorType> getOperatorTypes() {
        return operatorTypes;
    }

    public String getOracle(int k){
        return null;
    }

    public List<Boolean> getIsWithOperatorToken() {
        return isWithOperatorToken;
    }

    public List<operatorToken> getOperatorTokens() {
        return operatorTokens;
    }

    public List<String> getIsWithOperatorTokenString() {
        return isWithOperatorTokenString;
    }


    public void setIsWithOperatorTokenString(List<String> isWithOperatorTokenString) {
        this.isWithOperatorTokenString = isWithOperatorTokenString;
    }
}
