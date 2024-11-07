package ecnu.dbhammer.query.type;

import ecnu.dbhammer.utils.RandomDataGenerator;

/**
 * @author xiangzhaokun
 * @ClassName OperatorType.java
 * @Description TODO
 * @createTime 2022年01月15日 20:27:00
 */
public enum OperatorType {
    ADD("+"),
    SUB("-"),
    MUL("*"),
    //DIV("/"),
    MOD("%"),
    NULL("null");

    private String name;

    private OperatorType(String name){
        this.name = name;
    }

    public String getName(){
        return this.name;
    }

    public static OperatorType getRandomOperator(){
        OperatorType[] operatorTypes = OperatorType.values();
        int num = RandomDataGenerator.geneIndex(operatorTypes.length);
        return operatorTypes[num];
    }
}
