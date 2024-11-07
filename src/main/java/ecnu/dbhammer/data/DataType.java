package ecnu.dbhammer.data;

import org.apache.commons.lang3.NotImplementedException;

import java.util.Random;
/**
 * @author xiangzhaokun
 * @ClassName DataType.java
 * @Description 数据类型枚举类
 */
public enum DataType {
    TINYINT("tinyint"),
    INTEGER("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double"),
    DECIMAL("decimal"),
    VARCHAR("varchar"),
    TIMESTAMP("timestamp"),
    BOOL("bool"),
    BLOB("blob");
    private final String name;

    private DataType(String name){
        this.name = name;
    }

    public String getName(){
        return this.name;
    }

    public static boolean isDigit(DataType type){
        if(type==INTEGER || type==DOUBLE || type==DECIMAL || type==FLOAT || type == LONG){
            return true;
        }else{
            return false;
        }
    }
    public static DataType dataTypeConst2enum(int typeConst) {
        switch (typeConst) {
            case DataTypeConst.INTEGER:
                return INTEGER;
            case DataTypeConst.VARCHAR:
                return VARCHAR;
            case DataTypeConst.DOUBLE:
                return DOUBLE;
            case DataTypeConst.DECIMAL:
                return DECIMAL;
            case DataTypeConst.BLOB:
                return BLOB;
            case DataTypeConst.TIMESTAMP:
                return TIMESTAMP;
            case DataTypeConst.BOOL:
                return BOOL;
            case DataTypeConst.LONG:
                return LONG;
            case DataTypeConst.FLOAT:
                return FLOAT;

            default:
                throw new NotImplementedException();
        }
    }

    public DataType getRandomType(){
        Random random = new Random();
        return values()[random.nextInt(values().length)];
    }
}
