package ecnu.dbhammer.query;



import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.schema.Table;


import java.util.LinkedHashMap;

//适配器模式
//接口不定义{}
//抽象类实现接口，但是方法体为{}
//其余类再继承抽象类实现想实现的方法
public class QueryNode {

    // 0: Table 1: Filter; 2: Join
    //public int nodeType;
    //querynode的类型 filter Join Table
    //table 0 filter 1 join 2

    public LinkedHashMap<Attribute,Attribute> getJoinRelations(){
        return null;
    }

    public Table getTable(){
        return null;
    };

    public String getName(){
        return null;
    }


}
