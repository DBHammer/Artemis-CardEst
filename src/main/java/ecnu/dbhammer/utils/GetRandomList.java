package ecnu.dbhammer.utils;

import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.schema.Table;

import java.util.*;

/**
 * @author xiangzhaokun
 * @ClassName GetAttributeList.java
 * @Description TODO
 * @createTime 2022年01月14日 00:43:00
 */
public class GetRandomList {
    /**
     * 随机从给定的List中选择N个
     * @param attributes
     * @param randomNum
     * @return
     */
    public static List<Attribute> getRandomAttributeList(List<Attribute> attributes, int randomNum){

        List<Attribute> ans = new ArrayList<>();
        for(int i=0;i<randomNum;i++){
            ans.add(attributes.get(new Random().nextInt(attributes.size())));
        }
        return ans;
    }

    public static List<Attribute> getRandomAttributeList4Union(List<Attribute> attributes, int randomNum, List<Attribute> firstAttributes){
        List<Attribute> ans = new ArrayList<>();
        for(int i=0;i<randomNum;i++){
            while(true) {
                int index = new Random().nextInt(attributes.size());
                if(attributes.get(index).getDataType().getClass() == firstAttributes.get(i).getDataType().getClass()) {
                    ans.add(attributes.get(index));
                    break;
                } else{
                    continue;
                }
            }
        }
        return ans;
    }

    public static List<Table> getRandomTableList(List<Table> tables, int randomNum){
        List<Table> ans = new ArrayList<>();
        Collections.shuffle(tables);
        for(int i=0;i<randomNum;i++){
            ans.add(tables.get(i));
        }
        return ans;
    }
}
