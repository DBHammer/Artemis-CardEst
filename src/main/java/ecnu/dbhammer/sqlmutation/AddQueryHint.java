package ecnu.dbhammer.sqlmutation;

import ecnu.dbhammer.configuration.Configurations;


import java.util.Arrays;
import java.util.List;

/**
 * @author xiangzhaokun
 * @ClassName AddQueryHint.java
 * @Description QueryHint的Fuzzing，暂时未做
 * @createTime 2021年06月22日 10:33:00
 */
public class AddQueryHint {
    private List<String> joinType;

    public AddQueryHint(){
//        if (Configurations.getDatabaseBrand().equalsIgnoreCase("TiDB")) {
//            //tidb 只支持4中join type
//            String[] tidbJoinType = {"MERGE_JOIN","INL_JOIN","INL_HASH_JOIN","HASH_JOIN"};
//            joinType = Arrays.asList(tidbJoinType);
//        }
    }
}
