package ecnu.dbhammer.query.type;

/**
 * @author xiangzhaokun
 * @ClassName JoinType.java
 * @Description mysql 不支持 full join,可以通过union 左外连接和右外连接，目前仅支持：innerJoin，fullOuterJoin，leftOuterJoin，rightOuterJoin
 * @createTime 2021年12月12日 16:16:00
 */
public enum JoinType {
    INNERJOIN("innerJoin"),
    FULLOUTERJOIN("fullOuterJoin"),
    LEFTOUTERJOIN("leftOuterJoin"),
    RIGHTOUTERJOIN("rightOuterJoin"),
    LEFTSEMIJOIN("leftSemiJoin"),
    LEFTANTIJOIN("leftAntiJoin");

    private String name;
    private JoinType(String name){
        this.name = name;
    }
    public String getName(){
        return this.name;
    }

    public static String JoinType2StrJoinOperator(JoinType joinType) {
        if (joinType == JoinType.INNERJOIN) {
            return Math.random() < 0.0 ? " inner join " :  " join ";
        } else if (joinType == JoinType.FULLOUTERJOIN) {
            return Math.random() < 0.5 ? " full join " :  " full outer join ";
        } else if (joinType == JoinType.LEFTOUTERJOIN) {
            return Math.random() < 0.5 ? " left join " :  " left outer join ";
        } else if (joinType == JoinType.RIGHTOUTERJOIN) {
            return Math.random() < 0.5 ? " right join " :  " right outer join ";
        } else if (joinType == JoinType.LEFTSEMIJOIN) {
            return " left semi join ";
        } else if (joinType == JoinType.LEFTANTIJOIN) {
            return " left anti join ";
        }
        return null; // 理论上不会返回null
    }

}
