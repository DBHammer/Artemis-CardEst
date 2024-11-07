package ecnu.dbhammer.query.type;

/**
 * @author xiangzhaokun
 * @ClassName JoinMode.java
 * @Description 主键-外键连接、外键-外键连接、笛卡尔积
 * @createTime 2021年12月21日 16:04:00
 */
public enum JoinMode {
    PK2FK("pk-to-fk"),
    FK2FK("fk-to-fk"),
    CrossJoin("Cross");

    private String name;

    private JoinMode(String name){
        this.name = name;
    }

    public String getName(){
        return this.name;
    }
}
