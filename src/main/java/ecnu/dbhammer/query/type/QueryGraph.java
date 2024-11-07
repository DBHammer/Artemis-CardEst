package ecnu.dbhammer.query.type;

import java.util.Random;

/**
 * @author xiangzhaokun
 * @ClassName QueryGraph.java
 * @Description 连接图，现在7种连接图已经全部实现
 * @createTime 2021年12月27日 20:58:00
 */
public enum QueryGraph {
    STAR("star"),
    CHAIN("chain"),
    TREE("tree"),
    CYCLE("cycle"),
    CYCLIC("cyclic"),
    CLIQUE("clique"),
    GRID("grid");
    private final String name;

    private QueryGraph(String name){
        this.name = name;
    }

    public String getName(){
        return this.name;
    }

    public static QueryGraph getRandom(){
        int sum = QueryGraph.values().length;
        int index = (int)(Math.random() * sum);
        return QueryGraph.values()[index];
    }
}
