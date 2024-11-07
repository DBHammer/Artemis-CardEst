package ecnu.dbhammer.query;

import ecnu.dbhammer.query.type.AggregatioinType;

public class Having {
    Aggregation aggregation;
    double parameter;
    String operator;
    // TODO这里可以配置不同的having条件
    public Having(Aggregation aggregation) {
        this.aggregation = aggregation;
        String[] candidates = {">=","<=", ">", "<"};
        operator = candidates[(int)(Math.random() * 4 - 1)];
        parameter = Math.random() * 5 + 1;
    }
    
}
