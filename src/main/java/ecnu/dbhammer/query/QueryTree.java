package ecnu.dbhammer.query;

import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.data.DataType;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.query.type.AggregatioinType;
import ecnu.dbhammer.query.type.JoinType;
import ecnu.dbhammer.query.type.OrderByType;
import ecnu.dbhammer.query.type.QueryGraph;
import ecnu.dbhammer.result.FilterResult;
import ecnu.dbhammer.result.JoinResult;
import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.schema.Column;
import ecnu.dbhammer.schema.PrimaryKey;
import ecnu.dbhammer.schema.Table;
import ecnu.dbhammer.solver.JoinCondition;
import org.jgrapht.graph.Pseudograph;

import java.util.*;

/**
 * QueryTree类
 */
public class QueryTree {

    private QueryNode root;
    private Select select;
    private List<GroupBy> groupBys;
    private List<OrderBy> orderBys;
    private Having having;
    private List<Filter> filters;

    private String noStardardPredicate;
    private QueryGraph queryGraph;
    private Pseudograph<String, JoinCondition> queryConditionGraph = new Pseudograph<>(JoinCondition.class);


    //Union的QueryTree
    private List<QueryTree> unionSet;
    private List<List<Table>> unionTables;

    // TODO加入子查询，另外一个QueryTree
    private List<QueryTree> ss;

    private List<String> joinedTableOrder;
    //在ResultGenerator中赋值joinRelations
    public List<Integer> middleResultCount;

    private List<Table> tables;//querytree中的表

    private String sqlText;

    private List<Table> cardOptimialJoinOrder;


    private FilterResult[] filterResults;
    private JoinResult[] joinResults;
    private List<List<Integer>> finalKey4Join;

    private int finalResultSize;

    private String innerJoinPredicate = null;

    private Table subTable;

    private String lastSqlText; //在子查询树中，保存上一层的sql语句

    public QueryTree(QueryNode root, List<Integer> joinCardinality, Select select, List<GroupBy> groupBys,
                     List<OrderBy> orderBys, Having having, List<Table> tables, List<Filter> filters, String noStardardPredicate,QueryGraph queryGraph, Pseudograph<String, JoinCondition> queryConditionGraph,
                     List<Table> optimialOrder, FilterResult[] filterResults, JoinResult[] joinResults, List<List<Integer>> finalKey4Join) {
        super();
        this.root = root;
        this.select = select;
        this.groupBys = groupBys;
        this.orderBys = orderBys;
        this.having = having;
        this.tables = tables;
        this.filters = filters;
        this.middleResultCount = joinCardinality;
        this.sqlText = null;
        this.cardOptimialJoinOrder = optimialOrder;
        this.queryGraph = queryGraph;
        this.queryConditionGraph = queryConditionGraph;
        this.filterResults = filterResults;
        this.joinResults = joinResults;
        this.finalKey4Join = finalKey4Join;
        this.noStardardPredicate = noStardardPredicate;

    }

    public QueryTree(String sqlText, List<Table> tables){
        super();
        this.sqlText = sqlText;
        this.tables = tables;
    }


    public String getSqlText() {
        if (this.sqlText == null) {
            System.out.println("filter = " + filters.toString());
            this.sqlText = geneSQLJoinConInWhere();

        }
        return this.sqlText;
    }

    public String geneSQLJoinConInWhere() {
        StringBuilder ans = new StringBuilder("select ");
        if (this.select instanceof Aggregation) {
            // 1: sum, 2: avg, 3: count, 4: max, 5: min
            Aggregation aggregation = (Aggregation) this.select;
            if (aggregation.getAggregationType() == AggregatioinType.SUM) {
                ans.append(" sum(" + aggregation.getExpression().getText() + ") ");
            } else if (aggregation.getAggregationType() == AggregatioinType.AVG) {
                ans.append(" avg(" + aggregation.getExpression().getText() + ") ");
            } else if (aggregation.getAggregationType() == AggregatioinType.COUNT) {
                if (aggregation.getExpression() != null) {
                    ans.append(" count(" + aggregation.getExpression().getText() + ") ");
                } else {//Count的情况，如果CountStarPro为1，aggregations.get(i).getExpression()为NULL，生成Count(*)
                    ans.append(" count(*) ");
                }
            } else if (aggregation.getAggregationType() == AggregatioinType.MAX) {
                ans.append(" max(" + aggregation.getExpression().getText() + ") ");
            } else if (aggregation.getAggregationType() == AggregatioinType.MIN) {
                ans.append(" min(" + aggregation.getExpression().getText() + ") ");
            }
            ans.append("as result");
        } else if (this.select instanceof SelectItems) {
            SelectItems selectItems = (SelectItems) this.select;
            ans.append(selectItems.getText());
        }
        //构造select后面的操作
        ans.append(" from ");

        if(this.lastSqlText != null){
            ans.append("(" + this.lastSqlText + ")" + "as subTable");
            return ans.toString();
        } else{
            ans.append(geneFromSQL());
        }
        if(innerJoinPredicate != null) {
            ans.append(" where ");
           ans.append(innerJoinPredicate);
        } else {
            ans.append(" where ");
        }



        //先添加filter
        int filterSize = 0;
        if(filters != null){
            filterSize = filters.size();
        }
        for (int i = 0; i < filterSize; i++) {
            //update by ct: 一张表上可能有多个过滤谓词
            List<Predicate> predicateList = filters.get(i).getPredicateList();
            for(int j = 0; j < predicateList.size(); j++){
                ans.append(predicateList.get(j).getText());
                if(j == predicateList.size() -1)
                    ans.append(" ");
                else
                    ans.append(" and ");
            }

            if (i == filterSize - 1) {
                ans.append(" ");
            } else {
                ans.append(" and ");
            }
        }

        //再添加非标准过滤谓词
        if(noStardardPredicate!=null){
            ans.append("and "+noStardardPredicate);
        }

        StringBuilder groupBySql = new StringBuilder(" ");
        if (groupBys.size() > 0) {
            groupBySql.append(" group by ");
            for (int i = 0; i < groupBys.size(); i++) {
                groupBySql.append(groupBys.get(i).getColumnName().getFullAttrName());
                if (i != (groupBys.size() - 1)) {
                    groupBySql.append(", ");
                }
            }
        }//生成groupby语句

        StringBuilder havingSql = new StringBuilder(" ");
        if(having != null) {
            havingSql.append("having ");
            if (having.aggregation.getAggregationType() == AggregatioinType.SUM) {
                havingSql.append(" sum(" + having.aggregation.getExpression().getText() + ") ");
            } else if (having.aggregation.getAggregationType() == AggregatioinType.AVG) {
                havingSql.append(" avg(" + having.aggregation.getExpression().getText() + ") ");
            } else if (having.aggregation.getAggregationType() == AggregatioinType.COUNT) {
                if (having.aggregation.getExpression() != null) {
                    havingSql.append(" count(" + having.aggregation.getExpression().getText() + ") ");
                } else {//Count的情况，如果CountStarPro为1，aggregations.get(i).getExpression()为NULL，生成Count(*)
                    havingSql.append(" count(*) ");
                }
            } else if (having.aggregation.getAggregationType() == AggregatioinType.MAX) {
                havingSql.append(" max(" + having.aggregation.getExpression().getText() + ") ");
            } else if (having.aggregation.getAggregationType() == AggregatioinType.MIN) {
                havingSql.append(" min(" + having.aggregation.getExpression().getText() + ") ");
            }
            havingSql.append(having.operator + " ");
            if(having.aggregation.getAggregationType() == AggregatioinType.COUNT) {
                havingSql.append((int)having.parameter);
            } else {
               havingSql.append(having.parameter);
            }
        }//生成having语句

        StringBuilder orderBySql = new StringBuilder(" ");
        if (orderBys != null && orderBys.size() > 0) {
            groupBySql.append(" order by ");
            for (int i = 0; i < orderBys.size(); i++) {
                if (orderBys.get(i).getOrderByType() == OrderByType.DESC) {
                    orderBySql.append(orderBys.get(i).getAttributeName());
                    orderBySql.append(" desc ");
                } else {
                    orderBySql.append(orderBys.get(i).getAttributeName());
                    orderBySql.append(" asc ");
                }
                if (i != (orderBys.size() - 1)) {
                    orderBySql.append(", ");
                }
            }
        }//生成orderby语句

        //根据概率生成limit 这里可以控制
        StringBuilder limitSql = new StringBuilder("");
        int limitNum = 0;
        if(Math.random() < 0) {
            if(finalResultSize > 0 && this.select instanceof SelectItems) {
                limitNum = (int)(Math.random() * finalResultSize) + 1;
                limitSql.append(" limit " + limitNum);
            }
        }

        this.sqlText = ans.toString() + groupBySql.toString() + havingSql.toString() + orderBySql.toString() + limitSql.toString();

        if(this.unionSet != null) {
            for (int i = 0; i < this.unionSet.size(); i++) {
                QueryTree unionQueryTree = unionSet.get(i);
                String unionText = unionQueryTree.geneSQLJoinConInWhere();
                this.sqlText += " union all " + unionText;
            }
        }

        return sqlText;

    }

    public String geneCardOptimialJoinOrderQuery(String dbMSBrand) {

        StringBuilder ans = new StringBuilder("select ");
        if (dbMSBrand.equalsIgnoreCase("tidb") || dbMSBrand.equalsIgnoreCase("mysql")) {
            ans.append("/*! STRAIGHT_JOIN */");
        } else if (dbMSBrand.equalsIgnoreCase("oceanbase")) {
            StringBuilder joinOrderString = new StringBuilder();
            for (int i = 0; i < this.cardOptimialJoinOrder.size(); i++) {
                if (i != this.cardOptimialJoinOrder.size() - 1) {
                    joinOrderString.append(this.cardOptimialJoinOrder.get(i).getTableName() + ",");
                } else {
                    joinOrderString.append(this.cardOptimialJoinOrder.get(i).getTableName());
                }
            }
            ans.append("/*+LEADING("+joinOrderString.toString()+")*/");
        }
        if (this.select instanceof Aggregation) {
            // 1: sum, 2: avg, 3: count, 4: max, 5: min
            Aggregation aggregation = (Aggregation) this.select;
            if (aggregation.getAggregationType() == AggregatioinType.SUM) {
                ans.append(" sum(" + aggregation.getExpression().getText() + ") ");
            } else if (aggregation.getAggregationType() == AggregatioinType.AVG) {
                ans.append(" avg(" + aggregation.getExpression().getText() + ") ");
            } else if (aggregation.getAggregationType() == AggregatioinType.COUNT) {
                if (aggregation.getExpression() != null) {
                    ans.append(" count(" + aggregation.getExpression().getText() + ") ");
                } else {//Count的情况，如果CountStarPro为1，aggregations.get(i).getExpression()为NULL，生成Count(*)
                    ans.append(" count(*) ");
                }
            } else if (aggregation.getAggregationType() == AggregatioinType.MAX) {
                ans.append(" max(" + aggregation.getExpression().getText() + ") ");
            } else if (aggregation.getAggregationType() == AggregatioinType.MIN) {
                ans.append(" min(" + aggregation.getExpression().getText() + ") ");
            }
            ans.append("as result");
        } else if (this.select instanceof SelectItems) {
            SelectItems selectItems = (SelectItems) this.select;
            ans.append(selectItems.getText());
        }
        //构造select后面的操作
        ans.append(" from ");
        // for (int i = 0; i < cardOptimialJoinOrder.size(); i++) {
        //     ans.append(cardOptimialJoinOrder.get(i).getTableName());
        //     if (i == cardOptimialJoinOrder.size() - 1) {
        //         ans.append(" ");
        //     } else {
        //         ans.append(", ");
        //     }
        // }
        ans.append(this.lastSqlText);
        ans.append(geneFromSQL());
        ans.append(" where ");
        //先添加filter
        for (int i = 0; i < filters.size(); i++) {
            //update by ct: 一张表上可能有多个过滤谓词
            List<Predicate> predicateList = filters.get(i).getPredicateList();
            for(int j = 0; j < predicateList.size(); j++){
                ans.append(predicateList.get(j).getText());
                if(j == predicateList.size() -1)
                    ans.append(" ");
                else
                    ans.append(" and ");
            }
            if (i == filters.size() - 1) {
                ans.append(" ");
            } else {
                ans.append(" and ");
            }
        }
        //再添加join条件
        //joinRelation不能为Null，为null表示没有Join
        LinkedHashMap<Attribute, Attribute> joinRelations = this.root.getJoinRelations();
        if (joinRelations != null) {
            if (joinRelations.size() != 0) {
                ans.append("and ");
            }
            int cnt = 0;
            for (Attribute key : joinRelations.keySet()) {
                cnt++;
                Attribute value = joinRelations.get(key);
                ans.append(key.getFullAttrName() + " = " + value.getFullAttrName());
                if (cnt == joinRelations.size()) {
                    ans.append(" ");
                } else {
                    ans.append(" and ");
                }
            }
        }
        StringBuilder groupBySql = new StringBuilder(" ");
        if (groupBys.size() > 0) {
            groupBySql.append(" group by ");
            for (int i = 0; i < groupBys.size(); i++) {
                groupBySql.append(groupBys.get(i).getColumnName().getFullAttrName());
                if (i != (groupBys.size() - 1)) {
                    groupBySql.append(",");
                }
            }
        }//生成groupby语句

        StringBuilder orderBySql = new StringBuilder(" ");
        if (orderBys.size() > 0) {
            groupBySql.append(" order by ");
            for (int i = 0; i < orderBys.size(); i++) {
                if (orderBys.get(i).getOrderByType() == OrderByType.DESC) {
                    orderBySql.append(orderBys.get(i).getAttributeName());
                    orderBySql.append(" desc");
                } else {
                    orderBySql.append(orderBys.get(i).getAttributeName());
                    orderBySql.append(" asc");
                }
                if (i != (orderBys.size() - 1)) {
                    orderBySql.append(",");
                }
            }
        }//生成orderby语句
        return ans.toString() + groupBySql.toString() + orderBySql.toString();

    }


    public String geneFromSQL() {
        StringBuilder fromSql = new StringBuilder("");
        StringBuilder innerJoinPredicateSql = new StringBuilder("");
        List<QueryNode> allQueryNodes = new ArrayList<>();


        // 通过深度优先遍历，拿到所有的query node
        deepTraverseQueryTree(root, allQueryNodes);
        Collections.reverse(allQueryNodes);
        // Query Tree中所有出现的数据表
        Set<String> allTables = new HashSet<>();
        // 在from子句中，定义连接操作时已声明的数据表
        Set<String> fromJoinedTables = new HashSet<>();


        for (QueryNode queryNode : allQueryNodes) {
            if (queryNode instanceof Join) {
                Join join = (Join) queryNode;
                String leftTableName = join.getLeftAttribute().getTableName();
                String rightTableName = join.getRightAttribute().getTableName();
                String strJoinOperator = JoinType.JoinType2StrJoinOperator(join.getJoinType());
                if(join.getJoinType().name() != "INNERJOIN") {
                    if (fromSql.length() == 0) { // from中第一个连接操作
                        fromSql.append(leftTableName + strJoinOperator + rightTableName
                                + " on (" + join.getLeftAttribute().getFullAttrName() + " = " + join.getRightAttribute().getFullAttrName() + ")");
                    } else { // from中非第一个连接操作
                        fromSql.append(strJoinOperator + rightTableName
                                + " on (" + join.getLeftAttribute().getFullAttrName() + " = " + join.getRightAttribute().getFullAttrName() + ")");
                    }
                    fromJoinedTables.add(leftTableName);
                    fromJoinedTables.add(rightTableName);//from后面的所有表
                } else {
                    innerJoinPredicateSql.append(join.getLeftAttribute().getFullAttrName() + " = " + join.getRightAttribute().getFullAttrName() + " and ");
                }
                allTables.add(leftTableName);
                allTables.add(rightTableName);
            } else if (queryNode instanceof TableNode) {
                TableNode tableNode = (TableNode) queryNode;
                allTables.add(tableNode.getTable().getTableName());
            } else if (queryNode instanceof Filter) {//所有的filter都能写到sql后面
                Filter filter = (Filter) queryNode;
                allTables.add(filter.getTable().getTableName());
            }
        }
        if(innerJoinPredicateSql.length() != 0) {
            innerJoinPredicateSql.substring(0, innerJoinPredicateSql.length() - 5);
            innerJoinPredicate = innerJoinPredicateSql.toString();
        }

        allTables.removeAll(fromJoinedTables);
        Iterator<String> iter = allTables.iterator();
        while (iter.hasNext()) {
            if (fromSql.length() == 0) {
                fromSql.append(iter.next());
            } else {
                fromSql.append(", " + iter.next());
            }
        }
        return fromSql.toString();
    }


    //todo querytree = querynode+aggregations的list+groupby的list+orderby的list+table的list
    //
    // 将Query Tree转化成一条可执行的SQL语句
    // 暂且假设只有选择操作、连接操作和聚合操作
    // 支持的连接操作有：innerJoin，fullOuterJoin，leftOuterJoin，rightOuterJoin
    // innerJoin的连接条件即可以写在from中，也可以写作where中；而所有外连接的连接条件只能写在from中
    // 假设：任一数据表都只会在所有选择操作出现一次，对于所有连接操作也是如此~
    public String geneSqlInString() {
        StringBuilder ans = new StringBuilder("select ");
        if (this.select instanceof Aggregation) {
            // 1: sum, 2: avg, 3: count, 4: max, 5: min
            Aggregation aggregation = (Aggregation) this.select;
            if (aggregation.getAggregationType() == AggregatioinType.SUM) {
                ans.append(" sum(" + aggregation.getExpression().getText() + ") ");
            } else if (aggregation.getAggregationType() == AggregatioinType.AVG) {
                ans.append(" avg(" + aggregation.getExpression().getText() + ") ");
            } else if (aggregation.getAggregationType() == AggregatioinType.COUNT) {
                if (aggregation.getExpression() != null) {
                    ans.append(" count(" + aggregation.getExpression().getText() + ") ");
                } else {//Count的情况，如果CountStarPro为1，aggregations.get(i).getExpression()为NULL，生成Count(*)
                    ans.append(" count(*) ");
                }
            } else if (aggregation.getAggregationType() == AggregatioinType.MAX) {
                ans.append(" max(" + aggregation.getExpression().getText() + ") ");
            } else if (aggregation.getAggregationType() == AggregatioinType.MIN) {
                ans.append(" min(" + aggregation.getExpression().getText() + ") ");
            }
            ans.append("as result");
        } else if (this.select instanceof SelectItems) {
            SelectItems selectItems = (SelectItems) this.select;
            ans.append(selectItems.getText());
        }

        StringBuilder fromSql = new StringBuilder(" from ");
        StringBuilder whereSql = new StringBuilder(" where ");

        List<QueryNode> allQueryNodes = new ArrayList<>();
        // 通过深度优先遍历，拿到所有的query node
        deepTraverseQueryTree(root, allQueryNodes);

        // 因为当前是深度优先遍历，allQueryNodes中所有操作处于逆序状态（比如group by在join前面，join在filter前面）
        // 故这里需要逆序处理一下，这样方便后面SQL的生成，比如确保from中连接序是合理的~
        Collections.reverse(allQueryNodes);

        RecordLog.recordLog(LogLevelConstant.INFO, "QueryTree的遍历如下");
        for (QueryNode queryNode : allQueryNodes) {
            if (queryNode instanceof Filter)
                RecordLog.recordLog(LogLevelConstant.INFO, queryNode.getTable().getTableName() +" : "+ queryNode.getTable().getTableSize());
        }

        // Query Tree中所有出现的数据表
        Set<String> allTables = new HashSet<>();
        // 在from子句中，定义连接操作时已声明的数据表
        Set<String> fromJoinedTables = new HashSet<>();

        boolean filterFlag = false;
        for (QueryNode queryNode : allQueryNodes) {
            if (queryNode instanceof TableNode) {
                TableNode tableNode = (TableNode) queryNode;
                allTables.add(tableNode.getTable().getTableName());
            }
            if (queryNode instanceof Filter) {//所有的filter都能写到sql后面
                filterFlag = true;
                Filter filter = (Filter) queryNode;
                if (whereSql.length() == 7) { // 第一个where条件,因为目前只有" where "
                    whereSql.append(filter.getPredicate().getText());
                } else { // 非第一个where条件         TODO   暂时不考虑选择条件之间使用OR进行连接
                    whereSql.append(" and " + filter.getPredicate().getText());
                }
                allTables.add(filter.getTable().getTableName());

            } else if (queryNode instanceof Join) {
                Join join = (Join) queryNode;
                String leftTableName = join.getLeftAttribute().getTableName();
                String rightTableName = join.getRightAttribute().getTableName();

                String strJoinOperator = JoinType.JoinType2StrJoinOperator(join.getJoinType());

                // inner join既可以在from中定义，也可以在where中定义，这里假设两者概率相同，随机确定~
                // TODO modify 连接写法有问题  导致语法错误
//				if (join.getIntJoinType() == 1 && Math.random() < 0.5) {
//					// 这里不能使用leftColumnName和rightColumnName，属性名可能不唯一
//					String joinCondition = join.getLeftJoinColumnName() + " = " + join.getRightJoinColumnName();
//					if (whereSql.length() == 7) {
//						whereSql.append(joinCondition);
//					} else {
//						whereSql.append(" and " + joinCondition);
//					}
//
//				// inner join（50%），fullOuterJoin，leftOuterJoin，rightOuterJoin
//				} else {
                // TODO 列名需要使用全称吗
                if (fromSql.length() == 6) { // from中第一个连接操作
                    fromSql.append(leftTableName + strJoinOperator + rightTableName
                            + " on " + join.getLeftAttribute().getFullAttrName() + " = " + join.getRightAttribute().getFullAttrName());
                } else { // from中非第一个连接操作
                    fromSql.append(strJoinOperator + rightTableName
                            + " on " + join.getLeftAttribute().getFullAttrName() + " = " + join.getRightAttribute().getFullAttrName());
                }
                fromJoinedTables.add(leftTableName);
                fromJoinedTables.add(rightTableName);//from后面的所有表
//				}
                // 可能没有必要
                allTables.add(leftTableName);
                allTables.add(rightTableName);
            }else if (queryNode instanceof ComplexJoin){ //TODO 增加这个分支

            }
        }

        allTables.removeAll(fromJoinedTables);
        Iterator<String> iter = allTables.iterator();
        while (iter.hasNext()) {
            if (fromSql.length() == 6) {
                fromSql.append(iter.next());
            } else {
                fromSql.append(", " + iter.next());
            }
        }

        StringBuilder groupBySql = new StringBuilder(" ");
        if (groupBys.size() > 0) {
            groupBySql.append(" group by ");
            for (int i = 0; i < groupBys.size(); i++) {
                groupBySql.append(groupBys.get(i).getColumnName().getFullAttrName());
                if (i != (groupBys.size() - 1)) {
                    groupBySql.append(",");
                }
            }
        }//生成groupby语句

        StringBuilder orderBySql = new StringBuilder(" ");
        if (orderBys.size() > 0) {
            groupBySql.append(" order by ");
            for (int i = 0; i < orderBys.size(); i++) {
                if (orderBys.get(i).getOrderByType() == OrderByType.DESC) {
                    orderBySql.append(orderBys.get(i).getAttributeName());
                    orderBySql.append(" desc");
                } else {
                    orderBySql.append(orderBys.get(i).getAttributeName());
                    orderBySql.append(" asc");
                }
                if (i != (orderBys.size() - 1)) {
                    orderBySql.append(",");
                }
            }
        }

        if (!filterFlag)
            whereSql = new StringBuilder();
        return ans.toString() + fromSql.toString() + whereSql.toString() + groupBySql.toString() + orderBySql.toString();//构造query
    }

    //从根节点开始dfs
    public void deepTraverseQueryTree(QueryNode queryNode, List<QueryNode> allQueryNodes) {
        allQueryNodes.add(queryNode);

        // 目前只有选择和连接操作
        // todo 加入到table才return
        if (queryNode instanceof Filter) {//filter
            return;
        } else if (queryNode instanceof Join) {//join
            Join join = (Join) queryNode;//向下转型
            deepTraverseQueryTree(join.getLeftChildNode(), allQueryNodes);
            deepTraverseQueryTree(join.getRightChildNode(), allQueryNodes);
        } else if (queryNode instanceof ComplexJoin) {
            ComplexJoin complexJoin = (ComplexJoin) queryNode;
            for (int i = 0; i < complexJoin.getJoins().size(); i++) {
                Join join = (Join) complexJoin.getJoins().get(i);
                System.out.println(join.getJoinConditionList().toString());
                // deepTraverseQueryTree(join.getLeftChildNode(), allQueryNodes);
                // deepTraverseQueryTree(join.getRightChildNode(), allQueryNodes);
            }
        }
    }


    //update by ct
    public List<String> getTableOrder() {
        List<String> tableNameOrder = new LinkedList<>();
        for(Table table : tables){
            tableNameOrder.add(table.getTableName());
        }
        return tableNameOrder;
    }

    public Table getMaxTableInQuery(){
        int pivot = 0;
        Table ans = null;
        for(Table table : this.tables){
            if(table.getTableSize()>pivot){
                pivot = table.getTableSize();
                ans = table;
            }
        }
        return ans;
    }

    //根据queryTree构建一个子查询表，用于外层查询的生成
    public void generateSubTable() throws Exception {
        int tableIndex = tables.size();
        String tableName = "subTable";
        int tableSize = this.finalResultSize;
        PrimaryKey primaryKey = new PrimaryKey(tableName,tableSize,"primaryKey");
        List<Column> columnList = new ArrayList<>();
        if (this.select instanceof Aggregation) {
            Aggregation aggregation = (Aggregation) this.select;
            columnList.add(new Column(tableName,tableSize,"result",null, DataType.DECIMAL,1,0));
        }
        else if(this.select instanceof SelectItems) {
            SelectItems selectItems = (SelectItems) this.select;
            List<Attribute> attributes = selectItems.getVariables();
            for(int i = 0;i<attributes.size();i++){
                Column column = new Column(tableName,tableSize,"result_"+i,null,attributes.get(i).getDataType(),attributes.size(),i);
                columnList.add(column);
            }
        }
        this.subTable = new Table(tableIndex,tableName,tableSize,primaryKey,null,columnList,null);


    }


    public QueryNode getRoot() {
        return root;
    }

    public Select getSelect() {
        return this.select;
    }

    public List<GroupBy> getGroupBys() {
        return groupBys;
    }

    public List<OrderBy> getOrderBys() {
        return orderBys;
    }

    public List<Table> getTables() {
        return tables;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public List<Table> getCardOptimialJoinOrder() {
        return this.cardOptimialJoinOrder;
    }

    public FilterResult[] getFilterResults() {
        return filterResults;
    }

    public JoinResult[] getJoinResults() {
        return joinResults;
    }

    public List<List<Integer>> getFinalKey4Join() {
        return finalKey4Join;
    }

    public void setFinalResultSize(int finalResultSize){
        this.finalResultSize = finalResultSize;
    }
    public int getFinalResultSize(){
        return this.finalResultSize;

    }

    public List<QueryTree> getUnionSet() {
        return unionSet;
    }

    public void setUnionSet(List<QueryTree> unionSet) {
        this.unionSet = unionSet;
    }

    public Table getSubTable(){
        return this.subTable;
    }

    public void setLastSqlText(String lastSqlText){
        this.lastSqlText = lastSqlText;
    }
}
