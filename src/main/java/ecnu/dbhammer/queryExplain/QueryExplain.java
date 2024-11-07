package ecnu.dbhammer.queryExplain;

import com.alibaba.druid.support.spring.stat.annotation.Stat;
import ecnu.dbhammer.databaseAdapter.DBConnection;
import ecnu.dbhammer.schema.DBSchema;
import ecnu.dbhammer.schema.Table;
import ecnu.dbhammer.test.RawNode;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

/**
 * @author tingc
 * @ClassName QueryExplain.java
 * @Description TODO
 * @createTime 2022年2月17日 15:05:00
 */

public class QueryExplain {

    private DBConnection dbConnection;
    private Statement statement;
    private String query;
    private List<Table> tablesInQuery; //查询中的表

    private String joinedTableOrderStr;//连接顺序: 有bushy tree --> (A,B),C；只有左深树 --> A,B,C

    private List<Table> joinedTableOrder;//存储连接顺序
    private double estimatedCard;


    private static final Pattern ROW_COUNTS = Pattern.compile("rows:[0-9]+");
    private static final Pattern PLAN_ID = Pattern.compile("([a-zA-Z]+_[0-9]+)");
    private static final Pattern RANGE = Pattern.compile("range.+(?=, keep order)");
    private static final Pattern LEFT_RANGE_BOUND = Pattern.compile("(\\[|\\()([0-9.]+|\\+inf|-inf)");
    private static final Pattern RIGHT_RANGE_BOUND = Pattern.compile("([0-9.]+|\\+inf|-inf)(]|\\))");
    private static final Pattern INDEX_COLUMN = Pattern.compile("index:.+\\((.+)\\)");
    private static final Pattern EQ_OPERATOR = Pattern.compile("eq\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+), (\\S+)\\)");//等于
    private static final Pattern TABLE_NAME = Pattern.compile("table:([a-zA-Z0-9_$]+),");
    private static final Pattern LE_OPERATOR = Pattern.compile("le\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+), (\\S+)\\)");//小于等于
    private static final Pattern GE_OPERATOR = Pattern.compile("ge\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+), (\\S+)\\)");//大于等于
    private static final Pattern LT_OPERATOR = Pattern.compile("lt\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+), (\\S+)\\)");//小于
    private static final Pattern GT_OPERATOR = Pattern.compile("gt\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+), (\\S+)\\)");//大于
    private static final Pattern NE_OPERATOR = Pattern.compile("ne\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+), (\\S+)\\)");//不等于
    private static final Pattern COL_NAME = Pattern.compile("[a-zA-Z0-9_$]+\\.(([a-zA-Z0-9_$]+)\\.[a-zA-Z0-9_$]+)");
    private static final Pattern JOIN_INFO = Pattern.compile("\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+), ([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+)\\)");
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
    public static final Pattern PG_ROWS = Pattern.compile("rows=([0-9]+) ");
    public static final Pattern PG_TABLENAME = Pattern.compile(" on ([a-zA-Z0-9_$]+) ");
    private String[] sqlInfoColumns = {"id", "operator info", "estRows", "access object"};

    public QueryExplain(DBConnection dbConnection, Statement statement, String query, List<Table> tablesInQuery){
        this.dbConnection = dbConnection;
        this.statement = statement;
        this.query = query;
        this.tablesInQuery = tablesInQuery;
        joinedTableOrderStr = "";
        explainQuery();
    }

    public void explainQuery() {

        ArrayList<String[]> queryPlan= new ArrayList<>();
        try {
            //执行查询
            //System.out.println("开始执行查询！");
            StringBuffer execquery = new StringBuffer(query);

            execquery = new StringBuffer("explain ").append(execquery);
            //System.out.println(execquery);
            if(dbConnection.getDatabaseBrand().equalsIgnoreCase("tidb")) {
                ResultSet rs = statement.executeQuery(execquery.toString());
                while (rs.next()) {
                    String[] infos = new String[sqlInfoColumns.length];
                    for (int i = 0; i < sqlInfoColumns.length; i++) {
                        infos[i] = rs.getString(sqlInfoColumns[i]);
                    }
                    queryPlan.add(infos);
                }
                RawNode root = buildRawNodeTree_Tidb(queryPlan);
                postTravelPlan_Tidb(root);
                int n = joinedTableOrderStr.length();
                joinedTableOrderStr = joinedTableOrderStr.substring(0, n-1);//去除最后一个逗号
            }
            else if(dbConnection.getDatabaseBrand().equalsIgnoreCase("oceanbase")) {
                String info = "";
                ResultSet rs = statement.executeQuery(execquery.toString());
                ArrayList<String> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(rs.getString(1));
                    info += rs.getString(1);
                }
                // System.out.println(info);
                //处理ob输出结果
                String[] temp = info.split("Outputs & filters");
                String[] half1 = list.toArray(new String[list.size()]);

                String[] half2 = temp[1].split("\\s+[0-9]+\\s+-\\s+");

                int half2_index = 1;
                for(int i=3; i<half1.length -1; i++) {
                    if (half1[i+1].equals("Outputs & filters:")) {
                        break;
                    }
                    String[] columns = half1[i].split("\\|");
                    String[] infos = new String[5];
                    for (int j = 1; j < columns.length - 1; j++) {
                        infos[j-1] = columns[j];
                    }
                    infos[4] = half2[half2_index];
                    half2_index ++;
                    queryPlan.add(infos);
                }
                RawNode root = buildRawNodeTree_Oceanbase(queryPlan);
                // levelTravel(root);
                // depthTravelPlan_Oceanbase(root);
                bushyTreeToString(root);
                joinedTableOrderStr = root.bracketsStr;
            }else if(dbConnection.getDatabaseBrand().equalsIgnoreCase("postgresql") || dbConnection.getDatabaseBrand().equalsIgnoreCase("gaussdb")){
                //statement.execute("set join_collapse_limit = 1;");
                ResultSet rs = statement.executeQuery(execquery.toString());
                while (rs.next()) {
                    //System.out.println(rs.getString(1));
                    String[] info = rs.getString(1).split("\\(");
                    queryPlan.add(info);
                }
                RawNode root = buildRawNodeTree_Postgres(queryPlan);
                //levelTravel(root);
                bushyTreeToString(root);
                joinedTableOrderStr = root.bracketsStr;
            }
            else if(dbConnection.getDatabaseBrand().equalsIgnoreCase("mysql")){
                ResultSet rs = statement.executeQuery(execquery.toString());
                while (rs.next()) {
                    //System.out.println(rs.getString(3));
                    joinedTableOrderStr += rs.getString(3) + ",";
                }
                int n = joinedTableOrderStr.length();
                joinedTableOrderStr = joinedTableOrderStr.substring(0, n-1);//去除最后一个逗号
            }
            else{
                //其他数据库无法从EXPLAIN中获得连接顺序
                joinedTableOrderStr = query;
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }

    //解析Tidb查询计划，生成查询树形式，以便得到join order
    public RawNode buildRawNodeTree_Tidb(List<String[]> queryPlan)  {
        Deque<Pair<Integer, RawNode>> pStack = new ArrayDeque<>();
        List<List<String>> matches = matchPattern(PLAN_ID, queryPlan.get(0)[0]);//匹配出HashAgg_156
        String nodeType = matches.get(0).get(0).split("_")[0];
        String[] subQueryPlanInfo = extractSubQueryPlanInfo(queryPlan.get(0));
        //String[]{"id", "operator info", "estRows", "access object"}
        String planId = matches.get(0).get(0), operatorInfo = subQueryPlanInfo[1], executionInfo = subQueryPlanInfo[2];
        //if access object == null subqueryplan[1]=operator info else subqueryplan[1]=access object+operator info subQueryPlanInfo[2]=rows:estrows
        Matcher matcher;
        int rowCount = (matcher = ROW_COUNTS.matcher(executionInfo)).find() ?
                Integer.parseInt(matcher.group(0).split(":")[1]) : 0;//得到rows的行数
        RawNode rawNodeRoot = new RawNode(planId, null, null, nodeType, operatorInfo, rowCount, 0, "");
        RawNode rawNode;
        pStack.push(Pair.of(0, rawNodeRoot));//把根节点放入双端队列
        for (String[] subQueryPlan : queryPlan.subList(1, queryPlan.size())) {
            subQueryPlanInfo = extractSubQueryPlanInfo(subQueryPlan);
            matches = matchPattern(PLAN_ID, subQueryPlanInfo[0]);
            planId = matches.get(0).get(0);
            operatorInfo = subQueryPlanInfo[1];
            executionInfo = subQueryPlanInfo[2];
            nodeType = matches.get(0).get(0).split("_")[0];
            try {
                rowCount = (matcher = ROW_COUNTS.matcher(executionInfo)).find() ?
                        Integer.parseInt(matcher.group(0).split(":")[1]) : 0;
            } catch (ArrayIndexOutOfBoundsException e) {
                System.out.println("错误！");
            }
            rawNode = new RawNode(planId, null, null, nodeType, operatorInfo, rowCount, 0, "");
            int level = (subQueryPlan[0].split("─")[0].length() + 1) / 2;//获取层级
            while (!pStack.isEmpty() && pStack.peek().getKey() > level) {
                pStack.pop(); // pop直到找到同一个层级的节点
            }
            if (pStack.isEmpty()) {
                System.out.println("pStack不应为空");
            }

            if (pStack.peek().getKey().equals(level)) {
                pStack.pop();
                if (pStack.isEmpty()) {
                    System.out.println("pStack不应为空");
                }
                pStack.peek().getValue().right = rawNode;
            } else {
                pStack.peek().getValue().left = rawNode;
            }
            pStack.push(Pair.of(level, rawNode));//放入双端队列
        }
        return rawNodeRoot;
    }

    public RawNode buildRawNodeTree_Oceanbase(List<String[]> queryPlan)  {
        Deque<Pair<Integer, RawNode>> pStack = new ArrayDeque<>();
        //String[]:id nodeType table est_rows operator_info
        String nodeType = queryPlan.get(0)[1].trim();
        String planId = queryPlan.get(0)[1].trim() + "_" + queryPlan.get(0)[0].trim();
        String operatorInfo = queryPlan.get(0)[4].trim();
        String tableName = queryPlan.get(0)[2].trim();
        long rows = Long.parseLong(queryPlan.get(0)[3].trim());
        RawNode rawNodeRoot = new RawNode(planId, null, null, nodeType, operatorInfo, rows, 0, tableName);
        RawNode rawNode;
        pStack.push(Pair.of(0, rawNodeRoot));//把根节点放入双端队列
        for (String[] subQueryPlan : queryPlan.subList(1, queryPlan.size())) {
            nodeType = subQueryPlan[1].trim();
            planId = subQueryPlan[1].trim() + "_" + subQueryPlan[0].trim();
            operatorInfo = subQueryPlan[4].trim();
            rows = Long.parseLong(subQueryPlan[3].trim());
            tableName = subQueryPlan[2].trim();

            rawNode = new RawNode(planId, null, null, nodeType, operatorInfo, rows, 0, tableName);
        //    if(nodeType.toLowerCase().contains("scan") || nodeType.toLowerCase().contains("get")) {
        //        String selectId = "SELECTION_" + subQueryPlan[0].trim();
        //        RawNode selectNode = new RawNode(selectId, rawNode,null,"selection",operatorInfo,rows,0,tableName);
        //        rawNode = selectNode;
        //    }
            int index = 0;
            for(int i=0; i<subQueryPlan[1].length(); i++){
                if(subQueryPlan[1].charAt(i) >= 'A' && subQueryPlan[1].charAt(i) <= 'Z') {
                    index = i;
                    break;
                }
            }
            int level = index;//获取层级
            while (!pStack.isEmpty() && pStack.peek().getKey() > level) {
                pStack.pop(); // pop直到找到同一个层级的节点
            }
            if (pStack.isEmpty()) {
                System.out.println("pStack不应为空");
            }

            if (pStack.peek().getKey().equals(level)) {
                pStack.pop();
                if (pStack.isEmpty()) {
                    System.out.println("pStack不应为空");
                }
                pStack.peek().getValue().right = rawNode;
            } else {
                pStack.peek().getValue().left = rawNode;
            }
            pStack.push(Pair.of(level, rawNode));//放入双端队列
        }
        return rawNodeRoot;
    }

    public RawNode buildRawNodeTree_Postgres(List<String[]> queryPlan)  {
        Deque<Pair<Integer, RawNode>> pStack = new ArrayDeque<>();
        //String[]:id nodeType table est_rows operator_info
        String nodeType = queryPlan.get(0)[0];
        String planId = queryPlan.get(0)[0].trim() + "_1";
        String operatorInfo = queryPlan.get(0)[1].trim();
        List<List<String>> matches = matchPattern(PG_ROWS, operatorInfo);//匹配出PG的估算行
        long rows = 0;
        if(matches.size() > 0)
            rows = Long.parseLong(matches.get(0).get(1));
        String tableName = "";
        //long rows = Long.parseLong(queryPlan.get(0)[3].trim());
        RawNode rawNodeRoot = new RawNode(planId, null, null, nodeType, operatorInfo, rows, 0, tableName);
        RawNode rawNode;
        int id = 1;
        pStack.push(Pair.of(0, rawNodeRoot));//把根节点放入双端队列
        for (String[] subQueryPlan : queryPlan.subList(1, queryPlan.size())) {
            nodeType = subQueryPlan[0];
            if(!nodeType.contains("->")) continue;
            planId = subQueryPlan[0].trim() + "_" + id;
            id ++;
            operatorInfo = subQueryPlan[1].trim();
            rows = 0;
            tableName = "";
            //System.out.println(nodeType);
            //System.out.println(operatorInfo);
            matches = matchPattern(PG_ROWS, operatorInfo);//匹配出PG的估算行
            if(matches.size() > 0)
                rows = Long.parseLong(matches.get(0).get(1));
            if(nodeType.toLowerCase().contains("scan")) {
                matches = matchPattern(PG_TABLENAME, nodeType);
                tableName = matches.get(0).get(1);
            }

            rawNode = new RawNode(planId, null, null, nodeType, operatorInfo, rows, 0, tableName);
            int index = nodeType.indexOf("->");;
            int level = (index - 2) /6 + 1;//获取层级
            while (!pStack.isEmpty() && pStack.peek().getKey() > level) {
                pStack.pop(); // pop直到找到同一个层级的节点
            }
            if (pStack.isEmpty()) {
                System.out.println("pStack不应为空");
            }

            if (pStack.peek().getKey().equals(level)) {
                pStack.pop();
                if (pStack.isEmpty()) {
                    System.out.println("pStack不应为空");
                }
                pStack.peek().getValue().left = rawNode;
            } else {
                pStack.peek().getValue().right = rawNode;
            }
            pStack.push(Pair.of(level, rawNode));//放入双端队列
        }
        return rawNodeRoot;
    }

    //后序遍历，得到join order
    public void postTravelPlan_Tidb (RawNode root) {
        if(root != null && root.left != null) postTravelPlan_Tidb(root.left);
        if(root != null && root.right != null) postTravelPlan_Tidb(root.right);
        if(root != null) {
            if(root.nodeType.toLowerCase().contains("scan") ) {
                List<List<String>> matches = matchPattern(TABLE_NAME, root.operatorInfo);
                String tableName = matches.get(0).get(1);
                joinedTableOrderStr += tableName + ",";
            }
        }
    }

    public void levelTravel (RawNode root) {
        Queue<RawNode> queue = new LinkedList<>();
        queue.add(root);
        while(!queue.isEmpty()){
            RawNode now = queue.poll();
            System.out.print(now.nodeType + " "+ now.tableName);
            if(now.left != null) {
                System.out.print( "l>" + now.left.nodeType + now.left.tableName );
                queue.add(now.left);
            }
            if(now.right != null){
                System.out.print( "r>" + now.right.nodeType + now.right.tableName );
                queue.add(now.right);
            }
            System.out.println();
            System.out.println("---------");
        }
    }

    public static void bushyTreeToString(RawNode root){
        if(root!=null) {
            if(root.left != null) bushyTreeToString(root.left);
            if(root.right != null) bushyTreeToString(root.right);
            if(root.nodeType.toLowerCase().contains("join") || root.nodeType.toLowerCase().contains("loop")) {
                String s1 = root.left.bracketsStr;
                String s2 = root.right.bracketsStr;
                root.setBracketsStr("("+s1+","+s2+")");
            }
            else if(root.nodeType.toLowerCase().contains("scan") || root.nodeType.toLowerCase().contains("get")) {
                root.setBracketsStr(root.tableName);
            }
            else {
                if(root.left != null)
                    root.setBracketsStr(root.left.bracketsStr);
                else
                    root.setBracketsStr(root.right.bracketsStr);
            }
        }
    }


    private static String[] extractSubQueryPlanInfo(String[] data) {//

        //String[]{"id", "operator info", "estRows", "access object"}
        String[] ret = new String[3];
        ret[0] = data[0];//id
        ret[1] = data[3].isEmpty() ? data[1] : String.format("%s,%s", data[3], data[1]);//判断access object是否为空
        ret[2] = "rows:" + data[2];
        return ret;

    }
    public static List<List<String>> matchPattern(Pattern pattern, String str) {
        Matcher matcher = pattern.matcher(str);
        List<List<String>> ret = new ArrayList<>();
        while (matcher.find()) {
            List<String> groups = new ArrayList<>();
            for (int i = 0; i <= matcher.groupCount(); i++) {
                groups.add(matcher.group(i));
            }
            ret.add(groups);
        }

        return ret;
    }

    public String getJoinedTableOrderStr () {
        return this.joinedTableOrderStr;
    }

}
