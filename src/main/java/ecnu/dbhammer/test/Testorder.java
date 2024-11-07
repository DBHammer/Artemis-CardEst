package ecnu.dbhammer.test;



import com.alibaba.druid.DbType;
import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.schema.DBSchema;
import ecnu.dbhammer.schema.SchemaGenerator;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.descriptive.SynchronizedSummaryStatistics;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Testorder {
    private static final Pattern ROW_COUNTS = Pattern.compile("rows:[0-9]+");
    private static final Pattern PLAN_ID = Pattern.compile("([a-zA-Z]+_[0-9]+)");
    private static final Pattern RANGE = Pattern.compile("range.+(?=, keep order)");
    private static final Pattern LEFT_RANGE_BOUND = Pattern.compile("(\\[|\\()([0-9.]+|\\+inf|-inf)");
    private static final Pattern RIGHT_RANGE_BOUND = Pattern.compile("([0-9.]+|\\+inf|-inf)(]|\\))");
    private static final Pattern INDEX_COLUMN = Pattern.compile("index:.+\\((.+)\\)");
    private static final Pattern EQ_OPERATOR = Pattern.compile("eq\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+), ([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+)\\)");
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
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
    public static Connection connDB(String url, String username, String passwd){
        Connection conn = null;
        String driver = "com.mysql.cj.jdbc.Driver";
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, passwd);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }
    public static String[] getSqlInfoColumns() {
        return new String[]{"id", "operator info", "estRows", "access object"};
    }
    public static String explainQuery(Connection conn, StringBuffer execquery2dot, StringBuffer execquery) throws SQLException {

        // 从文件逐行读取查询语句，依次执行
        StringBuffer query = null;
        String dot = null;
        Map<String,String> operatorInfo = new HashMap<>();

        try {
            //执行查询
            System.out.println("开始执行查询！");
            Statement statement = conn.createStatement();

            System.out.println(execquery);
            System.out.println(execquery2dot);
            ResultSet rs = statement.executeQuery(execquery.toString());

            while (rs.next()) {
                String id = rs.getString("id");
                Matcher m = PLAN_ID.matcher(id);
                if(m.find())
                    id = m.group(0);
                else continue;
                String info = "_";
                if(!rs.getString("access object").equals(""))
                    info += "["+rs.getString("access object") + "]_[" + rs.getString("estRows") + "]";
                else
                    info += "[" + rs.getString("estRows") + "]";
                operatorInfo.put(id, info);
            }

            ResultSet rs2 = statement.executeQuery(execquery2dot.toString());
            while (rs2.next()) {
                dot = rs2.getString("dot contents");
            }
            int index = -1;
            index = dot.indexOf("{");
            String dot1 = dot.substring(0,index);
            String dot2 = dot.substring(index);

            //"dot contents"中没有表名-->根据explain输出信息，在dot中加上表名
            for(Map.Entry<String, String> entry:operatorInfo.entrySet()){
                String id = entry.getKey();
                dot2 = dot2.replaceAll(id, id + entry.getValue());
            }
            dot = dot1 + dot2;

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return dot;
    }

    private static void drawGraph(String dot, int index, File outFile, File dotFile){
        //画图
        String graphPngName = "query_" + index + ".png";
        //dot -Tpng -o Downloads/tree.png Desktop/tree.dot

        String dotFileName = dotFile.getName() + File.separator + "querytree_draw_" + index + ".dot";
        System.out.println(dotFileName);

        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dotFileName)))) {
            bw.write(dot);
            bw.flush();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        String drawQueryTreeCommand = "dot -Tpng -o "+outFile.getName() + File.separator + graphPngName + " " + dotFileName;
        System.out.println(drawQueryTreeCommand);

        try {
            Runtime rt = Runtime.getRuntime();
            Process p = rt.exec(drawQueryTreeCommand);
            System.out.println("画图结束");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static String[] extractSubQueryPlanInfo(String[] data) {//

        //String[]{"id", "operator info", "estRows", "access object"}
        String[] ret = new String[3];
        ret[0] = data[0];//id
        ret[1] = data[3].isEmpty() ? data[1] : String.format("%s,%s", data[3], data[1]);
        ret[2] = "rows:" + data[2];
        return ret;

    }
    public static String extractTableName(String operatorInfo) {
        String tableName = operatorInfo.split(",")[0].substring(6).toLowerCase();

        return tableName;
    }
    /**
     *
     * 合并节点，删除query plan中不需要或者不支持的节点，并根据节点类型提取对应信息
     * 关于join下推到tikv节点的处理:
     * 1. 有selection的下推
     * ***********************************************************************
     * *         IndexJoin                                       Filter      *
     * *         /       \                                         /         *
     * *    leftNode    IndexLookup              ===>>>          Join        *
     * *                  /         \                           /   \        *
     * *        IndexRangeScan     Selection              leftNode  Scan     *
     * *                            /                                        *
     * *                          Scan                                       *
     * ***********************************************************************
     * <p>
     * 2. 没有selection的下推(leftNode中有Selection节点)
     * ***********************************************************************
     * *         IndexJoin                                       Join        *
     * *         /       \                                      /    \       *
     * *    leftNode    IndexLookup              ===>>>   leftNode   Scan    *
     * *                /        \                                           *
     * *        IndexRangeScan  Scan                                         *
     * ***********************************************************************
     * <p>
     * 3. 没有selection的下推(leftNode中没有Selection节点，但右边扫描节点上有索引)
     * ***********************************************************************
     * *        IndexJoin                                        Join        *
     * *        /       \                                       /    \       *
     * *    leftNode   IndexReader              ===>>>    leftNode   Scan    *
     * *                /                                                    *
     * *          IndexRangeScan                                             *
     * ***********************************************************************
     *
     */

    private static ExecutionNode buildExecutionTree(RawNode rawNode) {
        if (rawNode == null) {
            return null;
        }
        String nodeType = rawNode.nodeType;
        TiDBNodeType nodeTypeRef = new TiDBNodeType();
        if (nodeTypeRef.isPassNode(nodeType)) {
            return rawNode.left == null ? null : buildExecutionTree(rawNode.left);
        }
        ExecutionNode node = null;
        if (nodeTypeRef.isRangeScanNode(nodeType)) { // 处理range scan
            String tableName = extractTableName(rawNode.operatorInfo);

            if (!rawNode.operatorInfo.contains("decided by")) { // 含有decided by的operator info表示join的index range scan
                String rangeInfo = matchPattern(RANGE, rawNode.operatorInfo).get(0).get(0);//匹配出range:[-inf,524385080]
                List<List<String>> leftRangeMatches = matchPattern(LEFT_RANGE_BOUND, rangeInfo), rightRangeMatches = matchPattern(RIGHT_RANGE_BOUND, rangeInfo);//[-inf //524385080]
                // 目前只支持含有一个range的情况
                if (leftRangeMatches.size() != 1 || rightRangeMatches.size() != 1 || leftRangeMatches.get(0).size() != 3 || rightRangeMatches.get(0).size() != 3) {
                    System.out.println("不支持的查询："+rawNode.operatorInfo);
                }
                String leftOperator = "(".equals(leftRangeMatches.get(0).get(1)) ? "gt" : "ge", leftOperand = leftRangeMatches.get(0).get(2);
                String rightOperator = ")".equals(rightRangeMatches.get(0).get(2)) ? "lt" : "le", rightOperand = rightRangeMatches.get(0).get(1);
                /*
                [
                -inf
                ]
                524385080
                 */
                List<List<String>> indexMatches = matchPattern(INDEX_COLUMN, rawNode.operatorInfo);
                String columnName=null;
                if (indexMatches.size() != 0) {
                    columnName = indexMatches.get(0).get(1);
                }
                if (leftOperand.contains("inf")) {
                    return new ExecutionNode(rawNode.id, ExecutionNode.ExecutionNodeType.filter,
                            String.format("%s(%s.%s, %s)", rightOperator, tableName, columnName, rightOperand));
                } else if (rightOperand.contains("inf")) {
                    return new ExecutionNode(rawNode.id, ExecutionNode.ExecutionNodeType.filter,
                            String.format("%s(%s.%s, %s)", leftOperator, tableName, columnName, leftOperand));
                } else {
                    return new ExecutionNode(rawNode.id, ExecutionNode.ExecutionNodeType.filter,
                            String.format("and(%s(%s.%s, %s), %s(%s.%s, %s))",
                                    leftOperator, tableName, columnName, leftOperand,
                                    rightOperator, tableName, columnName, rightOperand));
                }
            } else {
                System.out.println("不能处理的operatorinfo"+rawNode.operatorInfo);
            }
        }
        // 处理底层的TableScan
        else if (nodeTypeRef.isTableScanNode(nodeType)) {
            String tableName = extractTableName(rawNode.operatorInfo);
            return new ExecutionNode(rawNode.id, ExecutionNode.ExecutionNodeType.scan, "table:" + tableName);
        } else if (nodeTypeRef.isFilterNode(nodeType)) {
            node = new ExecutionNode(rawNode.id, ExecutionNode.ExecutionNodeType.filter, rawNode.operatorInfo);
            // 跳过底部的TableScan
            if (rawNode.left != null && nodeTypeRef.isTableScanNode(rawNode.left.nodeType)) {
                return node;
            }
            node.leftNode = rawNode.left == null ? null : buildExecutionTree(rawNode.left);
            node.rightNode = rawNode.right == null ? null : buildExecutionTree(rawNode.right);
        } else if (nodeTypeRef.isJoinNode(nodeType)) {
            // 处理IndexJoin有selection的下推到tikv情况
            if (nodeTypeRef.isReaderNode(rawNode.right.nodeType)
                    && rawNode.right.right != null
                    && nodeTypeRef.isIndexScanNode(rawNode.right.left.nodeType)
                    && nodeTypeRef.isFilterNode(rawNode.right.right.nodeType)) {
                node = new ExecutionNode(rawNode.right.right.id, ExecutionNode.ExecutionNodeType.filter, rawNode.right.right.operatorInfo);
                node.leftNode = new ExecutionNode(rawNode.right.left.id, ExecutionNode.ExecutionNodeType.join, rawNode.operatorInfo);
                String tableName = extractTableName(rawNode.right.right.left.operatorInfo);
                node.leftNode.rightNode = new ExecutionNode(rawNode.right.right.left.id, ExecutionNode.ExecutionNodeType.scan, "table:" + tableName);
                node.leftNode.leftNode = buildExecutionTree(rawNode.left);
                return node;
            }
            node = new ExecutionNode(rawNode.id, ExecutionNode.ExecutionNodeType.join,rawNode.operatorInfo);
            node.leftNode = rawNode.left == null ? null : buildExecutionTree(rawNode.left);
            node.rightNode = rawNode.right == null ? null : buildExecutionTree(rawNode.right);
        } else if (nodeTypeRef.isReaderNode(nodeType)) {
            if (rawNode.right != null) {
                List<List<String>> matches = matchPattern(EQ_OPERATOR, rawNode.left.operatorInfo);
                String tableName = extractTableName(rawNode.right.operatorInfo);
                // 处理IndexJoin没有selection的下推到tikv情况
                if (!matches.isEmpty() && nodeTypeRef.isTableScanNode(rawNode.right.nodeType)) {

                    node = new ExecutionNode(rawNode.id, ExecutionNode.ExecutionNodeType.scan, "table:" + tableName);
                    // 其他情况跳过左侧节点
                } else {
                    node = buildExecutionTree(rawNode.right);
                }
            }
            // 处理IndexReader后接一个IndexScan的情况
            else if (nodeTypeRef.isIndexScanNode(rawNode.left.nodeType) && rawNode.left.operatorInfo.contains("decided by")) {
                String tableName = extractTableName(rawNode.left.operatorInfo);
                // 处理IndexJoin没有selection的下推到tikv情况

                node = new ExecutionNode(rawNode.left.id, ExecutionNode.ExecutionNodeType.scan, "table:" + tableName);
            } else {
                node = buildExecutionTree(rawNode.left);
            }
        } else {
            System.out.println("未支持的查询树Node，类型为" + nodeType);
        }
        return node;
    }
    public static RawNode buildRawNodeTree(List<String[]> queryPlan)  {
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
        RawNode rawNodeRoot = new RawNode(planId, null, null, nodeType, operatorInfo, rowCount, 0,null);
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
            rawNode = new RawNode(planId, null, null, nodeType, operatorInfo, rowCount, 0, null);
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

    public ExecutionNode getExecutionTree(List<String[]> queryPlan) {
        RawNode rawNodeRoot = buildRawNodeTree(queryPlan);
        return buildExecutionTree(rawNodeRoot);
    }
    public static void bfsPhysicalPlan(RawNode root){
        Queue<RawNode> queue = new LinkedList<RawNode>();
        queue.offer(root);
        while(!queue.isEmpty()){
            RawNode now = queue.poll();
            System.out.println(now.toString());
            if(now.right!=null) queue.offer(now.right);
            if(now.left!=null) queue.offer(now.left);
        }
    }

    public static void bfsLogicalPlan(ExecutionNode root){
        Queue<ExecutionNode> queue = new LinkedList<ExecutionNode>();
        queue.offer(root);
        while(!queue.isEmpty()){
            ExecutionNode now = queue.poll();
            System.out.println(now.toString()+"----"+now.getInfo()+"----"+now.getType());
            if(now.leftNode!=null) queue.offer(now.leftNode);
            if(now.rightNode!=null) queue.offer(now.rightNode);
        }


    }

    public static void main(String[] args) throws SQLException {
        Deque<Pair<Integer, RawNode>> pStack = new ArrayDeque<>();
        List<String[]> queryPlan = new ArrayList<>();
        String url = "jdbc:mysql://10.11.6.20:4000/artemis?useSSL=false";
        String username = "root";
        String passwd = "";
        String sql = "select  count(table_0_3.col_13) as result from table_0_16 inner join table_0_3 on table_0_16.fk_5 = table_0_3.primaryKey join table_0_7 on table_0_16.fk_7 = table_0_7.primaryKey join table_0_15 on table_0_7.primaryKey = table_0_15.fk_2 inner join table_0_8 on table_0_3.primaryKey = table_0_8.fk_1 where table_0_8.col_32 <= -325358842.91449331540396000 and table_0_15.col_4 <= -3475871.7515793555233005 and table_0_7.col_0 > -455768799.15983876343668800 and table_0_3.col_1 >= -155984596.7316709142122416 and table_0_16.col_28 <= 605011687";
        String[] sqlInfoColumns = getSqlInfoColumns();
        //TODO 从文件中读取query，加一个循环体
        //连接数据库
        Connection conn = connDB(url, username, passwd);
        //创建文件夹保存图片
        File file = new File("D:\\lauca\\GitHub\\Artemis-DBhammer\\TreeGraph_tidb");
        if (!file.exists()) {
            file.mkdir();
        }
        File dotFile = new File("D:\\lauca\\GitHub\\Artemis-DBhammer\\TreeGraphDot_tidb");
        if (!dotFile.exists()) {
            dotFile.mkdir();
        }
        String[] results = null;
        //for循环：n次explain
        int cnt = 10;
        for(int i=1; i<=cnt; i++){
            StringBuffer execquery = new StringBuffer(sql);
            StringBuffer execquery2dot = null;
            execquery2dot = new StringBuffer("explain format = \"dot\" ").append(execquery);
            execquery = new StringBuffer("explain ").append(execquery);
            execquery.insert(15, "/*+ NTH_PLAN(" +  i+ ") */ ");
            execquery2dot.insert(23, "/*+ NTH_PLAN(" +  i+ ") */ ");
            String dot = explainQuery(conn, execquery2dot, execquery);
            drawGraph(dot, i, file, dotFile);
            //results[i] = dot;
        }
        //drawGraph(results, file, dotFile);


    }


}


