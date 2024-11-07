//package ecnu.dbhammer.test;
//
//
//
//
//
//import ecnu.dbhammer.configuration.Configurations;
//import ecnu.dbhammer.constraint.TableConstraint;
//import ecnu.dbhammer.databaseoperation.DatabaseConnection;
//import ecnu.dbhammer.result.FilterResult;
//import ecnu.dbhammer.schema.*;
//import ecnu.dbhammer.schema.genefunc.AttrGeneFunc;
//import ecnu.dbhammer.schema.genefunc.LinearFunc;
//import ecnu.dbhammer.tomathematica.SingleTableWithFilter;
//import ecnu.dbhammer.utils.SetMathematicaEnviroment;
//import ecnu.dbhammer.utils.Utils;
//import org.apache.commons.lang3.tuple.Pair;
//
//import java.io.*;
//import java.math.BigDecimal;
//import java.sql.*;
//import java.text.SimpleDateFormat;
//import java.util.*;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//public class Test {
//    private static final Pattern ROW_COUNTS = Pattern.compile("rows:[0-9]+");
//    private static final Pattern PLAN_ID = Pattern.compile("([a-zA-Z]+_[0-9]+)");
//    private static final Pattern RANGE = Pattern.compile("range.+(?=, keep order)");
//    private static final Pattern LEFT_RANGE_BOUND = Pattern.compile("(\\[|\\()([0-9.]+|\\+inf|-inf)");
//    private static final Pattern RIGHT_RANGE_BOUND = Pattern.compile("([0-9.]+|\\+inf|-inf)(]|\\))");
//    private static final Pattern INDEX_COLUMN = Pattern.compile("index:.+\\((.+)\\)");
//    private static final Pattern EQ_OPERATOR = Pattern.compile("eq\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+), (\\S+)\\)");//等于
//    private static final Pattern TABLE_NAME = Pattern.compile("table:([a-zA-Z0-9_$]+),");
//    private static final Pattern LE_OPERATOR = Pattern.compile("le\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+), (\\S+)\\)");//小于等于
//    private static final Pattern GE_OPERATOR = Pattern.compile("ge\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+), (\\S+)\\)");//大于等于
//    private static final Pattern LT_OPERATOR = Pattern.compile("lt\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+), (\\S+)\\)");//小于
//    private static final Pattern GT_OPERATOR = Pattern.compile("gt\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+), (\\S+)\\)");//大于
//    private static final Pattern NE_OPERATOR = Pattern.compile("ne\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+), (\\S+)\\)");//不等于
//    private static final Pattern COL_NAME = Pattern.compile("[a-zA-Z0-9_$]+\\.(([a-zA-Z0-9_$]+)\\.[a-zA-Z0-9_$]+)");
//    private static final Pattern JOIN_INFO = Pattern.compile("\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+), ([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+)\\)");
//    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
//
//    public static Map<String, Table> tableMap;
//    public static Map<String, TableConstraint> tableConstraintMap;
//    public static List<Table> existedTables;
//    //public static KernelLink mathematicaLink = SetMathematicaEnviroment.SetLink();
//
//    public static List<List<String>> matchPattern(Pattern pattern, String str) {
//        Matcher matcher = pattern.matcher(str);
//        List<List<String>> ret = new ArrayList<>();
//        while (matcher.find()) {
//            List<String> groups = new ArrayList<>();
//            for (int i = 0; i <= matcher.groupCount(); i++) {
//                groups.add(matcher.group(i));
//            }
//            ret.add(groups);
//        }
//
//        return ret;
//    }
//    public static String[] getSqlInfoColumns() {
//        return new String[]{"id", "operator info", "estRows", "access object"};
//    }
//
//
//    public static List<String[]> explainQuery(Connection conn, String sql, String[] sqlInfoColumns, int index) throws SQLException {
//
//        // 从文件逐行读取查询语句，依次执行
//        StringBuffer query = null;
//        ArrayList<String[]> result = new ArrayList<>();
//        try {
//            //执行查询
//            System.out.println("开始执行查询！");
//            Statement statement = conn.createStatement();
//            StringBuffer execquery = new StringBuffer(sql);
//
//            int cnt = index + 1;
//            execquery.insert(7, "/*+ NTH_PLAN(" +  cnt + ") */ ");
//            execquery = new StringBuffer("explain ").append(execquery);
//            System.out.println(execquery);
//            ResultSet rs = statement.executeQuery(execquery.toString());
//
//            while (rs.next()) {
//                String[] infos = new String[sqlInfoColumns.length];
//                for (int i = 0; i < sqlInfoColumns.length; i++) {
//                    infos[i] = rs.getString(sqlInfoColumns[i]);
//                }
//                result.add(infos);
//            }
//        } catch (SQLException throwables) {
//            throwables.printStackTrace();
//        }
//
//        return result;
//    }
//
//    private static String[] extractSubQueryPlanInfo(String[] data) {//
//
//        //String[]{"id", "operator info", "estRows", "access object"}
//        String[] ret = new String[3];
//        ret[0] = data[0];//id
//        ret[1] = data[3].isEmpty() ? data[1] : String.format("%s,%s", data[3], data[1]);//判断access object是否为空
//        ret[2] = "rows:" + data[2];
//        return ret;
//
//    }
//    public static String extractTableName(String operatorInfo) {
//        String tableName = operatorInfo.split(",")[0].substring(6).toLowerCase();
//
//        return tableName;
//    }
//    /**
//     *
//     * 合并节点，删除query plan中不需要或者不支持的节点，并根据节点类型提取对应信息
//     * 关于join下推到tikv节点的处理:
//     * 1. 有selection的下推
//     * ***********************************************************************
//     * *         IndexJoin                                       Filter      *
//     * *         /       \                                         /         *
//     * *    leftNode    IndexLookup              ===>>>          Join        *
//     * *                  /         \                           /   \        *
//     * *        IndexRangeScan     Selection              leftNode  Scan     *
//     * *                            /                                        *
//     * *                          Scan                                       *
//     * ***********************************************************************
//     * <p>
//     * 2. 没有selection的下推(leftNode中有Selection节点)
//     * ***********************************************************************
//     * *         IndexJoin                                       Join        *
//     * *         /       \                                      /    \       *
//     * *    leftNode    IndexLookup              ===>>>   leftNode   Scan    *
//     * *                /        \                                           *
//     * *        IndexRangeScan  Scan                                         *
//     * ***********************************************************************
//     * <p>
//     * 3. 没有selection的下推(leftNode中没有Selection节点，但右边扫描节点上有索引)
//     * ***********************************************************************
//     * *        IndexJoin                                        Join        *
//     * *        /       \                                       /    \       *
//     * *    leftNode   IndexReader              ===>>>    leftNode   Scan    *
//     * *                /                                                    *
//     * *          IndexRangeScan                                             *
//     * ***********************************************************************
//     *
//     */
//
//    private static ExecutionNode buildExecutionTree(RawNode rawNode) {
//        if (rawNode == null) {
//            return null;
//        }
//        String nodeType = rawNode.nodeType;
//        TiDBNodeType nodeTypeRef = new TiDBNodeType();
//        if (nodeTypeRef.isPassNode(nodeType)) {
//            return rawNode.left == null ? null : buildExecutionTree(rawNode.left);
//        }
//        ExecutionNode node = null;
//        if (nodeTypeRef.isRangeScanNode(nodeType)) { // 处理range scan
//            String tableName = extractTableName(rawNode.operatorInfo);
//
//            if (!rawNode.operatorInfo.contains("decided by")) { // 含有decided by的operator info表示join的index range scan
//                String rangeInfo = matchPattern(RANGE, rawNode.operatorInfo).get(0).get(0);//匹配出range:[-inf,524385080]
//                List<List<String>> leftRangeMatches = matchPattern(LEFT_RANGE_BOUND, rangeInfo), rightRangeMatches = matchPattern(RIGHT_RANGE_BOUND, rangeInfo);//[-inf //524385080]
//                // 目前只支持含有一个range的情况
//                if (leftRangeMatches.size() != 1 || rightRangeMatches.size() != 1 || leftRangeMatches.get(0).size() != 3 || rightRangeMatches.get(0).size() != 3) {
//                    System.out.println("不支持的查询："+rawNode.operatorInfo);
//                }
//                String leftOperator = "(".equals(leftRangeMatches.get(0).get(1)) ? "gt" : "ge", leftOperand = leftRangeMatches.get(0).get(2);
//                String rightOperator = ")".equals(rightRangeMatches.get(0).get(2)) ? "lt" : "le", rightOperand = rightRangeMatches.get(0).get(1);
//                /*
//                [
//                -inf
//                ]
//                524385080
//                 */
//                List<List<String>> indexMatches = matchPattern(INDEX_COLUMN, rawNode.operatorInfo);
//                String columnName=null;
//                if (indexMatches.size() != 0) {
//                    columnName = indexMatches.get(0).get(1);
//                }
//                if (leftOperand.contains("inf")) {
//                    return new ExecutionNode(rawNode.id, ExecutionNode.ExecutionNodeType.filter,
//                            String.format("%s(%s.%s, %s)", rightOperator, tableName, columnName, rightOperand));
//                } else if (rightOperand.contains("inf")) {
//                    return new ExecutionNode(rawNode.id, ExecutionNode.ExecutionNodeType.filter,
//                            String.format("%s(%s.%s, %s)", leftOperator, tableName, columnName, leftOperand));
//                } else {
//                    return new ExecutionNode(rawNode.id, ExecutionNode.ExecutionNodeType.filter,
//                            String.format("and(%s(%s.%s, %s), %s(%s.%s, %s))",
//                                    leftOperator, tableName, columnName, leftOperand,
//                                    rightOperator, tableName, columnName, rightOperand));
//                }
//            } else {
//                System.out.println("不能处理的operatorinfo"+rawNode.operatorInfo);
//            }
//        }
//        // 处理底层的TableScan
//        else if (nodeTypeRef.isTableScanNode(nodeType)) {
//            String tableName = extractTableName(rawNode.operatorInfo);
//            return new ExecutionNode(rawNode.id, ExecutionNode.ExecutionNodeType.scan, "table:" + tableName);
//        } else if (nodeTypeRef.isFilterNode(nodeType)) {
//            node = new ExecutionNode(rawNode.id, ExecutionNode.ExecutionNodeType.filter, rawNode.operatorInfo);
//            // 跳过底部的TableScan
//            if (rawNode.left != null && nodeTypeRef.isTableScanNode(rawNode.left.nodeType)) {
//                return node;
//            }
//            node.leftNode = rawNode.left == null ? null : buildExecutionTree(rawNode.left);
//            node.rightNode = rawNode.right == null ? null : buildExecutionTree(rawNode.right);
//        } else if (nodeTypeRef.isJoinNode(nodeType)) {
//            // 处理IndexJoin有selection的下推到tikv情况
//            if (nodeTypeRef.isReaderNode(rawNode.right.nodeType)
//                    && rawNode.right.right != null
//                    && nodeTypeRef.isIndexScanNode(rawNode.right.left.nodeType)
//                    && nodeTypeRef.isFilterNode(rawNode.right.right.nodeType)) {
//                node = new ExecutionNode(rawNode.right.right.id, ExecutionNode.ExecutionNodeType.filter, rawNode.right.right.operatorInfo);
//                node.leftNode = new ExecutionNode(rawNode.right.left.id, ExecutionNode.ExecutionNodeType.join, rawNode.operatorInfo);
//                String tableName = extractTableName(rawNode.right.right.left.operatorInfo);
//                node.leftNode.rightNode = new ExecutionNode(rawNode.right.right.left.id, ExecutionNode.ExecutionNodeType.scan, "table:" + tableName);
//                node.leftNode.leftNode = buildExecutionTree(rawNode.left);
//                return node;
//            }
//            node = new ExecutionNode(rawNode.id, ExecutionNode.ExecutionNodeType.join,rawNode.operatorInfo);
//            node.leftNode = rawNode.left == null ? null : buildExecutionTree(rawNode.left);
//            node.rightNode = rawNode.right == null ? null : buildExecutionTree(rawNode.right);
//        } else if (nodeTypeRef.isReaderNode(nodeType)) {
//            if (rawNode.right != null) {
//                List<List<String>> matches = matchPattern(EQ_OPERATOR, rawNode.left.operatorInfo);
//                String tableName = extractTableName(rawNode.right.operatorInfo);
//                // 处理IndexJoin没有selection的下推到tikv情况
//                if (!matches.isEmpty() && nodeTypeRef.isTableScanNode(rawNode.right.nodeType)) {
//
//                    node = new ExecutionNode(rawNode.id, ExecutionNode.ExecutionNodeType.scan, "table:" + tableName);
//                    // 其他情况跳过左侧节点
//                } else {
//                    node = buildExecutionTree(rawNode.right);
//                }
//            }
//            // 处理IndexReader后接一个IndexScan的情况
//            else if (nodeTypeRef.isIndexScanNode(rawNode.left.nodeType) && rawNode.left.operatorInfo.contains("decided by")) {
//                String tableName = extractTableName(rawNode.left.operatorInfo);
//                // 处理IndexJoin没有selection的下推到tikv情况
//
//                node = new ExecutionNode(rawNode.left.id, ExecutionNode.ExecutionNodeType.scan, "table:" + tableName);
//            } else {
//                node = buildExecutionTree(rawNode.left);
//            }
//        } else {
//            System.out.println("未支持的查询树Node，类型为" + nodeType);
//        }
//        return node;
//    }
//    public static RawNode buildRawNodeTree(List<String[]> queryPlan)  {
//        Deque<Pair<Integer, RawNode>> pStack = new ArrayDeque<>();
//        List<List<String>> matches = matchPattern(PLAN_ID, queryPlan.get(0)[0]);//匹配出HashAgg_156
//        String nodeType = matches.get(0).get(0).split("_")[0];
//        String[] subQueryPlanInfo = extractSubQueryPlanInfo(queryPlan.get(0));
//        //String[]{"id", "operator info", "estRows", "access object"}
//        String planId = matches.get(0).get(0), operatorInfo = subQueryPlanInfo[1], executionInfo = subQueryPlanInfo[2];
//        //if access object == null subqueryplan[1]=operator info else subqueryplan[1]=access object+operator info subQueryPlanInfo[2]=rows:estrows
//        Matcher matcher;
//        int rowCount = (matcher = ROW_COUNTS.matcher(executionInfo)).find() ?
//                Integer.parseInt(matcher.group(0).split(":")[1]) : 0;//得到rows的行数
//        RawNode rawNodeRoot = new RawNode(planId, null, null, nodeType, operatorInfo, rowCount, 0, "");
//        RawNode rawNode;
//        pStack.push(Pair.of(0, rawNodeRoot));//把根节点放入双端队列
//        for (String[] subQueryPlan : queryPlan.subList(1, queryPlan.size())) {
//            subQueryPlanInfo = extractSubQueryPlanInfo(subQueryPlan);
//            matches = matchPattern(PLAN_ID, subQueryPlanInfo[0]);
//            planId = matches.get(0).get(0);
//            operatorInfo = subQueryPlanInfo[1];
//            executionInfo = subQueryPlanInfo[2];
//            nodeType = matches.get(0).get(0).split("_")[0];
//            try {
//                rowCount = (matcher = ROW_COUNTS.matcher(executionInfo)).find() ?
//                        Integer.parseInt(matcher.group(0).split(":")[1]) : 0;
//            } catch (ArrayIndexOutOfBoundsException e) {
//                System.out.println("错误！");
//            }
//            rawNode = new RawNode(planId, null, null, nodeType, operatorInfo, rowCount, 0, "");
//            int level = (subQueryPlan[0].split("─")[0].length() + 1) / 2;//获取层级
//            while (!pStack.isEmpty() && pStack.peek().getKey() > level) {
//                pStack.pop(); // pop直到找到同一个层级的节点
//            }
//            if (pStack.isEmpty()) {
//                System.out.println("pStack不应为空");
//            }
//
//            if (pStack.peek().getKey().equals(level)) {
//                pStack.pop();
//                if (pStack.isEmpty()) {
//                    System.out.println("pStack不应为空");
//                }
//                pStack.peek().getValue().right = rawNode;
//            } else {
//                pStack.peek().getValue().left = rawNode;
//            }
//            pStack.push(Pair.of(level, rawNode));//放入双端队列
//        }
//        return rawNodeRoot;
//    }
//
//    public ExecutionNode getExecutionTree(List<String[]> queryPlan) {
//        RawNode rawNodeRoot = buildRawNodeTree(queryPlan);
//        return buildExecutionTree(rawNodeRoot);
//    }
//    public static void bfsPhysicalPlan(RawNode root){
//        Queue<RawNode> queue = new LinkedList<RawNode>();
//        queue.offer(root);
//        while(!queue.isEmpty()){
//            RawNode now = queue.poll();
//            System.out.println(now.toString());
//            if(now.right != null) System.out.println("right: "+now.right.toString());
//            if(now.left != null) System.out.println("left: "+now.left.toString());
//            if(now.right!=null) queue.offer(now.right);
//            if(now.left!=null) queue.offer(now.left);
//        }
//    }
//
//    public static long calculateResult(RawNode root){
//        long result = 0;
//        Queue<RawNode> queue = new LinkedList<RawNode>();
//        queue.offer(root);
//        while(!queue.isEmpty()){
//            RawNode now = queue.poll();
//            if(now.nodeType.toLowerCase().contains("selection") || now.nodeType.toLowerCase().contains("join"))
//                result += now.middleResult;
//            if(now.right!=null) queue.offer(now.right);
//            if(now.left!=null) queue.offer(now.left);
//        }
//        return result;
//    }
//
//    public static List<String> getPredicates (String operatorInfo) {
//        List<List<String>> le_matches = matchPattern(LE_OPERATOR, operatorInfo);
//        List<List<String>> ge_matches = matchPattern(GE_OPERATOR, operatorInfo);
//        List<List<String>> lt_matches = matchPattern(LT_OPERATOR, operatorInfo);
//        List<List<String>> gt_matches = matchPattern(GT_OPERATOR, operatorInfo);
//        List<List<String>> eq_matches = matchPattern(EQ_OPERATOR, operatorInfo);
//        List<List<String>> ne_matches = matchPattern(NE_OPERATOR, operatorInfo);
//
//        List<String> predicates = new ArrayList<>();
//        //TODO col_1 + col_2 >= 7
//
//        for(int i=0; i<le_matches.size(); i++){
//            String colName = le_matches.get(i).get(1);
//            String right_side = le_matches.get(i).get(2);
//            String predicate = colName + " <= " + right_side;
//            predicates.add(predicate);
//        }
//
//        for(int i=0; i<ge_matches.size(); i++){
//            String colName = ge_matches.get(i).get(1);
//            String right_side = ge_matches.get(i).get(2);
//            String predicate = colName + " >= " + right_side;
//            predicates.add(predicate);
//        }
//
//        for(int i=0; i<lt_matches.size(); i++){
//            String colName = lt_matches.get(i).get(1);
//            String right_side = lt_matches.get(i).get(2);
//            String predicate = colName + " < " + right_side;
//            predicates.add(predicate);
//        }
//
//        for(int i=0; i<gt_matches.size(); i++){
//            String colName = gt_matches.get(i).get(1);
//            String right_side = gt_matches.get(i).get(2);
//            String predicate = colName + " > " + right_side;
//            predicates.add(predicate);
//        }
//
//        for(int i=0; i<eq_matches.size(); i++){
//            String colName = eq_matches.get(i).get(1);
//            String right_side = eq_matches.get(i).get(2);
//            String predicate = colName + " = " + right_side;
//            predicates.add(predicate);
//        }
//
//        for(int i=0; i<ne_matches.size(); i++){
//            String colName = ne_matches.get(i).get(1);
//            String right_side = ne_matches.get(i).get(2);
//            String predicate = colName + " != " + right_side;
//            predicates.add(predicate);
//        }
//
//        return predicates;
//    }
//
//    //将谓词转换成带主键k的约束链形式：例：col_1 > 7 => 2*k + 1 > 7
//    public static Map<Integer, List<List<String>>> predicate2Constraint (List<String> predicates) {
//        Map<Integer, List<List<String>>> hash2contraintMap = new HashMap<>();
//        for (int i = 0; i < 4; i++) {
//            List<String> constraintList = new ArrayList();
//            for (int j = 0; j < predicates.size(); j++) {
//                String temp[] = predicates.get(j).split(" ");
//                String constraint = "";
//                for (int k = 0; k < temp.length; k++) {
//                    Matcher matcher = COL_NAME.matcher(temp[k]);
//                    if (matcher.find()) {
//                        String tableName = matcher.group(2);
//                        String colName = matcher.group(1);
//                        List<AttrGeneFunc> attrGeneFunc = tableMap.get(tableName).getColumnThroughColumnName(colName).getColumnGeneExpressions();
//                        constraint += " " + attrGeneFunc.get(i).getExpression();
//                    } else
//                        constraint += " " + temp[k];
//                }
//                constraintList.add(constraint);
//            }
//            List<List<String>> multiConstraints = new ArrayList<>();
//            multiConstraints.add(constraintList);//所有属性列都选择第I个生成函数时生成的表达式集合
//            hash2contraintMap.put(i, multiConstraints);//与I对应起来
//        }
//        return hash2contraintMap;
//    }
//
//    public static void setJoinRelations(RawNode root, String leftJoinColumnName, String rightJoinColumnName){
//        root.setLeftJoinColummnName(leftJoinColumnName);
//        root.setRightJoinColumnName(rightJoinColumnName);
//        Map<String, String> preJoinRelations = root.getPreJoinRelations();
//        if(!root.left.nodeType.toLowerCase().contains("join")&&!root.right.nodeType.toLowerCase().contains("join")) {//左右都是filter
//            if(leftJoinColumnName.split("\\.")[1].equalsIgnoreCase("primarykey"))//key 为外键 value为主键
//            {
//                preJoinRelations.put(rightJoinColumnName,leftJoinColumnName);//（x1,x2）x1为提供外键的column x2为提供主键的colunm
//            } else {
//                preJoinRelations.put(leftJoinColumnName,rightJoinColumnName);
//            }
//        } else if (root.left.nodeType.toLowerCase().contains("join")&&!root.right.nodeType.toLowerCase().contains("join")) {//左边为join类型
//            //为什么要用克隆:因为要用通过之前对象的引用克隆出新的对象
//            preJoinRelations = deepCloneMap(root.left.getPreJoinRelations());
//            if(leftJoinColumnName.split("\\.")[1].equalsIgnoreCase("primarykey"))
//            {
//                preJoinRelations.put(rightJoinColumnName,leftJoinColumnName);
//            } else {
//                preJoinRelations.put(leftJoinColumnName,rightJoinColumnName);
//            }
//        } else if(!root.left.nodeType.toLowerCase().contains("join")&&root.right.nodeType.toLowerCase().contains("join")) {//右边为join类型
//            preJoinRelations = deepCloneMap(root.right.getPreJoinRelations());
//            if(rightJoinColumnName.split("\\.")[1].equals("primaryKey"))
//            {
//                preJoinRelations.put(leftJoinColumnName,rightJoinColumnName);
//            } else {
//                preJoinRelations.put(rightJoinColumnName,leftJoinColumnName);
//            }
//        }
//        root.setPreJoinRelatons(preJoinRelations);
//    }
//
//    public static String getJoinConstraints(RawNode root){
//        List<List<String>> matches = matchPattern(JOIN_INFO, root.operatorInfo);
//        String temp1[] = matches.get(0).get(1).split("\\.");
//        String temp2[] = matches.get(0).get(2).split("\\.");
//        String fkTableName =  null;
//        String fkColumnName = null;
//        String pkTableName = null;
//        String pkColumnName = null;
//        String newTable = null;
//
//        //System.out.println(tableMap.get(temp1[1].trim()).getColumnThroughColumnName(temp1[1].trim()+"."+temp1[2].trim()).getClass().toString());
//        //判断哪个是主键，哪个是外键
//        System.out.println("*********"+temp1[1].trim()+"."+temp1[2].trim());
//        System.out.println("# "+tableMap.get(temp1[1].trim()).getColumnThroughColumnName(temp1[1].trim()+"."+temp1[2].trim()));
//        if(tableMap.get(temp1[1].trim()).getColumnThroughColumnName(temp1[1].trim()+"."+temp1[2].trim()).getClass().toString().toLowerCase().contains("primarykey")){
//            pkTableName = temp1[1].trim();
//            pkColumnName = temp1[2].trim();
//            fkTableName = temp2[1].trim();
//            fkColumnName = temp2[2].trim();
//        }else{
//            fkTableName = temp1[1].trim();
//            fkColumnName = temp1[2].trim();
//            pkTableName = temp2[1].trim();
//            pkColumnName = temp2[2].trim();
//        }
//        existedTables.add(tableMap.get(pkTableName));
//        existedTables.add(tableMap.get(fkTableName));
//        setJoinRelations(root, pkTableName+"."+pkColumnName, fkTableName+"."+fkColumnName);
//        System.out.println(root.left);
//        System.out.println(root.right);
//        if(root.left.nodeType.toLowerCase().contains("join") && root.right.tableName.equals(fkTableName))
//            newTable = "foreign";
//        else if(root.left.nodeType.toLowerCase().contains("join") && root.right.tableName.equals(pkTableName))
//            newTable = "primary";
//        else if(root.right.nodeType.toLowerCase().contains("join") && root.left.tableName.equals(fkTableName))
//            newTable = "foreign";
//        else if(root.right.nodeType.toLowerCase().contains("join") && root.left.tableName.equals(pkTableName))
//            newTable = "primary";
//        else //TODO bushy tree
//            newTable = "foreign";
//
//        //外键到主键约束
//        ForeignKey joinForeignKey = (ForeignKey) tableMap.get(fkTableName).getColumnThroughColumnName(fkTableName+"."+fkColumnName);
//        System.out.println(root.operatorInfo);
//
//        // 需要进行约束补充的表的名字集合
//        Set<String> tableNames2Deal = new HashSet<>();
//        for (Table table : existedTables) {
//            tableNames2Deal.add(table.getTableName());
//        }
//
//        //所有的表都需要补充约束(filter)
//        String transferingTableName = null;
//        Set<String> tableNamesCanTranser = new HashSet<>();
//
//        //转换约束链
//        //TODO 目前每张表上的谓词没有and，or，（），所以先不考虑那么多
//        if (newTable.equals("foreign")) {  // 新连接的表是fk表
//
//            System.out.println("新连接的表是fk表");
//            Map<String, String> joinRelations = root.getPreJoinRelations();
//
//            Map<Integer, List<List<String>>> oldPkConstraints = tableConstraintMap.get(pkTableName).getConstraint();
//            Map<Integer, List<List<String>>> newPkConstraints = new HashMap<>();
//            Map<Integer, List<List<String>>> oldFkConstraints = tableConstraintMap.get(fkTableName).getConstraint();
//            Map<Integer, List<List<String>>> newFkConstraints = deepCloneMap(oldFkConstraints);
//
//            //判断情况,当前连接的PK表为joinPkTableName
//            //求得上次Join的pk表和Fk表
//
//            // 将pk表上的约束转移到fk表上
//            // 由于pk表是旧表，可能不存在某些index fk表在进行转换时需要进行判断
//            Map<Integer, Integer> indexRelation = getIndexRelation(joinForeignKey);
//
//            Iterator<Map.Entry<Integer, List<List<String>>>> entryIterator = newFkConstraints.entrySet().iterator();
//            while (entryIterator.hasNext()) {
//                // TODO bug
//                //因为要构建新的fk约束，所以枚举新的fk
//                Map.Entry<Integer, List<List<String>>> entry = entryIterator.next();//有bug
//                if (oldPkConstraints.keySet().contains(indexRelation.get(entry.getKey()))) {
//                    // 旧的PK约束map中有这个index,fk表该index可以连接，转移约束信息
//                    List<List<String>> pkMultiConstraints = oldPkConstraints.get(indexRelation.get(entry.getKey()));
//                    if (pkMultiConstraints.size() > 1) {//[[],[]]
//                        //System.out.println("需要新增List<String>");
//                        // 需要转换多个pk表上的约束到fk上，需要新增List<String>，因为原先newfk的约束每个index下对应的List<List<String>>中只有1个list<String>
//                        for (String pkConstraint : pkMultiConstraints.get(0)) {
//                            String replaceExpression = "(" + joinForeignKey.getColumnGeneExpressions().get(entry.getKey()).getExpression() + ")";
//                            entry.getValue().get(0).add(pkConstraint.replace("k", replaceExpression));
//                        }//先添加第0个
//                        for (int i = 1; i < pkMultiConstraints.size(); i++) {
//                            List<String> fkConstraints = null;
//                            try {
//                                fkConstraints = deepCloneList(oldFkConstraints.get(entry.getKey()).get(0));//新的已经做出改变 所以要用旧的
//                                //old里面始终都有1个String
//                            } catch (IOException e) {
//                                e.printStackTrace();
//                            } catch (ClassNotFoundException e) {
//                                e.printStackTrace();
//                            }
//
//                            for (String pkConstraint : pkMultiConstraints.get(i)) {
//                                String replaceExpression = "(" + joinForeignKey.getColumnGeneExpressions().get(entry.getKey()).getExpression() + ")";
//                                // 新增的List<String>
//                                fkConstraints.add(pkConstraint.replace("k", replaceExpression));
//                                //entry.getValue().add(fkConstraints);
//                            }
//                            entry.getValue().add(fkConstraints);// 我觉得应该添加到这里
//                        }
//                    } else {  // 需要转移的pk表的约束只有一个List<String>
//                        //System.out.println("不需要新增List<String>");
//                        for (List<String> pkConstraints : pkMultiConstraints) {
//                            for (String pkConstraint : pkConstraints) {
//                                String replaceExpression = "(" + joinForeignKey.getColumnGeneExpressions().get(entry.getKey()).getExpression() + ")";
//                                entry.getValue().get(0).add(pkConstraint.replace("k", replaceExpression));
//                            }
//                        }
//                    }
//                } else {
//                    //System.out.println("可以和fk表进行连接的pk表的index已经不存在");
//                    //newFkConstraints.remove(entry.getKey());// 改成用迭代器方法remove
//                    entryIterator.remove();
//                }
//            }
//
//            System.out.println("外键表上的约束：");
//            for(Integer index: newFkConstraints.keySet()){
//                System.out.println("fk表的Key取模:"+index);
//                for(List<String> singleTableConstraint : newFkConstraints.get(index)){
//                    for(String str : singleTableConstraint){
//                        System.out.println(str);
//                    }
//                    System.out.println("--------------------------");
//
//                }
//
//            }
//            tableNames2Deal.remove(fkTableName);
//            // 通过newFkConstraints重写newPkConstraints
//            //newPkConstraints是从空开始构建的
//            newPkConstraints = fkConstraints2PkConstraints(newFkConstraints,joinForeignKey,tableMap.get(fkTableName));
//            // 将由fk转换得到的pk表的约束调整顺序以保证对每张表来说，其新增的约束在整个约束链的后端
//            Iterator<Map.Entry<Integer,List<List<String>>>> reverseIterator = newPkConstraints.entrySet().iterator();
//            while(reverseIterator.hasNext()) {
//                Map.Entry<Integer,List<List<String>>> entry = reverseIterator.next();
//                for (List<String> certainConstraints : entry.getValue()) {
//                    Collections.reverse(certainConstraints);
//                }
//            }//反转
//
//            System.out.println("主键表上的约束：");
//            for(Integer index: newPkConstraints.keySet()){
//                System.out.println("pk表的Key取模:"+index);
//                for(List<String> singleTableConstraint : newPkConstraints.get(index)){
//                    for(String str : singleTableConstraint){
//                        System.out.println(str);
//                    }
//                    System.out.println("-------------------------------------------------------------------------");
//                }
//
//            }
//            tableNames2Deal.remove(pkTableName);
//            tableConstraintMap.get(fkTableName).setConstraint(newFkConstraints);
//            tableConstraintMap.get(pkTableName).setConstraint(newPkConstraints);
//
//            transferingTableName = pkTableName;
//            //分情况：之前Join的PK在这次提供PK，之前Join的FK在这次提供PK
//            while (!tableNames2Deal.isEmpty()) { // 还有未得到最新约束的表
//                Iterator<Map.Entry<String,String>> iterator4JoinRelation = joinRelations.entrySet().iterator();
//                while (iterator4JoinRelation.hasNext()) {
//                    Map.Entry<String,String> entry = iterator4JoinRelation.next();//遍历JoinRelation
//                    //joinRelaion左边的为fk,右边为pk
//                    //如果transferingTableName为fk，而且需要添加filter的表是pk,直接调用fk2pk重写函数
//                    if (entry.getKey().split("\\.")[0].equals(transferingTableName) && tableNames2Deal.contains(entry.getValue().split("\\.")[0])) {
//                        // 找到一个连接关系 通过fk表上的新约束向pk表传递 直接用fk表约束重写pk表约束
//                        //System.out.println("用新fk转移到pk，直接用fk表约束重写pk表约束，操作2");
//
//                        // 为什么这里要用deepCloneMap
//                        Map<Integer,List<List<String>>> newConstraints = deepCloneMap(tableConstraintMap.get(transferingTableName).getConstraint());
//                        Map<Integer,List<List<String>>> newConvertPkConstraints = new HashMap<>();
//                        Table convertTable = tableMap.get(transferingTableName);
//                        ForeignKey convertForeignKey = (ForeignKey) convertTable.getColumnThroughColumnName(entry.getKey().split("\\.")[1]);
//
//                        newConvertPkConstraints = fkConstraints2PkConstraints(newConstraints,convertForeignKey,convertTable);
//
//                        //System.out.println("约束Transfer解决PK重写完成(操作2)，重写约束的主键表名称："+joinRelations.get(entry.getKey()).split("\\.")[0]);
//                        //System.out.println("重写约束的主键表上的约束：");
////                        for(Integer index: newConvertPkConstraints.keySet()){
////                            System.out.println("主键表的Key取模:"+index);
////                            for(List<String> singleTableConstraint : newConvertPkConstraints.get(index)){
////                                for(String str : singleTableConstraint){
////                                    System.out.println(str);
////                                }
////                                System.out.println("--------------------------");
////                            }
////                        }
//
//                        tableConstraintMap.get(joinRelations.get(entry.getKey()).split("\\.")[0]).setConstraint(newConvertPkConstraints);
//                        tableNames2Deal.remove(joinRelations.get(entry.getKey()).split("\\.")[0]);
//
//                        //System.out.println("加入一个可以传递约束的表：" + joinRelations.get(entry.getKey()).split("\\.")[0]);
//                        tableNamesCanTranser.add(joinRelations.get(entry.getKey()).split("\\.")[0]);
//                    } else if (entry.getValue().split("\\.")[0].equals(transferingTableName) && tableNames2Deal.contains(entry.getKey().split("\\.")[0])) {
//                        //JoinRelation右边为transferingTableName，左边为未添加filter的表，这时候由pk向fk传递，使用操作1
//                        // 找到一个连接关系 通过pk表上的新约束向fk表传递
//                        //System.out.println("之前的pk已经更新，需要枚举FK去新增PK上新增的约束，操作1");
//
//                        // 之前两个关系是等价的，即代表了当时join条件下各自表的主键约束信息，现在需要将pk表上的新约束添加到fk表上来保证他们之间的等价性
//                        // 传递约束的pk表的信息
//                        Map<Integer,List<List<String>>> oldPassPkConstraints = tableConstraintMap.get(transferingTableName).getConstraint();
//                        //旧的PK约束是从joinResult中拿来的
//                        Map<Integer,List<List<String>>> newPassPkConstraints = deepCloneMap(oldPassPkConstraints);
//                        //新的PK约束是从allTableConstraints中拿来的
//
//
//                        // 待转换的fk表的信息
//                        Map<Integer,List<List<String>>> newConvertFkConstraints = deepCloneMap(tableConstraintMap.get(entry.getKey().split("\\.")[0]).getConstraint());
//                        Table convertTable = tableMap.get(entry.getKey().split("\\.")[0]);
//                        ForeignKey convertForeignKey = (ForeignKey) convertTable.getColumnThroughColumnName(entry.getKey().split("\\.")[1]);
//                        Map<Integer,Integer> convertIndexRelation = getIndexRelation(convertForeignKey);
//                        Iterator<Map.Entry<Integer,List<List<String>>>> iterator4newConvertFkConstraints = newConvertFkConstraints.entrySet().iterator();
//
//                        while (iterator4newConvertFkConstraints.hasNext()) {
//                            //从pk到fk。fk需要加新filter,所以需要遍历fk
//                            Map.Entry<Integer,List<List<String>>> listEntry = iterator4newConvertFkConstraints.next();//TODO bug
//                            if(newPassPkConstraints.keySet().contains(convertIndexRelation.get(listEntry.getKey()))){
//                                // 找到新旧pk约束的差异，表示出来，理论上应该是长度差且List中的后几个
//                                //System.out.println("旧PK约束的差异");
//                                //System.out.println(oldPassPkConstraints);
//
//                                //System.out.println("新PK约束的差异");
//                                //System.out.println(newPassPkConstraints);
//
//
//                                int pkIndex = convertIndexRelation.get(listEntry.getKey());
//                                int differenceLength = newPassPkConstraints.get(pkIndex).get(0).size() - oldPassPkConstraints.get(pkIndex).get(0).size();
//                                int newLength = newPassPkConstraints.get(pkIndex).get(0).size();
//                                // multiExtraConstraints中只保存了不重复的新约束
//                                Set<List<String>> multiExtraConstraints = new HashSet<>();
//                                for(int j = 0; j < newPassPkConstraints.get(pkIndex).size(); j++){
//                                    List<String> extraConstraints = new ArrayList<>();
//                                    for (int i = 0; i < differenceLength; i ++) {
//                                        extraConstraints.add(newPassPkConstraints.get(pkIndex).get(j).get(newLength - 1 - i));//TODO 有Bug
//                                    }
//                                    multiExtraConstraints.add(extraConstraints);
//                                }
//                                // 把multiExtraConstraints中新增的约束加到fk表约束的后面
//                                int initialSize = listEntry.getValue().size();
//                                for (int i = 0; i < initialSize ; i++) {
//
//                                    for (List<String> certainExtreaConstraints : multiExtraConstraints) {
//                                        List<String> currentConstraints = null;
//                                        try {
//                                            currentConstraints = deepCloneList(newConvertFkConstraints.get(listEntry.getKey()).get(i));
//                                        } catch (IOException e) {
//                                            e.printStackTrace();
//                                        } catch (ClassNotFoundException e) {
//                                            e.printStackTrace();
//                                        }
//                                        for (String certainConstraint : certainExtreaConstraints) {
//                                            String replaceExpression = "(" + convertForeignKey.getColumnGeneExpressions().get(listEntry.getKey()).getExpression() + ")";
//                                            currentConstraints.add(certainConstraint.replace("k",replaceExpression));
//                                        }
//                                        newConvertFkConstraints.get(listEntry.getKey()).add(currentConstraints);
//                                    }
//                                }
//                                // 删除没有添加新约束的约束
//                                for (int i = 0; i < initialSize; i ++) {
//                                    newConvertFkConstraints.get(listEntry.getKey()).remove(0);
//                                }
//
//                            } else {
//                                // newPk中不存在相应的index 与fk中的index相匹配
//                                //newConvertFkConstraints.remove(listEntry.getKey());
//                                iterator4newConvertFkConstraints.remove();//TODO Bug修改
//                            }
//                        }
//                        tableConstraintMap.get(convertTable.getTableName()).setConstraint(newConvertFkConstraints);
//
////                        System.out.println("约束Transfer解决FK构建完成(操作1)，FK表名称："+joinRelations.get(entry.getKey()).split("\\.")[0]);
////                        System.out.println("构建约束的FK表上的约束：");
////                        for(Integer index: newConvertFkConstraints.keySet()){
////                            System.out.println("FK表的Key取模:"+index);
////                            for(List<String> singleTableConstraint : newConvertFkConstraints.get(index)){
////                                for(String str : singleTableConstraint){
////                                    System.out.println(str);
////                                }
////                                System.out.println("--------------------------");
////                            }
////                        }
//
//                        tableNames2Deal.remove(convertTable.getTableName());
//                        //System.out.println("加入一个可以传递约束的表：" + convertTable.getTableName());
//                        tableNamesCanTranser.add(convertTable.getTableName());
//                    }
//                }
//                // 遍历连接关系图之后，当前传递约束的表把约束传递给所有和它有连接关系的表
//                // 选择新的传递关系的表
//                //System.out.println("当前可以传递关系的所有表名：" + tableNamesCanTranser);
//                Iterator<String> setIterator = tableNamesCanTranser.iterator();
//                //System.out.println("新的传递关系的表为：" + setIterator.next());
//                while (setIterator.hasNext()) {
//                    transferingTableName = setIterator.next();
//                    //System.out.println("新的传递关系的表为：" + transferingTableName);
//                    break;
//                }
//                tableNamesCanTranser.remove(transferingTableName);
//            }
//        }else {// 新表主键参与连接
//            System.out.println("新连接的表是Pk表");
//            Map<String,String> joinRelations = root.getPreJoinRelations();
//
//            Map<Integer, List<List<String>>> oldPkConstraints = tableConstraintMap.get(pkTableName).getConstraint();
//            Map<Integer, List<List<String>>> newPkConstraints = new HashMap<>();
//            Map<Integer, List<List<String>>> oldFkConstraints = tableConstraintMap.get(fkTableName).getConstraint();
//            Map<Integer, List<List<String>>> newFkConstraints = deepCloneMap(oldFkConstraints);
//
//            Map<Integer, Integer> indexRelation = getIndexRelation(joinForeignKey);
//            // 将pk表上的约束转移到fk表上 遍历NewFk，按照indexRelation找到对应主键约束的约束，将其加入NewFK(NewFk是复制的OldFk,即为已经构建好的最新中间连接结果)
//            Iterator<Map.Entry<Integer, List<List<String>>>> entryIterator = newFkConstraints.entrySet().iterator();
//            while (entryIterator.hasNext()) {
//                Map.Entry<Integer, List<List<String>>> entry = entryIterator.next();
//                for (List<String> fkConstraints : entry.getValue()) {
//                    for (String pkConstraint : oldPkConstraints.get(indexRelation.get(entry.getKey())).get(0)) {
//                        String replaceExpression = "(" + joinForeignKey.getColumnGeneExpressions().get(entry.getKey()).getExpression() + ")";
//                        fkConstraints.add(pkConstraint.replace("k", replaceExpression));
//                    }
//                }
//            }
//
//            System.out.println("外键表上的约束：");
//            for (Integer index : newFkConstraints.keySet()) {
//                System.out.println("外键表的Key取模:" + index);
//                for (List<String> singleTableConstraint : newFkConstraints.get(index)) {
//                    for (String str : singleTableConstraint) {
//                        System.out.println(str);
//                    }
//                    System.out.println("--------------------------");
//                }
//            }
//            tableNames2Deal.remove(fkTableName);
//            // 将fk表上的约束转移到pk表上，通过newFkConstraints直接重写newPkConstraints
//            newPkConstraints = fkConstraints2PkConstraints(newFkConstraints,joinForeignKey,tableMap.get(fkTableName));
//            // 将由fk转换得到的pk表的约束调整顺序以保证对每张表来说，其新增的约束在整个约束链的后端
//            Iterator<Map.Entry<Integer,List<List<String>>>> reverseIterator = newPkConstraints.entrySet().iterator();
//            while(reverseIterator.hasNext()) {
//                Map.Entry<Integer,List<List<String>>> entry = reverseIterator.next();
//                for (List<String> certainConstraints : entry.getValue()) {
//                    Collections.reverse(certainConstraints);
//                }
//            }
//            System.out.println("主键表上的约束：");
//            for(Integer index: newPkConstraints.keySet()){
//                System.out.println("主键表的Key取模:"+index);
//                for(List<String> singleTableConstraint : newPkConstraints.get(index)){
//                    for(String str : singleTableConstraint){
//                        System.out.println(str);
//                    }
//                    System.out.println("--------------------------");
//                }
//            }
//            tableConstraintMap.get(fkTableName).setConstraint(newFkConstraints);
//            tableConstraintMap.get(pkTableName).setConstraint(newPkConstraints);
//
//            tableNames2Deal.remove(pkTableName);
//            transferingTableName = fkTableName;
//
//            while (!tableNames2Deal.isEmpty()) { // 还有未得到最新约束的表
//                // TODO 迭代器是否可以多次使用
//                Iterator<Map.Entry<String,String>> iterator4JoinRelation = joinRelations.entrySet().iterator();
//                while (iterator4JoinRelation.hasNext()) {
//                    Map.Entry<String,String> entry = iterator4JoinRelation.next();
//                    if (entry.getKey().split("\\.")[0].equals(transferingTableName) && tableNames2Deal.contains(entry.getValue().split("\\.")[0])) {
//                        // 找到一个连接关系 通过fk表上的新约束向pk表传递 直接用fk表约束重写pk表约束
//                        //System.out.println("需要添加Filter的表是PK表，直接通过最新Fliter的FK表重写PK约束，操作2");
//                        //Map<Integer,List<List<String>>> newConstraints = deepCloneMap(newPkConstraints);
//                        Map<Integer,List<List<String>>> newConstraints = deepCloneMap(tableConstraintMap.get(transferingTableName).getConstraint());
//                        Map<Integer,List<List<String>>> newConvertPkConstraints = new HashMap<>();
//                        Table convertTable = tableMap.get(transferingTableName);
//                        ForeignKey convertForeignKey = (ForeignKey) convertTable.getColumnThroughColumnName(entry.getKey().split("\\.")[1]);
//                        newConvertPkConstraints = fkConstraints2PkConstraints(newConstraints,convertForeignKey,convertTable);
//                        //System.out.println("约束Transfer解决PK重写完成(操作2)，重写约束的主键表名称："+joinRelations.get(entry.getKey()).split("\\.")[0]);
//                       // System.out.println("重写约束的主键表上的约束：");
////                        for(Integer index: newConvertPkConstraints.keySet()){
////                            System.out.println("主键表的Key取模:"+index);
////                            for(List<String> singleTableConstraint : newConvertPkConstraints.get(index)){
////                                for(String str : singleTableConstraint){
////                                    System.out.println(str);
////                                }
////                                System.out.println("--------------------------");
////                            }
////                        }
//
//
//                        tableConstraintMap.get(joinRelations.get(entry.getKey()).split("\\.")[0]).setConstraint(newConvertPkConstraints);
//                        tableNames2Deal.remove(joinRelations.get(entry.getKey()).split("\\.")[0]);
//                        //System.out.println("加入一个可以传递约束的表：" + joinRelations.get(entry.getKey()).split("\\.")[0]);
//                        tableNamesCanTranser.add(joinRelations.get(entry.getKey()).split("\\.")[0]);
//                    } else if (entry.getValue().split("\\.")[0].equals(transferingTableName) && tableNames2Deal.contains(entry.getKey().split("\\.")[0])) {
//                        // 找到一个连接关系 通过pk表上的新约束向fk表传递
//                        //System.out.println("需要添加Filter的表是FK表，通过构建好的PK表去添加到FK表中，操作1");
//
//                        Map<Integer,List<List<String>>> oldPassPkConstraints = tableConstraintMap.get(transferingTableName).getConstraint();
//                        Map<Integer,List<List<String>>> newPassPkConstraints = deepCloneMap(oldPassPkConstraints);
//
//                        // 待添加Filter的fk表的信息
//                        Map<Integer,List<List<String>>> newConvertFkConstraints = deepCloneMap(tableConstraintMap.get(entry.getKey().split("\\.")[0]).getConstraint());
//                        Table convertTable = tableMap.get(entry.getKey().split("\\.")[0]);//待更新的表
//                        ForeignKey convertForeignKey = (ForeignKey) convertTable.getColumnThroughColumnName(entry.getKey().split("\\.")[1]);
//                        Map<Integer,Integer> convertIndexRelation = getIndexRelation(convertForeignKey);
//                        Iterator<Map.Entry<Integer,List<List<String>>>> iterator4newConvertFkConstraints = newConvertFkConstraints.entrySet().iterator();
//                        while (iterator4newConvertFkConstraints.hasNext()) {
//                            Map.Entry<Integer,List<List<String>>> listEntry = iterator4newConvertFkConstraints.next();
//                            if(newPassPkConstraints.keySet().contains(convertIndexRelation.get(listEntry.getKey()))){
//                                // 找到新旧pk约束的差异，表示出来，理论上应该是长度差且List中的后几个
////                                System.out.println("旧PK约束的差异");
////                                System.out.println(oldPassPkConstraints);
////
////                                System.out.println("新PK约束的差异");
////                                System.out.println(newPassPkConstraints);
//                                int index = convertIndexRelation.get(listEntry.getKey());
//                                int differenceLength = newPassPkConstraints.get(index).get(0).size() - oldPassPkConstraints.get(index).get(0).size();
//                                int newLength = newPassPkConstraints.get(index).get(0).size();
//                                // multiExtraConstraints中只保存了不重复的新约束
//                                Set<List<String>> multiExtraConstraints = new HashSet<>();
//                                for(int j = 0; j < newPassPkConstraints.get(index).size(); j++){
//                                    List<String> extraConstraints = new ArrayList<>();
//                                    for (int i = 0; i < differenceLength; i ++) {
//                                        extraConstraints.add(newPassPkConstraints.get(index).get(j).get(newLength - 1 - i));
//                                    }
//                                    multiExtraConstraints.add(extraConstraints);
//                                }
//                                // 把multiExtraConstraints中新增的约束加到fk表约束的后面
//                                int initialSize = listEntry.getValue().size();
//                                for (int i = 0; i < initialSize ; i++) {
//
//                                    for (List<String> certainExtreaConstraints : multiExtraConstraints) {
//                                        List<String> currentConstraints = null;
//                                        try {
//                                            currentConstraints = deepCloneList(newConvertFkConstraints.get(listEntry.getKey()).get(i));
//                                        } catch (IOException e) {
//                                            e.printStackTrace();
//                                        } catch (ClassNotFoundException e) {
//                                            e.printStackTrace();
//                                        }
//                                        for (String certainConstraint : certainExtreaConstraints) {
//                                            String replaceExpression = "(" + convertForeignKey.getColumnGeneExpressions().get(listEntry.getKey()).getExpression() + ")";
//                                            currentConstraints.add(certainConstraint.replace("k",replaceExpression));
//                                        }
//                                        newConvertFkConstraints.get(listEntry.getKey()).add(currentConstraints);
//                                    }
//                                }
//                                // 删除没有添加新约束的约束
//                                for (int i = 0; i < initialSize; i ++) {
//                                    newConvertFkConstraints.get(listEntry.getKey()).remove(0);
//                                }
//
//                            } else {
//                                // newPk中不存在相应的index 与fk中的index相匹配
//                                newConvertFkConstraints.remove(listEntry.getKey());
//                            }
//                        }
//                        tableConstraintMap.get(convertTable.getTableName()).setConstraint(newConvertFkConstraints);
//
////                        System.out.println("约束Transfer解决FK构建完成(操作1)，FK表名称："+joinRelations.get(entry.getKey()).split("\\.")[0]);
////                        System.out.println("构建约束的FK表上的约束：");
////                        for(Integer index: newConvertFkConstraints.keySet()){
////                            System.out.println("FK表的Key取模:"+index);
////                            for(List<String> singleTableConstraint : newConvertFkConstraints.get(index)){
////                                for(String str : singleTableConstraint){
////                                    System.out.println(str);
////                                }
////                                System.out.println("--------------------------");
////                            }
////                        }
//
//                        tableNames2Deal.remove(convertTable.getTableName());
//                        //System.out.println("加入一个可以传递约束的表：" + convertTable.getTableName());
//                        tableNamesCanTranser.add(convertTable.getTableName());
//                    }
//                }
//                // 遍历连接关系图之后，当前传递约束的表把约束传递给所有和它有连接关系的表
//                // 选择新的传递关系的表
//                //System.out.println("当前可以传递关系的所有表名：" + tableNamesCanTranser);
//                Iterator<String> setIterator = tableNamesCanTranser.iterator();
//
//                while (setIterator.hasNext()) {
//                    transferingTableName = setIterator.next();
//                    //System.out.println("新的传递关系的表为：" + transferingTableName);
//                    break;
//                }
//                tableNamesCanTranser.remove(transferingTableName);
//            }
//
//        }
//        return pkTableName;
//    }
//
//    public static Map<Integer,List<List<String>>> fkConstraints2PkConstraints(Map<Integer,List<List<String>>> fkConstraints,ForeignKey foreignKey, Table fkTable) {
//        //fk表上在该次join下已经构建好的约束，转移到pk表上
//        //pk表上是有连接条件的约束的
//        Map<Integer,List<List<String>>> pkConstraints = new HashMap<>();
//        Map<Integer,Integer> indexRelation = getIndexRelation(foreignKey);
//
//        Iterator<Map.Entry<Integer,List<List<String>>>> entryIterator = fkConstraints.entrySet().iterator();
//        while (entryIterator.hasNext()) {
//            Map.Entry<Integer,List<List<String>>> entry = entryIterator.next();
//            if (pkConstraints.get(indexRelation.get(entry.getKey())) == null) { // pk表上,该index下暂时没有约束 List<List<>> 有多对一关系时 一个索引下会对应多个约束链
//                List<List<String>> multiConstraints = new ArrayList<>();
//
//                //fk表的生成函数
//                //外键的生成函数只有一次项，只能为long类型
//                AttrGeneFunc attrGeneFunc = foreignKey.getColumnGeneExpressions().get(entry.getKey());
//                BigDecimal coefficient1 = null;
//                BigDecimal coefficient0 = null;
//                if (attrGeneFunc instanceof LinearFunc){
//                    coefficient1 = ((LinearFunc)attrGeneFunc).getCoefficient1();
//                    coefficient0 = ((LinearFunc)attrGeneFunc).getCoefficient0();
//
//                }
////                BigDecimal coefficient1 = foreignKey.getColumnGeneExpressions()[entry.getKey()].getCoefficient1();
////
////                BigDecimal coefficient0 = foreignKey.getColumnGeneExpressions()[entry.getKey()].getCoefficient0();
//                // TODO  如何处理无一次项的表达式转换，已经避免外键生成的表达式无一次项
////                if (coefficient1.compareTo(new BigDecimal("0")) == 0) {
////                    // 外键的生成表达式的一次项系数为0，其值不受主键值的影响
////
////                }
////                ColumnGeneExpression inverseColumnGeneExpression = new ColumnGeneExpression(foreignKey.getColumnGeneExpressions()[entry.getKey()].getTableName(),
////                        foreignKey.getColumnGeneExpressions()[entry.getKey()].getColumnName(),
////                        new BigDecimal("1").divide(coefficient1,BigDecimal.ROUND_HALF_UP),
////                        coefficient0.multiply(new BigDecimal("-1")).divide(coefficient1,BigDecimal.ROUND_HALF_UP));
//                StringBuilder stringBuilder = new StringBuilder();
//                stringBuilder.append("(k - ");
//                stringBuilder.append(coefficient0.intValue());
//                stringBuilder.append(")");
//                stringBuilder.append("/");
//                stringBuilder.append(coefficient1.intValue());
//                String replaceInverseExpression = "(" + stringBuilder.toString() + ")";
//                //构建K_fk=K_pk+xxx
//                int divisor = coefficient1.intValue() * Configurations.getColumnGeneExpressionNum();
//                int minValue = coefficient1.intValue() * entry.getKey() + coefficient0.intValue();
//                int maxValueOfMod = Utils.getMaxValue((int) fkTable.getTableSize(),Configurations.getColumnGeneExpressionNum(),entry.getKey());
//                int maxValue = (int) (coefficient1.intValue() * maxValueOfMod + coefficient0.intValue());
//                int remainder = minValue % divisor;
//
//                String rangeConstraint4Join = minValue+ "<= k <=" + maxValue;
//                // TODO 大bug已修改
//                //k不仅仅要大于minValue，还要小于fk表的范围最大值加上公式所求的值
//
//                //String valueConstraint4Join = " k % " + divisor + " = " + remainder;
//                String valueConstraint4Join = "Mod[ k , " + divisor + " ] == " + remainder;
//                //范围约束和mod约束都是由fk直接转pk的时候使用转换公式求得
//
//                //连接条件的约束
//                for (List<String> certainFkConstraints : fkConstraints.get(entry.getKey())) {
//                    List<String> constraints = new ArrayList<>();
//                    constraints.add(rangeConstraint4Join);//加入范围约束
//                    constraints.add(valueConstraint4Join);
//                    for (String fkConstraint : certainFkConstraints) {
//                        constraints.add(fkConstraint.replace("k",replaceInverseExpression));
//                    }//加入范围约束后又加入转换过的约束
//                    multiConstraints.add(constraints);//把这个list<string>加入
//                }
//                pkConstraints.put(indexRelation.get(entry.getKey()),multiConstraints);
//            } else { // pk表上该index下已经有某些转换过的约束
//                AttrGeneFunc attrGeneFunc = foreignKey.getColumnGeneExpressions().get(entry.getKey());
//                BigDecimal coefficient1 = null;
//                BigDecimal coefficient0 = null;
//                if (attrGeneFunc instanceof LinearFunc){
//                    coefficient1 = ((LinearFunc)attrGeneFunc).getCoefficient1();
//                    coefficient0 = ((LinearFunc)attrGeneFunc).getCoefficient0();
//
//                }
////                BigDecimal coefficient1 = foreignKey.getColumnGeneExpressions()[entry.getKey()].getCoefficient1();
////                BigDecimal coefficient0 = foreignKey.getColumnGeneExpressions()[entry.getKey()].getCoefficient0();
//
//                StringBuilder stringBuilder = new StringBuilder();
//                stringBuilder.append("(k - ");
//                stringBuilder.append(coefficient0.intValue());
//                stringBuilder.append(")");
//                stringBuilder.append("/");
//                stringBuilder.append(coefficient1.intValue());
//                String replaceInverseExpression = "(" + stringBuilder.toString() + ")";
//
//                int divisor = coefficient1.intValue() * Configurations.getColumnGeneExpressionNum();
//
//                int minValue = coefficient1.intValue() * entry.getKey() + coefficient0.intValue();
//                int remainder = minValue % divisor;
//
//                String rangeConstraint4Join = " k >= " + minValue;
//                //TODO modify
//                //String valueConstraint4Join = " k % " + divisor + " = " + remainder;
//                String valueConstraint4Join = "Mod[ k , " + divisor + " ] == " + remainder;
//
//                for (List<String> certainFkConstraints : fkConstraints.get(entry.getKey())) {
//                    List<String> constraints = new ArrayList<>();
//                    constraints.add(rangeConstraint4Join);
//                    constraints.add(valueConstraint4Join);
//                    for (String fkConstraint : certainFkConstraints) {
//                        constraints.add(fkConstraint.replace("k",replaceInverseExpression));
//                    }
//                    pkConstraints.get(indexRelation.get(entry.getKey())).add(constraints);//如果已经有了 直接加进去
//                }
//            }
//        }
//        return  pkConstraints;
//    }
//
//    //map的深拷贝
//    public static <T> Map deepCloneMap(Map obj){
//        T clonedObj = null;
//        try {
//            ByteArrayOutputStream baos = new ByteArrayOutputStream();
//            ObjectOutputStream oos = new ObjectOutputStream(baos);
//            oos.writeObject(obj);
//            oos.close();
//            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
//            ObjectInputStream ois = new ObjectInputStream(bais);
//            clonedObj = (T) ois.readObject();
//            ois.close();
//
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        return (Map) clonedObj;
//    }
//    //List的深拷贝
//    public static <T> List<T> deepCloneList(List<T> src) throws IOException, ClassNotFoundException {
//        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
//        ObjectOutputStream out = new ObjectOutputStream(byteOut);
//        out.writeObject(src);
//
//        ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
//        ObjectInputStream in = new ObjectInputStream(byteIn);
//        @SuppressWarnings("unchecked")
//        List<T> dest = (List<T>) in.readObject();
//        return dest;
//    }
//
//    public static Map<Integer,Integer> getIndexRelation(ForeignKey foreignKey) {
//        Map<Integer,Integer> mapRelation = new HashMap<>();
//
//
//        return mapRelation;
//    }
//
//    public static void drawToFile (String outDotFile, String outPngFile, String drawStr, int queryIndex, int sqlIndex) {
//        File pngFile = new File(outPngFile);
//        if (!pngFile.exists()) {
//            pngFile.mkdir();
//        }
//        String graphPngName = "query_" + queryIndex + "_" + sqlIndex + ".png";
//
//        File dotFile = new File(outDotFile);
//        if (!dotFile.exists()) {
//            dotFile.mkdir();
//        }
//        String dotFileName = dotFile.getName() + File.separator + "querytree_draw_" + queryIndex + "_" + sqlIndex + ".dot";
//
//        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dotFileName), Configurations.getEncodeType()))) {
//            bw.write(drawStr);
//            bw.flush();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        String drawQueryTreeCommand = "dot -Tpng -o " +  pngFile.getName()+ File.separator + graphPngName + " " + dotFileName;
//        //System.out.println("&&&&&&&&"+drawQueryTreeCommand);
//
//        try {
//            Runtime.getRuntime().exec(drawQueryTreeCommand);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    public static void drawQueryTree(RawNode root, int queryIndex, int sqlIndex) {
//        StringBuilder drawStr4artemis = new StringBuilder();
//        StringBuilder drawStr4database = new StringBuilder();
//        drawStr4artemis.append("digraph binaryTree{");
//        drawStr4database.append("digraph binaryTree{");
//        drawQueryTreeNode(root, drawStr4artemis, drawStr4database);
//        drawStr4artemis.append("}");
//        drawStr4database.append("}");
//
//        drawToFile("./TreeGraphDot4artemis", "./TreeGraph4artemis", drawStr4artemis.toString(), queryIndex, sqlIndex);
//        drawToFile("./TreeGraphDot4adatabase", "./TreeGraph4database", drawStr4database.toString(), queryIndex, sqlIndex);
//
//    }
//
//    public static void drawQueryTreeNode(RawNode root, StringBuilder drawStr4artemis, StringBuilder drawStr4database){
//        if(root != null ){
//            String temp1 = "", temp2 = "", temp3 = "", temp4 = "";
//            temp1 += "\"" + root.id + "__" + root.tableName + "__[" + root.middleResult + "]\"";
//            temp2 += "\"" + root.id + "__" + root.tableName + "__[" + root.rowCount + "]\"";
//            if(root.left != null){
//                temp3 = "->"; temp4 = "->";
//                temp3 += "\"" + root.left.id + "__" + root.left.tableName + "__[" + root.left.middleResult + "]\"\r\n";
//                temp4 += "\"" + root.left.id + "__" + root.left.tableName + "__[" + root.left.rowCount + "]\"\r\n";
//                drawStr4artemis.append(temp1 + temp3);
//                drawStr4database.append(temp2 + temp4);
//            }
//            if(root.right != null){
//                temp3 = "->"; temp4 = "->";
//                temp3 += "\"" + root.right.id + "__" + root.right.tableName + "__[" + root.right.middleResult + "]\"\r\n";
//                temp4 += "\"" + root.right.id + "__" + root.right.tableName + "__[" + root.right.rowCount + "]\"\r\n";
//                drawStr4artemis.append(temp1 + temp3);
//                drawStr4database.append(temp2 + temp4);
//            }
//
//            drawQueryTreeNode(root.left, drawStr4artemis, drawStr4database);
//            drawQueryTreeNode(root.right, drawStr4artemis, drawStr4database);
//        }
//
//    }
//
//    //后序递归遍历节点
//    public static void calculateMiddleResult (RawNode root){
//
//        if(root != null){
//            calculateMiddleResult(root.left);
//            calculateMiddleResult(root.right);
//            //计算中间结果
//            //判断节点类型：scan, reader, selection, join, agg
//
//            if(root.nodeType.toLowerCase().contains("scan")){
//                List<List<String>> matches = matchPattern(TABLE_NAME, root.operatorInfo);
//                String tableName = matches.get(0).get(1);
//                root.setTableName(tableName);
//                root.setMiddleResult(tableMap.get(tableName).getTableSize());
//                System.out.println(root.nodeType + " " + root.middleResult);
//            }
//            else if(root.nodeType.toLowerCase().contains("selection")){
//                List<String> predicates = getPredicates(root.operatorInfo.toLowerCase());
//                Map<Integer, List<List<String>>> constraint = predicate2Constraint(predicates);
//
//                if(root.left != null) root.setTableName(root.left.tableName);
//                TableConstraint tableConstraint = new TableConstraint(tableMap.get(root.tableName), constraint,null);
//                tableConstraintMap.put(root.tableName, tableConstraint);//之后的约束就可以直接用
//                //System.out.println("$$$$$$$$$"+root.tableName + " " +tableConstraint.getConstraint());
//
//
//                //TODO  暂时修改
//                FilterResult filterResult = new FilterResult(tableMap.get(root.tableName),null);
//                List<Integer> keyNum = SingleTableWithFilter.Compute(filterResult);
//                //System.out.println(keyNum);
//                int middleResult = 0;
//
//                //TODO 1,8日暂时修改
//                    middleResult +=  keyNum.size();
//
//                root.setMiddleResult(middleResult);
//                System.out.println(root.nodeType + " " + root.middleResult);
//            }
//            else if(root.nodeType.toLowerCase().contains("reader")){
//                //或者: operator info中有data:selection_72，可以用map记录每个节点的名字和rownode类
//                if(root.left != null) {
//                    root.setTableName(root.left.tableName);
//                    root.setMiddleResult(root.left.middleResult);
//                }
//                System.out.println(root.nodeType + " " + root.middleResult);
//            }
//            else if(root.nodeType.toLowerCase().contains("join")){
//                String tableName = getJoinConstraints(root);
//                List<Integer> keyNum = SingleTableWithFilter.Compute(new FilterResult(tableConstraintMap.get(tableName).getTable(),tableConstraintMap.get(tableName).getSubConstraint()));
//                int middleResult = 0;
//
//                    middleResult += keyNum.size();
//
//                root.setMiddleResult(middleResult);
//                System.out.println(root.nodeType + " " + root.middleResult);
//            }
//            else if(root.nodeType.toLowerCase().contains("agg")){
//                root.setMiddleResult(1);
//            }
//        }
//    }
//
//    public static void bfsLogicalPlan(ExecutionNode root){
//        Queue<ExecutionNode> queue = new LinkedList<ExecutionNode>();
//        queue.offer(root);
//        while(!queue.isEmpty()){
//            ExecutionNode now = queue.poll();
//            System.out.println(now.toString()+"----"+now.getInfo()+"----"+now.getType());
//            if(now.leftNode!=null) queue.offer(now.leftNode);
//            if(now.rightNode!=null) queue.offer(now.rightNode);
//        }
//    }
//
//    public static List<Map.Entry<Integer, Long>> sortResults(Map<Integer, Long> results){
//        List<Map.Entry<Integer, Long>> list = new ArrayList<>(results.entrySet());
//        Collections.sort(list, new Comparator<Map.Entry<Integer, Long>>() {
//            //降序排序
//            public int compare(Map.Entry<Integer, Long> o1,
//                               Map.Entry<Integer, Long> o2) {
//                return o2.getValue().compareTo(o1.getValue());
//            }
//
//        });
//
//        return list;
//    }
//
//    public static int calculateL1Distance (List<Map.Entry<Integer, Long>> sortedResults) {
//        int distance = 0;
//        for(int i=0; i<sortedResults.size(); i++){
//            distance += Math.abs(sortedResults.get(i).getKey()-i);
//        }
//        return distance;
//    }
//
//    public static int calculateKendallTau (List<Map.Entry<Integer, Long>> sortedResults) {
//        List<Integer> a = new ArrayList<>();
//        List<Integer> b = new ArrayList<>();
//        for(int i=0; i<sortedResults.size(); i++){
//            a.add(i);
//            b.add(sortedResults.get(i).getKey());
//        }
//        if(a.size()!=b.size()){
//            throw new IllegalArgumentException("Array dimensions disagree");
//        }
//        int[] aIndex = new int[a.size()];
//        int[] bIndex = new int[b.size()];
//        //用数组aIndex存储数组a的索引值,可以看做（i，a[i]）,（i,b[i]）
//        for (int i=0;i<a.size();i++){
//            aIndex[a.get(i)] = i;
//        }
//        //bIndex数组引用A数组的索引，即bIndex数组存储b数组元素在a数组中的对应位置
//        for (int i=0;i<b.size();i++){
//            bIndex[i]=aIndex[b.get(i)];
//        }
//        return sortcount(bIndex);
//    }
//
//    public static int sortcount(int[] a){
//        int length = a.length;
//        int counter = 0;
//        //插入排序的交换次数就是倒置数
//        for (int i=1;i<length;i++){
//            for (int j=i;j>0&&a[j]<a[j-1];j--){
//                int temp = a[j];
//                a[j] = a[j-1];
//                a[j-1] = temp;
//                counter++;
//            }
//        }
//        return counter;
//    }
//
//    public static void main(String[] args) throws SQLException {
//
//        //不再重新生成数据库，从文件读入各种数据，各列生成函数，query
//        ReadDatabaseInfo readFromFile = new ReadDatabaseInfo();
////        Table2Rows = readFromFile.getTable2Rows();
////        Column2Expressions = readFromFile.getColumn2Expressions();
//        tableMap = readFromFile.getTableMap();
//
//        for(Map.Entry<String, Table> entry : tableMap.entrySet())
//            System.out.println(entry.getValue());
//
//        List<String> querys = readFromFile.getQuerys();
//        Map<Integer, Long> results;
//        List<Map.Entry<Integer, Long>> sortedResults;
//        List<List<Map.Entry<Integer, Long>>> sortedResultsList = new ArrayList<>();
//        List<Integer> L1DistanceList = new ArrayList<>();
//        List<Integer> KendallTauDistanceList = new ArrayList<>();
//
//        //连接数据库
//        try{
//            Connection conn = DatabaseConnection.getDatabaseConnection(false);
//
//            int nth_plan_num = 20; //遍历多少计划
//
//            //循环遍历query
//           for(int i=0; i<querys.size(); i++){
//               String sql = querys.get(i);
//               String[] sqlInfoColumns = getSqlInfoColumns();
//               results = new TreeMap<>();
//
//               for(int j=0; j<nth_plan_num; j++) {
//                   //清空
//                   tableConstraintMap = new HashMap<>();
//                   existedTables = new ArrayList<>();
//                   List<String[]> queryPlan = explainQuery(conn, sql, sqlInfoColumns, j);//会连接数据库执行explain select 查询
//
//                   //build physical tree
//                   RawNode rawNodeRoot = buildRawNodeTree(queryPlan);
//                   bfsPhysicalPlan(rawNodeRoot);
//
//                   //计算中间结果
//                   long result = 0;//保存中间结果之和
//                   calculateMiddleResult(rawNodeRoot);
//                   result = calculateResult(rawNodeRoot);
//                   results.put(j, result);
//                   //build logical tree
//                   //ExecutionNode logicplan = buildExecutionTree(rawNodeRoot);
//                   //bfsLogicalPlan(logicplan);
//
//                   drawQueryTree(rawNodeRoot, i, j);
//               }
//               //对计划排序（根据中间结果之和)
//               sortedResults = sortResults(results);
//
//               //求原排序和现排序的距离
//               //Spearman's Footrule (L1距离)
//               int distance1 = calculateL1Distance(sortedResults);
//               System.out.println("L1-distance = "+distance1);
//               //Kendall Tau 用逆序对数量来量化两个排序列表的不一致程度
//               int distance2 = calculateKendallTau(sortedResults);
//               System.out.println("KendallTau-distance = " + distance2);
//               L1DistanceList.add(distance1);
//               KendallTauDistanceList.add(distance2);
//               sortedResultsList.add(sortedResults);
//           }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        System.out.println(sortedResultsList);
//        System.out.println("L1-distance: " + L1DistanceList);
//        System.out.println("KendallTau-distance: " + KendallTauDistanceList);
//
//    }
//
//
//}
//
//
