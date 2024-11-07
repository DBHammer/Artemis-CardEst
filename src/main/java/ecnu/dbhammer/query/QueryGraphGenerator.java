package ecnu.dbhammer.query;

import ecnu.dbhammer.configuration.Configurations;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.main.QueryGene;
import ecnu.dbhammer.main.SchemaGene;
import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.schema.DBSchema;
import ecnu.dbhammer.schema.ForeignKey;
import ecnu.dbhammer.schema.Table;
import ecnu.dbhammer.solver.JoinCondition;
import ecnu.dbhammer.utils.GetRandomList;
import org.jgrapht.graph.DirectedPseudograph;
import org.jgrapht.graph.DirectedWeightedPseudograph;
import org.jgrapht.graph.Pseudograph;

import java.util.*;
/**
 * @author tingc
 * @ClassName QueryGraphGenerator.java
 * @Description 生成不同查询图
 * @createTime 2022年3月10日 22:35:00
 */

public class QueryGraphGenerator {

    private DBSchema dbSchema;
    private Pseudograph<String, JoinCondition> joinGraph;
    private List<Table> selectedTables;
    private Set<String> selectedTableNameSet;
    private Set<Table> oneDegreeTable;//保存度为1的表
    private List<Table> allTables;//schema中的所有表
    private Table firstTable;

    public QueryGraphGenerator(DBSchema dbSchema){
        this.dbSchema = dbSchema;
        this.allTables = dbSchema.getTableList();
        this.joinGraph = new Pseudograph<>(JoinCondition.class);
    }

    //Chain:每次向度为1的节点加边
    public void generateQueryGraph4Chain(int tableNum){
        selectedTables = new ArrayList<>();
        selectedTableNameSet = new HashSet<>();
        oneDegreeTable = new HashSet<>(); //保存度为1的表
        // 先随机选一个数据表加进去
        Table newTable = chooseFirstTable();
        firstTable = newTable;
        selectedTables.add(newTable);
        selectedTableNameSet.add(newTable.getTableName());
        oneDegreeTable.add(newTable);
        joinGraph.addVertex(newTable.getTableName());
        System.out.println("第1张： " + newTable.getTableName());
        int cnt = 1;

        // 记录循环次数，避免循环次数过多
        int whileTimes = tableNum > 5 ? tableNum*50 : 100;
        while (true) {
            // 可能不会找到足够的表参与连接
            if (selectedTables.size() == tableNum) {
                break;
            } else if (--whileTimes < 0) {
                System.out.println("chain:没有找到足够的表参与连接");
                break;
            }
            //任选一张不在set中的表，该表需要与度为1的节点有依赖
            int randomIndex = (int) (Math.random() * allTables.size());
            newTable = allTables.get(randomIndex);
            if(selectedTables.contains(newTable))
                continue;

            //判断newTable是否与一个度为1的表有依赖，在chain类型的表中，度为1的表只可能是头和尾
            List<Table> oneDegreeList = new LinkedList<>(oneDegreeTable);
            Collections.shuffle(oneDegreeList);//随机在头和尾添加，增大随机性
            for(Table oneDegree: oneDegreeList){
                ForeignKey newFk = hasEdge(oneDegree, newTable);
                if(newFk != null){
                    selectedTables.add(newTable);
                    selectedTableNameSet.add(newTable.getTableName());
                    joinGraph.addVertex(newTable.getTableName());
                    joinGraph.addEdge(newFk.getTableName(), newFk.getReferencedTableName(), new JoinCondition(newFk,newFk.getReferencePrimaryKey()));
                    if(oneDegreeTable.size() != 1) oneDegreeTable.remove(oneDegree);//更新度为1的set
                    oneDegreeTable.add(newTable);
                    System.out.println("第" + (++cnt) + "张： "+newTable.getTableName());
                    System.out.println("加边： " + newFk.getTableName() + "_"+ newFk.getFkColumnName() + " -> "+ newFk.getReferencedTableName());
                    break;
                }

            }
        }
    }
    //Tree
    public void generateQueryGraph4Tree(int tableNum){
        //如果生成的是Tree类型的Join
        selectedTables = new ArrayList<>();
        selectedTableNameSet = new HashSet<>();
        // 先随机选一个数据表加进去
        Table newTable = chooseFirstTable();
        firstTable = newTable;
        selectedTables.add(newTable);
        selectedTableNameSet.add(newTable.getTableName());
        joinGraph.addVertex(newTable.getTableName());
        System.out.println("第1张： " + newTable.getTableName());
        int cnt = 1;

        // 记录循环次数，避免循环次数过多
        int whileTimes = tableNum > 5 ? tableNum*50 : 100;
        while (true) {
            // 可能不会找到足够的表参与连接
            if (selectedTables.size() == tableNum) {
                break;
            } else if (--whileTimes < 0) {
                System.out.println("tree:没有找到足够的表参与连接");
                break;
            }

            //任选一张不在set中的表，该表与set中的一张表有依赖
            int randomIndex = (int) (Math.random() * allTables.size());
            newTable = allTables.get(randomIndex);
            if(selectedTables.contains(newTable))
                continue;

            List<Table> selectedTablesList = new LinkedList<>(selectedTables);
            Collections.shuffle(selectedTablesList);
            for(Table tableInSet : selectedTablesList){
                ForeignKey newFk = hasEdge(tableInSet, newTable);
                if(newFk != null){
                    selectedTables.add(newTable);
                    selectedTableNameSet.add(newTable.getTableName());
                    joinGraph.addVertex(newTable.getTableName());
                    joinGraph.addEdge(newFk.getTableName(), newFk.getReferencedTableName(), new JoinCondition(newFk,newFk.getReferencePrimaryKey()));
                    System.out.println("第" + (++cnt) + "张： "+newTable.getTableName());
                    System.out.println("加边： " + newFk.getTableName() + "_"+ newFk.getFkColumnName() + " -> "+ newFk.getReferencedTableName());
                    break;
                }
            }
        }
    }
    //Star  如果最大表的数据规模大，会导致求解器变慢
    public void generateQueryGraph4Star(int tableNum){
        //如果是star类型的Join，就选择Schema中最大的表，不断的让比它小的表和它join
        selectedTables = new ArrayList<>();
        // 先选最大的数据表加进去
        Table tmpTable = this.dbSchema.getMaxSizeTable();
        firstTable = tmpTable;
        selectedTables.add(tmpTable);
        joinGraph.addVertex(tmpTable.getTableName());
        System.out.println("第1张： "+tmpTable.getTableName());
        int cnt = 1;
        List<Table> referencedTables = new ArrayList<>();
        Map<Table, ForeignKey> tableToFk = new HashMap<>();
        for (ForeignKey foreignKey : tmpTable.getForeignKeys()) {
            Table table = this.dbSchema.getTableByName(foreignKey.getReferencedTableName());
            //实验用，为了防止表太大，OB跑得慢
            //if(table.getTableSize() > tmpTable.getTableSize() / 100) continue;
            referencedTables.add(table);
            tableToFk.put(table, foreignKey);
        }
        List<Table> randomTableList = GetRandomList.getRandomTableList(referencedTables, tableNum - 1);
        for (Table table : randomTableList) {
            joinGraph.addVertex(table.getTableName());
            joinGraph.addEdge(tmpTable.getTableName(), table.getTableName(), new JoinCondition(tableToFk.get(table),tableToFk.get(table).getReferencePrimaryKey()));
            System.out.println("第" + (++cnt) + "张： "+table.getTableName());
            System.out.println("加边： " + tmpTable.getTableName() + "_"+ tableToFk.get(table).getFkColumnName() + " -> "+ table.getTableName());
        }
        selectedTables.addAll(randomTableList);
    }
    //纯cycle（即n张表组成一个环）
    public void generateQueryGraph4Cycle(int tableNum){
        //前n-1张表先构成chain
        generateQueryGraph4Chain(tableNum-1);
        //最后选一张表与度为1的头尾节点都有依赖关系，即与头尾节点都有边相连，形成环
        List<Table> oneDegreeList = new LinkedList<>(oneDegreeTable);

        int whileTimes = tableNum > 5 ? tableNum*50 : 100;

        while (true) {
            // 可能不会找到足够的表参与连接
            if (--whileTimes < 0) {
                System.out.println("cycle:没有找到最后一张表形成环");
                break;
            }
            //任意选择一张不在set中的表
            int randomIndex = (int) (Math.random() * allTables.size());
            Table newTable = allTables.get(randomIndex);
            if (selectedTables.contains(newTable))
                continue;

            //判断newTable是否和度为1的两个节点都有依赖
            if(oneDegreeList.size() < 2) break;
            ForeignKey newFk1 = hasEdge(oneDegreeList.get(0), newTable);
            ForeignKey newFk2 = hasEdge(oneDegreeList.get(1), newTable);
            if (newFk1 == null || newFk2 == null)
                continue;

            //添加节点和边
            joinGraph.addVertex(newTable.getTableName());
            joinGraph.addEdge(newFk1.getTableName(), newFk1.getReferencedTableName(), new JoinCondition(newFk1,newFk1.getReferencePrimaryKey()));
            joinGraph.addEdge(newFk2.getTableName(), newFk2.getReferencedTableName(), new JoinCondition(newFk2,newFk2.getReferencePrimaryKey()));
            System.out.println("第" + tableNum + "张：" + newTable.getTableName());
            System.out.println("加边： " + newFk1.getTableName() + "_" + newFk1.getFkColumnName() + " -> " + newFk1.getReferencedTableName());
            System.out.println("加边： " + newFk2.getTableName() + "_" + newFk2.getFkColumnName() + " -> " + newFk2.getReferencedTableName());
            oneDegreeTable.clear();
            selectedTables.add(newTable);
            selectedTableNameSet.add(newTable.getTableName());
            break;
        }
    }


    //Cyclic：有环的，tableNum >= 4
    public void generateQueryGraph4Cyclic(int tableNum){
        //表数量小于4时，不适合生成cyclic类型；如果随机到生成cyclic则默认转换成生成chain
        if(tableNum < 4){
            generateQueryGraph4Chain(tableNum);
            return;
        }
        //前3张表先形成chain
        generateQueryGraph4Chain(3);

        int cnt = 3;

        // 记录循环次数，避免循环次数过多
        int whileTimes = tableNum > 5 ? tableNum*50 : 100;
        boolean hasCycle = false;
        while (true) {
            // 可能不会找到足够的表参与连接
            if (selectedTables.size() == tableNum) {
                break;
            } else if (--whileTimes < 0) {
                System.out.println("cyclic:没有找到足够的表参与连接");
                break;
            }
            //有两种操作：一、加边 二、加点  随机选一种，但要保证加边一定会被选择一次（保证有一个环）
            int caseIndex = (int) (Math.random() * 2);
            if(caseIndex == 0){ //加边：从selected集合中任选两张不直接相连的表，如果他两有依赖则加上
                if(addEdgeToCycle())
                    hasCycle = true; //为了确保一定有环
            }else{ //加点：加入新的节点，与selected集合中的任意一张表有依赖
                //任选一张不在set中的表，该表与set中的一张表有依赖
                int randomIndex = (int) (Math.random() * allTables.size());
                Table newTable = allTables.get(randomIndex);
                if(selectedTables.contains(newTable))
                    continue;

                List<Table> selectedTablesList = new LinkedList<>(selectedTables);
                Collections.shuffle(selectedTablesList);
                for(Table tableInSet : selectedTablesList){
                    ForeignKey newFk = hasEdge(tableInSet, newTable);
                    if(newFk != null){
                        selectedTables.add(newTable);
                        selectedTableNameSet.add(newTable.getTableName());
                        joinGraph.addVertex(newTable.getTableName());
                        joinGraph.addEdge(newFk.getTableName(), newFk.getReferencedTableName(), new JoinCondition(newFk,newFk.getReferencePrimaryKey()));
                        System.out.println("第" + (++cnt) + "张： "+newTable.getTableName());
                        System.out.println("加边： " + newFk.getTableName() + "_"+ newFk.getFkColumnName() + " -> "+ newFk.getReferencedTableName());
                        break;
                    }
                }
            }
        }
        if(!hasCycle) //若之前没有生成环，则最后生成一个环
            addEdgeToCycle();
    }

    public boolean addEdgeToCycle(){
        int whileTimes = selectedTables.size() > 5 ? selectedTables.size()*50 : 100;
        while(true) {
            if(--whileTimes < 0)
                break;
            int randomIndex1 = (int) (Math.random() * selectedTables.size());
            List<ForeignKey> allFKs = new LinkedList<>();
            allFKs.addAll(selectedTables.get(randomIndex1).getForeignKeys());
            if (allFKs.size() == 0) continue;
            //因为selectedTables是根据表加入的顺序排列的，所以后面的表一定要当前面表的主键表，为了加快计算速度
            for(int randomIndex2=randomIndex1+1;randomIndex2<selectedTables.size();randomIndex2++) {
                //判断两张表是否已有边
                String table1 = selectedTables.get(randomIndex1).getTableName();
                String table2 = selectedTables.get(randomIndex2).getTableName();
                if (joinGraph.containsEdge(table1, table2)
                        || joinGraph.containsEdge(table2, table1))
                    continue;
                //从table1的依赖里选一个，这样table2作为主键表
                for(int i=0;i<allFKs.size();i++) {
                    ForeignKey chosenFk = allFKs.get(i);
                    if (chosenFk.getTableName().equals(table1) && chosenFk.getReferencedTableName().equals(table2)) { //table1是fk表，table2是pk表
                        joinGraph.addEdge(table1, table2, new JoinCondition(chosenFk, chosenFk.getReferencePrimaryKey()));
                        System.out.println("加边： " + table1 + "_" + chosenFk.getFkColumnName() + " -> " + table2);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    //Clique：全连接；节点数>=3，小集团连接，每两张表都有连接关系
    public void generateQueryGraph4Clique(int tableNum){
        selectedTables = new ArrayList<>();
        selectedTableNameSet = new HashSet<>();
        oneDegreeTable = new HashSet<>(); //保存度为1的表
        // 先随机选一个数据表加进去
        Table tmpTable = chooseFirstTable();
        firstTable = tmpTable;
        selectedTables.add(tmpTable);
        selectedTableNameSet.add(tmpTable.getTableName());
        joinGraph.addVertex(tmpTable.getTableName());
        System.out.println("第1张： " + tmpTable.getTableName());
        int cnt = 1;

        // 记录循环次数，避免循环次数过多
        int whileTimes = tableNum > 5 ? tableNum*50 : 100;
        while (true) {
            // 可能不会找到足够的表参与连接
            if (selectedTables.size() == tableNum) {
                break;
            } else if (--whileTimes < 0) {
                System.out.println("clique:没有找到足够的表参与连接");
                break;
            }
            //加点：必须和set中所有节点都有依赖
            tmpTable = allTables.get((int) (Math.random() * allTables.size()));//随机选择一个表

            if (!selectedTableNameSet.contains(tmpTable.getTableName())) {
                int relationNum = 0; //记录选择的表与几个set中的表有依赖
                List<ForeignKey> relationFKs = new LinkedList<>();
                // 查看当前随机选择的数据表与已选择的数据表之间是否存在参照依赖关系
                // tmpTable是否参照已选择的数据表
//                List<ForeignKey> foreignKeys = tmpTable.getForeignKeys();
//                for (ForeignKey foreignKey : foreignKeys) {
//                    for (int i = 0; i < selectedTables.size(); i++) {
//                        if (foreignKey.getReferencedTableName().equals(selectedTables.get(i).getTableName())) {
//                            relationFKs.add(foreignKey);
//                            relationNum ++;
//                        }
//                    }
//                }

                // 已选择的数据表是否参照tmpTable
                for (int i = 0; i < selectedTables.size(); i++) {
                    List<ForeignKey> foreignKeys = selectedTables.get(i).getForeignKeys();
                    for (ForeignKey foreignKey : foreignKeys) {
                        if (foreignKey.getReferencedTableName().equals(tmpTable.getTableName())) {
                            relationFKs.add(foreignKey);
                            relationNum ++;
                            break;
                        }
                    }
                }
                //不能与所有set中的表相连
                if(relationNum < selectedTables.size()) continue;
                //能够形成全连接; 在图中加入点与边
                selectedTables.add(tmpTable);
                selectedTableNameSet.add(tmpTable.getTableName());
                joinGraph.addVertex(tmpTable.getTableName());
                System.out.println("第"+(++cnt)+"张： " + tmpTable.getTableName());
                for(ForeignKey fk : relationFKs){
                    joinGraph.addEdge(fk.getTableName(), fk.getReferencedTableName(), new JoinCondition(fk,fk.getReferencePrimaryKey()));
                    System.out.println("加边：" + fk.getTableName() + "_" + fk.getFkColumnName() + "->"+fk.getReferencedTableName());
                }
            }
        }
    }

    //Grid: 节点数>=4（且为双数），每次加2个节点和3条边
    public void generateQueryGraph4grid(int tableNum){
        //表数量小于4或是单数时，不适合生成grid类型；如果随机到生成grid则默认转换成生成chain
        if(tableNum < 4 || tableNum % 2 == 1 ){
            generateQueryGraph4Chain(tableNum);
            return;
        }
        //前4张表，先生成一个环
        generateQueryGraph4Cycle(4);
        int cnt = 4;

        //存放度为2的节点
        List<String> twoDegreeList = new LinkedList<>(joinGraph.vertexSet());
        // 记录循环次数，避免循环次数过多
        int whileTimes = tableNum > 5 ? tableNum*50 : 100;
        while (true) {
            // 可能不会找到足够的表参与连接
            if (selectedTables.size() == tableNum) {
                break;
            } else if (--whileTimes < 0) {
                System.out.println("grid:没有找到足够的表参与连接");
                break;
            }

            //第一步：从tableGraph中任选一个度为2的结点
            int randomIndex = (int) (Math.random() * twoDegreeList.size());
            String chosenTableName1 = twoDegreeList.get(randomIndex);

            //第二步：从该度为2的结点的相邻结点中选择也是度为2的一个
            String chosenTableName2 = null;
            Set<JoinCondition> edges = joinGraph.edgesOf(chosenTableName1);
            for(JoinCondition joinCondition : edges){
                if(joinCondition.getLeftAttribute().getTableName().equals(chosenTableName1)&&twoDegreeList.contains(joinCondition.getRightAttribute().getTableName())) {
                    chosenTableName2 = joinCondition.getRightAttribute().getTableName();
                    break;
                }
                if(joinCondition.getRightAttribute().getTableName().equals(chosenTableName1)&&twoDegreeList.contains(joinCondition.getLeftAttribute().getTableName())) {
                    chosenTableName2 = joinCondition.getLeftAttribute().getTableName();
                    break;
                }
            }

            //第三步：任选两张有依赖的表，要求两表分别与chosenTable1和chosenTable2有依赖
            randomIndex = (int) (Math.random() * allTables.size());
            Table newTable1 = allTables.get(randomIndex);
            if(!selectedTables.contains(newTable1)){
                //任选第一张新表的一个fk依赖，就可以得到第二张新表
                List<ForeignKey> fks = new ArrayList<>();
                fks.addAll(newTable1.getForeignKeys());
                Collections.shuffle(fks);
                for(ForeignKey newFk : fks){
                    if(!selectedTableNameSet.contains(newFk.getReferencedTableName())) {
                        Table newTable2 = dbSchema.getTableByName(newFk.getReferencedTableName());
                        //判断两张新表是否与graph中选出的两张表有依赖
                        List<ForeignKey> newFks = isGrid(dbSchema.getTableByName(chosenTableName1), dbSchema.getTableByName(chosenTableName2),
                                newTable1, newTable2);
                        if(newFks == null) continue;
                        //找到了2个点与3条边，加入图中
                        selectedTables.add(newTable1);
                        selectedTables.add(newTable2);
                        selectedTableNameSet.add(newTable1.getTableName());
                        selectedTableNameSet.add(newTable2.getTableName());
                        System.out.println("第"+(++cnt)+"张： " + newTable1.getTableName());
                        System.out.println("第"+(++cnt)+"张： " + newTable2.getTableName());
                        joinGraph.addVertex(newTable1.getTableName());
                        joinGraph.addVertex(newTable2.getTableName());
                        joinGraph.addEdge(newFk.getTableName(),newFk.getReferencedTableName(),new JoinCondition(newFk,newFk.getReferencePrimaryKey()));
                        joinGraph.addEdge(newFks.get(0).getTableName(),newFks.get(0).getReferencedTableName(),new JoinCondition(newFks.get(0),newFks.get(0).getReferencePrimaryKey()));
                        joinGraph.addEdge(newFks.get(1).getTableName(),newFks.get(1).getReferencedTableName(),new JoinCondition(newFks.get(1),newFks.get(1).getReferencePrimaryKey()));
                        System.out.println("加边：" + newFk.getTableName() + "_" + newFk.getFkColumnName() + "->"+newFk.getReferencedTableName());
                        System.out.println("加边：" + newFks.get(0).getTableName() + "_" + newFks.get(0).getFkColumnName() + "->"+newFks.get(0).getReferencedTableName());
                        System.out.println("加边：" + newFks.get(1).getTableName() + "_" + newFks.get(1).getFkColumnName() + "->"+newFks.get(1).getReferencedTableName());
                        //更新度为2的节点集合
                        twoDegreeList.remove(chosenTableName1);
                        twoDegreeList.remove(chosenTableName2);
                        twoDegreeList.add(newTable1.getTableName());
                        twoDegreeList.add(newTable2.getTableName());
                        break;
                    }
                }

            }
        }
    }

    private List<ForeignKey> isGrid(Table chosenTable1, Table chosenTable2, Table newTable1, Table newTable2){
        //chosenTable1——newTable1 and chosenTable2——newTable2
        //或者 chosenTable1——newTable2 and chosenTable2——newTable1
        List<ForeignKey> newFks = new LinkedList<>();
        ForeignKey fk1 = hasEdge(chosenTable1, newTable1);
        ForeignKey fk2 = hasEdge(chosenTable2, newTable2);
        if(fk1 != null && fk2 != null) {
            newFks.add(fk1);
            newFks.add(fk2);
            return newFks;
        }
        fk1 = hasEdge(chosenTable1, newTable2);
        fk2 = hasEdge(chosenTable2, newTable1);
        if(fk1 != null && fk2 != null) {
            newFks.add(fk1);
            newFks.add(fk2);
            return newFks;
        }
        return null;
    }

    //两张表之间是否有依赖，返回FK
    //TODO 这里暂时改成新表只能作为PK表；table1是旧表，table2是新表
    private ForeignKey hasEdge(Table table1, Table table2){
        //得到所有可能依赖关系，从中随机选择符合的
        List<ForeignKey> allFks = new LinkedList<>();
        allFks.addAll(table1.getForeignKeys());
        //allFks.addAll(table2.getForeignKeys());
        Collections.shuffle(allFks);
        for(ForeignKey fk : allFks){
//            if((fk.getTableName().equals(table1.getTableName()) && fk.getReferencedTableName().equals(table2.getTableName()))
//            || (fk.getTableName().equals(table2.getTableName()) && fk.getReferencedTableName().equals(table1.getTableName())))
            if((fk.getTableName().equals(table1.getTableName()) && fk.getReferencedTableName().equals(table2.getTableName())))
                return fk;
        }
        return null;
    }

    //update 22220802 找第一张表，为了后面尽可能连接到指定数目的表，选择外键数目>=连接数目的表作为第一张
    public Table chooseFirstTable(){
        int whileTimes = Math.min(allTables.size() * 5, 100);

        Table firstTable = null;
        while(true){
            if(--whileTimes < 0)
                return allTables.get((int) (Math.random() * allTables.size()));
            firstTable = allTables.get((int) (Math.random() * allTables.size()));
            if(firstTable.getForeignKeys().size() < Configurations.getTableNumPerQuery())
                continue;
            else
                return firstTable;
        }
    }


    public Pseudograph<String, JoinCondition> getJoinGraph() { return joinGraph; }

    public Table getFirstTable() {return firstTable;}

    //测试
    public static void main (String[] args) throws Exception {
        DBSchema dbSchema = SchemaGene.schemaGene();
        RecordLog.recordLog(LogLevelConstant.INFO, "------开始生成Query------");
        List<QueryTree> queryTrees = QueryGene.queryGene(dbSchema);
    }
}
