package ecnu.dbhammer.query;



import ecnu.dbhammer.schema.Table;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 过滤谓词生成，//Filter=Table+谓词，在某个表上生成一个谓词，表示Filter
 */
public class Filter extends QueryNode implements Serializable {

    // 选择谓词
    private Predicate predicate;
    //update by ct: 多个谓词
    private List<Predicate> predicateList = new ArrayList<>();

    // 只有一个孩子节点，该节点可以是：Join，Filter，Table（当前都是Table）
    // 因为这里的query tree丢给数据库执行前会被转化成SQL语句，故这里query tree的结构并不是实际数据库中执行的结构，因此我们这里
    // 都是先选择，再连接。同时，一个数据表上的多个过滤条件也会被组织成一个选择谓词（目前一个数据表上只有一个选择谓词，即不包含逻辑操作符）
    private Table table;

    public Filter(Predicate predicate, Table table) {
        super();
        //this.nodeType = 1;//目前只有两种NodeType 一种是filter 一种是join
        this.predicate = predicate;
        this.predicateList.add(predicate);
        this.table = table;
    }

    @Override
    public String toString() {
        return "Filter [predicate=" + predicate + ", predicateList=" + predicateList + ", table=" + table + "]";
    }

    public Filter(List<Predicate> predicateList, Table table){
        super();
        this.predicateList = predicateList;
        this.table = table;
    }

    public Predicate getPredicate() {
        return predicate;
    }
    public List<Predicate> getPredicateList() { return predicateList;}
    public void addPredicate(Predicate predicate){
        this.predicateList.add(predicate);
    }


    public Table getTable() {
        return table;
    }

    public String getName(){
        return this.table.getTableName();
    }

    public boolean equals(Object o) {
        if (o instanceof Filter) {
            Filter d = (Filter)o;
            return d.table.getTableName().equals(this.table.getTableName());
        }
        return false;
    }

}

