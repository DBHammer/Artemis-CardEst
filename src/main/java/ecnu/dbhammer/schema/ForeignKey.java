package ecnu.dbhammer.schema;



import ecnu.dbhammer.data.AttrValue;
import ecnu.dbhammer.data.DataType;
import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.result.TreeNode;
import ecnu.dbhammer.schema.genefunc.AttrGeneFunc;
import ecnu.dbhammer.schema.genefunc.LinearFunc;
import ecnu.dbhammer.schema.genefunc.ModFunc;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;
/**
 * 外键类，里面含有基于主键的数据映射
 */
public class ForeignKey extends Attribute implements Cloneable, Serializable {

    //该外键参考了哪个主键
    private PrimaryKey primaryKey;

    //外键属性拥有的生成函数个数
    private List<AttrGeneFunc> attrGeneFuncs;


    //TODO 加入生成函数Pool 生成函数生成器

    public ForeignKey() {
        super();
    }




    public ForeignKey(PrimaryKey primaryKey, String localTableName, int localTableSize, String fkColumnName) {
        super(localTableName,localTableSize,fkColumnName,DataType.INTEGER);
        this.primaryKey = primaryKey;

        initColumnGeneExpressions();//每一个外键对象构造时，需要初始化该外键的生成函数
    }

    /**
     * 外键属性的生成函数，关系到表之间的元组关系映射
     * 外键参照其他表的主键，必然是int型
     * 根据参照表的tableSize控制外键生成的表达式参数，使根据表达式生成的外键在参照表的主键范围内
     * 目前只采用一次项的生成公式,且参数为整数，因为使用再复杂的函数，也只是维持了一个映射关系
     */
    private void initColumnGeneExpressions4ForeignKey() {

        int columnGeneExpressionNum = 1;
        this.attrGeneFuncs = new ArrayList<>();

        for (int i = 0; i < columnGeneExpressionNum; i++) {
            BigDecimal cofficient1 = new BigDecimal((long) (Math.random() * (this.primaryKey.getTableSize() / super.getTableSize())));
            if (this.primaryKey.getTableSize() / super.getTableSize() < 1) {
                RecordLog.recordLog(LogLevelConstant.ERROR, "参数表选择错误！");
                System.out.println("参照表选择错误！");//根据表达式生成的外键在参照表的主键范围内所以需要referencedTablesize>localTablesize
            }
            // 保证外键上生成表达式的一次项系数不为0且正
            if (((this.primaryKey.getTableSize() / super.getTableSize()) >= 1) && (cofficient1.compareTo(new BigDecimal("0")) == 0)) {
                cofficient1 = new BigDecimal("1");//一次项都为1
            }
            BigDecimal cofficient0;

            long coff0 = (long) (Math.random() * (this.primaryKey.getTableSize() - cofficient1.longValue() * super.getTableSize()));

            cofficient0 = new BigDecimal(coff0);

            //测试 不使用cofficient0

            attrGeneFuncs.add(new LinearFunc(cofficient1, new BigDecimal(0)));
        }

    }

    private void initColumnGeneExpressions(){
//        if(this.primaryKey.getTableSize() <= super.getTableSize()){
//            //大表参考小表，这种情况控制生成函数使其满射，使用取模函数
//            this.attrGeneFuncs = new ArrayList<>();
//            attrGeneFuncs.add(new ModFunc(this.primaryKey.getTableSize()));
//        }else if(this.primaryKey.getTableSize() > super.getTableSize()){
//            this.initColumnGeneExpressions4ForeignKey();
//        }
        //update by ct: 外键都用取模函数
        this.attrGeneFuncs = new ArrayList<>();
        attrGeneFuncs.add(new ModFunc(this.primaryKey.getTableSize()));
    }

    /**
     * 给定一个Y值集合，根据映射得到所有X值集合
     * @param results
     * @return
     */
    public List<Integer> getReverse(List<Integer> results){
        List<Integer> ans = new ArrayList<>();
        for(Integer result : results) {
            ans.addAll(this.getReverse(new BigDecimal(result)));
        }
        return  ans;
    }

    public List<TreeNode> getReverseSet(List<TreeNode> nodes){
        List<TreeNode> finalLevel = new ArrayList<>();
        for(TreeNode node : nodes) {
            //对于每一个node，求其反函数，得到一个set，然后把这个set加到children中
            List<Integer> ansSet = this.getReverse(new BigDecimal(node.value));
            for (Integer ans : ansSet) {
                TreeNode child = new TreeNode(ans);
                node.childrens.add(child);
                child.parent = node;
            }
            finalLevel.addAll(node.childrens);
        }
        return finalLevel;
    }


    /**
     * 给定一个result，对于外键来说，找出所有的x,使得外键生成函数组生成的值为result
     * 基本思路：遍历外键生成函数Group中所有的生成函数，
     * @param result
     * @return
     */
    public List<Integer> getReverse(BigDecimal result){
        //由外键的所有生成函数都参与计算，求的所有复合条件的K值
        //但是生成函数是如何实现取反规则的，自己去定义就行
        List<Integer> ans = new ArrayList<>();
        for(int i=0; i<attrGeneFuncs.size();i++){
            AttrGeneFunc attrGeneFunc = attrGeneFuncs.get(i);
            List<Integer> single = attrGeneFunc.getReverse(result,0 ,this.getTableSize()-1);

            //这里来了一个y，用某个反函数取反，得到的x的集合set要必须符合集合中的每一个x都必须符合使用该生成函数的规则
            //例如，外键使用了取模规则，mod4余 0 使用f0(x) mod4余1 使用f1(x) mod4余2 使用f2(x) mod4余3 使用f3(x)
            // 这里传入一个y,使用生成函数f0(x)取反，得到一个解集set，但是set中的所有元素并不一定都是mod4余0的
            int mod = this.getColumnGeneExpressionNum();

            int finalI = i;

            ans.addAll(single.stream().filter((Integer q)->(q%mod == finalI && q>=0 && q<this.getTableSize())).collect(Collectors.toList()));
        }
        return ans;

    }


    /**
     * 给定pk集合，根据pk集合算出fk值的集合
     * @param pks
     * @return
     */
    public List<Integer> attrEvaluate(List<Integer> pks){
        List<Integer> ans = new ArrayList<>();
        for(Integer pk : pks){
            ans.add(this.attrEvaluate(pk).intValue());
        }
        return  ans;
    }

    public List<TreeNode> attrEvaluateSet(List<TreeNode> nodes){
        List<TreeNode> finalLevel = new ArrayList<>();
        for(TreeNode node : nodes){
            //对于每一个node，求其正函数，将该node求的的值连接到该Node的children中
            int value = this.attrEvaluate(node.value).intValue();
            TreeNode child = new TreeNode(value);
            node.childrens.add(child);
            child.parent = node;
            finalLevel.addAll(node.childrens);
        }
        return finalLevel;
    }

    @Override
    public BigDecimal attrEvaluate(int pk) {
        BigDecimal value = attrGeneFuncs.get(0).evaluate(pk);
        return value;
    }

    @Override
    public BigDecimal attrEvaluate2(int pk, int drivingCol) {
        return null;
    }

    /**
     * 给出一个范围，正向计算求的下一个范围，用于给出fk表的范围求pk表的范围
     * @param keyRange
     * @return
     */
    public List<Integer> getNextKeyRange(List<Integer> keyRange){
        List<Integer> ans = new ArrayList<>();
        for(Integer i : keyRange){
            int value = this.attrEvaluate(i).intValue();
            ans.add(value);
        }
        return ans;
    }

    public List<Integer> getNextKeyRange(int key){
        List<Integer> ans = new ArrayList<>();

        int value = this.attrEvaluate(key).intValue();
        ans.add(value);

        return ans;
    }

    /**
     * 给出一个范围，取反计算求的下一个范围，用于给出pk表的范围求fk表的范围
     * @param keyRange
     * @return
     */
    public List<Integer> getReverseKeyRange(List<Integer> keyRange){
        List<Integer> ans = new ArrayList<>();
        for(Integer i : keyRange){
            List<Integer> single = this.getReverse(new BigDecimal(i));
            ans.addAll(single.stream().filter((p)-> p>=0 && p<super.getTableSize()).collect(Collectors.toList()));
        }
        return ans;
    }

    public List<Integer> getReverseKeyRange(int key){
        List<Integer> ans = new ArrayList<>();

        List<Integer> single = this.getReverse(new BigDecimal(key));
        ans.addAll(single.stream().filter((p)-> p>=0 && p<super.getTableSize()).collect(Collectors.toList()));

        return ans;
    }

    @Override
    public AttrValue geneAttrValue(int pk) {


        Integer value = attrEvaluate(pk).intValue();

        return new AttrValue(DataType.INTEGER,value);

    }

    @Override
    public int getColumnGeneExpressionNum() {
        return this.attrGeneFuncs.size();
    }


    public String geneValue(int primaryKey) {//外键数据的生成
        BigInteger value = attrGeneFuncs.get(0).evaluate(primaryKey).toBigInteger();
        return (value.intValue()) + "";
    }

    public Attribute getReferencePrimaryKey(){
        return this.primaryKey;
    }

    public String getReferencedTableName() {
        return this.primaryKey.getTableName();
    }

    public String getReferencedTablePrimaryKeyName() {
        return this.primaryKey.getPrimaryKeyName();
    }

    public String getFkColumnName() {
        return super.getAttName();
    }

    public long getReferencedTableSize() {
        return this.primaryKey.getTableSize();
    }


    public List<AttrGeneFunc> getColumnGeneExpressions() {
        return attrGeneFuncs;
    }


    public DataType getDataType() {
        return super.getDataType();
    }

    @Override
    public boolean judgeProperties() {
        return false;
    }


    public void setReferencedTableName (String referencedTableName) {this.primaryKey.setTableName(referencedTableName);}

    public void setReferencedTableSize (int referencedTableSize) {this.primaryKey.setTableSize(referencedTableSize);}

    public void setReferencedTablePrimaryKeyName (String referencedTablePrimaryKeyName) {this.primaryKey.setAttName(referencedTablePrimaryKeyName);}

    public void setIntDataType (DataType dataType) {super.setDataType(dataType);}

    public void setLocalTableName (String localTableName) {super.setTableName(localTableName);}

    public void setLocalTableSize (int localTableSize) {super.setTableSize(localTableSize);}

    public void setFkColumnName (String fkColumnName) {super.setAttName(fkColumnName);}

    public void setPrimaryKey(PrimaryKey pk){
        this.primaryKey = pk;
    }

    public void setColumnGeneExpressions (List<AttrGeneFunc> attrGeneFuncs) {this.attrGeneFuncs = attrGeneFuncs;}

    // 深拷贝
    @Override
    public ForeignKey clone() {
        return new ForeignKey(primaryKey,super.getTableName(),super.getTableSize(),super.getAttName());
    }

    @Override
    public String toString() {
        return "\n\t"+this.primaryKey.toString()
                + ", localTableName=" + super.getTableName() + ", localTableSize=" + super.getTableSize() + ", fkColumnName="
                + super.getAttName() + ", columnGeneExpressions="
                + attrGeneFuncs + "]";
    }

}
