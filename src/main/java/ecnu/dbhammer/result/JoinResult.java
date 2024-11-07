package ecnu.dbhammer.result;

import ecnu.dbhammer.constant.LogLevelConstant;
import ecnu.dbhammer.log.RecordLog;
import ecnu.dbhammer.schema.ForeignKey;
import ecnu.dbhammer.schema.Table;

import java.util.*;

import ecnu.dbhammer.query.*;
import ecnu.dbhammer.solver.CSPDefination;
import ecnu.dbhammer.solver.FilterCardinalitySolver;
import ecnu.dbhammer.utils.DeepClone;
import org.apache.commons.lang3.tuple.Pair;

/**
 * 通过生成的Join，获得Join上的约束条件，以便求解器进行求解
 */
public class JoinResult implements NodeResult {

    // 从当前join生成joinResult
    private Join join;
    private ComplexJoin complexJoin;

    // 进行join后包含的所有表
    private List<Table> tables;

    private FilterResult fkTableFilterResult;
    private FilterResult pkTableFilterResult;

    // 非初始join结果由一个joinResult和一个filterResult构成
    private JoinResult joinResult = null;
    private FilterResult filterResult = null;

    // JoinResult类的作用是定义CSPDefination
    private CSPDefination cspDefination;

    // 第一次join JoinResult由两个FilterResult得出
    // TODO 添加一个FilterREsult和Table直接Join的
    public JoinResult(FilterResult fkTableFilterResult, FilterResult pkTableFilterResult, Join join) throws Exception {
        this.fkTableFilterResult = fkTableFilterResult;
        this.pkTableFilterResult = pkTableFilterResult;
        this.join = join;// join结果
        geneFirstJoinResult();// 初始化了pkTable,fkTable,
    }
    // 非第一次join JoinResult由一个JoinResult和一个FilterResult得出

    public JoinResult(JoinResult joinResult, FilterResult filterResult, Join join) throws Exception {
        this.joinResult = joinResult;
        this.filterResult = filterResult;
        this.join = join;// join结果
        geneJoinResult4More();// 需改动
    }

    public JoinResult(JoinResult joinResult, FilterResult filterResult, ComplexJoin complexJoin) throws Exception {
        this.joinResult = joinResult;
        this.filterResult = filterResult;
        this.complexJoin = complexJoin;
        geneJoinResult4More4ComplexJoin();// 需改动
    }

    // 第一次进行join，结果由两个filter组成
    public void geneFirstJoinResult() throws Exception {

        this.tables = new ArrayList<>();
        tables.add(this.join.getLeftChildNode().getTable());
        tables.add(this.join.getRightChildNode().getTable());

        RecordLog.recordLog(LogLevelConstant.INFO, "Google Solver");

        // 先求两个过滤算子
        List<Integer> fkRange = FilterCardinalitySolver.Compute(fkTableFilterResult, 0);

        if (fkRange.size() != 0) {
            RecordLog.recordLog(LogLevelConstant.INFO, fkTableFilterResult.getFilter().getTable().getTableName()
                    + "过滤出主键集合范围：[" + fkRange.get(0) + " " + fkRange.get(fkRange.size() - 1) + "]");
        }

        List<Integer> pkRange = FilterCardinalitySolver.Compute(pkTableFilterResult, 0);
        if (pkRange.size() != 0) {
            RecordLog.recordLog(LogLevelConstant.INFO, pkTableFilterResult.getFilter().getTable().getTableName()
                    + "过滤出主键集合范围：[" + pkRange.get(0) + " " + pkRange.get(pkRange.size() - 1) + "]");
        }

        List<Pair<Integer, Integer>> domainList = new ArrayList<>();
        if (fkTableFilterResult.getTable().getTableName()
                .equals(this.join.getLeftChildNode().getTable().getTableName())) { // 即先加左表范围，再加右表范围
            if (fkRange.size() != 0 && pkRange.size() != 0) {
                domainList.add(Pair.of(fkRange.get(0), fkRange.get(fkRange.size() - 1)));
                domainList.add(Pair.of(pkRange.get(0), pkRange.get(pkRange.size() - 1)));
            }

        } else if (pkTableFilterResult.getTable().getTableName()
                .equals(this.join.getLeftChildNode().getTable().getTableName())) {
            if (fkRange.size() != 0 && pkRange.size() != 0) {
                domainList.add(Pair.of(pkRange.get(0), pkRange.get(pkRange.size() - 1)));
                domainList.add(Pair.of(fkRange.get(0), fkRange.get(fkRange.size() - 1)));
            }

        }
        if (fkRange.size() != 0 && pkRange.size() != 0) {
            Table biggestTable = fkRange.size() > pkRange.size() ? fkTableFilterResult.getTable()
                    : pkTableFilterResult.getTable();

            this.cspDefination = new CSPDefination(biggestTable, this.tables, domainList,
                    this.join.getJoinConditionList());
        }

    }

    public void geneJoinResult4More() throws Exception {
        // 得出当前状态下所有表的主键约束信息，存入allTablesConstraints中
        tables = new ArrayList<>();

        tables.addAll(joinResult.getTables());

        tables.add(filterResult.getTable());
        // 得到所有表，为之前joinResult中的表加上新的filterResult中的表

        RecordLog.recordLog(LogLevelConstant.INFO, "Google求解器");
        List<Integer> newRange = FilterCardinalitySolver.Compute(filterResult, 0);
        if (newRange.size() != 0) {
            RecordLog.recordLog(LogLevelConstant.INFO, filterResult.getFilter().getTable().getTableName() +
                    "过滤出主键集合范围：[" + newRange.get(0) + " " + newRange.get(newRange.size() - 1) + "]");
            this.cspDefination = joinResult.getCspDefination().addConstraints(filterResult.getTable(),
                    Pair.of(newRange.get(0), newRange.get(newRange.size() - 1)), this.join.getJoinConditionList());
        }
    }

    public void geneJoinResult4More4ComplexJoin() throws Exception {
        // 得出当前状态下所有表的主键约束信息，存入allTablesConstraints中
        tables = new ArrayList<>();

        tables.addAll(joinResult.getTables());

        tables.add(filterResult.getTable());
        // 得到所有表，为之前joinResult中的表加上新的filterResult中的表

        RecordLog.recordLog(LogLevelConstant.INFO, "Google求解器");
        List<Integer> newRange = FilterCardinalitySolver.Compute(filterResult, 0);
        if (newRange.size() != 0) {
            RecordLog.recordLog(LogLevelConstant.INFO, filterResult.getFilter().getTable().getTableName() +
                    "过滤出主键集合范围：[" + newRange.get(0) + " " + newRange.get(newRange.size() - 1) + "]");
            this.cspDefination = joinResult.getCspDefination().addConstraints(filterResult.getTable(),
                    Pair.of(newRange.get(0), newRange.get(newRange.size() - 1)), this.join.getJoinConditionList());
        }
    }

    public List<Table> getTables() {
        return this.tables;
    }

    public Join getJoin() {
        return this.join;
    }

    public ComplexJoin getComplexJoin() {
        return this.complexJoin;
    }

    public CSPDefination getCspDefination() {
        return this.cspDefination;
    }

}