package ecnu.dbhammer.result;
//设置NodeResult接口，表示每个类型的QueryNode的应该拿去给求解器计算的表达式
//比如：
// 一张单独的表的Result就是该张表取值范围
// 一个单独的带Filter的表就是取值范围上加filter
// 一个JoinResult就是约束满足问题CSP

public interface NodeResult {

}
