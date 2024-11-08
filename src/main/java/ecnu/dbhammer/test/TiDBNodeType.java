package ecnu.dbhammer.test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class TiDBNodeType{
    private static final HashSet<String> READER_NODE_TYPES = new HashSet<>(Arrays.asList("TableReader", "IndexReader", "IndexLookUp"));
    private static final HashSet<String> PASS_NODE_TYPES = new HashSet<>(Arrays.asList("Projection", "TopN", "Sort", "HashAgg", "StreamAgg", "IndexScan",  "IndexFullScan"));
    private static final HashSet<String> JOIN_NODE_TYPES = new HashSet<>(Arrays.asList("HashRightJoin", "HashLeftJoin", "IndexMergeJoin", "IndexHashJoin", "IndexJoin", "MergeJoin", "HashJoin"));
    private static final HashSet<String> FILTER_NODE_TYPES = new HashSet<>(Collections.singletonList("Selection"));
    private static final HashSet<String> TABLE_SCAN_NODE_TYPES = new HashSet<>(Arrays.asList("TableScan", "TableFullScan", "TableRowIDScan", "TableRangeScan"));
    private static final HashSet<String> INDEX_SCAN_NODE_TYPES = new HashSet<>(Arrays.asList("IndexScan", "IndexRangeScan", "IndexFullScan"));
    private static final HashSet<String> RANGE_SCAN_NODE_TYPES = new HashSet<>(Arrays.asList("IndexRangeScan", "TableRangeScan"));




    public boolean isReaderNode(String nodeType) {
        return READER_NODE_TYPES.contains(nodeType);
    }


    public boolean isPassNode(String nodeType) {
        return PASS_NODE_TYPES.contains(nodeType);
    }


    public boolean isJoinNode(String nodeType) {
        return JOIN_NODE_TYPES.contains(nodeType);
    }


    public boolean isFilterNode(String nodeType) {
        return FILTER_NODE_TYPES.contains(nodeType);
    }


    public boolean isTableScanNode(String nodeType) {
        return TABLE_SCAN_NODE_TYPES.contains(nodeType);
    }


    public boolean isIndexScanNode(String nodeType) {
        return INDEX_SCAN_NODE_TYPES.contains(nodeType);
    }

    public boolean isRangeScanNode(String nodeType) {
        return RANGE_SCAN_NODE_TYPES.contains(nodeType);
    }
}

