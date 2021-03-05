package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.List; 

public class SortMergeJoin extends Join {
    private static final String FILE_HEADER = "SMtemp";

    private List<File> sortedRunsFiles;
    private int fileNumber;

    private int batchSize;
    private List<Integer> leftAttrIndexes;
    private List<Integer> rightAttrIndexes;

    private Batch leftBatch;
    private Batch rightBatch; 

    private int leftPointer;
    private int rightPointer;
    private boolean isEndOfLeftStream;
    private boolean isEndOfRightStream; 

    private ObjectInputStream inputStream;

    public SortMergeJoin(Join jn) {
        super(getSortOperator(jn.getLeft(), jn), 
            getSortOperator(jn.getRight(), jn), 
            jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    private static Sort getSortOperator(Operator base, Join jn) {
        List<Condition> conditions = jn.getConditionList();
        List<Attribute> attributes = new ArrayList<>();
        for (Condition condition : conditions) {
            attributes.add(condition.getLhs());
        }
        int numBuff = jn.getNumBuff();
        boolean isDescending = false; 
        int opType = OpType.SORT;
        return new Sort(base, attributes, numBuff, isDescending, opType);
    }
}
