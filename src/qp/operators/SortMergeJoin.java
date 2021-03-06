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
    private File file;

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

        int tuplesize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tuplesize;
        leftAttrIndexes = new ArrayList<>();
        rightAttrIndexes = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftAttrIndexes.add(left.getSchema().indexOf(leftattr));
            rightAttrIndexes.add(right.getSchema().indexOf(rightattr));
        }

        file = null;
        leftBatch = null;
        rightBatch = null;

        leftPointer = 0;
        rightPointer = 0;
        isEndOfLeftStream = false;
        isEndOfRightStream = false;

        inputStream = null;
    }

    @Override
    public boolean open() {
        if (!left.open() || !right.open()) {
            return false;
        }

        file = getRelationBatches(right);
        readNextLeftBatch();
        return true; 
    }

    @Override
    public Batch next() {
        if (isEndOfLeftStream) {
            return null;
        }

        return getNextBatch();
    }

    @Override
    public boolean close() {
        return true;
    }

    private Batch getNextBatch() {
        Batch nextBatch = new Batch(batchSize);
        if (!hasInitializedInputStream()) {
            return null; 
        }
        readNextRightBatch();

        while (!nextBatch.isFull()) {
            Tuple leftTuple = leftBatch.get(leftPointer);
            Tuple rightTuple = rightBatch.get(rightPointer);
            int comparisonResult = Tuple.compareTuples(leftTuple, rightTuple, 
                leftAttrIndexes, rightAttrIndexes);
            if (comparisonResult < 0) {
                hasInitializedInputStream();
                readNextLeftTuple();
            } else {
                if (comparisonResult == 0) {
                    Tuple resultTuple = leftTuple.joinWith(rightTuple);
                    nextBatch.add(resultTuple);
                }

                readNextRightTuple();
                if (isEndOfRightStream) {
                    hasInitializedInputStream();
                    readNextLeftTuple(); 
                    readNextRightBatch();
                }
                if (isEndOfLeftStream) {
                    break; 
                }
            }
        }

        return nextBatch;
    }

    private void readNextLeftTuple() {
        leftPointer ++;
        if (leftPointer >= leftBatch.size()) {
            readNextLeftBatch();
            leftPointer = 0;
        }
    }

    private void readNextLeftBatch() {
        if (leftBatch == null) {
            leftBatch = left.next();
        }

        if (leftBatch == null) {
            isEndOfLeftStream = true; 
        }
    }

    private void readNextRightTuple() {
        rightPointer ++;
        if (rightPointer >= rightBatch.size()) {
            readNextRightBatch();
            rightPointer = 0;
        }
    }

    private void readNextRightBatch() {
        if (rightBatch == null) {
            rightBatch = right.next();
        }

        if (rightBatch == null) {
            isEndOfRightStream = true; 
        }
    }

    private boolean hasInitializedInputStream() {
        if (inputStream == null) {
            try {
                FileInputStream fileInputStream = new FileInputStream(file);
                inputStream = new ObjectInputStream(fileInputStream);
                isEndOfRightStream = false;
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        return true;
    }

    private File getRelationBatches(Operator op) {
        Batch batch;
        try {
            File file = new File(FILE_HEADER);
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            ObjectOutputStream outputStream = new ObjectOutputStream(fileOutputStream);
            while ((batch = op.next()) != null) {
                outputStream.writeObject(batch);
            }
            outputStream.close();
            return file;
        } catch (IOException io) {
            return null; 
        }
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
