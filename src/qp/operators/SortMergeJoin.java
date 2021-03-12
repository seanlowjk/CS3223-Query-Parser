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
        super(jn.getLeft(), jn.getRight(),  
            jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();

        int tuplesize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tuplesize;
        leftAttrIndexes = new ArrayList<>();
        rightAttrIndexes = new ArrayList<>();

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

        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftAttrIndexes.add(left.getSchema().indexOf(leftattr));
            rightAttrIndexes.add(right.getSchema().indexOf(rightattr));
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
        file.delete();

        if (!left.close() || !right.close()) {
            return false;
        }
        return true;
    }

    private Batch getNextBatch() {
        Batch nextBatch = new Batch(batchSize);
        if (!hasInitializedInputStream()) {
            return null; 
        }

        while (!nextBatch.isFull()) {
            Tuple leftTuple = leftBatch.get(leftPointer);
            Tuple rightTuple = rightBatch.get(rightPointer);
            int comparisonResult = Tuple.compareTuples(leftTuple, rightTuple, 
                leftAttrIndexes, rightAttrIndexes);
            if (comparisonResult < 0) {
                hasInitializedInputStream();
                readNextLeftTuple();
                if (isEndOfLeftStream) {
                    return nextBatch; 
                }
            } else {
                if (comparisonResult == 0) {
                    Tuple resultTuple = leftTuple.joinWith(rightTuple);
                    nextBatch.add(resultTuple);
                }
                readNextRightTuple();
                
                if (isEndOfRightStream) {
                    readNextLeftTuple(); 
                    if (isEndOfLeftStream) {
                        return nextBatch; 
                    }

                    hasInitializedInputStream();
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
        leftBatch = left.next();

        if (leftBatch == null || leftBatch.size() == 0) {
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
        try {
            rightBatch = (Batch) inputStream.readObject();
        } catch (Exception e) {
            isEndOfRightStream = true; 
            rightBatch = null;
        }

        if (rightBatch == null || rightBatch.size() == 0) {
            rightBatch = null;
            isEndOfRightStream = true; 
        }
    }

    private boolean hasInitializedInputStream() {
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            inputStream = new ObjectInputStream(fileInputStream);
            readNextRightBatch();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private File getRelationBatches(Operator op) {
        Batch batch;
        try {
            file = new File(FILE_HEADER);
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
}
