package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.List; 

public class SortMergeJoin extends Join {
    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table

    String lfname;                  // The file name where the left table is materialized 
    String rfname;                  // The file name where the right table is materialized
    
    Batch outbatch;                 // Buffer page for output 
    Batch leftbatch;                // Buffer block for left input stream
    ObjectInputStream lin;          // File pointer to the right hand materialized file
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream rin;          // File pointer to the right hand materialized file

    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(),  
            jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    @Override
    public boolean open() {
        /** select number of tuples per batch/page **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }
        Batch rightpage;

        /** initialize the cursors of input buffers **/
        rcurs = 0;
        eosl = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;

        inputStream = null;

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

    /*
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
    */
}
