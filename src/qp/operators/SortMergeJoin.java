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

    String rfname;                  // The file name where the right table is materialized
    
    Batch outbatch;                 // Buffer page for output 
    Batch leftbatch;                // Buffer block for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;          // File pointer to the right hand materialized file

    int lcurs;                      // Cursor for left side buffer 
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
        lcurs = 0; 
        rcurs = 0;
        eosl = false;
        eosr = true;

        if (!right.open()) {
            return false;
        } else {
            filenum++;
            rfname = "SMJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
        }

        return left.open();
    }

    @Override
    public Batch next() {
        outbatch = new Batch(batchsize);

        if (eosl) {
            return null;
        }

        while (!outbatch.isFull()) {
            if (lcurs == 0) {
                /** new left page is to be fetched**/
                leftbatch = (Batch) left.next();
                if (leftbatch == null) {
                    eosl = true;
                    return outbatch;
                }
            }

            if (eosr) {
                try {
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    eosr = false;
                } catch (IOException io) {
                    System.err.println("SortMergeJoin:error in reading the right file");
                    System.exit(1);
                }
            }
            getJoinBatch();
        }
        return outbatch;
    }

    @Override
    public boolean close() {
        File rf = new File(rfname);
        rf.delete();
        return true;
    }

    private Batch getJoinBatch() {
        Batch nextbatch = new Batch(batchsize);
        return nextbatch;
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
