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

    int backtrackpointer;           // Find the minimum pointer for the right table
    int newrcurs;                   // New right cursor for backtracking
    int tuplestoclear;              // Number of tuples to clear in the incoming left batch

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

        /** for backtracking purposes for the algorithm */
        backtrackpointer = 0;
        newrcurs = 0;
        tuplestoclear = 0; 

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
                System.out.println("SortMergeJoin: Error writing to temporary file");
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
            if (lcurs == 0 && tuplestoclear == 0) {
                /** new left page is to be fetched**/
                leftbatch = (Batch) left.next();
                if (leftbatch == null) {
                    eosl = true;
                    return outbatch;
                }
                tuplestoclear = leftbatch.size(); 
            }

            if (eosr) {
                try {
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    eosr = false;

                    newrcurs = backtrackpointer;
                    while (newrcurs > 0) {
                        try {
                            rightbatch = (Batch) in.readObject();
                        } catch (Exception e) {
                            eosl = true; 
                            eosr = true;
                            return outbatch;
                        }

                        if (newrcurs >= rightbatch.size()) {
                            newrcurs -= rightbatch.size();
                        } 
                    }


                } catch (IOException io) {
                    System.err.println("SortMergeJoin: Error in reading the right file");
                    System.exit(1);
                }
            }
            getJoinBatch();
            if (lcurs >= leftbatch.size()) {
                lcurs = 0;
            }
        }
        return outbatch;
    }

    @Override
    public boolean close() {
        File rf = new File(rfname);
        rf.delete();
        return left.close() && right.close();
    }

    private void getJoinBatch() {
        int i = 0;
        int j = 0; 
        try {
            while (!eosr) {
                if (rightbatch == null || rcurs >= rightbatch.size()) {
                    if (rightbatch != null) {
                        rcurs = 0;
                        backtrackpointer += rightbatch.size(); 
                    }
                    rightbatch = (Batch) in.readObject();
                }

                for (i = lcurs; i < leftbatch.size(); ++i) {
                    boolean istupledone = false;
                    for (j = rcurs; j < rightbatch.size(); ++j) {
                        Tuple lefttuple = leftbatch.get(i);
                        Tuple righttuple = rightbatch.get(j);
                        if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                            Tuple outtuple = lefttuple.joinWith(righttuple);
                            outbatch.add(outtuple);
                            if (outbatch.isFull()) {
                                rcurs = ++j; 
                                return;
                            }
                        } else {
                            int comparisonfactor = Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex);
                            if (comparisonfactor > 0) {
                                rcurs = ++j;
                                if (j == rightbatch.size()) {  
                                    return; 
                                }
                            } else { // comparisonfacotr < 0;
                                rcurs = j;
                                lcurs++;
                                tuplestoclear--;
                                if (lcurs >= leftbatch.size()) {
                                    return;
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                }
                rcurs = ++j;
            }
        } catch (EOFException e) {
            try {
                in.close();
            } catch (IOException io) {
                System.out.println("SortMergeJoin: Error in reading temporary file");
            }
            eosr = true;
        } catch (ClassNotFoundException c) {
            System.out.println("SortMergeJoin: Error in deserialising temporary file ");
            System.exit(1);
        } catch (IOException io) {
            System.out.println("SortMergeJoin: Error in reading temporary file");
            System.exit(1);
        }
    }
}
