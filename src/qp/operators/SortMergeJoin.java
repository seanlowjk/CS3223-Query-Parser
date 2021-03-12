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
    boolean sosl;                   // Signifies start of reading from stream (left table)

    int gotopointer;                // Find the minimum pointer for the right table
    int newrcurs;                   // New right cursor for backtracking
    int tuplestoclear;              // Number of tuples to clear in the incoming left batch

    int backtrackpointer;
    int backtrackcurs;
    Tuple prevtuple; 

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
        sosl = true;

        /** for traversing the right file for the algorithm */
        gotopointer = 0;
        newrcurs = 0;
        tuplestoclear = 0; 

        /** for backtracking purposes */
        backtrackpointer = 0;
        backtrackcurs = 0;
        prevtuple = null;

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
                if (!readNextLeftBatch()) {
                    return outbatch;
                }
            }

            if (eosr) {
                resetRightFile(backtrackpointer, backtrackcurs);
                if (eosl) {
                    return outbatch;
                } else {
                    if (sosl) {
                        sosl = false;
                    } else {
                        readNextLeftTuple();
                        if (tuplestoclear == 0) {
                            continue;
                        }
                    }


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
        try {
            while (!eosr) {
                if (rightbatch == null || rcurs >= rightbatch.size()) {
                    if (rightbatch != null) {
                        rcurs = 0;
                        gotopointer += rightbatch.size(); 
                    }
                    rightbatch = (Batch) in.readObject();
                }

                while (rcurs < rightbatch.size()) {
                    Tuple lefttuple = leftbatch.get(lcurs);
                    Tuple righttuple = rightbatch.get(rcurs);
                    if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                        // Update the backtracker 
                        if (prevtuple == null || !prevtuple.checkJoin(lefttuple, leftindex, leftindex)) {
                            prevtuple = lefttuple;
                            backtrackpointer = gotopointer;
                            backtrackcurs = rcurs;
                        }

                        Tuple outtuple = lefttuple.joinWith(righttuple);
                        outbatch.add(outtuple);
                        rcurs++;
                        if (outbatch.isFull()) {
                            return;
                        }
                    } else {
                        int comparisonfactor = Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex);
                        if (comparisonfactor > 0) {
                            rcurs++;
                        } else { // comparisonfacotr < 0;
                            readNextLeftTuple();
                            if (lcurs >= leftbatch.size()) {
                                return;
                            } else {
                                break;
                            }
                        }
                    }
                }
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

    private boolean readNextLeftBatch() {
        /** new left page is to be fetched**/
        leftbatch = (Batch) left.next();
        if (leftbatch == null) {
            eosl = true;
            return false;
        }
        tuplestoclear = leftbatch.size(); 
        return true; 
    }

    private void readNextLeftTuple() {
        lcurs++;
        tuplestoclear--;

        if (tuplestoclear == 0) {
            lcurs = 0; 
            return;
        } 

        // Confirm with the backtracker 
        Tuple nexttuple = leftbatch.get(lcurs);
        if (prevtuple != null && nexttuple.checkJoin(prevtuple, leftindex, leftindex)) {
            try {
                in.close();
            } catch (IOException io) {
                System.out.println("SortMergeJoin: Error in reading temporary file");
            }
            resetRightFile(backtrackpointer, backtrackcurs);
        }
    }

    private void resetRightFile(int... args) {
        // args[0] and args[1] return you the pointer you are supposed to backtrack to. 
        if (args.length != 0) {
            gotopointer = args[0] + args[1];
        }
        try {
            in = new ObjectInputStream(new FileInputStream(rfname));
            eosr = false;

            newrcurs = gotopointer;
            while (newrcurs > 0) {
                try {
                    rightbatch = (Batch) in.readObject();
                } catch (Exception e) {
                    eosr = true;
                    return;
                }

                if (newrcurs > rightbatch.size()) {
                    newrcurs -= rightbatch.size();
                } else {
                    rcurs = newrcurs;
                    break;
                }
            }
        } catch (IOException io) {
            System.err.println("SortMergeJoin: Error in reading the right file");
            System.exit(1);
        }
    }
}
