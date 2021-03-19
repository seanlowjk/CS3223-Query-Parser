package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

/**
 * The Sort merge join operator.
 */
public class SortMergeJoin extends Join {
    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    ArrayList<Condition> condList;  // List of conditions in the order of join conditions

    String rfname;                  // The file name where the right table is materialized
    String bfname;                  // The file name where backtracking table is materialized

    Batch outbatch;                 // Buffer page for output
    Batch outbackbatch;             // Buffer page for backtracking batch

    Batch leftbatch;                // Buffer block for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    Batch backbatch;                // Buffer page for backtrack input stream

    ObjectInputStream in;           // File pointer to the right hand materialized file
    ObjectInputStream bin;          // File pointer to the backtracking file
    ObjectOutputStream bout;        // File pointer write to backtracking file;

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    int bcurs;                      // Cursor for backtracking buffer

    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached
    boolean eosb;                   // Whether end of stream (backtracking) is reached
    boolean sosl;                   // Signifies start of reading from stream (left table)

    int tuplestoclear;              // Number of tuples to clear in the incoming left batch

    Tuple bftuple;                  // To check if reinstating the backtracing file is needed. 
    int actualbend;                 // To keep track the number of tuples in the backtracking file
    int actualbcurs;                // To keep track of the actual pointer in the backtracking file wrt # of tuples. 
    boolean isBacktracking;         // To check for the need of backtracking
    boolean isInitDone;             // To check if the backtracking file is initialized finished. 

    /**
     * Instantiates a new Sort merge join.
     *
     * @param jn the generic join operator containing
     *           information required for join.
     */
    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(),
            jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * Opens connection to base operator
     * and finds the condition list.
     * @return true if successful open.
     */
    @Override
    public boolean open() {
        /** select number of tuples per batch/page **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        ArrayList<Attribute> leftAttributes = new ArrayList<>();
        rightindex = new ArrayList<>();
        ArrayList<Attribute> rightAttributes = new ArrayList<>();
        condList = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();

            leftindex.add(left.getSchema().indexOf(leftattr));
            leftAttributes.add(leftattr);
            rightindex.add(right.getSchema().indexOf(rightattr));
            rightAttributes.add(rightattr);
            condList.add(con);
        }

        /**
         * Prepare the left operator for sorting. 
         */
        Sort leftSort = new Sort(left, leftAttributes, numBuff, false, OpType.SORT);
        leftSort.setSchema(left.getSchema());
        setLeft(leftSort);

        /**
         * Prepare the right operator for sorting. 
         */
        Sort rightSort = new Sort(right, rightAttributes, numBuff, false, OpType.SORT);
        rightSort.setSchema(right.getSchema());
        setRight(rightSort);

        Batch rightpage;

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        eosl = false;
        eosr = true;

        sosl = false;

        tuplestoclear = 0;

        /** for backtracking purposes */
        bftuple = null;
        isBacktracking = false; 
        isInitDone = false;
        actualbend = 0;
        actualbcurs = 0;

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

    /**
     * Gets the next joined batch.
     * @return Batch containing the joined tuples.
     */
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

            /**
             * If backtracking is needed, enter this loop here. 
             */
            if (isBacktracking && isInitDone) {
                if (eosb) {
                    try {
                        /**
                         * Start reading from the buffer file. 
                         */
                        bin = new ObjectInputStream(new FileInputStream(bfname));
                        eosb = false;
                    } catch (IOException io) {
                        System.err.println("SortMergeJoin: Error in reading the backtracking file");
                        System.exit(1);
                    }

                    if (eosl) {
                        return outbatch;
                    } else if (!sosl) {
                        sosl = true;
                    } else {
                        readNextLeftTuple();
                        /**
                         * When a new left tuple is read, 
                         * we would need to reinitialize the backtracking file once again. 
                         */
                        try {
                            bin = new ObjectInputStream(new FileInputStream(bfname));
                            eosb = false;
                        } catch (IOException io) {
                            System.err.println("SortMergeJoin: Error in reading the backtracking file");
                            System.exit(1);
                        } 
                        if (lcurs >= leftbatch.size()) {
                            lcurs = 0;
                            continue;
                        }
                    }
                }

                /**
                 * Execute the backtracking algorithm to produce
                 * the necessary output tuples needed. 
                 */
                executeBacktrack();
                if (lcurs >= leftbatch.size()) {
                    lcurs = 0;
                }

                continue;
            } 

            if (eosr) {
                try {
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    eosr = false;
                } catch (IOException io) {
                    System.err.println("SortMergeJoin: Error in reading the right file");
                    System.exit(1);
                }

                if (eosl) {
                    return outbatch;
                } else if (!sosl) {
                    sosl = true;
                } else {
                    readNextLeftTuple();
                    if (lcurs >= leftbatch.size()) {
                        lcurs = 0; 
                        continue;
                    }
                }
            }

            executeJoin();
            if (lcurs >= leftbatch.size()) {
                lcurs = 0;
            }
        }

        return outbatch;
    }

    /**
     * Deletes files and closes left and right operator.
     * @return true if succesfully closed.
     */
    @Override
    public boolean close() {
        File rf = new File(rfname);
        rf.delete();
        if (bfname != null) {
            File bf = new File(bfname);
            if (bf.exists()) {
                bf.delete();
            }
        }
        return left.close() && right.close();
    }

    /**
     * Backtracking on the left batch.
     */
    private void executeBacktrack() {
        try {
            while (!eosb) {
                /** On a new batch, read a new one */
                if (backbatch == null || bcurs >= backbatch.size()) {
                    bcurs = 0;
                    backbatch = (Batch) bin.readObject();
                /** If a new tuple is read, reinstate the backtracking file */
                } else if (actualbcurs >= actualbend || bftuple == null || Tuple.compareTuples(bftuple, leftbatch.get(lcurs), leftindex, leftindex) != 0) {
                    try {
                        bftuple = leftbatch.get(lcurs);
                        bin = new ObjectInputStream(new FileInputStream(bfname));
                        eosb = false;
                        actualbcurs = 0;
                        bcurs = 0;
                        backbatch = (Batch) bin.readObject();
                    } catch (IOException io) {
                        System.err.println("SortMergeJoin: Error in reading the backtracking file");
                        System.exit(1);
                    } 
                }

                /**
                 * Simply get all the resultant tuples while the 
                 * backtracking cursor is not yet out of bounds. 
                 */
                while (bcurs < backbatch.size()) {
                    Tuple lefttuple = leftbatch.get(lcurs);
                    Tuple backtuple = backbatch.get(bcurs);
                    if (lefttuple.checkJoin(backtuple, leftindex, rightindex, condList)) {
                        Tuple outtuple = lefttuple.joinWith(backtuple);
                        outbatch.add(outtuple);
                        bcurs++;
                        actualbcurs++;
                    } else {
                        isBacktracking = false;
                        eosb = true;
                        return;
                    }
                }
            }
        } catch (EOFException e) {
            try {
                bin.close();
            } catch (IOException io) {
                System.out.println("SortMergeJoin: Error in reading temporary file");
            }
            eosb = true;
        } catch (ClassNotFoundException c) {
            System.out.println("SortMergeJoin: Error in deserialising temporary file ");
            System.exit(1);
        } catch (IOException io) {
            System.out.println("SortMergeJoin: Error in reading temporary file");
            System.exit(1);
        }
    }

    /**
     * Sort Merge Join on the left and right buffer.
     */
    private void executeJoin() {
        try {
            while (!eosr) {
                if (rightbatch == null || rcurs >= rightbatch.size()) {
                    if (rightbatch != null) {
                        rcurs = 0;
                    }
                    rightbatch = (Batch) in.readObject();
                }

                while (rcurs < rightbatch.size()) {
                    Tuple lefttuple = leftbatch.get(lcurs);
                    Tuple righttuple = rightbatch.get(rcurs);
                    if (lefttuple.checkJoin(righttuple, leftindex, rightindex, condList)) {
                        Tuple outtuple = lefttuple.joinWith(righttuple);
                        outbatch.add(outtuple);
                        rcurs++;

                        /**
                         * THe following conditions are used to initialize the backtracking file: 
                         * If the next tuple has the same join attribute value, get ready to initialize 
                         * the backtracking file. 
                         */
                        if (!isBacktracking) {
                            /**
                             * If it is not the last batcg, check with the next tuple
                             * and see if they are the same join attribute value
                             */
                            if (lcurs < leftbatch.size() - 1) {
                                Tuple othertuple = leftbatch.get(lcurs + 1);
                                if (Tuple.compareTuples(lefttuple, othertuple, leftindex, leftindex) == 0) {
                                    outbackbatch = new Batch(batchsize);
                                    actualbend = 0;
                                    initBacktrackingFile();
                                }
                            /** Else, just simply create the backtracking file just in case. */
                            } else {
                                outbackbatch = new Batch(batchsize);
                                actualbend = 0;
                                initBacktrackingFile();
                            }
                        }

                        if (isBacktracking) {
                            outbackbatch.add(righttuple);
                            actualbend ++;
                        } 
            
                        if (outbackbatch != null && outbackbatch.isFull()) {
                            bout.writeObject(outbackbatch);
                            outbackbatch = new Batch(batchsize);
                        }
                    } else {
                        int comparisonfactor = Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex);
                        if (comparisonfactor > 0) {
                            rcurs++;
                        } else { // comparisonfactor < 0;
                            if (isBacktracking) {
                                isInitDone = true; 
                                bout.writeObject(outbackbatch);
                                outbackbatch = new Batch(batchsize);
                                eosb = true; 
                                return; 
                            } else {
                                readNextLeftTuple();
                            }

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

    /**
     * Reads in the left page from the block buffer.
     * @return true for successful reading.
     */
    private boolean readNextLeftBatch() {
        /** new left page is to be fetched**/
        leftbatch = (Batch) left.next();
        if (leftbatch == null) {
            eosl = true;
            return false;
        }
        tuplestoclear = leftbatch.size(); 
        Tuple nexttuple = leftbatch.get(lcurs);
        return true; 
    }

    /**
     * Update the left tuple to be read next.
     */
    private void readNextLeftTuple() {
        lcurs++;
        tuplestoclear--;
        if (tuplestoclear == 0) {
            return;
        } 
    }

    /**
     * Materalise the right batches for future backtracking purposes.
     */
    private void initBacktrackingFile() {
        bfname = "B-SMJtemp-" + String.valueOf(filenum);
        try {
            bout = new ObjectOutputStream(new FileOutputStream(bfname));
            isBacktracking = true; 
            isInitDone = false;
        } catch (IOException io) {
            System.out.println("SortMergeJoin: Error writing to backtracking file");
            System.exit(1);
        }
    }
}
