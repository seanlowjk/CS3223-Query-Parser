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
import java.util.LinkedList;
import java.util.Queue;

public class BlockNestedJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    ArrayList<Condition> condList;  // List of conditions in the order of join conditions
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    ArrayList<Batch> leftBlock;     // Buffer block for left input stream
    Batch leftbatch;                // Buffer page for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int lbatch;                     // Cursor for left side buffer for batch number
    int lcurs;                      // Cursor for left side buffer for tuple number
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    @Override
    public boolean open() {
        /** select number of tuples per batch/page **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        condList = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
            condList.add(con);
        }
        Batch rightpage;

        /** initialize the cursors of input buffers **/
        lbatch = 0;
        lcurs = 0;
        rcurs = 0;
        eosl = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;

        /* Initialize the leftBlock, aka the left buffer. */
        leftBlock = new ArrayList<>();

        /** Right hand side table is to be materialized
         ** for the  Block Nested join to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            rfname = "BNJtemp-" + String.valueOf(filenum);
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

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    @Override
    public Batch next() {
        int h, i, j;
        if (eosl) {
            return null;
        }
        outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {
            if (lbatch == 0 && lcurs == 0 && eosr == true) {
                /** new left page is to be fetched**/
                try {
                    leftBlock = generateLeftBuffer();
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    System.exit(1);
                }
                if (leftBlock.size() == 0) {
                    eosl = true;
                    return outbatch;
                }
                /** Whenever a new left page came, we have to start the
                 ** scanning of right table
                 **/
                try {
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    eosr = false;
                } catch (IOException io) {
                    System.err.println("NestedJoin:error in reading the file");
                    System.exit(1);
                }
            }

            while (eosr == false) {
                try {
                    if (rcurs == 0 && lcurs == 0 && lbatch == 0) {
                        rightbatch = (Batch) in.readObject();
                    }
                    for (h = lbatch; h < leftBlock.size(); ++h) {
                        leftbatch = leftBlock.get(h);
                        for (i = lcurs; i < leftbatch.size(); ++i) {
                            for (j = rcurs; j < rightbatch.size(); ++j) {
                                Tuple lefttuple = leftbatch.get(i);
                                Tuple righttuple = rightbatch.get(j);
                                if (lefttuple.checkJoin(righttuple, leftindex, rightindex, condList)) {
                                    Tuple outtuple = lefttuple.joinWith(righttuple);
                                    outbatch.add(outtuple);
                                    if (outbatch.isFull()) {
                                        if (i == leftbatch.size() - 1 && j == rightbatch.size() - 1) {  //case 1
                                            lbatch = h + 1;
                                            lcurs = 0;
                                            rcurs = 0;
                                        } else if (i != leftbatch.size() - 1 && j == rightbatch.size() - 1) {  //case 2
                                            lbatch = h;
                                            lcurs = i + 1;
                                            rcurs = 0;
                                        } else if (i == leftbatch.size() - 1 && j != rightbatch.size() - 1) {  //case 3
                                            lbatch = h;
                                            lcurs = i;
                                            rcurs = j + 1;
                                        } else {
                                            lbatch = h;
                                            lcurs = i;
                                            rcurs = j + 1;
                                        }
                                        return outbatch;
                                    }
                                }
                            }
                            rcurs = 0;
                        }
                        lcurs = 0;
                    }
                    lbatch = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("NestedJoin: Error in reading temporary file");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("NestedJoin: Error in deserialising temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("NestedJoin: Error in reading temporary file");
                    System.exit(1);
                }
            }
        }
        return outbatch;
    }

    public ArrayList<Batch> generateLeftBuffer() throws Exception {
        int numAvailableBuffers = numBuff - 2;
        ArrayList<Batch> leftBuffer = new ArrayList<>();

        if (eosl) {
            return leftBuffer;
        }

        for (int i = 0; i < numAvailableBuffers; i++) {
            Batch leftBatch = left.next();
            if (leftBatch == null || leftBatch.isEmpty()) {
                return leftBuffer;
            } else {
                leftBuffer.add(leftBatch);
            }
        }
        return leftBuffer;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return left.close() && right.close();
    }
}
