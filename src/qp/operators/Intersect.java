/**
 * This is base class for the INTERSECT operator
 **/

package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

public class Intersect extends SetOperator {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the set attributes in left table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Queue<Tuple> leftBlock;         // Buffer block for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached
    int countLeft = 0;

    public Intersect(Operator left, Operator right, int opType) {
        super(left, right, opType);
        schema = left.getSchema();
        numBuff = BufferManager.getNumberOfBuffers();
    }

    /**
     * During open finds the index of the set attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    @Override
    public boolean open() {
        /** select number of tuples per batch/page **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        /** find indices attributes of equality conditions **/
        leftindex = new ArrayList<>();
        for (int i = 0; i < left.getSchema().getAttList().size(); i++) {
            leftindex.add(i);
        }
        Batch rightpage;

        /** initialize the cursors of input buffers **/
        rcurs = 0;
        eosl = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;

        /* Initialize the leftBlock, aka the left buffer. */
        leftBlock = new LinkedList<>();

        /** Right hand side table is to be materialized
         ** for the Intersection to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            rfname = "Itemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("Intersect: Error writing to temporary file");
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
            try {
                // need new left page
                if (leftBlock.isEmpty()) {
                    /** new left page is to be fetched**/
                    leftBlock = generateLeftBuffer(); 
                    if (leftBlock.isEmpty()) {
                        eosl = true;
                        if (outbatch.isEmpty()) {
                            return null;
                        } else {
                            return outbatch;
                        }
                    }
                }

                if (eosr) {
                    /** Whenever a new left page came, we have to start the
                    ** scanning of right table
                    **/
                    try {
                        in = new ObjectInputStream(new FileInputStream(rfname));
                        eosr = false;
                    } catch (IOException io) {
                        System.err.println("Intersect:error in reading the file");
                        System.exit(1);
                    }
                }
            } catch (IOException io) {
                System.err.println("Intersect:error in reading the file");
                System.exit(1);
            } catch (Exception e) {
                System.out.println(e.getMessage());
                System.exit(1);
            }
            getSetBatch();
        }
        return outbatch;
    }

    public void getSetBatch() {
        try {
            while (!eosr) {
                if (rcurs == 0) {
                    rightbatch = (Batch) in.readObject();
                }
                while (!leftBlock.isEmpty()) {
                    Tuple leftTuple = leftBlock.peek();
                    for (int i = rcurs; i < rightbatch.size(); i++) {
                        Tuple rightTuple = rightbatch.get(i);
                        if (leftTuple.checkJoin(rightTuple, leftindex, leftindex)) {
                            outbatch.add(leftTuple);
                            if (outbatch.isFull()) {
                                rcurs = i; 
                                if (!leftBlock.isEmpty() && rcurs != rightbatch.size() - 1) {
                                    rcurs++;
                                } else if (!leftBlock.isEmpty() && rcurs == rightbatch.size() - 1) {
                                    rightbatch = (Batch) in.readObject();
                                    rcurs = 0;
                                } else if (leftBlock.isEmpty() && rcurs != rightbatch.size() - 1) {
                                    // Do Nothing here. 
                                } else { // if (leftBlock.isEmpty() && rcurs == rightbatch.size() - 1) 
                                    rightbatch = (Batch) in.readObject();
                                    rcurs = 0;
                                }
                                break;
                            }
                        } 
                    }
                    rightbatch = (Batch) in.readObject();
                    rcurs = 0;
                }
            }
        } catch (EOFException e) {
            try {
                in.close();
            } catch (IOException io) {
                System.out.println("Intersect: Error in reading temporary file");
            }
            leftBlock.poll(); 
            eosr = true;
        } catch (ClassNotFoundException c) {
            System.out.println("Intersect: Error in deserialising temporary file ");
            System.exit(1);
        } catch (IOException io) {
            System.out.println("Intersect: Error in reading temporary file");
            System.exit(1);
        }

    }

    public Queue<Tuple> generateLeftBuffer() throws Exception {
        int numAvailableBuffers = numBuff - 2;
        Queue<Tuple> leftBuffer = new LinkedList<Tuple>();

        if (eosl) {
            return leftBuffer;
        }

        for (int i = 0; i < numAvailableBuffers; i++) {
            Batch leftBatch = left.next();
            if (leftBatch == null || leftBatch.isEmpty()) {
                return leftBuffer;
            } else {
                for(int j = 0; j < leftBatch.size(); j++) {
                    leftBuffer.add(leftBatch.get(j));
                }
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
        return true;
    }
}
