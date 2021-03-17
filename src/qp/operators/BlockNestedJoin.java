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


/**
 * The type Block nested join.
 */
public class BlockNestedJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Queue<Tuple> leftBlock;         // Buffer block for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

    /**
     * Instantiates a new Block nested join.
     *
     * @param jn the jn
     */
    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     *
     * @return
     */
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

        /* Initialize the leftBlock, aka the left buffer. */
        leftBlock = new LinkedList<>();

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
     *
     * @return
     */
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
                        System.err.println("BlockNestedJoin:error in reading the file");
                        System.exit(1);
                    }
                }
            } catch (IOException io) {
                System.err.println("BlockNestedJoin:error in reading the file");
                System.exit(1);
            } catch (Exception e) {
                System.out.println(e.getMessage());
                System.exit(1);
            }
            getJoinBatch();
        }
        return outbatch;
    }


    /**
     * Gets join batch.
     */
    public void getJoinBatch() {
        try {
            while (!eosr) {
                if (rcurs == 0) {
                    rightbatch = (Batch) in.readObject();
                }
                while (!leftBlock.isEmpty()) {
                    Tuple leftTuple = leftBlock.peek();
                    for (int i = rcurs; i < rightbatch.size(); i++) {
                        Tuple rightTuple = rightbatch.get(i);
                        if (leftTuple.checkJoin(rightTuple, leftindex, rightindex, conditionList)) {
                            Tuple joinedTuple = leftTuple.joinWith(rightTuple);
                            outbatch.add(joinedTuple);
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
                System.out.println("NestedJoin: Error in reading temporary file");
            }
            leftBlock.poll(); 
            eosr = true;
        } catch (ClassNotFoundException c) {
            System.out.println("NestedJoin: Error in deserialising temporary file ");
            System.exit(1);
        } catch (IOException io) {
            System.out.println("NestedJoin: Error in reading temporary file");
            System.exit(1);
        }

    }

    /**
     * Generate left buffer queue.
     *
     * @return the queue
     * @throws Exception the exception
     */
    public Queue<Tuple> generateLeftBuffer() throws Exception {

        if(left instanceof Scan) {
            return ((Scan) left).nextBlock((numBuff - 2) * batchsize);
        }

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
     *
     * @return
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return left.close() && right.close();
    }
}
