package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.BatchUpdateException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

public class BlockNestedJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Queue<Batch> leftBlock;         // Buffer block for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int lcurs;                      // Cursor for left side buffer
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
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;

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
            rfname = "NJtemp-" + String.valueOf(filenum);
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
        try {
            if (leftBlock == null) {
                genBlocks();
            }




        } catch (Exception e) {

        }
    }

    public void genBlocks() throws Exception {

        Queue<Batch> relationBlock = new LinkedList<Batch>();

        if(!right.open() || !left.open()) {
            throw new Exception("File supposed to be opened!");
        }

        if(eosl) {
            return;
        }

        for(int i = 0; i < numBuff- 2; i++) {

            Batch leftBatch = left.next();

            if(leftBatch == null) {
                eosl = true;
                leftBlock = relationBlock;
                return;
            }

            relationBlock.add(leftBatch);
        }

        leftBlock = relationBlock;
    }


}
