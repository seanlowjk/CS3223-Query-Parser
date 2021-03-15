/**
 * This is base class for the Union operator
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

public class Union extends SetOperator {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    String lfname;                  // The file name where the left table is materialized
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output

    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

    public Union(Operator left, Operator right, int opType) {
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

        /** initialize the cursors of input buffers **/
        eosl = false;
        eosr = false;

        return left.open() && right.open();
    }

    @Override
    public Batch next() {
        outbatch = new Batch(batchsize);

        if (eosl && eosr) {
            return null;
        }

        while (!eosl) {
            Batch leftbatch = left.next();
            if (leftbatch == null) {
                eosl = true;
            } else {
                return leftbatch;
            }
        }

        while (!eosr) {
            Batch rightbatch = right.next();
            if (rightbatch == null) {
                eosr = true;
            } else {
                return rightbatch;
            }
        }

        return null;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        return left.close() && right.close();
    }

    @Override
    public Object clone() {
        Operator newleft = (Operator) left.clone();
        Operator newright = (Operator) right.clone();
        Union newUnion = new Union(newleft, newright, OpType.UNION);
        newUnion.setSchema(newleft.getSchema().joinWith(newright.getSchema()));
        newUnion.setNumBuff(numBuff);
        return newUnion;
    }
}
