/**
 * This is base class for the Union operator
 **/

package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.Batch;


/**
 * The Union operator.
 */
public class Union extends SetOperator {
    int batchsize;                  // Number of tuples per out batch
    Batch outbatch;                 // Buffer page for output

    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

    /**
     * Instantiates a new Union.
     *
     * @param left   the left
     * @param right  the right
     * @param opType the op type
     */
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

    /**
     * Gets the next batch in the union table
     * @return next output batch of tuples
     */
    @Override
    public Batch next() {
        outbatch = new Batch(batchsize);

        /**
         * The idea of this algorithm is such that we just
         * simply retrieve all the tuples from the left and right 
         * operators. This is for UNION ALL. 
         */
        if (eosl && eosr) {
            return null;
        }

        /**
         * Retrieve all the left batches till there is none left. 
         */
        while (!eosl) {
            Batch leftbatch = left.next();
            if (leftbatch == null) {
                eosl = true;
            } else {
                return leftbatch;
            }
        }

        /**
         * Retrieve all the right batches till there is none left. 
         */
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
     * Close the base operators.
     */
    public boolean close() {
        return left.close() && right.close();
    }

    /**
     * Clones a new Union object.
     * @return a new Union object.
     */
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
