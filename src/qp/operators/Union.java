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

    ObjectInputStream lin;          // File pointer to the left hand materialized file
    ObjectInputStream rin;          // File pointer to the right hand materialized file

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

        Batch leftpage;
        if (!left.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            lfname = "Utemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(lfname));
                while ((leftpage = left.next()) != null) {
                    out.writeObject(leftpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("Union: Error writing to temporary file");
                return false;
            }
            if (!left.close()) {
                return false;
            }
        }

        Batch rightpage;
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            rfname = "Utemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("Union: Error writing to temporary file");
                return false;
            }
            return right.close();
        }
    }

    @Override
    public Batch next() {
        outbatch = new Batch(batchsize);

        if (eosl && eosr) {
            return null;
        }

        while (!eosl) {
            try {
                Batch leftbatch = (Batch) lin.readObject();
                return leftbatch;
            } catch (EOFException e) {
                try {
                    lin.close();
                } catch (IOException io) {
                    System.out.println("Union: Error in reading temporary file");
                }
                eosl = true; 
            } catch (ClassNotFoundException c) {
                System.out.println("Union: Error in deserialising temporary file ");
                System.exit(1);
            } catch (IOException io) {
                System.out.println("Union: Error in reading temporary file");
                System.exit(1);
            }
        }

        while (!eosr) {
            try {
                Batch rightbatch = (Batch) lin.readObject();
                return rightbatch;
            } catch (EOFException e) {
                try {
                    rin.close();
                } catch (IOException io) {
                    System.out.println("Union: Error in reading temporary file");
                }
                eosr = true;
            } catch (ClassNotFoundException c) {
                System.out.println("Union: Error in deserialising temporary file ");
                System.exit(1);
            } catch (IOException io) {
                System.out.println("Union: Error in reading temporary file");
                System.exit(1);
            }
        }

        return null;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
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
