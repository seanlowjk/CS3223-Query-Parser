/**
 * Scans the base relational table
 **/

package qp.operators;

import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Scan operator - read data from a file.
 */
public class Scan extends Operator {

    String filename;       // Corresponding file name
    String tabname;        // Table name
    int batchsize;         // Number of tuples per out batch;
    ObjectInputStream in;  // Input file being scanned
    boolean eos;           // To indicate whether end of stream reached or not

    /**
     * Constructor - just save filename
     */
    public Scan(String tabname, int type) {
        super(type);
        this.tabname = tabname;
        filename = tabname + ".tbl";
    }

    public String getTabName() {
        return tabname;
    }

    /**
     * Open file prepare a stream pointer to read input file.
     */
    public boolean open() {
        /** num of tuples per batch**/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        eos = false;
        try {
            in = new ObjectInputStream(new FileInputStream(filename));
        } catch (Exception e) {
            System.err.println(" Error reading " + filename);
            return false;
        }
        return true;
    }

    /**
     * Next operator - get a tuple from the file.
     **/
    public Batch next() {
        /** The file reached its end and no more to read **/
        if (eos) {
            close();
            return null;
        }
        Batch tuples = new Batch(batchsize);
        while (!tuples.isFull()) {
            try {
                Tuple data = (Tuple) in.readObject();
                tuples.add(data);
            } catch (ClassNotFoundException cnf) {
                System.err.println("Scan:Class not found for reading file  " + filename);
                System.exit(1);
            } catch (EOFException EOF) {
                /** At this point incomplete page is sent and at next call it considered
                 ** as end of file
                 **/
                eos = true;
                return tuples;
            } catch (IOException e) {
                System.err.println("Scan:Error reading " + filename);
                System.exit(1);
            }
        }
        return tuples;
    }

    /**
     * Close the file.. This routine is called when the end of filed
     * * is already reached
     **/
    public boolean close() {
        try {
            in.close();
        } catch (IOException e) {
            System.err.println("Scan: Error closing " + filename);
            return false;
        }
        return true;
    }

    /**
     * Clones a copy of the Scan object
     * @return new scan object
     */
    public Object clone() {
        String newtab = tabname;
        Scan newscan = new Scan(newtab, optype);
        newscan.setSchema((Schema) schema.clone());
        return newscan;
    }

}
