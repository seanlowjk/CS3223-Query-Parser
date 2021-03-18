/**
 * To projec out the required attributes from the result
 **/

package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

/**
 * The Project operator.
 */
public class Project extends Operator {

    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    int batchsize;                 // Number of tuples per outbatch
    int numOfBuffers;

    /**
     * The following fields are required during execution
     * * of the Project Operator
     */
    Batch inbatch; //pts to the first batch in block
    Batch outbatch;
    Queue<Batch> buffer;

    /**
     * index of the attributes in the base operator
     * * that are to be projected
     */
    int[] attrIndex;

    /**
     * The Index in batch.
     */
    int indexInBatch;
    boolean end;

    /**
     * Instantiates a new Project.
     *
     * @param base the base
     * @param as   the as
     * @param type the type
     */
    public Project(Operator base, ArrayList<Attribute> as, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
        this.numOfBuffers = BufferManager.getNumberOfBuffers();
    }

    /**
     * Gets base operator.
     *
     * @return the base operator.
     */
    public Operator getBase() {
        return base;
    }

    /**
     * Sets base operator.
     *
     * @param base the base operator.
     */
    public void setBase(Operator base) {
        this.base = base;
    }

    /**
     * Gets projection attribute list.
     *
     * @return the projection attribute list.
     */
    public ArrayList<Attribute> getProjAttr() {
        return attrset;
    }


    /**
     * Opens the connection to the base operator.
     * Generates the projection list.
     * @return true if successful open of base, false otherwise.
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;
        /** The following loop finds the index of the columns that
         ** are required from the base operator
         **/
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrset.size()];
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);

            if (attr.getAggType() != Attribute.NONE) {
                System.err.println("Aggragation is not implemented.");
                System.exit(1);
            }

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex[i] = index;
        }
        /**
         * Index on inbatch
         */
        buffer = new LinkedList<Batch>();
        indexInBatch = 0;
        end = false;
        return true;
    }

    /**
     * Gets the next batch of result
     * according to the projection list
     * @return Batch containing tuples
     * with projected values.
     */
    public Batch next() {
        if (end && buffer.isEmpty()) {
            return null;
        }

        outbatch = new Batch(batchsize);

        if (buffer.isEmpty()) {
            end = !genBlock();
            if (buffer.isEmpty()) {
                return null;
            } else {
                inbatch = buffer.peek();
            }
        }

        for (int i = 0; i < inbatch.size(); i++) {
            Tuple basetuple = inbatch.get(i);
            //Debug.PPrint(basetuple);
            //System.out.println();
            ArrayList<Object> present = new ArrayList<>();
            for (int j = 0; j < attrset.size(); j++) {
                Object data = basetuple.dataAt(attrIndex[j]);
                present.add(data);
            }
            Tuple outtuple = new Tuple(present);
            outbatch.add(outtuple);
        }

        buffer.poll();
        inbatch = buffer.peek();
        return outbatch;
    }

    /**
     * Reads in (B-1) batches to form block.
     *
     * @return true if successfully gem a block. false otherwise.
     */
    public boolean genBlock() {
        int numOfInputBuffers = numOfBuffers - 1;
        inbatch = base.next();

        if (inbatch == null) {
            return false; 
        }

        buffer.add(inbatch);

        Batch batch;
        for(int i = 1; i < numOfInputBuffers; i++) {
            batch = base.next();
            if(batch == null) {
                end = true;
                break;
            }
            buffer.add(batch);
        }
        return true; 
    }


    /**
     * Close the base operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        return true;
    }

    /**
     * Clone a new Project Object.
     * @return a new Project Object.
     */
    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Project newproj = new Project(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }

}
