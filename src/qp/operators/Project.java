/**
 * To projec out the required attributes from the result
 **/

package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.BatchUtils;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

public class Project extends Operator {

    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    int batchsize;                 // Number of tuples per outbatch
    int numOfBuffers;

    /**
     * The following fields are requied during execution
     * * of the Project Operator
     **/
    Batch inbatch;
    Batch outbatch;

    /**
     * Buffering
     */
    ArrayList<Batch> buffer;

    /**
     * index of the attributes in the base operator
     * * that are to be projected
     **/
    int[] attrIndex;

    /**
     * index of attr with aggregation in attrset
     */
    ArrayList<Integer> aggGrpByAttrPtr; //agg functions related to grpby
    ArrayList<Integer> aggAttrPtr;
    ArrayList<Integer> attrPtr;
    int indexInBatch;

    HashMap<Integer, ArrayList<File>> grpFiles;
    int grpCount;

    public Project(Operator base, ArrayList<Attribute> as, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
        this.numOfBuffers = BufferManager.getNumberOfBuffers();
        this.buffer = new ArrayList<Batch>();
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getProjAttr() {
        return attrset;
    }


    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
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
        aggAttrPtr= new ArrayList<Integer>();
        aggGrpByAttrPtr= new ArrayList<Integer>();
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);

            if (attr.getAggType() != Attribute.NONE) {
                aggAttrPtr.add(i);
                //check for whether is a grpbyAgg
            } else {
                attrPtr.add(i);
            }

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex[i] = index;
        }
        /**
         * Index on inbatch
         */
        indexInBatch = 0;
        grpCount = 0;
        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);


        if(aggAttrPtr.isEmpty() && aggGrpByAttrPtr.isEmpty()) {
            /** all the tuples in the inbuffer goes to the output buffer **/
            inbatch = base.next();
            if(inbatch == null) {
                return null;
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
        } else if (base instanceof GroupBy && !aggGrpByAttrPtr.isEmpty()) {
            /**
             * GroupBy with aggregate
             */
            ArrayList<Object> aggResult = new ArrayList<Object>();
            HashMap<Integer, ArrayList<Object>> aggGrpBYResult = new HashMap<Integer, ArrayList<Object>>();
            ArrayList<Integer> grpSize = new ArrayList<Integer>();

            ArrayList<ArrayList<Object>> tupleData = new ArrayList<ArrayList<Object>>();
            while(inbatch != null) {


            }

        } else {

        }

        return outbatch;
    }


    public void getAggResults(ArrayList<Object> aggResult, HashMap<Integer, Object> aggGrpByResult) {

    }

    public void genGrp() {
        int inputBuffers = numOfBuffers - 1;
        int blockCount = 0;
        buffer.clear();

        Batch bufferBatch =  new Batch(batchsize);
        Tuple prevBatchLastTuple = null;
        for(int i = 0; i < inputBuffers; i++) {

            if(inbatch == null || inbatch.size() == indexInBatch +1) {
                inbatch = base.next();
                if(inbatch == null) {
                    return;
                }
                indexInBatch = 0;
            }

            for(int j = indexInBatch; indexInBatch < inbatch.size(); j++) {
                if(!bufferBatch.isEmpty()) {
                  Tuple grpTuple = bufferBatch.get(0); //compare the currTuple to a tuple thats in the grp
                  Tuple currTuple = inbatch.get(j);

                  if(((GroupBy)base).compare(grpTuple, currTuple) != 0) {
                      indexInBatch = j;
                      break;
                    }
                } else if (!buffer.isEmpty()) {
                    Tuple currTuple = inbatch.get(j);
                    if(((GroupBy)base).compare(prevBatchLastTuple, currTuple) != 0) {
                        indexInBatch = j;
                        break;
                    }
                }

                bufferBatch.add(inbatch.get(j));

                if(bufferBatch.isFull()) {
                    prevBatchLastTuple = inbatch.get(j);
                    buffer.add(bufferBatch);
                    bufferBatch = new Batch(batchsize);

                }
                indexInBatch++;
            }

            inputBuffers++;

        }
        blockCount++;
        //writeout batch
        if(grpFiles == null) {
            grpFiles = new HashMap<Integer, ArrayList<File>>();
        }

        String grpFileName = String.format("groupby-%d%d", grpCount+ 1, blockCount);
        File grpFile = BatchUtils.writeRuns(buffer, grpFileName);

        if(grpFiles.containsKey(grpCount)) {
            ArrayList<File> files = grpFiles.get(grpCount);
            files.add(grpFile);
            grpFiles.put(grpCount, files);
        } else {
            ArrayList<File> files = new ArrayList<File>();
            files.add(grpFile);
            grpFiles.put(grpCount, files);
        }

        //check if group spans > 1 block
        if(inbatch!= null &&
                ((GroupBy)base).compare(buffer.get(0).get(0), inbatch.get(indexInBatch)) == 0) {
            genGrp();
        }

        grpCount++;
    }
    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        return true;
    }

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
