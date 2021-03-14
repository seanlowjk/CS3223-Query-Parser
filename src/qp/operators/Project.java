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

public class Project extends Operator {

    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    int batchsize;                 // Number of tuples per outbatch
    int numOfBuffers;

    /**
     * The following fields are requied during execution
     * * of the Project Operator
     **/
    Batch inbatch; //pts to the first batch in block
    Batch outbatch;

    /**
     * Buffering
     */
    Queue<Batch> buffer;

    /**
     * index of the attributes in the base operator
     * * that are to be projected
     **/
    int[] attrIndex;

    int indexInBatch;
    boolean end;

    public Project(Operator base, ArrayList<Attribute> as, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
        this.numOfBuffers = BufferManager.getNumberOfBuffers();
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
        System.out.println("Proj");
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        System.out.println("base");
        System.out.println(base.getOpType());
        base.open();
        if (!base.open()) return false;
        System.out.println("Proj Open");
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
     * Read next tuple from operator
     */
    public Batch next() {
        System.out.println("Proj Next");
        outbatch = new Batch(batchsize);

        if(buffer.isEmpty()) {
            genBlock();
        }

        if(end) {
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
        buffer.poll();
        inbatch = buffer.peek();
        return outbatch;
    }

    public void genBlock() {
        int numOfInputBuffers = numOfBuffers - 1;

        inbatch = base.next();

        if(inbatch == null) {
            end = true;
            return;
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

    }

/*    public void genGrp() {

        inbatch = base.next();

        int inputBuffers = numOfBuffers - 1;
        int blockCount = 0;
        buffer.clear();

        Batch bufferBatch =  new Batch(batchsize);
        Tuple prevBatchLastTuple = null;
        boolean isSameGrp = false;
        while (inbatch != null) {
            for (int i = 0; i < inputBuffers; i++) {

                if (inbatch == null) {
                    return;
                }

                if(inbatch.size() == indexInBatch) {
                    inbatch = base.next();
                    if (inbatch == null) {
                        return;
                    }
                    indexInBatch = 0;
                }

                for (int j = indexInBatch; indexInBatch < inbatch.size(); j++) {
                    if (!bufferBatch.isEmpty()) {
                        Tuple grpTuple = bufferBatch.get(0); //compare the currTuple to a tuple thats in the grp
                        Tuple currTuple = inbatch.get(j);

                        if (((GroupBy) base).compare(grpTuple, currTuple) != 0) {
                            indexInBatch = j;
                            isSameGrp = false;
                            break;
                        }
                    } else if (!buffer.isEmpty()) {
                        Tuple currTuple = inbatch.get(j);
                        if (((GroupBy) base).compare(prevBatchLastTuple, currTuple) != 0) {
                            indexInBatch = j;
                            isSameGrp = false;
                            break;
                        }
                    }

                    bufferBatch.add(inbatch.get(j));

                    if (bufferBatch.isFull()) {
                        prevBatchLastTuple = inbatch.get(j);
                        buffer.add(bufferBatch);
                        bufferBatch = new Batch(batchsize);

                    }
                    indexInBatch++;
                }

                if(isSameGrp) {
                    inputBuffers++;
                } else {
                    //another grp but in same batch
                    //write out all the remaining in current
                    break;
                }

            }
            writeGrpBlocks(blockCount);
            buffer.clear();
            blockCount++;

            //different grp
            if(inbatch!= null && !isSameGrp) {
               //init relevant info for looping next group
                blockCount = 0;
                if(indexInBatch == inbatch.size()) {
                    inbatch = ((GroupBy) base).next();
                    indexInBatch = 0;
                }
                isSameGrp = true;
                bufferBatch = new Batch(batchsize);
                grpCount++;
            }
        }

    }

    public void writeGrpBlocks(int blockCount) {

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
    }*/

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
