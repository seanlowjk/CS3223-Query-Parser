/**
 * Class which represents the GROUP BY operator, derived directly from the
 * DISTINCT class, except the removal of attributes.
 */
package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.AttributeDirection;
import qp.utils.Batch;
import qp.utils.BatchUtils;
import qp.utils.Schema;
import qp.utils.Tuple;
import qp.utils.TupleComparator;

import java.io.File;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The type Group by.
 */
public class GroupBy extends Operator {
    // The base table to project
    private Operator base;
    // The list of attributes to group on
    private ArrayList<Attribute> attrList;
    // The number of buffers available
    private int numberOfBuffers;
    // The comparator to use to compare between Tuples
    private TupleComparator comparator;
    // The number of tuples per batch
    private int batchSize;
    // The list of Files holding the sorted, distinct list of tuples
    private ArrayList<File> distinctFiles;
    // The pointer to the sorted, distinct list of tuples
    private ObjectInputStream distinctOut;

    /**
     * Creates a new GROUP BY operator.
     *
     * @param base     The base operator.
     * @param attrList The list of attributes to group on.
     */
    public GroupBy(Operator base, ArrayList<Attribute> attrList) {
        super(OpType.GROUPBY);
        this.base = base;
        this.attrList = attrList;
        this.numberOfBuffers = BufferManager.getNumberOfBuffers();
        this.comparator = new TupleComparator(base.getSchema(),
            AttributeDirection.getAttributeDirections(attrList, false));
        schema = base.getSchema();
    }

    /**
     * Gets base.
     *
     * @return the base
     */
    public Operator getBase() {
        return this.base;
    }

    /**
     * Sets base.
     *
     * @param base the base
     */
    public void setBase(Operator base) {
        this.base = base;
    }

    /**
     * Gets proj attr.
     *
     * @return the proj attr
     */
    public ArrayList<Attribute> getProjAttr() {
        return this.attrList;
    }

    /**
     * Compare int.
     *
     * @param left  the left
     * @param right the right
     * @return the int
     */
    public int compare(Tuple left, Tuple right) {
        return comparator.compare(left, right);
    }

    /**
     * Opens the connection to the base operator and identifies the attributes
     * to be projected from the base operator.
     */
    @Override
    public boolean open() {
        if (!base.open()) {
            return false;
        }

        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        ArrayList<File> sortedRuns = createSortedRuns(batchSize);
        this.distinctFiles = mergeSortedRunsAndRemoveDups(sortedRuns, batchSize);
        List<ObjectInputStream> inputstreams = BatchUtils.createInputStreams(this.distinctFiles);

        if (inputstreams.isEmpty()) {
            this.distinctOut = null;
        } else {
            this.distinctOut = BatchUtils.createInputStreams(this.distinctFiles).get(0);
        }

        return true;
    }

    /**
     * Reads the next batch of tuples from the base operator.
     */
    @Override
    public Batch next() {
        if (this.distinctOut == null) {
            return null;
        }

        return BatchUtils.readBatch(this.distinctOut);
    }

    /**
     * Closes the connection to the base operator.
     */
    @Override
    public boolean close() {
        for (File originalFile : this.distinctFiles) {
            originalFile.delete();
        }

        return base.close();
    }

    /**
     *
     * @return
     */
    @Override
    public Object clone() {
        Operator newBase = (Operator) base.clone();
        ArrayList<Attribute> newAttr = new ArrayList<>();

        for (int i = 0; i < this.attrList.size(); i++) {
            newAttr.add((Attribute) this.attrList.get(i).clone());
        }

        GroupBy newGroupBy = new GroupBy(newBase, newAttr);
        Schema newSchema = newBase.getSchema().subSchema(newAttr);
        newGroupBy.setSchema(newSchema);
        return newGroupBy;
    }

    /**
     * This function generates sorted runs and outputs them to disk.
     * @param batchSize The size of each batch, in number of tuples.
     * @return ArrayList<File> that represents the files pointing to each
     * individually sorted runs.
     */
    private ArrayList<File> createSortedRuns(int batchSize) {
        ArrayList<File> outputFiles = new ArrayList<>();
        int fileNumber = 0;
        Batch nextRun = base.next();
        while (nextRun != null) {
            List<Batch> initialRuns = new ArrayList<>();
            int numberOfBuffersUsed = 0;
            while (numberOfBuffersUsed < numberOfBuffers && nextRun != null) {
                initialRuns.add(nextRun);
                nextRun = base.next();
                if (nextRun == null) {
                    break;
                }
                numberOfBuffersUsed++;
            }

            ArrayList<Tuple> tuples = new ArrayList<>();
            ArrayList<Batch> sortedRuns = new ArrayList<>();
            for (Batch batch : initialRuns) {
                for (int i = 0; i < batch.size(); i++) {
                    Tuple inputTuple = batch.get(i);
                    tuples.add(inputTuple);
                }
            }

            Collections.sort(tuples, this.comparator);

            Batch sortedRun = new Batch(batchSize);
            for (int i = 0; i < tuples.size(); i++) {
                Tuple tuple = tuples.get(i);
                sortedRun.add(tuple);

                if (sortedRun.isFull()) {
                    sortedRuns.add(sortedRun);
                    sortedRun = new Batch(batchSize);
                }
            }

            if (!sortedRun.isEmpty()) {
                sortedRuns.add(sortedRun);
            }

            if (sortedRuns.size() > 0) {
                // Generates a file written with the generated sorted files.
                String filename = String.format("groupby-X-%d", fileNumber);
                File sortedRunsFile = BatchUtils.writeRuns(sortedRuns, filename);
                fileNumber ++;
                outputFiles.add(sortedRunsFile);
            }
        }

        return outputFiles;
    }

    /**
     * This function merges the sorted runs generated by createSortedRuns() and
     * removes duplicates before returning them.
     * @param sortedRuns The List of Files, where each file represents a Batch
     * that is individually sorted.
     * @param batchSize The size of each batch, in number of tuples.
     * @return File that represents a sorted list of Tuples that are distinct.
     */
    private ArrayList<File> mergeSortedRunsAndRemoveDups(
            ArrayList<File> sortedRuns, int batchSize) {
        // Use B - 1 buffer pages for input and 1 buffer page for output
        int numberOfAvailableBuffers = numberOfBuffers - 1;
        int roundNum = 0;
        ArrayList<File> sortedRunsFiles = new ArrayList<>(sortedRuns);

        while (sortedRunsFiles.size() > 1 || roundNum == 0) {
            ArrayList<File> newSortedRunsFiles = new ArrayList<>();
            ArrayList<Batch> interimBuffer = new ArrayList<>();
            Batch outputBatch = new Batch(batchSize);

            List<ObjectInputStream> sortedRunsOIS = BatchUtils.createInputStreams(sortedRunsFiles);
            int maxPages = Math.min(sortedRunsFiles.size(), numberOfAvailableBuffers);
            int roundParts = (int) Math.ceil(sortedRunsFiles.size() / (maxPages * 1.0));

            for (int i = 0; i < roundParts; i++) {
                int[] interimBufferPointers = new int[maxPages];

                // Initialise B - 1 buffer pages first
                for (int j = 0; j < maxPages; j++) {
                    int index = i * maxPages + j;

                    if (index >= sortedRunsFiles.size()) {
                        break;
                    }

                    Batch sortedBatch = BatchUtils.readBatch(sortedRunsOIS.get(index));
                    interimBuffer.add(sortedBatch);
                }

                // Initialise empty file to append buffer to
                List<Batch> outputBufferBatches = new ArrayList<>();
                String roundPartFilename = String.format("groupby-%d-%d", roundNum, i);

                // Perform (B - 1)-way merges
                boolean hasRemaining = true;

                while (hasRemaining) {
                    hasRemaining = false;
                    Tuple smallestTuple = null;
                    int batchId = 0;

                    for (int j = 0; j < maxPages; j++) {
                        int index = i * maxPages + j;

                        if (index >= sortedRunsFiles.size()) {
                            break;
                        }

                        Batch currBatch = interimBuffer.get(j);

                        // Check if the current sorted run has been exhausted
                        if (currBatch == null) {
                            continue;
                        }

                        // Retrieve the next batch of the same sorted run
                        if (interimBufferPointers[j] >= currBatch.size()) {
                            currBatch = BatchUtils.readBatch(sortedRunsOIS.get(index));
                            interimBuffer.set(j, currBatch);
                            interimBufferPointers[j] = 0;
                        }

                        // Check again to make sure that the existing batch is not null
                        if (currBatch == null) {
                            continue;
                        }

                        hasRemaining = true;
                        Tuple currTuple = currBatch.get(interimBufferPointers[j]);

                        // Check and discard current tuple if it is a duplicate
                        while (outputBatch.size() > 0) {
                            boolean isDuplicate = false;

                            for (int k = 0; k < outputBatch.size(); k++) {
                                if (this.comparator.compare(outputBatch.get(k), currTuple) == 0) {
                                    isDuplicate = true;
                                    break;
                                }
                            }

                            if (!isDuplicate) {
                                break;
                            }

                            interimBufferPointers[j]++;

                            // Exit if current batch is exhausted
                            if (interimBufferPointers[j] >= currBatch.size()) {
                                currTuple = null;
                                break;
                            }

                            currTuple = currBatch.get(interimBufferPointers[j]);
                        }

                        if (currTuple == null) {
                            continue;
                        }

                        if (smallestTuple == null || comparator.compare(smallestTuple, currTuple) > 0) {
                            smallestTuple = currTuple;
                            batchId = j;
                        }
                    }

                    if (smallestTuple != null) {
                        if (outputBatch.size() == batchSize) {
                            outputBufferBatches.add(outputBatch);
                            outputBatch = new Batch(batchSize);
                        }
                        outputBatch.add(smallestTuple);
                        interimBufferPointers[batchId]++;
                    }
                }

                if (outputBatch.size() > 0) {
                    outputBufferBatches.add(outputBatch);
                    outputBatch = new Batch(batchSize);
                }

                File roundPartFile = BatchUtils.writeRuns(outputBufferBatches, roundPartFilename);
                newSortedRunsFiles.add(roundPartFile);
                interimBuffer = new ArrayList<>();
            }

            // Delete the original set of files
            for (File originalRunFile : sortedRunsFiles) {
                originalRunFile.delete();
            }

            sortedRunsFiles = newSortedRunsFiles;
            roundNum++;
        }

        // There should only be one run by now
        return sortedRunsFiles;
    }
}
