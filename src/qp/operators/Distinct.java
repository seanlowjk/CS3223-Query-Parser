/**
 * Class which represents the DISTINCT operator, adapted from the optimised
 * sort-based approach.
 */
package qp.operators;

import java.io.File;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.AttributeDirection;
import qp.utils.Batch;
import qp.utils.BatchUtils;
import qp.utils.Schema;
import qp.utils.Tuple;
import qp.utils.TupleComparator;

public class Distinct extends Operator {
    // The base table to project
    private Operator base;
    // The list of attributes to project
    private ArrayList<Attribute> attrList;
    // The number of buffers available
    private int numberOfBuffers;
    // The comparator to use to compare between Tuples
    private TupleComparator comparator;
    // The number of tuples per batch
    private int batchSize;
    // Index of the attributes of the base operator to be projected
    private int[] attrIndices;
    // The list of Files holding the sorted, distinct list of tuples
    private ArrayList<File> distinctFiles;
    // The pointer to the sorted, distinct list of tuples
    private ObjectInputStream distinctOut;

    /**
     * Creates a new DISTINCT operator.
     * @param base The base operator.
     * @param attrList The list of attributes to project on.
     */
    public Distinct(Operator base, ArrayList<Attribute> attrList) {
        super(OpType.DISTINCT);
        this.base = base;
        this.attrList = attrList;
        this.numberOfBuffers = BufferManager.getNumberOfBuffers();
        this.comparator = new TupleComparator(base.getSchema(),
            AttributeDirection.getAttributeDirections(attrList, false));
    }

    public Operator getBase() {
        return this.base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getProjAttr() {
        return this.attrList;
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

        Schema baseSchema = base.getSchema();
        attrIndices = new int[attrList.size()];

        for (int i = 0; i < attrList.size(); i++) {
            Attribute attribute = attrList.get(i);
            int index = baseSchema.indexOf(attribute.getBaseAttribute());
            attrIndices[i] = index;
        }

        ArrayList<File> sortedRuns = createSortedRunsWithAttributes(batchSize);
        this.distinctFiles = mergeSortedRunsAndRemoveDups(sortedRuns, batchSize);
        this.distinctOut = BatchUtils.createInputStreams(this.distinctFiles).get(0);

        return true;
    }

    /**
     * Reads the next batch of tuples from the base operator.
     */
    @Override
    public Batch next() {
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

    @Override
    public Object clone() {
        return new Distinct(base, attrList);
    }

    /**
     * This function generates sorted runs while extracting the attributes in
     * attrList and outputs them to disk.
     * @param batchSize The size of each batch, in number of tuples.
     * @return ArrayList<File> that represents the files pointing to each
     * individually sorted runs.
     */
    private ArrayList<File> createSortedRunsWithAttributes(int batchSize) {
        Batch inputBatch = base.next();
        Batch outputBatch = new Batch(batchSize);
        ArrayList<Batch> outputBatches = new ArrayList<>();
        ArrayList<Tuple> bufferTuples = new ArrayList<>();
        ArrayList<File> outputFiles = new ArrayList<>();
        int numberOfBuffersUsed = 0;
        int roundNum = 0;

        while (inputBatch != null) {
            // Project only attributes that we are interested in
            for (int i = 0; i < inputBatch.size(); i++) {
                Tuple inputTuple = inputBatch.get(i);
                ArrayList<Object> projected = new ArrayList<>();

                for (int j = 0; j < attrList.size(); j++) {
                    Object data = inputTuple.dataAt(attrIndices[j]);
                    projected.add(data);
                }

                Tuple outputTuple = new Tuple(projected);
                bufferTuples.add(outputTuple);
            }

            numberOfBuffersUsed++;
            inputBatch = base.next();

            // Current buffer is full, sort and output to disk first
            if (numberOfBuffersUsed == numberOfBuffers || inputBatch == null) {
                // Generate a sorted run from the projected tuples
                Collections.sort(bufferTuples, this.comparator);

                // Returns the sorted run as a list of batches
                for (int i = 0; i < bufferTuples.size(); i++) {
                    outputBatch.add(bufferTuples.get(i));

                    if (((i+1) % batchSize == 0) || ((i+1) != bufferTuples.size())) {
                        outputBatches.add(outputBatch);
                        outputBatch = new Batch(batchSize);
                    }
                }

                // Write all batches of current buffer to file
                String sortedFilename = String.format("distinct-%d", roundNum);
                File sortedRunFile = BatchUtils.writeRuns(outputBatches, sortedFilename);
                outputFiles.add(sortedRunFile);

                roundNum++;
                numberOfBuffersUsed = 0;
                bufferTuples = new ArrayList<>();
                outputBatches = new ArrayList<>();
            }
        }

        return outputFiles;
    }

    /**
     * This function merges the sorted runs generated by
     * createSortedRunsWithAttributes() and removes duplicates before
     * returning them.
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
            int roundParts = (int) Math.ceil(sortedRunsFiles.size() / maxPages);
            System.out.println(maxPages);

            for (int i = 0; i < roundParts; i++) {
                int[] interimBufferPointers = new int[maxPages];

                // Initialise B - 1 buffer pages first
                for (int j = 0; j < maxPages; j++) {
                    Batch sortedBatch = BatchUtils.readBatch(sortedRunsOIS.get(j));
                    interimBuffer.add(sortedBatch);
                }

                // Initialise empty file to append buffer to
                String roundPartFilename = String.format("distinct-%d-%d", roundNum, i);
                File roundPartFile = new File(roundPartFilename);

                // Perform (B - 1)-way merges
                boolean hasRemaining = true;

                while (hasRemaining) {
                    hasRemaining = false;
                    Tuple smallestTuple = null;
                    int batchId = 0;

                    for (int j = 0; j < maxPages; j++) {
                        Batch currBatch = interimBuffer.get(j);

                        // Check if the current sorted run has been exhausted
                        if (currBatch == null) {
                            continue;
                        }

                        // Retrieve the next batch of the same sorted run
                        if (interimBufferPointers[j] >= currBatch.size()) {
                            currBatch = BatchUtils.readBatch(sortedRunsOIS.get(j));
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
                        outputBatch.add(smallestTuple);
                        interimBufferPointers[batchId]++;
                    }

                    if (outputBatch.size() == batchSize || smallestTuple == null) {
                        BatchUtils.appendRuns(List.of(outputBatch), roundPartFile);
                        outputBatch = new Batch(batchSize);
                    }
                }

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
