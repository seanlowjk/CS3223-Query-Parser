package qp.operators;

import qp.utils.Attribute;
import qp.utils.AttributeDirection;
import qp.utils.Batch;
import qp.utils.BatchUtils;
import qp.utils.Schema;
import qp.utils.Tuple;
import qp.utils.TupleComparator;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The type Sort.
 */
public class Sort extends Operator {
    private static final String FILE_HEADER = "Stemp";
    private static int totalFileCounter = 0;

    private Operator base;
    private List<Attribute> attributes;
    private boolean isDescending;
    private int numberOfBuffers;
    private int numberOfPages;

    private List<File> sortedRunsFiles;
    private TupleComparator comparator;

    private int fileCounter;
    private int roundNumber;
    private int fileNumber;

    private ObjectInputStream inputStream;

    /**
     * Creates the operator which represents the logic for Sorting.
     * The main algorithm used here is external sorting.
     *
     * @param base            the base operator.
     * @param attributes      the attributes to compare by.
     * @param numberOfBuffers the number of buffers given for sorting.
     * @param isDescending    whether the tuples should be compared in descending order or not.
     * @param type            the type of operator used.
     */
    public Sort(Operator base, List<Attribute> attributes, int numberOfBuffers, boolean isDescending, int type) {
        super(type);
        this.base = base;
        this.attributes = attributes;
        this.isDescending = isDescending;
        this.numberOfBuffers = numberOfBuffers;

        this.numberOfPages = 0;
        this.sortedRunsFiles = new ArrayList<>();
        this.comparator = new TupleComparator(base.getSchema(),
            AttributeDirection.getAttributeDirections(attributes, isDescending));

        this.fileCounter = ++totalFileCounter;
        this.roundNumber = -1;
        this.fileNumber = 0;
        this.inputStream = null;
    }

    /**
     *
     * @return
     */
    @Override
    public boolean open() {
        if (!base.open()) {
            return false;
        }

        int tupleSize = schema.getTupleSize();
        int batchSize = Batch.getPageSize() / tupleSize;
        generateSortedRuns(batchSize);
        mergeSortedRuns(batchSize);
        return retrieveInputStream();
    }

    /**
     *
     * @return
     */
    @Override
    public Batch next() {
        if (inputStream == null) {
            return null;
        }
        
        return BatchUtils.readBatch(inputStream);
    }

    /**
     *
     * @return
     */
    @Override
    public boolean close() {
        for (File originalRun : sortedRunsFiles) {
            originalRun.delete();
        }

        return super.close();
    }

    /**
     *
     * @return
     */
    @Override
    public int getOpType() {
        return super.getOpType();
    }

    /**
     *
     * @param schema
     */
    @Override
    public void setSchema(Schema schema) {
        super.setSchema(schema);
    }

    /**
     *
     * @return
     */
    @Override
    public Object clone() {
        Operator clone = (Operator) base.clone();
        Sort newSort = new Sort(clone, attributes, numberOfBuffers, isDescending, getOpType());
        newSort.setSchema((Schema) schema.clone());
        return newSort;
    }

    /**
     * Gets base.
     *
     * @return the base
     */
    public Operator getBase() {
        return base;
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
     * Returns the total I/O cost, which is represented by
     * 2 X |R| X numberOfPasses.
     *
     * @param numberOfPages   the number of pages
     * @param numberOfBuffers the number of buffers
     * @return the int
     */
    public static int calculateTotalIOCost(int numberOfPages, int numberOfBuffers) {
        return 2 * numberOfPages * calculateNumberOfPasses(numberOfPages, numberOfBuffers);
    }

    /**
     * Calculates the number of passes needed for the generatedSortedRuns and
     * mergeSortedRuns phases.
     */
    private static int calculateNumberOfPasses(int numberOfPages, int numberOfBuffers) {
        double numberOfOriginalSortedRuns = Math.ceil(numberOfPages / (1.0 * numberOfBuffers));
        int numPasses = 1 + (int) Math.ceil(Math.log(numberOfOriginalSortedRuns) / Math.log(numberOfPages -1));
        return numPasses;
    }

    /**
     * Gets number of pages.
     *
     * @return the number of pages
     */
    public int getNumberOfPages() {
        return this.numberOfPages;
    }

    /**
     * Sets the input stream for the next() operator to call on.
     * @return  true if there are any errors, false otherwise.
     */
    private boolean retrieveInputStream() {
        try {
            File mergedSortedRun = sortedRunsFiles.get(0);
            FileInputStream fileInputStream = new FileInputStream(mergedSortedRun);
            ObjectInputStream generatedInputStream = new ObjectInputStream(fileInputStream);
            this.inputStream = generatedInputStream;
        } catch (Exception exception) {
            this.inputStream = null;
        }
        return true;
    }

    /**
     * This is with regards to Phase 1 of the External Sort Algorithm.
     * Generates the required sorted runs for the first phase of
     * external sort, which requires all buffers given.
     * @param batchSize the size of the page given.
     */
    private void generateSortedRuns(int batchSize) {
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

            // Creates the sorted runs from the given initial runs made.
            List<Batch> sortedRuns = createSortedRuns(initialRuns, batchSize);

            if (sortedRuns.size() > 0) {
                // Generates a file written with the generated sorted files.
                String filename = String.format("%s-%d-%d-%d", FILE_HEADER, fileCounter, roundNumber, fileNumber);
                File sortedRunsFile = BatchUtils.writeRuns(sortedRuns, filename);
                fileNumber ++;
                sortedRunsFiles.add(sortedRunsFile);
            }
        }

        numberOfPages = sortedRunsFiles.size() * numberOfBuffers;
        gotoNextRound();
    }

    /**
     * Generated a list of pages which represent the sorted runs
     * after the first phase of external sort.
     * @param initialRuns the unsorted runs initially generated by the algorithm.
     * @param batchSize the size of the page given.
     */
    private List<Batch> createSortedRuns(List<Batch> initialRuns, int batchSize) {
        List<Tuple> tuples = new ArrayList<>();
        List<Batch> sortedRuns = new ArrayList<>();
        for (Batch batch : initialRuns) {
            for (int i = 0; i < batch.size(); i++) {
                Tuple batchTuple = batch.get(i);
                tuples.add(batchTuple);
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

        return sortedRuns;
    }

    /**
     * This is with regards to Phase 2 of the External Sort Algorithm.
     * Iteratively merges sorted runs into a larger sorter run, eventually
     * hitting a point where 1 full sorted run is generated.
     * @param batchSize the size of the page given.
     */
    private void mergeSortedRuns(int batchSize) {
        int numberOfAvailableBuffers = numberOfBuffers - 1;

        // Condition to check if more merging is needed before producing a full sorted run.
        while (sortedRunsFiles.size() > 1) {
            List<File> sortedRuns = new ArrayList<>();

            // Condition where the merging of sorted runs should end only if 
            // all the sorted run files have been processed. 
            int pagesRead = 0; 
            while (pagesRead < sortedRunsFiles.size()) {
                int numBuffersAllocated = Math.min(numberOfAvailableBuffers, sortedRunsFiles.size() - pagesRead);

                int firstFileNumber = pagesRead;
                int lastFileNumber = firstFileNumber + numBuffersAllocated - 1;

                List<File> runsToMerge = new ArrayList<>();
                for (int i = firstFileNumber; i <= lastFileNumber; i++) {
                    File sortedRun = sortedRunsFiles.get(i);
                    runsToMerge.add(sortedRun);
                }

                if (runsToMerge.isEmpty()) {
                    break;
                }

                // Combine the Sorted Runs into a merged sorted run,
                // and then writing the merged sorted run to a file.
                File combinedSortedRunFile = combineSortedRuns(runsToMerge, numBuffersAllocated, batchSize);
                sortedRuns.add(combinedSortedRunFile);

                pagesRead += numBuffersAllocated;
                gotoNextRound();
            }

            // Cleanup original sorted run files.
            for (File originalRun : sortedRunsFiles) {
                originalRun.delete();
            }

            sortedRunsFiles = sortedRuns;
        }
    }

    /**
     * Algorithm to combine the sorted runs into one larger merged sorted run
     * and write this larged merged sorted run into a file.
     * @param runsToMerge the runs that will be merged into a larger medged sorted run.
     * @param numberOfAvailableBuffers number of buffers for merging
     * @param batchSize the size of the page given.
     */
    private File combineSortedRuns(List<File> runsToMerge, int numberOfAvailableBuffers, int batchSize) {
        List<Batch> inputBuffers = new ArrayList<>();

        // Generates the list of input streams to read the input buffers.
        List<ObjectInputStream> inputStreams = BatchUtils.createInputStreams(runsToMerge);

        for (ObjectInputStream inputStream : inputStreams) {
            Batch batch = BatchUtils.readBatch(inputStream);
            inputBuffers.add(batch);
        }

        File outputBufferFile = combineInputBuffers(inputStreams, inputBuffers,
            numberOfAvailableBuffers, batchSize);

        return outputBufferFile;
    }

    /**
     * Given a list of input buffers, combine all the tuples within these
     * input buffers to generate an larger merged sorted run and eventually
     * write it to a file.
     * @param inputStreams the input streams used to geenrate the input buffers.
     * @param inputBuffers the input buffers used to store the smaller, sorted runs.
     * @param numberOfAvailableBuffers number of buffers for merging
     * @param batchSize the size of the page given.
     */
    private File combineInputBuffers(List<ObjectInputStream> inputStreams,
        List<Batch> inputBuffers, int numberOfAvailableBuffers, int batchSize) {

        // Initialize necessary variables.
        List<Batch> outputBatches = new ArrayList<>();
        Batch outputBuffer = new Batch(batchSize);
        File outputBufferFile = null;
        int[] inputBufferPointers = new int[numberOfAvailableBuffers];
        int numberOfRunsFinished = 0;

        // Condition where all the available buffers haven't been used up yet.
        while (numberOfRunsFinished < numberOfAvailableBuffers) {
            Tuple smallestTuple = null;
            int smallestTupleIndex = 0;

            for (int i = 0; i < inputBuffers.size(); i++) {
                Batch batch = inputBuffers.get(i);
                int inputBufferPointer = inputBufferPointers[i];

                // If out of bounds, means that the sorted run has been used completely,
                // This also helps us to prevent an IndexOutofBoundsException.
                if (inputBufferPointer >= batch.size()) {
                    continue;
                }

                Tuple currentTuple = batch.get(inputBufferPointer);
                if (smallestTuple == null || comparator.compare(smallestTuple, currentTuple) > 0) {
                    smallestTuple = currentTuple;
                    smallestTupleIndex = i;
                }
            }

            inputBufferPointers[smallestTupleIndex] ++;
            Batch desiredInputBuffer = inputBuffers.get(smallestTupleIndex);

            // Check if all of the elemnts within the input buffer has been read.
            // If so, try to read in the next tuples. If not, just leave it empty.
            // NOTE: this algorithm is slightly different from the one that we
            // have learnt in our CS3223 Lecture.
            if (inputBufferPointers[smallestTupleIndex] >= desiredInputBuffer.size()) {
                ObjectInputStream desiredInputStream = inputStreams.get(smallestTupleIndex);
                Batch nextBatch = BatchUtils.readBatch(desiredInputStream);

                if (nextBatch != null) {
                    inputBuffers.set(smallestTupleIndex, nextBatch);
                    inputBufferPointers[smallestTupleIndex] = 0;
                } else {
                    numberOfRunsFinished ++;
                }
            }

            outputBuffer.add(smallestTuple);

            if (outputBuffer.isFull()) {
                outputBatches.add(outputBuffer);
                outputBuffer = new Batch(batchSize);
            }

            if (smallestTuple == null) {
                break;
            }
        }

        if (!outputBuffer.isEmpty()) {
            outputBatches.add(outputBuffer);
            outputBuffer = new Batch(batchSize);
        }

        String filename = String.format("%s-%d-%d-%d", FILE_HEADER, fileCounter, roundNumber, fileNumber);
        fileNumber ++;
        outputBufferFile = BatchUtils.writeRuns(outputBatches, filename);

        return outputBufferFile;
    }

    /**
     * Updates the round number to increment and
     * resets the file number.
     */
    private void gotoNextRound() {
        this.roundNumber ++;
        this.fileNumber = 0;
    }
}
