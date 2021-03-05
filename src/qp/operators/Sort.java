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

public class Sort extends Operator {
    private static final String FILE_HEADER = "Stemp";

    private Operator base;
    private List<Attribute> attributes;
    private boolean isDescending;
    private int numberOfBuffers;
    private int numberOfPages; 

    private List<File> sortedRunsFiles;
    private TupleComparator comparator;
    private int roundNumber;
    private int fileNumber;

    private ObjectInputStream inputStream;

    /**
     * Creates the operator which represents the logic for Sorting. 
     * The main algorithm used here is external sorting. 
     * @param base the base operator. 
     * @param attributes the attributes to compare by.
     * @param numberOfBuffers the number of buffers given for sorting. 
     * @param isDescending whether the tuples should be compared in descending order or not. 
     * @param type the type of operator used. 
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
        this.roundNumber = 0;
        this.fileNumber = 0;
        this.inputStream = null;
    }

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

    @Override
    public Batch next() {
        return BatchUtils.readBatch(inputStream);
    }

    @Override
    public boolean close() {
        for (File originalRun : sortedRunsFiles) {
            originalRun.delete();
        }

        return super.close();
    }

    @Override
    public int getOpType() {
        return super.getOpType();
    }

    @Override
    public void setSchema(Schema schema) {
        super.setSchema(schema);
    }

    @Override
    public Object clone() {
        Operator clone = (Operator) base.clone();
        Sort newSort = new Sort(clone, attributes, numberOfBuffers, isDescending, getOpType());
        newSort.setSchema((Schema) schema.clone());
        return newSort;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }
    
    /**
     * Returns the total I/O cost, which is represented by 
     * 2 X |R| X numberOfPasses. 
     */
    public int calculateTotalIOCost() {
        return 2 * numberOfPages + calculateNumberOfPasses();
    }

    /**
     * Calculates the number of passes needed for the generatedSortedRuns and 
     * mergeSortedRuns phases. 
     */
    private int calculateNumberOfPasses() {
        double numberOfOriginalSortedRuns = Math.ceil(numberOfPages / (1.0 * numberOfBuffers));
        int numPasses = 1 + (int) Math.ceil(Math.log(numberOfOriginalSortedRuns) / Math.log(numberOfPages -1));
        return numPasses;
    }

    /**
     * Sets the input stream for the next() operator to call on.
     * Returns true if there are any errors, false otherwise. 
     */
    private boolean retrieveInputStream() {
        try {
            File mergedSortedRun = sortedRunsFiles.get(0);
            FileInputStream fileInputStream = new FileInputStream(mergedSortedRun);
            ObjectInputStream generatedInputStream = new ObjectInputStream(fileInputStream);
            this.inputStream = generatedInputStream;
            return true;
        } catch (Exception exception) {
            return false;
        }
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
                numberOfBuffersUsed++;
            }

            // Creates the sorted runs from the given initial runs made. 
            List<Batch> sortedRuns = createSortedRuns(initialRuns, batchSize);

            // Generates a file written with the generated sorted files. 
            File sortedRunsFile = writeSortedRuns(sortedRuns);
            sortedRunsFiles.add(sortedRunsFile);
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
            int round = 0;

            // Condition where the merging of sorted runs should end only if 
            // all the sorted run files have been processed. 
            while (round * numberOfAvailableBuffers < sortedRunsFiles.size()) {
                int firstFileNumber = round * numberOfAvailableBuffers;
                int lastFileNumber = Math.min((round + 1) * numberOfAvailableBuffers, 
                    sortedRunsFiles.size());
                
                List<File> runsToMerge = new ArrayList<>();
                for (int i = firstFileNumber; i < lastFileNumber; i++) {
                    File sortedRun = sortedRunsFiles.get(i);
                    runsToMerge.add(sortedRun);
                }

                if (runsToMerge.isEmpty()) {
                    break;
                }

                // Combine the Sorted Runs into a merged sorted run, 
                // and then writing the merged sorted run to a file. 
                File combinedSortedRunFile = combineSortedRuns(runsToMerge, numberOfAvailableBuffers, batchSize);
                sortedRuns.add(combinedSortedRunFile);
            }

            gotoNextRound();

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

        Batch outputBuffer = new Batch(batchSize);
        File outputBufferFile = combineInputBuffers(inputStreams, inputBuffers, 
            numberOfAvailableBuffers, outputBuffer);

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
        List<Batch> inputBuffers, int numberOfAvailableBuffers, Batch outputBuffer) {
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
            if (inputBufferPointers[smallestTupleIndex] == desiredInputBuffer.capacity()) {
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
                List<Batch> outputBuffers = new ArrayList<>();
                outputBuffers.add(outputBuffer);
                outputBufferFile = appendSortedRuns(outputBuffers, outputBufferFile);
            }
        }

        if (!outputBuffer.isEmpty()) {
            List<Batch> outputBuffers = new ArrayList<>();
            outputBuffers.add(outputBuffer);
            outputBufferFile = appendSortedRuns(outputBuffers, outputBufferFile);
        }
        
        return outputBufferFile;
    }

    /**
     * Appends a list of sorted runs to a given file. 
     * @param sortedRuns the list of sorted runs given. 
     * @param file the file the sorted runs are appended to. 
     */
    private File appendSortedRuns(List<Batch> sortedRuns, File file) {
        if (file == null) {
            return writeSortedRuns(sortedRuns);
        }

        try {
            FileOutputStream fileOutputStream = new FileOutputStream(file, true);
            ObjectOutputStream outputStream = new ObjectOutputStream(fileOutputStream);
            for (Batch batch : sortedRuns) {
                outputStream.writeObject(batch);
            }
            outputStream.close();

            return file;
        } catch (IOException exception) {
            return null;
        }
    }

    /**
     * Writes a list of sorted runs into a new file. 
     * @param sortedRuns the list of sorted runs given. 
     */
    private File writeSortedRuns(List<Batch> sortedRuns) {
        try {
            String filename = String.format("%s-%d-%d", FILE_HEADER, roundNumber, fileNumber);
            File sortedRunsFile = new File(filename);
            FileOutputStream fileOutputStream = new FileOutputStream(sortedRunsFile);
            ObjectOutputStream outputStream = new ObjectOutputStream(fileOutputStream);
            for (Batch sortedRun : sortedRuns) {
                outputStream.writeObject(sortedRun);
            }
            outputStream.close();
            this.fileNumber ++;

            return sortedRunsFile;
        } catch (IOException exception) {
            return null;
        }
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
