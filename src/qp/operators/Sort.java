package qp.operators;

import qp.utils.AttributeDirection;
import qp.utils.Batch;
import qp.utils.Tuple;
import qp.utils.TupleComparator;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Sort extends Operator {

    private static final String FILE_HEADER = "EStemp";

    private Operator op;
    private List<AttributeDirection> attributeDirections;
    private int numberOfBuffers;

    private List<File> sortedRunsFiles;
    private TupleComparator comparator;
    private int roundNumber;
    private int fileNumber;

    public Sort(Operator op, List<AttributeDirection> attributeDirections, int numberOfBuffers, int type) {
        super(type);
        this.op = op;
        this.attributeDirections = attributeDirections;
        this.numberOfBuffers = numberOfBuffers;

        this.sortedRunsFiles = new ArrayList<>();
        this.comparator = new TupleComparator(op.getSchema(), this.attributeDirections);
        this.roundNumber = 0;
        this.fileNumber = 0;
    }

    @Override
    public boolean open() {
        int tupleSize = schema.getTupleSize();
        int batchSize = Batch.getPageSize() / tupleSize;

        createSortedRuns(batchSize);
        mergeSortedRuns();

        return true;
    }

    private void createSortedRuns(int batchSize) {
        Batch nextRun = op.next();
        while (nextRun != null) {
            List<Batch> initialRuns = new ArrayList<>();
            int numberOfBuffersUsed = 0;
            while (numberOfBuffersUsed < numberOfBuffers && nextRun != null) {
                initialRuns.add(nextRun);
                nextRun = op.next();
                numberOfBuffersUsed++;
            }
            List<Batch> sortedRuns = generateSortedRuns(initialRuns, batchSize);
            File sortedRunsFile = writeSortedRuns(sortedRuns);
            sortedRunsFiles.add(sortedRunsFile);
        }

        gotoNextRound();
    }

    private List<Batch> generateSortedRuns(List<Batch> initialRuns, int batchSize) {
        List<Tuple> tuples = new ArrayList<>();
        List<Batch> sortedRuns = new ArrayList<>();
        for (Batch batch : initialRuns) {
            for (int i = 0; i < initialRuns.size(); i++) {
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

    private File writeSortedRuns(List<Batch> sortedRuns) {
        try {
            String filename = String.format("%s-%d-%d", FILE_HEADER, roundNumber, fileNumber);
            File sortedRunsFile = new File(filename);
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(sortedRunsFile));
            for (Batch sortedRun : sortedRuns) {
                out.writeObject(sortedRun);
            }
            out.close();
            this.fileNumber ++;

            return sortedRunsFile;
        } catch (IOException exception) {
            System.out.println(exception);
        }

        return null;
    }

    private void mergeSortedRuns() {

    }

    private void gotoNextRound() {
        this.roundNumber ++;
        this.fileNumber = 0;
    }
}
