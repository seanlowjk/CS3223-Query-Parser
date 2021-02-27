package qp.operators;

import qp.utils.AttributeDirection;
import qp.utils.Batch;
import qp.utils.TupleComparator;

import java.util.List;

public class Sort extends Operator {

    private Operator op;
    private List<AttributeDirection> attributeDirections;
    private int numberOfBuffers;

    private TupleComparator comparator;
    private int roundNumber;
    private int fileNumber;

    public Sort(Operator op, List<AttributeDirection> attributeDirections, int numberOfBuffers, int type) {
        super(type);
        this.op = op;
        this.attributeDirections = attributeDirections;
        this.numberOfBuffers = numberOfBuffers;

        this.comparator = new TupleComparator(op.getSchema(), this.attributeDirections);
        this.roundNumber = 0;
        this.fileNumber = 0;
    }

    @Override
    public boolean open() {
        int tuplesize = schema.getTupleSize();
        int batchsize = Batch.getPageSize() / tuplesize;

        createSortedRuns();
        mergeSortedRuns();

        return true;
    }

    private void createSortedRuns() {

    }

    private void mergeSortedRuns() {

    }

    private void gotoNextRound() {
        this.roundNumber ++;
        this.fileNumber = 0;
    }
}
