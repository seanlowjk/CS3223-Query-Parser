package qp.operators;

import qp.utils.AttributeDirection;

import java.util.List;

public class Sort extends Operator {

    private Operator op;
    private List<AttributeDirection> attributeDirections;
    private int numberOfBuffers;

    public Sort(Operator op, List<AttributeDirection> attributeDirections, int numberOfBuffers, int type) {
        super(type);
        this.op = op;
        this.attributeDirections = attributeDirections;
        this.numberOfBuffers = numberOfBuffers;
    }
}
