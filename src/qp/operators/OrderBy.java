/**
 * Class which represents the exact ORDERBY operator, 
 * which is based on the external sort algorithm implemented
 * in this application.
 */

package qp.operators;

import qp.utils.AttributeDirection;

import java.util.List;

public class OrderBy extends Sort {
    public OrderBy(Operator op, List<AttributeDirection> attributeDirections, int numberOfBuffers) {
        super(op, attributeDirections, numberOfBuffers, OpType.SORT);
    }
}
