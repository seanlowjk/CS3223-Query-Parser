/**
 * Class which represents the exact ORDERBY operator, 
 * which is based on the external sort algorithm implemented
 * in this application.
 */
package qp.operators;

import qp.utils.Attribute;
import qp.utils.AttributeDirection;
import qp.utils.OrderDirection;

import java.util.ArrayList;
import java.util.List;

public class OrderBy {

    /**
     * Creates the operator which represents the logic for ORDERBY. 
     * The main algorithm used here is external sorting. 
     * @param op the base operator. 
     * @param attributes the attributes to compare by.
     * @param numberOfBuffers the number of buffers given for sorting. 
     * @param isDescending whether the tuples should be compared in descending order or not. 
     */
    public static Sort createOperator(Operator op, List<Attribute> attributes, int numberOfBuffers, boolean isDescending) {
        return new Sort(op, getAttributeDirections(attributes, isDescending), numberOfBuffers, OpType.SORT);
    }

    /**
     * Retrieves the attributes associated with a direction, whether 
     * it should be compared in ascending or descending order.
     * @param attributes the attributes to compare by.
     * @param isDescending whether the tuples should be compared in descending order or not. 
     */
    private static List<AttributeDirection> getAttributeDirections(List<Attribute> attributes, boolean isDescending) {
        List<AttributeDirection> attributeDirections = new ArrayList<>();
        OrderDirection direction = OrderDirection.getOrderDirection(isDescending);
        for (Attribute attribute : attributes) {
            attributeDirections.add(new AttributeDirection(attribute, direction));
        }
        return attributeDirections;
    }
}
