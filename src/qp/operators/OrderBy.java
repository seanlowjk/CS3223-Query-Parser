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
    public static Sort createOperator(Operator op, List<Attribute> attributes, int numberOfBuffers, boolean isDescending) {
        return new Sort(op, getAttributeDirections(attributes, isDescending), numberOfBuffers, OpType.SORT);
    }

    private static List<AttributeDirection> getAttributeDirections(List<Attribute> attributes, boolean isDescending) {
        List<AttributeDirection> attributeDirections = new ArrayList<>();
        OrderDirection direction = OrderDirection.getOrderDirection(isDescending);
        for (Attribute attribute : attributes) {
            attributeDirections.add(new AttributeDirection(attribute, direction));
        }
        return attributeDirections;
    }
}
