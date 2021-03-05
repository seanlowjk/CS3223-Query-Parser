package qp.utils;

import java.util.ArrayList;
import java.util.List; 

public class AttributeDirection {

    public static final int ASC_MULTIPLER = 1;
    public static final int DESC_MULTIPLER = -1;

    private Attribute attribute;
    private OrderDirection direction;

    public AttributeDirection(Attribute attribute, OrderDirection direction){
        this.attribute = attribute;
        this.direction = direction;
    }

    /**
     * Retrieves the attributes associated with a direction, whether 
     * it should be compared in ascending or descending order.
     * @param attributes the attributes to compare by.
     * @param isDescending whether the tuples should be compared in descending order or not. 
     */
    public static List<AttributeDirection> getAttributeDirections(List<Attribute> attributes, boolean isDescending) {
        List<AttributeDirection> attributeDirections = new ArrayList<>();
        OrderDirection direction = OrderDirection.getOrderDirection(isDescending);
        for (Attribute attribute : attributes) {
            attributeDirections.add(new AttributeDirection(attribute, direction));
        }
        return attributeDirections;
    }

    public Attribute getAttribute() {
        return this.attribute;
    }

    public int getComparisonMultiplier() {
        if (this.direction.isAscending()) {
            return AttributeDirection.ASC_MULTIPLER;
        }

        return AttributeDirection.DESC_MULTIPLER;
    }
}
