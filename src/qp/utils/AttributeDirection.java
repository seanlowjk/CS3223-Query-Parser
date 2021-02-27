package qp.utils;

public class AttributeDirection {

    public static final int ASC_MULTIPLER = 1;
    public static final int DESC_MULTIPLER = -1;

    private Attribute attribute;
    private OrderDirection direction;

    public AttributeDirection(Attribute attribute, OrderDirection direction){
        this.attribute = attribute;
        this.direction = direction;
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
