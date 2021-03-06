/**
 * Simple Comparator to compare between Tuples absed 
 * on the given list of attribute directions.  
 */

package qp.utils;

import java.util.Comparator;
import java.util.List;

public class TupleComparator implements Comparator<Tuple> {
    private Schema schema;
    private List<AttributeDirection> attributeDirections;
    private int numberOfAttributes;

    public TupleComparator(Schema schema, List<AttributeDirection> attributeDirections) {
        this.schema = schema;
        this.attributeDirections = attributeDirections;
        this.numberOfAttributes = attributeDirections.size();
    }

    private static String throwAttributeMissingError(Attribute attribute) {
        return String.format("%s attribute is missing.", attribute);
    }

    @Override
    public int compare(Tuple tuple, Tuple other) {
        for (int i = 0; i < numberOfAttributes; i++) {
            AttributeDirection attributeDirection = attributeDirections.get(i);
            int comparisonMultipler = attributeDirection.getComparisonMultiplier();

            Attribute sortingAttribute = attributeDirection.getAttribute();
            int attributeIndex = schema.indexOf(sortingAttribute);
            assert attributeIndex != -1 : throwAttributeMissingError(sortingAttribute);
            
            int comparisonResult = Tuple.compareTuples(tuple, other, attributeIndex);
            if (comparisonResult != 0) {
                return comparisonMultipler * comparisonResult;
            }
        }
        return 0;
    }
}
