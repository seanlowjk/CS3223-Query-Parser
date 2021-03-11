/**
 * Class which represents the GROUP BY operator, derived directly from the
 * DISTINCT class.
 */
package qp.operators;

import java.util.ArrayList;

import qp.utils.Attribute;

public class GroupBy extends Distinct {
    public GroupBy(Operator base, ArrayList<Attribute> groupByList) {
        super(base, groupByList);
    }
}
