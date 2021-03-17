/**
 * Tuple container class
 **/

package qp.utils;

import java.sql.Time;
import java.util.*;
import java.io.*;

/**
 * Tuple - a simple object which holds an ArrayList of data
 */
public class Tuple implements Serializable {

    public ArrayList<Object> _data;

    public Tuple(ArrayList<Object> d) {
        _data = d;
    }

    /**
     * Accessor for data
     */
    public ArrayList<Object> data() {
        return _data;
    }

    public Object dataAt(int index) {
        return _data.get(index);
}

    /**
     * Checks whether the join condition is satisfied or not with multiple conditions
     * * before performing actual join operation
     **/
    public boolean checkJoin(Tuple right, ArrayList<Integer> leftindex, ArrayList<Integer> rightindex, ArrayList<Condition> condList) {
        if (leftindex.size() != rightindex.size() && leftindex.size() != condList.size() && rightindex.size() != condList.size())
            return false;
        for (int i = 0; i < leftindex.size(); ++i) {
            Object leftData = dataAt(leftindex.get(i));
            Object rightData = right.dataAt(rightindex.get(i));
            int exprtype = condList.get(i).getExprType();

            if (exprtype == Condition.LESSTHAN) {
                if (leftData instanceof Integer) {
                    return (((Integer) leftData).compareTo((Integer) rightData) < 0);
                } else if (leftData instanceof String) {
                    return (((String) leftData).compareTo((String) rightData) < 0);
                } else if (leftData instanceof Float) {
                    return (((Float) leftData).compareTo((Float) rightData) < 0);
                } else if (leftData instanceof Time)  {
                    return (((Time) leftData).compareTo((Time) rightData) < 0);
                } else {
                    System.out.println("Tuple: Unknown comparision of the tuples");
                    System.exit(1);
                    return false;
                }
            } else if (exprtype == Condition.GREATERTHAN) {
                if (leftData instanceof Integer) {
                    return (((Integer) leftData).compareTo((Integer) rightData) > 0);
                } else if (leftData instanceof String) {
                    return (((String) leftData).compareTo((String) rightData) > 0);
                } else if (leftData instanceof Float) {
                    return (((Float) leftData).compareTo((Float) rightData) > 0);
                } else if (leftData instanceof Time)  {
                    return (((Time) leftData).compareTo((Time) rightData) > 0);
                } else {
                    System.out.println("Tuple: Unknown comparision of the tuples");
                    System.exit(1);
                    return false;
                }
            } else if (exprtype == Condition.LTOE) {
                if (leftData instanceof Integer) {
                    return (((Integer) leftData).compareTo((Integer) rightData) <= 0);
                } else if (leftData instanceof String) {
                    return (((String) leftData).compareTo((String) rightData) <= 0);
                } else if (leftData instanceof Float) {
                    return (((Float) leftData).compareTo((Float) rightData) <= 0);
                } else if (leftData instanceof Time)  {
                    return (((Time) leftData).compareTo((Time) rightData) <= 0);
                } else {
                    System.out.println("Tuple: Unknown comparision of the tuples");
                    System.exit(1);
                    return false;
                }
            } else if (exprtype == Condition.GTOE) {
                if (leftData instanceof Integer) {
                    return (((Integer) leftData).compareTo((Integer) rightData) >= 0);
                } else if (leftData instanceof String) {
                    return (((String) leftData).compareTo((String) rightData) >= 0);
                } else if (leftData instanceof Float) {
                    return (((Float) leftData).compareTo((Float) rightData) >= 0);
                } else if (leftData instanceof Time)  {
                    return (((Time) leftData).compareTo((Time) rightData) >= 0);
                } else {
                    System.out.println("Tuple: Unknown comparision of the tuples");
                    System.exit(1);
                    return false;
                }
            } else if (exprtype == Condition.EQUAL) {
                if (leftData instanceof Integer) {
                    return (((Integer) leftData).compareTo((Integer) rightData) == 0);
                } else if (leftData instanceof String) {
                    return (((String) leftData).compareTo((String) rightData) == 0);
                } else if (leftData instanceof Float) {
                    return (((Float) leftData).compareTo((Float) rightData) == 0);
                } else if (leftData instanceof Time)  {
                    return (((Time) leftData).compareTo((Time) rightData) == 0);
                } else {
                    System.out.println("Tuple: Unknown comparision of the tuples");
                    System.exit(1);
                    return false;
                }
            } else if (exprtype == Condition.NOTEQUAL) {
                if (leftData instanceof Integer) {
                    return (((Integer) leftData).compareTo((Integer) rightData) != 0);
                } else if (leftData instanceof String) {
                    return (((String) leftData).compareTo((String) rightData) != 0);
                } else if (leftData instanceof Float) {
                    return (((Float) leftData).compareTo((Float) rightData) != 0);
                } else if (leftData instanceof Time)  {
                    return (((Time) leftData).compareTo((Time) rightData) != 0);
                } else {
                    System.out.println("Tuple: Unknown comparision of the tuples");
                    System.exit(1);
                    return false;
                }
            } else {
                System.out.println("Tuple: Incorrect condition operator");
            }
        }
        return true;
    }

    /**
     * Joining two tuples without duplicate column elimination
     **/
    public Tuple joinWith(Tuple right) {
        ArrayList<Object> newData = new ArrayList<>(this.data());
        newData.addAll(right.data());
        return new Tuple(newData);
    }

    /**
     * Compare two tuples in the same table on given attribute
     **/
    public static int compareTuples(Tuple left, Tuple right, int index) {
        return compareTuples(left, right, index, index);
    }

    /**
     * Comparing tuples in different tables, used for join condition checking
     **/
    public static int compareTuples(Tuple left, Tuple right, int leftIndex, int rightIndex) {
        Object leftdata = left.dataAt(leftIndex);
        Object rightdata = right.dataAt(rightIndex);
        if (leftdata instanceof Integer) {
            return ((Integer) leftdata).compareTo((Integer) rightdata);
        } else if (leftdata instanceof String) {
            return ((String) leftdata).compareTo((String) rightdata);
        } else if (leftdata instanceof Float) {
            return ((Float) leftdata).compareTo((Float) rightdata);
        } else if (leftdata instanceof Time)  {
            return ((Time) leftdata).compareTo((Time) rightdata);
        } else {
            System.out.println("Tuple: Unknown comparision of the tuples");
            System.exit(1);
            return 0;
        }
    }

    /**
     * Comparing tuples in different tables with multiple conditions, used for join condition checking
     **/
    public static int compareTuples(Tuple left, Tuple right, List<Integer> leftIndex, List<Integer> rightIndex) {
        if (leftIndex.size() != rightIndex.size()) {
            System.out.println("Tuple: Unknown comparision of the tuples");
            System.exit(1);
            return 0;
        }
        for (int i = 0; i < leftIndex.size(); ++i) {
            Object leftdata = left.dataAt(leftIndex.get(i));
            Object rightdata = right.dataAt(rightIndex.get(i));
            if (leftdata.equals(rightdata)) continue;
            if (leftdata instanceof Integer) {
                return ((Integer) leftdata).compareTo((Integer) rightdata);
            } else if (leftdata instanceof String) {
                return ((String) leftdata).compareTo((String) rightdata);
            } else if (leftdata instanceof Float) {
                return ((Float) leftdata).compareTo((Float) rightdata);
            } else if (leftdata instanceof Time)  {
                return ((Time) leftdata).compareTo((Time) rightdata);
            } else {
                System.out.println("Tuple: Unknown comparision of the tuples");
                System.exit(1);
                return 0;
            }
        }
        return 0;
    }
}
