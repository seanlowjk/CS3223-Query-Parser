/**
 * Enumeration of join algorithm types
 * Change this class depending on actual algorithms
 * you have implemented in your query processor
 **/

package qp.operators;

public class JoinType {

    public static final int NESTEDJOIN = 0;
    public static final int BLOCKNESTED = 1;
    public static final int SORTMERGE = 2;
    public static final int HASHJOIN = 3;

    public static int numJoinTypes() {
        return 4;
    }

    public static int getValidJoinType(int type) {
        if (type == HASHJOIN) {
            return NESTEDJOIN;
        }

        return type;
    }
}
