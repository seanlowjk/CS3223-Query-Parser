/**
 * Determines the sorting order, ASC or DESC
 */

package qp.utils;

public enum OrderDirection {

    ASC{
        @Override
        public boolean isAscending() {
            return true;
        }
    },
    DESC{
        @Override
        public boolean isAscending() {
            return false;
        }
    };

    public abstract boolean isAscending();

    public static OrderDirection getOrderDirection(boolean isDescending) {
        if (isDescending) {
            return OrderDirection.DESC;
        } else {
            return OrderDirection.ASC;
        }
    }
}
