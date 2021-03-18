/**
 * This is base class for all the set operators
 **/

package qp.operators;

import qp.utils.Schema;

/**
 * The Set operator.
 */
public class SetOperator extends Operator {

    Operator left;                       // Left child
    Operator right;                      // Right child
    int setOpType;                       // Type of Set Operator
    int numBuff;                         // Number of buffers available

    /**
     * Instantiates a new Set operator.
     *
     * @param left  the left operator
     * @param right the right operator
     * @param type  the type of set operation
     */
    public SetOperator(Operator left, Operator right, int type) {
        super(type);
        this.left = left;
        this.right = right;
        if (!this.left.getSchema().checkCompat(this.right.getSchema())) {
            System.out.println("Check your relations for set compatibility");
            System.exit(1);
        }
        schema = this.left.getSchema(); 
    }

    /**
     * Gets number of available buffer.
     * @return the number of available buffer.
     */
    public int getNumBuff() {
        return numBuff;
    }

    /**
     * Sets number of available buffer.
     * @param num, the number of available buffer.
     */
    public void setNumBuff(int num) {
        this.numBuff = num;
    }

    /**
     * Gets left operator.
     * @return the left operator.
     */
    public Operator getLeft() {
        return left;
    }

    /**
     * Sets left operator.
     * @param left the left operator.
     */
    public void setLeft(Operator left) {
        this.left = left;
    }

    /**
     * Gets right operator.
     * @return the right operator.
     */
    public Operator getRight() {
        return right;
    }

    /**
     * Sets set operation type.
     * @param type the set operation.
     */
    public void setSetOpType(int type) {
        setOpType = type;
    }

    /**
     * Gets set opt type.
     * @return the set opt type.
     */
    public int getSetOptTpe() {
        return setOpType;
    }

    /**
     * Sets right operator.
     *
     * @param right the right operator.
     */
    public void setRight(Operator right) {
        this.right = right;
    }

    /**
     * Clones a new Set operator.
     * @return new SetOperator object.
     */
    public Object clone() {
        Operator newleft = (Operator) left.clone();
        Operator newright = (Operator) right.clone();
        SetOperator setOp = new SetOperator(newleft, newright, optype);
        Schema newsche = newleft.getSchema();
        setOp.setSchema(newsche);
        setOp.setOpType(optype);
        setOp.setNumBuff(numBuff);
        return setOp;
    }
}
