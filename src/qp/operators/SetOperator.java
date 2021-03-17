/**
 * This is base class for all the set operators
 **/

package qp.operators;

import qp.utils.Schema;

/**
 * The type Set operator.
 */
public class SetOperator extends Operator {

    Operator left;                       // Left child
    Operator right;                      // Right child
    int setOpType;                       // Type of Set Operator
    int numBuff;                         // Number of buffers available

    /**
     * Instantiates a new Set operator.
     *
     * @param left  the left
     * @param right the right
     * @param type  the type
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
     * Gets num buff.
     *
     * @return the num buff
     */
    public int getNumBuff() {
        return numBuff;
    }

    /**
     * Sets num buff.
     *
     * @param num the num
     */
    public void setNumBuff(int num) {
        this.numBuff = num;
    }

    /**
     * Gets left.
     *
     * @return the left
     */
    public Operator getLeft() {
        return left;
    }

    /**
     * Sets left.
     *
     * @param left the left
     */
    public void setLeft(Operator left) {
        this.left = left;
    }

    /**
     * Gets right.
     *
     * @return the right
     */
    public Operator getRight() {
        return right;
    }

    /**
     * Sets set op type.
     *
     * @param type the type
     */
    public void setSetOpType(int type) {
        setOpType = type;
    }

    /**
     * Gets set opt tpe.
     *
     * @return the set opt tpe
     */
    public int getSetOptTpe() {
        return setOpType;
    }

    /**
     * Sets right.
     *
     * @param right the right
     */
    public void setRight(Operator right) {
        this.right = right;
    }

    /**
     *
     * @return
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
