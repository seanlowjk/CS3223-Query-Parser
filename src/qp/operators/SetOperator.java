/**
 * This is base class for all the set operators
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Schema;

import java.util.ArrayList;

public class SetOperator extends Operator {

    Operator left;                       // Left child
    Operator right;                      // Right child
    int setOpType;                       // Type of Set Operator
    int numBuff;                         // Number of buffers available

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

    public int getNumBuff() {
        return numBuff;
    }

    public void setNumBuff(int num) {
        this.numBuff = num;
    }

    public Operator getLeft() {
        return left;
    }

    public void setLeft(Operator left) {
        this.left = left;
    }

    public Operator getRight() {
        return right;
    }

    public void setSetOpType(int type) {
        setOpType = type;
    }

    public int getSetOptTpe() {
        return setOpType;
    }

    public void setRight(Operator right) {
        this.right = right;
    }

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
