/**
 * prepares a random initial plan for the given SQL query
 **/

package qp.optimizer;

import qp.operators.*;
import qp.utils.*;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;

public class RandomInitialPlan {

    SQLQuery sqlquery;

    ArrayList<Attribute> projectlist;
    ArrayList<String> fromlist;
    ArrayList<Condition> selectionlist;     // List of select conditons
    ArrayList<Condition> joinlist;          // List of join conditions
    ArrayList<Attribute> groupbylist;
    ArrayList<Attribute> orderByList;
    boolean isDescending;
    int numJoin;                            // Number of joins in this query
    HashMap<String, Operator> tab_op_hash;  // Table name to the Operator
    Operator root;                          // Root of the query plan tree               

    int setOpType;                          // Type of Set Operation (INTERSECT)

    public RandomInitialPlan(SQLQuery sqlquery) {
        this.sqlquery = sqlquery;
        projectlist = sqlquery.getProjectList();
        fromlist = sqlquery.getFromList();
        selectionlist = sqlquery.getSelectionList();
        joinlist = sqlquery.getJoinList();
        groupbylist = sqlquery.getGroupByList();
        orderByList = sqlquery.getOrderByList();
        isDescending = sqlquery.isDescending();
        numJoin = joinlist.size();
        setOpType = sqlquery.getSetOperationType();
    }

    /**
     * number of join conditions
     **/
    public int getNumJoins() {
        return numJoin;
    }

    /**
     * prepare initial plan for the query
     * @param operators additional operators provided if it's a set operation 
     **/
    public Operator prepareInitialPlan(Operator... operators) {
        if (operators.length > 0) {
            createSetOp(operators);
            createDistinctOp();
            return root; 
        }

        tab_op_hash = new HashMap<>();
        createScanOp();
        createSelectOp();
        if (numJoin != 0) {
            createJoinOp();
        }

        if (fromlist.size() > joinlist.size() + 1) {
            createProductOp();
        }

        createGroupByOp();

        if (orderByList.size() > 0) {
            createOrderByOp();
        }

        createProjectOp();
        createDistinctOp();

        return root;
    }

    /**
     * Create Scan Operator for each of the table
     * * mentioned in from list
     **/
    public void createScanOp() {
        int numtab = fromlist.size();
        Scan tempop = null;
        for (int i = 0; i < numtab; ++i) {  // For each table in from list
            String tabname = fromlist.get(i);
            Scan op1 = new Scan(tabname, OpType.SCAN);
            tempop = op1;

            /** Read the schema of the table from tablename.md file
             ** md stands for metadata
             **/
            String filename = tabname + ".md";
            try {
                ObjectInputStream _if = new ObjectInputStream(new FileInputStream(filename));
                Schema schm = (Schema) _if.readObject();
                op1.setSchema(schm);
                _if.close();
            } catch (Exception e) {
                System.err.println("RandomInitialPlan:Error reading Schema of the table " + filename);
                System.err.println(e);
                System.exit(1);
            }
            tab_op_hash.put(tabname, op1);
        }

        // 12 July 2003 (whtok)
        // To handle the case where there is no where clause
        // selectionlist is empty, hence we set the root to be
        // the scan operator. the projectOp would be put on top of
        // this later in CreateProjectOp
        if (selectionlist.size() == 0) {
            root = tempop;
            return;
        }

    }

    /**
     * Create Selection Operators for each of the
     * * selection condition mentioned in Condition list
     **/
    public void createSelectOp() {
        Select op1 = null;
        for (int j = 0; j < selectionlist.size(); ++j) {
            Condition cn = selectionlist.get(j);
            if (cn.getOpType() == Condition.SELECT) {
                String tabname = cn.getLhs().getTabName();
                Operator tempop = (Operator) tab_op_hash.get(tabname);
                op1 = new Select(tempop, cn, OpType.SELECT);
                /** set the schema same as base relation **/
                op1.setSchema(tempop.getSchema());
                modifyHashtable(tempop, op1);
            }
        }

        /** The last selection is the root of the plan tre
         ** constructed thus far
         **/
        if (selectionlist.size() != 0)
            root = op1;
    }

    /**
     * create cross product operators 
     */
    public void createProductOp() {
        int jnnum = numJoin; 
        String leftTable = null;
        Operator leftOp = null;
        Join cp = null;
        for (HashMap.Entry<String, Operator> entry : tab_op_hash.entrySet()) {
            String tableName = entry.getKey();
            Operator op = entry.getValue();
            if (leftOp == null) {
                leftTable = tableName;
                leftOp = op;
                continue; 
            }

            if (!leftOp.getSchema().checkCompat(op.getSchema())) {
                System.out.printf("Product needed! %s and %s\n\n", leftTable, tableName);
                cp = new Join(leftOp, op, OpType.JOIN);
                cp.setNodeIndex(jnnum);
                Schema newsche = leftOp.getSchema().joinWith(op.getSchema());
                cp.setSchema(newsche);

                modifyHashtable(leftOp, cp);
                modifyHashtable(op, cp);

                for (HashMap.Entry<String, Operator> oldentry : tab_op_hash.entrySet()) {
                    String oldtablename = oldentry.getKey();
                    Operator oldOP = oldentry.getValue();
                }

                leftTable = tableName;
                leftOp = cp;
                
                jnnum ++;
            }
        }

        if (cp != null) {
            root = cp;
        }
    }

    /**
     * create join operators
     **/
    public void createJoinOp() {
        Join jn = null;

        // Modify these few depending on what you need
        int[] conditionOrder = new int[]{ 0, 3, 1, 2 };
        int joinMeth = JoinType.SORTMERGE;
        boolean shouldFlip = true; 
        // End here 

        int count = 0; 
        /** Repeat until all the join conditions are considered **/
        while (count < conditionOrder.length) {
            /** If this condition is already consider chose
             ** another join condition
             **/

            int jnum = conditionOrder[count];

            Condition cn = (Condition) joinlist.get(jnum);
            String lefttab = cn.getLhs().getTabName();
            String righttab = ((Attribute) cn.getRhs()).getTabName();
            Operator left = (Operator) tab_op_hash.get(lefttab);
            Operator right = (Operator) tab_op_hash.get(righttab);
            jn = new Join(left, right, cn, OpType.JOIN);
            jn.setNodeIndex(jnum);
            Schema newsche = left.getSchema().joinWith(right.getSchema());
            jn.setSchema(newsche);

            /** randomly select a join type**/
            if (shouldFlip) {
                System.out.println(count);
                if (count % 2 == 0) {
                    joinMeth = JoinType.BLOCKNESTED;
                } else {
                    joinMeth = JoinType.SORTMERGE;
                }
            }
            count++; 
            System.out.println(count);

            jn.setJoinType(joinMeth);
            modifyHashtable(left, jn);
            modifyHashtable(right, jn);
        }

        /** The last join operation is the root for the
         ** constructed till now
         **/
        if (numJoin != 0)
            root = jn;
    }

    public void createProjectOp() {
        Operator base = root;
        if (projectlist == null)
            projectlist = new ArrayList<Attribute>();
        if (!projectlist.isEmpty()) {
            root = new Project(base, projectlist, OpType.PROJECT);
            Schema newSchema = base.getSchema().subSchema(projectlist);
            root.setSchema(newSchema);
        }
    }

    public void createOrderByOp() {
        Operator base = root;
        root = new Sort(base, orderByList, BufferManager.getNumberOfBuffers(), isDescending, OpType.SORT);
        Schema newSchema = base.getSchema();
        root.setSchema(newSchema);
    }

    public void createSetOp(Operator... operators) {
        Operator left = operators[0];
        Operator right = operators[1];
        switch(setOpType) {
            case OpType.INTERSECT:
                root = new Intersect(left, right, OpType.INTERSECT);
                break;
            case OpType.UNION:
                root = new Union(left, right, OpType.UNION);
                break;
            default: 
                System.out.println("Invalid Set Operation. Please use a valid set operation");
                System.exit(1);
        }
        Schema newSchema = left.getSchema();
        root.setSchema(newSchema);
    }

    /**
     * Supports the use of the DISTINCT operator.
     */
    public void createDistinctOp() {
        if (!sqlquery.isDistinct()) {
            return;
        }

        Operator base = root;
        root = new Distinct(root, sqlquery.getProjectList());
        root.setSchema(base.getSchema());
    }

    /**
     * Supports the use of the GROUP BY operator.
     */
    public void createGroupByOp() {
        if (!sqlquery.isGroupBy()) {
            return;
        }

        Operator base = root;
        root = new GroupBy(base, sqlquery.getGroupByList());
        root.setSchema(base.getSchema());
    }

    private void modifyHashtable(Operator old, Operator newop) {
        for (HashMap.Entry<String, Operator> entry : tab_op_hash.entrySet()) {
            if (entry.getValue().equals(old)) {
                entry.setValue(newop);
            }
        }
    }
}
