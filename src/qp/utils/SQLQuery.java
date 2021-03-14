/**
 * the format of the parser for SQL query, also see readme file
 **/

package qp.utils;

import java.util.ArrayList;

public class SQLQuery {

    SQLQuery leftQuery;                  // Left Operand for Set Operation
    SQLQuery rightQuery;                 // Right Operand for Set Operation
    int setOpType;                       // Type of Set Operation (INTERSECT)

    ArrayList<Attribute> projectList;    // List of project attributes from select clause
    ArrayList<String> fromList;          // List of tables in from clause
    ArrayList<Condition> conditionList;  // List of conditions appeared in where clause

    /**
     * represent the selection and join conditions
     * again in separate lists
     **/
    ArrayList<Condition> selectionList;  // List of select predicates
    ArrayList<Condition> joinList;       // List of join predicates
    ArrayList<Attribute> groupbyList;    // List of attibutes in groupby clause
    ArrayList<Attribute> orderbyList;    // List of attibutes in orderby clause

    boolean isDistinct = false;          // Whether distinct key word appeared in select clause
    boolean isDescending = false;        // Whether desc key word appeared in the orderby clause

    public SQLQuery(SQLQuery query1, SQLQuery query2) {
        leftQuery = query1;
        rightQuery = query2; 
        projectList = new ArrayList<>();
        fromList = new ArrayList<>();
        conditionList = new ArrayList<>();
        joinList = new ArrayList<>();
        groupbyList = new ArrayList<>();
        orderbyList = new ArrayList<>();
        selectionList = new ArrayList<>();
    }

    public SQLQuery(ArrayList<Attribute> list1, ArrayList<String> list2, ArrayList<Condition> list3) {
        projectList = list1;
        fromList = list2;
        conditionList = list3;
        groupbyList = new ArrayList<>();
        orderbyList = new ArrayList<>();
        joinList = new ArrayList<>();
        splitConditionList(conditionList);
    }

    // 12 july 2003 (whtok)
    // Constructor to handle no WHERE clause case
    public SQLQuery(ArrayList<Attribute> list1, ArrayList<String> list2) {
        projectList = list1;
        fromList = list2;
        conditionList = new ArrayList<>();
        joinList = new ArrayList<>();
        groupbyList = new ArrayList<>();
        orderbyList = new ArrayList<>();
        selectionList = new ArrayList<>();
    }

    /**
     * Split the condition list into selection and join list
     **/
    protected void splitConditionList(ArrayList<Condition> tempVector) {
        selectionList = new ArrayList<>();
        joinList = new ArrayList<>();
        for (int i = 0; i < tempVector.size(); ++i) {
            Condition cn = tempVector.get(i);
            if (cn.getOpType() == Condition.SELECT)
                selectionList.add(cn);
            else {
                Attribute lhs = cn.getLhs();
                Attribute rhs = (Attribute) cn.getRhs();
                // Conditions referring to attributes of the same table should be considered as a 'selection condition'
                if (lhs.getTabName().equals(rhs.getTabName())) {
                    selectionList.add(cn);
                } else {
                    joinList.add(cn);
                }
            }
        }
    }

    /** Precondition: leftQuery and rightQuery MUST exist */
    public boolean isUnionCompatible() {
        ArrayList<Attribute> leftAttributes = leftQuery.getProjectList();
        ArrayList<Attribute> rightAttributes = rightQuery.getProjectList();

        if (leftAttributes.size() != rightAttributes.size()) {
            return false;
        }

        for (int i = 0; i < leftAttributes.size(); i++) {
            if (leftAttributes.get(i).getProjectedType() != rightAttributes.get(i).getProjectedType()) {
                return false;
            }
        }

        return true; 
    }

    public int getSetOperationType() {
        return this.setOpType; 
    }

    public void setSetOperationType(int type) {
        this.setOpType = type;
    }

    public boolean isSetOperation() {
        return leftQuery != null && rightQuery != null; 
    }

    public SQLQuery getLeftQuery() {
        return leftQuery; 
    }

    public SQLQuery getRightQuery() {
        return rightQuery; 
    }

    public void setIsDistinct(boolean flag) {
        isDistinct = flag;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public ArrayList<Attribute> getProjectList() {
        return projectList;
    }

    public ArrayList<String> getFromList() {
        return fromList;
    }

    public ArrayList<Condition> getConditionList() {
        return conditionList;
    }

    public ArrayList<Condition> getSelectionList() {
        return selectionList;
    }

    public ArrayList<Condition> getJoinList() {
        return joinList;
    }

    public void setGroupByList(ArrayList<Attribute> list) {
        groupbyList = list;
    }

    public ArrayList<Attribute> getGroupByList() {
        return groupbyList;
    }

    public void setOrderByList(ArrayList<Attribute> list) {
        orderbyList = list;
    }

    public ArrayList<Attribute> getOrderByList() {
        return orderbyList;
    }

    public void setDescendingOrder() {
        isDescending = true;
    }

    public boolean isDescending() {
        return isDescending;
    }

    public int getNumJoin() {
        if (joinList == null)
            return 0;

        return joinList.size();
    }

    public int getNumOrder() {
        if (orderbyList == null)
            return 0;
        return orderbyList.size();
    }

    public boolean isGroupBy() {
        return !(groupbyList == null || groupbyList.isEmpty());
    }
}
