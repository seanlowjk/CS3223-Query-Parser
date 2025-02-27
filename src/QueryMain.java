/**
 * This is main driver program of the query processor
 **/

import qp.operators.Debug;
import qp.operators.Operator;
import qp.optimizer.BufferManager;
import qp.optimizer.PlanCost;
import qp.optimizer.RandomOptimizer;
import qp.parser.Scaner;
import qp.parser.parser;
import qp.utils.*;

import java.io.*;
import java.sql.Time;

public class QueryMain {

    static PrintWriter out;
    static int numAtts;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("usage: java QueryMain <queryfilename> <resultfile> <pagesize> <numbuffer>");
            System.exit(1);
        }

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        Batch.setPageSize(getPageSize(args, in));

        SQLQuery sqlquery = getSQLQuery(args[0]);

        /**
         * Checks if the sql query is a set operation. 
         */
        if (sqlquery.isSetOperation()) {
            /**
             * Get the left query from the set operation and configure the query parameters. 
             */
            SQLQuery leftquery = sqlquery.getLeftQuery();
            configureBufferManager(leftquery.getNumJoin(), leftquery.getNumOrder(), leftquery.isDistinct(),
                leftquery.isGroupBy(), leftquery.isSetOperation(), args, in);

            System.out.println("=================  left set operand ===================================");
            Operator leftroot = getQueryPlan(leftquery);
            System.out.println("==========  end of left set operand ===================================n");

            /**
             * Get the right query from the set operation and configure the query parameters. 
             */
            SQLQuery rightquery = sqlquery.getRightQuery();
            configureBufferManager(rightquery.getNumJoin(), rightquery.getNumOrder(), rightquery.isDistinct(),
                rightquery.isGroupBy(), rightquery.isSetOperation(), args, in);

            System.out.println("================= right set operand ===================================");
            Operator rightroot = getQueryPlan(rightquery);
            System.out.println("========== end of right set operand ===================================n");

            /**
             * Configure the main query based on the left, right operators and the set operation itself. 
             */
            configureBufferManager(sqlquery.getNumJoin(), sqlquery.getNumOrder(),
                sqlquery.isDistinct(), sqlquery.isGroupBy(), sqlquery.isSetOperation(), args, in);

            Operator root = getQueryPlan(sqlquery, leftroot, rightroot);
            printFinalPlan(root, args, in);
            executeQuery(root, args[1]);
        } else {
            configureBufferManager(sqlquery.getNumJoin(), sqlquery.getNumOrder(),
                sqlquery.isDistinct(), sqlquery.isGroupBy(), sqlquery.isSetOperation(), args, in);

            Operator root = getQueryPlan(sqlquery);
            printFinalPlan(root, args, in);
            executeQuery(root, args[1]);
        }
    }

    /**
     * Get page size from arguments, if not provided request as input
     **/
    private static int getPageSize(String[] args, BufferedReader in) {
        int pagesize = -1;
        if (args.length < 3) {
            /** Enter the number of bytes per page **/
            System.out.println("enter the number of bytes per page");
            try {
                String temp = in.readLine();
                pagesize = Integer.parseInt(temp);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else pagesize = Integer.parseInt(args[2]);
        return pagesize;
    }

    /**
     * Parse query from query file
     **/
    public static SQLQuery getSQLQuery(String queryfile) {
        /** Read query file **/
        FileInputStream source = null;
        try {
            source = new FileInputStream(queryfile);
        } catch (FileNotFoundException ff) {
            System.out.println("File not found: " + queryfile);
            System.exit(1);
        }

        /** Scan the query **/
        Scaner sc = new Scaner(source);
        parser p = new parser();
        p.setScanner(sc);

        /** Parse the query **/
        try {
            p.parse();
        } catch (Exception e) {
            System.out.println("Exception occured while parsing");
            System.exit(1);
        }

        return p.getSQLQuery();
    }

    /**
     * If there are joins then assigns buffers to each join operator while preparing the plan.
     * As buffer manager is not implemented, just input the number of buffers available.
     **/
    private static void configureBufferManager(int numJoin, int numOrder,
            boolean isDistinct, boolean isGroupBy, boolean isSetOp, String[] args,
            BufferedReader in) {
        if (numJoin != 0 || numOrder != 0 || isDistinct || isGroupBy || isSetOp) {
            int numBuff = 1000;
            if (args.length < 4) {
                System.out.println("enter the number of buffers available");
                try {
                    String temp = in.readLine();
                    numBuff = Integer.parseInt(temp);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else numBuff = Integer.parseInt(args[3]);
            BufferManager bm = new BufferManager(numBuff, numJoin);
        }

        /** Check the number of buffers available is enough or not **/
        int numBuff = BufferManager.getBuffersPerJoin();
        if (numJoin > 0 && numBuff < 3) {
            System.out.println("Minimum 3 buffers are required per join operator ");
            System.exit(1);
        }
    }

    /**
     * Run optimiser and get the final query plan as an Operator
     **/
    public static Operator getQueryPlan(SQLQuery sqlquery, Operator... operators) {
        Operator root = null;

        RandomOptimizer optimizer = new RandomOptimizer(sqlquery);
        Operator planroot = optimizer.getOptimizedPlan(operators);

        if (planroot == null) {
            System.out.println("DPOptimizer: query plan is null");
            System.exit(1);
        }

        root = RandomOptimizer.makeExecPlan(planroot);

        /**
         * If there maximum tuple size is greater than the batch size
         * provided by the user, throw an error and exit the program, indicating
         * the minimum number of bytes needed to be supplied by the user. 
         */
        if (RandomOptimizer.maxTupleSize > Batch.getPageSize()) {
            System.out.println("Cannot proceed with final plan.");
            System.out.printf("Error: Minimum %d bytes per page.\nYou only have given %d bytes per page.\n",
                RandomOptimizer.maxTupleSize, Batch.getPageSize());
            System.exit(1);
        }

        return root;
    }

    /**
     * Print final Plan and ask user whether to continue
     **/
    private static void printFinalPlan(Operator root, String[] args, BufferedReader in) {
        System.out.println("----------------------Execution Plan----------------");
        Debug.PPrint(root);
        PlanCost pc = new PlanCost();
        System.out.printf("\nExpected cost: %d\n", pc.getCost(root));
        if (args.length < 5) {
            /** Ask user whether to continue execution of the program **/
            System.out.println("enter 1 to continue, 0 to abort ");
            try {
                String temp = in.readLine();
                int flag = Integer.parseInt(temp);
                if (flag == 0) {
                    System.exit(1);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Execute query and print run statistics
     **/
    public static double executeQuery(Operator root, String resultfile) {
        long starttime = System.currentTimeMillis();
        if (root.open() == false) {
            System.out.println("Root: Error in opening of root");
            System.exit(1);
        }
        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter(resultfile)));
        } catch (IOException io) {
            System.out.println("QueryMain:error in opening result file: " + resultfile);
            System.exit(1);
        }

        /** Print the schema of the result **/
        Schema schema = root.getSchema();
        numAtts = schema.getNumCols();
        printSchema(schema);

        /** Print each tuple in the result **/
        Batch resultbatch;
        while ((resultbatch = root.next()) != null) {
            for (int i = 0; i < resultbatch.size(); ++i) {
                printTuple(resultbatch.get(i));
            }
        }
        root.close();
        out.close();

        long endtime = System.currentTimeMillis();
        double executiontime = (endtime - starttime) / 1000.0;
        System.out.printf("Execution time = %.4f\n", executiontime);
        return executiontime;
    }

    protected static void printSchema(Schema schema) {
        String[] aggregates = new String[]{"", "MAX", "MIN", "SUM", "COUNT", "AVG"};
        for (int i = 0; i < numAtts; ++i) {
            Attribute attr = schema.getAttribute(i);
            int aggregate = attr.getAggType();
            String tabname = attr.getTabName();
            String colname = attr.getColName();
            if (aggregate == 0) {
                out.print(tabname + "." + colname + "  ");
            } else {
                out.print(aggregates[aggregate] + "(" + tabname + "." + colname + ")  ");
            }
        }
        out.println();
    }

    protected static void printTuple(Tuple t) {
        for (int i = 0; i < numAtts; ++i) {
            Object data = t.dataAt(i);
            if (data instanceof Integer) {
                out.print(((Integer) data).intValue() + "\t");
            } else if (data instanceof Float) {
                out.print(((Float) data).floatValue() + "\t");
            } else if (data instanceof Time) {
                out.print(((Time) data).getTime() + "\t");
            } else if (data == null) {
                out.print("-NULL-\t");
            } else {
                out.print(((String) data) + "\t");
            }
        }
        out.println();
    }
}