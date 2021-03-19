# CS3223 Database Systems Implementation

This project is part of the CS3223 Database Systems Implementation module taken at the National University of Singapore (NUS) School of Computing in AY2020/21 Semester 2. The project focuses on a simple SPJ (Select-Project-Join) query engine and its implementation, which provides some insights into how a modern database management system (DBMS) works in practice.

This repository contains code written as part of this project and is done by:

- Hsu Zhong Jun
- Low Jun Kai, Sean
- Ng Xin Pei

## Setup Instructions
As this project is written in Java (primarily Java 11), ensure that you have [Java](https://openjdk.java.net/projects/jdk/11/) installed in your system.

1. Navigate to the project root and set up the environment using `source queryenv` (for Unix users) or `queryenv.bat` (for Windows users).
2. Build the Java project using `build.sh` (for Unix users) or `build.bat` (for Windows users).

## Running the project
1. Populate the database by running `bash populate_table.sh`
2. The queries can be ran by `java QueryMain your_query.sql your_query_result.out`

## Modifications
The following modifications to the original template was made by the team:

- Block Nested Loop Join (`qp.operators.BlockNestedJoin`)
- Sort Merge Join (`qp.operators.SortMergeJoin`)
- The `DISTINCT` operator (`qp.operators.Distinct`)
- The `ORDERBY` operator (`qp.operators.Sort`)
- The `GROUPBY` operator (`qp.operators.GroupBy`)
- The `PROJECT` operator (`qp.operators.Project`)

As part of the bonus tasks, the team have made the following additional modifications to set operations:
- The INTERSECT and INTERSECT ALL operator (`qp.operators.Intersect`)
- The UNION and UNION ALL operator (`qp.operators.Union`)

A new `TIME` data type was also implemented to support generating time based value.

Bugs fixed:
1. Cartesian product was not supported in the original implementation and has now been fixed by treating it as a special join operator with no join conditions. (`qp.optimizer.RandomInitialPlan`)
2. Running the query engine with a smaller amount of bytes per page required would cause it to enter into an infinite loop. This is now fixed by checking if the number of bytes per page specified by the user is sufficiently large. (`QueryMain`)
3. The join operator was not closed at the end of the execution and has now been fixed. (`qp.operators.NestedJoin`)
4. The original nested join operator did not consider conditions with expressions other than the equality expression. This is now fixed by checking the conditions more appropriately based on the expression provided. (`qp.utils.Tuple`)
