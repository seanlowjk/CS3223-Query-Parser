# CS3223 Database Systems Implementation

This project is part of the CS3223 Database Systems Implementation module taken at the National University of Singapore (NUS) School of Computing in AY2020/21 Semester 2. The project focuses on a simple SPJ (Select-Project-Join) query engine and its implementation, which provides some insights into how a modern database management system (DBMS) works in practice.

This repository contains code written as part of this project and is done by:

- Hsu Zhong Jun
- Low Jun Kai, Sean
- Ng Xin Pei

## Modifications
The following modifications to the original template was made by the team:

- Block Nested Loops Join (qp.operators.BlockNestedJoin)
- Sort Merge Join (qp.operators.SortMergeJoin)
- The `DISTINCT` operator (qp.operators.Distinct)
- The `ORDERBY` operator (qp.operators.Sort)
- The `GROUPBY` operator (qp.operators.GroupBy)

As part of the bonus tasks, the team have made the following additional modifications:

- The `INTERSECT` and `INTERSECT ALL` operators (qp.operators.Intersect)
- The `UNION` and `UNION ALL` operators (qp.operators.Union)
- The `TIME` datatype

## Setup Instructions
As this project is written in Java (primarily Java 11), ensure that you have [Java](https://openjdk.java.net/projects/jdk/11/) installed in your system.

1. Navigate to the project root and set up the environment using `source queryenv` (for Unix users) or `queryenv.bat` (for Windows users).
2. Build the Java project using `build.sh` (for Unix users) or `build.bat` (for Windows users).
