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
- The `PROJECT` operator (qp.operators.Project)

As part of the bonus tasks, the team have made the following additional modifications:

- The `INTERSECT`, `INTERSECT ALL`, `UNION `, `UNION ALL`  operator (qp.operators.Intersect)
- The `TIME` datatype

## Experiment results

To test our team's implementation of the query engine, some experiments were performed using the `Flights`, `Aircrafts`, `Schedule`, `Certified` and `Employees` table. The following are the results of each experiment:

### Joins
TODO: To include the timings, the plans used, and a discussion of the differences (if any) between timings on different algorithms and different plans.   Also discuss why query execution plan orderings can make a more significant difference in a real-world system with larger data sets.

### Scheduled pilots
TODO: Include above information.
