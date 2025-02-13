/** What is analytic functions in SQL. This is a type of functions thta performs calculations across specified data range
of rows  related to the current rows withing a query result set. This  function are often used in combining with
the OVER() clause, which defines partitions and ordering of the data. 
Note: the partition and the OVER, you can partition the data into a subset like grouping and order the rows for the most broken 
detailed specified component (Granular control).

The common analytic function are
A. The Ranking Function includes:
1. The Row_Number () = A row number is used to assign a unique number to each row based on a specified order
2. Rank() = A Rank assigns a Rank to each row, with gaps if they are the same or ties 
3. Dense_Rank() = A Dense Rank works like a Rank but without gaps incase of same record or ties 

B. we have the window aggregate function 
1. The Sum() = The sum calculates the total running of the windows sum
2. The Min() = The min calculates the total running of the windows min
3. The Max() = The max calculates the total running of the windows max
4. The Avg() = The Avg calculates the total running of the windows avg


C. The Offset Functions 
1. The Lag() = The lag assess the value of a column in a previous row
2. The Lead() = The lead assess the value of a column in a subsequent row
3. The First_Value() = Returns the firts value in a partition 
4. The Last_Value() = Returns the last value in a partition 

These are the three characteristics of Analytic functions and are widely being used in advanced reporting and data analytics task

**/

CREATE TABLE Sales (
    SaleID INT,
    EmployeeID INT,
    DepartmentID INT,
    SaleAmount DECIMAL(10, 2),
    SaleDate DATE
);

INSERT INTO Sales VALUES
(1, 101, 1, 150.00, '2024-01-01'),
(2, 102, 1, 800.00, '2024-01-02'),
(3, 103, 2, 900.00, '2024-01-03'),
(4, 101, 1, 200.00, '2024-01-04'),
(5, 102, 1, 400.00, '2024-01-05'),
(6, 103, 2, 800.00, '2024-01-06'),
(7, 101, 1, 600.00, '2024-01-07'),
(8, 102, 1, 500.00, '2024-01-08');

SELECT *
FROM Sales
--ROW_NUMBER / rank /dense_rank
SELECT SaleID, EmployeeID, DepartmentID, SaleAmount,
row_number() over ( partition by DepartmentID order by SaleAmount Desc) as SalesRowNumber,
rank() over ( partition by DepartmentID order by SaleAmount Desc) as SalesRank, 
DENSE_RANK() over ( partition by DepartmentID order by SaleAmount Desc) as SalesDenseRank
from sales

--report the higest sales in each department from the sales table
select * 
from (
SELECT SaleID, EmployeeID, DepartmentID, SaleAmount,
row_number() over ( partition by DepartmentID order by SaleAmount desc) as SalesRowNumber
From Sales
) as rankedsales
where SalesRowNumber = 1

select * 
from (
SELECT SaleID, EmployeeID, DepartmentID, SaleAmount,
rANK() over ( partition by DepartmentID order by SaleAmount desc) as SalesRowNumber
From Sales
) as rankedsales
where SalesRowNumber = 3

select * 
from (
SELECT SaleID, EmployeeID, DepartmentID, SaleAmount,
DENSE_RANK() over ( partition by DepartmentID order by SaleAmount desc) as SalesRowNumber
From Sales
) as rankedsales
where SalesRowNumber IN (3 ,4)

-- USING SUM 
SELECT SaleID, DepartmentID, SaleAmount,
SUM(SaleAmount) over ( partition by DepartmentID order by SaleID ) as SalesRowNumber
From Sales

SELECT saledate,
SUM(SaleAmount) as dailysales,
SUM(SaleAmount) over (order by saledate ) as cumulativeSales
From Sales
group by SaleDate, SaleAmount