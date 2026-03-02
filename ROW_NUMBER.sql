--LEVEL 1 – BEGINNER
--1. Assign row numbers to employees ordered by EmployeeID.
--2. List row numbers for orders sorted by OrderDate.
--3. Show row numbers for products sorted by Price descending.
--LEVEL 2 – INTERMEDIATE
--4. For each department, assign row numbers ordered by salary.
--5. In each customer segment, number orders by total amount.
--6. For each city, rank stores by revenue using ROW_NUMBER.
--LEVEL 3 – ADVANCED
--7. Identify each department’s top 3 highest-paid employees.
--8. Find the latest order per customer using ROW_NUMBER().
--9. In each product category, return the second most expensive product.
--10. Detect duplicates in a table by assigning row_number partitioned by (Email) ordered by
--CreatedAt.
--11. Paginate results: return rows 11–20 from a students table ordered by Score.
--12. In each month, identify the first order placed.
--Use these problems to practice ROW_NUMBER() with PARTITION BY and ORDER BY clauses

--1. Assign row numbers to employees ordered by EmployeeID.
SELECT 
ROW_NUMBER() OVER ( ORDER BY EmployeeID ) AS RM, 
Employees.EmployeeID, Employees.EmployeeName, Employees.Department, Employees.Salary
FROM Employees

--2. List row numbers for orders sorted by OrderDate.
SELECT 
ROW_NUMBER() OVER( ORDER BY OrderDate) AS RM,
Orders.CustomerID, Orders.OrderDate, Orders.OrderID, Orders.Amount
FROM Orders

--3. Show row numbers for products sorted by Price descending.
SELECT 
ROW_NUMBER() OVER (ORDER BY Price DESC) AS RM,
Products.Category, Products.ProductID, Products.ProductName, Products.Price
FROM Products
--4. For each department, assign row numbers ordered by salary.
SELECT
Employees.EmployeeID, Employees.EmployeeName,Department,Salary,
ROW_NUMBER() OVER( PARTITION BY Department ORDER BY Salary) AS RM
FROM Employees

--6. For each city, rank stores by revenue using ROW_NUMBER.
SELECT 
City, price*quanity AS revenue,
ROW_NUMBER() OVER(PARTITION BY City ORDER BY price*quantity) AS RM
FROM Customer 
LEFT JOIN Orders ON Customer.ID = Orders.CustomerID

--LEVEL 3 – ADVANCED
--7. Identify each department’s top 3 highest-paid employees.
--8. Find the latest order per customer using ROW_NUMBER().
--9. In each product category, return the second most expensive product.
--10. Detect duplicates in a table by assigning row_number partitioned by (Email) ordered by

--7. Identify each department’s top 3 highest-paid employees.
SELECT 
*
FROM ( SELECT EmployeeName,Department,Salary,
ROW_NUMBER() OVER( PARTITION BY  Employees.Department ORDER BY Salary DESC) AS Rank
FROM Employees) AS T
WHERE Rank<=3

--8. Find the latest order per customer using ROW_NUMBER().
SELECT *
FROM(
SELECT OrderID,CustomerID, OrderDate,
ROW_NUMBER() OVER(PARTITION BY CustomerID ORDER BY (OrderDate))AS Latest_Order
FROM Orders) AS T
WHERE Latest_Order = 1


--9. In each product category, return the second most expensive product.
SELECT *
FROM(
SELECT ProductID, ProductName,Category, Price,
ROW_NUMBER() OVER(PARTITION BY Category ORDER BY Price) AS Expence
FROM Products) AS T
WHERE Expence = 2

--10. Detect duplicates in a table by assigning row_number partitioned by (Email) ordered by CreatedAT

SELECT Name,Email,CreatedAt,Email,
ROW_NUMBER() OVER(PARTITION BY Email ORDER BY CreatedAT) AS duplicate
FROM Customers

