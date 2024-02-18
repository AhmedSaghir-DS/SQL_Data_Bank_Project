select * from customer_nodes;
select * from customer_transactions;
select * from regions;

-- A. Customer Nodes Exploration -- 

-- 1. How many unique nodes are there on the Data Bank system?
SELECT count(distinct node_id) AS nodesCount 
FROM customer_nodes;

-- 2. What is the number of nodes per region?
SELECT r.region_name, count(c.node_id) as nodesCount
FROM customer_nodes c 
JOIN regions r ON r.region_id = c.region_id 
GROUP BY region_name
ORDER BY nodesCount DESC;

-- 3. How many customers are allocated to each region?
SELECT r.region_name, count(distinct customer_id) as customersCount
FROM customer_nodes c 
JOIN regions r ON r.region_id = c.region_id 
GROUP BY region_name
ORDER BY customersCount DESC;

-- 4. How many days on average are customers reallocated to a different node?
SELECT node_id AS Nodes, round(avg(datediff(end_date, start_date)),2) as averageReallocationDays
FROM customer_nodes WHERE YEAR(end_date) <> 9999
GROUP BY node_id
ORDER BY node_id;

-- 5. What is the median, 80th and 95th percentile for this same reallocation days metric for each region?
WITH rowsTable as (
SELECT c.customer_id, r.region_name, DATEDIFF(c.end_date, c.start_date) AS daysDifference,
row_number() over (partition by r.region_name order by DATEDIFF(c.end_date, c.start_date)) AS rowsNumber,
count(*) over (partition by r.region_name) as totalRows  
FROM customer_nodes c JOIN regions r ON c.region_id = r.region_id
WHERE c.end_date not like '%9999%'
)

SELECT region_name,
round(AVG(CASE WHEN rowsNumber between (totalRows/2) and ((totalRows/2)+1) THEN daysDifference END), 0) AS Median,
max(CASE WHEN rowsNumber = round((0.80 * totalRows),0) THEN daysDifference END) AS Percentile_80th,
max(CASE WHEN rowsNumber = round((0.95 * totalRows),0) THEN daysDifference END) AS Percentile_95th
FROM rowsTable
GROUP BY region_name;

-- B. Customer Transactions --

-- 1. What is the unique count and total amount for each transaction type?
SELECT txn_type, count(distinct customer_id) AS Count, sum(txn_amount) AS totalTransactions
FROM customer_transactions
GROUP BY txn_type;

-- 2. What is the average total historical deposit counts and amounts for all customers?
SELECT * FROM customer_transactions;
SELECT customer_id AS customer, count(customer_id) AS depositCount, round(avg(txn_amount),2) AS avgTotalDeposit 
FROM customer_transactions
WHERE txn_type = 'deposit'
GROUP BY customer_id
ORDER BY avgtotalDeposit DESC;

-- 3. For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?
SELECT Month, COUNT(customer_id) AS Customers
FROM ( SELECT MONTH(txn_date) AS month, customer_id,
    COUNT(CASE WHEN txn_type = 'Deposit' THEN 1 END) AS depositCount,
    COUNT(CASE WHEN txn_type = 'Purchase' THEN 1 END) AS purchaseCount,
    COUNT(CASE WHEN txn_type = 'Withdrawal' THEN 1 END) AS withdrawalCount
	FROM customer_transactions
	GROUP BY month, customer_id
	HAVING depositCount > 1 AND (purchaseCount >= 1 OR withdrawalCount >= 1)
	) AS monthlyTransactions
GROUP BY month
ORDER BY month;

-- 4. What is the closing balance for each customer at the end of the month?
SELECT customer_id, month(txn_date) AS Month,
SUM( CASE WHEN txn_type = 'Deposit' THEN txn_amount
	 WHEN txn_type IN ('Withdrawal', 'Purchase') THEN -txn_amount
	 ELSE 0 END
	) AS closingBalance
FROM customer_transactions
GROUP BY customer_id, Month
ORDER BY customer_id, Month;

-- 5. What is the percentage of customers who increase their closing balance by more than 5%?

WITH CustomerBalances AS 
(
SELECT customer_id,
SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE 0 END) AS totalDeposits,
SUM(CASE WHEN txn_type = 'purchase' THEN txn_amount ELSE 0 END) AS totalPurchases
FROM customer_transactions
GROUP BY customer_id
),
BalanceChanges AS 
(
SELECT customer_id, totalDeposits, totalPurchases,
(totalDeposits - totalPurchases) AS NetBalanceChange,
((totalDeposits - totalPurchases) / NULLIF(totalDeposits, 0)) * 100 AS PercentageIncrease
FROM CustomerBalances
)

SELECT COUNT(*) AS totalCustomers,
SUM(CASE WHEN PercentageIncrease > 5 THEN 1 ELSE 0 END) AS "countOfCustomersWith5%Increase",
(SUM(CASE WHEN PercentageIncrease > 5 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 AS "%ofCustomers"
FROM BalanceChanges;


-- C. Data Allocation Challenge --

-- 1. Running customer balance column that includes the impact each transaction
SELECT customer_id, txn_date, txn_amount,
SUM(txn_amount) OVER (PARTITION BY customer_id ORDER BY txn_date) AS runningBalance
FROM customer_transactions
ORDER BY customer_id, txn_date;

-- 2. Customer balance at the end of each month
SELECT customer_id, DATE_FORMAT(txn_date, '%Y-%m') AS month, SUM(txn_amount) AS monthEndBalance
FROM customer_transactions
GROUP BY customer_id, month
ORDER BY customer_id, month;

-- 3. Minimum, average and maximum values of the running balance for each customer
WITH RunningBalances AS 
(
SELECT customer_id, txn_date,
SUM(txn_amount) OVER (PARTITION BY customer_id ORDER BY txn_date) AS runningBalance
FROM customer_transactions
)

SELECT customer_id, min(runningBalance) AS minBalance, 
max(runningBalance) AS maxBalance, round(avg(runningBalance),2) AS avgBalance
FROM RunningBalances
GROUP BY customer_id;

/*select customer_id, txn_date, txn_type, txn_amount, 
sum(case when txn_type = 'deposit' then txn_amount else -txn_amount end) over (partition by customer_id order by txn_date) as currentBalance from
customer_transactions;*/
-- ------------------------------------------------------------------------------------