
DROP TABLE employees;
CREATE TABLE employees (
    emp_id INT,
    name VARCHAR(50),
    department VARCHAR(50),
    salary INT
);

INSERT INTO employees (emp_id, name, department, salary) VALUES
(1, 'Amit', 'IT', 60000),
(2, 'Neha', 'IT', 75000),
(3, 'Raj', 'IT', 60000),
(4, 'Priya', 'HR', 50000),
(5, 'Karan', 'HR', 50000),
(6, 'Sneha', 'HR', 70000),
(7, 'Vikas', 'Sales', 45000),
(8, 'Rohit', 'Sales', 55000);

select * from employees;

-- 1. ROW_NUMBER()
-- Assigns a unique sequential number to each row.
-- Even if salaries are the same, numbers will still be different.

-- Example (IT department):

-- Neha (75000) → 1
-- Amit (60000) → 2
-- Raj (60000) → 3

-- 2. RANK()
-- Same values get the same rank.
-- Skips the next rank(s) after a tie.

-- Example:

-- 75000 → Rank 1
-- 60000 → Rank 2
-- 60000 → Rank 2
-- 550000 -> Next rank → 4 (skips 3)


-- 3. DENSE_RANK()
-- Same values get the same rank.
-- No gaps in ranking.

-- Example:

-- 75000 → Rank 1
-- 60000 → Rank 2
-- 60000 → Rank 2
-- Next rank → 3 (no skipping)

-- 4. PERCENT_RANK()
-- Shows relative ranking between 0 and 1.
-- Formula used internally:
-- PERCENT_RANK= rank - 1 / total rows in partition -1
-- 	              
-- PERCENT_RANK() tells you how a row compares to others as a percentage instead of just a position.

--  Example:

-- Highest salary → 0
-- Lowest salary → 1 (or close depending on dataset)


SELECT 
    emp_id,
    name,
    department,
    salary,
    
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num,
    
    RANK() OVER (PARTITION BY department ORDER BY SALARY DESC) AS rank_val,
    
    DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rank_val,
    
    PERCENT_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS percent_rank_val

FROM employees;




-- Top 1 salary per department
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn
    FROM employees
) t
WHERE rn = 1;
