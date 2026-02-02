CREATE DATABASE sql_practice;

CREATE TABLE departments (
    dept_id SERIAL PRIMARY KEY,
    dept_name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE employees (
    emp_id SERIAL PRIMARY KEY,
    emp_name VARCHAR(100),
    salary NUMERIC(10,2),
    dept_id INT REFERENCES departments(dept_id),
    manager_id INT,
    hire_date DATE DEFAULT CURRENT_DATE
);

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100),
    city VARCHAR(50),
    email VARCHAR(120) UNIQUE
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    price NUMERIC(8,2),
    stock INT
);


CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    product_id INT REFERENCES products(product_id),
    quantity INT,
    order_date DATE DEFAULT CURRENT_DATE
);


CREATE TABLE payments (
    payment_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(order_id),
    amount NUMERIC(10,2),
    payment_date DATE
);


INSERT INTO departments (dept_name)
VALUES ('HR'), ('IT'), ('Sales');

INSERT INTO employees (emp_name, salary, dept_id, manager_id, hire_date) VALUES
('Mahesh', 70000, 2, NULL, '2022-01-10'),
('Ravi', 50000, 2, 1, '2023-03-12'),
('Priya', 45000, 1, NULL, '2021-05-22'),
('Amit', 60000, 3, NULL, '2022-09-01'),
('John', 55000, 3, 4, '2023-01-15');

INSERT INTO customers (customer_name, city, email) VALUES
('Alice','NY','alice@gmail.com'),
('Bob','Chicago','bob@gmail.com'),
('Charlie','Dallas','charlie@gmail.com');

INSERT INTO products (product_name, price, stock) VALUES
('Laptop', 900, 20),
('Phone', 600, 30),
('Tablet', 300, 15);

INSERT INTO orders (customer_id, product_id, quantity, order_date) VALUES
(1,1,1,'2024-01-01'),
(2,2,2,'2024-01-03'),
(3,3,1,'2024-01-04'),
(1,2,3,'2024-01-05');

INSERT INTO payments (order_id, amount, payment_date) VALUES
(1,900,'2024-01-01'),
(2,1200,'2024-01-03'),
(3,300,'2024-01-04'),
(4,1800,'2024-01-05');


UPDATE employees
SET salary = salary * 1.10
WHERE dept_id = 2;

SELECT * FROM employees;

'''UPSERT '''
INSERT INTO products(product_id, product_name, price, stock)
VALUES (1, 'Laptop', 950, 25)
ON CONFLICT (product_id)
DO UPDATE SET
    price = EXCLUDED.price,
    stock = EXCLUDED.stock;

'''Inner join'''
SELECT e.emp_name, d.dept_name
FROM employees e
JOIN departments d
ON e.dept_id = d.dept_id;

'''Left join'''
SELECT c.customer_name, o.order_id
FROM customers c
LEFT JOIN orders o
ON c.customer_id = o.customer_id;

'''complex join'''

SELECT
    c.customer_name,
    p.product_name,
    o.quantity
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id;

'''Window functions'''

''' Rank by salary'''
SELECT
    emp_name,
    salary,
    RANK() OVER (ORDER BY salary DESC) AS rank
FROM employees;

'''Department wise row number'''
SELECT
    emp_name,
    dept_id,
    ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC)
FROM employees;


'''Running total of payments'''
SELECT
    payment_date,
    amount,
    SUM(amount) OVER (ORDER BY payment_date) AS running_total
FROM payments;

'''Analytical functions'''

'''Highest salary per department'''
SELECT DISTINCT
    dept_id,
    MAX(salary) OVER (PARTITION BY dept_id) AS max_salary
FROM employees;


'''CTE'''

'''Employee earning above avg'''

WITH avg_sal AS (
    SELECT AVG(salary) AS avg_salary FROM employees
)
SELECT *
FROM employees
WHERE salary > (SELECT avg_salary FROM avg_sal);


WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) rn
    FROM employees
)
SELECT *
FROM ranked
WHERE rn <= 2;


SELECT DISTINCT salary
FROM employees
ORDER BY salary DESC
OFFSET 1 LIMIT 1;

'''Sample SubQueries'''
SELECT emp_name
FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees);

SELECT customer_name
FROM customers
WHERE customer_id NOT IN (SELECT customer_id FROM orders);


'''VIEWS'''
CREATE VIEW sales_summary AS
SELECT c.customer_name,
SUM(p.amount) revenue
FROM payments p
JOIN orders o USING(order_id)
JOIN customers c USING(customer_id)
GROUP BY c.customer_name;


SELECT * FROM sales_summary;


'''STORED PROCEDURE'''
CREATE OR REPLACE PROCEDURE give_bonus(percent NUMERIC)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE employees
    SET salary = salary + salary*percent/100;
END;
$$;

CALL give_bonus(5);



