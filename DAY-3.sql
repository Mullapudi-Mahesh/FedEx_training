CREATE DATABASE sql_practice_day3;

CREATE TABLE stores(
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(100),
    city VARCHAR(50)
);

CREATE TABLE categories(
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100)
);

CREATE TABLE sales(
    sale_id SERIAL PRIMARY KEY,
    store_id INT REFERENCES stores(store_id),
    category_id INT REFERENCES categories(category_id),
    sale_month INT,
    amount NUMERIC(10,2)
);

CREATE TABLE employees_hierarchy(
    emp_id SERIAL PRIMARY KEY,
    emp_name VARCHAR(100),
    manager_id INT
);

CREATE TABLE app_logs(
    log_id SERIAL PRIMARY KEY,
    log_data JSONB
);


CREATE TABLE orders_xml(
    id SERIAL PRIMARY KEY,
    order_data XML
);

INSERT INTO stores(store_name,city) VALUES
('Store A','NY'),
('Store B','Chicago');


INSERT INTO categories(category_name) VALUES
('Electronics'),
('Clothing'),
('Grocery');

INSERT INTO sales(store_id,category_id,sale_month,amount) VALUES
(1,1,1,1000),
(1,2,1,500),
(1,3,1,300),
(1,1,2,1200),
(1,2,2,600),
(2,1,1,800),
(2,3,2,900);

INSERT INTO employees_hierarchy(emp_name,manager_id) VALUES
('CEO',NULL),
('Manager1',1),
('Manager2',1),
('Dev1',2),
('Dev2',2),
('Sales1',3);

INSERT INTO app_logs(log_data) VALUES
('{"user":"Mahesh","action":"login","device":"mobile","time":"2024-01-01"}'),
('{"user":"Ravi","action":"purchase","amount":1200,"items":[1,2,3]}'),
('{"user":"Mahesh","action":"logout"}');

INSERT INTO orders_xml(order_data) VALUES
('<order>
    <customer>Alice</customer>
    <items>
        <item price="100">Laptop</item>
        <item price="50">Mouse</item>
    </items>
</order>');


'''PIVOT'''

SELECT
    store_id,
    SUM(amount) FILTER (WHERE category_id=1) AS electronics,
    SUM(amount) FILTER (WHERE category_id=2) AS clothing,
    SUM(amount) FILTER (WHERE category_id=3) AS grocery
FROM sales
GROUP BY store_id;

SELECT
    store_id,
    SUM(CASE WHEN sale_month=1 THEN amount END) jan,
    SUM(CASE WHEN sale_month=2 THEN amount END) feb
FROM sales
GROUP BY store_id;



WITH RECURSIVE emp_tree AS (
    SELECT emp_id, emp_name, manager_id, 1 AS level
    FROM employees_hierarchy
    WHERE manager_id IS NULL

    UNION ALL

    SELECT e.emp_id, e.emp_name, e.manager_id, t.level+1
    FROM employees_hierarchy e
    JOIN emp_tree t ON e.manager_id=t.emp_id
)
SELECT * FROM emp_tree;



