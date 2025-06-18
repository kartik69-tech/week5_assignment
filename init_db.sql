-- Create the source and target databases
CREATE DATABASE IF NOT EXISTS source_db;
CREATE DATABASE IF NOT EXISTS target_db;

-- Use the source database
USE source_db;

-- Create the 'employees' table
CREATE TABLE IF NOT EXISTS employees (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(50)
);

-- Insert sample data into 'employees'
INSERT INTO employees (id, name, department) VALUES
(1, 'Alice', 'Engineering'),
(2, 'Bob', 'Sales'),
(3, 'Charlie', 'HR');

-- Create the 'sales' table
CREATE TABLE IF NOT EXISTS sales (
    sale_id INT PRIMARY KEY,
    date DATE,
    amount DECIMAL(10, 2)
);

-- Insert sample data into 'sales'
INSERT INTO sales (sale_id, date, amount) VALUES
(101, '2024-01-15', 2500.00),
(102, '2024-02-10', 800.00),
(103, '2024-03-20', 3200.00);
