import configparser
import logging
import mysql.connector

# DROP TABLES IF EXISTS
drop_product_table = ("""DROP TABLE IF EXISTS product""")
drop_employee_table = ("""DROP TABLE IF EXISTS employee""")
drop_customer_table = ("""DROP TABLE IF EXISTS customer""")
drop_detailed_order = ("""DROP TABLE IF EXISTS detailed_order""")

# CREATE TABLE QUERIES FOR THE WAREHOUSEinst

create_detailed_order = ("""CREATE TABLE IF NOT EXISTS detailed_order(
    orderNumber int NOT NULL,
    orderDate date NOT NULL,
    shippedDate date DEFAULT NULL,
    requiredDate date DEFAULT NULL,
    orderStatus varchar(15) NOT NULL,
    orderLineNumber smallint(6) NOT NULL,
    cust_id int NOT NULL,
    emp_id int NOT NULL,
    prod_id varchar(15) NOT NULL,
    quantityOrdered int NOT NULL,
    tot_price int NOT NULL,
    days_delay_from_reqDate smallint DEFAULT NULL,
    num_days_to_ship smallint DEFAULT NULL,
    PRIMARY KEY (orderNumber)
    FOREIGN KEY(cust_id) REFERENCES customer (id),
    FOREIGN KEY(emp_id) REFERENCES employee (id),
    FOREIGN KEY(prod_id) REFERENCES product (id));""")


create_employee = ("""CREATE TABLE IF NOT EXISTS employee(
    id INT NOT NULL PRIMARY KEY IDENTITY (1,1)
    empNumber int NOT NULL PRIMARY KEY,
    lastName varchar(50) NOT NULL,
    firstName varchar(50) NOT NULL,
    extension varchar(10) NOT NULL,
    email varchar(100) NOT NULL,
    officeCode varchar(10) NOT NULL,
    reportsTo int DEFAULT NULL,
    jobTitle varchar(50) NOT NULL,
    empStatus char(15) DEFAULT 'Employed',
    FOREIGN KEY (reportsTo) REFERENCES employee (empNumber));""")


create_product = ("""CREATE TABLE IF NOT EXISTS product (
    id int NOT NULL PRIMARY KEY IDENTITY (1,1)
    prodCode varchar(15) NOT NULL PRIMARY KEY,
    prodName varchar(70) NOT NULL,
    prodLine varchar(50) NOT NULL,
    prodScale varchar(10) NOT NULL,
    prodVendor varchar(50) NOT NULL,
    prodDescription varchar(500) NOT NULL,
    buyPrice decimal(10,2) NOT NULL,
    MSRP decimal(10,2) NOT NULL,
    status varchar(10) DEFAULT 'Valid');""")


create_customer = ("""CREATE TABLE IF NOT EXISTS customer (
  id int NOT NULL PRIMARY KEY IDENTITY (1,1)
  custNumber int NOT NULL,
  custName varchar(50) NOT NULL,
  contactLastName varchar(50) NOT NULL,
  contactFirstName varchar(50) NOT NULL,
  phone varchar(50) NOT NULL,
  addressLine1 varchar(50) NOT NULL,
  addressLine2 varchar(50) DEFAULT NULL,
  city varchar(50) NOT NULL,
  state varchar(50) DEFAULT NULL,
  postalCode varchar(15) DEFAULT NULL,
  country varchar(50) NOT NULL,
  salesRepNumber int DEFAULT NULL,
  creditLimit decimal(10,2) DEFAULT NULL)""")


# INSERT TABLE DATA INTO WAREHOUSE
insert_detailedorder_to_dwh = ("""INSERT INTO detailed_order VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")

# insert product with default status
insert_product_to_dwh = ("""INSERT INTO product (prodCode, prodName, prodLine, prodScale, prodVendor, prodDescription, buyPrice, MSRP)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""")

# insert customer data
insert_customer_to_dwh = ("""INSERT INTO customer(custNumber, custName, contactLastName, contactFirstName, phone, addressLine1,
    addressLine2, city, state, postalCode, country, salesRepNumber, creditLimit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")


# insert with default empolyment status
insert_employee_to_dwh = ("""INSERT INTO employee(empNumber, lastName, firstName, extension, email, officeCode, reportsTo, jobTitle, empStatus)
                                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""")

# SELECT TABLE QUERY FROM DATABASE
select_orders_db = ("""SELECT od.orderNumber, orderDate, shippedDate, requiredDate, status AS orderStatus,
                            od.orderLineNumber, orders.customerNumber, cust.salesRepEmployeeNumber, od.productCode, quantityOrdered,
                            (quantityOrdered*priceEach) as tot_price,
                            (requiredDate-shippedDate) AS days_delay_from_reqDate, (shippedDate-orderDate) AS num_days_to_ship
                        FROM orderdetails AS od
                        JOIN orders ON orders.orderNumber = od.orderNumber
                        JOIN customers cust ON cust.customerNumber = orders.customerNumber
                        JOIN products ON od.productCode = products.productCode
                    """)

select_products_db = ("""SELECT productCode, productName, productLine, productScale, productVendor,
                        productDescription, buyPrice, MSRP FROM products""")

select_customers_db = ("""SELECT * FROM customers""")

select_employees_db = ("""SELECT * FROM employees""")

select_product_ids = ("""SELECT id,prodCode FROM product""")
select_customer_ids = ("""SELECT id,custNumber FROM customer""")
select_emp_ids = ("""SELECT id,empNumber FROM employee""")

# QUERY LISTS

create_table_queries = [create_product, create_customer, create_employee, create_detailed_order]
drop_table_queries = [drop_product_table, drop_customer_table, drop_employee_table, drop_detailed_order]
dimension_etl_query = [[select_products_db, insert_product_to_dwh],
                       [select_customers_db, insert_customer_to_dwh],
                       [select_employees_db, insert_employee_to_dwh]]
