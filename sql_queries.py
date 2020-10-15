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
    orderNumber int NOT NULL PRIMARY KEY,
    orderDate date NOT NULL,
    shippedDate date DEFAULT NULL,
    requiredDate date DEFAULT NULL,
    order_status varchar(15) NOT NULL,
    customerNumber int NOT NULL,
    employeeNumber int NOT NULL,
    productCode varchar(15) NOT NULL,
    quantityOrdered int NOT NULL,
    tot_price int NOT NULL,
    days_delay_from_reqDate smallint NOT NULL,
    num_days_to_ship smallint NOT NULL,
    CONSTRAINT orders_by_prod_ibfk_1 FOREIGN KEY (customerNumber) REFERENCES customers (customernumber),
    CONSTRAINT orders_by_prod_ibfk_2 FOREIGN KEY (employeeNumber) REFERENCES employees (employeeNumber),
    CONSTRAINT orders_by_prod_ibfk_3 FOREIGN KEY (productCode) REFERENCES products (productCode)
    )""")

create_employee = ("""CREATE TABLE IF NOT EXISTS employee(
    employeeNumber int NOT NULL PRIMARY KEY,
    lastName varchar(50) NOT NULL,
    firstName varchar(50) NOT NULL,
    extension varchar(10) NOT NULL,
    email varchar(100) NOT NULL,
    officeCode varchar(10) NOT NULL,
    reportsTo int DEFAULT NULL,
    jobTitle varchar(50) NOT NULL,
    CONSTRAINT employees_ibfk_1 FOREIGN KEY (reportsTo) REFERENCES employees (employeenumber),
    )""")


create_product = ("""CREATE TABLE IF NOT EXISTS product (
    productCode varchar(15) NOT NULL PRIMARY KEY,
    productName varchar(70) NOT NULL,
    productLine varchar(50) NOT NULL,
    productScale varchar(10) NOT NULL,
    productVendor varchar(50) NOT NULL,
    productDescription varchar(500) NOT NULL,
    buyPrice decimal(10,2) NOT NULL,
    MSRP decimal(10,2) NOT NULL,
    )""")

create_customer = ("""CREATE TABLE IF NOT EXISTS customer (
  customerNumber int NOT NULL PRIMARY KEY,
  customerName varchar(50) NOT NULL,
  contactLastName varchar(50) NOT NULL,
  contactFirstName varchar(50) NOT NULL,
  phone varchar(50) NOT NULL,
  addressLine1 varchar(50) NOT NULL,
  addressLine2 varchar(50) DEFAULT NULL,e
  city varchar(50) NOT NULL,
  state varchar(50) DEFAULT NULL,
  postalCode varchar(15) DEFAULT NULL,
  country varchar(50) NOT NULL,
  salesRepEmployeeNumber int DEFAULT NULL,
  creditLimit decimal(10,2) DEFAULT NULL,
)""")

# INSERT TABLE DATA INTO WAREHOUSE
insert_detailedorder_to_dwh = ("""INSERT INTO detailed_order VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")

insert_product_to_dwh = ("""INSERT INTO product VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""")

insert_customer_to_dwh = ("""INSERT INTO customer VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")

insert_employee_to_dwh = ("""INSERT INTO employee VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""")

# SELECT TABLE QUERY FROM DATABASE

select_orders_db = ("""SELECT od.orderNumber, orderDate, shippedDate, status AS order_status,
                            orders.customerNumber, od.productCode, quantityOrdered, (quantityOrdered*priceEach) as tot_value
                        FROM orderdetails AS od
                        JOIN orders ON orders.orderNumber = od.orderNumber
                        JOIN customers cust ON cust.customerNumber = orders.customerNumber
                        JOIN products ON od.productCode = products.productCode
                    """)

select_products_db = ("""SELECT productCode, productName, productLine, productScale, productVendor,
                        productDescription, buyPrice, MSRP FROM products""")

select_customers_db = ("""SELECT * FROM customers""")

select_employees_db = ("""SELECT * FROM employees""")

# QUERY LISTS
create_tables_query = [create_product, create_customer, create_employee, create_detailed_order]
drop_query_list = [drop_product_table, drop_customer_table, drop_employee_table, drop_detailed_order]
dimension_etl_query = [[select_products_db, insert_product_to_dwh],
                       [select_customers_db, insert_customer_to_dwh],
                       [select_employees_db, insert_employee_to_dwh],
                       [select_orders_db, insert_detailedorder_to_dwh]]
