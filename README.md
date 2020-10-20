# aws_etl_rds_to_redshift
This is a small Etl python project that transforms and loads data from AWS RDS to AWS Redshift. The focus of this project is to understand how data modelling needs to be thought 
through in designing warehouse and how to create the scripts to create tables, load data and perform transformations. 


## setup
I am using a MySQL sample transactional database - classic models which is loaded into my AWS RDS. I am transforming this data and loading into AWS Redshift Datawarehouse. Both the
databases are with a public access setup though this wont be the case in reality. The databases are meant to be private(inaccessible via public internet), in which case the script 
would be run by an EC2 cluster with required permissions setup via an IAM role. 

### Model
The transactional model is as below:

![MySQL-Sample-Database-Schema](https://user-images.githubusercontent.com/7806480/96574487-3dfd7e80-131b-11eb-8fc7-ed0cd76ac0c4.png)

Creating a dimensions & measures table the model in datawarehouse is as below:

![Screenshot 2020-10-20 at 9 20 34 PM](https://user-images.githubusercontent.com/7806480/96574515-481f7d00-131b-11eb-987a-97164770ce8c.png)

### Data Transformations
- A surrogate key is created for each dimensions table instead of using the natural key. This is inorder to seperate the production system design from analytical model.
- The orders table granularity is set at each product in an order per customer. 
- Some aggregated transactions like tot_value, number of days it took to ship etc are added into the order measures.

## Knowledge gained
- Learnt about python logging and how to setup and read config.ini file. These two are crucial for any script generation. Logging can help to perform any post mortem analysis for 
scripts that ran overnight or without monitoring. Config file enables to share the script while protecting sensitive data like user ID and passwords.
- Understand how the datawarehouse modelling needs to capture transactional data and how the model should be more flexible than transactional systems. Difference between surrogate
keys and natural keys.

