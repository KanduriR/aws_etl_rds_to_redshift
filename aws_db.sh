#!bin/bash
aws rds start-db-instance --db-instance-identifier classic-models
aws redshift resume-cluster --cluster-identifier classic-models-dwh
aws rds describe-db-instances
aws redshift describe-clusters
