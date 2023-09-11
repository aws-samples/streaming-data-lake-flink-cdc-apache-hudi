# Build a modern streaming data architecture using Flink CDC and Apache Hudi

This sample demonstrates how using Flink CDC connectors and Apache Hudi we are able to build a modern streaming data lake by only using an Amazon Kinesis Data Analytics Application for Apache Flink. As well on how to manage AWS Lake Formation when working with KDA Studio.

### Dependencies
This sample uses the following libraries and versions (Already included when deploying application)
- Hudi 0.13.1
- Flink 1.15
- PostgreSQL CDC Flink Connector 2.4.1
- PostgreSQL 12.12

## Overall architecture

![Architecture](/img/architecture.png)

In this sample, we will have an Amazon EventBridge Rule triggering an AWS Lambda Function that will be simulating CDC data into our Amazon RDS PostgreSQL. We will be consuming those changes using an Apache Flink Application, which we'll deploy using Amazon Kinesis Data Analytics Studio. Within the Studio, we will be consuming from 3 different tables and write the results into an Amazon S3 Bucket using Apache Hudi. By using Apache Hudi, we will be able to perform ACID transaction on top of our Data lake, which can be later on queried using Amazon Athena

## Getting started

In this sample you will deploy a CDK application that configures our streaming infrastructure. It will create
- An Amazon VPC with public and private subnets, including a NAT Gateway
- A lambda function that will generate upserts in our Database
- An EventBridge rule that will trigger the lambda function every minute
- A RDS PostgreSQL Database launched in a private subnet
- An EC2 instance that will load an sporttickets databases (Mostly used in DMS Labs/Workshops)
- A Glue Database, which will be used in the Amazon Kinesis Data Analytics Studio Application for creating the Sources and Sinks of the application
- An Amazon Kinesis Data Analytics Studio application deployed in the VPC, already configured with the necessary connectors for connecting to the Postgres Database and writing into S3 using Apache Hudi
- A second Glue Database, which we'll use to be able to query the data from Amazon Athena
- Finally, it will generate the DDL for our Hudi Table.

### Deploy infrastructure on AWS

Make sure you have the necessary IAM permissions to deploy CDK and all resources that will be deployed in this sample

#### Pre-requisites

If you want to use AWS Lake Formation to manage permissions and access to the Hudi Table, please do the following, if not you can continue to **Deployment**

1. If its your first time using AWS Lake Formation it will as to set yourself as Data Lake Admin
2. Under **Permissions** select *Administrative roles and tasks*
   1. Under Database creators revoke the permission to the **IAMAllowedPrincipals**
3. Under **Data Catalog**, select *Settings*
   1. Unselect the following statements
      - Use only IAM access control fo new databases
      - Use only IAM access control for new tables in new databases 

#### Deployment 

4. Clone this repository
    ```shell
   git clone <repository>
   ```
5. Navigate into the directory
    ```shell
    cd streaming-data-lake-flink-cdc-apache-hudi
    ```
6. Download and install the necessary npm modules
    ```shell
   npm install
    ```
7. Navigate into the connectors folder and run the download_connectors script
    ```shell
   cd connectors
   sh download_connectors.sh
   cd ..
   ```
8. Go to the scripts folder and download the necessary library for the CDC Lambda Function
    ```shell
   cd scripts/cdclambda
   npm install
    ```
9. Bootstrap the CDK Application
    ```shell
    cd ../..
    cdk bootstrap
    ```
10. If you were already using AWS Lake Formation or followed the pre-requisites to enable AWS Lake Formation.
    1. Go to AWS Lake Formation
       1. Under **Permissions**, select *Administrative roles and tasks*
       2. In Database creators, grant permissions to the Cloudformation Execution Role created in the CDKToolkit Bootstrap Template
          - It follows a similar format as *cdk-<hash>-cfn-exec-role-<account-number>-<region>*

11. Deploy the application
     ```shell
     cdk deploy
     ```
   
The deployment should take around 15 minutes to deploy, as it waits for the Database to be loaded with the data.

12. After the Cloudformation template, go back to AWS Lake Formation.
    1. Under **Permissions**, select *Administrative roles and tasks*
       - In Database creators, grant permissions to the KDA Role created in the Cloudformation Template

### Start the Amazon Kinesis Data Analytics Studio

1. Navigate to the Amazon Kinesis console 

2. Go to Analytics applications

3. Select Studio

4. Select the *cdc-hudi-studio*

We have configured the notebook to have a parallelism of 8 with 1 parallelism per KPU, giving us a total of 8 KPU for our Studio environment

### Open The Apache Zepellin environment

1. Once the status of the Studio notebook is *Running*, you may click the **Open in Apache Zepellin**

2. In the new tab, click in import note

3. Provide a name to the note such as **CDC-Hudi-Notebook** and upload the *cdc-hudi-notebook.zpln* that is in the directory

4. Follow the instructions in the notebook.

### Amazon Athena

When writing Hudi Tables that have not been created using AWS Glue, we have to register them manually. Luckily our CDK application has already done that for us in the *hudidatabase*. You can now query the Apache Hudi Table directly from Amazon Athena

1. First go to AWS Lake Formation and give your User/Role permissions to Select columns from the ticket_view table in the Hudi Database
2. Navigate to the Amazon Athena console
3. If you have not, configure an Amazon S3 Bucket for your result ouputs
4. Verify in the left, that you are using the **hudidatabase**
5. Select the ticket_view and click Preview Table

## Clean Up
1. You can now execute the last cell in the Zepellin notebook in order to delete all tables in the Glue Data Catalog
2. To delete all created stack resources you can use
```shell
  cdk destroy --all
```

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
