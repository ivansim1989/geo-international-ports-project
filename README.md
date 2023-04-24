# Geo International Ports Project
## Description
This dataset represents the details of the ports around the world.

## _Instruction_
1. Create a custom **role** in Google Cloud with all the required permissions to operate in BigQuery and setup a **service account** with that role.
2. After creating the service account, click the **three-dot menu** under sev and select **Manage keys**.
3. Click **Add Key** and choose **JSON**. This will download a JSON credentials file to your computer.
4. Rename the JSON credentials file to **credentials.json** and place it in the parent directory of the project.
5. Create **dataset** under name **foodpandas_result** and tables below in Google BigQuery (_Due to time constraint, haven't explore how to create dataset and tables automatically_)

   i. 1_5_nearest_ports
   ii. 2_largest_number_of_port
   iii. 3_nearest_port 
6. Open the parent directory of the project in VS code or your preferred code editors and open the **terminal**.
7. Execute **. start.sh** in the terminal to setup the docker environments.
8. Setup email alart

   i. Setup app passwords in https://security.google.com/settings/security/apppasswords
   ii. Setup smtp under **airflow.cfg** as below (The rests remind the same)
       smtp_host = smtp.gmail.com
       smtp_user = {your email}
       smtp_password = {password generated from app password}
       smtp_port = 587
       smtp_mail_from = {your email}
9. Open web browser and go to **http://localhost:8080/**
   user: airflow
   password: airflow
10. Switch on the DAG **geo_international_ports** and click on it.
11. Click the play buttom on the top right for manual triggers (else it will be shceduled at 12am UTC daily)
12. Close the project by executing **. stop.sh** in terminal

**Note:** Assuming that you have already set up a basic development environment and docker desktop on your workstation.

## _Other considerations_
I was considering setting up separate Docker containers for Apache Spark and connected through the airflow connections. I wouldn't recommend setting up Spark within the Airflow container, as Airflow is meant for job orchestration, and combining them could lead to memory overflow issues when processing large datasets.

Although I successfully set up the Spark container, I encountered an issue with the BigQuery Connector that I couldn't resolve quickly. The error message was as follows:

`java.util.serviceconfigurationerror: org.apache.spark.sql.sources.datasourceregister: provider com.google.cloud.spark.bigquery.bigqueryrelationprovider could not be instantiated`

Furthermore, the dataset in question is not very large, so using Spark may not offer significant advantages. As a result, I decided to use Pandas for this task, as it is a powerful tool for data analysis and can handle the given dataset efficiently.