# Data Engineering Project - Stock Analysis by the Minute 📈📊🐂

This Data Engineering Project - Stock Analysis by the Minute - is a data pipeline built on Google Cloud Platform (GCP) that retrieves real-time stock data from the Metatrader 5 platform and identifies the stocks with the highest positive or negative variation in the last minute. This project aims to provide valuable insights into stock market trends and facilitate informed decision-making for traders and investors, focusing on identifying potential trades with a good risk-reward ratio.

## Problem Statement

Traditional stock market analysis tools, such as Google Sheets, often suffer from a significant time delay in receiving real-time stock data. In the case of Google Sheets, this delay can be as long as 15 minutes, which hampers timely decision-making in fast-paced markets. Traders and investors require access to up-to-date information to make informed choices and take advantage of rapidly changing market conditions.

## Solution

The Data Engineering Project addresses the time delay problem by utilizing the power of Metatrader 5, a popular trading platform known for its real-time data feeds. By extracting real-time stock data directly from Metatrader 5 and leveraging the capabilities of Google Cloud Platform, our project enables near real-time analysis of stock market data, focusing on identifying stocks with significant price variations to identify trading opportunities.

## How it Works

The project utilizes PySpark, a Python library for distributed data processing, to perform batch processing on the stock data. The data extraction process fetches real-time stock data from Metatrader 5 and stores it in a suitable storage solution, such as Google Cloud Storage or Hadoop Distributed File System (HDFS).

PySpark is then used to process the stored data in batch mode, performing calculations on minute-to-minute stock price variations and identifying the stocks with the highest positive or negative changes within the specified time window. This information is crucial for traders aiming to enter trades quickly and take advantage of potential market opportunities.

The processed results are stored in a data storage solution, such as Google BigQuery or another suitable database, allowing for easy access, querying, and visualization of the identified stocks with significant price variations. This information can be used by traders, investors, and analysts to make informed decisions regarding their stock portfolios, with a particular focus on executing trades with a good risk-reward ratio.

## Used Technologies

For this project, the following tools and technologies were utilized:

- **Metatrader 5 & MQL5**: Metatrader 5 and MQL5 were used for data gathering, providing real-time stock data directly from the trading platform.

- **Prefect & GitHub Actions**: Prefect and GitHub Actions were used for workflow orchestration, allowing for the coordination and scheduling of data processing tasks.

- **Terraform**: Terraform was chosen as the Infrastructure-as-Code (IaC) tool to provision and manage the necessary infrastructure resources on Google Cloud Platform (GCP) for the data pipeline.

- **Google Cloud Storage (GCS)**: Google Cloud Storage was used as the data lake for storing the raw data retrieved from Metatrader 5 before further processing.

- **Google BigQuery**: Google BigQuery served as the project's data warehouse, enabling scalable storage and efficient querying of the processed data.

- **Dataproc, Clusters, Jobs & Cloud Scheduler**: GCP's Dataproc, Clusters, Jobs, and Cloud Scheduler were used for the transformation of raw data into refined data. These services facilitated the distributed processing and transformation of the data using PySpark.

- **Google Looker Studio** : Google Looker studio was used for visualizations, providing a platform to create and share interactive dashboards and reports based on the processed data.

These technologies were carefully chosen to ensure efficient data processing, reliable infrastructure management, and meaningful visualizations for the project.

## Key Features

- **Real-time data extraction from the Metatrader 5 trading platform**: The project leverages Python scripts to extract real-time stock data from Metatrader 5. The extracted data is stored in files generated by Metatrader 5 and uploaded to the project's Google Cloud Storage (GCS) bucket.

- **Ingestion of data into Google BigQuery**: The processed data is ingested into Google BigQuery, serving as the project's data warehouse. The BigQuery dataset table is partitioned by date, providing optimization for future queries. Additionally, the table is clustered by Ticker, improving query performance by physically organizing similar data together.

- **Efficient data management and storage**: The project utilizes Google Cloud Storage as a data lake for storing the raw data files generated by Metatrader 5. After the data is processed and ingested into Google BigQuery, the local files are deleted to free up storage space. Additionally, a follow-up list is updated to keep track of which files have already been processed, ensuring data integrity and preventing duplication.

- **Prefect orchestration for workflow management**: Prefect is used for orchestrating the data ingestion and processing workflows. It enables the scheduling, coordination, and monitoring of the different steps involved in the data pipeline, ensuring the timely execution of tasks and the proper handling of dependencies.

- **Batch processing and transformation with PySpark in Dataproc using Clusters, Jobs & Cloud Scheduler**: PySpark is utilized to process the uploaded data in batch mode. The data transformation process is carried out using PySpark on Google Cloud Dataproc. Clusters are provisioned to execute PySpark jobs, which perform the necessary transformations on the data. Cloud Scheduler is employed to create recurring schedules for the PySpark jobs, ensuring that the data is regularly updated and refined.

These key features collectively enable the project to efficiently ingest real-time stock data, process it in a scalable and distributed manner, and store it in Google BigQuery for further analysis and decision-making. The use of BigQuery partitioning and clustering enhances query performance and optimizes data storage. The transformation process utilizing PySpark on Dataproc ensures the data is refined and up to date, while Prefect orchestration facilitates the reliable and streamlined execution of the entire data pipeline.

# Installation and Setup

To set up the project, follow the detailed installation and configuration instructions provided below. The guide will walk you through the necessary steps, including the installation of required dependencies and the configuration of GCP services.

## 1 - Metatrader 5

To run the code and interact with Metatrader 5, follow these steps:

- **1.1. Download Metatrader 5**: Download the Metatrader 5 platform from the official MetaQuotes website or use the installer provided in this repository.

- **1.2. Install Metatrader 5**: Run the downloaded installer and follow the on-screen instructions to install Metatrader 5 on your computer. Choose the desired installation location and options during the installation process.

- **1.3. Open Metatrader 5**: Launch Metatrader 5 after the installation is complete.

- **1.4 Enable Expert Advisors (EAs)**: In Metatrader 5, go to `Options` → `Expert Advisors` and make sure the following settings are enabled:
   - Check the box for "Allow automated trading."
   - Check the box for "Allow DLL imports."
   - Check the box for "Confirm DLL function calls."
   - Click **OK** to save the changes.

- **1.5. Import the code**: In Metatrader 5, go to `File` → `Open Data Folder`. This will open the data folder associated with your Metatrader 5 installation.

- **1.6. Locate the MQL5 folder**: In the opened data folder, navigate to the `MQL5` folder. This is where you will place the code files.

- **1.7. Copy the code files**: Copy the code files from this repository and paste them into the `MQL5` folder. Ensure that the code files are placed in the correct directories according to their structure.

- **1.8. Compile the code**: In Metatrader 5, go to `View` → `Terminal` (or press `Ctrl + T`) to open the Terminal window. In the Terminal window, navigate to the `Navigator` tab, expand the `Expert Advisors` section, and find the imported code files. Right-click on each code file and select `Compile` to compile the code.

- **1.9. Attach the code to a chart**: Once the code is successfully compiled, you can attach it to a chart by dragging and dropping it from the `Navigator` window onto the desired chart. Adjust the code's input parameters if necessary.

- **1.10. Enable live trading**: To enable live trading with the code, ensure that the Auto Trading button in the toolbar is enabled (green). You may need to log in to your trading account within Metatrader 5 to enable live trading.

- **1.11. Start the code**: Click the "Play" button in the toolbar or press `F5` to start running the code on the attached chart.

These steps will guide you through the process of downloading, installing, and setting up Metatrader 5 to run the code using MQL5. Ensure that you have the necessary permissions and credentials to access the trading platform and start trading with the code. I used the stocks from the Brazilian Stock Index (Ibovespa), you may use any stocks that you see fit.
In case you don't wish to download and setup the Metatrader into in your computer, I provided a backup from all the files generated in 2023.04.28 so you can unzip it and try for yourself in a folder that you designate the files.
# 2 - GCP - Google Cloud Platform

Below are the two main steps to setup the GCP platform.

- **2.1 - Setup GCP Project**: Create a new project in the Google Cloud Console. Choose a meaningful name for your project and make note of the project ID.

- **2.2 - Create Service Account**

To create a service account in Google Cloud Platform (GCP), follow these steps:

1. Go to the Google Cloud Console and navigate to **IAM & Admin** → **Service Accounts**.

2. Click on the **Create Service Account** button to create a new service account.

3. Enter a **name** and **description** for the service account, and click **Create**.

4. In the **Service account permissions** section, grant the following roles to the service account:
   - **Viewer**: Allows viewing of resources in the project.
   - **Storage Admin**: Provides management access to Google Cloud Storage resources.
   - **Storage Object Admin**: Enables management of objects in Google Cloud Storage.
   - **BigQuery Admin**: Gives administrative access to manage and administer BigQuery resources.

5. Click **Continue** to proceed.

6. On the service account details page, click the three dots (**⋮**) on the right side and select **Manage keys**.

7. Click on **Add Key** → **Create new key**.

8. Choose the **JSON** key type and click **Create**.

9. The key file will be downloaded to your local computer, which you can use for authentication and authorization purposes.

By following these steps, you will create a service account with the necessary roles and generate a JSON key file to authenticate and authorize your application or project to access the specified GCP resources.
# 3 - Terraform


To install and set up Terraform, follow these steps:

- **3.1. Download Terraform**: Visit the official [Terraform downloads page](https://www.terraform.io/downloads.html) and download the appropriate Terraform package for your operating system.

- **3.2. Extract the Terraform binary**: After downloading the Terraform package, extract the contents of the package to a directory of your choice.

- **3.3. Add the Terraform binary to the system PATH**: Add the directory containing the Terraform binary to your system's `PATH` environment variable.

- **3.4. Verify the installation**: Open a new terminal or command prompt and run the following command to verify that Terraform is installed correctly:

   ```shell
   terraform version

If Terraform is installed correctly, you will see the version number printed in the terminal, like the image below.
![Terraform version](https://user-images.githubusercontent.com/22395461/235557232-2f6c4a57-17f5-4aa0-929f-1e8fcbe67876.JPG)

- **3.5. Authenticate with your cloud provider**: Configure API credentials or use a service account key file to authenticate with your cloud provider. Refer to your cloud provider's documentation for detailed instructions on authentication.

- **3.6. Initialize a Terraform project**: Navigate to the root directory of your Terraform project in the CLI and run the following command to initialize the project:
    ```shell
    terraform init

This command initializes the project, downloads the required provider plugins, and sets up the Terraform backend.

- **3.7. Write your infrastructure code**: Create or modify Terraform configuration files (with a .tf extension) to define your desired infrastructure resources and their configurations. Refer to the Terraform documentation and examples for guidance on writing Terraform code. Below are examples from this project for the files variables.tf:
![Terraform Variables](https://user-images.githubusercontent.com/22395461/235557290-c749e858-10b7-4f3a-ab21-2b5b55d40047.png)

- **3.8. Plan and apply changes**: Use the following commands to create an execution plan and apply changes to your infrastructure:
    ```shell
    terraform plan
    terraform apply

The terraform plan command creates an execution plan that previews the changes to your infrastructure. Review the plan to ensure it aligns with your expectations. The terraform apply command applies the changes. Enter yes when prompted to confirm.

- **3.9. Manage your infrastructure**: Use Terraform commands such as *plan*, *apply*, *destroy*, and more to manage and update your infrastructure. Refer to the Terraform documentation for a comprehensive list of available commands and their usage.

# 4 - Ingestion
## Prefect Orchestrator Configuration for Ingestion Process

To configure the Prefect orchestrator for the ingestion process, follow these steps:

- **4.1. Create a virtual environment**: Use the following commands to create a virtual environment and install the required dependencies:

    ```shell
    conda create -n de_project python=3.10
    conda activate de_project
    pip install -r requirements.txt
    ```

- **4.2. Start Prefect**: Use the following command to start Prefect Orion and visualize all the flows and tasks in the pipeline:

    ```shell
    prefect orion start
    ```

- **4.3. Create GCP Credentials & Bucket blocks**: Create a Google Cloud Storage (GCS) bucket and copy the key generated when you created the service account. Paste the key to authenticate and access the GCS bucket.
![Prefect GCS Credentials Block](https://user-images.githubusercontent.com/22395461/235559668-ac4d2557-17d0-49c8-94e2-edc8c4738f53.JPG)
------
![Prefect GCS Bucket Block](https://user-images.githubusercontent.com/22395461/235559680-286c8fab-02e9-4fae-86ef-c7fd05943162.JPG)

- **4.4. Prefect deployment & Agent creation**:

    - Build and deploy the "Local to GCP" flow:
        ```shell
        prefect deployment build 3_Ingestion/1_local_to_gcp_prefect.py:etl_local_to_gsc -n "Local to GCP"
        prefect deployment apply etl_local_to_gsc-deployment.yaml
        ```

    - Build and deploy the "GCS to BQ" flow:
        ```shell
        prefect deployment build 3_Ingestion/2_etl_gcs_to_bq.py:etl_gcs_to_bq -n "GCS to BQ"
        prefect deployment apply etl_gcs_to_bq-deployment.yaml
        ```

    - Start the Prefect agents for the respective work queues:
        ```shell
        prefect agent start --work-queue "AgentLocal"
        prefect agent start --work-queue "AgentBQ"
        ```
![Deployment Transform BQ](https://user-images.githubusercontent.com/22395461/235559977-7fdbe2d7-6670-4ef6-ab98-297ea872a201.JPG)
------
![Prefect Deployments](https://user-images.githubusercontent.com/22395461/235559706-5859608e-1bc7-4086-b2ac-864cf5880dab.png)
------
![Prefect - Work Queues](https://user-images.githubusercontent.com/22395461/235559712-b19f0cf7-816c-43fa-9d97-0c8a40f5f048.JPG)

- **4.5. Prefect Flow Runs follow-up**:

    - Monitor the status and progress of the Prefect flow runs using the Prefect UI or Prefect CLI, like in the image below where you can check the system running in a specific day, with time taken for each flow in seconds.
    - Use the Prefect UI to view logs, inspect task results, and manage the execution of the flows.
    - Use the Prefect CLI to interact with flows, perform operations, and get detailed information about the flow runs.
![Prefect Flows Run 28 04 23](https://user-images.githubusercontent.com/22395461/235559840-5aa4e23a-c952-419a-8429-e50a3f032988.JPG)

These steps will guide you through the configuration of the Prefect orchestrator for the ingestion process. Make sure to follow each step carefully and refer to the Prefect documentation for more advanced features and customization options.

# 5 - Data Warehouse - Partitioning and Clustering Optimization

To optimize query performance in the data warehouse, follow these steps:

- **5.1. Create a partitioned table**: After loading the data into BigQuery, create a partitioned table using SQL queries in Python. Here's an example of how to create a partitioned table using the `bq` Python package:

   ```python
   import pandas as pd
   from google.oauth2 import service_account

   credentials = service_account.Credentials.from_service_account_file(filename=`credentials_path`,
                                                                     scopes=["https://www.googleapis.com/auth/cloud-platform"])
      query = '''
      CREATE OR REPLACE TABLE `de-zoomcamp-project-hfelipini.de_project_dataset.python_test_partitioned`
      PARTITION BY DATE(DateTime)
      CLUSTER BY Ticker AS (
         SELECT * FROM `de-zoomcamp-project-hfelipini.de_project_dataset.python_test`
      );
   '''
   pd.read_gbq(credentials=credentials, query=query)

- **5.2. Verify the partition and clustering**: Use the BigQuery UI or the following command to verify that the table is properly partitioned and clustered:
   ```shell
   bq show --format=prettyjson project:dataset.table

Replace project:dataset.table with the destination table name.
In BigQuery will appear the new partitioned table with an altered icon, as seen in the image below.
![DW](https://user-images.githubusercontent.com/22395461/235561329-4949b5a9-cded-4449-b0e8-51d739fbab3f.JPG)

- **5.3. Run optimized queries**: With the partitioning and clustering in place, run queries against the table to take advantage of the optimization. Queries that include filtering on the partition field or the clustered fields should benefit from improved performance.

# 6 - Transformation - Option A - GCP, Option B - Local

# 7 - Report


## Usage

Once the project is properly installed and configured, you can run the batch processing job using PySpark, Prefect and GCP to retrieve and process real-time stock data. The resulting stocks with significant price variations can be accessed and analyzed through the designated data storage solution.

## Contributing

Contributions to the project are welcome! If you would like to contribute, please follow the guidelines outlined in the [Contributing Guide](link-to-contributing-guide). This guide provides information on how to set up a development environment, submit bug reports, suggest improvements, and propose new features.

## Next Steps

- Using streaming process instead of batch processing
- Just the tip of iceberg for future trading strategies
- Create the version with local processing using Docker and PostgreSQL
