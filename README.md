# Predictive Maintenance ETL + ML

## Project Overview
*This project implements an **end-to-end predictive maintenance system** using the **AI4I 2020 dataset***.  

The pipeline simulates IoT-style streaming of machine sensor data, performs automated ETL/ELT for processing and warehousing, and integrates an ML model to predict equipment failures. Predictions are served via a FastAPI service and results are visualized in Power BI dashboards.

---

##  Problem Statement
Modern manufacturing environments rely on continuous monitoring of equipment to prevent costly downtime.  
The objective of this project is to design a scalable data pipeline that:
1. Ingests and processes machine sensor data in real time.  
2. Transforms and stores it in a warehouse with a dimensional model for analytics.  
3. Trains and deploys a machine learning model to predict machine failures.  
4. Exposes predictions via an API and provides business insights through dashboards.  

---

## 🛠 Tech Stack
- **Ingestion**: Kafka (simulate IoT sensor streaming)  
- **Processing**: Spark Structured Streaming  
- **Storage**: Snowflake  
- **Transformations**: dbt (star schema modeling)  
- **Orchestration**: Apache Airflow  
- **Analytics**: Power BI  
- **Machine Learning**: XGBoost (with scikit-learn baselines)
- **Deployment**: FastAPI (REST API for model inference)  

---

- ## 📊 Dataset
- **Source**: [Predictive Maintenance Dataset (AI4I 2020)](https://www.kaggle.com/datasets/stephanmatzka/predictive-maintenance-dataset-ai4i-2020)  
- **Size**: 10,000 rows, 14 features  
- **Target Variable**: `machine_failure` (binary classification)  
- **Features include**: air/process temperature, rotational speed, torque, tool wear, and multiple failure modes.  

**Reference**:  
S. Matzka, *Explainable Artificial Intelligence for Predictive Maintenance Applications,* 2020 Third International Conference on Artificial Intelligence for Industries (AI4I), pp. 69–74, doi: [10.1109/AI4I49448.2020.00023](https://doi.org/10.1109/AI4I49448.2020.00023).

---

##  Features
- Real-time ingestion of synthetic IoT machine data (Kafka → Spark → Snowflake)  
- Automated data transformations and star schema creation with dbt  
- Workflow scheduling and monitoring with Airflow  
- ML pipeline for predicting machine failures (XGBoost)  
- REST API with FastAPI for serving predictions  
- Interactive dashboard in Power BI showing failure trends, root causes, and predictions  

---

## 📈 Future Enhancements
- Add Docker for containerized deployment of all components  
- Integrate CI/CD for automated testing and deployment  
- Extend ML layer with deep learning models (PyTorch/TensorFlow)  
- Deploy to a cloud platform (AWS/GCP/Azure) for production scalability 
