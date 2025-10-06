# Predictive Maintenance – Intelligent Data & ML Automation

### Real-world simulation of how industrial machines can predict failures before they happen.

---

## Project Summary
This project demonstrates how sensor data from factory machines can be transformed into intelligent predictions using a fully automated data and ML workflow.  
It combines **data engineering, machine learning, and analytics automation** to deliver actionable machine-health insights — the kind of workflow used in real-world predictive maintenance systems.

---

## 💡 What I Built
- Designed an **automated data pipeline** that collects continuous sensor readings, cleans and enriches them, and generates fresh predictions with every batch.  
- Used **PySpark with Delta Lake** to handle large data volumes efficiently and ensure versioned, reliable storage.  
- Deployed an **XGBoost model** that calculates failure probabilities for each machine based on temperature, torque, and speed features.  
- Orchestrated the entire workflow with **Apache Airflow**, turning raw data into scored insights with a single click.  
- Built a **Power BI dashboard** that visualizes machine-failure risk in near-real-time, making the results accessible to both technical and business users.

---

## ⚙️ Tools & Technologies
| Category | Stack |
|-----------|--------|
| Data Engineering | Python · PySpark · Delta Lake · Apache Airflow |
| Machine Learning | XGBoost · pandas · joblib |
| Automation | Docker Compose · Kafka (for simulated streaming) |
| Analytics & Visualization | Power BI |
| Environment | macOS · Dockerized setup for reproducibility |

---

## Outcome
- Reduced manual effort: once deployed, the pipeline ingests, processes, scores, and exports data **automatically**.  
- Improved visibility: Power BI dashboard provides instant insight into **which machines are most likely to fail next**.  
- Demonstrated the full lifecycle of a modern **data-to-insight system**, from raw data to predictive analytics.

---

## 🧭 Key Learning Takeaways
- Learned how to **combine data engineering and ML orchestration** to solve an applied industry problem.  
- Gained hands-on experience with **production-style tools** used by data teams (Airflow, Delta Lake, Kafka).  
- Understood how to turn predictive models into **operational, continuously updated pipelines**.  
- Practiced designing for **reliability, automation, and scalability** even on a local environment.

---

## 🖥️ Visualization Preview
The Power BI dashboard summarizes:
- Machine-wise failure probability (`failure_proba`)
- Failure alerts (`failure_flag`)
- Temperature & torque correlation with predicted risk
- Trend of predicted failures over time

---

- ## 📊 Dataset
- **Source**: [Predictive Maintenance Dataset (AI4I 2020)](https://www.kaggle.com/datasets/stephanmatzka/predictive-maintenance-dataset-ai4i-2020)  
- **Size**: 10,000 rows, 14 features  
- **Target Variable**: `machine_failure` (binary classification)  
- **Features include**: air/process temperature, rotational speed, torque, tool wear, and multiple failure modes.  

**Reference**:  
S. Matzka, *Explainable Artificial Intelligence for Predictive Maintenance Applications,* 2020 Third International Conference on Artificial Intelligence for Industries (AI4I), pp. 69–74, doi: [10.1109/AI4I49448.2020.00023](https://doi.org/10.1109/AI4I49448.2020.00023).
---
