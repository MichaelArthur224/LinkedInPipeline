# 👔 Automated LinkedIn Job Alert Pipeline

An automated data pipeline that daily scrapes LinkedIn for targeted job listings, processes the results into a clean HTML format, and emails a beautifully styled summary table directly to your inbox. 

Built using **Apache Airflow**, **JobSpy**, and **Pandas**.

---

## 🚀 Features

* **Targeted Scraping:** Pulls real-time LinkedIn job listings matching specific roles and locations using `jobspy`.
* **Duplicate & Noise Filtering:** Targets only postings from the last 24 hours to prevent recurring noise.
* **Styled HTML Email Delivery:** Generates a custom CSS-styled email digest featuring an interactive data table with direct application links.
* **Dual Execution Flexibility:** Engineered to run seamlessly as an automated orchestration graph in **Apache Airflow** or standalone via local python execution.

---

## 🛠️ Tech Stack

* **Orchestration:** Apache Airflow
* **Data Manipulation:** Pandas
* **Web Scraping:** JobSpy (a robust, unified scraping library for job boards)
* **Communication Protocol:** SMTP / Email Operators

---
