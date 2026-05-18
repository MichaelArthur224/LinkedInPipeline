# Import Packages
import os
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
from jobspy import scrape_jobs
from airflow.sdk import dag, task
from airflow.providers.smtp.operators.smtp import EmailOperator

# Set Credentials
os.environ["AIRFLOW__SMTP__SMTP_HOST"] = SMTP_HOST
os.environ["AIRFLOW__SMTP__SMTP_PORT"] = SMTP_PORT
os.environ["AIRFLOW__SMTP__SMTP_USER"] = SMTP_USER
os.environ["AIRFLOW__SMTP__SMTP_PASSWORD"] = SMTP_PASSWORD
os.environ["AIRFLOW__SMTP__SMTP_MAIL_FROM"] = SMTP_MAIL_FROM

# Set Search Configs
SEARCH_TERM = "Data Engineer"
LOCATION = "Fullerton, CA"
EMAIL_RECIPIENT = SMTP_USER

# Define Args
default_args = {
    'owner': 'michael',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define Dag
@dag(
    dag_id='linkedin_job_alert_pipeline',
    default_args=default_args,
    description='Scrapes daily LinkedIn job postings and emails a report',
    schedule='0 8 * * *',  # Runs every day at 8:00 AM
    catchup=False,
    tags=['job_search', 'automation']
)
def job_alert_dag():
    @task(task_id='fetch_and_filter_jobs')
    def fetch_jobs() -> str:
        """Scrapes LinkedIn for new job postings and converts them to HTML."""
        try:
            print(f"Scraping LinkedIn for '{SEARCH_TERM}' in '{LOCATION}'...")
            jobs = scrape_jobs(
                site_name=["linkedin"],
                search_term=SEARCH_TERM,
                location=LOCATION,
                results_wanted=15,
                hours_old=24,
                country_inference="search_url"
            )
        except Exception as e:
            return f"<p>Error fetching jobs from LinkedIn: {str(e)}</p>"

        if jobs.empty:
            return f"<h3>No new jobs found in the last 24 hours for {SEARCH_TERM} in {LOCATION}.</h3>"

        print(f"Found {len(jobs)} jobs!")

        relevant_jobs = jobs[['title', 'company', 'location', 'job_url']].copy()
        relevant_jobs['Application Link'] = relevant_jobs['job_url'].apply(
            lambda url: f'<a href="{url}" target="_blank">Apply Here</a>'
        )
        relevant_jobs.drop(columns=['job_url'], inplace=True)
        relevant_jobs.columns = ['Job Title', 'Company', 'Location', 'Application Link']

        html_table = relevant_jobs.to_html(index=False, escape=False, classes='job-table')

        email_style = """
        <style>
            .job-table { border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; }
            .job-table td, .job-table th { border: 1px solid #ddd; padding: 12px; }
            .job-table tr:nth-child(even){ background-color: #f2f2f2; }
            .job-table th { padding-top: 12px; padding-bottom: 12px; text-align: left; background-color: #0077B5; color: white; }
            a { color: #0077B5; text-decoration: none; font-weight: bold; }
        </style>
        """

        email_body = f"""
        {email_style}
        <h2>Daily LinkedIn Job Report</h2>
        <p>Here are the new <strong>{SEARCH_TERM}</strong> postings in <strong>{LOCATION}</strong> from the past 24 hours:</p>
        <br>
        {html_table}
        """
        return email_body

    job_html_content = fetch_jobs()

    send_email = EmailOperator(
        task_id='send_email_digest',
        to=EMAIL_RECIPIENT,
        subject=f"Daily Job Alerts: {SEARCH_TERM} in {LOCATION}",
        html_content=job_html_content,
    )

    job_html_content >> send_email

# Start Dag
my_dag = job_alert_dag()

# Execute
if __name__ == "__main__":

    # Run Scraping Task
    html_content = my_dag.get_task('fetch_and_filter_jobs').python_callable()

    # Send email
    print("\nSending Email....")

    msg = MIMEMultipart()
    msg['From'] = os.environ["AIRFLOW__SMTP__SMTP_MAIL_FROM"]
    msg['To'] = EMAIL_RECIPIENT
    msg['Subject'] = f"Daily Job Alerts: {SEARCH_TERM} in {LOCATION} - {datetime.now().strftime('%b %d')}"
    msg.attach(MIMEText(html_content, 'html'))

    try:
        # Connect to Gmail and send
        server = smtplib.SMTP(os.environ["AIRFLOW__SMTP__SMTP_HOST"], int(os.environ["AIRFLOW__SMTP__SMTP_PORT"]))
        server.starttls()
        server.login(os.environ["AIRFLOW__SMTP__SMTP_USER"], os.environ["AIRFLOW__SMTP__SMTP_PASSWORD"])
        server.send_message(msg)
        server.quit()
        print("Pipeline Success Check your inbox.")
    except Exception as e:
        print(f"Pipeline Failed. Error: {e}")