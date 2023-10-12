from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from github import Github

# Define a function to run your "Daily Scrape.py" script
def run_daily_scrape():
    # Replace this with the logic to run your "Daily Scrape.py" script
    print("Running Daily Scrape.py")

# Define a function to upload CSV files to GitHub
def upload_csv_to_github():
    # GitHub credentials and repository information
    username = "kjchrz03"
    access_token = "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"
    repo_name = "hockey-streamlit"
    repo_owner = "kjchrz03"

    files_to_upload = [
        {
            "file_path": "C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\goal_counts.csv",
            "file_name": "goal_counts.csv",
        },
        {
            "file_path": "C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\goal_locations.csv",
            "file_name": "goal_locations.csv",
        },
        {
            "file_path": "C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\ice_map_data.csv",
            "file_name": "ice_map_data.csv",
        }
    ]

    commit_message = "Data updates"

    # Initialize a GitHub instance
    g = Github(username, access_token)

    # Get the repository
    repo = g.get_user(repo_owner).get_repo(repo_name)

    for file_info in files_to_upload:
        file_path = file_info["file_path"]
        file_name = file_info["file_name"]

        # Read the content of the CSV file
        with open(file_path, 'r') as file:
            content = file.read()

        # Create a new file in the repository
        try:
            repo.create_file(file_name, commit_message, content)
            print(f"CSV file '{file_name}' uploaded to GitHub successfully.")
        except Exception as e:
            print(f"Error uploading the file '{file_name}': {str(e)}")

# Define the DAG configuration
with DAG(
    dag_id="my_daily_scrape_dag",  # Unique DAG ID
    schedule_interval="0 8 * * *",  # Run at 8:00 AM daily
    start_date=datetime(2023, 10, 12),  # Start date
    catchup=False  # Do not catch up on missed runs
) as dag:

    # Create a PythonOperator to run the "Daily Scrape.py" script
    run_script_task = PythonOperator(
        task_id="run_daily_scrape_task",
        python_callable=run_daily_scrape,
    )

    # Create another PythonOperator to upload CSV files to GitHub
    upload_to_github_task = PythonOperator(
        task_id="upload_to_github_task",
        python_callable=upload_csv_to_github,
    )

    # Set up task dependencies
    run_script_task >> upload_to_github_task
