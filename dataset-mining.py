import pandas as pd
from datasets import load_dataset

# Load dataset
dataset = load_dataset("dataset_name_here")

# Task 1: Pull Requests
pr_data = dataset['all_pull_request']
task1_df = pd.DataFrame({
    'TITLE': pr_data['title'],
    'ID': pr_data['id'],
    'AGENTNAME': pr_data['agent'],
    'BODYSTRING': pr_data['body'],
    'REPOID': pr_data['repo_id'],
    'REPOURL': pr_data['repo_url']
})
task1_df.to_csv('task1_pull_requests.csv', index=False)

# Repeat similar pattern for Tasks 2-4

# Task 5: Security Analysis
security_keywords = ['vulnerability', 'CVE', 'security', 'exploit', ...]  # Add full list

def check_security(title, body):
    text = str(title).lower() + ' ' + str(body).lower()
    return 1 if any(keyword in text for keyword in security_keywords) else 0

# Merge data and apply security check
final_df['SECURITY'] = final_df.apply(
    lambda row: check_security(row['TITLE'], row['BODYSTRING']), axis=1
)
