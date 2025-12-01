import pandas as pd
from datasets import load_dataset
import re

# Task 1: Pull Requests
all_pull_request = load_dataset("hao-li/AIDev", "all_pull_request")
available_keys = list(all_pull_request.keys()) if hasattr(all_pull_request, 'keys') else []
if 'all_pull_request' in available_keys:
    pr_data = all_pull_request['all_pull_request']
else:
    key = available_keys[0]
    pr_data = all_pull_request[key]
task1_df = pd.DataFrame({
    'TITLE': pr_data['title'],
    'ID': pr_data['id'],
    'AGENTNAME': pr_data['agent'],
    'BODYSTRING': pr_data['body'],
    'REPOID': pr_data['repo_id'],
    'REPOURL': pr_data['repo_url']
})
#task1_df.to_csv('task1_pull_requests.csv', index=False)

# Ensure `final_df` exists in case later steps reference it (this file previously
# referenced `final_df` but it wasn't created). For now use Task 1 data as the base.
final_df = task1_df.copy()

# Task 2: Repository
all_repo = load_dataset("hao-li/AIDev", "all_repository")
available_keys = list(all_repo.keys()) if hasattr(all_repo, 'keys') else []
if 'all_repository' in available_keys:
    repo_data = all_repo['all_repository']
else:
    key = available_keys[0]
    repo_data = all_repo[key]
task2_df = pd.DataFrame({
    'REPOID': repo_data['id'],
    'LANG': repo_data['language'],
    'STARS': repo_data['stars'],
    'REPOURL': repo_data['url']
})
#task2_df.to_csv('task2_repositories.csv', index=False)

# Task 3: PR Task Type
pr_task_type = load_dataset("hao-li/AIDev", "pr_task_type")
available_keys = list(pr_task_type.keys()) if hasattr(pr_task_type, 'keys') else []
if 'pr_task_type' in available_keys:
    pr_task_data = pr_task_type['pr_task_type']
else:
    key = available_keys[0]
    pr_task_data = pr_task_type[key]
task3_df = pd.DataFrame({
    'PRID': pr_task_data['id'],
    'PRTITLE': pr_task_data['title'],
    'PRREASON': pr_task_data['reason'],
    'PRTYPE': pr_task_data['type'],
    'CONFIDENCE': pr_task_data['confidence']
})
#task3_df.to_csv('task3_pr_task_type.csv', index=False)

# Task 4: PR Commmit Details
pr_commit_details = load_dataset("hao-li/AIDev", "pr_commit_details")
available_keys = list(pr_commit_details.keys()) if hasattr(pr_commit_details, 'keys') else []
if 'pr_commit_details' in available_keys:
    pr_commit_data = pr_commit_details['pr_commit_details']
else:
    key = available_keys[0]
    pr_commit_data = pr_commit_details[key]
def clean_patch_text(x: object) -> object:
    if not isinstance(x, str):
        return x
    x = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]", "", x)
    x = x.encode('utf-8', 'replace').decode('utf-8')
    return x
task4_df = pd.DataFrame({
    'PRID': list(pr_commit_data['pr_id']),
    'PRSHA': list(pr_commit_data['sha']),
    'PRCOMMITMESSAGE': list(pr_commit_data['message']),
    'PRFILE': list(pr_commit_data['filename']),
    'PRSTATUS': list(pr_commit_data['status']),
    'PRADDS': list(pr_commit_data['additions']),
    'PRDELSS': list(pr_commit_data['deletions']),
    'PRCHANGECOUNT': list(pr_commit_data['changes']),
    'PRDIFF': [clean_patch_text(x) for x in list(pr_commit_data['patch'])]
})
#task4_df.to_csv('task4_pr_commit_details.csv', index=False)

# Task 5: Security Analysis
security_keywords = ['race', 'racy', 'buffer', 'overflow', 'stack', 'integer', 'signedness', 
                     'underflow', 'improper', 'unauthenticated', 'gain access', 'permission', 
                     'cross site', 'css', 'xss', 'denial service', 'dos', 'crash', 'deadlock', 
                     'injection', 'request forgery', 'csrf', 'xsrf', 'forged', 'security', 
                     'vulnerability', 'vulnerable', 'exploit', 'attack', 'bypass', 'backdoor', 
                     'threat', 'expose', 'breach', 'violate', 'fatal', 'blacklist', 'overrun', 
                     'insecure']

def check_security(title, body):
    text = str(title).lower() + ' ' + str(body).lower()
    return 1 if any(keyword in text for keyword in security_keywords) else 0

# Merge data and apply security check
def merge_datasets(task1_df, task2_df, task3_df, task4_df):
    merged_df = pd.merge(
        task1_df,
        task3_df,
        left_on='ID',
        right_on='PRID',
        how='left'
    )
    
    merged_df = pd.merge(
        merged_df,
        task2_df,
        left_on='REPOID',
        right_on='REPOID',
        how='left',
        suffixes=('', '_repo')
    )
    
    task4_agg = task4_df.groupby('PRID').agg({
        'PRSHA': 'count',  # Count number of commits
        'PRADDS': 'sum',   # Total additions
        'PRDELSS': 'sum',  # Total deletions
        'PRCHANGECOUNT': 'sum',  # Total changes
        'PRFILE': 'count'  # Number of files changed
    }).reset_index()
    
    task4_agg.columns = [
        'PRID',
        'COMMIT_COUNT',
        'TOTAL_ADDITIONS',
        'TOTAL_DELETIONS',
        'TOTAL_CHANGES',
        'FILES_CHANGED'
    ]
    
    merged_df = pd.merge(
        merged_df,
        task4_agg,
        left_on='ID',
        right_on='PRID',
        how='left',
        suffixes=('', '_commit')
    )
    return merged_df

merged_df = merge_datasets(task1_df, task2_df, task3_df, task4_df)

merged_df['SECURITY'] = merged_df.apply(
    lambda row: check_security(
        row.get('TITLE', ''),
        row.get('BODYSTRING', '')
    ),
    axis=1
)

task5_df = pd.DataFrame({
    'ID': merged_df['ID'],
    'AGENT': merged_df['AGENTNAME'],
    'TYPE': merged_df['PRTYPE'],
    'CONFIDENCE': merged_df['CONFIDENCE'],
    'SECURITY': merged_df['SECURITY']
})
task5_df.to_csv('task5_security_analysis.csv', index=False)