import pandas as pd
from datasets import load_dataset

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
task1_df.to_csv('task1_pull_requests.csv', index=False)

# Ensure `final_df` exists in case later steps reference it (this file previously
# referenced `final_df` but it wasn't created). For now use Task 1 data as the base.
final_df = task1_df.copy()

# Task 2: Repository
all_repo = load_dataset("hao-li/AIDev", "all_repository")

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
final_df['SECURITY'] = final_df.apply(
    lambda row: check_security(row['TITLE'], row['BODYSTRING']), axis=1
)
