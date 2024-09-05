try:
    import pandas as pd
    print("Pandas version:", pd.__version__)
except ModuleNotFoundError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas"])
    import pandas as pd
    print("Pandas successfully installed. Version:", pd.__version__)

import requests
import pandas as pd
import json

# get access token

url = 'https://accounts.zoho.com/oauth/v2/token'

refresh_obj2 = {"refresh_token": "1000.61404ac544b6533fc0b37bbeb1c6a98f.3aeba0a9c28c45834e55fc12417e3947",
"grant_type": "refresh_token",
"client_id": "1000.5HC65GEQP3DKVKU00ZIXCALIGY3RIT",
"client_secret": "eb1fb31dc03c1e6ac53f1d4df4074decf91dc17466",
"redirect_uri": "https://www.zoho.com/",
"scope": "SDPOnDemand.projects.ALL"}

x = requests.post(url, data=refresh_obj2)

access_token = x.json()['access_token']


#####################
def get_projects(start=1):
    # get projects list
    get_url = 'https://sdpondemand.manageengine.com/api/v3/projects/'
    headers = {'authorization': 'Zoho-oauthtoken ' + access_token,
    'accept': 'application/vnd.manageengine.v3+json',
    'Content-Type': 'application/x-www-form-urlencoded'}
    listInfo = f'''
    {{"list_info":{{"start_index":{start},"row_count":100,"sort_fields":[{{"field":"display_id","order":"desc"}}]}}}}
    '''
    getParams = {'input_data':listInfo}
    response = requests.get(get_url,headers = headers,params = getParams)

    s=str(response.content,'utf-8')

    y = json.loads(s)
    df = pd.DataFrame.from_dict(pd.json_normalize(y['projects']), orient='columns')
    return df

projects = pd.DataFrame()
for j in range(5):
    projects_int = get_projects(j*100+1)
    projects = pd.concat([projects, projects_int])
projects = projects[(projects['template.name'] != 'Default Project Template') & (projects['template.name'] != 'GBT - Project Management Application')]


#######################################
# get tasks
# project_id = 126573000016835941
def get_tasks(project_id, start=1):
    get_url = f'https://sdpondemand.manageengine.com/api/v3/projects/{project_id}/tasks'
    headers = {'authorization': 'Zoho-oauthtoken ' + access_token,
    'accept': 'application/vnd.manageengine.v3+json',
    'Content-Type': 'application/x-www-form-urlencoded'}
    listInfo = f'''
    {{"list_info":{{"start_index":{start},"row_count":100,"sort_fields":[{{"field":"display_id","order":"desc"}}]}}}}
    '''
    getParams = {'input_data':listInfo}
    response = requests.get(get_url,headers = headers,params = getParams)

    ss = str(response.content, 'utf-8')
    yy = json.loads(ss)
    dfs = pd.DataFrame.from_dict(pd.json_normalize(yy['tasks']), orient='columns')

    return dfs


tasks = pd.DataFrame()

for idd in projects['id']:
    for i in range(4):
        tasks_list = [get_tasks(idd, i*100+1)]
        tasks_list_df = pd.concat(tasks_list)
        tasks = pd.concat([tasks, tasks_list_df])
