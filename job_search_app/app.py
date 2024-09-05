from flask import Flask, render_template, request, redirect
import pandas as pd
import os
from datetime import datetime

app = Flask(__name__)

folder_path = 'C:/Users/fmdno/Dropbox/My Resume/Job Search/'
file_name = 'Job_search_log.csv'
today = datetime.today().strftime("%d/%m/%Y")


@app.route('/')
def job_form():
    return render_template('job_form.html')


@app.route('/submit', methods=['POST'])
def submit_job():
    # Get form data from HTML input
    company = request.form['company']
    position = request.form['position']
    location = request.form['location']
    date_applied = request.form['date_applied']
    status = request.form['status']
    application_method = request.form['application_method']

    if application_method == 'online':
        information = request.form['website']
    elif application_method == 'walk-in':
        information = request.form['contact']
    else:
        information = request.form['info']

    # Create a DataFrame for the new data
    df = pd.DataFrame({
        'Date': [today],
        'Applied Date': [date_applied],
        'Company': [company],
        'Position Applied': [position],
        'Location': [location],
        'Status': [status],
        'Status Date': [datetime.now().strftime("%d/%m/%Y %H:%M")],
        'Application': [application_method],
        'Information': [information]
    })

    # Check if file exists, then append to CSV
    if not os.path.exists(os.path.join(folder_path, file_name)):
        df.to_csv(os.path.join(folder_path, file_name), index=False)
    else:
        df.to_csv(os.path.join(folder_path, file_name), mode='a', header=False, index=False)

    return redirect('/')


if __name__ == '__main__':
    app.run(debug=True)
