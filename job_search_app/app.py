from flask import Flask, render_template, request, redirect, url_for
import pandas as pd
import os
from datetime import datetime

app = Flask(__name__)

folder_path = 'C:/Users/fmdno/Dropbox/My Resume/Job Search/'
file_name = 'Job_search_log.csv'
today = datetime.today().strftime("%d/%m/%Y")


# Home page (menu)
@app.route('/')
def menu():
    return render_template('menu.html')


# Route to add a new job log
@app.route('/add', methods=['GET', 'POST'])
def add_job():
    if request.method == 'POST':
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

        # Append data to the CSV file
        if not os.path.exists(os.path.join(folder_path, file_name)):
            df.to_csv(os.path.join(folder_path, file_name), index=False)
        else:
            df.to_csv(os.path.join(folder_path, file_name), mode='a', header=False, index=False)

        return redirect(url_for('menu'))

    return render_template('job_form.html')


# Route to view all job logs
@app.route('/view')
def view_jobs():
    if os.path.exists(os.path.join(folder_path, file_name)):
        df = pd.read_csv(os.path.join(folder_path, file_name))
        jobs = df.to_dict(orient='records')  # Convert the DataFrame to a list of dictionaries for easier rendering
    else:
        jobs = []
    return render_template('view_jobs.html', jobs=jobs)


# Route to edit a job log
@app.route('/edit/<int:index>', methods=['GET', 'POST'])
def edit_job(index):
    if os.path.exists(os.path.join(folder_path, file_name)):
        df = pd.read_csv(os.path.join(folder_path, file_name))

        # Handle the form submission for editing a job log
        if request.method == 'POST':
            df.at[index, 'Company'] = request.form['company']
            df.at[index, 'Position Applied'] = request.form['position']
            df.at[index, 'Location'] = request.form['location']
            df.at[index, 'Applied Date'] = request.form['date_applied']
            df.at[index, 'Status'] = request.form['status']
            df.at[index, 'Application'] = request.form['application_method']
            df.at[index, 'Information'] = request.form['info']
            df.at[index, 'Status Date'] = datetime.now().strftime("%d/%m/%Y %H:%M")

            # Save the updated DataFrame back to CSV
            df.to_csv(os.path.join(folder_path, file_name), index=False)
            return redirect(url_for('view_jobs'))

        job = df.iloc[index].to_dict()  # Get the job details for the selected index
        return render_template('edit_job.html', job=job, index=index)

    return redirect(url_for('view_jobs'))


if __name__ == '__main__':
    app.run(debug=True)
