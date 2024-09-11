import base64
import io
import os
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)

# Define folder path and file name for the CSV file
folder_path = 'C:/Users/fmdno/Dropbox/My Resume/Job Search/'
file_name = 'Job_search_log.csv'
today = datetime.today().strftime('%d-%m-%Y')

def date_format(date_list):
    # Convert 'Applied Date' to a standard format

    new_applied_date = []
    date_strings = date_list
    for date_string in date_strings:
        try:
            date_obj = datetime.strptime(date_string, '%d-%m-%Y')
        except ValueError:
            date_obj = datetime.strptime(date_string, '%d/%m/%Y')
        new_applied_date.append(date_obj.strftime('%d-%m-%Y'))

    return new_applied_date

if os.path.exists(os.path.join(folder_path, file_name)):
    # Read job logs from the CSV file
    df = pd.read_csv(os.path.join(folder_path, file_name))
    df['No.'] = df.index + 1  # Add a column for row numbers
    record_date = df['RecordDate'].tolist()
    df['RecordDate'] = date_format(record_date)
    applied_date = df['Applied Date'].tolist()
    df['Applied Date'] = date_format(applied_date)
    df2 = df[['No.', 'RecordDate', 'Applied Date', 'Company', 'Position Applied', 'Location', 'Status', 'Status Date', 'Application', 'Information', 'Interview Date']]
    df.to_csv(os.path.join(folder_path, file_name), index=False)

# Home page route
@app.route('/')
def menu():
    """Render the main menu page."""
    return render_template('menu.html')


# Route to add a new job log
@app.route('/add', methods=['GET', 'POST'])
def add_job():
    """Handle job log addition."""
    if request.method == 'POST':
        # Extract form data from the POST request
        company = request.form['company']
        position = request.form['position']
        location = request.form['location']
        date_applied = request.form['date_applied']
        status = request.form['status']
        application_method = request.form['application_method']

        # Determine information based on the application method
        if application_method == 'online':
            information = request.form['website']
        elif application_method == 'walk-in':
            information = request.form['contact']
        else:
            information = request.form['info']

        # Create a DataFrame for the new job log entry
        df = pd.DataFrame({
            'RecordDate': [today],
            'Applied Date': [date_applied],
            'Company': [company],
            'Position Applied': [position],
            'Location': [location],
            'Status': [status],
            'Status Date': [datetime.now().strftime("%d-%m-%Y %H:%M")],
            'Application': [application_method],
            'Information': [information]
        })

        # Append the new entry to the existing CSV file or create it if it doesn't exist
        if not os.path.exists(os.path.join(folder_path, file_name)):
            df.to_csv(os.path.join(folder_path, file_name), index=False)
        else:
            df.to_csv(os.path.join(folder_path, file_name), mode='a', header=False, index=False)

        # Redirect to the main menu page
        return redirect(url_for('menu'))

    # Render the job log form for GET requests
    return render_template('job_form.html')


# Route to view all job logs
@app.route('/view')
def view_jobs():
    """Render a page showing all job logs."""
    if os.path.exists(os.path.join(folder_path, file_name)):
        df = pd.read_csv(os.path.join(folder_path, file_name))
        jobs = df.to_dict(orient='records')  # Convert DataFrame to list of dictionaries for rendering
    else:
        jobs = []

    # Render the view_jobs.html template with job data
    return render_template('view_jobs.html', jobs=jobs)


# Route to edit a specific job log entry
@app.route('/edit/<int:index>', methods=['GET', 'POST'])
def edit_job(index):
    """Handle editing of a specific job log entry."""
    if os.path.exists(os.path.join(folder_path, file_name)):
        df = pd.read_csv(os.path.join(folder_path, file_name))

        # Handle form submission for editing a job log
        if request.method == 'POST':
            # Update the job log entry with the new data from the form
            df.at[index, 'Company'] = request.form['company']
            df.at[index, 'Position Applied'] = request.form['position']
            df.at[index, 'Location'] = request.form['location']
            df.at[index, 'Applied Date'] = request.form['date_applied']
            df.at[index, 'Status'] = request.form['status']
            df.at[index, 'Application'] = request.form['application_method']
            df.at[index, 'Information'] = request.form['info']
            df.at[index, 'Status Date'] = datetime.now().strftime("%d-%m-%Y %H:%M")
            df.at[index, 'Interview Date'] = request.form['interview_date']

            # Save the updated DataFrame back to the CSV file
            df.to_csv(os.path.join(folder_path, file_name), index=False)
            return redirect(url_for('view_jobs'))

        # Get the job details for the specified index and render the edit form
        job = df.iloc[index].to_dict()
        return render_template('edit_job.html', job=job, index=index)

    return redirect(url_for('view_jobs'))


@app.route('/stat')
def stat():
    if os.path.exists(os.path.join(folder_path, file_name)):
        jobs = pd.read_csv(os.path.join(folder_path, file_name))

        # Convert 'Applied Date' to datetime with correct format
        applied_date = jobs['Applied Date'].tolist()
        jobs['Applied Date'] = date_format(applied_date)
        jobs['Month'] = pd.to_datetime(jobs['Applied Date'], format='%d-%m-%Y').dt.month_name()

        # Count jobs by month
        monthly_jobs = jobs['Month'].value_counts().sort_index()

        # Count jobs by status
        status_counts = jobs['Status'].value_counts()

        # Count jobs by days
        daily_jobs = jobs.groupby('Applied Date').size().reset_index(name='Job Count')
        daily_jobs['Applied Date'] = pd.to_datetime(daily_jobs['Applied Date'], format='%d-%m-%Y')
        daily_jobs = daily_jobs.sort_values(by='Applied Date', ascending=True)
        daily_jobs.reset_index(drop=True, inplace=True)

        # Calculate number of days from start_date to today
        start_date = daily_jobs['Applied Date'].iloc[0]
        num_of_days = (datetime.today() - start_date).days

        # Create a DataFrame with a date range and initialize counts
        date_list = [start_date + timedelta(days=x) for x in range(num_of_days + 1)]
        df2_date = pd.DataFrame(date_list, columns=['Date'])
        df2_date.set_index('Date', inplace=True)

        # Merge with daily_jobs to fill in counts
        df2_date = df2_date.join(daily_jobs.set_index('Applied Date'), how='left').reset_index()

        # Fill NaN values with 0 for days with no jobs
        df2_date['Job Count'].fillna(0, inplace=True)

        # Create plots
        fig, axs = plt.subplots(3, 1, figsize=(15, 20))

        # Number of jobs applied by month plot
        monthly_jobs.plot(kind='bar', ax=axs[0], color='skyblue')
        axs[0].set_title('Number of Jobs Applied by Month')
        axs[0].set_xlabel('Month')
        axs[0].set_ylabel('Number of Jobs')
        axs[0].tick_params(axis='x', rotation=45)

        # Add table values to the monthly jobs bar chart
        for i, value in enumerate(monthly_jobs):
            axs[0].text(i, value + 0.5, str(value), ha='center', va='bottom')

        # Number of jobs by day plot
        df2_date.plot(kind='line', x='Date', y='Job Count', ax=axs[1], color='lightcoral')
        axs[1].set_title('Number of Jobs by Day')
        axs[1].set_xlabel('Date')
        axs[1].set_ylabel('Job Count')

        # Add table values to the daily jobs line chart
        for i, value in df2_date.iterrows():
            axs[1].text(value['Date'], value['Job Count'] + 0.5, str(int(value['Job Count'])), ha='center', va='bottom')

        # Number of jobs by status plot
        status_counts.plot(kind='bar', ax=axs[2], color='lightcoral')
        axs[2].set_title('Number of Jobs by Status')
        axs[2].set_xlabel('Job Status Category')
        axs[2].set_ylabel('Number of Jobs')
        axs[2].tick_params(axis='x', rotation=0)

        # Add table values to the status bar chart
        for i, value in enumerate(status_counts):
            axs[2].text(i, value + 0.5, str(value), ha='center', va='bottom')



        # Save plot to a BytesIO object
        img = io.BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)
        img_base64 = base64.b64encode(img.getvalue()).decode('utf-8')

        plt.close(fig)  # Close the figure to avoid memory leaks

        return render_template('stat_jobs.html', jobs=jobs, img_data=img_base64)
    else:
        return render_template('stat_jobs.html', jobs=[], img_data=None)


if __name__ == '__main__':
    app.run(debug=True)
