import gspread
import pymysql
from oauth2client.service_account import ServiceAccountCredentials

# Set up your Google Sheets credentials and open the sheet
scope = ["https://spreadsheets.google.com"]
credentials = ServiceAccountCredentials.from_json_keyfile_name('cred.json', scope)
gc = gspread.authorize(credentials)
worksheet = gc.open("Weekly consultations data").worksheet("Automation")

# Find the last row with data in column B
existing_data = worksheet.col_values(2)
last_row_with_data = len(existing_data)

# Connect to the MySQL database
db = pymysql.connect(host="", user="", password="", db="")
cursor = db.cursor()

# Set your start and end date here
start_date = '2023-11-20 00:00:00'
end_date = '2023-11-26 23:59:59'

# Define your SQL queries
queries = [
    "SELECT count(DISTINCT v.id) \
    FROM visits v \
    LEFT JOIN doctors d ON v.doctor_id = d.id \
    LEFT JOIN users u ON d.user_id = u.id \
    WHERE 1 = 1 \
    AND v.status = 'visited' \
    AND u.is_test = 0 \
    AND v.fee > 0 \
    AND v.created_at BETWEEN %s AND %s;",

    "SELECT count(DISTINCT v.id) \
    FROM visits v \
    LEFT JOIN doctors d ON v.doctor_id = d.id \
    LEFT JOIN users u ON d.user_id = u.id \
    WHERE 1 = 1 \
    AND v.status = 'visited' \
    AND u.is_test = 0 \
    AND v.fee > 0 \
    AND v.doctor_id IN (SELECT doctor_id FROM doctor_specialities ds WHERE ds.proof_type IN \
                        ('degree_certificate', 'diploma_certificate', 'temporary') AND ds.status = 'approved') \
    AND v.created_at BETWEEN %s AND %s;",

    "SELECT count(DISTINCT v.id) \
    FROM visits v \
    LEFT JOIN doctors d ON v.doctor_id = d.id \
    LEFT JOIN users u ON d.user_id = u.id \
    WHERE 1 = 1 \
    AND v.status = 'visited' \
    AND u.is_test = 0 \
    AND v.fee > 0 \
    AND v.is_special_fee != 1 \
    AND v.doctor_id IN (SELECT doctor_id FROM doctor_specialities ds WHERE ds.proof_type IN \
                        ('degree_certificate', 'diploma_certificate', 'temporary') AND ds.status = 'approved') \
    AND v.created_at BETWEEN %s AND %s;",

    "SELECT count(DISTINCT v.id) \
    FROM visits v \
    LEFT JOIN doctors d ON v.doctor_id = d.id \
    LEFT JOIN users u ON d.user_id = u.id \
    WHERE 1 = 1 \
    AND v.status = 'visited' \
    AND u.is_test = 0 \
    AND v.fee > 0 \
    AND v.is_special_fee = 1 \
    AND v.doctor_id IN (SELECT doctor_id FROM doctor_specialities ds WHERE ds.proof_type IN \
                        ('degree_certificate', 'diploma_certificate', 'temporary') AND ds.status = 'approved') \
    AND v.created_at BETWEEN %s AND %s;",

    "SELECT count(DISTINCT v.id) \
    FROM visits v \
    LEFT JOIN doctors d ON v.doctor_id = d.id \
    LEFT JOIN users u ON d.user_id = u.id \
    WHERE 1 = 1 \
    AND v.status = 'visited' \
    AND u.is_test = 0 \
    AND v.fee > 0 \
    AND v.doctor_id NOT IN (SELECT doctor_id FROM doctor_specialities ds WHERE ds.proof_type IN \
                            ('degree_certificate', 'diploma_certificate', 'temporary') AND ds.status = 'approved') \
    AND v.created_at BETWEEN %s AND %s;",

    "SELECT count(DISTINCT v.id) \
    FROM visits v \
    LEFT JOIN doctors d ON v.doctor_id = d.id \
    LEFT JOIN users u ON d.user_id = u.id \
    WHERE 1 = 1 \
    AND v.status = 'visited' \
    AND u.is_test = 0 \
    AND v.fee > 0 \
    AND v.is_special_fee != 1 \
    AND v.doctor_id NOT IN (SELECT doctor_id FROM doctor_specialities ds WHERE ds.proof_type IN \
                            ('degree_certificate', 'diploma_certificate', 'temporary') AND ds.status = 'approved') \
    AND v.created_at BETWEEN %s AND %s;",

    "SELECT count(DISTINCT v.id) \
    FROM visits v \
    LEFT JOIN doctors d ON v.doctor_id = d.id \
    LEFT JOIN users u ON d.user_id = u.id \
    WHERE 1 = 1 \
    AND v.status = 'visited' \
    AND u.is_test = 0 \
    AND v.fee > 0 \
    AND v.is_special_fee = 1 \
    AND v.doctor_id NOT IN (SELECT doctor_id FROM doctor_specialities ds WHERE ds.proof_type IN \
                            ('degree_certificate', 'diploma_certificate', 'temporary') AND ds.status = 'approved') \
    AND v.created_at BETWEEN %s AND %s;"
]

# Iterate through queries and populate Google Sheet starting from the last row with data
for i, query in enumerate(queries):
    cursor.execute(query, (start_date, end_date))
    result = cursor.fetchone()
    count = result[0] if result else 0
    # Update the corresponding cell in the sheet, starting from the last row with data
    worksheet.update_cell(last_row_with_data + 1, i + 2, count)

# Close the database connection
cursor.close()
db.close()
