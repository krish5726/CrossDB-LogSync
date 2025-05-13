import csv
from pymongo import MongoClient
from datetime import datetime
import pytz

# Setup MongoDB client
client = MongoClient("mongodb://localhost:27017/")
# db = client["nosqlprojectdb"]
db=client["testdb"]
collection = db["student_course_grades"]

# IST timezone
ist = pytz.timezone("Asia/Kolkata")
custom_headers = ["studentid", "courseid","rollno", "emailid","grade"]
# Load CSV and add timestamp
with open("student_course_grades.csv") as f:
    reader = csv.DictReader(f, fieldnames=custom_headers)
    next(reader)
    data = []
    for row in reader:
        now_ist = datetime.now(ist)
        row["last_modified"] = now_ist  # This will include microseconds (6 digits)
        data.append(row)

# Insert into MongoDB
collection.insert_many(data)
