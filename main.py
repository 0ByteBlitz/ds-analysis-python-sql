import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, func, select

from db_connection import connect_db

# Load the environment variables
load_dotenv()

# Get the environment variables
dbname = os.getenv('DB_NAME')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')

# Connect to the database
connection = connect_db(dbname=dbname, user=user, password=password)

# Read the CSV file into DataFrame
df = pd.read_csv('students.csv')

# Write the DataFrame to the database
df.to_sql('datacamp_students_mental_health',
          connection, if_exists='replace', index=False)

# Get the metadata of the database
metadata = MetaData()

# Reflect the table from the database
students_table = Table('datacamp_students_mental_health',
                       metadata, autoload_with=connection)

# Connect to the database
with connection.connect() as con:
    # Analysis 1: How the length of stay impacts the average mental health diagnosis score of international students present in the study?
    query_analysis_1 = (
        select(
            students_table.c.stay,
            func.count(students_table.c.inter_dom).label('count_int'),
            func.avg(students_table.c.todep).label('average_phq'),
            func.avg(students_table.c.tosc).label('average_scs'),
            func.avg(students_table.c.toas).label('average_as')
        )
        .where(students_table.c.inter_dom == 'Inter')
        .group_by(students_table.c.stay)
        .order_by(students_table.c.stay.desc())
        .limit(9)
    )

    # Execute the query and fetch the results
    results_analysis_1 = con.execute(query_analysis_1).fetchall()

    # Create a DataFrame from the results
    df_analysis_1 = pd.DataFrame(
        results_analysis_1, columns=['stay', 'count_int', 'average_phq', 'average_scs', 'average_as'])

    # Round the values in the DataFrame
    df_analysis_1['average_phq'] = df_analysis_1['average_phq'].round(2)
    df_analysis_1['average_scs'] = df_analysis_1['average_scs'].round(2)
    df_analysis_1['average_as'] = df_analysis_1['average_as'].round(2)

    # Print the results
    print(df_analysis_1)

    # Close the connection
    con.close()
