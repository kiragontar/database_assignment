import csv
import mysql.connector

# Function to split and clean released column values
def split_released(released_value):
    # Attempts to split a released value by commato covert to date and country) and handle potential exceptions.
    # Args:
    #     released_value (str): The value from the 'released' column in the CSV.
    # Returns:
    #     tuple: A tuple containing the cleaned released date (str) and
    #             released country (str) or None if the value is empty or invalid.
    try:
        # Remove leading/trailing whitespace and split on comma
        split_data = released_value.strip().split("(")
        released_date = split_data[0].strip()
        released_country = split_data[1].strip() if len(split_data) > 1 else None
        # Delete )
        if released_country != None:
            p_split = released_country.strip().split(")")
            released_country = p_split[0].strip()
        return released_date, released_country
    except (IndexError, ValueError):
        # Handle cases where the released value is empty or cannot be split
        return None, None

# Function to insert data into the 'movies_1nf' table
def insert_data(data):
    
    # Inserts a record into the 'movies_1nf' MySQL table.
    # Args:
    #     data (tuple): A tuple containing the data to be inserted into the table to the column order in the 'movies_1nf' table.
    # Returns:
    #     None

    # Database connection details
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Hhkn3481$",
        database="database_assignment_1nf")
    cursor = connection.cursor()

    insert_query = """
        INSERT INTO movies_1nf (name, rating, genre, year, released_date, released_country, score, votes, director, writer, star, country, budget, gross, company, runtime)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        cursor.execute(insert_query, data)
        connection.commit()
        print("Record inserted successfully")
    except mysql.connector.Error as err:
        print(f"Error inserting data: {err}")
    finally:
        # Ensure proper connection closure even if exceptions occur
        if connection.is_connected():
            connection.close()
        cursor.close()

# Open CSV file, process data, and insert into database
with open("movies.csv", "r", encoding="utf-8") as csvfile:
    reader = csv.reader(csvfile, skipinitialspace=True)
    next(reader)  # Skip header row

    for row in reader:
        name, rating, genre, year, released, score, votes, director, writer, star, country, budget, gross, company, runtime = row

        # Split and clean released value
        released_date, released_country = split_released(released)

        # Prepare data for insertion (handle None values for empty fields)
        data = (
            name,
            rating if rating else None,  # Convert rating if not empty
            genre,
            int(year) if year else None,  # Convert year to int if not empty
            released_date,
            released_country,
            float(score) if score else None,  # Convert score to float if not empty
            int(votes) if votes else None,  # Convert votes to int if not empty
            director,
            writer,
            star,
            country,
            int(budget) if budget else None,  # Convert budget to int if not empty
            int(gross) if gross else None,  # Convert gross to int if not empty
            company,
            int(runtime) if runtime else None,  # Convert runtime to int if not empty
        )

        insert_data(data)


print("finaly complete xdxd")

# current table:
# CREATE TABLE IF NOT EXISTS movies_1nf (
#   m_id INT AUTO_INCREMENT PRIMARY KEY,
#   name VARCHAR(255),
#   rating VARCHAR(10),
#   genre VARCHAR(50),
#   year INT,
#   released_date VARCHAR(50),
#   released_country VARCHAR(50),
#   score DECIMAL(2, 1),
#   votes INT,
#   director VARCHAR(255),
#   writer VARCHAR(255),
#   star VARCHAR(255),
#   country VARCHAR(50),
#   budget INT,
#   gross INT,
#   company VARCHAR(255),
#   runtime INT
# )
