from mig import ConnectDB,db_settings
import csv


db_connection = ConnectDB()
db_connection.connect(db_settings)


def data_migration(file_path: str, table_name: str):
    data = []
    with open(file_path, "r") as file:
        csv_reader = csv.DictReader(file)
        data = [row for row in csv_reader]
        for i in data:
            columns = ", ".join(i.keys())
            print(columns,"ppppp")
            placeholders = ", ".join(["%s"] * len(i))
            values = i.values()
            query = f"""INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"""
            db_connection.cursor.execute(query, list(values))
    print(f"Database values for {table_name} updated succesfully")
    return f"Database values for {table_name} updated succesfully"


file_path = "/home/rasif/Documents/property.csv"
table_name = "property_property"
data_migration(
    file_path,
    table_name
)

db_connection.connection.commit()
db_connection.close_connection()

print("SUCCESSFULLY DATABASE OPERATIONS COMPLETED")
