from mig import db_settings, ConnectDB
import csv
from datetime import datetime
import decimal

db_connection = ConnectDB()
db_connection.connect(db_settings)


def data_migration_expense(file_path: str, table_name: str):
    data = []
    with open(file_path, "r") as file:
        csv_reader = csv.DictReader(file)
        data = [row for row in csv_reader]
        for i in data:
            i.pop("Transaction id", None)
            i["created_at"] = datetime.strptime(i.pop("created_at"), "%d/%m/%Y")
            expense_amount_str = (
                i.get("expense_amount", "0").replace("₹", "").replace(",", "").strip()
            )
            expense_amount = decimal.Decimal(expense_amount_str)
            i["expense_amount"] = expense_amount
            property_name = i.pop("property_name", None)
            columns = ", ".join(i.keys())
            select_query = "SELECT id FROM property_property WHERE property_name = %s"
            selected_key = db_connection.cursor.execute(select_query, (property_name,))
            ids = db_connection.cursor.fetchone()
            i["property_id_id"] = ids[0] if ids else None
            values = i.values()

            if ids:
                placeholders = ", ".join(["%s"] * len(i))
                select_query = f"""INSERT INTO {table_name} ({columns},property_id_id) VALUES ({placeholders})"""
                db_connection.cursor.execute(select_query, list(values))

    print(f"Database values for {table_name} updated succesfully")
    return f"Database values for {table_name} updated succesfully"


file_path = "/home/rasif/Documents/exp1.csv"
table_name = "expense_expense"

data_migration_expense(file_path, table_name)

db_connection.connection.commit()
db_connection.close_connection()

print("SUCCESSFULLY DATABASE OPERATIONS COMPLETED")
