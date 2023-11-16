import time as time_module
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pandas as pd
import pymongo
import numpy as np

class MyHandler(FileSystemEventHandler):
    def __init__(self, db_collection):
        self.db_collection = db_collection

    def convert_to_valid_dates(self, row):
        for key, value in row.items():
            if pd.isna(value):
                row[key] = None
            elif isinstance(value, str):
                try:
                    row[key] = pd.to_datetime(value)
                except (ValueError, pd.errors.ParserError):
                    row[key] = None

    def clean_dataframe(self, df):
        # Filter out columns with the name "Unnamed"
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')].copy()

        # Fill in null values appropriately
        for column in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[column]):
                df.loc[:, column] = df[column].fillna(pd.NaT)
            else:
                df.loc[:, column] = df[column].fillna(np.nan)

        # Convert the dates to strings
        df = df.astype({col: 'str' for col in df.select_dtypes('datetime').columns})

        # Apply the convert_to_valid_dates function to each row
        df.apply(self.convert_to_valid_dates, axis=1)

        return df

    def on_created(self, event):
        print("Archivo detectado")
        if not event.is_directory:
            file_path = os.path.abspath(event.src_path)
            print(f"Nuevo archivo detectado: {file_path}")

            # Read the Excel file into a DataFrame and handle null values
            df = pd.read_excel(file_path, na_values=["", "NULL", "-", "Null"])

            # Clean and preprocess the DataFrame
            df = self.clean_dataframe(df)

            # Convert the DataFrame to a list of dictionaries
            data = df.to_dict(orient="records")

            # Insert or update the data in the MongoDB collection based on the "#" column
            for row in data:
                try:
                    filter_criteria = {"#": row["#"]}
                    update_data = {"$set": row}
                    self.db_collection.update_one(filter_criteria, update_data, upsert=True)
                except pymongo.errors.InvalidDocument:
                    print(f"Error al insertar el documento con '#' '{row['#']}'. Formato de datos no válido.")

            print(f"Datos insertados/actualizados en MongoDB (manejando errores y evitando duplicados).")


if __name__ == "__main__":
    # MongoDB configuration
    client = pymongo.MongoClient("localhost", 27017)
    db = client["excelBD"]
    collection = db["coke"]

    # Event observer configuration
    path = r"C:\Users\CETAD\Desktop\data_excel"
    event_handler = MyHandler(collection)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)

    observer.start()

    try:
        while True:
            time_module.sleep(10)  # Use the 'time_module' alias instead of 'time'
    except KeyboardInterrupt:
        observer.stop()

    observer.join()

    # Close the connection to MongoDB upon termination
    client.close()