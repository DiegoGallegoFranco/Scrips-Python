import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pandas as pd
import pymongo
import numpy as np


class MyHandler(FileSystemEventHandler):
    def __init__(self, db_collection):
        self.db_collection = db_collection

    def on_created(self, event):
        print("Archivo detectado")
        if not event.is_directory:
            file_path = os.path.abspath(event.src_path)
            print(f"Nuevo archivo detectado: {file_path}")

            # Lee el archivo Excel en un DataFrame y maneja fechas nulas
            df = pd.read_excel(file_path, na_values=["", "NULL", "-", "Nan"])

            # Rellenar valores nulos de manera adecuada
            for column in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[column]):
                    df[column].fillna(pd.NaT, inplace=True)
                else:
                    df[column].fillna(np.nan, inplace=True)  # Usa np.nan para otros tipos

            # Convierte las fechas a strings
            for column in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[column]):
                    df[column] = df[column].astype(str)

            # Convierte el DataFrame a lista de diccionarios
            data = df.to_dict(orient="records")

            # Inserta los datos en la colección de MongoDB evitando duplicados
            for row in data:
                existing_document = self.db_collection.find_one({"id": row["id"]})
                if existing_document is None:
                    self.db_collection.insert_one(row)
                else:
                    print(f"Documento con id '{row['id']}' ya existe. Omitiendo inserción.")

            print(f"Datos insertados (sin duplicados) en MongoDB.")


if __name__ == "__main__":
    # Configuración de MongoDB
    client = pymongo.MongoClient("localhost", 27017)
    db = client["permanencias"]
    collection = db["tramitadas"]

    # Configuración del observador de eventos
    path = r"C:\Users\CETAD\Desktop\data_excel"
    event_handler = MyHandler(collection)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)

    observer.start()

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()

    # Cierra la conexión a MongoDB al finalizar
    client.close()
