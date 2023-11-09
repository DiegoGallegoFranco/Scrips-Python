from operator import concat

import empty as empty
import psycopg2
import serial
from datetime import datetime

ser = serial.Serial("/dev/ttyACM0", 9600)
while True:
     conexion = psycopg2.connect(host="localhost", database="root", user="root", password="root")
     cur = conexion.cursor()
     cc = str(ser.readline().decode())
     cur.execute(("SELECT nombre FROM public.personal t  where uid  = (%s)"), (cc[1:][:-2],))
     data=cur.fetchall()
     print(data)
     if data == []:
          print("vacio")
          sql = ("insert into registros(uid,nombre,fecha) VALUES (%s,%s,%s) ")
          datos = ((cc[1:][:-2]), "No Registrado", datetime.now())
          cur.execute(sql, datos)
          conexion.commit()
     else :
          sql=("insert into registros(uid,nombre,fecha) VALUES (%s,%s,%s) ")
          datos=((cc[1:][:-2]),(data[0][0]),datetime.now())
          cur.execute(sql, datos)
          conexion.commit()
     conexion.close()