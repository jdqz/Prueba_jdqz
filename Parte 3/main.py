from fastapi import FastAPI
from sparky_bc import Sparky
import pandas as pd
import json
from getpass import getuser, getpass

usuario = getuser() # Nombre de usuario	
clave = getpass(prompt='Ingrese Contraseña ' + usuario +':')  # Contraseña usuario
sp = Sparky(usuario, password=clave, 
            hostname='sbmdeblze004', dsn="IMPALA_PROD", remote=True)

app = FastAPI()

@app.get("/productos-by-id")
def productos(id: float):
  df = sp.helper.obtener_dataframe(consulta=
  "SELECT radicado,producto,valor_inicial,plazo,periodicidad,saldo_deuda,tasa_efectiva,valor_final " +
  "FROM proceso.obligaciones_clientes_final WHERE num_documento = " + str(id))
  return df

@app.get("/valor-final")
def valor_final(id: float):
  df = sp.helper.obtener_dataframe(consulta=
  "SELECT valor_final " +
  "FROM proceso.obligaciones_clientes_unico WHERE num_documento = " + str(id))
  return df
