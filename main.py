#-*- coding: utf-8 -*-
import json
import locale
import os
from datetime import datetime
from enum import Enum

import pandas as pd
import requests

from sqlalchemy import create_engine

import sqlalchemy as db
from decouple import config


locale.setlocale(locale.LC_TIME, "es")

def nombre_subcarpeta():
  mes = datetime.now().strftime("%B")
  anio = datetime.now().year
  return f'{anio}-{mes}'

with open("fuentes.json") as f:
    fuentes = json.load(f)

def generar_nombre_archivo(categoria):
  fecha_actual = datetime.now().strftime('%d-%m-%Y')
  return f'{categoria}-{fecha_actual}.csv'


def crear_carpetas(categoria):
  assert isinstance(categoria, str), 'Parámetro debe ser string'
  if not os.path.exists(os.path.join(os.getcwd(), categoria)):
      os.mkdir(categoria)
  if not os.path.exists(os.path.join(os.getcwd(), categoria, nombre_subcarpeta())):
    os.mkdir(os.path.join(categoria, nombre_subcarpeta()))
  

def descargar_fuentes (categoria, url):
  nombre_archivo = generar_nombre_archivo(categoria)
  path_archivo = os.path.join(os.getcwd(), categoria, nombre_subcarpeta(), nombre_archivo)
  req = requests.get(url)
  req.encoding = 'utf-8'
  with open(path_archivo, 'w+', encoding='utf-8') as f:
    f.write(req.text)

columnas = {
    'Cod_Localidad' : "cod_localidad",
    'ID_Provincia': "id_provincia",
    'ID_Departamento' : "id_departamento",
    'Categoria' : "categoria",
    'Provincia' : "provincia",
    'Localidad' : "localidad",
    'Nombre' : "nombre",
    'Domicilio' : "domicilio",
    'Codigo_Postal' : "codigo_postal",
    'Numero_Tel' : "numero_de_telefono",
    'Mail' : "mail",
    "Web" : "web" 
}

def main():

  for categoria, url in fuentes.items():
    crear_carpetas(categoria)
    descargar_fuentes(categoria, url)
  
  categorias = list(fuentes.keys())
  def fname (categoria):
    return os.path.join(os.getcwd(), categoria, nombre_subcarpeta(), generar_nombre_archivo(categoria))
  
  df_museos = pd.read_csv(fname(categorias[0]), encoding="utf-8")
  df_cines = pd.read_csv(fname(categorias[1]), encoding="utf-8")
  df_bibliotecas = pd.read_csv(fname(categorias[2]), encoding="utf-8")
  
  columnas_a_filtrar = list(columnas.values())  
  
  df_bibliotecas = df_bibliotecas.rename(columns={
      "Cod_Loc" : columnas['Cod_Localidad'],
      "IdProvincia" : columnas['ID_Provincia'],
      "IdDepartamento" : columnas['ID_Departamento'],
      "Categoría" : columnas['Categoria'],
      "Provincia" : columnas['Provincia'],
      "Localidad" : columnas['Localidad'],
      "Nombre" : columnas['Nombre'],
      "Domicilio" : columnas['Domicilio'],
      "CP" : columnas['Codigo_Postal'],
      "Teléfono" : columnas['Numero_Tel'],
      "Mail" : columnas['Mail'],
      "Web" : columnas['Web']
  })
  df_bibliotecas1 = df_bibliotecas[columnas_a_filtrar].copy()
    
  df_cines = df_cines.rename(columns={
      "Cod_Loc" : columnas['Cod_Localidad'],
      "IdProvincia" : columnas['ID_Provincia'],
      "IdDepartamento" : columnas['ID_Departamento'],
      "Categoría" : columnas['Categoria'],
      "Provincia" : columnas['Provincia'],
      "Localidad" : columnas['Localidad'],
      "Nombre" : columnas['Nombre'],
      "Dirección" : columnas['Domicilio'],
      "CP" : columnas['Codigo_Postal'],
      "Teléfono" : columnas['Numero_Tel'],
      "Mail" : columnas['Mail'],
      "Web" : columnas['Web']
  })
  df_cines1 = df_cines[columnas_a_filtrar].copy()
  
  df_museos = df_museos.rename(columns={
      "Cod_Loc" : columnas['Cod_Localidad'],
      "IdProvincia" : columnas['ID_Provincia'],
      "IdDepartamento" : columnas['ID_Departamento'],
      "categoria" : columnas['Categoria'],
      "provincia" : columnas['Provincia'],
      "localidad" : columnas['Localidad'],
      "nombre" : columnas['Nombre'],
      "direccion" : columnas['Domicilio'],
      "CP" : columnas['Codigo_Postal'],
      "telefono" : columnas['Numero_Tel'],
      "Mail" : columnas['Mail'],
      "Web" : columnas['Web']
  })
  df_museos1 = df_museos[columnas_a_filtrar].copy()
  
  df_conjunto = pd.concat([
      df_bibliotecas1,
      df_cines1,
      df_museos1
  ])
  
 
  ###########
  # ANALISIS

  #Cant registros tot por categoria
  cant_registros = df_conjunto['categoria'].value_counts()

  #Cant registros tot por fuente
  cant_museos = len(df_museos.index)
  cant_cines = len(df_cines.index)
  cant_bibliotecas = len(df_bibliotecas.index)

  # Cant de registros por provincia y categoria
  lista_prov = (df_conjunto['provincia']).unique()
  df_provincia_categoria = df_conjunto.groupby(['provincia', 'categoria']).size()

  
  # Selección cines
  df_cines_seleccion = pd.read_csv(fname(categorias[1]), encoding="utf-8")
  df_cines_tabla = df_cines_seleccion[['Provincia', 'Pantallas', 'Butacas', 'espacio_INCAA']].copy() 
  incaa_valores=df_cines_tabla['espacio_INCAA'].unique()

  df_cines_tabla.replace({'espacio_INCAA': {'si': 1, 'SI': 1, 'nan':0, '0':0}}, inplace=True) 
  df_tabla_cines_group = df_cines_tabla.groupby(['Provincia']).sum()

  #################
  # ANALISIS TO SQL

  engine = db.create_engine(config("DATABASE_URL"))
  conexion_db = engine.connect()
 
  df = pd.DataFrame(df_conjunto)
  df.to_sql('registros', con=conexion_db, index=False, if_exists="replace")

  df_a = pd.DataFrame(df_provincia_categoria)
  df_a.to_sql('registros_categoria', con=conexion_db, index=False, if_exists="replace")

  df_cine = pd.DataFrame(df_tabla_cines_group)
  df_cine.to_sql('registros_cines', con=conexion_db, index=False, if_exists="replace")

if __name__ == "__main__":
    main()