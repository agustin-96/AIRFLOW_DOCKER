from datetime import datetime,timedelta
import pendulum
import os
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

def ExtraerInfo(num:str,nombre:str)-> None:
    # NOTE: configure this as appropriate for your airflow environment
    data_path = "/opt/airflow/dags/files/"+nombre+".csv"
    os.makedirs(os.path.dirname(data_path), exist_ok=True)

    #Declaro el URL BASE para importar informacion
    URL_BASE= "https://api.bcra.gob.ar/estadisticas/v2.0/datosvariable/"
    
    #Obtengo el dia de ayer
    final= datetime.today()- timedelta(1)
    final_str= final.strftime('%Y-%m-%d')

    #Obtengo el dia correspondiente a 60 dias atras
    inicio = final - timedelta(60)
    inicio_str= inicio.strftime('%Y-%m-%d')
    
    #Creacion de URL completo
    URL_VAR= URL_BASE+num+"/"+inicio_str+"/"+final_str

    #Hago request a la API
    response_var = requests.get(url=URL_VAR,
                                verify=False)
    response_json_var = response_var.json()

    data_tags_var= response_json_var['results']

    #Trandformo el diccionario en un Dataframe
    df = pd.DataFrame(data_tags_var, columns=['idVariable', 'fecha', 'valor'])

    #Exporto el dataframe en un CSV
    df.to_csv(data_path, index=False)


def CrearTablaTemporal(nombre:str)-> str:
    sql= f"""
            DROP TABLE IF EXISTS "STG".{nombre};
            CREATE TABLE "STG".{nombre} (
                "idVariable" NUMERIC,
                "fecha" Date,
                "valor" float
            );"""
    return sql


def ConsumoArchivo(nombre_archivo:str, nombre_tabla:str):
        postgres_hook = PostgresHook(postgres_conn_id="postgres")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        data_path = f"/opt/airflow/dags/files/{nombre_archivo}.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        with open(data_path, "r") as file:
            cur.copy_expert(
                f"""COPY "STG".{nombre_tabla} FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'""",
                file,
            )
        conn.commit()

