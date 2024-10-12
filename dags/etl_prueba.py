from datetime import datetime,timedelta
import pendulum
import os
import pandas as pd

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="process_variables_financieras",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def variables_financieras():



    create_reservas_table = PostgresOperator(
        task_id="create_reservas_table",
        postgres_conn_id="postgres",
        sql="""
            DROP TABLE IF EXISTS reservas;
            CREATE TABLE IF NOT EXISTS reservas (
                "idVariable" NUMERIC,
                "fecha" Date,
                "valor" float
            );""",
    )


    
    
    create_reservas_temp_table = PostgresOperator(
        task_id="create_reservas_temp_table",
        postgres_conn_id="postgres",
        sql="""
            DROP TABLE IF EXISTS "STG".var_1_reservas_usd;
            CREATE TABLE "STG".var_1_reservas_usd (
                "idVariable" NUMERIC,
                "fecha" Date,
                "valor" float
            );""",
    )

    @task
    def get_data():
        # NOTE: configure this as appropriate for your airflow environment
        data_path = "/opt/airflow/dags/files/reservas.csv"
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
        URL_VAR= URL_BASE+"1/"+inicio_str+"/"+final_str

        #Hago request a la API
        response_var = requests.get(url=URL_VAR,
                                    verify=False)
        response_json_var = response_var.json()

        data_tags_var= response_json_var['results']

        #Trandformo el diccionario en un Dataframe
        df = pd.DataFrame(data_tags_var, columns=['idVariable', 'fecha', 'valor'])

        #Exporto el dataframe en un CSV
        df.to_csv('/opt/airflow/dags/files/reservas.csv', index=False)

    @task
    def consumo_stg_variable_1_reservas():
        postgres_hook = PostgresHook(postgres_conn_id="postgres")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        data_path = "/opt/airflow/dags/files/reservas.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        with open(data_path, "r") as file:
            cur.copy_expert(
                """COPY "STG".var_1_reservas_usd FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'""",
                file,
            )
        conn.commit()

    @task
    def merge_data():
        query = """
            INSERT INTO "BCRA".var_1_reservas_usd
            SELECT *
            FROM (
                SELECT  *
                FROM "STG".var_1_reservas_usd
            ) t
            ON CONFLICT ("fecha") DO UPDATE
            SET
              "valor" = excluded."valor";
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="postgres")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return 0
        except Exception as e:
            return 1

    [create_reservas_table, create_reservas_temp_table] >> consumo_stg_variable_1_reservas() >> merge_data()


dag = variables_financieras()