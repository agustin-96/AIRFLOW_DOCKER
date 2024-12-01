from datetime import datetime,timedelta
import pendulum
import os
import pandas as pd

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from include.functions import ExtraerInfo,CrearTablaTemporal,ConsumoArchivo


@dag(
    dag_id="process_variables_financieras",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def variables_financieras():



    

    
    
    create_var1_reservas_table = PostgresOperator(
        task_id="create_reservas_temp_table",
        postgres_conn_id="postgres",
        sql=CrearTablaTemporal('var_1_reservas_usd'),
    )

    @task
    def get_data_var1():
        ExtraerInfo('1','reservas')

 

    @task
    def consumo_stg_var1_reservas():
        ConsumoArchivo("reservas.csv","var_1_reservas_usd")


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

    create_var1_reservas_table >> get_data_var1() >> consumo_stg_var1_reservas() >> merge_data()


dag = variables_financieras()