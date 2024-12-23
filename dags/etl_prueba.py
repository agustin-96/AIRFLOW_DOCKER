from datetime import datetime,timedelta
import pendulum
import os
import pandas as pd

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from include.functions import ExtraerInfo,CrearTablaTemporal,ConsumoArchivo,ExportarDataTablaFinal


@dag(
    dag_id="process_variables_financieras",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def variables_financieras():

    """
    ----------------VARIABLE 1 RESERVAS----------------------------
    """


    create_var1_reservas_table = PostgresOperator(
        task_id="create_reservas_temp_table",
        postgres_conn_id="postgres",
        sql=CrearTablaTemporal('var1_reservas_usd'),
    )

    @task
    def get_data_var1():
        ExtraerInfo('1','var1_reservas')

    @task
    def consumo_stg_var1():
        ConsumoArchivo("var1_reservas","var1_reservas_usd")

    @task
    def merge_data():
        ExportarDataTablaFinal("var1_reservas_usd")

    """
    ----------------------VARIABLE 4 CAMBIO MINORISTA----------------------------
    """
    create_var4_cambio_minorista_table = PostgresOperator(
        task_id="create_cambio_minorista_temp_table",
        postgres_conn_id="postgres",
        sql=CrearTablaTemporal('var4_cambio_minorista_pesos'),
    )

    @task
    def get_data_var4():
        ExtraerInfo('4','var4_cambio_minorista')

    @task
    def consumo_stg_var4():
        ConsumoArchivo("var4_cambio_minorista","var4_cambio_minorista_pesos")

    @task
    def merge_data_var4():
        ExportarDataTablaFinal("var4_cambio_minorista_pesos")


    """
    ----------------------VARIABLE 5 CAMBIO MAYORISTA----------------------------
    """
    create_var5_cambio_mayorista_table = PostgresOperator(
        task_id="create_cambio_mayorista_temp_table",
        postgres_conn_id="postgres",
        sql=CrearTablaTemporal('var5_cambio_mayorista_pesos'),
    )

    @task
    def get_data_var5():
        ExtraerInfo('5','var5_cambio_mayorista')

    @task
    def consumo_stg_var5():
        ConsumoArchivo("var5_cambio_mayorista","var5_cambio_mayorista_pesos")

    @task
    def merge_data_var5():
        ExportarDataTablaFinal("var5_cambio_mayorista_pesos")


    """
    ----------------------VARIABLE 6 TPM----------------------------
    """
    create_var6_tpm_table = PostgresOperator(
        task_id="create_TPM_temp_table",
        postgres_conn_id="postgres",
        sql=CrearTablaTemporal('var6_tpm_na'),
    )

    @task
    def get_data_var6():
        ExtraerInfo('6','var6_tpm')

    @task
    def consumo_stg_var6():
        ConsumoArchivo("var6_tpm","var6_tpm_na")

    @task
    def merge_data_var6():
        ExportarDataTablaFinal("var6_tpm_na")


    """
    ----------------------VARIABLE 7 BADLAR----------------------------
    """
    create_var7_badlar_table = PostgresOperator(
        task_id="create_badlar_temp_table",
        postgres_conn_id="postgres",
        sql=CrearTablaTemporal('var7_badlar_na'),
    )

    @task
    def get_data_var7():
        ExtraerInfo('7','var7_badlar')

    @task
    def consumo_stg_var7():
        ConsumoArchivo("var7_badlar","var7_badlar_na")

    @task
    def merge_data_var7():
        ExportarDataTablaFinal("var7_badlar_na")

    create_var1_reservas_table >> get_data_var1() >> consumo_stg_var1() >> merge_data()
    create_var4_cambio_minorista_table >> get_data_var4() >> consumo_stg_var4() >> merge_data_var4()
    create_var5_cambio_mayorista_table >> get_data_var5() >> consumo_stg_var5() >> merge_data_var5()
    create_var6_tpm_table >> get_data_var6() >> consumo_stg_var6() >> merge_data_var6()
    create_var7_badlar_table >> get_data_var7() >> consumo_stg_var7() >> merge_data_var7()

dag = variables_financieras()