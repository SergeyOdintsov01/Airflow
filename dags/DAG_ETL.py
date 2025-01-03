from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from transform_script import transfrom

# Путь к данным в проекте
DATA_PATH = "/home/sergey/airflow"

def extract_func():
    """
    Функция для извлечения данных из profit_table.csv.
    Сохраняет DataFrame в файл и передаёт путь через XCom.
    """
    df = pd.read_csv(f"{DATA_PATH}/profit_table.csv")
    output_path = f"{DATA_PATH}/extracted_data.csv"
    df.to_csv(output_path, index=False)  # Сохраняем DataFrame в файл
    return output_path  # Возвращаем путь к файлу

def transform_func(date, **kwargs):
    """
    Функция для обработки данных для конкретного продукта.
    Сохраняет результат обработки в файл.
    """
    ti = kwargs["ti"]

    # Получаем путь к файлу с извлечёнными данными
    file_path = ti.xcom_pull(task_ids="extract_task")

    # Читаем данные из файла
    profit_table = pd.read_csv(file_path)

    # Применяем трансформацию для заданного продукта
    product_id = kwargs["product_list"][0]
    result = transfrom(profit_table, date, product_list=[product_id])

    # Сохраняем результат обработки в отдельный файл
    output_path = f"{DATA_PATH}/flags_activity_{product_id}.csv"
    result.to_csv(output_path, index=False)

    # Возвращаем путь к файлу
    return output_path



def load_func(**kwargs):
    """
    Функция для объединения результатов из файлов и записи их в итоговый файл.
    """
    ti = kwargs["ti"]

    # Список файлов, созданных задачами transform_task_<product_id>
    task_ids = [
        f"transform_task_{product_id}"
        for product_id in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
    ]
    file_paths = ti.xcom_pull(task_ids=task_ids)

    # Объединяем все DataFrame из файлов
    combined_data = pd.concat([pd.read_csv(file) for file in file_paths])

    # Записываем объединённый результат в flags_activity.csv
    combined_data.to_csv(f"{DATA_PATH}/flags_activity.csv", mode="a", index=False)


# Параметры по умолчанию для DAG
default_args = {
    "owner": "Sergey Odintsov", 
    "start_date": datetime(2024, 1, 1),  
}

# Создание DAG
with DAG(
    dag_id="etl_parallel_processing_sergey_odintsov", 
    default_args=default_args,
    schedule_interval="0 0 5 * *",  # DAG запускается 5-го числа каждого месяца
    catchup=False,
) as dag:
    # Задача Extract: Чтение данных
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_func,
    )

    # Создание задач Transform для каждого продукта
    transform_tasks = []
    for product_id in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]:
        transform_tasks.append(
            PythonOperator(
                task_id=f"transform_task_{product_id}",
                python_callable=transform_func,
                op_kwargs={"date": "2024-03-01", "product_list": [product_id]},
                provide_context=True,
            )
        )

    # Задача Load: Объединение результатов и запись в файл
    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_func,
        provide_context=True,
    )

    # Определение зависимостей задач
    extract_task >> transform_tasks >> load_task
