📜 Описание DAG-файла
Файл etl_parallel_processing_sergey_odintsov реализует DAG (Directed Acyclic Graph) для выполнения ETL-процесса.

DAG выполняется каждый месяц 5-го числа в 00:00.

- Основные задачи:
Задача	Описание
```extract_task```: Извлекает данные из profit_table.csv.
```transform_task```: Преобразует данные и создает флаги активности.
```load_task```: Объединяет результаты и сохраняет в итоговый файл flags_activity.csv.

- Запуск проекта в Ubuntu(VirtualBox): ```airflow scheduler & airflow webserver -p 8080```
