# ETL-процессы

Для регулярного запуска пайплайна информационной системы был выбран инструмент оркестрации Apache Airflow, с помощью которого ежемесячно запускаются два DAG’а, каждых из которых отвечает за выполнение группы шагов пайплайна.

Первый DAG запускает программный модуль, он настроен на запуск первого числа ежемесячно. 
Второй DAG запускает процедуры на стороне базы данных, он настроен на запуск второго числа ежемесячно. 

Ежемесячный запуск определен на основании частоты обновления данных ЦБ РФ и при необходимости может быть изменен.

Первый DAG не является триггером для второго с целью контроля качества выполнения и более точной фиксации времени обновления данных хранилища.
