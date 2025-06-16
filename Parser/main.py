import pandas as pd
import numpy as np
import os
import psycopg2
import download_cbrf_data as dd
import inn_fns as fns
import fns_pdf_parser as p
from load_logging import set_log
from concurrent.futures import ThreadPoolExecutor   # Для параллельного скачивания файлов
from multiprocessing import Pool, cpu_count  # Для параллельного парсинга
from functools import partial
from sqlalchemy import create_engine



def save_dataframe_to_excel(df, filename, sheet_name='Sheet1'):
    """
    Сохраняет pandas DataFrame в Excel файл.
    """
    try:
        df.to_excel(filename, sheet_name=sheet_name, index=False)
        set_log(f"Файл сохранен: {filename}")
    except Exception as e:
        set_log(f"Ошибка при сохранении файла: {e}")

def upload_df_to_db(df, table_name, schema_name):
    try:
        # conn_string = 'postgresql://postgres:postgres@localhost:5432/fns'
        conn_string = 'postgresql://postgres:postgres@192.168.0.100:5432/fns' # Необходимо изменить под конкретную БД
        db = create_engine(conn_string)
        conn = db.connect()
        set_log('Connect to database FNS')
        df.to_sql(table_name, con=conn, schema=schema_name, if_exists='append',
                  index=False)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        conn.close()
        set_log('Disconnect to database FNS')
    except Exception as e:
        set_log(f"Ошибка: {e}")
        print('Can`t establish connection to database')


def main():
    set_log("start")
    project_path = os.path.dirname(os.path.abspath(__file__))  # Полный путь к текущей рабочей директории
    # dd.delete_object('ul_result.xlsx')

    # Получение данных ЦБ РФ
    nrows = 2755   # количество ЮЛ
    cb_df = dd.cbrf_to_df(nrows)

    # Загружаем в БД данные cb_df
    try:
        # Создаем копию DataFrame
        cb_df_to_db = cb_df.copy()

        # Преобразуем столбец "ИНН" в строковый тип и дополняем лидирующими нулями до длины 10
        cb_df_to_db['ИНН'] = cb_df_to_db['ИНН'].fillna(0).astype(int).astype(str).str.zfill(10)
        cb_df_to_db['ИНН'] = cb_df_to_db['ИНН'].replace('0000000000', np.nan)
        # Преобразуем столбец "ОГРН" в строковый тип и дополняем лидирующими нулями до длины 13
        cb_df_to_db['ОГРН'] = cb_df_to_db['ОГРН'].fillna(0).astype(int).astype(str).str.zfill(13)
        cb_df_to_db['ОГРН'] = cb_df_to_db['ОГРН'].replace('0000000000000', np.nan)
        # Преобразуем столбец "КПП" в строковый тип и дополняем лидирующими нулями до длины 9
        cb_df_to_db['КПП'] = cb_df_to_db['КПП'].fillna(0).astype(int).astype(str).str.zfill(9)
        cb_df_to_db['КПП'] = cb_df_to_db['КПП'].replace('000000000', np.nan)

        # Переименовываем столбцы в копии
        cb_df_to_db.rename(columns={
            'Краткое (унифицированное) наименование': 'ul_name',
            'Краткое наименование (ЕГРЮЛ)': 'sname',
            'Полное наименование': 'fname',
            'ИНН': 'inn',
            'ОГРН': 'ogrn',
            'КПП': 'kpp',
            'Сфера деятельности': 'activity',
            'Адрес': 'address',
            'Страна': 'country',
            'Состояние (ЕГРЮЛ)': 'status'
        }, inplace=True)

        upload_df_to_db(cb_df_to_db, 'ul_cbrf_extract_data', 'fns_storage')
    except Exception as e:
        set_log(f"Ошибка: {e}")

    # Список словарей для заполнения итогового датафрейма по всем выпискам
    ul_dat = []
    try:
        # В связи с ограничением памяти для хранения pdf выгружаем и обрабатываем файлы партиями
        nrows = 2755
        n_batches = 5
        batch_size = int(nrows/n_batches)  # кол-во загружаемых и обрабатываемых pdf
        lower_bound = 0
        upper_bound = lower_bound + batch_size
        num_pdfs = 0
        for batch in range(n_batches):
            for row_idx in range(lower_bound, upper_bound):
                # set_log(f'{row_idx} {cb_df["ИНН"].iloc[row_idx]}')
                num_pdfs += fns.download_fns_pdf(cb_df["ИНН"].iloc[row_idx])

            # with Pool(cpu_count()) as pool:
            #     num_pdfs = sum(pool.map(fns.download_fns_pdf, cb_df.loc[lower_bound:upper_bound, "ИНН"].tolist()))
            # with ThreadPoolExecutor(10) as executor:
            #     num_pdfs = sum(executor.map(fns.download_fns_pdf, cb_df.loc[lower_bound:upper_bound,"ИНН"].tolist()))
            set_log(f"На шаге {batch} загружено {num_pdfs} pdf")
            if num_pdfs > 0:
                # Фиксируем cb_df как второй аргумент
                parse_fns_with_df = partial(p.parse_fns_pdf, cb_df=cb_df)
                with Pool(cpu_count()) as pool:
                    dat = pool.map(parse_fns_with_df, cb_df.loc[lower_bound:upper_bound-1,"ИНН"].tolist())
                # with Pool(cpu_count()) as pool:
                #     dat = pool.map(p.parse_fns_pdf, cb_df.loc[lower_bound:upper_bound-1,"ИНН"].tolist())  # Вычесть из верхней границы 1, так как обе включены
            lower_bound = upper_bound
            upper_bound += batch_size

            # загружаем партию в бд
            upload_df_to_db(pd.DataFrame(data=dat), 'ul_fns_extract_data', 'fns_storage')

            ul_dat = ul_dat + dat

            # ul_df = pd.DataFrame(data=ul_dat)  # собираем датафрейм по всем выпискам для выгрузки в слой накопления
            # print(ul_df.head(15))
    except Exception as e:
        set_log(f"Ошибка: {e}")
        # ul_df = pd.DataFrame(data=ul_dat)  # собираем датафрейм по всем выпискам для выгрузки в слой накопления

    # Получаем список всех файлов в директории
    files = os.listdir(f'{project_path}/pdf_files')

    # Перебираем файлы и удаляем их
    for file_name in files:
        # Полный путь к файлу
        file_path = os.path.join(project_path, 'pdf_files', file_name)
        dd.delete_object(file_path)

    # save_dataframe_to_excel(ul_df, 'ul_result.xlsx', sheet_name='Sheet1')
    # upload_df_to_db(ul_df)
    set_log("finish")


if __name__ == '__main__':
    main()