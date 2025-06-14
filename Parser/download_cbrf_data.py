import zipfile
import subprocess
import requests
from bs4 import BeautifulSoup
import re
import os
import pandas as pd
from load_logging import set_log


# URL страницы с архивами
BASE_URL = "https://cbr.ru/registries/rcb/inn_ogrn/"



def get_latest_archive_url():
    response = requests.get(BASE_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    # Ищем ссылку на архив (обычно ZIP)
    archive_link = None
    for link in soup.find_all("a", href=True):
        if re.search(r".*\.(zip|rar)$", link["href"]):  # Фильтруем ссылки на ZIP-архивы
            archive_link = link["href"]
            break  # Берем первую найденную ссылку
    if not archive_link:
        set_log("Архив не найден")
        return None
    # Полный URL архива
    if not archive_link.startswith("http"):
        archive_link = f"https://cbr.ru{archive_link}"
    return archive_link


def download_archive(url, save_path=None):
    if save_path is None:
        filename = os.path.basename(url)  # Название файла берем из URL
        save_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
    print(save_path)
    response = requests.get(url)
    response.raise_for_status()
    with open(save_path, "wb") as f:
        for chunk in response.iter_content(1024):
            f.write(chunk)
    return save_path


def extract_archive(archive_path, extract_folder):
    try:
        if archive_path.endswith(".zip"):
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                zip_ref.extractall(extract_folder)
            set_log(f"ZIP-архив распакован в: {extract_folder}")
        elif archive_path.endswith(".rar"):
            '''
            # Для запуска в Windows
            
            project_path = os.path.abspath(os.getcwd())  # Полный путь к текущей рабочей директории
            command = f'"c:\\program files\\winrar\\rar.exe" x "{project_path}\\{archive_path}" "{project_path}\\{extract_folder}"'
            subprocess.run(command, shell=True)
            '''
            # Для запуска в Linux
            # Используем утилиту `unrar` в WSL/Linux
            command = ["unrar", "x", "-y", archive_path, extract_folder]
            subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            set_log(f"RAR-архив распакован в: {extract_folder}")
        else:
            set_log("Неизвестный формат архива!")
    except Exception as e:
        set_log(f"Не удалось распаковать архив! Ошибка: {e}")



def delete_object(obj_path):
    if os.path.exists(obj_path):
        os.remove(obj_path)
        set_log(f"{obj_path} удалён.")
    else:
        set_log("Файл не найден.")


def cbrf_to_df(nrows):
    project_path = os.path.dirname(os.path.abspath(__file__))  # Полный путь к текущей рабочей директории
    # Получение данных ЦБ РФ
    archive_url = get_latest_archive_url()  # определяем ссылку на самый актуальный архив с данными
    archive_name = download_archive(archive_url)  # скачиваем архив и определяем его имя
    print(archive_name)
    extract_folder = "cb_excel"  # папка для выгрузки данных из архива
    print(project_path)
    extract_folder = f'{project_path}/{extract_folder}'# абсолютный путь до папки
    print(extract_folder)
    extract_archive(archive_name, extract_folder)  # Распаковываем архив
    extracted_files = os.listdir(extract_folder)  # определяем, какие файлы там есть
    delete_object(archive_name)  # Удаляем архив
    # Считываем excel в pandas dataframe
    cb_df = pd.read_excel(f'{extract_folder}/{extracted_files[0]}',
                          engine='openpyxl',
                          sheet_name="Справочник",
                          header=1,
                          nrows=nrows)
    delete_object(f'{extract_folder}/{extracted_files[0]}')  # Удаляем файл
    return cb_df

