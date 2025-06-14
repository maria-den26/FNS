import requests
from fake_useragent import UserAgent
import time
import random
import os
from load_logging import set_log

def download_fns_pdf(p_inn):
    # print(p_inn)
    project_path = os.path.dirname(os.path.abspath(__file__))  # Полный путь к текущей рабочей директории
    inn = str(p_inn).zfill(10)
    print(inn)
    url = 'https://egrul.nalog.ru'
    # ua = UserAgent(platforms='desktop', os='Windows')
    # ua = UserAgent()
    # headers = {
    #     'accept': '*/*',
    #     'user-agent': ua.random
    # }

    '''
    headers = {
        'accept': '*/*',
        'user-agent': ua.chrome
    }
    '''

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'ru,en;q=0.9',
        'connection': 'keep-alive',
        'referer': 'https://egrul.nalog.ru/index.html',
        'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "YaBrowser";v="25.2", "Yowser";v="2.5"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 YaBrowser/25.2.0.0 Safari/537.36'
    }

    # Используем requests.Session() для сохранения cookies
    with requests.Session() as s:
        # Отправляем запрос на получение токена поиска
        resp = s.post(url,
                      headers=headers,
                      data={'query': inn})
        if resp.status_code != 200:
            set_log(f"{inn} resp.status_code {resp.status_code}")
            return 0
        search_token = resp.json().get('t')
        if not search_token:
            # raise ValueError("Не удалось получить токен поиска")
            set_log("Не удалось получить токен поиска")
            return 0
        time.sleep(random.uniform(1, 3))  # Случайная задержка
        # Запрашиваем результат поиска
        search = s.get(f'{url}/search-result/{search_token}',
                       headers=headers,
                       cookies=resp.cookies
                       )
        if search.status_code != 200:
            set_log(f"{inn} search.status_code {search.status_code}")
            return 0
        search_data = search.json()
        if 'rows' not in search_data or not search_data['rows']:
            # raise ValueError("Не найдено результатов для ИНН")
            set_log(f"Не найдено результатов для ИНН {inn}")
            return 0
        download_token = search_data['rows'][0]['t']
        time.sleep(random.uniform(3, 7))  # Случайная задержка
        vrequest = s.get(f'{url}/vyp-request/{download_token}',
                         headers=headers,
                         cookies=search.cookies
                         )
        if vrequest.status_code != 200:
            set_log(f"{inn} vrequest.status_code {vrequest.status_code}")
            return 0
        time.sleep(random.uniform(3, 8))  # Случайная задержка
        vstatus = s.get(f'{url}/vyp-status/{download_token}',
                        headers=headers,
                        cookies=vrequest.cookies
                        )
        if vstatus.status_code != 200:
            set_log("vstatus.status_code ", vstatus.status_code)
            return 0
        # Попытки скачать файл с повторениями при ошибке 500
        MAX_RETRIES = 3
        for attempt in range(MAX_RETRIES):
            download = s.get(f'{url}/vyp-download/{download_token}',
                             headers=headers,
                             cookies=vstatus.cookies,
                             stream=True)
            if download.status_code == 500:
                WAIT_TIME = random.uniform(5, 10)
                # print(f"Попытка {attempt + 1}/{MAX_RETRIES}: Ошибка 500. Жду {WAIT_TIME} секунд...")
                time.sleep(WAIT_TIME)
                if (attempt + 1 == MAX_RETRIES):
                    set_log(f"{inn} download.status_code {download.status_code}")
                    return 0
                continue
            else:
                break  # Если запрос успешен, выходим из цикла
        # Сохраняем файл
        file_path = f'{project_path}/pdf_files/ul_{inn}.pdf'
        with open(file_path, 'wb') as file:
            for chunk in download.iter_content(8192):
                file.write(chunk)
        set_log(f'Файл успешно скачан: {file_path}')
        print(f'Файл успешно скачан: {file_path}')
        time.sleep(random.uniform(1, 3))  # Случайная задержка
        return 1
