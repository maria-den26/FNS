from datetime import datetime
import os

def set_log(log_message, log_file="log.txt"):
    project_path = os.path.dirname(os.path.abspath(__file__))  # Полный путь к текущей рабочей директории
    log_file = f'{project_path}/{log_file}'
    # Записывает сообщение логирования в файл с меткой даты и времени.
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {log_message}\n"
    try:
        with open(log_file, "a", encoding="utf-8") as file:
            file.write(log_entry)
    except Exception as e:
        print(f"Ошибка при записи лога: {e}")

