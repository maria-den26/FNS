from pypdf import PdfReader
import re
import camelot
import pandas as pd
import bisect
import os
import download_cbrf_data as dd
from load_logging import set_log



def get_status(inn, cb_df):
    try:
        status = cb_df.loc[cb_df["ИНН"] == int(inn), "Состояние (ЕГРЮЛ)"].iloc[0]
        return status
    except Exception as e:
        return None

def parse_fns_pdf(p_inn, cb_df):
    inn = str(p_inn).zfill(10)
    project_path = os.path.dirname(os.path.abspath(__file__))  # Полный путь к текущей рабочей директории
    pdf_name = f'{project_path}/pdf_files/ul_{inn}.pdf'
    try:
        reader = PdfReader(pdf_name)
        pdf_text = reader.pages[0].extract_text()

        # Дата обращения
        rep_date = re.search(r'\d{2}\.\d{2}\.\d{4}', pdf_text).group()
        # Номер выписки
        rep_num = re.search(r'([0-9А-Яа-я])+-\d{2}-\n\d+', pdf_text).group().replace('\n', '')

        # Извлекаем все таблицы из файла
        abc = camelot.read_pdf(pdf_name, pages='all')
        pages = abc.n
        df = abc[0].df  # преобразуем первую таблицу в Pandas DataFrame
        # каждая страница как отдельная таблица
        # объединяем таблицы воедино
        full_tbl = pd.DataFrame()
        for i in range(1, pages):
            full_tbl = pd.concat([df, abc[i].df], ignore_index=True)
            df = full_tbl
        full_tbl = full_tbl.replace(r'\n', ' ', regex=True)

        # Для сбора строки данных об ЮЛ, которая пойдет в итоговый датафрейм
        ul_dat = {}
        # Словарь необходимых разделов выписки и соответствующих им маскам атрибутов
        section_headmasks = {'Наименование': r"^(fname|sname)",
                             'Место нахождения и адрес юридического лица': r"^(location|address)",
                             'Сведения о регистрации': r"^(formation|ogrn|registration)",
                             'Сведения о регистрирующем органе по месту нахождения юридического лица': r"^reg_",
                             'Сведения о состоянии юридического лица': r"^status",
                             'Сведения о прекращении юридического лица': r"^end",
                             'Сведения о правопредшественнике': r"^predecessor",
                             'Сведения о правопреемнике': r"^successor",
                             'Сведения о лице, имеющем право без доверенности действовать от имени юридического лица': r"^ul_face",
                             'Сведения об уставном капитале / складочном капитале / уставном фонде / паевом фонде': r"^capital",
                             'Сведения об учете в налоговом органе': r"^nalog",
                             'Сведения об основном виде деятельности': r"^main_act",
                             'Сведения о дополнительных видах деятельности': r"additional_activity_cnt",
                             'Сведения об реорганизации': r"^reorg",
                             'Сведения о записях, внесенных в Единый государственный реестр юридических лиц': r"_egrul$"
                             # '': r" "
                             }
        # Словарь атрибутов и соответствующих им формулировкам из выписки
        attr_names = {
            # Наименование
            'fname': 'Полное наименование на русском языке',
            'fname_grn_dt': 'ГРН и дата внесения в ЕГРЮЛ записи, содержащей указанные сведения',
            'sname': 'Сокращенное наименование на русском языке',
            'sname_grn_dt': 'ГРН и дата внесения в ЕГРЮЛ записи, содержащей указанные сведения',
            # Место нахождения и адрес юр лица
            'location_ul': 'Место нахождения юридического лица',
            'location_grn_dt': 'ГРН и дата внесения в ЕГРЮЛ записи, содержащей указанные сведения',
            'address': 'Адрес юридического лица',
            'address_grn_dt': 'ГРН и дата внесения в ЕГРЮЛ записи, содержащей указанные сведения',
            # Сведения о регистрации
            'formation': 'Способ образования',
            'ogrn': 'ОГРН',
            'ogrn_dt': 'Дата присвоения ОГРН',
            'registration_dt': 'Дата регистрации',
            'registration_grn_dt': 'ГРН и дата внесения в ЕГРЮЛ записи, содержащей указанные сведения',
            # Сведения о регистрирующем органе по месту нахождения юр лица
            'reg_organ': 'Наименование регистрирующего органа',
            'reg_organ_addr': 'Адрес регистрирующего органа',
            'reg_organ_grn_dt': 'ГРН и дата внесения в ЕГРЮЛ записи, содержащей указанные сведения',
            # сведения о состоянии юр лица                                 (лучше брать из excel)
            'status': 'Состояние',
            # Сведения о прекращении юр лица
            'ending': 'Способ прекращения',
            'end_dt': 'Дата прекращения',
            'end_organ': 'Наименование органа, внесшего запись о прекращении юридического лица',
            'end_grn_dt': 'ГРН и дата внесения в ЕГРЮЛ записи, содержащей указанные сведения',
            # сведения о правопредшественнике
            'predecessor_ogrn': 'ОГРН',
            'predecessor_inn': 'ИНН',
            'predecessor_fname': 'Полное наименование юридического лица',
            'predecessor_grn_dt': 'ГРН и дата внесения в ЕГРЮЛ записи, содержащей указанные сведения',
            # сведения о правопреемнике
            'successor_ogrn': 'ОГРН',
            'successor_inn': 'ИНН',
            'successor_fname': 'Полное наименование юридического лица',
            'successor_grn_dt': 'ГРН и дата внесения в ЕГРЮЛ записи, содержащей указанные сведения',
            # Сведения о лице, имеющем право без доверенности действовать от имени юр лица
            'ul_face': 'Фамилия Имя Отчество',
            'ul_face_inn': 'ИНН',
            'ul_face_grn_dt': 'ГРН и дата внесения в ЕГРЮЛ записи, содержащей указанные сведения',
            'ul_face_post': 'Должность',
            'ul_face_post_grn_dt': 'ГРН и дата внесения в ЕГРЮЛ записи, содержащей указанные сведения',
            # Сведения об уставном капитале/фонде
            'capital': 'Вид',
            'capital_size': 'Размер (в рублях)',
            'capital_grn_dt': 'ГРН и дата внесения в ЕГРЮЛ записи, содержащей указанные сведения',
            # Сведения об учете в налоговом органе
            'nalog_ul_inn': 'ИНН юридического лица',
            'nalog_ul_kpp': 'КПП юридического лица',
            'nalog_accounting_date': 'Дата постановки на учет в налоговом органе',
            'nalog_organ': 'Сведения о налоговом органе, в котором юридическое лицо состоит (для юридических лиц, прекративших деятельность - состояло) на учете',
            'nalog_grn_dt': 'ГРН и дата внесения в ЕГРЮЛ записи, содержащей указанные сведения',
            # Сведения об основной экономической деятельности
            'main_activity': 'Код и наименование вида деятельности',
            'main_act_grn_dt': 'ГРН и дата внесения в ЕГРЮЛ записи, содержащей указанные сведения',
            # Сведения о дополнительной экономической деятельности (кол-во)
            'additional_activity_cnt': 'Код и наименование вида деятельности',
            # Сведения об реорганизации
            'reorg_form': 'Форма реорганизации',
            'reorg_grn_dt': 'ГРН и дата внесения в ЕГРЮЛ записи, содержащей указанные сведения',
            # сведения о записях в ЕГРЮЛ
            'first_egrul': 'ГРН и дата внесения записи в ЕГРЮЛ',
            'last_egrul': 'ГРН и дата внесения записи в ЕГРЮЛ'
        }

        # собираем в ul_dat информацию из выписки
        ul_dat['rep_date'] = rep_date
        ul_dat['rep_num'] = rep_num
        # собираем информацию по dataframe
        head_indexes = full_tbl.index[
            full_tbl.iloc[:, 0].str.replace(" ", "").str.match(r"^(?!\d).+")]  # список индексов всех заголовков
        for key_s in section_headmasks:

            head = full_tbl.index[full_tbl.iloc[:, 0] == key_s].tolist()  # расположение раздела в датафрейме
            cur_attr = {k: a for k, a in attr_names.items() if
                        re.search(section_headmasks[key_s], k)}  # подмножество атрибутов, относящихся к текущему разделу

            if len(head) == 0:  # Если разделел не найден в выписке
                # print(key_s)
                if key_s == 'Сведения о состоянии юридического лица':
                    # Берем данные из excel
                    ul_dat['status'] = get_status(inn, cb_df)
                else:
                    for key_a in cur_attr:
                        ul_dat[key_a] = None
            elif key_s == 'Сведения о записях, внесенных в Единый государственный реестр юридических лиц':
                head_idx = head[0]

                # Отбираем индексы потенциальных строк
                indexes_1 = full_tbl.index[
                    full_tbl.iloc[:, 1] == attr_names['first_egrul']].tolist()
                indexes_2 = full_tbl.index[
                    full_tbl.iloc[:, 0].str.contains(attr_names['first_egrul'], na=False, case=False)].tolist()
                indexes = sorted(indexes_1 + indexes_2)
                # Фильтруем
                threshold_1 = bisect.bisect_right(indexes, head_idx)
                indexes = indexes[threshold_1:]
                # Определяем первую и последнюю записи
                rows = [indexes[0], indexes[-1]]
                i = 0
                for key_a in cur_attr:
                    # Определяем в какой столбец попали данные
                    col_index = 0
                    for j in range(3):
                        if full_tbl[i][rows[i]] != ' ' and full_tbl[i][rows[i]] != '':
                            col_index = j
                    # Выделяем из строки данные
                    ul_dat[key_a] = full_tbl[col_index][rows[i]]
                    i += 1
            elif key_s == 'Сведения о дополнительных видах деятельности':
                head_idx = head[0] + 1
                # Отбираем индексы потенциальных строк
                indexes_1 = full_tbl.index[
                    full_tbl.iloc[:, 1] == attr_names['additional_activity_cnt']].tolist()
                indexes_2 = full_tbl.index[
                    full_tbl.iloc[:, 0].str.contains(attr_names['additional_activity_cnt'], na=False, case=False)].tolist()
                indexes = sorted(indexes_1 + indexes_2)
                # Фильтруем
                threshold_1 = bisect.bisect_right(indexes, head_idx)
                threshold_2 = bisect.bisect_left(indexes, head_indexes[bisect.bisect_right(head_indexes, head_idx)])
                indexes = indexes[threshold_1:threshold_2]
                ul_dat['additional_activity_cnt'] = len(indexes)
            else:
                if key_s == 'Сведения об основном виде деятельности':
                    head_idx = head[0] + 1
                else:
                    head_idx = head[0]
                prev_row_index = head_idx
                prev_flg = 1

                for key_a in cur_attr:

                    if key_a == 'nalog_ul_inn':
                        ul_dat[key_a] = inn
                    else:
                        indexes = full_tbl.index[
                            full_tbl.iloc[:, 1] == attr_names[key_a]].tolist()  # Отбираем индексы потенциальных строк

                        # Фильтруем лишнее
                        threshold_1 = bisect.bisect_right(indexes, prev_row_index)
                        threshold_2 = bisect.bisect_left(indexes, head_indexes[bisect.bisect_right(head_indexes, head_idx)])
                        indexes = indexes[threshold_1:threshold_2]

                        if len(indexes) == 0:

                            # Возможно произошел сбой чтения, нужно искать данные в столбце 0
                            indexes = full_tbl.index[full_tbl.iloc[:, 0].str.contains(attr_names[key_a], na=False,
                                                                                      case=False)].tolist()  # Отбираем индексы потенциальных строк
                            # Фильтруем лишнее
                            threshold_1 = bisect.bisect_right(indexes, prev_row_index)
                            threshold_2 = bisect.bisect_left(indexes,
                                                             head_indexes[bisect.bisect_right(head_indexes, head_idx)])
                            indexes = indexes[threshold_1:threshold_2]

                            if len(indexes) == 0:
                                ul_dat[key_a] = None
                                prev_flg = 0

                            else:
                                row_index = indexes[bisect.bisect_right(indexes, prev_row_index)]

                                # Определяем в какой столбец попали данные
                                col_index = 0
                                for i in range(3):
                                    if full_tbl[i][row_index] != ' ' and full_tbl[i][row_index] != '':
                                        col_index = i

                                # Выделяем из строки данные
                                s = full_tbl[col_index][row_index].replace(attr_names[key_a] + ' ', '').replace(r'^\d+', '')
                                ul_dat[key_a] = ''.join(s.split(' ', 1)[1:])
                                prev_row_index = row_index
                                prev_flg = 1

                        elif prev_flg == 0 and re.search(r"_grn_dt$", key_a):
                            ul_dat[key_a] = None
                            prev_flg = 0

                        else:
                            ul_dat[key_a] = full_tbl[2][indexes[0]]
                            prev_row_index = indexes[0]
                            prev_flg = 1
        dd.delete_object(pdf_name)
        return ul_dat
    except Exception as e:
        set_log(f"ИНН {inn} - Ошибка при обработке файла: {e}")
        return {}
