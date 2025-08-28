import os
import csv
import logging
from datetime import datetime
import time
import psycopg2
from psycopg2 import sql
import sys

# Настройка кодировки для Windows
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_process.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация подключения к БД
DB_CONFIG = {
    'host': 'localhost',
    'database': 'neoflex_etl',
    'user': 'postgres',
    'password': '1111',
    'port': '5432'
}

# Пути к CSV файлам
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

CSV_FILES = {
    'ft_balance_f': os.path.join(DATA_DIR, 'ft_balance_f.csv'),
    'ft_posting_f': os.path.join(DATA_DIR, 'ft_posting_f.csv'),
    'md_account_d': os.path.join(DATA_DIR, 'md_account_d.csv'),
    'md_currency_d': os.path.join(DATA_DIR, 'md_currency_d.csv'),
    'md_exchange_rate_d': os.path.join(DATA_DIR, 'md_exchange_rate_d.csv'),
    'md_ledger_account_s': os.path.join(DATA_DIR, 'md_ledger_account_s.csv')
}


class ETLError(Exception):
    pass


def detect_csv_format(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            first_line = f.readline().strip()
            if ';' in first_line:
                dialect = csv.excel()
                dialect.delimiter = ';'
                has_header = True
            else:
                f.seek(0)
                sample = f.read(1024)
                f.seek(0)
                sniffer = csv.Sniffer()
                dialect = sniffer.sniff(sample)
                has_header = sniffer.has_header(sample)

            logger.info(f"Определен формат файла {file_path}: "
                        f"разделитель={dialect.delimiter!r}, "
                        f"квотирование={dialect.quotechar!r}")
            return dialect, has_header
    except Exception as e:
        raise ETLError(f"Ошибка определения формата CSV: {str(e)}")


def parse_date(date_str):
    date_formats = [
        '%d.%m.%Y', '%Y-%m-%d', '%d-%m-%Y', '%Y%m%d', '%m/%d/%Y'
    ]
    if not date_str or str(date_str).strip().lower() in ('null', ''):
        return None
    for fmt in date_formats:
        try:
            return datetime.strptime(str(date_str).strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Неизвестный формат даты: {date_str}")


def log_process(conn, process_name, start_time, end_time=None, status='STARTED',
                rows_processed=None, error_message=None):
    try:
        with conn.cursor() as cursor:
            query = """
                INSERT INTO logs.etl_logs 
                (process_name, start_time, end_time, status, rows_processed, error_message)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                process_name,
                start_time,
                end_time or datetime.now(),
                status,
                rows_processed,
                error_message
            ))
        conn.commit()
    except Exception as e:
        logger.error(f"Ошибка при логировании: {str(e)}")
        conn.rollback()


def create_table_mapping():
    return {
        'ft_balance_f': {
            'columns': ['on_date', 'account_rk', 'currency_rk', 'balance_out'],
            'types': ['date', 'numeric', 'numeric', 'float'],
            'pk': ['on_date', 'account_rk'],
            'truncate_before_load': True
        },
        'ft_posting_f': {
            'columns': ['oper_date', 'credit_account_rk', 'debet_account_rk',
                       'credit_amount', 'debet_amount'],
            'types': ['date', 'numeric', 'numeric', 'float', 'float'],
            'truncate_before_load': True
        },
        'md_account_d': {
            'columns': ['data_actual_date', 'data_actual_end_date', 'account_rk',
                       'account_number', 'char_type', 'currency_rk', 'currency_code'],
            'types': ['date', 'date', 'numeric', 'varchar', 'varchar', 'numeric', 'varchar'],
            'pk': ['data_actual_date', 'account_rk'],
            'truncate_before_load': True
        },
        'md_currency_d': {
            'columns': ['currency_rk', 'data_actual_date', 'data_actual_end_date',
                       'currency_code', 'code_iso_char'],
            'types': ['numeric', 'date', 'date', 'varchar', 'varchar'],
            'pk': ['currency_rk', 'data_actual_date'],
            'truncate_before_load': True
        },
        'md_exchange_rate_d': {
            'columns': ['data_actual_date', 'data_actual_end_date', 'currency_rk',
                       'reduced_cource', 'code_iso_num'],
            'types': ['date', 'date', 'numeric', 'float', 'varchar'],
            'pk': ['data_actual_date', 'currency_rk'],
            'truncate_before_load': True
        },
        'md_ledger_account_s': {
            'columns': ['chapter', 'chapter_name', 'section_number', 'section_name',
                       'subsection_name', 'ledger1_account', 'ledger1_account_name',
                       'ledger_account', 'ledger_account_name', 'characteristic',
                       'start_date', 'end_date'],
            'types': ['varchar', 'varchar', 'integer', 'varchar', 'varchar',
                      'integer', 'varchar', 'integer', 'varchar', 'varchar',
                       'date', 'date'],
            'pk': ['ledger_account', 'start_date'],
            'truncate_before_load': True
        }
    }


def prepare_row(row, table_info):
    prepared = {}
    for col in table_info['columns']:
        value = row.get(col)
        if value is None or str(value).strip() == '':
            prepared[col] = None
            continue
        col_type = table_info['types'][table_info['columns'].index(col)]
        if 'date' in col_type.lower():
            prepared[col] = parse_date(value)
        elif 'numeric' in col_type.lower() or 'integer' in col_type.lower():
            try:
                prepared[col] = float(value) if '.' in str(value) else int(value)
            except (ValueError, TypeError):
                prepared[col] = None
        elif 'float' in col_type.lower():
            try:
                prepared[col] = float(value)
            except (ValueError, TypeError):
                prepared[col] = None
        else:
            prepared[col] = str(value).strip()
    return prepared


def load_table(conn, table_name, csv_file, table_info):
    start_time = datetime.now()
    rows_processed = 0
    process_name = f"LOAD_{table_name}"

    try:
        log_process(conn, process_name, start_time, status='STARTED')

        if table_info.get('truncate_before_load', False):
            with conn.cursor() as cursor:
                cursor.execute(sql.SQL("TRUNCATE TABLE ds.{}").format(
                    sql.Identifier(table_name)
                ))
            logger.info(f"Таблица {table_name} очищена перед загрузкой")

        if not os.path.exists(csv_file):
            raise ETLError(f"Файл {csv_file} не найден")

        dialect, has_header = detect_csv_format(csv_file)

        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, dialect=dialect)
            if not reader.fieldnames:
                raise ETLError(f"Файл {csv_file} не содержит заголовков столбцов")

            required_columns = set(table_info['columns'])
            available_columns = set(reader.fieldnames)
            missing_columns = required_columns - available_columns
            if missing_columns:
                raise ETLError(f"В файле отсутствуют обязательные колонки: {missing_columns}")

            columns = [col for col in table_info['columns'] if col in reader.fieldnames]
            query = sql.SQL("INSERT INTO ds.{table} ({fields}) VALUES ({values})").format(
                table=sql.Identifier(table_name),
                fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
                values=sql.SQL(', ').join([sql.Placeholder()] * len(columns))
            )

            with conn.cursor() as cursor:
                batch = []
                for row in reader:
                    try:
                        prepared_row = prepare_row(row, table_info)
                        batch_values = [prepared_row.get(col) for col in columns]
                        batch.append(batch_values)
                        rows_processed += 1
                        if len(batch) >= 1000:
                            cursor.executemany(query, batch)
                            conn.commit()
                            batch = []
                    except Exception as e:
                        logger.error(f"Ошибка обработки строки {rows_processed + 1}: {str(e)}")
                        continue
                if batch:
                    cursor.executemany(query, batch)
                conn.commit()

        logger.info(f"Успешно загружено {rows_processed} строк в таблицу {table_name}")
        log_process(conn, process_name, start_time, datetime.now(), 'COMPLETED', rows_processed)

    except Exception as e:
        logger.error(f"Ошибка при загрузке таблицы {table_name}: {str(e)}")
        conn.rollback()
        log_process(conn, process_name, start_time, datetime.now(), 'FAILED', rows_processed, str(e))
        raise ETLError(f"Ошибка загрузки {table_name}") from e


def check_files_exist():
    missing_files = [f for f in CSV_FILES.values() if not os.path.exists(f)]
    if missing_files:
        error_msg = "Отсутствуют следующие файлы:\n" + "\n".join(missing_files)
        logger.error(error_msg)
        raise ETLError(error_msg)
    else:
        logger.info("Все CSV файлы найдены")


def main():
    overall_start = datetime.now()
    logger.info("Начало ETL-процесса")
    try:
        check_files_exist()
        conn = psycopg2.connect(**DB_CONFIG)
        try:
            log_process(conn, 'ETL_PROCESS', overall_start, status='STARTED')
            logger.info("Пауза 5 секунд...")
            time.sleep(5)
            table_mapping = create_table_mapping()
            for table_name, csv_file in CSV_FILES.items():
                if table_name in table_mapping:
                    logger.info(f"Загрузка данных в таблицу {table_name} из файла {csv_file}")
                    load_table(conn, table_name, csv_file, table_mapping[table_name])
                else:
                    logger.warning(f"Нет информации о таблице {table_name} в маппинге")
            log_process(conn, 'ETL_PROCESS', overall_start, datetime.now(), 'COMPLETED', None)
            logger.info(f"ETL-процесс успешно завершен за {datetime.now() - overall_start}")
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Критическая ошибка ETL-процесса: {str(e)}")
        raise


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.critical(f"Завершение работы с ошибкой: {str(e)}")
        exit(1)
