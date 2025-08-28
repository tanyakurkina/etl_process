import csv
import psycopg2
from config import DB_CONFIG, setup_logging

logger = setup_logging()


class CSVManager:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.db_config)
            logger.info("Успешное подключение к базе данных")
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {str(e)}")
            return False

    def disconnect(self):
        if self.connection:
            self.connection.close()
            logger.info("Отключение от базы данных")

    def export_to_csv(self, table_name, output_file):
        try:
            if not self.connect():
                return False

            logger.info(f"Начало экспорта данных из таблицы {table_name}")

            with self.connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table_name}")
                rows = cursor.fetchall()

                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name.split('.')[-1]}' 
                    AND table_schema = '{table_name.split('.')[0]}'
                    ORDER BY ordinal_position
                """)
                columns = [row[0] for row in cursor.fetchall()]

                with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile, delimiter=',', quotechar='"',
                                        quoting=csv.QUOTE_MINIMAL)

                    writer.writerow(columns)

                    for row in rows:
                        writer.writerow(row)

                logger.info(f"Успешно экспортировано {len(rows)} строк в файл {output_file}")
                return True

        except Exception as e:
            logger.error(f"Ошибка при экспорте данных: {str(e)}")
            return False
        finally:
            self.disconnect()

    def create_table_copy(self, original_table, new_table):
        try:
            if not self.connect():
                return False

            logger.info(f"Создание копии таблицы {original_table} -> {new_table}")

            with self.connection.cursor() as cursor:
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {new_table} 
                    (LIKE {original_table} INCLUDING ALL)
                """)

                cursor.execute(f"TRUNCATE TABLE {new_table}")

                self.connection.commit()
                logger.info(f"Таблица {new_table} успешно создана")
                return True

        except Exception as e:
            logger.error(f"Ошибка при создании копии таблицы: {str(e)}")
            self.connection.rollback()
            return False
        finally:
            self.disconnect()

    def import_from_csv(self, csv_file, target_table):
        try:
            logger.info(f"Начало импорта данных из {csv_file} в таблицу {target_table}")

            rows_to_insert = []
            with open(csv_file, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                columns = next(reader)

                for row in reader:
                    processed_row = [None if cell == '' else cell for cell in row]
                    rows_to_insert.append(processed_row)

            if not self.connect():
                return False

            with self.connection.cursor() as cursor:
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join([f'"{col}"' for col in columns])

                query = f"""
                    INSERT INTO {target_table} ({columns_str})
                    VALUES ({placeholders})
                """

                cursor.executemany(query, rows_to_insert)
                self.connection.commit()

                logger.info(f"Успешно импортировано {len(rows_to_insert)} строк в таблицу {target_table}")
                return True

        except Exception as e:
            logger.error(f"Ошибка при импорте данных: {str(e)}")
            if self.connection:
                self.connection.rollback()
            return False
        finally:
            self.disconnect()


def main():
    manager = CSVManager(DB_CONFIG)

    original_table = "dm.dm_f101_round_f"
    copied_table = "dm.dm_f101_round_f_v2"
    csv_file = "dm_f101_round_f_export.csv"

    logger.info("=" * 50)
    logger.info("ЭКСПОРТ/ИМПОРТ ДАННЫХ В CSV")
    logger.info("=" * 50)

    logger.info("ШАГ 1: Экспорт данных в CSV")
    if manager.export_to_csv(original_table, csv_file):
        logger.info("Экспорт завершен успешно")
    else:
        logger.error("Ошибка экспорта")
        return

    logger.info("ШАГ 2: Создание копии таблицы")
    if manager.create_table_copy(original_table, copied_table):
        logger.info("Копия таблицы создана успешно")
    else:
        logger.error(" Ошибка создания копии таблицы")
        return

    logger.info("ШАГ 3: Импорт данных из CSV")
    if manager.import_from_csv(csv_file, copied_table):
        logger.info("Импорт завершен успешно")
    else:
        logger.error("Ошибка импорта")
        return

    logger.info("=" * 50)
    logger.info("ПРОЦЕСС ЗАВЕРШЕН")
    logger.info("=" * 50)

    print(f"\nФайл CSV создан: {csv_file}")
    print("Вы можете изменить данные в CSV и запустить:")
    print("   python modify_csv.py")


if __name__ == "__main__":
    main()