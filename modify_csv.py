import csv
import random
from datetime import datetime
from config import setup_logging

logger = setup_logging()

def modify_csv_file(input_file, output_file=None, changes_count=3):
    if output_file is None:
        output_file = input_file.replace('.csv', '_modified.csv')

    try:
        logger.info(f"Модификация файла: {input_file}")

        with open(input_file, 'r', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            data = list(reader)

        if len(data) <= 1:
            logger.warning("Файл не содержит данных для модификации")
            return input_file

        changes_made = 0
        changes_log = []

        for _ in range(min(changes_count, len(data) - 1)):
            row_idx = random.randint(1, len(data) - 1)
            col_idx = random.randint(0, len(data[0]) - 1)

            original_value = data[row_idx][col_idx]

            if original_value and original_value.replace('.', '').replace('-', '').isdigit():
                new_value = str(float(original_value) + random.randint(100, 1000))
            else:
                new_value = f"MODIFIED_{datetime.now().strftime('%H%M%S')}"

            data[row_idx][col_idx] = new_value
            changes_made += 1
            changes_log.append(f"строка {row_idx}, колонка {col_idx}: {original_value} -> {new_value}")

        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(data)

        logger.info(f"Создан модифицированный файл: {output_file}")
        logger.info(f"Внесено изменений: {changes_made}")

        for change in changes_log:
            logger.info(f"Изменение: {change}")

        return output_file

    except Exception as e:
        logger.error(f"Ошибка при модификации CSV: {str(e)}")
        return input_file


def import_modified_file(csv_file, target_table):
    """Импорт модифицированного файла"""
    try:
        from csv_export_import import CSVManager
        from config import DB_CONFIG

        manager = CSVManager(DB_CONFIG)
        logger.info(f"Импорт модифицированного файла: {csv_file}")

        if manager.connect():
            with manager.connection.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {target_table}")
                manager.connection.commit()
            manager.disconnect()

        success = manager.import_from_csv(csv_file, target_table)

        if success:
            logger.info("Модифицированные данные успешно импортированы")
        else:
            logger.error("Ошибка импорта модифицированных данных")

        return success

    except Exception as e:
        logger.error(f"Ошибка при импорте модифицированного файла: {str(e)}")
        return False


def main():
    input_file = "dm_f101_round_f_export.csv"
    target_table = "dm.dm_f101_round_f_v2"

    logger.info("МОДИФИКАЦИЯ CSV ФАЙЛА")

    modified_file = modify_csv_file(input_file)

    logger.info("\nШАГ 2: Импорт модифицированных данных")
    import_modified_file(modified_file, target_table)

    logger.info("ПРОЦЕСС МОДИФИКАЦИИ ЗАВЕРШЕН")


if __name__ == "__main__":
    main()