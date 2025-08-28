-- Создание схем
CREATE SCHEMA IF NOT EXISTS DS;
CREATE SCHEMA IF NOT EXISTS LOGS;

-- Таблица для логов ETL-процессов
CREATE TABLE LOGS.etl_logs (
    log_id SERIAL PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    rows_processed INTEGER,
    error_message TEXT,
    duration INTERVAL GENERATED ALWAYS AS (end_time - start_time) STORED
);

COMMENT ON TABLE LOGS.etl_logs IS 'Логирование ETL-процессов';
COMMENT ON COLUMN LOGS.etl_logs.process_name IS 'Наименование процесса';
COMMENT ON COLUMN LOGS.etl_logs.start_time IS 'Время начала процесса';
COMMENT ON COLUMN LOGS.etl_logs.end_time IS 'Время окончания процесса';
COMMENT ON COLUMN LOGS.etl_logs.status IS 'Статус выполнения (STARTED, SUCCESS, ERROR, COMPLETED)';
COMMENT ON COLUMN LOGS.etl_logs.rows_processed IS 'Количество обработанных строк';
COMMENT ON COLUMN LOGS.etl_logs.error_message IS 'Сообщение об ошибке (если есть)';
COMMENT ON COLUMN LOGS.etl_logs.duration IS 'Продолжительность выполнения процесса';

-- Таблица балансов
CREATE TABLE DS.FT_BALANCE_F (
    on_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    currency_rk NUMERIC,
    balance_out FLOAT,
    PRIMARY KEY (on_date, account_rk)
);

COMMENT ON TABLE DS.FT_BALANCE_F IS 'Балансы счетов';
COMMENT ON COLUMN DS.FT_BALANCE_F.on_date IS 'Дата баланса';
COMMENT ON COLUMN DS.FT_BALANCE_F.account_rk IS 'Идентификатор счета';
COMMENT ON COLUMN DS.FT_BALANCE_F.currency_rk IS 'Идентификатор валюты';
COMMENT ON COLUMN DS.FT_BALANCE_F.balance_out IS 'Сумма остатка';

-- Таблица проводок
CREATE TABLE DS.FT_POSTING_F (
    oper_date DATE NOT NULL,
    credit_account_rk NUMERIC NOT NULL,
    debet_account_rk NUMERIC NOT NULL,
    credit_amount FLOAT,
    debet_amount FLOAT
);

COMMENT ON TABLE DS.FT_POSTING_F IS 'Проводки по счетам';
COMMENT ON COLUMN DS.FT_POSTING_F.oper_date IS 'Дата операции';
COMMENT ON COLUMN DS.FT_POSTING_F.credit_account_rk IS 'Идентификатор счета кредита';
COMMENT ON COLUMN DS.FT_POSTING_F.debet_account_rk IS 'Идентификатор счета дебета';
COMMENT ON COLUMN DS.FT_POSTING_F.credit_amount IS 'Сумма по кредиту';
COMMENT ON COLUMN DS.FT_POSTING_F.debet_amount IS 'Сумма по дебету';

-- Таблица счетов
CREATE TABLE DS.MD_ACCOUNT_D (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    account_number VARCHAR(20) NOT NULL,
    char_type VARCHAR(1) NOT NULL,
    currency_rk NUMERIC NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    PRIMARY KEY (data_actual_date, account_rk)
);

COMMENT ON TABLE DS.MD_ACCOUNT_D IS 'Справочник счетов';
COMMENT ON COLUMN DS.MD_ACCOUNT_D.data_actual_date IS 'Дата начала актуальности записи';
COMMENT ON COLUMN DS.MD_ACCOUNT_D.data_actual_end_date IS 'Дата окончания актуальности записи';
COMMENT ON COLUMN DS.MD_ACCOUNT_D.account_rk IS 'Идентификатор счета';
COMMENT ON COLUMN DS.MD_ACCOUNT_D.account_number IS 'Номер счета';
COMMENT ON COLUMN DS.MD_ACCOUNT_D.char_type IS 'Тип счета (А - активный, П - пассивный)';
COMMENT ON COLUMN DS.MD_ACCOUNT_D.currency_rk IS 'Идентификатор валюты';
COMMENT ON COLUMN DS.MD_ACCOUNT_D.currency_code IS 'Код валюты';

-- Таблица валют
CREATE TABLE DS.MD_CURRENCY_D (
    currency_rk NUMERIC NOT NULL,
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_code VARCHAR(3),
    code_iso_char VARCHAR(3),
    PRIMARY KEY (currency_rk, data_actual_date)
);

COMMENT ON TABLE DS.MD_CURRENCY_D IS 'Справочник валют';
COMMENT ON COLUMN DS.MD_CURRENCY_D.currency_rk IS 'Идентификатор валюты';
COMMENT ON COLUMN DS.MD_CURRENCY_D.data_actual_date IS 'Дата начала актуальности записи';
COMMENT ON COLUMN DS.MD_CURRENCY_D.data_actual_end_date IS 'Дата окончания актуальности записи';
COMMENT ON COLUMN DS.MD_CURRENCY_D.currency_code IS 'Код валюты';
COMMENT ON COLUMN DS.MD_CURRENCY_D.code_iso_char IS 'Код валюты по ISO';

-- Таблица курсов валют
CREATE TABLE DS.MD_EXCHANGE_RATE_D (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_rk NUMERIC NOT NULL,
    reduced_cource FLOAT,
    code_iso_num VARCHAR(3),
    PRIMARY KEY (data_actual_date, currency_rk)
);

COMMENT ON TABLE DS.MD_EXCHANGE_RATE_D IS 'Курсы валют';
COMMENT ON COLUMN DS.MD_EXCHANGE_RATE_D.data_actual_date IS 'Дата начала актуальности записи';
COMMENT ON COLUMN DS.MD_EXCHANGE_RATE_D.data_actual_end_date IS 'Дата окончания актуальности записи';
COMMENT ON COLUMN DS.MD_EXCHANGE_RATE_D.currency_rk IS 'Идентификатор валюты';
COMMENT ON COLUMN DS.MD_EXCHANGE_RATE_D.reduced_cource IS 'Курс валюты';
COMMENT ON COLUMN DS.MD_EXCHANGE_RATE_D.code_iso_num IS 'Цифровой код валюты по ISO';

-- Таблица балансовых счетов
CREATE TABLE DS.MD_LEDGER_ACCOUNT_S (
    chapter CHAR(1),
    chapter_name VARCHAR(16),
    section_number INTEGER,
    section_name VARCHAR(22),
    subsection_name VARCHAR(21),
    ledger1_account INTEGER,
    ledger1_account_name VARCHAR(47),
    ledger_account INTEGER NOT NULL,
    ledger_account_name VARCHAR(153),
    characteristic CHAR(1),
    is_resident INTEGER,
    is_reserve INTEGER,
    is_reserved INTEGER,
    is_loan INTEGER,
    is_reserved_assets INTEGER,
    is_overdue INTEGER,
    is_interest INTEGER,
    pair_account VARCHAR(5),
    start_date DATE NOT NULL,
    end_date DATE,
    is_rub_only INTEGER,
    min_term VARCHAR(1),
    min_term_measure VARCHAR(1),
    max_term VARCHAR(1),
    max_term_measure VARCHAR(1),
    ledger_acc_full_name_translit VARCHAR(1),
    is_revaluation INTEGER,
    is_correct INTEGER,
    PRIMARY KEY (ledger_account, start_date)
);

COMMENT ON TABLE DS.MD_LEDGER_ACCOUNT_S IS 'Справочник балансовых счетов';
COMMENT ON COLUMN DS.MD_LEDGER_ACCOUNT_S.chapter IS 'Глава';
COMMENT ON COLUMN DS.MD_LEDGER_ACCOUNT_S.chapter_name IS 'Наименование главы';
COMMENT ON COLUMN DS.MD_LEDGER_ACCOUNT_S.section_number IS 'Номер раздела';
COMMENT ON COLUMN DS.MD_LEDGER_ACCOUNT_S.section_name IS 'Наименование раздела';
COMMENT ON COLUMN DS.MD_LEDGER_ACCOUNT_S.subsection_name IS 'Наименование подраздела';
COMMENT ON COLUMN DS.MD_LEDGER_ACCOUNT_S.ledger1_account IS 'Счет первого порядка';
COMMENT ON COLUMN DS.MD_LEDGER_ACCOUNT_S.ledger1_account_name IS 'Наименование счета первого порядка';
COMMENT ON COLUMN DS.MD_LEDGER_ACCOUNT_S.ledger_account IS 'Балансовый счет';
COMMENT ON COLUMN DS.MD_LEDGER_ACCOUNT_S.ledger_account_name IS 'Наименование балансового счета';
COMMENT ON COLUMN DS.MD_LEDGER_ACCOUNT_S.characteristic IS 'Характеристика счета';
COMMENT ON COLUMN DS.MD_LEDGER_ACCOUNT_S.start_date IS 'Дата начала действия';
COMMENT ON COLUMN DS.MD_LEDGER_ACCOUNT_S.end_date IS 'Дата окончания действия';

-- Создание индексов для улучшения производительности
CREATE INDEX idx_ft_posting_f_oper_date ON DS.FT_POSTING_F(oper_date);
CREATE INDEX idx_ft_posting_f_credit_account ON DS.FT_POSTING_F(credit_account_rk);
CREATE INDEX idx_ft_posting_f_debet_account ON DS.FT_POSTING_F(debet_account_rk);

CREATE INDEX idx_md_account_d_account_rk ON DS.MD_ACCOUNT_D(account_rk);
CREATE INDEX idx_md_account_d_currency_rk ON DS.MD_ACCOUNT_D(currency_rk);

CREATE INDEX idx_md_currency_d_currency_code ON DS.MD_CURRENCY_D(currency_code);

CREATE INDEX idx_md_exchange_rate_d_currency_rk ON DS.MD_EXCHANGE_RATE_D(currency_rk);

-- Представление для удобного просмотра логов
CREATE OR REPLACE VIEW LOGS.v_etl_logs_report AS
SELECT
    process_name,
    start_time,
    end_time,
    status,
    rows_processed,
    duration,
    error_message
FROM LOGS.etl_logs
ORDER BY start_time DESC;

COMMENT ON VIEW LOGS.v_etl_logs_report IS 'Отчет по логам ETL-процессов';

-- Функция для очистки таблицы FT_POSTING_F
CREATE OR REPLACE FUNCTION DS.truncate_ft_posting_f()
RETURNS VOID AS $$
BEGIN
    TRUNCATE TABLE DS.FT_POSTING_F;
    INSERT INTO LOGS.etl_logs (process_name, start_time, end_time, status)
    VALUES ('TRUNCATE FT_POSTING_F', NOW(), NOW(), 'COMPLETED');
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION DS.truncate_ft_posting_f() IS 'Очистка таблицы проводок перед загрузкой новых данных';