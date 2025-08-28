CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate IN DATE)
LANGUAGE PLPGSQL
AS $$
DECLARE
  l_start_time TIMESTAMP := CLOCK_TIMESTAMP();
  l_end_time TIMESTAMP;
  l_records_processed INTEGER := 0;
  l_from_date DATE := DATE_TRUNC('MONTH', i_OnDate) - INTERVAL '1 MONTH';  --первый день месяца
  l_to_date DATE := DATE_TRUNC('MONTH', i_OnDate) - INTERVAL '1 DAY';      -- последний
BEGIN
  IF EXTRACT(DAY FROM i_OnDate) != 1 THEN
     RAISE EXCEPTION 'i_OnDate has to be the first day of a month, whereas % is not !', i_OnDate;
  END IF;

  DELETE FROM dm.dm_f101_round_f
  WHERE from_date = l_from_date AND to_date = l_to_date;

  -- вставка данных в целевую таблицу
  INSERT INTO dm.dm_f101_round_f (
    from_date,
    to_date,
    chapter,
    ledger_account,
    characteristic,
    balance_in_rub, balance_in_val, balance_in_total,
    turn_deb_rub, turn_deb_val, turn_deb_total,
    turn_cre_rub, turn_cre_val, turn_cre_total,
    balance_out_rub, balance_out_val, balance_out_total
  )

  WITH account_info AS (
    SELECT
      a.account_rk,
      SUBSTRING(a.account_number FROM 1 FOR 5) AS ledger_account,  -- первые 5 символов
      la.chapter,
      a.char_type AS characteristic,
      CASE WHEN a.currency_code IN ('810', '643') THEN 1 ELSE 0 END AS is_rub
    FROM ds.md_account_d a
    LEFT JOIN ds.md_ledger_account_s la ON SUBSTRING(a.account_number FROM 1 FOR 5)::INTEGER = la.ledger_account
  ),

  -- входящие остатки на день перед началом периода
  start_balances AS (
    SELECT
      ai.ledger_account,
      ai.chapter,
      ai.characteristic,
      SUM(CASE WHEN ai.is_rub = 1 THEN b.balance_out_rub ELSE 0 END) AS balance_in_rub,
      SUM(CASE WHEN ai.is_rub = 0 THEN b.balance_out_rub ELSE 0 END) AS balance_in_val,
      SUM(b.balance_out_rub) AS balance_in_total
    FROM dm.dm_account_balance_f b
    JOIN account_info ai ON b.account_rk = ai.account_rk
    WHERE b.on_date = l_from_date - INTERVAL '1 DAY'
    GROUP BY ai.ledger_account, ai.chapter, ai.characteristic
  ),

  -- обороты за отчетный период
  turnovers AS (
    SELECT
      ai.ledger_account,
      ai.chapter,
      ai.characteristic,
      SUM(CASE WHEN ai.is_rub = 1 THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_rub,
      SUM(CASE WHEN ai.is_rub = 0 THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_val,
      SUM(t.debet_amount_rub) AS turn_deb_total,
      SUM(CASE WHEN ai.is_rub = 1 THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_rub,
      SUM(CASE WHEN ai.is_rub = 0 THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_val,
      SUM(t.credit_amount_rub) AS turn_cre_total
    FROM dm.dm_account_turnover_f t
    JOIN account_info ai ON t.account_rk = ai.account_rk
    WHERE t.on_date BETWEEN l_from_date AND l_to_date
    GROUP BY ai.ledger_account, ai.chapter, ai.characteristic
  ),

  --остатки на конец отчетного периода
  end_balances AS (
    SELECT
      ai.ledger_account,
      ai.chapter,
      ai.characteristic,
      SUM(CASE WHEN ai.is_rub = 1 THEN b.balance_out_rub ELSE 0 END) AS balance_out_rub,
      SUM(CASE WHEN ai.is_rub = 0 THEN b.balance_out_rub ELSE 0 END) AS balance_out_val,
      SUM(b.balance_out_rub) AS balance_out_total
    FROM dm.dm_account_balance_f b
    JOIN account_info ai ON b.account_rk = ai.account_rk
    WHERE b.on_date = l_to_date
    GROUP BY ai.ledger_account, ai.chapter, ai.characteristic
  )

    SELECT
          l_from_date,
          l_to_date,
          sb.chapter,
          sb.ledger_account,
          sb.characteristic,
          balance_in_rub,
          balance_in_val,
          balance_in_total,

          turn_deb_rub,
          turn_deb_val,
          turn_deb_total,
          turn_cre_rub,
          turn_cre_val,
          turn_cre_total,

          balance_out_rub,
          balance_out_val,
          balance_out_total

    FROM start_balances sb JOIN end_balances eb ON sb.ledger_account = eb.ledger_account
    LEFT JOIN turnovers t ON sb.ledger_account = t.ledger_account
  ;
  INSERT INTO LOGS.ETL_LOGS(process_name, start_time, end_time, status, rows_processed)
  VALUES ('FILL_F101_ROUND_F for '||i_OnDate, l_start_time, CLOCK_TIMESTAMP(), 'ok', 0);
  COMMIT;
END;
$$;

CALL dm.fill_f101_round_f('2018-02-01');

SELECT * FROM LOGS.ETL_LOGS
ORDER BY log_id;

SELECT * FROM dm.dm_f101_round_f;