CREATE OR REPLACE PROCEDURE ds.p_fill_account_turnover(in_date IN DATE)
LANGUAGE PLPGSQL
AS $$
  DECLARE rec RECORD;
  l_account_cnt INTEGER = 0;
  l_start_time TIMESTAMP = LOCALTIMESTAMP;
BEGIN
  DELETE FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = in_date;

  FOR rec IN
    WITH credit AS (
        SELECT credit_account_rk AS account_rk, SUM(credit_amount) AS credit_sum
        FROM DS.FT_POSTING_F
        WHERE oper_date = in_date
        GROUP BY credit_account_rk
    ),
    debet AS (
        SELECT debet_account_rk AS account_rk, SUM(debet_amount) AS debet_sum
        FROM DS.FT_POSTING_F
        WHERE oper_date = in_date
        GROUP BY debet_account_rk
    )
    SELECT
      a.account_rk,
      a.currency_rk,
      a.currency_code,
      credit.credit_sum,
      credit.credit_sum * ds.f_getrate(a.currency_rk, in_date) AS credit_sum_rub,
      debet.debet_sum,
      debet.debet_sum * ds.f_getrate(a.currency_rk, in_date) AS debet_sum_rub
    FROM DS.MD_ACCOUNT_D a LEFT JOIN credit ON a.account_rk = credit.account_rk
    LEFT JOIN debet ON a.account_rk = debet.account_rk
    ORDER BY 1
  LOOP
    --хотя бы одна сумма не null
    IF COALESCE(rec.credit_sum, rec.debet_sum, -1) > 0 THEN
      --raise notice '%: credit = %, debet = %', rec.account_rk, rec.credit_sum, rec.debet_sum;
      INSERT INTO DM.DM_ACCOUNT_TURNOVER_F(on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
      VALUES (in_date, rec.account_rk, rec.credit_sum, rec.credit_sum_rub, rec.debet_sum, rec.debet_sum_rub);
      l_account_cnt = l_account_cnt + 1;
    END IF;
  END LOOP;
  INSERT INTO LOGS.ETL_LOGS(process_name, start_time, end_time, status, rows_processed)
  VALUES ('P_FILL_ACCOUNT_TURNOVER', l_start_time, LOCALTIMESTAMP, 'ok', l_account_cnt);
  COMMIT;
END;
$$;