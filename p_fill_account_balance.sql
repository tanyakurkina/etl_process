CREATE OR REPLACE PROCEDURE ds.p_fill_account_balance(in_date IN DATE)
LANGUAGE PLPGSQL
AS $$
  DECLARE
  rec RECORD;
  l_account_cnt INTEGER = 0;
  l_start_time TIMESTAMP = LOCALTIMESTAMP;
  l_prev_balance_rec RECORD;
  l_turnover_rec RECORD;
BEGIN
  DELETE FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = in_date;

  IF in_date = '2017-12-31' THEN
    INSERT INTO DM.DM_ACCOUNT_BALANCE_F(on_date, account_rk, balance_out, balance_out_rub)
    SELECT b.on_date, b.account_rk, b.balance_out, ROUND((b.balance_out * ds.f_getrate(b.currency_rk, b.on_date))::numeric, 2)
    FROM DS.FT_BALANCE_F b JOIN DS.MD_ACCOUNT_D a ON b.account_rk = a.account_rk
    WHERE b.on_date = in_date;
  ELSE
    FOR rec IN
      SELECT account_rk, char_type
      FROM DS.MD_ACCOUNT_D
      WHERE in_date BETWEEN data_actual_date AND data_actual_end_date
      ORDER BY 1
    LOOP
      --условие: баланс за предыдущий день должен уже быть
      SELECT balance_out, balance_out_rub
      INTO l_prev_balance_rec
      FROM DM.DM_ACCOUNT_BALANCE_F
      WHERE on_date = in_date - INTERVAL '1' DAY
      AND account_rk = rec.account_rk;
      IF l_prev_balance_rec.balance_out IS NULL THEN
        RAISE EXCEPTION 'There is no balance for account_rk = % and on_date = %', rec.account_rk, in_date - INTERVAL '1' DAY;
      END IF;
      --turnover для in_date
      SELECT
        account_rk,
        debet_amount,
        debet_amount_rub,
        credit_amount,
        credit_amount_rub
      INTO l_turnover_rec
      FROM DM.DM_ACCOUNT_TURNOVER_F
      WHERE account_rk = rec.account_rk AND on_date = in_date;

      l_turnover_rec.debet_amount = COALESCE(l_turnover_rec.debet_amount, 0);
      l_turnover_rec.debet_amount_rub = COALESCE(l_turnover_rec.debet_amount_rub, 0);
      l_turnover_rec.credit_amount = COALESCE(l_turnover_rec.credit_amount, 0);
      l_turnover_rec.credit_amount_rub = COALESCE(l_turnover_rec.credit_amount_rub, 0);

      IF rec.char_type = 'А' THEN
         INSERT INTO DM.DM_ACCOUNT_BALANCE_F(on_date, account_rk, balance_out, balance_out_rub)
         VALUES (
           in_date,
           rec.account_rk,
           ROUND((l_prev_balance_rec.balance_out + l_turnover_rec.debet_amount - l_turnover_rec.credit_amount)::numeric, 2),
           ROUND((l_prev_balance_rec.balance_out_rub + l_turnover_rec.debet_amount_rub - l_turnover_rec.credit_amount_rub)::numeric, 2)
         );
      ELSIF rec.char_type = 'П' THEN
         INSERT INTO DM.DM_ACCOUNT_BALANCE_F(on_date, account_rk, balance_out, balance_out_rub)
         VALUES (
           in_date,
           rec.account_rk,
           ROUND((l_prev_balance_rec.balance_out - l_turnover_rec.debet_amount + l_turnover_rec.credit_amount)::numeric, 2),
           ROUND((l_prev_balance_rec.balance_out_rub - l_turnover_rec.debet_amount_rub + l_turnover_rec.credit_amount_rub)::numeric, 2)
         );
      ELSE
        RAISE EXCEPTION 'error char_type = "%" for account_rk = %?', rec.char_type, rec.account_rk;
      END IF;
    END LOOP;

  END IF;
  INSERT INTO LOGS.ETL_LOGS(process_name, start_time, end_time, status, rows_processed)
  VALUES ('P_FILL_ACCOUNT_BALANCE for '||in_date, l_start_time, LOCALTIMESTAMP, 'ok', 0);
  COMMIT;
END;
$$;