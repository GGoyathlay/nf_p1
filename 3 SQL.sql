CREATE TABLE DM.DM_F101_ROUND_F (
    FROM_DATE DATE NOT NULL, -- Первый день отчетного периода
    TO_DATE DATE NOT NULL, -- Последний день отчетного периода
    CHAPTER CHAR(1), -- Глава из справочника балансовых счетов
    LEDGER_ACCOUNT CHAR(5), -- Балансовый счет второго порядка
    CHARACTERISTIC CHAR(1) NOT NULL, -- Характеристика счета
    BALANCE_IN_RUB NUMERIC(23, 8) DEFAULT 0, -- Остатки в рублях на начало периода
    BALANCE_IN_VAL NUMERIC(23, 8) DEFAULT 0, -- Остатки в валюте на начало периода
    BALANCE_IN_TOTAL NUMERIC(23, 8) DEFAULT 0, -- Общие остатки на начало периода
    TURN_DEB_RUB NUMERIC(23, 8) DEFAULT 0, -- Дебетовые обороты в рублях
    TURN_DEB_VAL NUMERIC(23, 8) DEFAULT 0, -- Дебетовые обороты в валюте
    TURN_DEB_TOTAL NUMERIC(23, 8) DEFAULT 0, -- Общие дебетовые обороты
    TURN_CRE_RUB NUMERIC(23, 8) DEFAULT 0, -- Кредитовые обороты в рублях
    TURN_CRE_VAL NUMERIC(23, 8) DEFAULT 0, -- Кредитовые обороты в валюте
    TURN_CRE_TOTAL NUMERIC(23, 8) DEFAULT 0, -- Общие кредитовые обороты
    BALANCE_OUT_RUB NUMERIC(23, 8) DEFAULT 0, -- Остатки в рублях на конец периода
    BALANCE_OUT_VAL NUMERIC(23, 8) DEFAULT 0, -- Остатки в валюте на конец периода
    BALANCE_OUT_TOTAL NUMERIC(23, 8) DEFAULT 0 -- Общие остатки на конец периода
);

-- Процедура расчета витрины 101 формы
CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql AS $$
DECLARE
    v_FromDate DATE := i_OnDate - INTERVAL '1 month'; -- Первый день отчетного периода
    v_ToDate DATE := i_OnDate - INTERVAL '1 day';    -- Последний день отчетного периода
BEGIN
    -- Логирование начала расчета
    INSERT INTO logs.process_log (process_name, log_date, status, details) 
    VALUES ('fill_f101_round_f', CURRENT_TIMESTAMP, 'START', 'Начало расчета витрины 101 формы за дату: ' || i_OnDate);

    -- Удаление старых данных за дату расчета
    DELETE FROM DM.DM_F101_ROUND_F WHERE FROM_DATE = v_FromDate AND TO_DATE = v_ToDate;

    -- Вставка новых данных в витрину
    INSERT INTO DM.DM_F101_ROUND_F (
        FROM_DATE, TO_DATE, CHAPTER, LEDGER_ACCOUNT, CHARACTERISTIC,
        BALANCE_IN_RUB, BALANCE_IN_VAL, BALANCE_IN_TOTAL,
        TURN_DEB_RUB, TURN_DEB_VAL, TURN_DEB_TOTAL,
        TURN_CRE_RUB, TURN_CRE_VAL, TURN_CRE_TOTAL,
        BALANCE_OUT_RUB, BALANCE_OUT_VAL, BALANCE_OUT_TOTAL
    )
    SELECT
        v_FromDate AS FROM_DATE,  -- Первый день отчетного периода
        v_ToDate AS TO_DATE,      -- Последний день отчетного периода
        l.chapter,                -- Глава из справочника балансовых счетов
        LEFT(a.account_number, 5) AS LEDGER_ACCOUNT, -- Балансовый счет второго порядка
        a.char_type AS CHARACTERISTIC, -- Характеристика счета

        -- Остатки на начало периода
        SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS BALANCE_IN_RUB,
        SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS BALANCE_IN_VAL,
        SUM(b.balance_out_rub) AS BALANCE_IN_TOTAL,

        -- Дебетовые обороты за период
        SUM(CASE WHEN a.currency_code IN ('810', '643') THEN COALESCE(t.debet_amount_rub, 0) ELSE 0 END) AS TURN_DEB_RUB,
        SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN COALESCE(t.debet_amount_rub, 0) ELSE 0 END) AS TURN_DEB_VAL,
        SUM(COALESCE(t.debet_amount_rub, 0)) AS TURN_DEB_TOTAL,

        -- Кредитовые обороты за период
        SUM(CASE WHEN a.currency_code IN ('810', '643') THEN COALESCE(t.credit_amount_rub, 0) ELSE 0 END) AS TURN_CRE_RUB,
        SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN COALESCE(t.credit_amount_rub, 0) ELSE 0 END) AS TURN_CRE_VAL,
        SUM(COALESCE(t.credit_amount_rub, 0)) AS TURN_CRE_TOTAL,

        -- Остатки на конец периода
        SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b_end.balance_out_rub ELSE 0 END) AS BALANCE_OUT_RUB,
        SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b_end.balance_out_rub ELSE 0 END) AS BALANCE_OUT_VAL,
        SUM(b_end.balance_out_rub) AS BALANCE_OUT_TOTAL
    FROM DS.MD_ACCOUNT_D a
    LEFT JOIN DS.MD_LEDGER_ACCOUNT_S l ON LEFT(a.account_number, 5)::char = l.ledger_account::char
    LEFT JOIN DM.DM_ACCOUNT_BALANCE_F b ON b.on_date = v_FromDate - INTERVAL '1 day' AND b.account_rk = a.account_rk
    LEFT JOIN DM.DM_ACCOUNT_TURNOVER_F t ON t.on_date BETWEEN v_FromDate AND v_ToDate AND t.account_rk = a.account_rk
    LEFT JOIN DM.DM_ACCOUNT_BALANCE_F b_end ON b_end.on_date = v_ToDate AND b_end.account_rk = a.account_rk
    WHERE a.data_actual_date <= v_ToDate
      AND (a.data_actual_end_date IS NULL OR a.data_actual_end_date >= v_FromDate)
    GROUP BY l.chapter, LEFT(a.account_number, 5), a.char_type;

    -- Логирование завершения расчета
    INSERT INTO logs.process_log (process_name, log_date, status, details) 
    VALUES ('fill_f101_round_f', CURRENT_TIMESTAMP, 'END', 'Завершение расчета витрины 101 формы за дату: ' || i_OnDate);
END;
$$;

CALL dm.fill_f101_round_f('2018-02-01');



