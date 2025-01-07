-- Создаем схему DM, если она еще не существует
CREATE SCHEMA IF NOT EXISTS DM;
-- Создаем таблицу DM.DM_ACCOUNT_TURNOVER_F
-- Эта таблица содержит данные об оборотах по лицевым счетам в разрезе дней
CREATE TABLE DM.DM_ACCOUNT_TURNOVER_F (
    on_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    credit_amount NUMERIC(23,8),
    credit_amount_rub NUMERIC(23,8),
    debet_amount NUMERIC(23,8),
    debet_amount_rub NUMERIC(23,8),
    PRIMARY KEY (on_date, account_rk)
);
-- Создаем таблицу DM.DM_ACCOUNT_BALANCE_F
-- Эта таблица хранит информацию об остатках по лицевым счетам в разрезе дней
CREATE TABLE DM.DM_ACCOUNT_BALANCE_F (
    on_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    balance_out NUMERIC(23,8),
    balance_out_rub NUMERIC(23,8),
    PRIMARY KEY (on_date, account_rk)
);
-- Создаем таблицы логов в схеме logs
CREATE TABLE logs.process_log (
    log_id SERIAL PRIMARY KEY,              -- Уникальный идентификатор лога
    process_name VARCHAR(255) NOT NULL,     -- Название процесса
    log_date TIMESTAMP NOT NULL,            -- Дата и время записи
    status VARCHAR(50) NOT NULL,            -- Статус процесса
    details TEXT                            -- Дополнительные детали (необязательно)
);

-- Вставляем данные в витрину остатков DM_ACCOUNT_BALANCE_F за 31 декабря 2017 года
-- Используем данные из таблицы DS.FT_BALANCE_F и курсы валют из DS.MD_EXCHANGE_RATE_D
INSERT INTO DM.DM_ACCOUNT_BALANCE_F (on_date, account_rk, balance_out, balance_out_rub)
SELECT 
    DATE '2017-12-31' AS on_date, -- Указываем дату расчета
    account_rk,                   -- Уникальный идентификатор лицевого счета
    balance_out,                  -- Остаток по счету в валюте счета
    balance_out * COALESCE(
        (SELECT reduced_cource    -- Извлекаем курс валюты за указанную дату
         FROM DS.MD_EXCHANGE_RATE_D 
         WHERE data_actual_date <= DATE '2017-12-31' 
         AND data_actual_end_date >= DATE '2017-12-31'
         AND currency_rk = ft.currency_rk 
         ORDER BY data_actual_date DESC
         LIMIT 1), 
        1)                        -- Если курс отсутствует, используем 1 (для валюты счета, равной рублям)
        AS balance_out_rub        -- Остаток по счету, пересчитанный в рубли
FROM DS.FT_BALANCE_F ft;            -- Источник данных об остатках

CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql AS $$
BEGIN
    -- Логирование начала расчета
    INSERT INTO logs.process_log (process_name, log_date, status, details) 
    VALUES ('fill_account_turnover_f', NOW(), 'START', 'Начало расчета витрины оборотов за дату: ' || i_OnDate);

    -- Удаляем старые данные за указанную дату
    DELETE FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate;

    -- Заполняем витрину оборотов
    INSERT INTO DM.DM_ACCOUNT_TURNOVER_F (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
    SELECT 
        i_OnDate AS on_date,  -- Указываем дату расчета
        COALESCE(credit_account_rk, debet_account_rk) AS account_rk,  -- Уникальный идентификатор счета
        SUM(CASE 
                WHEN credit_account_rk IS NOT NULL THEN credit_amount  -- Если проводка по кредиту
                ELSE 0 
            END) AS credit_amount,  -- Сумма оборотов по кредиту
        SUM(CASE 
                WHEN credit_account_rk IS NOT NULL THEN credit_amount * COALESCE(
                    (SELECT reduced_cource 
                     FROM DS.MD_EXCHANGE_RATE_D 
                     WHERE data_actual_date <= i_OnDate  -- Используем data_actual_date для актуальности
                     AND (data_actual_end_date IS NULL OR data_actual_end_date >= i_OnDate)  -- Проверка на конец актуальности
                     AND currency_code = (SELECT currency_code 
                                           FROM DS.MD_ACCOUNT_D 
                                           WHERE account_rk = credit_account_rk) 
                     LIMIT 1), 
                    1)
                ELSE 0 
            END) AS credit_amount_rub,  -- Сумма оборотов по кредиту в рублях
        SUM(CASE 
                WHEN debet_account_rk IS NOT NULL THEN debet_amount  -- Если проводка по дебету
                ELSE 0 
            END) AS debet_amount,  -- Сумма оборотов по дебету
        SUM(CASE 
                WHEN debet_account_rk IS NOT NULL THEN debet_amount * COALESCE(
                    (SELECT reduced_cource 
                     FROM DS.MD_EXCHANGE_RATE_D 
                     WHERE data_actual_date <= i_OnDate  -- Используем data_actual_date для актуальности
                     AND (data_actual_end_date IS NULL OR data_actual_end_date >= i_OnDate)  -- Проверка на конец актуальности
                     AND currency_code = (SELECT currency_code 
                                           FROM DS.MD_ACCOUNT_D 
                                           WHERE account_rk = debet_account_rk) 
                     LIMIT 1), 
                    1)
                ELSE 0 
            END) AS debet_amount_rub  -- Сумма оборотов по дебету в рублях
    FROM DS.FT_POSTING_F f
    LEFT JOIN DS.MD_ACCOUNT_D acc ON f.credit_account_rk = acc.account_rk OR f.debet_account_rk = acc.account_rk  -- Подключаем таблицу счетов для получения валюты счета
    WHERE f.oper_date = i_OnDate  -- Обороты только за указанную дату
    AND EXISTS (
        SELECT 1
        FROM DS.MD_ACCOUNT_D acc_inner
        WHERE (f.credit_account_rk = acc_inner.account_rk OR f.debet_account_rk = acc_inner.account_rk)
        AND acc_inner.data_actual_date <= i_OnDate  -- Проверяем, что счет был активен на указанную дату
        AND (acc_inner.data_actual_end_date IS NULL OR acc_inner.data_actual_end_date >= i_OnDate)  -- Учитываем конец актуальности
    )
    GROUP BY COALESCE(f.credit_account_rk, f.debet_account_rk);  -- Группируем по уникальному идентификатору счета

    -- Логирование завершения расчета
    INSERT INTO logs.process_log (process_name, log_date, status, details) 
    VALUES ('fill_account_turnover_f', NOW(), 'END', 'Завершение расчета витрины оборотов за дату: ' || i_OnDate);
END;
$$;


DO $$ 
DECLARE
    i INTEGER;
BEGIN
    -- Выполняем процедуру расчета оборотов за каждый день января 2018 года
    FOR i IN 0..30 LOOP
        CALL ds.fill_account_turnover_f(DATE '2018-01-01' + i); -- Передаем дату в процедуру
    END LOOP;
END $$;


-- Создание процедуры ds.fill_account_balance_f
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Логирование начала выполнения
    INSERT INTO logs.process_log (process_name, log_date, status, details)
    VALUES ('fill_account_balance_f', NOW(), 'START', 'Начало расчета витрины остатков за дату: ' || i_OnDate);

    -- Удаление старых данных за дату расчета
    DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate;

    -- Расчет остатков
    INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
    SELECT
        i_OnDate AS on_date,
        acc.account_rk,
        CASE 
            WHEN acc.char_type = 'А' THEN COALESCE(prev.balance_out, 0) + COALESCE(turnover.debet_amount, 0) - COALESCE(turnover.credit_amount, 0)
            WHEN acc.char_type = 'П' THEN COALESCE(prev.balance_out, 0) - COALESCE(turnover.debet_amount, 0) + COALESCE(turnover.credit_amount, 0)
        END AS balance_out,
        CASE 
            WHEN acc.char_type = 'А' THEN COALESCE(prev.balance_out_rub, 0) + COALESCE(turnover.debet_amount_rub, 0) - COALESCE(turnover.credit_amount_rub, 0)
            WHEN acc.char_type = 'П' THEN COALESCE(prev.balance_out_rub, 0) - COALESCE(turnover.debet_amount_rub, 0) + COALESCE(turnover.credit_amount_rub, 0)
        END AS balance_out_rub
    FROM 
        ds.md_account_d acc
    LEFT JOIN dm.dm_account_balance_f prev
        ON acc.account_rk = prev.account_rk AND prev.on_date = i_OnDate - INTERVAL '1 day'
    LEFT JOIN dm.dm_account_turnover_f turnover
        ON acc.account_rk = turnover.account_rk AND turnover.on_date = i_OnDate
    WHERE 
        i_OnDate >= acc.data_actual_date AND i_OnDate <= acc.data_actual_end_date;

    -- Логирование завершения выполнения
    INSERT INTO logs.process_log (process_name, log_date, status, details)
    VALUES ('fill_account_balance_f', NOW(), 'END', 'Завершение расчета витрины остатков за дату: ' || i_OnDate);
END;
$$;

-- Запуск процедуры для всех дней января 2018 года
DO $$
DECLARE
    calc_date DATE := '2018-01-01';
BEGIN
    WHILE calc_date <= '2018-01-31' LOOP
        CALL ds.fill_account_balance_f(calc_date);
        calc_date := calc_date + INTERVAL '1 day';
    END LOOP;
END;
$$;