CREATE SCHEMA IF NOT EXISTS DS;
CREATE SCHEMA IF NOT EXISTS LOGS;

-- Таблицы данных
CREATE TABLE IF NOT EXISTS DS.FT_BALANCE_F (
    on_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    currency_rk NUMERIC,
    balance_out FLOAT,
    PRIMARY KEY (on_date, account_rk)
);

CREATE TABLE IF NOT EXISTS DS.FT_POSTING_F (
    oper_date DATE NOT NULL,
    credit_account_rk NUMERIC NOT NULL,
    debet_account_rk NUMERIC NOT NULL,
    credit_amount FLOAT,
    debet_amount FLOAT
);

CREATE TABLE IF NOT EXISTS DS.MD_ACCOUNT_D (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    account_number VARCHAR(20) NOT NULL,
    char_type VARCHAR(1) NOT NULL,
    currency_rk NUMERIC NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    PRIMARY KEY (data_actual_date, account_rk)
);

CREATE TABLE IF NOT EXISTS DS.MD_CURRENCY_D (
    currency_rk NUMERIC NOT NULL,
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_code VARCHAR(3),
    code_iso_char VARCHAR(3),
    PRIMARY KEY (currency_rk, data_actual_date)
);

CREATE TABLE IF NOT EXISTS DS.MD_EXCHANGE_RATE_D (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_rk NUMERIC NOT NULL,
    reduced_cource FLOAT,
    code_iso_num VARCHAR(3),
    PRIMARY KEY (data_actual_date, currency_rk)
);

CREATE TABLE DS.MD_LEDGER_ACCOUNT_S (
    chapter                 VARCHAR(16),
    chapter_name            VARCHAR(255),
    section_number          INTEGER,
    section_name            VARCHAR(255),
    subsection_name         VARCHAR(255),
    ledger1_account         INTEGER,
    ledger1_account_name    VARCHAR(153),
    ledger_account          INTEGER NOT NULL,
    ledger_account_name     VARCHAR(255),
    characteristic          CHAR(1),
    start_date              DATE NOT NULL,
    end_date                DATE,
    PRIMARY KEY (ledger_account, start_date)
);

-- Таблица логов
CREATE TABLE IF NOT EXISTS LOGS.ETL_LOG (
    log_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(255),
    task_id VARCHAR(255),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(50),
    records_processed INT
);


