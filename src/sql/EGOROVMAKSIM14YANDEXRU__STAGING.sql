--DROP TABLE IF EXISTS EGOROVMAKSIM14YANDEXRU__STAGING.transactions CASCADE;
CREATE TABLE IF NOT EXISTS EGOROVMAKSIM14YANDEXRU__STAGING.transactions
    (operation_id varchar(60), 
    account_number_from int,
    account_number_to int,
    currency_code int,
    country varchar(30),
    status varchar(30),
    transaction_type varchar(30),
    amount int,
    transaction_dt timestamp)
    PARTITION BY transaction_dt::date;
   
   
CREATE PROJECTION EGOROVMAKSIM14YANDEXRU__STAGING.transactions_projection 
    AS SELECT * FROM EGOROVMAKSIM14YANDEXRU__STAGING.transactions
    ORDER BY transaction_dt
    SEGMENTED BY hash(operation_id,transaction_dt) ALL NODES;



--DROP TABLE IF EXISTS EGOROVMAKSIM14YANDEXRU__STAGING.currencies;   
CREATE TABLE IF NOT EXISTS EGOROVMAKSIM14YANDEXRU__STAGING.currencies
    (date_update timestamp, 
    currency_code int,
    currency_code_with int,
    currency_with_div numeric(5,3))
    PARTITION BY date_update::date;


CREATE PROJECTION EGOROVMAKSIM14YANDEXRU__STAGING.currencies
    AS SELECT * FROM EGOROVMAKSIM14YANDEXRU__STAGING.currencies
    ORDER BY date_update
    SEGMENTED BY hash(currency_code,date_update) ALL NODES;