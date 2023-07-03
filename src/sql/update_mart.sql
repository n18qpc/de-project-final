                    INSERT INTO EGOROVMAKSIM14YANDEXRU__DWH.global_metrics
                        (date_update, currency_from, amount_total, cnt_transactions, 
                        avg_transactions_per_account, cnt_accounts_make_transactions)
                    WITH cte AS ( -- код валюты, отношение валюты к доллару(430) и диапазон актуальности данных
                         SELECT  currency_code,
                              currency_code_with,
                              currency_with_div as div_to_usd,
                              date_update as effective_from,
                              COALESCE(LAG(date_update, 1) 
                                OVER (PARTITION BY currency_code ORDER BY date_update), '9999-12-31') as effective_to
                         FROM EGOROVMAKSIM14YANDEXRU__STAGING.currencies
                         WHERE currency_code_with = 430
                         ORDER BY currency_code),
                    table_accounts_transactions AS ( -- выборка кодов валюты и уникальных 
                         SELECT 
                              currency_code,
                              transaction_dt::date as transaction_day, 
                              COUNT(distinct account_number_from) as cnt_accounts_make_transactions
                         FROM EGOROVMAKSIM14YANDEXRU__STAGING.transactions
                         GROUP BY currency_code, transaction_dt::date
                    )    
                    SELECT 
                         chosen_day::DATE as date_update, --DAG.get_run_dates.start_date -- дата расчёта
                         transactions.currency_code as currency_from, --код валюты транзакции
                         SUM(amount*div_to_usd) as amount_total, --общая сумма транзакций по валюте в долларах
                         COUNT(transactions.currency_code) as cnt_transactions, --общий объём транзакций по валюте
                         SUM(amount*div_to_usd)/max(cnt_accounts_make_transactions) as avg_transactions_per_account, 
                         --средний объём транзакций с аккаунта
                         max(cnt_accounts_make_transactions) as cnt_accounts_make_transactions 
                         -- количество уникальных аккаунтов с совершёнными транзакциями по валюте
                    FROM
                         EGOROVMAKSIM14YANDEXRU__STAGING.transactions
                    INNER JOIN cte
                    ON transactions.currency_code = cte.currency_code
                         AND transaction_dt BETWEEN cte.effective_from and effective_to 
                         -- Объединяем по коду валюты и дате проведения транзакции и курса валюты
                    INNER JOIN table_accounts_transactions
                    ON transactions.currency_code = table_accounts_transactions.currency_code 
                        and transactions.transaction_dt::date = table_accounts_transactions.transaction_day
                    WHERE 1=1
                    AND account_number_from != '-1' -- убираем тестовые аккаутны
                    AND account_number_to != '-1' -- убираем тестовые аккаутны
                    AND transaction_dt::date = chosen_day::DATE - INTERVAL '1 DAY' -- транзакции за предыдущий день
                    GROUP BY transaction_dt::date, transactions.currency_code;