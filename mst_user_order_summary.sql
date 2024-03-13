/* 注文明細に対して、ユーザーごとに注文順の番号を振る */
WITH row_num_user_order AS (
    SELECT
        user_id,
        DATE(TIMESTAMP_SECONDS(created_at), "Asia/Tokyo") AS purchase_date,
        order_id,
        product_id,
        price,
        purchase_type,
        ROW_NUMBER() OVER(
            PARTITION BY
                user_id
            ORDER BY
                created_at
        ) AS order_sequence
    FROM order_details
),

/* 2回目の注文日に365日を足した日付を取得する（後続のLTVの計算用） */
second_date AS (
    SELECT
        user_id,
        --2回目の注文日に365日を足す
        DATE_ADD(purchase_date, INTERVAL 365 DAY) AS 365_days_after_second_date
    FROM row_num_user_order
    WHERE
        order_sequence = 2
),

/* 2回目の注文から365日以内かどうかのフラグを付与する */
add_flag_column AS (
    SELECT
        rn.*,
        sd.365_days_after_second_date,
        --2回目の注文日から365日以内であるかどうかでフラグ立て
        IF(rn.purchase_date <= sd.365_days_after_second_date, 1, 0) AS whithin_365_days_flag
    FROM row_num_user_order AS rn
    LEFT JOIN second_date AS sd
        ON rn.user_id = sd.user_id
),

/* ユーザーごとに注文の情報を横持ちにする */
pivot_data AS (
    SELECT
        user_id,
        MAX(CASE
            WHEN order_sequence = 1
                THEN purchase_date
            ELSE NULL
        END) AS first_purchase_date,
        MAX(CASE
            WHEN order_sequence = 2
                THEN purchase_date
            ELSE NULL
        END) AS second_purchase_date,
        MAX(CASE
            WHEN order_sequence = 2
                THEN purchase_type
            ELSE NULL
        END) AS second_purchase_type,
        COUNT(DISTINCT CASE
            WHEN order_sequence = 2
                THEN product_id
            ELSE NULL
        END) AS second_purchase_item_quantity,
        SUM(CASE
            WHEN order_sequence = 2
                THEN price
            ELSE NULL
        END) AS second_purchase_price,
        -- Repeat for each order up to 12
        MAX(CASE
            WHEN order_sequence = 12
                THEN purchase_date
            ELSE NULL
        END) AS twelfth_purchase_date,
        MAX(CASE
            WHEN order_sequence = 12
                THEN purchase_type
            ELSE NULL
        END) AS twelfth_purchase_type,
        COUNT(DISTINCT CASE
            WHEN order_sequence = 12
                THEN product_id
            ELSE NULL
        END) AS twelfth_purchase_item_quantity,
        SUM(CASE
            WHEN order_sequence = 12
                THEN price
            ELSE NULL
        END) AS twelfth_purchase_price,
        --注文が2回目以上　＆　2回目の注文日から365日以内　の注文の金額の合計
        SUM(IF(
            order_sequence > 1 AND whithin_365_days_flag = 1,
            price,
            NULL
        )) AS ltv_365
    FROM add_flag_column
    GROUP BY
        user_id
)

SELECT *
FROM pivot_data
;