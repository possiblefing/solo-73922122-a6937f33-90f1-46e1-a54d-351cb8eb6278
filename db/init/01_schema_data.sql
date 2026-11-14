-- =====================================================================
-- Fictional point-of-sale data + two read-only query templates.
-- Idempotent: safe to run against a freshly initialized data volume.
-- =====================================================================

DROP TABLE IF EXISTS sales CASCADE;
DROP TABLE IF EXISTS query_template CASCADE;

CREATE TABLE sales (
    sale_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_id     INTEGER      NOT NULL,
    sale_date    DATE         NOT NULL,
    sku          INTEGER      NOT NULL,
    qty          INTEGER      NOT NULL,
    unit_price   NUMERIC(10,2) NOT NULL,
    amount       NUMERIC(12,2) NOT NULL,
    channel      TEXT         NOT NULL
);

-- 200 stores x 100 days x 84 sales = 1,680,000 rows (~ a few hundred MB),
-- big enough that an unindexed aggregation over the whole table is slow.
INSERT INTO sales (store_id, sale_date, sku, qty, unit_price, amount, channel)
SELECT
    (g % 200) + 1                                   AS store_id,
    CURRENT_DATE - ((g / 200) % 100)                AS sale_date,
    (g % 400) + 1                                   AS sku,
    1 + (g % 9)                                     AS qty,
    ROUND((5 + (g % 95)) + 0.49, 2)                 AS unit_price,
    ROUND((1 + (g % 9)) * ((5 + (g % 95)) + 0.49), 2) AS amount,
    CASE (g % 3) WHEN 0 THEN 'store' WHEN 1 THEN 'app' ELSE 'mini_program' END
FROM generate_series(1, 200 * 100 * 84) AS g;

-- Supports the fast daily-summary template (index scan on store + date).
CREATE INDEX idx_sales_store_date ON sales (store_id, sale_date);
ANALYZE sales;

-- ---------------------------------------------------------------------
-- Query template catalog.  The web app loads these by name and binds the
-- %(store)s parameter with psycopg.  Both statements are read-only SELECTs.
-- ---------------------------------------------------------------------
CREATE TABLE query_template (
    name        TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    sql_text    TEXT NOT NULL
);

-- Q1: indexed, single-store / single-day settlement summary (fast, <50ms).
INSERT INTO query_template (name, description, sql_text) VALUES (
    'q1_daily_summary',
    '单店单日销售汇总（走 store_id + sale_date 索引，快速）',
$f$
SELECT
    s.store_id,
    s.sale_date,
    COUNT(*)            AS ticket_lines,
    COALESCE(SUM(s.qty), 0)    AS items_sold,
    COALESCE(SUM(s.amount), 0) AS gross_amount
FROM sales AS s
WHERE s.store_id = %(store)s::integer
  AND s.sale_date = CURRENT_DATE - 1
GROUP BY s.store_id, s.sale_date
$f$
);

-- Q2: heavy settlement audit.  A full-table window/aggregate over every
-- store plus a fixed 1.5s server-side wait, so on a default Docker host the
-- statement runs ~2-3s and visibly collides with the 3s deadline once two
-- connections are saturated and queueing builds up.
INSERT INTO query_template (name, description, sql_text) VALUES (
    'q2_settlement_audit',
    '门店对账审计：全表窗口重算 + 固定等待（慢查询样例，约 2-3 秒）',
$f$
WITH slow AS (
    SELECT pg_sleep(1.5) AS waited
),
audit AS (
    SELECT
        x.store_id,
        x.sale_date,
        x.amount,
        SUM(x.amount) OVER (
            PARTITION BY x.store_id ORDER BY x.sale_date, x.sale_id
        ) AS running_amount,
        RANK() OVER (
            PARTITION BY x.store_id, x.sale_date ORDER BY x.amount DESC
        ) AS line_rank
    FROM sales x
)
SELECT
    a.store_id,
    a.sale_date,
    COUNT(*)                          AS audited_lines,
    ROUND(SUM(a.amount), 2)           AS day_amount,
    ROUND(MAX(a.running_amount), 2)   AS running_amount,
    MAX(a.line_rank)                  AS max_line_rank,
    MAX(s.waited)                     AS throttle_marker
FROM audit a
CROSS JOIN slow s
WHERE a.store_id = %(store)s::integer
GROUP BY a.store_id, a.sale_date
ORDER BY a.sale_date DESC
$f$
);
