-- ============================================================================
-- COMPREHENSIVE PostgreSQL PL/pgSQL ERP DEMO SEED
-- ----------------------------------------------------------------------------
-- Purpose: a large, feature-rich demo whose deep call chain lets a static
--          analyzer build a rich call tree. Themed on the Oracle ERP demo
--          (data/plsql-parse/ERP_USP_RUN_CYCLE__WL2ZTC) and ported/expanded to
--          the PostgreSQL 16 PL/pgSQL feature subset.
--
-- Entry procedure for analysis: erp.erp_usp_run_cycle
--
-- Clean-slate + idempotent. Loads into PostgreSQL 16.
--
-- FEATURE COVERAGE (each used many times across the routines):
--   * transactions: COMMIT + EXCEPTION-path ROLLBACK per procedure;
--     savepoints simulated via nested BEGIN..EXCEPTION sub-blocks
--   * triggers (row + statement, BEFORE + AFTER), views, materialized views,
--     sequences, GENERATED/serial
--   * dynamic SQL: EXECUTE format(%I) and EXECUTE ... USING
--   * IF/ELSIF/ELSE, CASE, WHILE, FOR i IN 1..n, FOR rec IN SELECT, cursors
--     (explicit OPEN/FETCH/CLOSE + FOR-cursor)
--   * subqueries, correlated subqueries, CTE, RECURSIVE CTE, LATERAL,
--     window functions, INNER/LEFT/RIGHT/FULL/CROSS/self joins, deeply nested
--     subqueries, temp tables, UPSERT (ON CONFLICT .. RETURNING), MERGE (PG15+)
--   * DML (INSERT/UPDATE/DELETE/SELECT) + DDL (CREATE TABLE / TEMP TABLE)
--   * function OVERLOADING, global (GUC) vs local scope
--   * arrays, JSONB, custom composite TYPEs, RETURNS TABLE / RETURNS SETOF,
--     DO blocks, RETURNING
--   * edge cases: recursive CTE, self-join, NULL/COALESCE, and a controlled
--     circular call guarded by a depth parameter
-- ============================================================================

DROP SCHEMA IF EXISTS erp CASCADE;
CREATE SCHEMA erp;
DROP SCHEMA IF EXISTS erp_ref CASCADE;
CREATE SCHEMA erp_ref;
SET search_path TO erp;
SET check_function_bodies = false;   -- allow forward references between routines

-- ---------------------------------------------------------------- composite TYPES
CREATE TYPE erp.erp_money AS (amount numeric, currency text);
CREATE TYPE erp.erp_demand_rec AS (product_id int, demand numeric, source_type text);

-- ---------------------------------------------------------------- sequences
CREATE SEQUENCE erp.erp_mrp_seq      START 100000;
CREATE SEQUENCE erp.erp_wo_seq       START 200000;
CREATE SEQUENCE erp.erp_ship_seq     START 300000;
CREATE SEQUENCE erp.erp_invoice_seq  START 400000;
CREATE SEQUENCE erp.erp_glentry_seq  START 500000;
CREATE SEQUENCE erp.erp_audit_seq    START 600000;

-- ---------------------------------------------------------------- tables
CREATE TABLE IF NOT EXISTS erp.erp_config (
    config_key   text PRIMARY KEY,
    config_value text
);
CREATE TABLE IF NOT EXISTS erp.erp_currency (
    currency_id int PRIMARY KEY,
    code        text NOT NULL
);
CREATE TABLE IF NOT EXISTS erp.erp_exchange_rate (
    rate_id     int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    currency_id int REFERENCES erp.erp_currency(currency_id),
    rate        numeric NOT NULL,
    rate_date   date NOT NULL
);
CREATE TABLE IF NOT EXISTS erp.erp_customer (
    customer_id   int PRIMARY KEY,
    customer_name text NOT NULL,
    currency_id   int REFERENCES erp.erp_currency(currency_id)
);
CREATE TABLE IF NOT EXISTS erp.erp_product (
    product_id int PRIMARY KEY,
    sku        text UNIQUE NOT NULL,
    list_price numeric DEFAULT 0,
    std_cost   numeric DEFAULT 0,
    make_buy   text CHECK (make_buy IN ('M','B'))
);
CREATE TABLE IF NOT EXISTS erp.erp_warehouse (
    warehouse_id int PRIMARY KEY,
    wh_code      text NOT NULL
);
CREATE TABLE IF NOT EXISTS erp.erp_inventory (
    inventory_id int PRIMARY KEY,
    product_id   int REFERENCES erp.erp_product(product_id),
    warehouse_id int REFERENCES erp.erp_warehouse(warehouse_id),
    qty_on_hand  numeric DEFAULT 0,
    qty_reserved numeric DEFAULT 0
);
CREATE TABLE IF NOT EXISTS erp.erp_inventory_txn (
    txn_id       int PRIMARY KEY,
    inventory_id int REFERENCES erp.erp_inventory(inventory_id),
    txn_type     text,
    qty          numeric,
    txn_date     date
);
CREATE TABLE IF NOT EXISTS erp.erp_sales_order (
    order_id     int PRIMARY KEY,
    customer_id  int REFERENCES erp.erp_customer(customer_id),
    status       text DEFAULT 'NEW',
    order_date   date,
    total_amount numeric DEFAULT 0
);
CREATE TABLE IF NOT EXISTS erp.erp_sales_order_line (
    line_id     int PRIMARY KEY,
    order_id    int REFERENCES erp.erp_sales_order(order_id),
    product_id  int REFERENCES erp.erp_product(product_id),
    qty         numeric,
    line_amount numeric,
    line_status text DEFAULT 'OPEN'
);
CREATE TABLE IF NOT EXISTS erp.erp_forecast (
    forecast_id int PRIMARY KEY,
    product_id  int REFERENCES erp.erp_product(product_id),
    qty         numeric
);
CREATE TABLE IF NOT EXISTS erp.erp_demand (
    demand_id   int PRIMARY KEY,
    product_id  int,
    source_type text,
    source_id   int,
    qty         numeric,
    need_date   date
);
CREATE TABLE IF NOT EXISTS erp.erp_bom_header (
    bom_id      int PRIMARY KEY,
    product_id  int REFERENCES erp.erp_product(product_id),
    active_flag text DEFAULT 'Y'
);
CREATE TABLE IF NOT EXISTS erp.erp_bom_component (
    bom_comp_id  int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    bom_id       int REFERENCES erp.erp_bom_header(bom_id),
    component_id int REFERENCES erp.erp_product(product_id),
    qty_per      numeric
);
CREATE TABLE IF NOT EXISTS erp.erp_mrp_run (
    mrp_run_id    int PRIMARY KEY,
    run_date      date,
    status        text,
    planned_count int DEFAULT 0
);
CREATE TABLE IF NOT EXISTS erp.erp_mrp_plan (
    plan_id    int PRIMARY KEY,
    mrp_run_id int REFERENCES erp.erp_mrp_run(mrp_run_id),
    product_id int REFERENCES erp.erp_product(product_id),
    gross_req  numeric,
    net_req    numeric,
    action     text
);
CREATE TABLE IF NOT EXISTS erp.erp_work_order (
    wo_id         int PRIMARY KEY,
    product_id    int REFERENCES erp.erp_product(product_id),
    bom_id        int,
    routing_id    int,
    qty_planned   numeric,
    qty_completed numeric DEFAULT 0,
    status        text,
    due_date      date
);
CREATE TABLE IF NOT EXISTS erp.erp_material_issue (
    issue_id     int PRIMARY KEY,
    wo_id        int REFERENCES erp.erp_work_order(wo_id),
    component_id int,
    qty_issued   numeric,
    issue_date   date
);
CREATE TABLE IF NOT EXISTS erp.erp_routing (
    routing_id int PRIMARY KEY,
    product_id int REFERENCES erp.erp_product(product_id)
);
CREATE TABLE IF NOT EXISTS erp.erp_work_center (
    work_center_id int PRIMARY KEY,
    wc_code        text,
    capacity_hrs   numeric
);
CREATE TABLE IF NOT EXISTS erp.erp_routing_operation (
    op_id          int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    routing_id     int REFERENCES erp.erp_routing(routing_id),
    work_center_id int REFERENCES erp.erp_work_center(work_center_id),
    run_time_min   numeric
);
CREATE TABLE IF NOT EXISTS erp.erp_shipment (
    shipment_id int PRIMARY KEY,
    order_id    int REFERENCES erp.erp_sales_order(order_id),
    ship_date   date,
    carrier     text,
    status      text
);
CREATE TABLE IF NOT EXISTS erp.erp_shipment_line (
    ship_line_id int PRIMARY KEY,
    shipment_id  int REFERENCES erp.erp_shipment(shipment_id),
    line_id      int,
    qty_shipped  numeric
);
CREATE TABLE IF NOT EXISTS erp.erp_invoice (
    invoice_id   int PRIMARY KEY,
    order_id     int REFERENCES erp.erp_sales_order(order_id),
    customer_id  int REFERENCES erp.erp_customer(customer_id),
    invoice_date date,
    amount       numeric,
    status       text
);
CREATE TABLE IF NOT EXISTS erp.erp_invoice_line (
    inv_line_id int PRIMARY KEY,
    invoice_id  int REFERENCES erp.erp_invoice(invoice_id),
    line_id     int,
    amount      numeric
);
CREATE TABLE IF NOT EXISTS erp.erp_gl_account (
    account_id   int PRIMARY KEY,
    account_code text UNIQUE,
    account_type text
);
CREATE TABLE IF NOT EXISTS erp.erp_gl_entry (
    gl_entry_id int PRIMARY KEY,
    account_id  int REFERENCES erp.erp_gl_account(account_id),
    entry_date  date,
    debit       numeric DEFAULT 0,
    credit      numeric DEFAULT 0,
    source_doc  text
);
CREATE TABLE IF NOT EXISTS erp.erp_supplier (
    supplier_id   int PRIMARY KEY,
    supplier_name text
);
CREATE TABLE IF NOT EXISTS erp.erp_purchase_order (
    po_id       int PRIMARY KEY,
    supplier_id int REFERENCES erp.erp_supplier(supplier_id),
    status      text
);
CREATE TABLE IF NOT EXISTS erp.erp_purchase_order_line (
    po_line_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    po_id      int REFERENCES erp.erp_purchase_order(po_id),
    product_id int REFERENCES erp.erp_product(product_id),
    qty        numeric,
    unit_cost  numeric
);
CREATE TABLE IF NOT EXISTS erp.erp_price_list (
    price_list_id int PRIMARY KEY,
    list_name     text,
    active_flag   text DEFAULT 'Y'
);
CREATE TABLE IF NOT EXISTS erp.erp_product_price (
    pp_id         int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    price_list_id int REFERENCES erp.erp_price_list(price_list_id),
    product_id    int REFERENCES erp.erp_product(product_id),
    unit_price    numeric
);
CREATE TABLE IF NOT EXISTS erp.erp_audit_log (
    audit_id    int PRIMARY KEY,
    event_type  text,
    object_name text,
    message     text,
    created_at  timestamptz DEFAULT now()
);
CREATE TABLE IF NOT EXISTS erp.erp_trigger_log (
    log_id     int PRIMARY KEY,
    table_name text,
    op         text,
    note       text,
    logged_at  timestamptz DEFAULT now()
);

-- ---------------------------------------------------------------- indexes
CREATE INDEX IF NOT EXISTS ix_sol_order   ON erp.erp_sales_order_line(order_id);
CREATE INDEX IF NOT EXISTS ix_sol_product ON erp.erp_sales_order_line(product_id);
CREATE INDEX IF NOT EXISTS ix_inv_product ON erp.erp_inventory(product_id);
CREATE INDEX IF NOT EXISTS ix_bc_bom      ON erp.erp_bom_component(bom_id);
CREATE INDEX IF NOT EXISTS ix_gl_source   ON erp.erp_gl_entry(source_doc);
CREATE INDEX IF NOT EXISTS ix_demand_type ON erp.erp_demand(source_type);
CREATE INDEX IF NOT EXISTS ix_plan_run    ON erp.erp_mrp_plan(mrp_run_id);

-- ---------------------------------------------------------------- seed data
INSERT INTO erp.erp_config(config_key, config_value) VALUES
    ('SAFETY_STOCK_PCT','10'),('MRP_HORIZON_DAYS','30'),('MAX_DEPTH','8') ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_currency(currency_id, code) VALUES
    (1,'USD'),(2,'EUR'),(3,'GBP') ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_exchange_rate(currency_id, rate, rate_date) VALUES
    (1,1.00,DATE '2026-07-01'),(2,1.08,DATE '2026-07-01'),(3,1.27,DATE '2026-07-01') ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_customer(customer_id, customer_name, currency_id) VALUES
    (1,'Acme Corp',1),(2,'Globex EU',2),(3,'Initech UK',3) ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_product(product_id, sku, list_price, std_cost, make_buy) VALUES
    (1,'FG-100',500,300,'M'),(2,'FG-200',800,450,'M'),
    (3,'RM-10',40,25,'B'),(4,'RM-20',60,35,'B'),(5,'SA-50',150,90,'M') ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_warehouse(warehouse_id, wh_code) VALUES
    (1,'WH-MAIN'),(2,'WH-WEST') ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_inventory(inventory_id, product_id, warehouse_id, qty_on_hand, qty_reserved) VALUES
    (1,1,1,50,10),(2,2,1,20,5),(3,3,1,500,50),(4,4,1,300,20),(5,5,2,80,0) ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_sales_order(order_id, customer_id, status, order_date, total_amount) VALUES
    (1,1,'NEW',DATE '2026-07-10',0),(2,2,'CONFIRMED',DATE '2026-07-12',0),(3,3,'NEW',DATE '2026-07-15',0) ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_sales_order_line(line_id, order_id, product_id, qty, line_amount, line_status) VALUES
    (1,1,1,10,5000,'OPEN'),(2,1,2,5,4000,'OPEN'),(3,2,1,8,4000,'OPEN'),(4,3,5,12,1800,'OPEN') ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_forecast(forecast_id, product_id, qty) VALUES
    (1,1,30),(2,2,15),(3,5,20) ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_bom_header(bom_id, product_id, active_flag) VALUES
    (1,1,'Y'),(2,2,'Y'),(3,5,'Y') ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_bom_component(bom_id, component_id, qty_per) VALUES
    (1,3,2),(1,4,1),(2,3,3),(2,5,1),(3,4,2) ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_routing(routing_id, product_id) VALUES (1,1),(2,2),(3,5) ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_work_center(work_center_id, wc_code, capacity_hrs) VALUES
    (1,'WC-ASSY',8),(2,'WC-PACK',8) ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_routing_operation(routing_id, work_center_id, run_time_min) VALUES
    (1,1,30),(1,2,10),(2,1,45),(3,1,20) ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_supplier(supplier_id, supplier_name) VALUES
    (1,'Raw Metals Inc'),(2,'Fasteners Ltd') ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_purchase_order(po_id, supplier_id, status) VALUES (1,1,'OPEN'),(2,2,'OPEN') ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_purchase_order_line(po_id, product_id, qty, unit_cost) VALUES
    (1,3,100,24),(1,4,80,34),(2,3,50,26) ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_price_list(price_list_id, list_name, active_flag) VALUES
    (1,'STANDARD','Y'),(2,'PROMO','Y') ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_product_price(price_list_id, product_id, unit_price) VALUES
    (1,1,500),(1,2,800),(2,1,470),(2,5,140) ON CONFLICT DO NOTHING;
INSERT INTO erp.erp_gl_account(account_id, account_code, account_type) VALUES
    (1,'1200','ASSET'),(2,'4000','REVENUE'),(3,'5000','EXPENSE') ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------- views
CREATE OR REPLACE VIEW erp.erp_v_order_summary AS
SELECT so.order_id, so.customer_id, c.customer_name, so.status,
       COUNT(sol.line_id) AS line_count,
       COALESCE(SUM(sol.line_amount),0) AS order_total
FROM erp.erp_sales_order so
JOIN erp.erp_customer c ON c.customer_id = so.customer_id
LEFT JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
GROUP BY so.order_id, so.customer_id, c.customer_name, so.status;

CREATE OR REPLACE VIEW erp.erp_v_inventory_value AS
SELECT i.product_id, p.sku, i.qty_on_hand, p.std_cost,
       (i.qty_on_hand - i.qty_reserved) * p.std_cost AS ext_value
FROM erp.erp_inventory i
JOIN erp.erp_product p ON p.product_id = i.product_id;

-- ---------------------------------------------------------------- materialized views
CREATE MATERIALIZED VIEW erp.erp_mv_product_sales AS
SELECT p.product_id, p.sku,
       COALESCE(SUM(sol.qty),0) AS total_qty,
       COALESCE(SUM(sol.line_amount),0) AS total_amount
FROM erp.erp_product p
LEFT JOIN erp.erp_sales_order_line sol ON sol.product_id = p.product_id
GROUP BY p.product_id, p.sku
WITH DATA;

CREATE MATERIALIZED VIEW erp.erp_mv_customer_rev AS
SELECT c.customer_id, c.customer_name,
       COALESCE(SUM(sol.line_amount),0) AS revenue
FROM erp.erp_customer c
LEFT JOIN erp.erp_sales_order so ON so.customer_id = c.customer_id
LEFT JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
GROUP BY c.customer_id, c.customer_name
WITH DATA;

-- ============================================================================
-- FUNCTIONS  (base calc + composite + overloads + RETURNS TABLE/SETOF + jsonb)
-- ============================================================================

CREATE OR REPLACE FUNCTION erp.erp_fn_get_config(p_key text)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE v_val text;
BEGIN
  SELECT config_value INTO v_val FROM erp.erp_config WHERE config_key = p_key;
  RETURN v_val;
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END; $$;

-- overload set: erp_fn_fx_rate(int) and erp_fn_fx_rate(int, date)
CREATE OR REPLACE FUNCTION erp.erp_fn_fx_rate(p_currency_id int)
RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE v_rate numeric := 1;
BEGIN
  SELECT rate INTO v_rate FROM erp.erp_exchange_rate
  WHERE currency_id = p_currency_id ORDER BY rate_date DESC LIMIT 1;
  RETURN COALESCE(v_rate, 1);
EXCEPTION WHEN OTHERS THEN RETURN 1;
END; $$;

CREATE OR REPLACE FUNCTION erp.erp_fn_fx_rate(p_currency_id int, p_as_of date)
RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE v_rate numeric := 1;
BEGIN
  SELECT rate INTO v_rate FROM erp.erp_exchange_rate
  WHERE currency_id = p_currency_id AND rate_date <= p_as_of
  ORDER BY rate_date DESC LIMIT 1;
  RETURN COALESCE(v_rate, 1);
EXCEPTION WHEN OTHERS THEN RETURN 1;
END; $$;

-- overload set: erp_fn_onhand_qty(int) and erp_fn_onhand_qty(int, int)
CREATE OR REPLACE FUNCTION erp.erp_fn_onhand_qty(p_product_id int)
RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE v_qty numeric := 0;
BEGIN
  SELECT COALESCE(SUM(qty_on_hand - qty_reserved),0) INTO v_qty
  FROM erp.erp_inventory WHERE product_id = p_product_id;
  RETURN v_qty;
END; $$;

CREATE OR REPLACE FUNCTION erp.erp_fn_onhand_qty(p_product_id int, p_warehouse_id int)
RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE v_qty numeric := 0;
BEGIN
  SELECT COALESCE(SUM(qty_on_hand - qty_reserved),0) INTO v_qty
  FROM erp.erp_inventory WHERE product_id = p_product_id AND warehouse_id = p_warehouse_id;
  RETURN v_qty;
END; $$;

CREATE OR REPLACE FUNCTION erp.erp_fn_order_total(p_order_id int)
RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE v_total numeric := 0;
BEGIN
  SELECT COALESCE(SUM(line_amount),0) INTO v_total
  FROM erp.erp_sales_order_line WHERE order_id = p_order_id;
  RETURN v_total;
END; $$;

CREATE OR REPLACE FUNCTION erp.erp_fn_bom_component_count(p_product_id int)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE v_cnt int := 0;
BEGIN
  SELECT COUNT(*) INTO v_cnt
  FROM erp.erp_bom_header bh
  JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
  WHERE bh.product_id = p_product_id AND bh.active_flag = 'Y';
  RETURN v_cnt;
END; $$;

CREATE OR REPLACE FUNCTION erp.erp_fn_gross_requirement(p_product_id int)
RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE v_so numeric := 0; v_fc numeric := 0;
BEGIN
  SELECT COALESCE(SUM(sol.qty),0) INTO v_so
  FROM erp.erp_sales_order_line sol
  JOIN erp.erp_sales_order so ON so.order_id = sol.order_id
  WHERE sol.product_id = p_product_id AND so.status IN ('NEW','CONFIRMED');
  SELECT COALESCE(SUM(qty),0) INTO v_fc FROM erp.erp_forecast WHERE product_id = p_product_id;
  RETURN v_so + v_fc;
END; $$;

-- calls erp_fn_get_config
CREATE OR REPLACE FUNCTION erp.erp_fn_safety_stock(p_product_id int)
RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE v_pct numeric := 0; v_onhand numeric := 0;
BEGIN
  v_pct := COALESCE(erp.erp_fn_get_config('SAFETY_STOCK_PCT')::numeric, 0);
  SELECT COALESCE(SUM(qty_on_hand),0) INTO v_onhand FROM erp.erp_inventory WHERE product_id = p_product_id;
  RETURN ROUND(v_onhand * v_pct / 100, 2);
EXCEPTION WHEN OTHERS THEN RETURN 0;
END; $$;

-- calls erp_fn_gross_requirement, erp_fn_onhand_qty, erp_fn_safety_stock
CREATE OR REPLACE FUNCTION erp.erp_fn_net_requirement(p_product_id int)
RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE v_gross numeric; v_onhand numeric; v_safety numeric; v_net numeric;
BEGIN
  v_gross  := erp.erp_fn_gross_requirement(p_product_id);
  v_onhand := erp.erp_fn_onhand_qty(p_product_id);
  v_safety := erp.erp_fn_safety_stock(p_product_id);
  v_net    := v_gross - v_onhand + v_safety;
  IF v_net < 0 THEN v_net := 0; END IF;
  RETURN v_net;
END; $$;

-- composite-returning function
CREATE OR REPLACE FUNCTION erp.erp_fn_money(p_amount numeric, p_currency text)
RETURNS erp.erp_money LANGUAGE plpgsql AS $$
DECLARE v erp.erp_money;
BEGIN
  v.amount := COALESCE(p_amount, 0);
  v.currency := COALESCE(p_currency, 'USD');
  RETURN v;
END; $$;

-- overload trio: erp_fn_calc(int) / erp_fn_calc(int,int) / erp_fn_calc(numeric)
CREATE OR REPLACE FUNCTION erp.erp_fn_calc(p_a int)
RETURNS numeric LANGUAGE plpgsql AS $$
BEGIN RETURN p_a * 2; END; $$;

CREATE OR REPLACE FUNCTION erp.erp_fn_calc(p_a int, p_b int)
RETURNS numeric LANGUAGE plpgsql AS $$
BEGIN RETURN p_a + p_b; END; $$;

CREATE OR REPLACE FUNCTION erp.erp_fn_calc(p_a numeric)
RETURNS numeric LANGUAGE plpgsql AS $$
BEGIN RETURN ROUND(p_a * 1.5, 2); END; $$;

-- array-consuming function
CREATE OR REPLACE FUNCTION erp.erp_fn_sum_array(p_vals numeric[])
RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE v_sum numeric := 0; v_x numeric;
BEGIN
  IF p_vals IS NULL THEN RETURN 0; END IF;
  FOREACH v_x IN ARRAY p_vals LOOP v_sum := v_sum + COALESCE(v_x,0); END LOOP;
  RETURN v_sum;
END; $$;

-- jsonb-returning function
CREATE OR REPLACE FUNCTION erp.erp_fn_order_json(p_order_id int)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE v_js jsonb;
BEGIN
  SELECT jsonb_build_object(
           'order_id', so.order_id,
           'status', so.status,
           'total', erp.erp_fn_order_total(so.order_id),
           'lines', COALESCE(jsonb_agg(jsonb_build_object('line', sol.line_id, 'amt', sol.line_amount)), '[]'::jsonb)
         )
    INTO v_js
  FROM erp.erp_sales_order so
  LEFT JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
  WHERE so.order_id = p_order_id
  GROUP BY so.order_id, so.status;
  RETURN COALESCE(v_js, '{}'::jsonb);
END; $$;

-- RETURNS TABLE
CREATE OR REPLACE FUNCTION erp.erp_fn_product_demand()
RETURNS TABLE(product_id int, demand numeric) LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  SELECT p.product_id, erp.erp_fn_gross_requirement(p.product_id)
  FROM erp.erp_product p
  ORDER BY p.product_id;
END; $$;

-- RETURNS SETOF a table rowtype
CREATE OR REPLACE FUNCTION erp.erp_fn_active_products()
RETURNS SETOF erp.erp_product LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY SELECT * FROM erp.erp_product WHERE make_buy IN ('M','B') ORDER BY product_id;
END; $$;

-- ============================================================================
-- TRIGGER FUNCTIONS + TRIGGERS  (row + statement, BEFORE + AFTER)
-- ============================================================================

CREATE OR REPLACE FUNCTION erp.erp_trg_wo_biu() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.status IS NULL THEN NEW.status := 'NEW'; END IF;
  INSERT INTO erp.erp_trigger_log(log_id, table_name, op, note)
  VALUES (nextval('erp.erp_audit_seq'), 'erp_work_order', TG_OP, 'row before insert/update wo='||COALESCE(NEW.wo_id,0));
  RETURN NEW;
END; $$;

CREATE OR REPLACE FUNCTION erp.erp_trg_invoice_ai() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO erp.erp_trigger_log(log_id, table_name, op, note)
  VALUES (nextval('erp.erp_audit_seq'), 'erp_invoice', TG_OP, 'row after invoice='||COALESCE(NEW.invoice_id,0));
  RETURN NULL;
END; $$;

CREATE OR REPLACE FUNCTION erp.erp_trg_gl_stmt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO erp.erp_trigger_log(log_id, table_name, op, note)
  VALUES (nextval('erp.erp_audit_seq'), 'erp_gl_entry', TG_OP, 'statement-level '||TG_WHEN);
  RETURN NULL;
END; $$;

CREATE OR REPLACE FUNCTION erp.erp_trg_so_bu() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.total_amount IS NULL THEN NEW.total_amount := 0; END IF;
  INSERT INTO erp.erp_trigger_log(log_id, table_name, op, note)
  VALUES (nextval('erp.erp_audit_seq'), 'erp_sales_order', TG_OP, 'row before update order='||COALESCE(NEW.order_id,0));
  RETURN NEW;
END; $$;

DROP TRIGGER IF EXISTS trg_wo_biu ON erp.erp_work_order;
CREATE TRIGGER trg_wo_biu BEFORE INSERT OR UPDATE ON erp.erp_work_order
  FOR EACH ROW EXECUTE FUNCTION erp.erp_trg_wo_biu();

DROP TRIGGER IF EXISTS trg_invoice_ai ON erp.erp_invoice;
CREATE TRIGGER trg_invoice_ai AFTER INSERT ON erp.erp_invoice
  FOR EACH ROW EXECUTE FUNCTION erp.erp_trg_invoice_ai();

DROP TRIGGER IF EXISTS trg_gl_stmt ON erp.erp_gl_entry;
CREATE TRIGGER trg_gl_stmt AFTER INSERT ON erp.erp_gl_entry
  FOR EACH STATEMENT EXECUTE FUNCTION erp.erp_trg_gl_stmt();

DROP TRIGGER IF EXISTS trg_so_bu ON erp.erp_sales_order;
CREATE TRIGGER trg_so_bu BEFORE UPDATE ON erp.erp_sales_order
  FOR EACH ROW EXECUTE FUNCTION erp.erp_trg_so_bu();

-- ============================================================================
-- PROCEDURES
-- ============================================================================

-- Leaf logger. No COMMIT/ROLLBACK: it is invoked inside caller cursor loops,
-- where committing would be illegal in PL/pgSQL.
CREATE OR REPLACE PROCEDURE erp.erp_usp_write_audit_trail(
  p_event text, p_object text, p_message text)
LANGUAGE plpgsql AS $$
DECLARE v_id int; v_cnt int := 0;
BEGIN
  v_id := nextval('erp.erp_audit_seq');
  INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
  VALUES (v_id, p_event, p_object, substr(p_message, 1, 400));
  BEGIN
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_audit_log;
    IF v_cnt > 500000 THEN
      DELETE FROM erp.erp_audit_log WHERE audit_id < (v_id - 400000);
    END IF;
  EXCEPTION WHEN OTHERS THEN NULL;
  END;
EXCEPTION WHEN OTHERS THEN NULL;
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_edge_recalc(p_mrp_run_id int)
LANGUAGE plpgsql AS $$
DECLARE v_i int := 0; v_grade text; v_total int := 0; v_lvls int := 0;
BEGIN
  CALL erp.erp_usp_write_audit_trail('EDGE_START','ERP_EDGE','recalc run '||p_mrp_run_id);
  WHILE v_i < 5 LOOP
    v_i := v_i + 1;
    CASE
      WHEN v_i <= 1 THEN v_grade := 'LOW';
      WHEN v_i <= 3 THEN v_grade := 'MED';
      ELSE v_grade := 'HIGH';
    END CASE;
    INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
    VALUES (nextval('erp.erp_audit_seq'),'EDGE_ITER','ERP_EDGE','iter '||v_i||' grade '||v_grade);
  END LOOP;
  BEGIN
    WITH RECURSIVE bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.component_id
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_total, v_lvls FROM bom_tree;
  EXCEPTION WHEN OTHERS THEN v_total := 0; v_lvls := 0;
  END;
  BEGIN
    REFRESH MATERIALIZED VIEW erp.erp_mv_product_sales;
    REFRESH MATERIALIZED VIEW erp.erp_mv_customer_rev;
  EXCEPTION WHEN OTHERS THEN
    CALL erp.erp_usp_write_audit_trail('MV_WARN','ERP_EDGE',SQLERRM);
  END;
  CALL erp.erp_usp_write_audit_trail('EDGE_END','ERP_EDGE','recalc done rows='||v_total||' maxlvl='||v_lvls);
  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('EDGE_ERR','ERP_EDGE',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_recurse(p_depth int, p_max int DEFAULT 5)
LANGUAGE plpgsql AS $$
BEGIN
  CALL erp.erp_usp_write_audit_trail('RECURSE','ERP_RECURSE','depth '||p_depth||'/'||p_max);
  IF p_depth < p_max THEN
    CALL erp.erp_usp_recurse(p_depth + 1, p_max);   -- self-call, guarded by p_max
  END IF;
  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('RECURSE_ERR','ERP_RECURSE',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_ping(p_depth int, p_max int DEFAULT 3)
LANGUAGE plpgsql AS $$
BEGIN
  CALL erp.erp_usp_write_audit_trail('PING','ERP_PINGPONG','ping '||p_depth);
  IF p_depth < p_max THEN
    CALL erp.erp_usp_pong(p_depth, p_max);          -- circular: ping -> pong
  END IF;
  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('PING_ERR','ERP_PINGPONG',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_pong(p_depth int, p_max int DEFAULT 3)
LANGUAGE plpgsql AS $$
BEGIN
  CALL erp.erp_usp_write_audit_trail('PONG','ERP_PINGPONG','pong '||p_depth);
  IF p_depth + 1 < p_max THEN
    CALL erp.erp_usp_ping(p_depth + 1, p_max);       -- circular: pong -> ping (guarded)
  END IF;
  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('PONG_ERR','ERP_PINGPONG',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_feature_showcase()
LANGUAGE plpgsql AS $$
DECLARE
  v_j jsonb; v_s numeric; v_c1 numeric; v_c2 numeric; v_c3 numeric; r record; v_cnt int := 0; v_fx numeric;
BEGIN
  CALL erp.erp_usp_write_audit_trail('SHOWCASE_START','FEATURE','begin');
  FOR r IN SELECT * FROM erp.erp_fn_product_demand() LOOP v_cnt := v_cnt + 1; END LOOP;
  FOR r IN SELECT * FROM erp.erp_fn_active_products() LOOP v_cnt := v_cnt + 1; END LOOP;
  v_s  := erp.erp_fn_sum_array(ARRAY[1,2,3,4]::numeric[]);
  v_c1 := erp.erp_fn_calc(5);
  v_c2 := erp.erp_fn_calc(5, 7);
  v_c3 := erp.erp_fn_calc(2.5::numeric);
  v_fx := erp.erp_fn_fx_rate(2, now()::date);
  v_j  := erp.erp_fn_order_json(1);
  CALL erp.erp_usp_write_audit_trail('SHOWCASE','FEATURE',
    'cnt='||v_cnt||' sum='||v_s||' calc='||v_c1||'/'||v_c2||'/'||v_c3||' fx='||v_fx||' j='||COALESCE(v_j->>'order_id','-'));

  -- ---- feature block shwc : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','shwc','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','shwc',SQLERRM);
  END;
  -- ---- feature block shwc : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','shwc','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','shwc',SQLERRM);
  END;
  -- ---- feature block shwc : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','shwc','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','shwc',SQLERRM);
  END;
  -- ---- feature block shwc : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','shwc','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','shwc',SQLERRM);
  END;
  -- ---- feature block shwc : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','shwc','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','shwc',SQLERRM);
  END;
  -- ---- feature block shwc : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','shwc','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','shwc',SQLERRM);
  END;
  -- ---- feature block shwc : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_shwc (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_shwc(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_shwc.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_shwc(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_shwc.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_shwc;
    CALL erp.erp_usp_write_audit_trail('UPSERT','shwc','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','shwc',SQLERRM);
  END;
  -- ---- feature block shwc : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_shwc')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_shwc', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_shwc';
    CALL erp.erp_usp_write_audit_trail('MERGE','shwc','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','shwc',SQLERRM);
  END;
  -- ---- feature block shwc : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','shwc','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','shwc',SQLERRM);
  END;
  -- ---- feature block shwc : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','shwc','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','shwc','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','shwc',SQLERRM);
  END;
  -- ---- feature block shwc : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','shwc',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','shwc',SQLERRM);
  END;
  -- ---- feature block shwc : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','shwc','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','shwc',SQLERRM);
  END;
  -- ---- feature block shwc : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'shwc', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','shwc','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','shwc',SQLERRM);
  END;
  -- ---- feature block shwc : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'shwc', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','shwc','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','shwc',SQLERRM);
  END;

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_FEATURE_SHOWCASE_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_finalize_gl_batch(p_source_doc text)
LANGUAGE plpgsql AS $$
DECLARE
  v_dr numeric := 0; v_cr numeric := 0; v_diff numeric := 0; v_lines int := 0;
BEGIN
  CALL erp.erp_usp_write_audit_trail('GL_FIN_START','ERP_GL_ENTRY','Finalizing '||p_source_doc);
  SELECT COALESCE(SUM(debit),0), COALESCE(SUM(credit),0), COUNT(*)
    INTO v_dr, v_cr, v_lines
  FROM erp.erp_gl_entry WHERE source_doc = p_source_doc;
  v_diff := v_dr - v_cr;

  -- ---- feature block glfin : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','glfin','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','glfin',SQLERRM);
  END;
  -- ---- feature block glfin : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','glfin','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','glfin',SQLERRM);
  END;
  -- ---- feature block glfin : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','glfin','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','glfin',SQLERRM);
  END;
  -- ---- feature block glfin : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','glfin','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','glfin',SQLERRM);
  END;
  -- ---- feature block glfin : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','glfin','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','glfin',SQLERRM);
  END;
  -- ---- feature block glfin : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','glfin','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','glfin',SQLERRM);
  END;
  -- ---- feature block glfin : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_glfin (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_glfin(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_glfin.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_glfin(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_glfin.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_glfin;
    CALL erp.erp_usp_write_audit_trail('UPSERT','glfin','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','glfin',SQLERRM);
  END;
  -- ---- feature block glfin : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_glfin')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_glfin', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_glfin';
    CALL erp.erp_usp_write_audit_trail('MERGE','glfin','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','glfin',SQLERRM);
  END;
  -- ---- feature block glfin : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','glfin','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','glfin',SQLERRM);
  END;
  -- ---- feature block glfin : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','glfin','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','glfin','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','glfin',SQLERRM);
  END;
  -- ---- feature block glfin : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','glfin',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','glfin',SQLERRM);
  END;
  -- ---- feature block glfin : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','glfin','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','glfin',SQLERRM);
  END;
  -- ---- feature block glfin : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'glfin', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','glfin','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','glfin',SQLERRM);
  END;
  -- ---- feature block glfin : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'glfin', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','glfin','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','glfin',SQLERRM);
  END;

  IF v_diff <> 0 THEN
    CALL erp.erp_usp_write_audit_trail('GL_FIN_WARN','ERP_GL_ENTRY','Unbalanced '||p_source_doc||' diff='||v_diff);
  END IF;
  CALL erp.erp_usp_write_audit_trail('GL_FIN','ERP_GL_ENTRY','Finalized '||p_source_doc||' lines='||v_lines);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_FINALIZE_GL_BATCH_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_post_gl_entries(p_source_doc text, p_amount numeric)
LANGUAGE plpgsql AS $$
DECLARE
  v_ar_acct int; v_rev_acct int; v_cogs_acct int; v_cnt int := 0;
BEGIN
  CALL erp.erp_usp_write_audit_trail('GL_POST_START','ERP_GL_ENTRY','Posting '||p_source_doc);
  SELECT account_id INTO v_ar_acct  FROM erp.erp_gl_account WHERE account_code = '1200';
  SELECT account_id INTO v_rev_acct FROM erp.erp_gl_account WHERE account_code = '4000';
  BEGIN
    SELECT account_id INTO STRICT v_cogs_acct FROM erp.erp_gl_account WHERE account_code = '5000';
  EXCEPTION WHEN NO_DATA_FOUND THEN v_cogs_acct := v_rev_acct; END;
  INSERT INTO erp.erp_gl_entry(gl_entry_id, account_id, entry_date, debit, credit, source_doc)
  VALUES (nextval('erp.erp_glentry_seq'), v_ar_acct, now()::date, p_amount, 0, p_source_doc);
  INSERT INTO erp.erp_gl_entry(gl_entry_id, account_id, entry_date, debit, credit, source_doc)
  VALUES (nextval('erp.erp_glentry_seq'), v_rev_acct, now()::date, 0, p_amount, p_source_doc);
  SELECT COUNT(*) INTO v_cnt FROM erp.erp_gl_entry WHERE source_doc = p_source_doc;

  -- ---- feature block glpost : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','glpost','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','glpost',SQLERRM);
  END;
  -- ---- feature block glpost : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','glpost','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','glpost',SQLERRM);
  END;
  -- ---- feature block glpost : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','glpost','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','glpost',SQLERRM);
  END;
  -- ---- feature block glpost : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','glpost','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','glpost',SQLERRM);
  END;
  -- ---- feature block glpost : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','glpost','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','glpost',SQLERRM);
  END;
  -- ---- feature block glpost : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','glpost','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','glpost',SQLERRM);
  END;
  -- ---- feature block glpost : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_glpost (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_glpost(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_glpost.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_glpost(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_glpost.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_glpost;
    CALL erp.erp_usp_write_audit_trail('UPSERT','glpost','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','glpost',SQLERRM);
  END;
  -- ---- feature block glpost : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_glpost')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_glpost', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_glpost';
    CALL erp.erp_usp_write_audit_trail('MERGE','glpost','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','glpost',SQLERRM);
  END;
  -- ---- feature block glpost : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','glpost','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','glpost',SQLERRM);
  END;
  -- ---- feature block glpost : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','glpost','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','glpost','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','glpost',SQLERRM);
  END;
  -- ---- feature block glpost : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','glpost',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','glpost',SQLERRM);
  END;
  -- ---- feature block glpost : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','glpost','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','glpost',SQLERRM);
  END;
  -- ---- feature block glpost : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'glpost', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','glpost','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','glpost',SQLERRM);
  END;
  -- ---- feature block glpost : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'glpost', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','glpost','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','glpost',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('GL_POST','ERP_GL_ENTRY','Posted '||v_cnt||' lines for '||p_source_doc);

  CALL erp.erp_usp_finalize_gl_batch(p_source_doc);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_POST_GL_ENTRIES_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_generate_invoice(p_order_id int)
LANGUAGE plpgsql AS $$
DECLARE
  v_invoice_id int; v_customer int; v_amount numeric := 0; v_exists int := 0;
BEGIN
  CALL erp.erp_usp_write_audit_trail('INV_GEN_START','ERP_INVOICE','Order '||p_order_id);
  SELECT customer_id INTO v_customer FROM erp.erp_sales_order WHERE order_id = p_order_id;
  v_amount := erp.erp_fn_order_total(p_order_id);
  SELECT COUNT(*) INTO v_exists FROM erp.erp_invoice WHERE order_id = p_order_id;
  IF v_exists > 0 THEN
    CALL erp.erp_usp_write_audit_trail('INVOICE_SKIP','ERP_INVOICE','exists for '||p_order_id);
    RETURN;
  END IF;
  v_invoice_id := nextval('erp.erp_invoice_seq');
  INSERT INTO erp.erp_invoice(invoice_id, order_id, customer_id, invoice_date, amount, status)
  VALUES (v_invoice_id, p_order_id, v_customer, now()::date, v_amount, 'OPEN');
  INSERT INTO erp.erp_invoice_line(inv_line_id, invoice_id, line_id, amount)
  SELECT nextval('erp.erp_invoice_seq'), v_invoice_id, line_id, line_amount
  FROM erp.erp_sales_order_line WHERE order_id = p_order_id;

  -- ---- feature block invgen : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','invgen','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','invgen',SQLERRM);
  END;
  -- ---- feature block invgen : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','invgen','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','invgen',SQLERRM);
  END;
  -- ---- feature block invgen : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','invgen','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','invgen',SQLERRM);
  END;
  -- ---- feature block invgen : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','invgen','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','invgen',SQLERRM);
  END;
  -- ---- feature block invgen : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','invgen','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','invgen',SQLERRM);
  END;
  -- ---- feature block invgen : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','invgen','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','invgen',SQLERRM);
  END;
  -- ---- feature block invgen : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_invgen (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_invgen(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_invgen.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_invgen(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_invgen.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_invgen;
    CALL erp.erp_usp_write_audit_trail('UPSERT','invgen','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','invgen',SQLERRM);
  END;
  -- ---- feature block invgen : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_invgen')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_invgen', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_invgen';
    CALL erp.erp_usp_write_audit_trail('MERGE','invgen','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','invgen',SQLERRM);
  END;
  -- ---- feature block invgen : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','invgen','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','invgen',SQLERRM);
  END;
  -- ---- feature block invgen : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','invgen','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','invgen','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','invgen',SQLERRM);
  END;
  -- ---- feature block invgen : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','invgen',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','invgen',SQLERRM);
  END;
  -- ---- feature block invgen : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','invgen','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','invgen',SQLERRM);
  END;
  -- ---- feature block invgen : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'invgen', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','invgen','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','invgen',SQLERRM);
  END;
  -- ---- feature block invgen : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'invgen', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','invgen','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','invgen',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('INV_GEN','ERP_INVOICE','Invoice '||v_invoice_id||' amount '||v_amount);

  CALL erp.erp_usp_post_gl_entries('INV-'||v_invoice_id, v_amount);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_GENERATE_INVOICE_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_ship_order(p_order_id int)
LANGUAGE plpgsql AS $$
DECLARE
  v_ship_id int; v_inv_id int;
BEGIN
  CALL erp.erp_usp_write_audit_trail('SHIP_START','ERP_SHIPMENT','Order '||p_order_id);
  v_ship_id := nextval('erp.erp_ship_seq');
  INSERT INTO erp.erp_shipment(shipment_id, order_id, ship_date, carrier, status)
  VALUES (v_ship_id, p_order_id, now()::date, 'DEFAULT', 'SHIPPED');
  FOR ln IN SELECT sol.line_id, sol.product_id, sol.qty
            FROM erp.erp_sales_order_line sol
            WHERE sol.order_id = p_order_id AND sol.line_status = 'OPEN' LOOP
    INSERT INTO erp.erp_shipment_line(ship_line_id, shipment_id, line_id, qty_shipped)
    VALUES (nextval('erp.erp_ship_seq'), v_ship_id, ln.line_id, ln.qty);
    BEGIN
      SELECT inventory_id INTO STRICT v_inv_id FROM erp.erp_inventory WHERE product_id = ln.product_id LIMIT 1;
      INSERT INTO erp.erp_inventory_txn(txn_id, inventory_id, txn_type, qty, txn_date)
      VALUES (nextval('erp.erp_ship_seq'), v_inv_id, 'SHIP', -1 * ln.qty, now()::date);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      CALL erp.erp_usp_write_audit_trail('SHIP_WARN','ERP_INVENTORY','no inv product '||ln.product_id);
    END;
    UPDATE erp.erp_sales_order_line SET line_status = 'SHIPPED' WHERE line_id = ln.line_id;
  END LOOP;
  UPDATE erp.erp_sales_order SET status = 'SHIPPED' WHERE order_id = p_order_id;

  -- ---- feature block ship : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','ship','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','ship',SQLERRM);
  END;
  -- ---- feature block ship : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','ship','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','ship',SQLERRM);
  END;
  -- ---- feature block ship : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','ship','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','ship',SQLERRM);
  END;
  -- ---- feature block ship : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','ship','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','ship',SQLERRM);
  END;
  -- ---- feature block ship : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','ship','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','ship',SQLERRM);
  END;
  -- ---- feature block ship : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','ship','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','ship',SQLERRM);
  END;
  -- ---- feature block ship : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_ship (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_ship(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_ship.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_ship(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_ship.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_ship;
    CALL erp.erp_usp_write_audit_trail('UPSERT','ship','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','ship',SQLERRM);
  END;
  -- ---- feature block ship : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_ship')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_ship', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_ship';
    CALL erp.erp_usp_write_audit_trail('MERGE','ship','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','ship',SQLERRM);
  END;
  -- ---- feature block ship : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','ship','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','ship',SQLERRM);
  END;
  -- ---- feature block ship : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','ship','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','ship','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','ship',SQLERRM);
  END;
  -- ---- feature block ship : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','ship',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','ship',SQLERRM);
  END;
  -- ---- feature block ship : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','ship','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','ship',SQLERRM);
  END;
  -- ---- feature block ship : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'ship', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','ship','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','ship',SQLERRM);
  END;
  -- ---- feature block ship : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'ship', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','ship','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','ship',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('SHIP_DONE','ERP_SHIPMENT','shipment '||v_ship_id);

  CALL erp.erp_usp_generate_invoice(p_order_id);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_SHIP_ORDER_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_complete_work_order(p_wo_id int)
LANGUAGE plpgsql AS $$
DECLARE
  v_product_id int; v_bom_id int; v_qty numeric; v_inv_id int; v_related_order int;
BEGIN
  CALL erp.erp_usp_write_audit_trail('WO_START','ERP_WORK_ORDER','WO '||p_wo_id);
  SELECT product_id, bom_id, qty_planned INTO v_product_id, v_bom_id, v_qty
  FROM erp.erp_work_order WHERE wo_id = p_wo_id;
  FOR comp IN SELECT bc.component_id, bc.qty_per
              FROM erp.erp_bom_header bh
              JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
              WHERE bh.bom_id = v_bom_id LOOP
    INSERT INTO erp.erp_material_issue(issue_id, wo_id, component_id, qty_issued, issue_date)
    VALUES (nextval('erp.erp_wo_seq'), p_wo_id, comp.component_id, comp.qty_per * v_qty, now()::date);
    BEGIN
      SELECT inventory_id INTO STRICT v_inv_id FROM erp.erp_inventory WHERE product_id = comp.component_id LIMIT 1;
      INSERT INTO erp.erp_inventory_txn(txn_id, inventory_id, txn_type, qty, txn_date)
      VALUES (nextval('erp.erp_wo_seq'), v_inv_id, 'ISSUE', -1 * comp.qty_per * v_qty, now()::date);
    EXCEPTION WHEN NO_DATA_FOUND THEN NULL; END;
  END LOOP;
  BEGIN
    SELECT inventory_id INTO STRICT v_inv_id FROM erp.erp_inventory WHERE product_id = v_product_id LIMIT 1;
    INSERT INTO erp.erp_inventory_txn(txn_id, inventory_id, txn_type, qty, txn_date)
    VALUES (nextval('erp.erp_wo_seq'), v_inv_id, 'RECEIPT', v_qty, now()::date);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    CALL erp.erp_usp_write_audit_trail('WO_WARN','ERP_INVENTORY','no FG inv '||v_product_id);
  END;
  UPDATE erp.erp_work_order SET status='COMPLETE', qty_completed=v_qty WHERE wo_id=p_wo_id;

  -- ---- feature block wocmp : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','wocmp','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','wocmp',SQLERRM);
  END;
  -- ---- feature block wocmp : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','wocmp','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','wocmp',SQLERRM);
  END;
  -- ---- feature block wocmp : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','wocmp','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','wocmp',SQLERRM);
  END;
  -- ---- feature block wocmp : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','wocmp','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','wocmp',SQLERRM);
  END;
  -- ---- feature block wocmp : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','wocmp','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','wocmp',SQLERRM);
  END;
  -- ---- feature block wocmp : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','wocmp','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','wocmp',SQLERRM);
  END;
  -- ---- feature block wocmp : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_wocmp (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_wocmp(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_wocmp.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_wocmp(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_wocmp.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_wocmp;
    CALL erp.erp_usp_write_audit_trail('UPSERT','wocmp','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','wocmp',SQLERRM);
  END;
  -- ---- feature block wocmp : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_wocmp')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_wocmp', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_wocmp';
    CALL erp.erp_usp_write_audit_trail('MERGE','wocmp','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','wocmp',SQLERRM);
  END;
  -- ---- feature block wocmp : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','wocmp','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','wocmp',SQLERRM);
  END;
  -- ---- feature block wocmp : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','wocmp','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','wocmp','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','wocmp',SQLERRM);
  END;
  -- ---- feature block wocmp : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','wocmp',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','wocmp',SQLERRM);
  END;
  -- ---- feature block wocmp : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','wocmp','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','wocmp',SQLERRM);
  END;
  -- ---- feature block wocmp : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'wocmp', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','wocmp','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','wocmp',SQLERRM);
  END;
  -- ---- feature block wocmp : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'wocmp', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','wocmp','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','wocmp',SQLERRM);
  END;

  BEGIN
    SELECT so.order_id INTO STRICT v_related_order
    FROM erp.erp_sales_order so
    JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
    WHERE sol.product_id = v_product_id AND so.status='NEW' LIMIT 1;
    CALL erp.erp_usp_ship_order(v_related_order);
  EXCEPTION WHEN NO_DATA_FOUND THEN NULL; END;
  CALL erp.erp_usp_write_audit_trail('WO_COMPLETE','ERP_WORK_ORDER','completed '||p_wo_id);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_COMPLETE_WORK_ORDER_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_release_work_orders(p_mrp_run_id int)
LANGUAGE plpgsql AS $$
DECLARE
  v_wo_id int; v_bom_id int; v_rt_id int; v_count int := 0;
BEGIN
  CALL erp.erp_usp_write_audit_trail('WO_REL_START','ERP_WORK_ORDER','Run '||p_mrp_run_id);
  FOR plan IN SELECT mp.plan_id, mp.product_id, mp.net_req
              FROM erp.erp_mrp_plan mp
              WHERE mp.mrp_run_id = p_mrp_run_id AND mp.action = 'MAKE' AND mp.net_req > 0 LOOP
    BEGIN
      SELECT bom_id INTO STRICT v_bom_id FROM erp.erp_bom_header WHERE product_id = plan.product_id AND active_flag='Y' LIMIT 1;
    EXCEPTION WHEN NO_DATA_FOUND THEN v_bom_id := NULL; END;
    BEGIN
      SELECT routing_id INTO STRICT v_rt_id FROM erp.erp_routing WHERE product_id = plan.product_id LIMIT 1;
    EXCEPTION WHEN NO_DATA_FOUND THEN v_rt_id := NULL; END;
    v_wo_id := nextval('erp.erp_wo_seq');
    INSERT INTO erp.erp_work_order(wo_id, product_id, bom_id, routing_id, qty_planned, status, due_date)
    VALUES (v_wo_id, plan.product_id, v_bom_id, v_rt_id, plan.net_req, 'RELEASED', now()::date + 7);
    IF v_bom_id IS NOT NULL THEN
      CALL erp.erp_usp_complete_work_order(v_wo_id);
    END IF;
    v_count := v_count + 1;
  END LOOP;

  -- ---- feature block worel : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','worel','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','worel',SQLERRM);
  END;
  -- ---- feature block worel : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','worel','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','worel',SQLERRM);
  END;
  -- ---- feature block worel : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','worel','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','worel',SQLERRM);
  END;
  -- ---- feature block worel : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','worel','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','worel',SQLERRM);
  END;
  -- ---- feature block worel : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','worel','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','worel',SQLERRM);
  END;
  -- ---- feature block worel : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','worel','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','worel',SQLERRM);
  END;
  -- ---- feature block worel : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_worel (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_worel(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_worel.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_worel(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_worel.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_worel;
    CALL erp.erp_usp_write_audit_trail('UPSERT','worel','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','worel',SQLERRM);
  END;
  -- ---- feature block worel : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_worel')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_worel', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_worel';
    CALL erp.erp_usp_write_audit_trail('MERGE','worel','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','worel',SQLERRM);
  END;
  -- ---- feature block worel : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','worel','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','worel',SQLERRM);
  END;
  -- ---- feature block worel : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','worel','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','worel','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','worel',SQLERRM);
  END;
  -- ---- feature block worel : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','worel',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','worel',SQLERRM);
  END;
  -- ---- feature block worel : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','worel','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','worel',SQLERRM);
  END;
  -- ---- feature block worel : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'worel', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','worel','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','worel',SQLERRM);
  END;
  -- ---- feature block worel : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'worel', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','worel','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','worel',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('WO_REL','ERP_WORK_ORDER','released '||v_count);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_RELEASE_WORK_ORDERS_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_compute_net_requirements(p_mrp_run_id int)
LANGUAGE plpgsql AS $$
DECLARE
  v_action text; v_planned int := 0;
BEGIN
  CALL erp.erp_usp_write_audit_trail('NETREQ_START','ERP_MRP_PLAN','Run '||p_mrp_run_id);
  FOR pr IN SELECT product_id FROM erp.erp_product WHERE make_buy IN ('M','B') LOOP
    INSERT INTO erp.erp_mrp_plan(plan_id, mrp_run_id, product_id, gross_req, net_req, action)
    VALUES (nextval('erp.erp_mrp_seq'), p_mrp_run_id, pr.product_id,
            erp.erp_fn_gross_requirement(pr.product_id),
            erp.erp_fn_net_requirement(pr.product_id), 'PENDING');
  END LOOP;
  FOR rec IN SELECT plan_id, product_id, net_req FROM erp.erp_mrp_plan WHERE mrp_run_id = p_mrp_run_id LOOP
    IF rec.net_req <= 0 THEN v_action := 'NONE';
    ELSE
      SELECT make_buy INTO v_action FROM erp.erp_product WHERE product_id = rec.product_id;
      v_action := CASE WHEN v_action='M' THEN 'MAKE' ELSE 'BUY' END;
      v_planned := v_planned + 1;
    END IF;
    UPDATE erp.erp_mrp_plan SET action = v_action WHERE plan_id = rec.plan_id;
  END LOOP;
  UPDATE erp.erp_mrp_run SET planned_count = v_planned WHERE mrp_run_id = p_mrp_run_id;

  -- ---- feature block netreq : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','netreq','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','netreq',SQLERRM);
  END;
  -- ---- feature block netreq : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','netreq','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','netreq',SQLERRM);
  END;
  -- ---- feature block netreq : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','netreq','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','netreq',SQLERRM);
  END;
  -- ---- feature block netreq : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','netreq','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','netreq',SQLERRM);
  END;
  -- ---- feature block netreq : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','netreq','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','netreq',SQLERRM);
  END;
  -- ---- feature block netreq : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','netreq','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','netreq',SQLERRM);
  END;
  -- ---- feature block netreq : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_netreq (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_netreq(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_netreq.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_netreq(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_netreq.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_netreq;
    CALL erp.erp_usp_write_audit_trail('UPSERT','netreq','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','netreq',SQLERRM);
  END;
  -- ---- feature block netreq : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_netreq')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_netreq', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_netreq';
    CALL erp.erp_usp_write_audit_trail('MERGE','netreq','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','netreq',SQLERRM);
  END;
  -- ---- feature block netreq : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','netreq','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','netreq',SQLERRM);
  END;
  -- ---- feature block netreq : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','netreq','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','netreq','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','netreq',SQLERRM);
  END;
  -- ---- feature block netreq : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','netreq',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','netreq',SQLERRM);
  END;
  -- ---- feature block netreq : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','netreq','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','netreq',SQLERRM);
  END;
  -- ---- feature block netreq : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'netreq', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','netreq','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','netreq',SQLERRM);
  END;
  -- ---- feature block netreq : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'netreq', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','netreq','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','netreq',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('NETREQ_DONE','ERP_MRP_PLAN','planned='||v_planned);

  CALL erp.erp_usp_release_work_orders(p_mrp_run_id);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_COMPUTE_NET_REQUIREMENTS_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_explode_demand(p_mrp_run_id int)
LANGUAGE plpgsql AS $$
DECLARE
  v_dep int := 0;
BEGIN
  CALL erp.erp_usp_write_audit_trail('EXPLODE_START','ERP_DEMAND','Run '||p_mrp_run_id);
  FOR dep IN WITH RECURSIVE bom_tree(top_product, component_id, bom_level, qty_per) AS (
               SELECT bh.product_id, bc.component_id, 1, bc.qty_per
               FROM erp.erp_bom_component bc
               JOIN erp.erp_bom_header bh ON bh.bom_id = bc.bom_id
               WHERE bh.product_id IN (SELECT product_id FROM erp.erp_sales_order_line)
               UNION ALL
               SELECT t.top_product, bc.component_id, t.bom_level + 1, bc.qty_per
               FROM bom_tree t
               JOIN erp.erp_bom_header bh ON bh.product_id = t.component_id
               JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
               WHERE t.bom_level < 10
             )
             SELECT top_product, component_id, bom_level, qty_per FROM bom_tree LOOP
    INSERT INTO erp.erp_demand(demand_id, product_id, source_type, source_id, qty, need_date)
    VALUES (nextval('erp.erp_mrp_seq'), dep.component_id, 'DEPENDENT', p_mrp_run_id, dep.qty_per, now()::date + dep.bom_level);
    v_dep := v_dep + 1;
  END LOOP;

  -- ---- feature block explode : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','explode','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','explode',SQLERRM);
  END;
  -- ---- feature block explode : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','explode','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','explode',SQLERRM);
  END;
  -- ---- feature block explode : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','explode','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','explode',SQLERRM);
  END;
  -- ---- feature block explode : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','explode','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','explode',SQLERRM);
  END;
  -- ---- feature block explode : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','explode','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','explode',SQLERRM);
  END;
  -- ---- feature block explode : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','explode','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','explode',SQLERRM);
  END;
  -- ---- feature block explode : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_explode (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_explode(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_explode.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_explode(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_explode.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_explode;
    CALL erp.erp_usp_write_audit_trail('UPSERT','explode','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','explode',SQLERRM);
  END;
  -- ---- feature block explode : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_explode')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_explode', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_explode';
    CALL erp.erp_usp_write_audit_trail('MERGE','explode','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','explode',SQLERRM);
  END;
  -- ---- feature block explode : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','explode','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','explode',SQLERRM);
  END;
  -- ---- feature block explode : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','explode','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','explode','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','explode',SQLERRM);
  END;
  -- ---- feature block explode : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','explode',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','explode',SQLERRM);
  END;
  -- ---- feature block explode : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','explode','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','explode',SQLERRM);
  END;
  -- ---- feature block explode : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'explode', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','explode','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','explode',SQLERRM);
  END;
  -- ---- feature block explode : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'explode', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','explode','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','explode',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('EXPLODE_DONE','ERP_DEMAND','dependent='||v_dep);

  CALL erp.erp_usp_compute_net_requirements(p_mrp_run_id);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_EXPLODE_DEMAND_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_collect_demand(p_mrp_run_id int)
LANGUAGE plpgsql AS $$
DECLARE
  v_cnt int := 0;
BEGIN
  CALL erp.erp_usp_write_audit_trail('COLLECT_START','ERP_DEMAND','Run '||p_mrp_run_id);
  INSERT INTO erp.erp_demand(demand_id, product_id, source_type, source_id, qty, need_date)
  SELECT nextval('erp.erp_mrp_seq'), sol.product_id, 'SALES_ORDER', so.order_id, sol.qty, so.order_date + 14
  FROM erp.erp_sales_order so
  JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
  WHERE so.status IN ('NEW','CONFIRMED');
  GET DIAGNOSTICS v_cnt = ROW_COUNT;
  INSERT INTO erp.erp_demand(demand_id, product_id, source_type, source_id, qty, need_date)
  SELECT nextval('erp.erp_mrp_seq'), product_id, 'FORECAST', forecast_id, qty, now()::date + 30
  FROM erp.erp_forecast;

  -- ---- feature block collect : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','collect','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','collect',SQLERRM);
  END;
  -- ---- feature block collect : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','collect','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','collect',SQLERRM);
  END;
  -- ---- feature block collect : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','collect','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','collect',SQLERRM);
  END;
  -- ---- feature block collect : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','collect','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','collect',SQLERRM);
  END;
  -- ---- feature block collect : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','collect','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','collect',SQLERRM);
  END;
  -- ---- feature block collect : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','collect','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','collect',SQLERRM);
  END;
  -- ---- feature block collect : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_collect (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_collect(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_collect.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_collect(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_collect.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_collect;
    CALL erp.erp_usp_write_audit_trail('UPSERT','collect','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','collect',SQLERRM);
  END;
  -- ---- feature block collect : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_collect')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_collect', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_collect';
    CALL erp.erp_usp_write_audit_trail('MERGE','collect','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','collect',SQLERRM);
  END;
  -- ---- feature block collect : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','collect','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','collect',SQLERRM);
  END;
  -- ---- feature block collect : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','collect','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','collect','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','collect',SQLERRM);
  END;
  -- ---- feature block collect : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','collect',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','collect',SQLERRM);
  END;
  -- ---- feature block collect : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','collect','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','collect',SQLERRM);
  END;
  -- ---- feature block collect : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'collect', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','collect','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','collect',SQLERRM);
  END;
  -- ---- feature block collect : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'collect', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','collect','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','collect',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('MRP_DEMAND','ERP_DEMAND','collected '||v_cnt);

  CALL erp.erp_usp_explode_demand(p_mrp_run_id);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_COLLECT_DEMAND_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_validate_master_data(p_mrp_run_id int)
LANGUAGE plpgsql AS $$
DECLARE
  v_missing_bom int := 0; v_neg_inv int := 0;
BEGIN
  CALL erp.erp_usp_write_audit_trail('VALIDATE_START','ERP_MRP_RUN','Run '||p_mrp_run_id);
  SELECT COUNT(*) INTO v_missing_bom
  FROM erp.erp_product p
  WHERE p.make_buy = 'M'
    AND NOT EXISTS (SELECT 1 FROM erp.erp_bom_header b WHERE b.product_id = p.product_id AND b.active_flag='Y');
  SELECT COUNT(*) INTO v_neg_inv FROM erp.erp_inventory WHERE qty_on_hand < 0;
  IF v_missing_bom > 0 THEN
    CALL erp.erp_usp_write_audit_trail('VALIDATE_WARN','ERP_PRODUCT',v_missing_bom||' missing BOM');
  END IF;
  IF v_neg_inv > 0 THEN
    CALL erp.erp_usp_write_audit_trail('VALIDATE_WARN','ERP_INVENTORY',v_neg_inv||' negative inv');
  END IF;

  -- ---- feature block valid : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','valid','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','valid',SQLERRM);
  END;
  -- ---- feature block valid : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','valid','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','valid',SQLERRM);
  END;
  -- ---- feature block valid : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','valid','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','valid',SQLERRM);
  END;
  -- ---- feature block valid : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','valid','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','valid',SQLERRM);
  END;
  -- ---- feature block valid : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','valid','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','valid',SQLERRM);
  END;
  -- ---- feature block valid : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','valid','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','valid',SQLERRM);
  END;
  -- ---- feature block valid : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_valid (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_valid(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_valid.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_valid(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_valid.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_valid;
    CALL erp.erp_usp_write_audit_trail('UPSERT','valid','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','valid',SQLERRM);
  END;
  -- ---- feature block valid : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_valid')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_valid', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_valid';
    CALL erp.erp_usp_write_audit_trail('MERGE','valid','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','valid',SQLERRM);
  END;
  -- ---- feature block valid : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','valid','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','valid',SQLERRM);
  END;
  -- ---- feature block valid : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','valid','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','valid','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','valid',SQLERRM);
  END;
  -- ---- feature block valid : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','valid',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','valid',SQLERRM);
  END;
  -- ---- feature block valid : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','valid','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','valid',SQLERRM);
  END;
  -- ---- feature block valid : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'valid', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','valid','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','valid',SQLERRM);
  END;
  -- ---- feature block valid : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'valid', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','valid','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','valid',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('VALIDATE_DONE','ERP_MRP_RUN','missing='||v_missing_bom||' neg='||v_neg_inv);

  CALL erp.erp_usp_collect_demand(p_mrp_run_id);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_VALIDATE_MASTER_DATA_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_init_mrp_run(INOUT p_mrp_run_id int)
LANGUAGE plpgsql AS $$
DECLARE
  v_run_id int; v_horizon int;
BEGIN
  v_run_id := nextval('erp.erp_mrp_seq');
  v_horizon := COALESCE(erp.erp_fn_get_config('MRP_HORIZON_DAYS')::int, 30);
  CALL erp.erp_usp_write_audit_trail('INIT_START','ERP_MRP_RUN','Run '||v_run_id);
  INSERT INTO erp.erp_mrp_run(mrp_run_id, run_date, status, planned_count)
  VALUES (v_run_id, now()::date, 'RUNNING', 0);
  DELETE FROM erp.erp_demand WHERE source_type IN ('SALES_ORDER','FORECAST','DEPENDENT');
  p_mrp_run_id := v_run_id;

  -- ---- feature block init : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','init','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','init',SQLERRM);
  END;
  -- ---- feature block init : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','init','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','init',SQLERRM);
  END;
  -- ---- feature block init : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','init','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','init',SQLERRM);
  END;
  -- ---- feature block init : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','init','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','init',SQLERRM);
  END;
  -- ---- feature block init : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','init','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','init',SQLERRM);
  END;
  -- ---- feature block init : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','init','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','init',SQLERRM);
  END;
  -- ---- feature block init : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_init (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_init(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_init.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_init(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_init.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_init;
    CALL erp.erp_usp_write_audit_trail('UPSERT','init','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','init',SQLERRM);
  END;
  -- ---- feature block init : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_init')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_init', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_init';
    CALL erp.erp_usp_write_audit_trail('MERGE','init','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','init',SQLERRM);
  END;
  -- ---- feature block init : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','init','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','init',SQLERRM);
  END;
  -- ---- feature block init : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','init','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','init','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','init',SQLERRM);
  END;
  -- ---- feature block init : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','init',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','init',SQLERRM);
  END;
  -- ---- feature block init : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','init','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','init',SQLERRM);
  END;
  -- ---- feature block init : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'init', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','init','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','init',SQLERRM);
  END;
  -- ---- feature block init : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'init', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','init','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','init',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('INIT_DONE','ERP_MRP_RUN','horizon='||v_horizon);

  CALL erp.erp_usp_validate_master_data(v_run_id);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_INIT_MRP_RUN_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_01(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 1;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_01','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg01 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg01','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg01',SQLERRM);
  END;
  -- ---- feature block stg01 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg01','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg01',SQLERRM);
  END;
  -- ---- feature block stg01 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg01','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg01',SQLERRM);
  END;
  -- ---- feature block stg01 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg01','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg01',SQLERRM);
  END;
  -- ---- feature block stg01 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg01','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg01',SQLERRM);
  END;
  -- ---- feature block stg01 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg01','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg01',SQLERRM);
  END;
  -- ---- feature block stg01 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg01 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg01(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg01.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg01(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg01.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg01;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg01','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg01',SQLERRM);
  END;
  -- ---- feature block stg01 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg01')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg01', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg01';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg01','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg01',SQLERRM);
  END;
  -- ---- feature block stg01 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg01','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg01',SQLERRM);
  END;
  -- ---- feature block stg01 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg01','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg01','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg01',SQLERRM);
  END;
  -- ---- feature block stg01 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg01',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg01',SQLERRM);
  END;
  -- ---- feature block stg01 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg01','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg01',SQLERRM);
  END;
  -- ---- feature block stg01 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg01', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg01','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg01',SQLERRM);
  END;
  -- ---- feature block stg01 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg01', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg01','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg01',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_01','done depth '||p_depth);

  CALL erp.erp_usp_stage_02(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_01_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_02(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 2;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_02','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg02 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg02','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg02',SQLERRM);
  END;
  -- ---- feature block stg02 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg02','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg02',SQLERRM);
  END;
  -- ---- feature block stg02 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg02','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg02',SQLERRM);
  END;
  -- ---- feature block stg02 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg02','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg02',SQLERRM);
  END;
  -- ---- feature block stg02 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg02','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg02',SQLERRM);
  END;
  -- ---- feature block stg02 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg02','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg02',SQLERRM);
  END;
  -- ---- feature block stg02 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg02 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg02(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg02.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg02(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg02.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg02;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg02','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg02',SQLERRM);
  END;
  -- ---- feature block stg02 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg02')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg02', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg02';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg02','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg02',SQLERRM);
  END;
  -- ---- feature block stg02 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg02','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg02',SQLERRM);
  END;
  -- ---- feature block stg02 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg02','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg02','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg02',SQLERRM);
  END;
  -- ---- feature block stg02 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg02',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg02',SQLERRM);
  END;
  -- ---- feature block stg02 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg02','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg02',SQLERRM);
  END;
  -- ---- feature block stg02 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg02', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg02','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg02',SQLERRM);
  END;
  -- ---- feature block stg02 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg02', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg02','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg02',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_02','done depth '||p_depth);

  CALL erp.erp_usp_stage_03(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_02_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_03(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 3;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_03','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg03 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg03','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg03',SQLERRM);
  END;
  -- ---- feature block stg03 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg03','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg03',SQLERRM);
  END;
  -- ---- feature block stg03 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg03','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg03',SQLERRM);
  END;
  -- ---- feature block stg03 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg03','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg03',SQLERRM);
  END;
  -- ---- feature block stg03 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg03','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg03',SQLERRM);
  END;
  -- ---- feature block stg03 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg03','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg03',SQLERRM);
  END;
  -- ---- feature block stg03 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg03 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg03(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg03.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg03(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg03.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg03;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg03','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg03',SQLERRM);
  END;
  -- ---- feature block stg03 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg03')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg03', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg03';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg03','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg03',SQLERRM);
  END;
  -- ---- feature block stg03 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg03','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg03',SQLERRM);
  END;
  -- ---- feature block stg03 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg03','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg03','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg03',SQLERRM);
  END;
  -- ---- feature block stg03 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg03',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg03',SQLERRM);
  END;
  -- ---- feature block stg03 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg03','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg03',SQLERRM);
  END;
  -- ---- feature block stg03 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg03', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg03','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg03',SQLERRM);
  END;
  -- ---- feature block stg03 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg03', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg03','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg03',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_03','done depth '||p_depth);

  CALL erp.erp_usp_stage_04(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_03_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_04(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 4;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_04','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg04 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg04','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg04',SQLERRM);
  END;
  -- ---- feature block stg04 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg04','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg04',SQLERRM);
  END;
  -- ---- feature block stg04 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg04','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg04',SQLERRM);
  END;
  -- ---- feature block stg04 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg04','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg04',SQLERRM);
  END;
  -- ---- feature block stg04 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg04','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg04',SQLERRM);
  END;
  -- ---- feature block stg04 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg04','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg04',SQLERRM);
  END;
  -- ---- feature block stg04 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg04 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg04(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg04.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg04(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg04.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg04;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg04','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg04',SQLERRM);
  END;
  -- ---- feature block stg04 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg04')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg04', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg04';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg04','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg04',SQLERRM);
  END;
  -- ---- feature block stg04 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg04','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg04',SQLERRM);
  END;
  -- ---- feature block stg04 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg04','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg04','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg04',SQLERRM);
  END;
  -- ---- feature block stg04 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg04',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg04',SQLERRM);
  END;
  -- ---- feature block stg04 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg04','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg04',SQLERRM);
  END;
  -- ---- feature block stg04 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg04', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg04','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg04',SQLERRM);
  END;
  -- ---- feature block stg04 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg04', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg04','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg04',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_04','done depth '||p_depth);

  CALL erp.erp_usp_stage_05(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_04_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_05(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 5;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_05','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg05 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg05','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg05',SQLERRM);
  END;
  -- ---- feature block stg05 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg05','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg05',SQLERRM);
  END;
  -- ---- feature block stg05 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg05','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg05',SQLERRM);
  END;
  -- ---- feature block stg05 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg05','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg05',SQLERRM);
  END;
  -- ---- feature block stg05 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg05','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg05',SQLERRM);
  END;
  -- ---- feature block stg05 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg05','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg05',SQLERRM);
  END;
  -- ---- feature block stg05 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg05 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg05(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg05.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg05(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg05.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg05;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg05','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg05',SQLERRM);
  END;
  -- ---- feature block stg05 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg05')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg05', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg05';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg05','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg05',SQLERRM);
  END;
  -- ---- feature block stg05 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg05','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg05',SQLERRM);
  END;
  -- ---- feature block stg05 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg05','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg05','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg05',SQLERRM);
  END;
  -- ---- feature block stg05 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg05',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg05',SQLERRM);
  END;
  -- ---- feature block stg05 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg05','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg05',SQLERRM);
  END;
  -- ---- feature block stg05 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg05', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg05','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg05',SQLERRM);
  END;
  -- ---- feature block stg05 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg05', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg05','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg05',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_05','done depth '||p_depth);

  CALL erp.erp_usp_stage_06(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_05_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_06(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 6;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_06','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg06 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg06','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg06',SQLERRM);
  END;
  -- ---- feature block stg06 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg06','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg06',SQLERRM);
  END;
  -- ---- feature block stg06 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg06','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg06',SQLERRM);
  END;
  -- ---- feature block stg06 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg06','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg06',SQLERRM);
  END;
  -- ---- feature block stg06 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg06','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg06',SQLERRM);
  END;
  -- ---- feature block stg06 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg06','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg06',SQLERRM);
  END;
  -- ---- feature block stg06 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg06 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg06(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg06.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg06(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg06.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg06;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg06','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg06',SQLERRM);
  END;
  -- ---- feature block stg06 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg06')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg06', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg06';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg06','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg06',SQLERRM);
  END;
  -- ---- feature block stg06 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg06','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg06',SQLERRM);
  END;
  -- ---- feature block stg06 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg06','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg06','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg06',SQLERRM);
  END;
  -- ---- feature block stg06 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg06',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg06',SQLERRM);
  END;
  -- ---- feature block stg06 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg06','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg06',SQLERRM);
  END;
  -- ---- feature block stg06 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg06', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg06','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg06',SQLERRM);
  END;
  -- ---- feature block stg06 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg06', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg06','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg06',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_06','done depth '||p_depth);

  CALL erp.erp_usp_stage_07(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_06_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_07(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 7;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_07','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg07 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg07','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg07',SQLERRM);
  END;
  -- ---- feature block stg07 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg07','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg07',SQLERRM);
  END;
  -- ---- feature block stg07 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg07','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg07',SQLERRM);
  END;
  -- ---- feature block stg07 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg07','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg07',SQLERRM);
  END;
  -- ---- feature block stg07 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg07','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg07',SQLERRM);
  END;
  -- ---- feature block stg07 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg07','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg07',SQLERRM);
  END;
  -- ---- feature block stg07 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg07 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg07(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg07.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg07(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg07.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg07;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg07','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg07',SQLERRM);
  END;
  -- ---- feature block stg07 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg07')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg07', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg07';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg07','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg07',SQLERRM);
  END;
  -- ---- feature block stg07 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg07','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg07',SQLERRM);
  END;
  -- ---- feature block stg07 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg07','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg07','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg07',SQLERRM);
  END;
  -- ---- feature block stg07 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg07',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg07',SQLERRM);
  END;
  -- ---- feature block stg07 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg07','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg07',SQLERRM);
  END;
  -- ---- feature block stg07 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg07', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg07','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg07',SQLERRM);
  END;
  -- ---- feature block stg07 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg07', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg07','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg07',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_07','done depth '||p_depth);

  CALL erp.erp_usp_stage_08(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_07_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_08(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 8;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_08','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg08 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg08','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg08',SQLERRM);
  END;
  -- ---- feature block stg08 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg08','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg08',SQLERRM);
  END;
  -- ---- feature block stg08 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg08','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg08',SQLERRM);
  END;
  -- ---- feature block stg08 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg08','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg08',SQLERRM);
  END;
  -- ---- feature block stg08 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg08','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg08',SQLERRM);
  END;
  -- ---- feature block stg08 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg08','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg08',SQLERRM);
  END;
  -- ---- feature block stg08 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg08 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg08(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg08.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg08(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg08.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg08;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg08','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg08',SQLERRM);
  END;
  -- ---- feature block stg08 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg08')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg08', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg08';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg08','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg08',SQLERRM);
  END;
  -- ---- feature block stg08 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg08','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg08',SQLERRM);
  END;
  -- ---- feature block stg08 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg08','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg08','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg08',SQLERRM);
  END;
  -- ---- feature block stg08 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg08',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg08',SQLERRM);
  END;
  -- ---- feature block stg08 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg08','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg08',SQLERRM);
  END;
  -- ---- feature block stg08 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg08', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg08','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg08',SQLERRM);
  END;
  -- ---- feature block stg08 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg08', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg08','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg08',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_08','done depth '||p_depth);

  CALL erp.erp_usp_stage_09(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_08_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_09(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 9;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_09','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg09 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg09','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg09',SQLERRM);
  END;
  -- ---- feature block stg09 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg09','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg09',SQLERRM);
  END;
  -- ---- feature block stg09 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg09','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg09',SQLERRM);
  END;
  -- ---- feature block stg09 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg09','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg09',SQLERRM);
  END;
  -- ---- feature block stg09 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg09','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg09',SQLERRM);
  END;
  -- ---- feature block stg09 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg09','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg09',SQLERRM);
  END;
  -- ---- feature block stg09 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg09 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg09(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg09.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg09(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg09.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg09;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg09','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg09',SQLERRM);
  END;
  -- ---- feature block stg09 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg09')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg09', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg09';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg09','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg09',SQLERRM);
  END;
  -- ---- feature block stg09 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg09','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg09',SQLERRM);
  END;
  -- ---- feature block stg09 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg09','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg09','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg09',SQLERRM);
  END;
  -- ---- feature block stg09 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg09',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg09',SQLERRM);
  END;
  -- ---- feature block stg09 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg09','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg09',SQLERRM);
  END;
  -- ---- feature block stg09 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg09', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg09','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg09',SQLERRM);
  END;
  -- ---- feature block stg09 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg09', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg09','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg09',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_09','done depth '||p_depth);

  CALL erp.erp_usp_stage_10(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_09_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_10(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 10;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_10','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg10 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg10','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg10',SQLERRM);
  END;
  -- ---- feature block stg10 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg10','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg10',SQLERRM);
  END;
  -- ---- feature block stg10 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg10','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg10',SQLERRM);
  END;
  -- ---- feature block stg10 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg10','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg10',SQLERRM);
  END;
  -- ---- feature block stg10 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg10','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg10',SQLERRM);
  END;
  -- ---- feature block stg10 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg10','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg10',SQLERRM);
  END;
  -- ---- feature block stg10 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg10 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg10(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg10.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg10(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg10.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg10;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg10','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg10',SQLERRM);
  END;
  -- ---- feature block stg10 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg10')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg10', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg10';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg10','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg10',SQLERRM);
  END;
  -- ---- feature block stg10 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg10','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg10',SQLERRM);
  END;
  -- ---- feature block stg10 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg10','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg10','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg10',SQLERRM);
  END;
  -- ---- feature block stg10 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg10',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg10',SQLERRM);
  END;
  -- ---- feature block stg10 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg10','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg10',SQLERRM);
  END;
  -- ---- feature block stg10 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg10', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg10','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg10',SQLERRM);
  END;
  -- ---- feature block stg10 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg10', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg10','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg10',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_10','done depth '||p_depth);

  CALL erp.erp_usp_stage_11(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_10_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_11(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 11;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_11','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg11 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg11','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg11',SQLERRM);
  END;
  -- ---- feature block stg11 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg11','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg11',SQLERRM);
  END;
  -- ---- feature block stg11 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg11','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg11',SQLERRM);
  END;
  -- ---- feature block stg11 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg11','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg11',SQLERRM);
  END;
  -- ---- feature block stg11 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg11','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg11',SQLERRM);
  END;
  -- ---- feature block stg11 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg11','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg11',SQLERRM);
  END;
  -- ---- feature block stg11 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg11 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg11(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg11.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg11(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg11.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg11;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg11','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg11',SQLERRM);
  END;
  -- ---- feature block stg11 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg11')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg11', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg11';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg11','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg11',SQLERRM);
  END;
  -- ---- feature block stg11 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg11','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg11',SQLERRM);
  END;
  -- ---- feature block stg11 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg11','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg11','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg11',SQLERRM);
  END;
  -- ---- feature block stg11 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg11',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg11',SQLERRM);
  END;
  -- ---- feature block stg11 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg11','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg11',SQLERRM);
  END;
  -- ---- feature block stg11 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg11', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg11','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg11',SQLERRM);
  END;
  -- ---- feature block stg11 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg11', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg11','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg11',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_11','done depth '||p_depth);

  CALL erp.erp_usp_stage_12(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_11_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_12(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 12;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_12','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg12 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg12','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg12',SQLERRM);
  END;
  -- ---- feature block stg12 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg12','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg12',SQLERRM);
  END;
  -- ---- feature block stg12 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg12','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg12',SQLERRM);
  END;
  -- ---- feature block stg12 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg12','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg12',SQLERRM);
  END;
  -- ---- feature block stg12 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg12','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg12',SQLERRM);
  END;
  -- ---- feature block stg12 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg12','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg12',SQLERRM);
  END;
  -- ---- feature block stg12 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg12 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg12(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg12.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg12(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg12.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg12;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg12','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg12',SQLERRM);
  END;
  -- ---- feature block stg12 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg12')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg12', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg12';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg12','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg12',SQLERRM);
  END;
  -- ---- feature block stg12 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg12','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg12',SQLERRM);
  END;
  -- ---- feature block stg12 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg12','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg12','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg12',SQLERRM);
  END;
  -- ---- feature block stg12 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg12',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg12',SQLERRM);
  END;
  -- ---- feature block stg12 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg12','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg12',SQLERRM);
  END;
  -- ---- feature block stg12 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg12', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg12','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg12',SQLERRM);
  END;
  -- ---- feature block stg12 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg12', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg12','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg12',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_12','done depth '||p_depth);

  CALL erp.erp_usp_stage_13(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_12_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_13(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 13;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_13','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg13 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg13','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg13',SQLERRM);
  END;
  -- ---- feature block stg13 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg13','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg13',SQLERRM);
  END;
  -- ---- feature block stg13 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg13','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg13',SQLERRM);
  END;
  -- ---- feature block stg13 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg13','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg13',SQLERRM);
  END;
  -- ---- feature block stg13 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg13','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg13',SQLERRM);
  END;
  -- ---- feature block stg13 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg13','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg13',SQLERRM);
  END;
  -- ---- feature block stg13 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg13 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg13(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg13.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg13(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg13.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg13;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg13','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg13',SQLERRM);
  END;
  -- ---- feature block stg13 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg13')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg13', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg13';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg13','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg13',SQLERRM);
  END;
  -- ---- feature block stg13 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg13','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg13',SQLERRM);
  END;
  -- ---- feature block stg13 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg13','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg13','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg13',SQLERRM);
  END;
  -- ---- feature block stg13 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg13',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg13',SQLERRM);
  END;
  -- ---- feature block stg13 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg13','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg13',SQLERRM);
  END;
  -- ---- feature block stg13 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg13', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg13','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg13',SQLERRM);
  END;
  -- ---- feature block stg13 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg13', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg13','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg13',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_13','done depth '||p_depth);

  CALL erp.erp_usp_stage_14(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_13_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_14(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 14;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_14','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg14 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg14','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg14',SQLERRM);
  END;
  -- ---- feature block stg14 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg14','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg14',SQLERRM);
  END;
  -- ---- feature block stg14 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg14','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg14',SQLERRM);
  END;
  -- ---- feature block stg14 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg14','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg14',SQLERRM);
  END;
  -- ---- feature block stg14 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg14','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg14',SQLERRM);
  END;
  -- ---- feature block stg14 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg14','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg14',SQLERRM);
  END;
  -- ---- feature block stg14 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg14 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg14(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg14.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg14(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg14.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg14;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg14','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg14',SQLERRM);
  END;
  -- ---- feature block stg14 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg14')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg14', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg14';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg14','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg14',SQLERRM);
  END;
  -- ---- feature block stg14 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg14','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg14',SQLERRM);
  END;
  -- ---- feature block stg14 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg14','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg14','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg14',SQLERRM);
  END;
  -- ---- feature block stg14 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg14',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg14',SQLERRM);
  END;
  -- ---- feature block stg14 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg14','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg14',SQLERRM);
  END;
  -- ---- feature block stg14 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg14', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg14','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg14',SQLERRM);
  END;
  -- ---- feature block stg14 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg14', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg14','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg14',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_14','done depth '||p_depth);

  CALL erp.erp_usp_stage_15(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_14_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_15(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 15;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_15','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg15 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg15','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg15',SQLERRM);
  END;
  -- ---- feature block stg15 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg15','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg15',SQLERRM);
  END;
  -- ---- feature block stg15 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg15','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg15',SQLERRM);
  END;
  -- ---- feature block stg15 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg15','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg15',SQLERRM);
  END;
  -- ---- feature block stg15 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg15','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg15',SQLERRM);
  END;
  -- ---- feature block stg15 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg15','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg15',SQLERRM);
  END;
  -- ---- feature block stg15 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg15 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg15(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg15.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg15(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg15.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg15;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg15','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg15',SQLERRM);
  END;
  -- ---- feature block stg15 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg15')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg15', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg15';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg15','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg15',SQLERRM);
  END;
  -- ---- feature block stg15 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg15','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg15',SQLERRM);
  END;
  -- ---- feature block stg15 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg15','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg15','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg15',SQLERRM);
  END;
  -- ---- feature block stg15 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg15',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg15',SQLERRM);
  END;
  -- ---- feature block stg15 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg15','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg15',SQLERRM);
  END;
  -- ---- feature block stg15 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg15', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg15','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg15',SQLERRM);
  END;
  -- ---- feature block stg15 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg15', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg15','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg15',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_15','done depth '||p_depth);

  CALL erp.erp_usp_stage_16(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_15_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_16(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 16;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_16','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg16 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg16','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg16',SQLERRM);
  END;
  -- ---- feature block stg16 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg16','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg16',SQLERRM);
  END;
  -- ---- feature block stg16 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg16','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg16',SQLERRM);
  END;
  -- ---- feature block stg16 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg16','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg16',SQLERRM);
  END;
  -- ---- feature block stg16 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg16','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg16',SQLERRM);
  END;
  -- ---- feature block stg16 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg16','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg16',SQLERRM);
  END;
  -- ---- feature block stg16 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg16 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg16(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg16.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg16(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg16.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg16;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg16','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg16',SQLERRM);
  END;
  -- ---- feature block stg16 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg16')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg16', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg16';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg16','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg16',SQLERRM);
  END;
  -- ---- feature block stg16 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg16','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg16',SQLERRM);
  END;
  -- ---- feature block stg16 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg16','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg16','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg16',SQLERRM);
  END;
  -- ---- feature block stg16 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg16',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg16',SQLERRM);
  END;
  -- ---- feature block stg16 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg16','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg16',SQLERRM);
  END;
  -- ---- feature block stg16 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg16', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg16','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg16',SQLERRM);
  END;
  -- ---- feature block stg16 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg16', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg16','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg16',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_16','done depth '||p_depth);

  CALL erp.erp_usp_stage_17(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_16_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_17(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 17;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_17','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg17 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg17','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg17',SQLERRM);
  END;
  -- ---- feature block stg17 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg17','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg17',SQLERRM);
  END;
  -- ---- feature block stg17 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg17','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg17',SQLERRM);
  END;
  -- ---- feature block stg17 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg17','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg17',SQLERRM);
  END;
  -- ---- feature block stg17 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg17','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg17',SQLERRM);
  END;
  -- ---- feature block stg17 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg17','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg17',SQLERRM);
  END;
  -- ---- feature block stg17 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg17 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg17(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg17.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg17(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg17.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg17;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg17','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg17',SQLERRM);
  END;
  -- ---- feature block stg17 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg17')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg17', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg17';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg17','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg17',SQLERRM);
  END;
  -- ---- feature block stg17 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg17','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg17',SQLERRM);
  END;
  -- ---- feature block stg17 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg17','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg17','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg17',SQLERRM);
  END;
  -- ---- feature block stg17 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg17',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg17',SQLERRM);
  END;
  -- ---- feature block stg17 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg17','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg17',SQLERRM);
  END;
  -- ---- feature block stg17 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg17', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg17','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg17',SQLERRM);
  END;
  -- ---- feature block stg17 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg17', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg17','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg17',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_17','done depth '||p_depth);

  CALL erp.erp_usp_stage_18(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_17_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_18(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 18;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_18','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg18 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg18','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg18',SQLERRM);
  END;
  -- ---- feature block stg18 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg18','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg18',SQLERRM);
  END;
  -- ---- feature block stg18 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg18','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg18',SQLERRM);
  END;
  -- ---- feature block stg18 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg18','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg18',SQLERRM);
  END;
  -- ---- feature block stg18 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg18','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg18',SQLERRM);
  END;
  -- ---- feature block stg18 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg18','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg18',SQLERRM);
  END;
  -- ---- feature block stg18 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg18 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg18(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg18.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg18(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg18.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg18;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg18','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg18',SQLERRM);
  END;
  -- ---- feature block stg18 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg18')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg18', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg18';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg18','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg18',SQLERRM);
  END;
  -- ---- feature block stg18 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg18','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg18',SQLERRM);
  END;
  -- ---- feature block stg18 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg18','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg18','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg18',SQLERRM);
  END;
  -- ---- feature block stg18 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg18',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg18',SQLERRM);
  END;
  -- ---- feature block stg18 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg18','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg18',SQLERRM);
  END;
  -- ---- feature block stg18 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg18', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg18','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg18',SQLERRM);
  END;
  -- ---- feature block stg18 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg18', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg18','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg18',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_18','done depth '||p_depth);

  CALL erp.erp_usp_stage_19(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_18_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_19(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 19;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_19','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg19 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg19','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg19',SQLERRM);
  END;
  -- ---- feature block stg19 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg19','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg19',SQLERRM);
  END;
  -- ---- feature block stg19 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg19','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg19',SQLERRM);
  END;
  -- ---- feature block stg19 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg19','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg19',SQLERRM);
  END;
  -- ---- feature block stg19 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg19','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg19',SQLERRM);
  END;
  -- ---- feature block stg19 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg19','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg19',SQLERRM);
  END;
  -- ---- feature block stg19 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg19 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg19(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg19.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg19(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg19.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg19;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg19','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg19',SQLERRM);
  END;
  -- ---- feature block stg19 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg19')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg19', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg19';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg19','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg19',SQLERRM);
  END;
  -- ---- feature block stg19 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg19','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg19',SQLERRM);
  END;
  -- ---- feature block stg19 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg19','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg19','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg19',SQLERRM);
  END;
  -- ---- feature block stg19 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg19',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg19',SQLERRM);
  END;
  -- ---- feature block stg19 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg19','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg19',SQLERRM);
  END;
  -- ---- feature block stg19 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg19', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg19','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg19',SQLERRM);
  END;
  -- ---- feature block stg19 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg19', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg19','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg19',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_19','done depth '||p_depth);

  CALL erp.erp_usp_stage_20(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_19_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_20(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 20;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_20','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg20 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg20','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg20',SQLERRM);
  END;
  -- ---- feature block stg20 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg20','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg20',SQLERRM);
  END;
  -- ---- feature block stg20 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg20','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg20',SQLERRM);
  END;
  -- ---- feature block stg20 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg20','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg20',SQLERRM);
  END;
  -- ---- feature block stg20 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg20','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg20',SQLERRM);
  END;
  -- ---- feature block stg20 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg20','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg20',SQLERRM);
  END;
  -- ---- feature block stg20 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg20 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg20(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg20.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg20(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg20.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg20;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg20','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg20',SQLERRM);
  END;
  -- ---- feature block stg20 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg20')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg20', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg20';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg20','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg20',SQLERRM);
  END;
  -- ---- feature block stg20 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg20','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg20',SQLERRM);
  END;
  -- ---- feature block stg20 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg20','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg20','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg20',SQLERRM);
  END;
  -- ---- feature block stg20 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg20',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg20',SQLERRM);
  END;
  -- ---- feature block stg20 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg20','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg20',SQLERRM);
  END;
  -- ---- feature block stg20 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg20', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg20','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg20',SQLERRM);
  END;
  -- ---- feature block stg20 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg20', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg20','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg20',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_20','done depth '||p_depth);

  CALL erp.erp_usp_stage_21(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_20_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_21(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 21;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_21','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg21 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg21','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg21',SQLERRM);
  END;
  -- ---- feature block stg21 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg21','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg21',SQLERRM);
  END;
  -- ---- feature block stg21 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg21','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg21',SQLERRM);
  END;
  -- ---- feature block stg21 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg21','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg21',SQLERRM);
  END;
  -- ---- feature block stg21 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg21','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg21',SQLERRM);
  END;
  -- ---- feature block stg21 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg21','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg21',SQLERRM);
  END;
  -- ---- feature block stg21 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg21 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg21(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg21.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg21(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg21.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg21;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg21','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg21',SQLERRM);
  END;
  -- ---- feature block stg21 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg21')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg21', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg21';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg21','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg21',SQLERRM);
  END;
  -- ---- feature block stg21 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg21','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg21',SQLERRM);
  END;
  -- ---- feature block stg21 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg21','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg21','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg21',SQLERRM);
  END;
  -- ---- feature block stg21 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg21',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg21',SQLERRM);
  END;
  -- ---- feature block stg21 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg21','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg21',SQLERRM);
  END;
  -- ---- feature block stg21 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg21', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg21','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg21',SQLERRM);
  END;
  -- ---- feature block stg21 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg21', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg21','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg21',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_21','done depth '||p_depth);

  CALL erp.erp_usp_stage_22(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_21_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_22(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 22;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_22','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg22 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg22','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg22',SQLERRM);
  END;
  -- ---- feature block stg22 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg22','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg22',SQLERRM);
  END;
  -- ---- feature block stg22 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg22','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg22',SQLERRM);
  END;
  -- ---- feature block stg22 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg22','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg22',SQLERRM);
  END;
  -- ---- feature block stg22 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg22','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg22',SQLERRM);
  END;
  -- ---- feature block stg22 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg22','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg22',SQLERRM);
  END;
  -- ---- feature block stg22 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg22 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg22(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg22.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg22(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg22.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg22;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg22','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg22',SQLERRM);
  END;
  -- ---- feature block stg22 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg22')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg22', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg22';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg22','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg22',SQLERRM);
  END;
  -- ---- feature block stg22 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg22','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg22',SQLERRM);
  END;
  -- ---- feature block stg22 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg22','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg22','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg22',SQLERRM);
  END;
  -- ---- feature block stg22 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg22',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg22',SQLERRM);
  END;
  -- ---- feature block stg22 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg22','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg22',SQLERRM);
  END;
  -- ---- feature block stg22 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg22', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg22','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg22',SQLERRM);
  END;
  -- ---- feature block stg22 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg22', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg22','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg22',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_22','done depth '||p_depth);

  CALL erp.erp_usp_stage_23(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_22_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_23(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 23;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_23','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg23 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg23','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg23',SQLERRM);
  END;
  -- ---- feature block stg23 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg23','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg23',SQLERRM);
  END;
  -- ---- feature block stg23 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg23','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg23',SQLERRM);
  END;
  -- ---- feature block stg23 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg23','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg23',SQLERRM);
  END;
  -- ---- feature block stg23 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg23','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg23',SQLERRM);
  END;
  -- ---- feature block stg23 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg23','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg23',SQLERRM);
  END;
  -- ---- feature block stg23 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg23 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg23(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg23.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg23(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg23.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg23;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg23','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg23',SQLERRM);
  END;
  -- ---- feature block stg23 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg23')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg23', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg23';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg23','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg23',SQLERRM);
  END;
  -- ---- feature block stg23 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg23','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg23',SQLERRM);
  END;
  -- ---- feature block stg23 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg23','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg23','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg23',SQLERRM);
  END;
  -- ---- feature block stg23 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg23',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg23',SQLERRM);
  END;
  -- ---- feature block stg23 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg23','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg23',SQLERRM);
  END;
  -- ---- feature block stg23 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg23', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg23','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg23',SQLERRM);
  END;
  -- ---- feature block stg23 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg23', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg23','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg23',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_23','done depth '||p_depth);

  CALL erp.erp_usp_stage_24(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_23_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_24(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 24;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_24','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg24 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg24','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg24',SQLERRM);
  END;
  -- ---- feature block stg24 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg24','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg24',SQLERRM);
  END;
  -- ---- feature block stg24 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg24','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg24',SQLERRM);
  END;
  -- ---- feature block stg24 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg24','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg24',SQLERRM);
  END;
  -- ---- feature block stg24 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg24','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg24',SQLERRM);
  END;
  -- ---- feature block stg24 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg24','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg24',SQLERRM);
  END;
  -- ---- feature block stg24 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg24 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg24(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg24.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg24(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg24.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg24;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg24','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg24',SQLERRM);
  END;
  -- ---- feature block stg24 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg24')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg24', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg24';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg24','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg24',SQLERRM);
  END;
  -- ---- feature block stg24 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg24','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg24',SQLERRM);
  END;
  -- ---- feature block stg24 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg24','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg24','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg24',SQLERRM);
  END;
  -- ---- feature block stg24 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg24',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg24',SQLERRM);
  END;
  -- ---- feature block stg24 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg24','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg24',SQLERRM);
  END;
  -- ---- feature block stg24 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg24', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg24','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg24',SQLERRM);
  END;
  -- ---- feature block stg24 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg24', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg24','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg24',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_24','done depth '||p_depth);

  CALL erp.erp_usp_stage_25(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_24_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_25(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 25;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_25','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg25 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg25','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg25',SQLERRM);
  END;
  -- ---- feature block stg25 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg25','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg25',SQLERRM);
  END;
  -- ---- feature block stg25 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg25','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg25',SQLERRM);
  END;
  -- ---- feature block stg25 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg25','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg25',SQLERRM);
  END;
  -- ---- feature block stg25 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg25','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg25',SQLERRM);
  END;
  -- ---- feature block stg25 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg25','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg25',SQLERRM);
  END;
  -- ---- feature block stg25 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg25 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg25(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg25.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg25(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg25.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg25;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg25','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg25',SQLERRM);
  END;
  -- ---- feature block stg25 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg25')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg25', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg25';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg25','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg25',SQLERRM);
  END;
  -- ---- feature block stg25 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg25','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg25',SQLERRM);
  END;
  -- ---- feature block stg25 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg25','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg25','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg25',SQLERRM);
  END;
  -- ---- feature block stg25 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg25',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg25',SQLERRM);
  END;
  -- ---- feature block stg25 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg25','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg25',SQLERRM);
  END;
  -- ---- feature block stg25 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg25', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg25','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg25',SQLERRM);
  END;
  -- ---- feature block stg25 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg25', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg25','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg25',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_25','done depth '||p_depth);

  CALL erp.erp_usp_stage_26(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_25_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_26(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 26;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_26','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg26 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg26','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg26',SQLERRM);
  END;
  -- ---- feature block stg26 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg26','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg26',SQLERRM);
  END;
  -- ---- feature block stg26 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg26','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg26',SQLERRM);
  END;
  -- ---- feature block stg26 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg26','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg26',SQLERRM);
  END;
  -- ---- feature block stg26 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg26','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg26',SQLERRM);
  END;
  -- ---- feature block stg26 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg26','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg26',SQLERRM);
  END;
  -- ---- feature block stg26 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg26 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg26(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg26.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg26(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg26.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg26;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg26','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg26',SQLERRM);
  END;
  -- ---- feature block stg26 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg26')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg26', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg26';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg26','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg26',SQLERRM);
  END;
  -- ---- feature block stg26 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg26','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg26',SQLERRM);
  END;
  -- ---- feature block stg26 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg26','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg26','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg26',SQLERRM);
  END;
  -- ---- feature block stg26 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg26',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg26',SQLERRM);
  END;
  -- ---- feature block stg26 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg26','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg26',SQLERRM);
  END;
  -- ---- feature block stg26 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg26', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg26','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg26',SQLERRM);
  END;
  -- ---- feature block stg26 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg26', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg26','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg26',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_26','done depth '||p_depth);

  CALL erp.erp_usp_stage_27(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_26_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_27(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 27;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_27','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg27 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg27','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg27',SQLERRM);
  END;
  -- ---- feature block stg27 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg27','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg27',SQLERRM);
  END;
  -- ---- feature block stg27 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg27','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg27',SQLERRM);
  END;
  -- ---- feature block stg27 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg27','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg27',SQLERRM);
  END;
  -- ---- feature block stg27 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg27','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg27',SQLERRM);
  END;
  -- ---- feature block stg27 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg27','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg27',SQLERRM);
  END;
  -- ---- feature block stg27 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg27 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg27(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg27.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg27(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg27.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg27;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg27','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg27',SQLERRM);
  END;
  -- ---- feature block stg27 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg27')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg27', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg27';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg27','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg27',SQLERRM);
  END;
  -- ---- feature block stg27 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg27','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg27',SQLERRM);
  END;
  -- ---- feature block stg27 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg27','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg27','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg27',SQLERRM);
  END;
  -- ---- feature block stg27 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg27',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg27',SQLERRM);
  END;
  -- ---- feature block stg27 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg27','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg27',SQLERRM);
  END;
  -- ---- feature block stg27 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg27', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg27','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg27',SQLERRM);
  END;
  -- ---- feature block stg27 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg27', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg27','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg27',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_27','done depth '||p_depth);

  CALL erp.erp_usp_stage_28(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_27_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_28(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 28;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_28','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg28 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg28','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg28',SQLERRM);
  END;
  -- ---- feature block stg28 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg28','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg28',SQLERRM);
  END;
  -- ---- feature block stg28 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg28','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg28',SQLERRM);
  END;
  -- ---- feature block stg28 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg28','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg28',SQLERRM);
  END;
  -- ---- feature block stg28 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg28','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg28',SQLERRM);
  END;
  -- ---- feature block stg28 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg28','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg28',SQLERRM);
  END;
  -- ---- feature block stg28 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg28 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg28(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg28.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg28(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg28.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg28;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg28','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg28',SQLERRM);
  END;
  -- ---- feature block stg28 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg28')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg28', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg28';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg28','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg28',SQLERRM);
  END;
  -- ---- feature block stg28 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg28','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg28',SQLERRM);
  END;
  -- ---- feature block stg28 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg28','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg28','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg28',SQLERRM);
  END;
  -- ---- feature block stg28 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg28',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg28',SQLERRM);
  END;
  -- ---- feature block stg28 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg28','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg28',SQLERRM);
  END;
  -- ---- feature block stg28 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg28', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg28','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg28',SQLERRM);
  END;
  -- ---- feature block stg28 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg28', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg28','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg28',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_28','done depth '||p_depth);

  CALL erp.erp_usp_stage_29(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_28_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_29(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 29;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_29','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg29 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg29','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg29',SQLERRM);
  END;
  -- ---- feature block stg29 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg29','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg29',SQLERRM);
  END;
  -- ---- feature block stg29 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg29','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg29',SQLERRM);
  END;
  -- ---- feature block stg29 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg29','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg29',SQLERRM);
  END;
  -- ---- feature block stg29 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg29','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg29',SQLERRM);
  END;
  -- ---- feature block stg29 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg29','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg29',SQLERRM);
  END;
  -- ---- feature block stg29 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg29 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg29(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg29.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg29(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg29.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg29;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg29','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg29',SQLERRM);
  END;
  -- ---- feature block stg29 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg29')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg29', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg29';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg29','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg29',SQLERRM);
  END;
  -- ---- feature block stg29 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg29','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg29',SQLERRM);
  END;
  -- ---- feature block stg29 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg29','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg29','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg29',SQLERRM);
  END;
  -- ---- feature block stg29 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg29',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg29',SQLERRM);
  END;
  -- ---- feature block stg29 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg29','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg29',SQLERRM);
  END;
  -- ---- feature block stg29 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg29', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg29','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg29',SQLERRM);
  END;
  -- ---- feature block stg29 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg29', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg29','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg29',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_29','done depth '||p_depth);

  CALL erp.erp_usp_stage_30(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_29_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_30(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 30;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_30','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg30 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg30','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg30',SQLERRM);
  END;
  -- ---- feature block stg30 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg30','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg30',SQLERRM);
  END;
  -- ---- feature block stg30 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg30','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg30',SQLERRM);
  END;
  -- ---- feature block stg30 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg30','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg30',SQLERRM);
  END;
  -- ---- feature block stg30 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg30','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg30',SQLERRM);
  END;
  -- ---- feature block stg30 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg30','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg30',SQLERRM);
  END;
  -- ---- feature block stg30 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg30 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg30(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg30.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg30(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg30.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg30;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg30','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg30',SQLERRM);
  END;
  -- ---- feature block stg30 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg30')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg30', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg30';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg30','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg30',SQLERRM);
  END;
  -- ---- feature block stg30 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg30','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg30',SQLERRM);
  END;
  -- ---- feature block stg30 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg30','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg30','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg30',SQLERRM);
  END;
  -- ---- feature block stg30 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg30',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg30',SQLERRM);
  END;
  -- ---- feature block stg30 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg30','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg30',SQLERRM);
  END;
  -- ---- feature block stg30 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg30', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg30','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg30',SQLERRM);
  END;
  -- ---- feature block stg30 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg30', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg30','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg30',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_30','done depth '||p_depth);

  CALL erp.erp_usp_stage_31(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_30_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_31(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 31;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_31','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg31 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg31','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg31',SQLERRM);
  END;
  -- ---- feature block stg31 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg31','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg31',SQLERRM);
  END;
  -- ---- feature block stg31 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg31','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg31',SQLERRM);
  END;
  -- ---- feature block stg31 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg31','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg31',SQLERRM);
  END;
  -- ---- feature block stg31 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg31','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg31',SQLERRM);
  END;
  -- ---- feature block stg31 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg31','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg31',SQLERRM);
  END;
  -- ---- feature block stg31 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg31 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg31(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg31.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg31(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg31.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg31;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg31','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg31',SQLERRM);
  END;
  -- ---- feature block stg31 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg31')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg31', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg31';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg31','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg31',SQLERRM);
  END;
  -- ---- feature block stg31 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg31','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg31',SQLERRM);
  END;
  -- ---- feature block stg31 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg31','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg31','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg31',SQLERRM);
  END;
  -- ---- feature block stg31 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg31',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg31',SQLERRM);
  END;
  -- ---- feature block stg31 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg31','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg31',SQLERRM);
  END;
  -- ---- feature block stg31 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg31', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg31','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg31',SQLERRM);
  END;
  -- ---- feature block stg31 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg31', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg31','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg31',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_31','done depth '||p_depth);

  CALL erp.erp_usp_stage_32(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_31_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_32(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 32;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_32','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg32 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg32','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg32',SQLERRM);
  END;
  -- ---- feature block stg32 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg32','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg32',SQLERRM);
  END;
  -- ---- feature block stg32 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg32','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg32',SQLERRM);
  END;
  -- ---- feature block stg32 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg32','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg32',SQLERRM);
  END;
  -- ---- feature block stg32 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg32','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg32',SQLERRM);
  END;
  -- ---- feature block stg32 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg32','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg32',SQLERRM);
  END;
  -- ---- feature block stg32 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg32 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg32(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg32.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg32(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg32.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg32;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg32','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg32',SQLERRM);
  END;
  -- ---- feature block stg32 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg32')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg32', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg32';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg32','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg32',SQLERRM);
  END;
  -- ---- feature block stg32 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg32','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg32',SQLERRM);
  END;
  -- ---- feature block stg32 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg32','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg32','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg32',SQLERRM);
  END;
  -- ---- feature block stg32 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg32',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg32',SQLERRM);
  END;
  -- ---- feature block stg32 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg32','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg32',SQLERRM);
  END;
  -- ---- feature block stg32 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg32', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg32','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg32',SQLERRM);
  END;
  -- ---- feature block stg32 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg32', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg32','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg32',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_32','done depth '||p_depth);

  CALL erp.erp_usp_stage_33(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_32_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_33(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 33;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_33','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg33 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg33','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg33',SQLERRM);
  END;
  -- ---- feature block stg33 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg33','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg33',SQLERRM);
  END;
  -- ---- feature block stg33 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg33','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg33',SQLERRM);
  END;
  -- ---- feature block stg33 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg33','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg33',SQLERRM);
  END;
  -- ---- feature block stg33 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg33','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg33',SQLERRM);
  END;
  -- ---- feature block stg33 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg33','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg33',SQLERRM);
  END;
  -- ---- feature block stg33 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg33 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg33(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg33.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg33(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg33.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg33;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg33','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg33',SQLERRM);
  END;
  -- ---- feature block stg33 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg33')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg33', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg33';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg33','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg33',SQLERRM);
  END;
  -- ---- feature block stg33 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg33','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg33',SQLERRM);
  END;
  -- ---- feature block stg33 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg33','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg33','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg33',SQLERRM);
  END;
  -- ---- feature block stg33 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg33',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg33',SQLERRM);
  END;
  -- ---- feature block stg33 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg33','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg33',SQLERRM);
  END;
  -- ---- feature block stg33 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg33', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg33','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg33',SQLERRM);
  END;
  -- ---- feature block stg33 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg33', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg33','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg33',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_33','done depth '||p_depth);

  CALL erp.erp_usp_stage_34(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_33_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_34(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 34;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_34','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg34 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg34','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg34',SQLERRM);
  END;
  -- ---- feature block stg34 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg34','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg34',SQLERRM);
  END;
  -- ---- feature block stg34 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg34','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg34',SQLERRM);
  END;
  -- ---- feature block stg34 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg34','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg34',SQLERRM);
  END;
  -- ---- feature block stg34 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg34','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg34',SQLERRM);
  END;
  -- ---- feature block stg34 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg34','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg34',SQLERRM);
  END;
  -- ---- feature block stg34 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg34 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg34(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg34.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg34(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg34.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg34;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg34','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg34',SQLERRM);
  END;
  -- ---- feature block stg34 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg34')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg34', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg34';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg34','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg34',SQLERRM);
  END;
  -- ---- feature block stg34 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg34','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg34',SQLERRM);
  END;
  -- ---- feature block stg34 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg34','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg34','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg34',SQLERRM);
  END;
  -- ---- feature block stg34 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg34',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg34',SQLERRM);
  END;
  -- ---- feature block stg34 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg34','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg34',SQLERRM);
  END;
  -- ---- feature block stg34 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg34', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg34','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg34',SQLERRM);
  END;
  -- ---- feature block stg34 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg34', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg34','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg34',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_34','done depth '||p_depth);

  CALL erp.erp_usp_stage_35(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_34_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_35(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 35;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_35','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg35 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg35','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg35',SQLERRM);
  END;
  -- ---- feature block stg35 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg35','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg35',SQLERRM);
  END;
  -- ---- feature block stg35 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg35','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg35',SQLERRM);
  END;
  -- ---- feature block stg35 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg35','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg35',SQLERRM);
  END;
  -- ---- feature block stg35 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg35','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg35',SQLERRM);
  END;
  -- ---- feature block stg35 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg35','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg35',SQLERRM);
  END;
  -- ---- feature block stg35 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg35 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg35(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg35.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg35(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg35.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg35;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg35','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg35',SQLERRM);
  END;
  -- ---- feature block stg35 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg35')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg35', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg35';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg35','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg35',SQLERRM);
  END;
  -- ---- feature block stg35 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg35','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg35',SQLERRM);
  END;
  -- ---- feature block stg35 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg35','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg35','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg35',SQLERRM);
  END;
  -- ---- feature block stg35 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg35',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg35',SQLERRM);
  END;
  -- ---- feature block stg35 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg35','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg35',SQLERRM);
  END;
  -- ---- feature block stg35 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg35', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg35','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg35',SQLERRM);
  END;
  -- ---- feature block stg35 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg35', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg35','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg35',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_35','done depth '||p_depth);

  CALL erp.erp_usp_stage_36(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_35_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_36(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 36;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_36','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg36 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg36','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg36',SQLERRM);
  END;
  -- ---- feature block stg36 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg36','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg36',SQLERRM);
  END;
  -- ---- feature block stg36 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg36','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg36',SQLERRM);
  END;
  -- ---- feature block stg36 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg36','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg36',SQLERRM);
  END;
  -- ---- feature block stg36 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg36','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg36',SQLERRM);
  END;
  -- ---- feature block stg36 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg36','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg36',SQLERRM);
  END;
  -- ---- feature block stg36 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg36 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg36(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg36.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg36(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg36.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg36;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg36','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg36',SQLERRM);
  END;
  -- ---- feature block stg36 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg36')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg36', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg36';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg36','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg36',SQLERRM);
  END;
  -- ---- feature block stg36 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg36','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg36',SQLERRM);
  END;
  -- ---- feature block stg36 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg36','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg36','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg36',SQLERRM);
  END;
  -- ---- feature block stg36 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg36',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg36',SQLERRM);
  END;
  -- ---- feature block stg36 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg36','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg36',SQLERRM);
  END;
  -- ---- feature block stg36 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg36', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg36','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg36',SQLERRM);
  END;
  -- ---- feature block stg36 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg36', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg36','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg36',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_36','done depth '||p_depth);

  CALL erp.erp_usp_stage_37(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_36_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_37(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 37;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_37','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg37 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg37','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg37',SQLERRM);
  END;
  -- ---- feature block stg37 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg37','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg37',SQLERRM);
  END;
  -- ---- feature block stg37 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg37','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg37',SQLERRM);
  END;
  -- ---- feature block stg37 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg37','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg37',SQLERRM);
  END;
  -- ---- feature block stg37 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg37','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg37',SQLERRM);
  END;
  -- ---- feature block stg37 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg37','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg37',SQLERRM);
  END;
  -- ---- feature block stg37 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg37 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg37(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg37.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg37(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg37.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg37;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg37','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg37',SQLERRM);
  END;
  -- ---- feature block stg37 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg37')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg37', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg37';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg37','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg37',SQLERRM);
  END;
  -- ---- feature block stg37 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg37','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg37',SQLERRM);
  END;
  -- ---- feature block stg37 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg37','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg37','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg37',SQLERRM);
  END;
  -- ---- feature block stg37 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg37',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg37',SQLERRM);
  END;
  -- ---- feature block stg37 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg37','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg37',SQLERRM);
  END;
  -- ---- feature block stg37 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg37', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg37','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg37',SQLERRM);
  END;
  -- ---- feature block stg37 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg37', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg37','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg37',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_37','done depth '||p_depth);

  CALL erp.erp_usp_stage_38(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_37_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_38(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 38;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_38','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg38 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg38','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg38',SQLERRM);
  END;
  -- ---- feature block stg38 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg38','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg38',SQLERRM);
  END;
  -- ---- feature block stg38 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg38','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg38',SQLERRM);
  END;
  -- ---- feature block stg38 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg38','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg38',SQLERRM);
  END;
  -- ---- feature block stg38 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg38','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg38',SQLERRM);
  END;
  -- ---- feature block stg38 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg38','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg38',SQLERRM);
  END;
  -- ---- feature block stg38 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg38 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg38(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg38.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg38(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg38.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg38;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg38','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg38',SQLERRM);
  END;
  -- ---- feature block stg38 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg38')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg38', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg38';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg38','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg38',SQLERRM);
  END;
  -- ---- feature block stg38 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg38','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg38',SQLERRM);
  END;
  -- ---- feature block stg38 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg38','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg38','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg38',SQLERRM);
  END;
  -- ---- feature block stg38 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg38',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg38',SQLERRM);
  END;
  -- ---- feature block stg38 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg38','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg38',SQLERRM);
  END;
  -- ---- feature block stg38 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg38', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg38','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg38',SQLERRM);
  END;
  -- ---- feature block stg38 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg38', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg38','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg38',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_38','done depth '||p_depth);

  CALL erp.erp_usp_stage_39(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_38_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_39(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 39;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_39','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg39 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg39','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg39',SQLERRM);
  END;
  -- ---- feature block stg39 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg39','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg39',SQLERRM);
  END;
  -- ---- feature block stg39 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg39','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg39',SQLERRM);
  END;
  -- ---- feature block stg39 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg39','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg39',SQLERRM);
  END;
  -- ---- feature block stg39 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg39','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg39',SQLERRM);
  END;
  -- ---- feature block stg39 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg39','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg39',SQLERRM);
  END;
  -- ---- feature block stg39 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg39 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg39(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg39.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg39(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg39.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg39;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg39','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg39',SQLERRM);
  END;
  -- ---- feature block stg39 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg39')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg39', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg39';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg39','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg39',SQLERRM);
  END;
  -- ---- feature block stg39 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg39','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg39',SQLERRM);
  END;
  -- ---- feature block stg39 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg39','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg39','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg39',SQLERRM);
  END;
  -- ---- feature block stg39 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg39',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg39',SQLERRM);
  END;
  -- ---- feature block stg39 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg39','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg39',SQLERRM);
  END;
  -- ---- feature block stg39 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg39', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg39','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg39',SQLERRM);
  END;
  -- ---- feature block stg39 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg39', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg39','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg39',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_39','done depth '||p_depth);

  CALL erp.erp_usp_stage_40(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_39_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_40(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 40;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_40','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg40 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg40','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg40',SQLERRM);
  END;
  -- ---- feature block stg40 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg40','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg40',SQLERRM);
  END;
  -- ---- feature block stg40 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg40','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg40',SQLERRM);
  END;
  -- ---- feature block stg40 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg40','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg40',SQLERRM);
  END;
  -- ---- feature block stg40 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg40','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg40',SQLERRM);
  END;
  -- ---- feature block stg40 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg40','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg40',SQLERRM);
  END;
  -- ---- feature block stg40 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg40 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg40(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg40.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg40(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg40.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg40;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg40','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg40',SQLERRM);
  END;
  -- ---- feature block stg40 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg40')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg40', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg40';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg40','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg40',SQLERRM);
  END;
  -- ---- feature block stg40 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg40','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg40',SQLERRM);
  END;
  -- ---- feature block stg40 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg40','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg40','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg40',SQLERRM);
  END;
  -- ---- feature block stg40 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg40',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg40',SQLERRM);
  END;
  -- ---- feature block stg40 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg40','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg40',SQLERRM);
  END;
  -- ---- feature block stg40 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg40', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg40','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg40',SQLERRM);
  END;
  -- ---- feature block stg40 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg40', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg40','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg40',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_40','done depth '||p_depth);

  CALL erp.erp_usp_stage_41(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_40_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_41(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 41;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_41','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg41 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg41','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg41',SQLERRM);
  END;
  -- ---- feature block stg41 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg41','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg41',SQLERRM);
  END;
  -- ---- feature block stg41 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg41','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg41',SQLERRM);
  END;
  -- ---- feature block stg41 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg41','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg41',SQLERRM);
  END;
  -- ---- feature block stg41 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg41','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg41',SQLERRM);
  END;
  -- ---- feature block stg41 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg41','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg41',SQLERRM);
  END;
  -- ---- feature block stg41 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg41 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg41(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg41.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg41(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg41.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg41;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg41','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg41',SQLERRM);
  END;
  -- ---- feature block stg41 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg41')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg41', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg41';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg41','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg41',SQLERRM);
  END;
  -- ---- feature block stg41 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg41','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg41',SQLERRM);
  END;
  -- ---- feature block stg41 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg41','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg41','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg41',SQLERRM);
  END;
  -- ---- feature block stg41 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg41',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg41',SQLERRM);
  END;
  -- ---- feature block stg41 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg41','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg41',SQLERRM);
  END;
  -- ---- feature block stg41 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg41', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg41','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg41',SQLERRM);
  END;
  -- ---- feature block stg41 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg41', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg41','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg41',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_41','done depth '||p_depth);

  CALL erp.erp_usp_stage_42(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_41_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_42(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 42;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_42','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg42 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg42','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg42',SQLERRM);
  END;
  -- ---- feature block stg42 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg42','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg42',SQLERRM);
  END;
  -- ---- feature block stg42 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg42','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg42',SQLERRM);
  END;
  -- ---- feature block stg42 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg42','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg42',SQLERRM);
  END;
  -- ---- feature block stg42 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg42','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg42',SQLERRM);
  END;
  -- ---- feature block stg42 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg42','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg42',SQLERRM);
  END;
  -- ---- feature block stg42 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg42 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg42(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg42.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg42(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg42.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg42;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg42','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg42',SQLERRM);
  END;
  -- ---- feature block stg42 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg42')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg42', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg42';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg42','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg42',SQLERRM);
  END;
  -- ---- feature block stg42 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg42','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg42',SQLERRM);
  END;
  -- ---- feature block stg42 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg42','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg42','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg42',SQLERRM);
  END;
  -- ---- feature block stg42 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg42',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg42',SQLERRM);
  END;
  -- ---- feature block stg42 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg42','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg42',SQLERRM);
  END;
  -- ---- feature block stg42 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg42', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg42','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg42',SQLERRM);
  END;
  -- ---- feature block stg42 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg42', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg42','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg42',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_42','done depth '||p_depth);

  CALL erp.erp_usp_stage_43(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_42_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_43(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 43;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_43','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg43 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg43','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg43',SQLERRM);
  END;
  -- ---- feature block stg43 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg43','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg43',SQLERRM);
  END;
  -- ---- feature block stg43 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg43','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg43',SQLERRM);
  END;
  -- ---- feature block stg43 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg43','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg43',SQLERRM);
  END;
  -- ---- feature block stg43 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg43','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg43',SQLERRM);
  END;
  -- ---- feature block stg43 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg43','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg43',SQLERRM);
  END;
  -- ---- feature block stg43 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg43 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg43(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg43.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg43(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg43.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg43;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg43','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg43',SQLERRM);
  END;
  -- ---- feature block stg43 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg43')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg43', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg43';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg43','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg43',SQLERRM);
  END;
  -- ---- feature block stg43 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg43','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg43',SQLERRM);
  END;
  -- ---- feature block stg43 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg43','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg43','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg43',SQLERRM);
  END;
  -- ---- feature block stg43 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg43',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg43',SQLERRM);
  END;
  -- ---- feature block stg43 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg43','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg43',SQLERRM);
  END;
  -- ---- feature block stg43 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg43', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg43','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg43',SQLERRM);
  END;
  -- ---- feature block stg43 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg43', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg43','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg43',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_43','done depth '||p_depth);

  CALL erp.erp_usp_stage_44(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_43_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_44(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 44;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_44','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg44 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg44','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg44',SQLERRM);
  END;
  -- ---- feature block stg44 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg44','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg44',SQLERRM);
  END;
  -- ---- feature block stg44 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg44','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg44',SQLERRM);
  END;
  -- ---- feature block stg44 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg44','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg44',SQLERRM);
  END;
  -- ---- feature block stg44 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg44','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg44',SQLERRM);
  END;
  -- ---- feature block stg44 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg44','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg44',SQLERRM);
  END;
  -- ---- feature block stg44 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg44 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg44(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg44.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg44(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg44.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg44;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg44','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg44',SQLERRM);
  END;
  -- ---- feature block stg44 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg44')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg44', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg44';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg44','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg44',SQLERRM);
  END;
  -- ---- feature block stg44 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg44','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg44',SQLERRM);
  END;
  -- ---- feature block stg44 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg44','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg44','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg44',SQLERRM);
  END;
  -- ---- feature block stg44 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg44',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg44',SQLERRM);
  END;
  -- ---- feature block stg44 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg44','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg44',SQLERRM);
  END;
  -- ---- feature block stg44 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg44', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg44','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg44',SQLERRM);
  END;
  -- ---- feature block stg44 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg44', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg44','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg44',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_44','done depth '||p_depth);

  CALL erp.erp_usp_stage_45(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_44_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_45(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 45;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_45','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg45 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg45','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg45',SQLERRM);
  END;
  -- ---- feature block stg45 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg45','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg45',SQLERRM);
  END;
  -- ---- feature block stg45 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg45','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg45',SQLERRM);
  END;
  -- ---- feature block stg45 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg45','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg45',SQLERRM);
  END;
  -- ---- feature block stg45 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg45','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg45',SQLERRM);
  END;
  -- ---- feature block stg45 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg45','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg45',SQLERRM);
  END;
  -- ---- feature block stg45 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg45 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg45(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg45.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg45(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg45.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg45;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg45','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg45',SQLERRM);
  END;
  -- ---- feature block stg45 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg45')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg45', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg45';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg45','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg45',SQLERRM);
  END;
  -- ---- feature block stg45 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg45','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg45',SQLERRM);
  END;
  -- ---- feature block stg45 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg45','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg45','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg45',SQLERRM);
  END;
  -- ---- feature block stg45 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg45',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg45',SQLERRM);
  END;
  -- ---- feature block stg45 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg45','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg45',SQLERRM);
  END;
  -- ---- feature block stg45 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg45', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg45','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg45',SQLERRM);
  END;
  -- ---- feature block stg45 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg45', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg45','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg45',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_45','done depth '||p_depth);

  CALL erp.erp_usp_stage_46(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_45_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_46(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 46;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_46','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg46 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg46','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg46',SQLERRM);
  END;
  -- ---- feature block stg46 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg46','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg46',SQLERRM);
  END;
  -- ---- feature block stg46 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg46','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg46',SQLERRM);
  END;
  -- ---- feature block stg46 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg46','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg46',SQLERRM);
  END;
  -- ---- feature block stg46 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg46','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg46',SQLERRM);
  END;
  -- ---- feature block stg46 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg46','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg46',SQLERRM);
  END;
  -- ---- feature block stg46 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg46 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg46(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg46.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg46(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg46.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg46;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg46','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg46',SQLERRM);
  END;
  -- ---- feature block stg46 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg46')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg46', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg46';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg46','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg46',SQLERRM);
  END;
  -- ---- feature block stg46 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg46','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg46',SQLERRM);
  END;
  -- ---- feature block stg46 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg46','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg46','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg46',SQLERRM);
  END;
  -- ---- feature block stg46 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg46',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg46',SQLERRM);
  END;
  -- ---- feature block stg46 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg46','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg46',SQLERRM);
  END;
  -- ---- feature block stg46 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg46', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg46','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg46',SQLERRM);
  END;
  -- ---- feature block stg46 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg46', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg46','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg46',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_46','done depth '||p_depth);

  CALL erp.erp_usp_stage_47(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_46_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_47(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 47;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_47','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg47 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg47','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg47',SQLERRM);
  END;
  -- ---- feature block stg47 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg47','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg47',SQLERRM);
  END;
  -- ---- feature block stg47 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg47','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg47',SQLERRM);
  END;
  -- ---- feature block stg47 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg47','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg47',SQLERRM);
  END;
  -- ---- feature block stg47 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg47','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg47',SQLERRM);
  END;
  -- ---- feature block stg47 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg47','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg47',SQLERRM);
  END;
  -- ---- feature block stg47 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg47 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg47(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg47.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg47(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg47.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg47;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg47','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg47',SQLERRM);
  END;
  -- ---- feature block stg47 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg47')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg47', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg47';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg47','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg47',SQLERRM);
  END;
  -- ---- feature block stg47 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg47','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg47',SQLERRM);
  END;
  -- ---- feature block stg47 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg47','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg47','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg47',SQLERRM);
  END;
  -- ---- feature block stg47 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg47',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg47',SQLERRM);
  END;
  -- ---- feature block stg47 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg47','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg47',SQLERRM);
  END;
  -- ---- feature block stg47 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg47', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg47','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg47',SQLERRM);
  END;
  -- ---- feature block stg47 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg47', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg47','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg47',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_47','done depth '||p_depth);

  CALL erp.erp_usp_stage_48(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_47_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_48(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 48;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_48','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg48 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg48','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg48',SQLERRM);
  END;
  -- ---- feature block stg48 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg48','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg48',SQLERRM);
  END;
  -- ---- feature block stg48 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg48','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg48',SQLERRM);
  END;
  -- ---- feature block stg48 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg48','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg48',SQLERRM);
  END;
  -- ---- feature block stg48 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg48','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg48',SQLERRM);
  END;
  -- ---- feature block stg48 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg48','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg48',SQLERRM);
  END;
  -- ---- feature block stg48 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg48 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg48(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg48.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg48(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg48.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg48;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg48','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg48',SQLERRM);
  END;
  -- ---- feature block stg48 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg48')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg48', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg48';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg48','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg48',SQLERRM);
  END;
  -- ---- feature block stg48 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg48','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg48',SQLERRM);
  END;
  -- ---- feature block stg48 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg48','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg48','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg48',SQLERRM);
  END;
  -- ---- feature block stg48 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg48',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg48',SQLERRM);
  END;
  -- ---- feature block stg48 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg48','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg48',SQLERRM);
  END;
  -- ---- feature block stg48 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg48', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg48','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg48',SQLERRM);
  END;
  -- ---- feature block stg48 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg48', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg48','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg48',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_48','done depth '||p_depth);

  CALL erp.erp_usp_stage_49(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_48_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_49(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 49;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_49','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg49 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg49','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg49',SQLERRM);
  END;
  -- ---- feature block stg49 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg49','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg49',SQLERRM);
  END;
  -- ---- feature block stg49 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg49','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg49',SQLERRM);
  END;
  -- ---- feature block stg49 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg49','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg49',SQLERRM);
  END;
  -- ---- feature block stg49 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg49','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg49',SQLERRM);
  END;
  -- ---- feature block stg49 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg49','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg49',SQLERRM);
  END;
  -- ---- feature block stg49 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg49 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg49(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg49.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg49(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg49.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg49;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg49','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg49',SQLERRM);
  END;
  -- ---- feature block stg49 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg49')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg49', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg49';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg49','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg49',SQLERRM);
  END;
  -- ---- feature block stg49 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg49','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg49',SQLERRM);
  END;
  -- ---- feature block stg49 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg49','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg49','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg49',SQLERRM);
  END;
  -- ---- feature block stg49 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg49',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg49',SQLERRM);
  END;
  -- ---- feature block stg49 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg49','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg49',SQLERRM);
  END;
  -- ---- feature block stg49 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg49', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg49','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg49',SQLERRM);
  END;
  -- ---- feature block stg49 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg49', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg49','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg49',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_49','done depth '||p_depth);

  CALL erp.erp_usp_stage_50(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_49_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_50(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 50;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_50','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg50 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg50','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg50',SQLERRM);
  END;
  -- ---- feature block stg50 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg50','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg50',SQLERRM);
  END;
  -- ---- feature block stg50 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg50','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg50',SQLERRM);
  END;
  -- ---- feature block stg50 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg50','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg50',SQLERRM);
  END;
  -- ---- feature block stg50 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg50','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg50',SQLERRM);
  END;
  -- ---- feature block stg50 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg50','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg50',SQLERRM);
  END;
  -- ---- feature block stg50 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg50 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg50(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg50.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg50(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg50.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg50;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg50','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg50',SQLERRM);
  END;
  -- ---- feature block stg50 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg50')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg50', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg50';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg50','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg50',SQLERRM);
  END;
  -- ---- feature block stg50 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg50','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg50',SQLERRM);
  END;
  -- ---- feature block stg50 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg50','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg50','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg50',SQLERRM);
  END;
  -- ---- feature block stg50 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg50',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg50',SQLERRM);
  END;
  -- ---- feature block stg50 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg50','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg50',SQLERRM);
  END;
  -- ---- feature block stg50 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg50', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg50','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg50',SQLERRM);
  END;
  -- ---- feature block stg50 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg50', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg50','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg50',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_50','done depth '||p_depth);

  CALL erp.erp_usp_stage_51(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_50_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_51(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 51;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_51','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg51 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg51','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg51',SQLERRM);
  END;
  -- ---- feature block stg51 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg51','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg51',SQLERRM);
  END;
  -- ---- feature block stg51 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg51','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg51',SQLERRM);
  END;
  -- ---- feature block stg51 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg51','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg51',SQLERRM);
  END;
  -- ---- feature block stg51 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg51','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg51',SQLERRM);
  END;
  -- ---- feature block stg51 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg51','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg51',SQLERRM);
  END;
  -- ---- feature block stg51 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg51 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg51(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg51.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg51(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg51.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg51;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg51','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg51',SQLERRM);
  END;
  -- ---- feature block stg51 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg51')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg51', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg51';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg51','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg51',SQLERRM);
  END;
  -- ---- feature block stg51 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg51','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg51',SQLERRM);
  END;
  -- ---- feature block stg51 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg51','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg51','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg51',SQLERRM);
  END;
  -- ---- feature block stg51 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg51',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg51',SQLERRM);
  END;
  -- ---- feature block stg51 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg51','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg51',SQLERRM);
  END;
  -- ---- feature block stg51 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg51', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg51','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg51',SQLERRM);
  END;
  -- ---- feature block stg51 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg51', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg51','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg51',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_51','done depth '||p_depth);

  CALL erp.erp_usp_stage_52(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_51_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_52(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 52;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_52','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg52 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg52','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg52',SQLERRM);
  END;
  -- ---- feature block stg52 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg52','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg52',SQLERRM);
  END;
  -- ---- feature block stg52 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg52','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg52',SQLERRM);
  END;
  -- ---- feature block stg52 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg52','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg52',SQLERRM);
  END;
  -- ---- feature block stg52 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg52','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg52',SQLERRM);
  END;
  -- ---- feature block stg52 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg52','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg52',SQLERRM);
  END;
  -- ---- feature block stg52 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg52 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg52(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg52.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg52(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg52.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg52;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg52','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg52',SQLERRM);
  END;
  -- ---- feature block stg52 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg52')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg52', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg52';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg52','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg52',SQLERRM);
  END;
  -- ---- feature block stg52 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg52','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg52',SQLERRM);
  END;
  -- ---- feature block stg52 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg52','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg52','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg52',SQLERRM);
  END;
  -- ---- feature block stg52 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg52',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg52',SQLERRM);
  END;
  -- ---- feature block stg52 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg52','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg52',SQLERRM);
  END;
  -- ---- feature block stg52 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg52', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg52','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg52',SQLERRM);
  END;
  -- ---- feature block stg52 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg52', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg52','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg52',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_52','done depth '||p_depth);

  CALL erp.erp_usp_stage_53(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_52_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_53(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 53;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_53','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg53 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg53','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg53',SQLERRM);
  END;
  -- ---- feature block stg53 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg53','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg53',SQLERRM);
  END;
  -- ---- feature block stg53 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg53','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg53',SQLERRM);
  END;
  -- ---- feature block stg53 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg53','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg53',SQLERRM);
  END;
  -- ---- feature block stg53 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg53','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg53',SQLERRM);
  END;
  -- ---- feature block stg53 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg53','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg53',SQLERRM);
  END;
  -- ---- feature block stg53 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg53 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg53(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg53.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg53(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg53.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg53;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg53','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg53',SQLERRM);
  END;
  -- ---- feature block stg53 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg53')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg53', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg53';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg53','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg53',SQLERRM);
  END;
  -- ---- feature block stg53 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg53','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg53',SQLERRM);
  END;
  -- ---- feature block stg53 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg53','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg53','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg53',SQLERRM);
  END;
  -- ---- feature block stg53 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg53',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg53',SQLERRM);
  END;
  -- ---- feature block stg53 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg53','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg53',SQLERRM);
  END;
  -- ---- feature block stg53 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg53', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg53','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg53',SQLERRM);
  END;
  -- ---- feature block stg53 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg53', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg53','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg53',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_53','done depth '||p_depth);

  CALL erp.erp_usp_stage_54(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_53_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_54(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 54;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_54','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg54 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg54','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg54',SQLERRM);
  END;
  -- ---- feature block stg54 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg54','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg54',SQLERRM);
  END;
  -- ---- feature block stg54 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg54','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg54',SQLERRM);
  END;
  -- ---- feature block stg54 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg54','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg54',SQLERRM);
  END;
  -- ---- feature block stg54 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg54','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg54',SQLERRM);
  END;
  -- ---- feature block stg54 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg54','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg54',SQLERRM);
  END;
  -- ---- feature block stg54 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg54 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg54(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg54.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg54(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg54.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg54;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg54','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg54',SQLERRM);
  END;
  -- ---- feature block stg54 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg54')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg54', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg54';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg54','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg54',SQLERRM);
  END;
  -- ---- feature block stg54 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg54','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg54',SQLERRM);
  END;
  -- ---- feature block stg54 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg54','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg54','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg54',SQLERRM);
  END;
  -- ---- feature block stg54 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg54',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg54',SQLERRM);
  END;
  -- ---- feature block stg54 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg54','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg54',SQLERRM);
  END;
  -- ---- feature block stg54 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg54', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg54','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg54',SQLERRM);
  END;
  -- ---- feature block stg54 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg54', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg54','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg54',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_54','done depth '||p_depth);

  CALL erp.erp_usp_stage_55(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_54_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_55(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 55;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_55','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg55 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg55','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg55',SQLERRM);
  END;
  -- ---- feature block stg55 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg55','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg55',SQLERRM);
  END;
  -- ---- feature block stg55 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg55','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg55',SQLERRM);
  END;
  -- ---- feature block stg55 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg55','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg55',SQLERRM);
  END;
  -- ---- feature block stg55 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg55','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg55',SQLERRM);
  END;
  -- ---- feature block stg55 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg55','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg55',SQLERRM);
  END;
  -- ---- feature block stg55 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg55 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg55(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg55.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg55(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg55.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg55;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg55','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg55',SQLERRM);
  END;
  -- ---- feature block stg55 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg55')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg55', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg55';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg55','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg55',SQLERRM);
  END;
  -- ---- feature block stg55 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg55','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg55',SQLERRM);
  END;
  -- ---- feature block stg55 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg55','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg55','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg55',SQLERRM);
  END;
  -- ---- feature block stg55 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg55',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg55',SQLERRM);
  END;
  -- ---- feature block stg55 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg55','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg55',SQLERRM);
  END;
  -- ---- feature block stg55 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg55', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg55','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg55',SQLERRM);
  END;
  -- ---- feature block stg55 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg55', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg55','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg55',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_55','done depth '||p_depth);

  CALL erp.erp_usp_stage_56(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_55_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_56(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 56;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_56','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg56 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg56','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg56',SQLERRM);
  END;
  -- ---- feature block stg56 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg56','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg56',SQLERRM);
  END;
  -- ---- feature block stg56 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg56','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg56',SQLERRM);
  END;
  -- ---- feature block stg56 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg56','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg56',SQLERRM);
  END;
  -- ---- feature block stg56 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg56','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg56',SQLERRM);
  END;
  -- ---- feature block stg56 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg56','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg56',SQLERRM);
  END;
  -- ---- feature block stg56 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg56 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg56(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg56.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg56(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg56.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg56;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg56','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg56',SQLERRM);
  END;
  -- ---- feature block stg56 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg56')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg56', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg56';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg56','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg56',SQLERRM);
  END;
  -- ---- feature block stg56 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg56','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg56',SQLERRM);
  END;
  -- ---- feature block stg56 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg56','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg56','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg56',SQLERRM);
  END;
  -- ---- feature block stg56 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg56',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg56',SQLERRM);
  END;
  -- ---- feature block stg56 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg56','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg56',SQLERRM);
  END;
  -- ---- feature block stg56 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg56', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg56','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg56',SQLERRM);
  END;
  -- ---- feature block stg56 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg56', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg56','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg56',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_56','done depth '||p_depth);

  CALL erp.erp_usp_stage_57(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_56_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_57(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 57;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_57','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg57 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg57','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg57',SQLERRM);
  END;
  -- ---- feature block stg57 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg57','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg57',SQLERRM);
  END;
  -- ---- feature block stg57 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg57','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg57',SQLERRM);
  END;
  -- ---- feature block stg57 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg57','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg57',SQLERRM);
  END;
  -- ---- feature block stg57 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg57','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg57',SQLERRM);
  END;
  -- ---- feature block stg57 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg57','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg57',SQLERRM);
  END;
  -- ---- feature block stg57 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg57 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg57(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg57.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg57(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg57.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg57;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg57','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg57',SQLERRM);
  END;
  -- ---- feature block stg57 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg57')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg57', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg57';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg57','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg57',SQLERRM);
  END;
  -- ---- feature block stg57 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg57','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg57',SQLERRM);
  END;
  -- ---- feature block stg57 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg57','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg57','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg57',SQLERRM);
  END;
  -- ---- feature block stg57 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg57',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg57',SQLERRM);
  END;
  -- ---- feature block stg57 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg57','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg57',SQLERRM);
  END;
  -- ---- feature block stg57 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg57', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg57','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg57',SQLERRM);
  END;
  -- ---- feature block stg57 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg57', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg57','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg57',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_57','done depth '||p_depth);

  CALL erp.erp_usp_stage_58(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_57_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_58(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 58;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_58','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg58 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg58','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg58',SQLERRM);
  END;
  -- ---- feature block stg58 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg58','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg58',SQLERRM);
  END;
  -- ---- feature block stg58 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg58','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg58',SQLERRM);
  END;
  -- ---- feature block stg58 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg58','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg58',SQLERRM);
  END;
  -- ---- feature block stg58 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg58','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg58',SQLERRM);
  END;
  -- ---- feature block stg58 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg58','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg58',SQLERRM);
  END;
  -- ---- feature block stg58 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg58 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg58(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg58.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg58(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg58.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg58;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg58','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg58',SQLERRM);
  END;
  -- ---- feature block stg58 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg58')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg58', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg58';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg58','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg58',SQLERRM);
  END;
  -- ---- feature block stg58 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg58','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg58',SQLERRM);
  END;
  -- ---- feature block stg58 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg58','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg58','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg58',SQLERRM);
  END;
  -- ---- feature block stg58 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg58',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg58',SQLERRM);
  END;
  -- ---- feature block stg58 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg58','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg58',SQLERRM);
  END;
  -- ---- feature block stg58 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg58', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg58','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg58',SQLERRM);
  END;
  -- ---- feature block stg58 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg58', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg58','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg58',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_58','done depth '||p_depth);

  CALL erp.erp_usp_stage_59(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_58_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_59(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 59;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_59','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg59 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg59','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg59',SQLERRM);
  END;
  -- ---- feature block stg59 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg59','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg59',SQLERRM);
  END;
  -- ---- feature block stg59 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg59','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg59',SQLERRM);
  END;
  -- ---- feature block stg59 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg59','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg59',SQLERRM);
  END;
  -- ---- feature block stg59 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg59','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg59',SQLERRM);
  END;
  -- ---- feature block stg59 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg59','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg59',SQLERRM);
  END;
  -- ---- feature block stg59 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg59 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg59(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg59.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg59(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg59.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg59;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg59','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg59',SQLERRM);
  END;
  -- ---- feature block stg59 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg59')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg59', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg59';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg59','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg59',SQLERRM);
  END;
  -- ---- feature block stg59 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg59','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg59',SQLERRM);
  END;
  -- ---- feature block stg59 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg59','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg59','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg59',SQLERRM);
  END;
  -- ---- feature block stg59 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg59',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg59',SQLERRM);
  END;
  -- ---- feature block stg59 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg59','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg59',SQLERRM);
  END;
  -- ---- feature block stg59 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg59', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg59','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg59',SQLERRM);
  END;
  -- ---- feature block stg59 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg59', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg59','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg59',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_59','done depth '||p_depth);

  CALL erp.erp_usp_stage_60(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_59_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_60(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 60;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_60','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg60 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg60','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg60',SQLERRM);
  END;
  -- ---- feature block stg60 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg60','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg60',SQLERRM);
  END;
  -- ---- feature block stg60 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg60','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg60',SQLERRM);
  END;
  -- ---- feature block stg60 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg60','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg60',SQLERRM);
  END;
  -- ---- feature block stg60 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg60','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg60',SQLERRM);
  END;
  -- ---- feature block stg60 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg60','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg60',SQLERRM);
  END;
  -- ---- feature block stg60 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg60 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg60(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg60.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg60(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg60.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg60;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg60','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg60',SQLERRM);
  END;
  -- ---- feature block stg60 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg60')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg60', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg60';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg60','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg60',SQLERRM);
  END;
  -- ---- feature block stg60 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg60','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg60',SQLERRM);
  END;
  -- ---- feature block stg60 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg60','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg60','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg60',SQLERRM);
  END;
  -- ---- feature block stg60 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg60',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg60',SQLERRM);
  END;
  -- ---- feature block stg60 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg60','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg60',SQLERRM);
  END;
  -- ---- feature block stg60 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg60', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg60','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg60',SQLERRM);
  END;
  -- ---- feature block stg60 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg60', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg60','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg60',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_60','done depth '||p_depth);

  CALL erp.erp_usp_stage_61(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_60_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_61(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 61;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_61','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg61 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg61','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg61',SQLERRM);
  END;
  -- ---- feature block stg61 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg61','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg61',SQLERRM);
  END;
  -- ---- feature block stg61 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg61','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg61',SQLERRM);
  END;
  -- ---- feature block stg61 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg61','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg61',SQLERRM);
  END;
  -- ---- feature block stg61 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg61','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg61',SQLERRM);
  END;
  -- ---- feature block stg61 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg61','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg61',SQLERRM);
  END;
  -- ---- feature block stg61 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg61 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg61(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg61.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg61(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg61.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg61;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg61','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg61',SQLERRM);
  END;
  -- ---- feature block stg61 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg61')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg61', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg61';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg61','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg61',SQLERRM);
  END;
  -- ---- feature block stg61 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg61','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg61',SQLERRM);
  END;
  -- ---- feature block stg61 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg61','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg61','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg61',SQLERRM);
  END;
  -- ---- feature block stg61 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg61',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg61',SQLERRM);
  END;
  -- ---- feature block stg61 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg61','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg61',SQLERRM);
  END;
  -- ---- feature block stg61 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg61', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg61','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg61',SQLERRM);
  END;
  -- ---- feature block stg61 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg61', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg61','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg61',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_61','done depth '||p_depth);

  CALL erp.erp_usp_stage_62(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_61_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_62(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 62;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_62','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg62 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg62','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg62',SQLERRM);
  END;
  -- ---- feature block stg62 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg62','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg62',SQLERRM);
  END;
  -- ---- feature block stg62 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg62','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg62',SQLERRM);
  END;
  -- ---- feature block stg62 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg62','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg62',SQLERRM);
  END;
  -- ---- feature block stg62 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg62','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg62',SQLERRM);
  END;
  -- ---- feature block stg62 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg62','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg62',SQLERRM);
  END;
  -- ---- feature block stg62 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg62 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg62(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg62.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg62(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg62.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg62;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg62','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg62',SQLERRM);
  END;
  -- ---- feature block stg62 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg62')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg62', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg62';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg62','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg62',SQLERRM);
  END;
  -- ---- feature block stg62 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg62','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg62',SQLERRM);
  END;
  -- ---- feature block stg62 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg62','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg62','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg62',SQLERRM);
  END;
  -- ---- feature block stg62 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg62',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg62',SQLERRM);
  END;
  -- ---- feature block stg62 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg62','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg62',SQLERRM);
  END;
  -- ---- feature block stg62 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg62', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg62','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg62',SQLERRM);
  END;
  -- ---- feature block stg62 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg62', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg62','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg62',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_62','done depth '||p_depth);

  CALL erp.erp_usp_stage_63(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_62_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_63(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 63;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_63','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg63 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg63','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg63',SQLERRM);
  END;
  -- ---- feature block stg63 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg63','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg63',SQLERRM);
  END;
  -- ---- feature block stg63 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg63','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg63',SQLERRM);
  END;
  -- ---- feature block stg63 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg63','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg63',SQLERRM);
  END;
  -- ---- feature block stg63 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg63','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg63',SQLERRM);
  END;
  -- ---- feature block stg63 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg63','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg63',SQLERRM);
  END;
  -- ---- feature block stg63 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg63 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg63(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg63.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg63(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg63.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg63;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg63','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg63',SQLERRM);
  END;
  -- ---- feature block stg63 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg63')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg63', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg63';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg63','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg63',SQLERRM);
  END;
  -- ---- feature block stg63 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg63','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg63',SQLERRM);
  END;
  -- ---- feature block stg63 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg63','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg63','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg63',SQLERRM);
  END;
  -- ---- feature block stg63 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg63',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg63',SQLERRM);
  END;
  -- ---- feature block stg63 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg63','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg63',SQLERRM);
  END;
  -- ---- feature block stg63 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg63', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg63','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg63',SQLERRM);
  END;
  -- ---- feature block stg63 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg63', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg63','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg63',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_63','done depth '||p_depth);

  CALL erp.erp_usp_stage_64(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_63_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_64(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 64;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_64','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg64 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg64','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg64',SQLERRM);
  END;
  -- ---- feature block stg64 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg64','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg64',SQLERRM);
  END;
  -- ---- feature block stg64 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg64','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg64',SQLERRM);
  END;
  -- ---- feature block stg64 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg64','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg64',SQLERRM);
  END;
  -- ---- feature block stg64 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg64','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg64',SQLERRM);
  END;
  -- ---- feature block stg64 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg64','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg64',SQLERRM);
  END;
  -- ---- feature block stg64 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg64 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg64(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg64.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg64(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg64.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg64;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg64','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg64',SQLERRM);
  END;
  -- ---- feature block stg64 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg64')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg64', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg64';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg64','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg64',SQLERRM);
  END;
  -- ---- feature block stg64 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg64','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg64',SQLERRM);
  END;
  -- ---- feature block stg64 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg64','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg64','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg64',SQLERRM);
  END;
  -- ---- feature block stg64 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg64',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg64',SQLERRM);
  END;
  -- ---- feature block stg64 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg64','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg64',SQLERRM);
  END;
  -- ---- feature block stg64 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg64', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg64','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg64',SQLERRM);
  END;
  -- ---- feature block stg64 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg64', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg64','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg64',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_64','done depth '||p_depth);

  CALL erp.erp_usp_stage_65(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_64_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_65(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 65;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_65','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg65 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg65','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg65',SQLERRM);
  END;
  -- ---- feature block stg65 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg65','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg65',SQLERRM);
  END;
  -- ---- feature block stg65 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg65','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg65',SQLERRM);
  END;
  -- ---- feature block stg65 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg65','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg65',SQLERRM);
  END;
  -- ---- feature block stg65 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg65','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg65',SQLERRM);
  END;
  -- ---- feature block stg65 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg65','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg65',SQLERRM);
  END;
  -- ---- feature block stg65 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg65 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg65(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg65.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg65(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg65.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg65;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg65','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg65',SQLERRM);
  END;
  -- ---- feature block stg65 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg65')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg65', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg65';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg65','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg65',SQLERRM);
  END;
  -- ---- feature block stg65 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg65','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg65',SQLERRM);
  END;
  -- ---- feature block stg65 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg65','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg65','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg65',SQLERRM);
  END;
  -- ---- feature block stg65 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg65',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg65',SQLERRM);
  END;
  -- ---- feature block stg65 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg65','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg65',SQLERRM);
  END;
  -- ---- feature block stg65 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg65', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg65','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg65',SQLERRM);
  END;
  -- ---- feature block stg65 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg65', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg65','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg65',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_65','done depth '||p_depth);

  CALL erp.erp_usp_stage_66(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_65_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_66(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 66;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_66','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg66 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg66','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg66',SQLERRM);
  END;
  -- ---- feature block stg66 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg66','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg66',SQLERRM);
  END;
  -- ---- feature block stg66 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg66','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg66',SQLERRM);
  END;
  -- ---- feature block stg66 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg66','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg66',SQLERRM);
  END;
  -- ---- feature block stg66 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg66','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg66',SQLERRM);
  END;
  -- ---- feature block stg66 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg66','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg66',SQLERRM);
  END;
  -- ---- feature block stg66 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg66 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg66(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg66.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg66(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg66.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg66;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg66','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg66',SQLERRM);
  END;
  -- ---- feature block stg66 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg66')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg66', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg66';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg66','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg66',SQLERRM);
  END;
  -- ---- feature block stg66 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg66','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg66',SQLERRM);
  END;
  -- ---- feature block stg66 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg66','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg66','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg66',SQLERRM);
  END;
  -- ---- feature block stg66 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg66',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg66',SQLERRM);
  END;
  -- ---- feature block stg66 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg66','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg66',SQLERRM);
  END;
  -- ---- feature block stg66 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg66', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg66','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg66',SQLERRM);
  END;
  -- ---- feature block stg66 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg66', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg66','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg66',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_66','done depth '||p_depth);

  CALL erp.erp_usp_stage_67(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_66_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_67(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 67;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_67','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg67 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg67','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg67',SQLERRM);
  END;
  -- ---- feature block stg67 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg67','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg67',SQLERRM);
  END;
  -- ---- feature block stg67 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg67','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg67',SQLERRM);
  END;
  -- ---- feature block stg67 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg67','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg67',SQLERRM);
  END;
  -- ---- feature block stg67 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg67','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg67',SQLERRM);
  END;
  -- ---- feature block stg67 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg67','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg67',SQLERRM);
  END;
  -- ---- feature block stg67 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg67 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg67(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg67.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg67(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg67.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg67;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg67','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg67',SQLERRM);
  END;
  -- ---- feature block stg67 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg67')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg67', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg67';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg67','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg67',SQLERRM);
  END;
  -- ---- feature block stg67 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg67','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg67',SQLERRM);
  END;
  -- ---- feature block stg67 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg67','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg67','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg67',SQLERRM);
  END;
  -- ---- feature block stg67 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg67',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg67',SQLERRM);
  END;
  -- ---- feature block stg67 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg67','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg67',SQLERRM);
  END;
  -- ---- feature block stg67 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg67', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg67','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg67',SQLERRM);
  END;
  -- ---- feature block stg67 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg67', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg67','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg67',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_67','done depth '||p_depth);

  CALL erp.erp_usp_stage_68(p_depth + 1);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_67_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_stage_68(p_depth int DEFAULT 0)
LANGUAGE plpgsql AS $$
DECLARE
  v_marker int := 68;
BEGIN
  CALL erp.erp_usp_write_audit_trail('STAGE_START','STAGE_68','depth '||p_depth||' marker '||v_marker);

  -- ---- feature block stg68 : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','stg68','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','stg68',SQLERRM);
  END;
  -- ---- feature block stg68 : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','stg68','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','stg68',SQLERRM);
  END;
  -- ---- feature block stg68 : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','stg68','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','stg68',SQLERRM);
  END;
  -- ---- feature block stg68 : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','stg68','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','stg68',SQLERRM);
  END;
  -- ---- feature block stg68 : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','stg68','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','stg68',SQLERRM);
  END;
  -- ---- feature block stg68 : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','stg68','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','stg68',SQLERRM);
  END;
  -- ---- feature block stg68 : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_stg68 (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_stg68(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg68.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_stg68(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_stg68.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_stg68;
    CALL erp.erp_usp_write_audit_trail('UPSERT','stg68','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','stg68',SQLERRM);
  END;
  -- ---- feature block stg68 : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_stg68')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_stg68', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_stg68';
    CALL erp.erp_usp_write_audit_trail('MERGE','stg68','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','stg68',SQLERRM);
  END;
  -- ---- feature block stg68 : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','stg68','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','stg68',SQLERRM);
  END;
  -- ---- feature block stg68 : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','stg68','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','stg68','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','stg68',SQLERRM);
  END;
  -- ---- feature block stg68 : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','stg68',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','stg68',SQLERRM);
  END;
  -- ---- feature block stg68 : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','stg68','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','stg68',SQLERRM);
  END;
  -- ---- feature block stg68 : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'stg68', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','stg68','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','stg68',SQLERRM);
  END;
  -- ---- feature block stg68 : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'stg68', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','stg68','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','stg68',SQLERRM);
  END;

  CALL erp.erp_usp_write_audit_trail('STAGE_END','STAGE_68','done depth '||p_depth);

  CALL erp.erp_usp_edge_recalc(p_depth);

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('ERP_USP_STAGE_68_ERR','ERP',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_touch_views()
LANGUAGE plpgsql AS $$
DECLARE v_n int := 0;
BEGIN
  SELECT COUNT(*) INTO v_n FROM erp.erp_v_order_summary;
  CALL erp.erp_usp_write_audit_trail('TOUCH_VIEW','erp_v_order_summary','rows='||v_n);
  SELECT COUNT(*) INTO v_n FROM erp.erp_v_inventory_value;
  CALL erp.erp_usp_write_audit_trail('TOUCH_VIEW','erp_v_inventory_value','rows='||v_n);
  SELECT COUNT(*) INTO v_n FROM erp.erp_mv_product_sales;
  CALL erp.erp_usp_write_audit_trail('TOUCH_VIEW','erp_mv_product_sales','rows='||v_n);
  SELECT COUNT(*) INTO v_n FROM erp.erp_mv_customer_rev;
  CALL erp.erp_usp_write_audit_trail('TOUCH_VIEW','erp_mv_customer_rev','rows='||v_n);
EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('TOUCH_VIEW_ERR','views',SQLERRM);
END; $$;

-- ============================================================================
-- CROSS-SCHEMA reference objects in erp_ref, referenced from erp
-- ============================================================================

-- reference table
CREATE TABLE IF NOT EXISTS erp_ref.ref_region (
  region_id   int PRIMARY KEY,
  region_name text,
  active      boolean DEFAULT true
);

-- trigger log used by the AFTER INSERT trigger
CREATE TABLE IF NOT EXISTS erp_ref.ref_region_log (
  log_id      int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  region_id   int,
  note        text,
  logged_at   timestamptz DEFAULT now()
);

-- audit table used by usp_ref_log
CREATE TABLE IF NOT EXISTS erp_ref.ref_audit (
  audit_id    int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  event_type  text,
  message     text,
  logged_at   timestamptz DEFAULT now()
);

INSERT INTO erp_ref.ref_region (region_id, region_name, active) VALUES
  (1, 'NORTH', true),
  (2, 'SOUTH', true),
  (3, 'EAST',  true),
  (4, 'WEST',  false)
ON CONFLICT (region_id) DO NOTHING;

-- view over the reference table
CREATE OR REPLACE VIEW erp_ref.vw_region AS
  SELECT region_id, region_name, active
  FROM erp_ref.ref_region
  WHERE active;

-- scalar function returning a region name
CREATE OR REPLACE FUNCTION erp_ref.fn_region_name(p_region_id int)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE v_name text;
BEGIN
  SELECT region_name INTO v_name FROM erp_ref.ref_region WHERE region_id = p_region_id;
  RETURN COALESCE(v_name, 'UNKNOWN');
END; $$;

-- audit-log procedure (no-op safe)
CREATE OR REPLACE PROCEDURE erp_ref.usp_ref_log(p_event text, p_message text)
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO erp_ref.ref_audit(event_type, message)
  VALUES (p_event, substr(p_message, 1, 400));
EXCEPTION WHEN OTHERS THEN NULL;
END; $$;

-- trigger function + trigger on erp_ref.ref_region (AFTER INSERT)
CREATE OR REPLACE FUNCTION erp_ref.trg_fn_ref_region() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO erp_ref.ref_region_log(region_id, note)
  VALUES (NEW.region_id, 'row after insert region='||COALESCE(NEW.region_name,'-'));
  RETURN NULL;
END; $$;

DROP TRIGGER IF EXISTS trg_ref_region ON erp_ref.ref_region;
CREATE TRIGGER trg_ref_region AFTER INSERT ON erp_ref.ref_region
  FOR EACH ROW EXECUTE FUNCTION erp_ref.trg_fn_ref_region();

-- cross-schema touch proc in erp that reaches into erp_ref
CREATE OR REPLACE PROCEDURE erp.erp_usp_xschema_touch()
LANGUAGE plpgsql AS $$
DECLARE v_cnt int := 0; v_vcnt int := 0; v_name text; v_new_id int;
BEGIN
  -- (a) cross-schema CALL
  CALL erp_ref.usp_ref_log('XSCHEMA_START','erp reaching into erp_ref');

  -- (b) cross-schema reads: table, view, function
  SELECT COUNT(*) INTO v_cnt  FROM erp_ref.ref_region;
  SELECT COUNT(*) INTO v_vcnt FROM erp_ref.vw_region;
  v_name := erp_ref.fn_region_name(1);

  -- (c) cross-schema INSERT that fires erp_ref.trg_ref_region
  SELECT COALESCE(MAX(region_id),0) + 1 INTO v_new_id FROM erp_ref.ref_region;
  INSERT INTO erp_ref.ref_region (region_id, region_name, active)
  VALUES (v_new_id, 'GEN_'||v_new_id, true);

  CALL erp_ref.usp_ref_log('XSCHEMA_END',
    'regions='||v_cnt||' active='||v_vcnt||' name1='||COALESCE(v_name,'-')||' new='||v_new_id);
  CALL erp.erp_usp_write_audit_trail('XSCHEMA','erp_usp_xschema_touch',
    'touched erp_ref: regions='||v_cnt||' active='||v_vcnt);
EXCEPTION WHEN OTHERS THEN
  CALL erp.erp_usp_write_audit_trail('XSCHEMA_ERR','erp_usp_xschema_touch',SQLERRM);
END; $$;


CREATE OR REPLACE PROCEDURE erp.erp_usp_run_cycle()
LANGUAGE plpgsql AS $$
DECLARE v_run_id int; v_orders int := 0; v_revenue numeric := 0; v_maxdepth int;
BEGIN
  CALL erp.erp_usp_write_audit_trail('CYCLE_START','ERP_RUN_CYCLE','Starting full ERP cycle');
  v_maxdepth := COALESCE(erp.erp_fn_get_config('MAX_DEPTH')::int, 8);

  -- Deep MRP/finance business chain (depth >= 12)
  CALL erp.erp_usp_init_mrp_run(v_run_id);

  SELECT COUNT(DISTINCT so.order_id), COALESCE(SUM(sol.line_amount),0)
    INTO v_orders, v_revenue
  FROM erp.erp_sales_order so
  JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
  JOIN erp.erp_customer c ON c.customer_id = so.customer_id;

  -- ---- feature block cycle : control flow (IF/ELSIF/ELSE, CASE, WHILE, FOR i) ----
  DECLARE v_i int := 0; v_acc int := 0; v_grade text;
  BEGIN
    WHILE v_i < 5 LOOP
      v_i := v_i + 1;
      CASE
        WHEN v_i <= 1 THEN v_grade := 'LOW';
        WHEN v_i <= 3 THEN v_grade := 'MED';
        ELSE v_grade := 'HIGH';
      END CASE;
      IF v_grade = 'HIGH' THEN v_acc := v_acc + v_i;
      ELSIF v_grade = 'MED' THEN v_acc := v_acc + 1;
      ELSE v_acc := v_acc + 0;
      END IF;
    END LOOP;
    FOR v_i IN 1..3 LOOP v_acc := v_acc + v_i; END LOOP;
    CALL erp.erp_usp_write_audit_trail('CTL','cycle','ctl acc='||v_acc||' grade='||COALESCE(v_grade,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CTL_ERR','cycle',SQLERRM);
  END;
  -- ---- feature block cycle : explicit cursor + FOR-cursor + INNER/LEFT join ----
  DECLARE
    c1 CURSOR FOR
      SELECT p.product_id, p.sku, COALESCE(i.qty_on_hand,0) AS qoh
      FROM erp.erp_product p
      LEFT JOIN erp.erp_inventory i ON i.product_id = p.product_id;
    v_rec record; v_cnt int := 0; v_sum numeric := 0;
  BEGIN
    OPEN c1;
    LOOP
      FETCH c1 INTO v_rec;
      EXIT WHEN NOT FOUND;
      v_cnt := v_cnt + 1;
      v_sum := v_sum + v_rec.qoh;
    END LOOP;
    CLOSE c1;
    FOR v_rec IN
      SELECT so.order_id, c.customer_name
      FROM erp.erp_sales_order so
      INNER JOIN erp.erp_customer c ON c.customer_id = so.customer_id
    LOOP
      v_cnt := v_cnt + 1;
    END LOOP;
    CALL erp.erp_usp_write_audit_trail('CURSOR','cycle','rows='||v_cnt||' sum='||v_sum);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('CURSOR_ERR','cycle',SQLERRM);
  END;
  -- ---- feature block cycle : CTE + window functions (RANK, SUM OVER) ----
  DECLARE v_top text; v_n int := 0;
  BEGIN
    WITH sales AS (
      SELECT so.customer_id, SUM(sol.line_amount) AS rev
      FROM erp.erp_sales_order so
      JOIN erp.erp_sales_order_line sol ON sol.order_id = so.order_id
      GROUP BY so.customer_id
    ), ranked AS (
      SELECT customer_id, rev,
             RANK() OVER (ORDER BY rev DESC) AS rnk,
             SUM(rev) OVER () AS total,
             LAG(rev) OVER (ORDER BY rev DESC) AS prev_rev
      FROM sales
    )
    SELECT customer_id::text INTO v_top FROM ranked WHERE rnk = 1 LIMIT 1;
    SELECT COUNT(*) INTO v_n FROM erp.erp_product;
    CALL erp.erp_usp_write_audit_trail('WINDOW','cycle','top_cust='||COALESCE(v_top,'none')||' prods='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('WINDOW_ERR','cycle',SQLERRM);
  END;
  -- ---- feature block cycle : RECURSIVE CTE over the BOM tree ----
  DECLARE v_rows int := 0; v_max int := 0;
  BEGIN
    WITH RECURSIVE tree(pid, cid, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp.erp_bom_header bh
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.pid, bc.component_id, t.lvl + 1
      FROM tree t
      JOIN erp.erp_bom_header bh ON bh.product_id = t.cid
      JOIN erp.erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), COALESCE(MAX(lvl),0) INTO v_rows, v_max FROM tree;
    CALL erp.erp_usp_write_audit_trail('RECURSIVE','cycle','rows='||v_rows||' maxlvl='||v_max);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('RECURSIVE_ERR','cycle',SQLERRM);
  END;
  -- ---- feature block cycle : LATERAL join (top-2 lines per order) ----
  DECLARE v_cnt int := 0; v_amt numeric := 0;
  BEGIN
    SELECT COUNT(*), COALESCE(SUM(l.line_amount),0) INTO v_cnt, v_amt
    FROM erp.erp_sales_order so
    CROSS JOIN LATERAL (
      SELECT sol.line_amount
      FROM erp.erp_sales_order_line sol
      WHERE sol.order_id = so.order_id
      ORDER BY sol.line_amount DESC
      LIMIT 2
    ) l;
    CALL erp.erp_usp_write_audit_trail('LATERAL','cycle','cnt='||v_cnt||' amt='||v_amt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('LATERAL_ERR','cycle',SQLERRM);
  END;
  -- ---- feature block cycle : join variety (correlated / FULL / RIGHT / CROSS / self) ----
  DECLARE v_a int := 0; v_b int := 0; v_c int := 0; v_d int := 0; v_e int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a FROM erp.erp_product p
      WHERE EXISTS (SELECT 1 FROM erp.erp_sales_order_line sol WHERE sol.product_id = p.product_id);
    SELECT COUNT(*) INTO v_b FROM erp.erp_customer c
      FULL OUTER JOIN erp.erp_invoice i ON i.customer_id = c.customer_id;
    SELECT COUNT(*) INTO v_c FROM erp.erp_sales_order a
      RIGHT JOIN erp.erp_sales_order b ON a.customer_id = b.customer_id;
    SELECT COUNT(*) INTO v_d FROM erp.erp_product p1
      JOIN erp.erp_product p2 ON p1.make_buy = p2.make_buy AND p1.product_id < p2.product_id;
    SELECT COUNT(*) INTO v_e FROM erp.erp_warehouse w CROSS JOIN erp.erp_currency cur;
    CALL erp.erp_usp_write_audit_trail('JOINS','cycle','corr='||v_a||' full='||v_b||' right='||v_c||' self='||v_d||' cross='||v_e);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JOINS_ERR','cycle',SQLERRM);
  END;
  -- ---- feature block cycle : TEMP TABLE + UPSERT (ON CONFLICT ... RETURNING) ----
  DECLARE v_id int; v_cnt int := 0;
  BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_cycle (k int PRIMARY KEY, v numeric);
    INSERT INTO tmp_cycle(k, v) VALUES (1, 10)
    ON CONFLICT (k) DO UPDATE SET v = tmp_cycle.v + EXCLUDED.v
    RETURNING k INTO v_id;
    INSERT INTO tmp_cycle(k, v) VALUES (2, 20)
    ON CONFLICT (k) DO UPDATE SET v = tmp_cycle.v + EXCLUDED.v;
    SELECT COUNT(*) INTO v_cnt FROM tmp_cycle;
    CALL erp.erp_usp_write_audit_trail('UPSERT','cycle','last_k='||COALESCE(v_id,0)||' rows='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('UPSERT_ERR','cycle',SQLERRM);
  END;
  -- ---- feature block cycle : MERGE (PG15+) forecast -> demand staging ----
  DECLARE v_cnt int := 0;
  BEGIN
    MERGE INTO erp.erp_demand d
    USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f GROUP BY f.product_id) s
    ON (d.product_id = s.product_id AND d.source_type = 'MRG_cycle')
    WHEN MATCHED THEN UPDATE SET qty = s.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (nextval('erp.erp_mrp_seq'), s.product_id, 'MRG_cycle', 0, s.qty, now()::date + 30);
    SELECT COUNT(*) INTO v_cnt FROM erp.erp_demand WHERE source_type = 'MRG_cycle';
    CALL erp.erp_usp_write_audit_trail('MERGE','cycle','merged='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('MERGE_ERR','cycle',SQLERRM);
  END;
  -- ---- feature block cycle : dynamic SQL (EXECUTE format %I + EXECUTE ... USING) + arrays ----
  DECLARE v_tab text; v_cnt int := 0; v_total int := 0;
  BEGIN
    FOREACH v_tab IN ARRAY ARRAY['erp_sales_order','erp_product','erp_invoice','erp_work_order'] LOOP
      EXECUTE format('SELECT count(*) FROM erp.%I', v_tab) INTO v_cnt;
      v_total := v_total + v_cnt;
    END LOOP;
    EXECUTE 'SELECT count(*) FROM erp.erp_demand WHERE qty > $1' INTO v_cnt USING 0;
    CALL erp.erp_usp_write_audit_trail('DYNSQL','cycle','total='||v_total||' dqty='||v_cnt);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('DYNSQL_ERR','cycle',SQLERRM);
  END;
  -- ---- feature block cycle : arrays + JSONB (jsonb_build_object, to_jsonb, unnest) ----
  DECLARE v_arr int[]; v_sum int := 0; v_js jsonb;
  BEGIN
    SELECT array_agg(product_id ORDER BY product_id) INTO v_arr FROM erp.erp_product;
    IF v_arr IS NOT NULL THEN
      SELECT COALESCE(SUM(x),0) INTO v_sum FROM unnest(v_arr) AS x;
    END IF;
    v_js := jsonb_build_object('prefix','cycle','count',COALESCE(array_length(v_arr,1),0),'ids',to_jsonb(v_arr));
    CALL erp.erp_usp_write_audit_trail('JSONB','cycle','sum='||v_sum||' count='||(v_js->>'count'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('JSONB_ERR','cycle',SQLERRM);
  END;
  -- ---- feature block cycle : function calls (all base fns + composite TYPE) ----
  DECLARE
    v_pid int; v_net numeric := 0; v_oh numeric := 0; v_ss numeric := 0;
    v_bc int := 0; v_ot numeric := 0; v_fx numeric := 1; v_cfg text; v_m erp.erp_money;
  BEGIN
    v_cfg := erp.erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR v_pid IN SELECT product_id FROM erp.erp_product LOOP
      v_net := v_net + erp.erp_fn_net_requirement(v_pid);
      v_oh  := v_oh  + erp.erp_fn_onhand_qty(v_pid);
      v_ss  := v_ss  + erp.erp_fn_safety_stock(v_pid);
      v_bc  := v_bc  + erp.erp_fn_bom_component_count(v_pid);
    END LOOP;
    v_ot := erp.erp_fn_order_total(1);
    v_fx := erp.erp_fn_fx_rate(2);
    v_m  := erp.erp_fn_money(v_ot, 'USD');
    CALL erp.erp_usp_write_audit_trail('FNCALLS','cycle',
      'net='||v_net||' oh='||v_oh||' ss='||v_ss||' bc='||v_bc||' amt='||(v_m).amount||' fx='||v_fx||' cfg='||COALESCE(v_cfg,'-'));
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('FNCALLS_ERR','cycle',SQLERRM);
  END;
  -- ---- feature block cycle : deeply nested subqueries ----
  DECLARE v_n int := 0;
  BEGIN
    SELECT COUNT(*) INTO v_n FROM erp.erp_product p
    WHERE p.product_id IN (
      SELECT product_id FROM erp.erp_sales_order_line WHERE order_id IN (
        SELECT order_id FROM erp.erp_sales_order WHERE customer_id IN (
          SELECT customer_id FROM erp.erp_customer WHERE currency_id IN (
            SELECT currency_id FROM erp.erp_currency WHERE code IN ('USD','EUR','GBP')
          )
        )
      )
    );
    CALL erp.erp_usp_write_audit_trail('NESTED','cycle','n='||v_n);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('NESTED_ERR','cycle',SQLERRM);
  END;
  -- ---- feature block cycle : global (GUC) vs local scope ----
  DECLARE v_g text; v_local int := 42;
  BEGIN
    PERFORM set_config('erp.run_tag', 'cycle', true);
    v_g := current_setting('erp.run_tag', true);
    CALL erp.erp_usp_write_audit_trail('SCOPE','cycle','guc='||COALESCE(v_g,'-')||' local='||v_local);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SCOPE_ERR','cycle',SQLERRM);
  END;
  -- ---- feature block cycle : savepoint-simulation via nested BEGIN..EXCEPTION ----
  DECLARE v_ok int := 0; v_bad int := 0;
  BEGIN
    BEGIN
      v_bad := (1 / 0);          -- deliberate error, isolated like a rolled-back savepoint
    EXCEPTION WHEN division_by_zero THEN
      v_bad := -1;
    END;
    BEGIN
      INSERT INTO erp.erp_audit_log(audit_id, event_type, object_name, message)
      VALUES (nextval('erp.erp_audit_seq'), 'SP_OK', 'cycle', 'savepoint-sim ok');
      v_ok := 1;
    EXCEPTION WHEN OTHERS THEN
      v_ok := 0;
    END;
    CALL erp.erp_usp_write_audit_trail('SAVEPOINT','cycle','ok='||v_ok||' bad='||v_bad);
  EXCEPTION WHEN OTHERS THEN CALL erp.erp_usp_write_audit_trail('SAVEPOINT_ERR','cycle',SQLERRM);
  END;

  -- Deep analytics stage chain (depth >= 68)
  CALL erp.erp_usp_stage_01(0);

  -- Controlled recursion + circular call, both guarded by a depth limit
  CALL erp.erp_usp_recurse(0, v_maxdepth);
  CALL erp.erp_usp_ping(0, 3);

  -- Feature showcase (overloads, RETURNS TABLE/SETOF, arrays, jsonb)
  CALL erp.erp_usp_feature_showcase();

  -- Touch every view + materialized view so they enter the call tree
  CALL erp.erp_usp_touch_views();

  -- Cross-schema reach into erp_ref (call, reads, trigger-firing insert)
  CALL erp.erp_usp_xschema_touch();

  CALL erp.erp_usp_write_audit_trail('CYCLE_END','ERP_RUN_CYCLE',
    'run='||v_run_id||' orders='||v_orders||' revenue='||v_revenue);
  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  CALL erp.erp_usp_write_audit_trail('CYCLE_ERROR','ERP_RUN_CYCLE',SQLERRM);
END; $$;


-- ============================================================================
-- DO blocks (anonymous code blocks)
-- ============================================================================
DO $$
DECLARE v_cnt int;
BEGIN
  SELECT COUNT(*) INTO v_cnt FROM erp.erp_product;
  RAISE NOTICE 'erp seed loaded: % products', v_cnt;
END $$;

DO $$
BEGIN
  PERFORM set_config('erp.loaded_at', now()::text, false);
END $$;
