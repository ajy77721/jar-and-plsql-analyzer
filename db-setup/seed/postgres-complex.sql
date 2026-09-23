-- ============================================================================
-- PostgreSQL COMPLEX ERP demo (PL/pgSQL) — a deep MRP order-to-cash call chain
-- mirroring the other dialects' ERP_USP_RUN_CYCLE flow (depth ~12).
-- Idempotent: safe to re-run. Schema: erp.
-- Entry procedure for analysis:  erp.usp_run_cycle
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS erp;
SET search_path TO erp;

-- ── Tables ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS erp.product        (id int PRIMARY KEY, sku text, name text, is_mfg boolean DEFAULT false, std_cost numeric DEFAULT 0);
CREATE TABLE IF NOT EXISTS erp.inventory      (product int PRIMARY KEY, qty_onhand numeric DEFAULT 0, qty_reserved numeric DEFAULT 0);
CREATE TABLE IF NOT EXISTS erp.customer       (id int PRIMARY KEY, name text, tier text, credit_limit numeric DEFAULT 0);
CREATE TABLE IF NOT EXISTS erp.supplier       (id int PRIMARY KEY, name text, score numeric DEFAULT 0, currency text DEFAULT 'USD');
CREATE TABLE IF NOT EXISTS erp.currency       (code text PRIMARY KEY, rate numeric DEFAULT 1);
CREATE TABLE IF NOT EXISTS erp.sales_order    (id int PRIMARY KEY, cust int, status text, total numeric DEFAULT 0);
CREATE TABLE IF NOT EXISTS erp.sales_order_line(id int PRIMARY KEY, ord int, product int, qty numeric, price numeric);
CREATE TABLE IF NOT EXISTS erp.demand         (id int PRIMARY KEY, product int, qty numeric, due date);
CREATE TABLE IF NOT EXISTS erp.bom            (parent int, child int, qty numeric);
CREATE TABLE IF NOT EXISTS erp.work_order     (id int PRIMARY KEY, product int, qty numeric, status text);
CREATE TABLE IF NOT EXISTS erp.purchase_req   (id int PRIMARY KEY, product int, qty numeric, supplier int);
CREATE TABLE IF NOT EXISTS erp.purchase_order (id int PRIMARY KEY, supplier int, product int, qty numeric, status text);
CREATE TABLE IF NOT EXISTS erp.shipment       (id int PRIMARY KEY, ord int, status text);
CREATE TABLE IF NOT EXISTS erp.invoice        (id int PRIMARY KEY, ord int, amount numeric, status text);
CREATE TABLE IF NOT EXISTS erp.gl_batch       (id int PRIMARY KEY, period text, status text);
CREATE TABLE IF NOT EXISTS erp.gl_entry       (id serial PRIMARY KEY, batch int, ord int, amount numeric, posted boolean DEFAULT false);
CREATE TABLE IF NOT EXISTS erp.journal_line   (id serial PRIMARY KEY, entry int, account text, debit numeric, credit numeric);
CREATE TABLE IF NOT EXISTS erp.audit_trail    (id serial PRIMARY KEY, area text, ref_id int, action text, detail text, ts timestamp DEFAULT now());

-- ── Functions (pure calculations — called inside expressions) ────────────────
CREATE OR REPLACE FUNCTION erp.fn_fx_rate(p_code text) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE v numeric := 1;
BEGIN
  SELECT rate INTO v FROM erp.currency WHERE code = p_code;
  RETURN COALESCE(v, 1);
END; $$;

CREATE OR REPLACE FUNCTION erp.fn_order_total(p_ord int) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE v numeric := 0;
BEGIN
  SELECT COALESCE(SUM(qty * price), 0) INTO v FROM erp.sales_order_line WHERE ord = p_ord;
  RETURN v;
END; $$;

CREATE OR REPLACE FUNCTION erp.fn_safety_stock(p_prod int) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE v numeric := 0;
BEGIN
  SELECT COALESCE(qty_onhand * 0.1, 0) INTO v FROM erp.inventory WHERE product = p_prod;
  RETURN v;
END; $$;

CREATE OR REPLACE FUNCTION erp.fn_customer_tier(p_cust int) RETURNS text LANGUAGE plpgsql AS $$
DECLARE v text := 'STD';
BEGIN
  SELECT tier INTO v FROM erp.customer WHERE id = p_cust;
  RETURN COALESCE(v, 'STD');
END; $$;

CREATE OR REPLACE FUNCTION erp.fn_supplier_score(p_sup int) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE v numeric := 0;
BEGIN
  SELECT score INTO v FROM erp.supplier WHERE id = p_sup;
  RETURN COALESCE(v, 0);
END; $$;

-- ── Deep procedure chain (d11 → d0) ──────────────────────────────────────────
CREATE OR REPLACE PROCEDURE erp.usp_finalize_gl_batch(p_batch int) LANGUAGE plpgsql AS $$
DECLARE v_cnt int;
BEGIN
  UPDATE erp.gl_batch SET status = 'FINAL' WHERE id = p_batch;
  SELECT COUNT(*) INTO v_cnt FROM erp.gl_entry WHERE batch = p_batch;
  INSERT INTO erp.audit_trail(area, ref_id, action, detail) VALUES('usp_finalize_gl_batch', p_batch, 'FINALIZE', 'entries=' || v_cnt);
  COMMIT;
EXCEPTION WHEN OTHERS THEN ROLLBACK;
END; $$;

CREATE OR REPLACE PROCEDURE erp.usp_post_gl_entries(p_ord int) LANGUAGE plpgsql AS $$
DECLARE v_amt numeric; v_batch int := 1;
BEGIN
  v_amt := erp.fn_order_total(p_ord);
  INSERT INTO erp.gl_entry(batch, ord, amount, posted) VALUES(v_batch, p_ord, v_amt, true);
  INSERT INTO erp.journal_line(entry, account, debit, credit) VALUES(v_batch, 'AR', v_amt, 0);
  INSERT INTO erp.journal_line(entry, account, debit, credit) VALUES(v_batch, 'REV', 0, v_amt);
  INSERT INTO erp.audit_trail(area, ref_id, action, detail) VALUES('usp_post_gl_entries', p_ord, 'POST', 'amt=' || v_amt);
  CALL erp.usp_finalize_gl_batch(v_batch);
  COMMIT;
EXCEPTION WHEN OTHERS THEN ROLLBACK;
END; $$;

CREATE OR REPLACE PROCEDURE erp.usp_generate_invoice(p_ord int) LANGUAGE plpgsql AS $$
DECLARE v_amt numeric;
BEGIN
  v_amt := erp.fn_order_total(p_ord) * erp.fn_fx_rate('USD');
  INSERT INTO erp.invoice(id, ord, amount, status) VALUES(p_ord, p_ord, v_amt, 'ISSUED')
    ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount;
  INSERT INTO erp.audit_trail(area, ref_id, action, detail) VALUES('usp_generate_invoice', p_ord, 'INVOICE', 'amt=' || v_amt);
  CALL erp.usp_post_gl_entries(p_ord);
  COMMIT;
EXCEPTION WHEN OTHERS THEN ROLLBACK;
END; $$;

CREATE OR REPLACE PROCEDURE erp.usp_ship_order(p_ord int) LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO erp.shipment(id, ord, status) VALUES(p_ord, p_ord, 'SHIPPED') ON CONFLICT (id) DO UPDATE SET status = 'SHIPPED';
  UPDATE erp.sales_order SET status = 'SHIPPED' WHERE id = p_ord;
  UPDATE erp.inventory SET qty_reserved = qty_reserved - 1 WHERE product = p_ord;
  INSERT INTO erp.audit_trail(area, ref_id, action, detail) VALUES('usp_ship_order', p_ord, 'SHIP', 'ok');
  CALL erp.usp_generate_invoice(p_ord);
  COMMIT;
EXCEPTION WHEN OTHERS THEN ROLLBACK;
END; $$;

CREATE OR REPLACE PROCEDURE erp.usp_complete_work_order(p_wo int) LANGUAGE plpgsql AS $$
DECLARE v_prod int;
BEGIN
  SELECT product INTO v_prod FROM erp.work_order WHERE id = p_wo;
  UPDATE erp.work_order SET status = 'DONE' WHERE id = p_wo;
  UPDATE erp.inventory SET qty_onhand = qty_onhand + 10 WHERE product = v_prod;
  INSERT INTO erp.audit_trail(area, ref_id, action, detail) VALUES('usp_complete_work_order', p_wo, 'COMPLETE', 'prod=' || v_prod);
  CALL erp.usp_ship_order(p_wo);
  COMMIT;
EXCEPTION WHEN OTHERS THEN ROLLBACK;
END; $$;

CREATE OR REPLACE PROCEDURE erp.usp_release_work_orders(p_prod int) LANGUAGE plpgsql AS $$
DECLARE v_ss numeric;
BEGIN
  v_ss := erp.fn_safety_stock(p_prod);
  INSERT INTO erp.work_order(id, product, qty, status) VALUES(p_prod, p_prod, 100 + v_ss, 'RELEASED') ON CONFLICT (id) DO UPDATE SET status = 'RELEASED';
  INSERT INTO erp.audit_trail(area, ref_id, action, detail) VALUES('usp_release_work_orders', p_prod, 'RELEASE', 'ss=' || v_ss);
  CALL erp.usp_complete_work_order(p_prod);
  COMMIT;
EXCEPTION WHEN OTHERS THEN ROLLBACK;
END; $$;

CREATE OR REPLACE PROCEDURE erp.usp_compute_net_requirements(p_prod int) LANGUAGE plpgsql AS $$
DECLARE v_onhand numeric;
BEGIN
  SELECT COALESCE(qty_onhand, 0) INTO v_onhand FROM erp.inventory WHERE product = p_prod;
  INSERT INTO erp.purchase_req(id, product, qty, supplier) VALUES(p_prod, p_prod, GREATEST(0, 100 - v_onhand), 1) ON CONFLICT (id) DO UPDATE SET qty = EXCLUDED.qty;
  INSERT INTO erp.audit_trail(area, ref_id, action, detail) VALUES('usp_compute_net_requirements', p_prod, 'NETREQ', 'onhand=' || v_onhand);
  CALL erp.usp_release_work_orders(p_prod);
  COMMIT;
EXCEPTION WHEN OTHERS THEN ROLLBACK;
END; $$;

CREATE OR REPLACE PROCEDURE erp.usp_explode_demand(p_prod int) LANGUAGE plpgsql AS $$
DECLARE r RECORD;
BEGIN
  FOR r IN SELECT child, qty FROM erp.bom WHERE parent = p_prod LOOP
    INSERT INTO erp.demand(id, product, qty, due) VALUES(r.child, r.child, r.qty, now()::date) ON CONFLICT (id) DO UPDATE SET qty = erp.demand.qty + EXCLUDED.qty;
  END LOOP;
  INSERT INTO erp.audit_trail(area, ref_id, action, detail) VALUES('usp_explode_demand', p_prod, 'EXPLODE', 'ok');
  CALL erp.usp_compute_net_requirements(p_prod);
  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
END; $$;

CREATE OR REPLACE PROCEDURE erp.usp_collect_demand(p_prod int) LANGUAGE plpgsql AS $$
DECLARE v_total numeric;
BEGIN
  SELECT COALESCE(SUM(qty), 0) INTO v_total FROM erp.demand WHERE product = p_prod;
  UPDATE erp.demand SET qty = qty WHERE product = p_prod;
  INSERT INTO erp.audit_trail(area, ref_id, action, detail) VALUES('usp_collect_demand', p_prod, 'COLLECT', 'total=' || v_total);
  CALL erp.usp_explode_demand(p_prod);
  COMMIT;
EXCEPTION WHEN OTHERS THEN ROLLBACK;
END; $$;

CREATE OR REPLACE PROCEDURE erp.usp_validate_master_data(p_prod int) LANGUAGE plpgsql AS $$
DECLARE v_ok int;
BEGIN
  SELECT COUNT(*) INTO v_ok FROM erp.product WHERE id = p_prod;
  UPDATE erp.product SET std_cost = COALESCE(std_cost, 0) WHERE id = p_prod;
  INSERT INTO erp.audit_trail(area, ref_id, action, detail) VALUES('usp_validate_master_data', p_prod, 'VALIDATE', 'ok=' || v_ok);
  CALL erp.usp_collect_demand(p_prod);
  COMMIT;
EXCEPTION WHEN OTHERS THEN ROLLBACK;
END; $$;

CREATE OR REPLACE PROCEDURE erp.usp_init_mrp_run(p_prod int) LANGUAGE plpgsql AS $$
DECLARE v_tier text;
BEGIN
  v_tier := erp.fn_customer_tier(1);
  INSERT INTO erp.gl_batch(id, period, status) VALUES(1, '2026-07', 'OPEN') ON CONFLICT (id) DO NOTHING;
  INSERT INTO erp.audit_trail(area, ref_id, action, detail) VALUES('usp_init_mrp_run', p_prod, 'INIT', 'tier=' || v_tier);
  CALL erp.usp_validate_master_data(p_prod);
  COMMIT;
EXCEPTION WHEN OTHERS THEN ROLLBACK;
END; $$;

-- Edge proc (called by root; touches suppliers/pricing) to widen the tree
CREATE OR REPLACE PROCEDURE erp.usp_edge_recalc(p_sup int) LANGUAGE plpgsql AS $$
DECLARE v_score numeric;
BEGIN
  v_score := erp.fn_supplier_score(p_sup);
  UPDATE erp.supplier SET score = v_score + 1 WHERE id = p_sup;
  UPDATE erp.purchase_order SET status = 'REPRICED' WHERE supplier = p_sup;
  INSERT INTO erp.audit_trail(area, ref_id, action, detail) VALUES('usp_edge_recalc', p_sup, 'RECALC', 'score=' || v_score);
  COMMIT;
EXCEPTION WHEN OTHERS THEN ROLLBACK;
END; $$;

-- Root (d0)
CREATE OR REPLACE PROCEDURE erp.usp_run_cycle(p_prod int) LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO erp.audit_trail(area, ref_id, action, detail) VALUES('usp_run_cycle', p_prod, 'START', 'begin cycle');
  CALL erp.usp_edge_recalc(1);
  CALL erp.usp_init_mrp_run(p_prod);
  UPDATE erp.sales_order SET status = 'CYCLED' WHERE cust = p_prod;
  COMMIT;
EXCEPTION WHEN OTHERS THEN ROLLBACK;
END; $$;

-- ── Minimal seed data ────────────────────────────────────────────────────────
INSERT INTO erp.currency(code, rate) VALUES ('USD', 1) ON CONFLICT (code) DO NOTHING;
INSERT INTO erp.product(id, sku, name, is_mfg, std_cost) VALUES (1, 'SKU1', 'Widget', true, 10) ON CONFLICT (id) DO NOTHING;
INSERT INTO erp.inventory(product, qty_onhand, qty_reserved) VALUES (1, 50, 0) ON CONFLICT (product) DO NOTHING;
INSERT INTO erp.customer(id, name, tier, credit_limit) VALUES (1, 'Acme', 'GOLD', 100000) ON CONFLICT (id) DO NOTHING;
INSERT INTO erp.supplier(id, name, score, currency) VALUES (1, 'SupCo', 5, 'USD') ON CONFLICT (id) DO NOTHING;
INSERT INTO erp.sales_order(id, cust, status, total) VALUES (1, 1, 'NEW', 0) ON CONFLICT (id) DO NOTHING;
INSERT INTO erp.sales_order_line(id, ord, product, qty, price) VALUES (1, 1, 1, 5, 20) ON CONFLICT (id) DO NOTHING;
INSERT INTO erp.bom(parent, child, qty) VALUES (1, 1, 2) ON CONFLICT DO NOTHING;
