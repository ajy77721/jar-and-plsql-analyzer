-- ============================================================================
-- Oracle PL/SQL COMPREHENSIVE ERP demo seed
--   schema/user : ANALYZER            password : Password123!
--   container    : oracle-free         service  : FREEPDB1
--   entry point  : ERP_USP_RUN_CYCLE
--
-- Run as the ANALYZER user, e.g.:
--   sqlplus analyzer/Password123!@//localhost:1521/FREEPDB1 @oracle.sql
--
-- Idempotent / clean-slate demo seed:
--   * tables & GTT   : dropped-if-exists then CREATE
--   * sequences      : dropped-if-exists then CREATE
--   * views / mviews : CREATE OR REPLACE / drop-if-exists then CREATE
--   * routines       : CREATE OR REPLACE
--
-- Feature matrix (each used multiple times):
--   transactions (COMMIT/ROLLBACK/SAVEPOINT/ROLLBACK TO), exception handlers,
--   triggers (row/statement/compound), views, materialized views, sequences,
--   dynamic SQL (EXECUTE IMMEDIATE / OPEN-FOR / DBMS_SQL), IF/ELSIF/ELSE, CASE,
--   LOOP/WHILE/FOR/cursor-FOR, explicit cursors + REF CURSOR, subqueries,
--   correlated subqueries, WITH/CTE incl. RECURSIVE, CONNECT BY hierarchical,
--   INNER/LEFT/RIGHT/FULL/CROSS/self joins, inline views, analytic/window fns,
--   GLOBAL TEMPORARY TABLE, MERGE, INSERT/UPDATE/DELETE/SELECT + DDL,
--   PACKAGES (spec+body) with OVERLOADED subprograms + package GLOBAL vars,
--   collections (assoc array / nested table / VARRAY), BULK COLLECT + FORALL,
--   PIPELINED table function, %TYPE / %ROWTYPE, PRAGMA AUTONOMOUS_TRANSACTION,
--   REF CURSOR OUT params, depth-guarded controlled recursion,
--   DIRECT self-recursion (erp_usp_explode_bom) + MUTUAL/CIRCULAR recursion
--   (erp_pkg_planner.walk_down <-> walk_up, depth guarded).
--
-- Deep call chain (static-analysis depth 12):
--   ERP_USP_RUN_CYCLE -> ERP_USP_INIT_MRP_RUN -> ERP_USP_VALIDATE_MASTER_DATA
--   -> ERP_USP_COLLECT_DEMAND -> ERP_USP_EXPLODE_DEMAND
--   -> ERP_USP_COMPUTE_NET_REQUIREMENTS -> ERP_USP_RELEASE_WORK_ORDERS
--   -> ERP_USP_COMPLETE_WORK_ORDER -> ERP_USP_SHIP_ORDER
--   -> ERP_USP_GENERATE_INVOICE -> ERP_USP_POST_GL_ENTRIES
--   -> ERP_USP_FINALIZE_GL_BATCH
-- ============================================================================

SET DEFINE OFF
WHENEVER SQLERROR CONTINUE


-- ---------------------------------------------------------------- clean slate
BEGIN
  FOR r IN (
    SELECT 'DROP TABLE '||table_name||' CASCADE CONSTRAINTS' AS ddl
    FROM user_tables WHERE table_name IN (
      'ERP_CONFIG',
      'ERP_CURRENCY',
      'ERP_EXCHANGE_RATE',
      'ERP_CUSTOMER',
      'ERP_PRODUCT',
      'ERP_WAREHOUSE',
      'ERP_INVENTORY',
      'ERP_SALES_ORDER',
      'ERP_SALES_ORDER_LINE',
      'ERP_SUPPLIER',
      'ERP_PURCHASE_ORDER',
      'ERP_PURCHASE_ORDER_LINE',
      'ERP_BOM_HEADER',
      'ERP_BOM_COMPONENT',
      'ERP_ROUTING',
      'ERP_ROUTING_OPERATION',
      'ERP_WORK_CENTER',
      'ERP_WORK_ORDER',
      'ERP_MATERIAL_ISSUE',
      'ERP_DEMAND',
      'ERP_FORECAST',
      'ERP_MRP_RUN',
      'ERP_MRP_PLAN',
      'ERP_INVOICE',
      'ERP_INVOICE_LINE',
      'ERP_SHIPMENT',
      'ERP_SHIPMENT_LINE',
      'ERP_GL_ACCOUNT',
      'ERP_GL_ENTRY',
      'ERP_GL_BATCH',
      'ERP_PRICE_LIST',
      'ERP_PRODUCT_PRICE',
      'ERP_AUDIT_LOG',
      'ERP_GTT_STAGE'
    )
  ) LOOP
    BEGIN EXECUTE IMMEDIATE r.ddl; EXCEPTION WHEN OTHERS THEN NULL; END;
  END LOOP;
END;
/

BEGIN
  FOR r IN (
    SELECT 'DROP SEQUENCE '||sequence_name AS ddl
    FROM user_sequences WHERE sequence_name IN (
      'ERP_MRP_SEQ',
      'ERP_AUDIT_SEQ',
      'ERP_DOC_SEQ'
    )
  ) LOOP
    BEGIN EXECUTE IMMEDIATE r.ddl; EXCEPTION WHEN OTHERS THEN NULL; END;
  END LOOP;
END;
/

-- ---------------------------------------------------------------- sequences
CREATE SEQUENCE erp_mrp_seq   START WITH 100000 NOCACHE;
CREATE SEQUENCE erp_audit_seq START WITH 1      NOCACHE;
CREATE SEQUENCE erp_doc_seq   START WITH 500000 NOCACHE;

-- ---------------------------------------------------------------- tables
CREATE TABLE erp_config (
  config_key   VARCHAR2(60) PRIMARY KEY,
  config_value VARCHAR2(200)
);

CREATE TABLE erp_currency (
  currency_id NUMBER PRIMARY KEY,
  code        VARCHAR2(3) NOT NULL,
  name        VARCHAR2(60),
  last_seen   DATE
);

CREATE TABLE erp_exchange_rate (
  rate_id     NUMBER PRIMARY KEY,
  currency_id NUMBER NOT NULL,
  rate        NUMBER DEFAULT 1,
  rate_date   DATE   DEFAULT SYSDATE,
  CONSTRAINT fk_fx_cur FOREIGN KEY (currency_id) REFERENCES erp_currency(currency_id)
);

CREATE TABLE erp_customer (
  customer_id   NUMBER PRIMARY KEY,
  customer_name VARCHAR2(120) NOT NULL,
  currency_id   NUMBER,
  CONSTRAINT fk_cust_cur FOREIGN KEY (currency_id) REFERENCES erp_currency(currency_id)
);

CREATE TABLE erp_product (
  product_id NUMBER PRIMARY KEY,
  sku        VARCHAR2(40) NOT NULL,
  std_cost   NUMBER DEFAULT 0,
  list_price NUMBER DEFAULT 0,
  make_buy   VARCHAR2(1) DEFAULT 'M'
);

CREATE TABLE erp_warehouse (
  warehouse_id NUMBER PRIMARY KEY,
  wh_code      VARCHAR2(10) NOT NULL,
  name         VARCHAR2(80)
);

CREATE TABLE erp_inventory (
  inv_id       NUMBER PRIMARY KEY,
  product_id   NUMBER NOT NULL,
  warehouse_id NUMBER NOT NULL,
  qty_on_hand  NUMBER DEFAULT 0,
  qty_reserved NUMBER DEFAULT 0,
  CONSTRAINT fk_inv_prod FOREIGN KEY (product_id)   REFERENCES erp_product(product_id),
  CONSTRAINT fk_inv_wh   FOREIGN KEY (warehouse_id) REFERENCES erp_warehouse(warehouse_id)
);

CREATE TABLE erp_sales_order (
  order_id     NUMBER PRIMARY KEY,
  customer_id  NUMBER NOT NULL,
  order_date   DATE DEFAULT SYSDATE,
  total_amount NUMBER DEFAULT 0,
  status       VARCHAR2(20) DEFAULT 'NEW',
  CONSTRAINT fk_so_cust FOREIGN KEY (customer_id) REFERENCES erp_customer(customer_id)
);

CREATE TABLE erp_sales_order_line (
  line_id     NUMBER PRIMARY KEY,
  order_id    NUMBER NOT NULL,
  product_id  NUMBER NOT NULL,
  qty         NUMBER DEFAULT 1,
  unit_price  NUMBER DEFAULT 0,
  line_amount NUMBER DEFAULT 0,
  CONSTRAINT fk_sol_ord  FOREIGN KEY (order_id)   REFERENCES erp_sales_order(order_id),
  CONSTRAINT fk_sol_prod FOREIGN KEY (product_id) REFERENCES erp_product(product_id)
);

CREATE TABLE erp_supplier (
  supplier_id   NUMBER PRIMARY KEY,
  supplier_name VARCHAR2(120) NOT NULL
);

CREATE TABLE erp_purchase_order (
  po_id       NUMBER PRIMARY KEY,
  supplier_id NUMBER NOT NULL,
  order_date  DATE DEFAULT SYSDATE,
  status      VARCHAR2(20) DEFAULT 'OPEN',
  CONSTRAINT fk_po_sup FOREIGN KEY (supplier_id) REFERENCES erp_supplier(supplier_id)
);

CREATE TABLE erp_purchase_order_line (
  pol_id     NUMBER PRIMARY KEY,
  po_id      NUMBER NOT NULL,
  product_id NUMBER NOT NULL,
  qty        NUMBER DEFAULT 1,
  unit_cost  NUMBER DEFAULT 0,
  CONSTRAINT fk_pol_po   FOREIGN KEY (po_id)      REFERENCES erp_purchase_order(po_id),
  CONSTRAINT fk_pol_prod FOREIGN KEY (product_id) REFERENCES erp_product(product_id)
);

CREATE TABLE erp_bom_header (
  bom_id      NUMBER PRIMARY KEY,
  product_id  NUMBER NOT NULL,
  active_flag VARCHAR2(1) DEFAULT 'Y',
  CONSTRAINT fk_bom_prod FOREIGN KEY (product_id) REFERENCES erp_product(product_id)
);

CREATE TABLE erp_bom_component (
  bom_comp_id  NUMBER PRIMARY KEY,
  bom_id       NUMBER NOT NULL,
  component_id NUMBER NOT NULL,
  qty_per      NUMBER DEFAULT 1,
  CONSTRAINT fk_bc_bom  FOREIGN KEY (bom_id)       REFERENCES erp_bom_header(bom_id),
  CONSTRAINT fk_bc_prod FOREIGN KEY (component_id) REFERENCES erp_product(product_id)
);

CREATE TABLE erp_work_center (
  work_center_id NUMBER PRIMARY KEY,
  wc_code        VARCHAR2(10) NOT NULL,
  capacity_hrs   NUMBER DEFAULT 8
);

CREATE TABLE erp_routing (
  routing_id NUMBER PRIMARY KEY,
  product_id NUMBER NOT NULL,
  CONSTRAINT fk_rt_prod FOREIGN KEY (product_id) REFERENCES erp_product(product_id)
);

CREATE TABLE erp_routing_operation (
  op_id          NUMBER PRIMARY KEY,
  routing_id     NUMBER NOT NULL,
  work_center_id NUMBER NOT NULL,
  seq_no         NUMBER DEFAULT 10,
  run_time_min   NUMBER DEFAULT 0,
  CONSTRAINT fk_ro_rt FOREIGN KEY (routing_id)     REFERENCES erp_routing(routing_id),
  CONSTRAINT fk_ro_wc FOREIGN KEY (work_center_id) REFERENCES erp_work_center(work_center_id)
);

CREATE TABLE erp_work_order (
  wo_id       NUMBER PRIMARY KEY,
  product_id  NUMBER NOT NULL,
  qty_planned NUMBER DEFAULT 0,
  qty_done    NUMBER DEFAULT 0,
  status      VARCHAR2(20) DEFAULT 'PLANNED',
  CONSTRAINT fk_wo_prod FOREIGN KEY (product_id) REFERENCES erp_product(product_id)
);

CREATE TABLE erp_material_issue (
  issue_id     NUMBER PRIMARY KEY,
  wo_id        NUMBER NOT NULL,
  component_id NUMBER NOT NULL,
  qty_issued   NUMBER DEFAULT 0,
  CONSTRAINT fk_mi_wo   FOREIGN KEY (wo_id)        REFERENCES erp_work_order(wo_id),
  CONSTRAINT fk_mi_prod FOREIGN KEY (component_id) REFERENCES erp_product(product_id)
);

CREATE TABLE erp_demand (
  demand_id   NUMBER PRIMARY KEY,
  product_id  NUMBER NOT NULL,
  source_type VARCHAR2(30),
  source_id   NUMBER,
  qty         NUMBER DEFAULT 0,
  need_date   DATE DEFAULT SYSDATE,
  CONSTRAINT fk_dmd_prod FOREIGN KEY (product_id) REFERENCES erp_product(product_id)
);

CREATE TABLE erp_forecast (
  forecast_id   NUMBER PRIMARY KEY,
  product_id    NUMBER NOT NULL,
  qty           NUMBER DEFAULT 0,
  forecast_date DATE DEFAULT SYSDATE,
  CONSTRAINT fk_fc_prod FOREIGN KEY (product_id) REFERENCES erp_product(product_id)
);

CREATE TABLE erp_mrp_run (
  mrp_run_id    NUMBER PRIMARY KEY,
  run_date      DATE DEFAULT SYSDATE,
  status        VARCHAR2(20),
  planned_count NUMBER DEFAULT 0
);

CREATE TABLE erp_mrp_plan (
  plan_id    NUMBER PRIMARY KEY,
  mrp_run_id NUMBER NOT NULL,
  product_id NUMBER NOT NULL,
  action     VARCHAR2(10),
  qty        NUMBER DEFAULT 0,
  CONSTRAINT fk_mp_run  FOREIGN KEY (mrp_run_id) REFERENCES erp_mrp_run(mrp_run_id),
  CONSTRAINT fk_mp_prod FOREIGN KEY (product_id) REFERENCES erp_product(product_id)
);

CREATE TABLE erp_invoice (
  invoice_id   NUMBER PRIMARY KEY,
  customer_id  NUMBER NOT NULL,
  amount       NUMBER DEFAULT 0,
  status       VARCHAR2(20) DEFAULT 'OPEN',
  invoice_date DATE DEFAULT SYSDATE,
  CONSTRAINT fk_inv_cust FOREIGN KEY (customer_id) REFERENCES erp_customer(customer_id)
);

CREATE TABLE erp_invoice_line (
  invoice_line_id NUMBER PRIMARY KEY,
  invoice_id      NUMBER NOT NULL,
  product_id      NUMBER NOT NULL,
  amount          NUMBER DEFAULT 0,
  CONSTRAINT fk_il_inv  FOREIGN KEY (invoice_id) REFERENCES erp_invoice(invoice_id),
  CONSTRAINT fk_il_prod FOREIGN KEY (product_id) REFERENCES erp_product(product_id)
);

CREATE TABLE erp_shipment (
  shipment_id NUMBER PRIMARY KEY,
  order_id    NUMBER NOT NULL,
  ship_date   DATE DEFAULT SYSDATE,
  CONSTRAINT fk_sh_ord FOREIGN KEY (order_id) REFERENCES erp_sales_order(order_id)
);

CREATE TABLE erp_shipment_line (
  ship_line_id NUMBER PRIMARY KEY,
  shipment_id  NUMBER NOT NULL,
  product_id   NUMBER NOT NULL,
  qty_shipped  NUMBER DEFAULT 0,
  CONSTRAINT fk_sl_sh   FOREIGN KEY (shipment_id) REFERENCES erp_shipment(shipment_id),
  CONSTRAINT fk_sl_prod FOREIGN KEY (product_id)  REFERENCES erp_product(product_id)
);

CREATE TABLE erp_gl_account (
  account_id   NUMBER PRIMARY KEY,
  account_type VARCHAR2(20),
  name         VARCHAR2(80)
);

CREATE TABLE erp_gl_batch (
  batch_id NUMBER PRIMARY KEY,
  status   VARCHAR2(20) DEFAULT 'OPEN',
  posted   DATE
);

CREATE TABLE erp_gl_entry (
  entry_id   NUMBER PRIMARY KEY,
  batch_id   NUMBER,
  account_id NUMBER NOT NULL,
  debit      NUMBER DEFAULT 0,
  credit     NUMBER DEFAULT 0,
  CONSTRAINT fk_ge_batch FOREIGN KEY (batch_id)   REFERENCES erp_gl_batch(batch_id),
  CONSTRAINT fk_ge_acct  FOREIGN KEY (account_id) REFERENCES erp_gl_account(account_id)
);

CREATE TABLE erp_price_list (
  price_list_id NUMBER PRIMARY KEY,
  list_name     VARCHAR2(60),
  active_flag   VARCHAR2(1) DEFAULT 'Y'
);

CREATE TABLE erp_product_price (
  product_price_id NUMBER PRIMARY KEY,
  price_list_id    NUMBER NOT NULL,
  product_id       NUMBER NOT NULL,
  unit_price       NUMBER DEFAULT 0,
  CONSTRAINT fk_pp_pl   FOREIGN KEY (price_list_id) REFERENCES erp_price_list(price_list_id),
  CONSTRAINT fk_pp_prod FOREIGN KEY (product_id)    REFERENCES erp_product(product_id)
);

CREATE TABLE erp_audit_log (
  audit_id    NUMBER PRIMARY KEY,
  event_type  VARCHAR2(40),
  object_name VARCHAR2(60),
  message     VARCHAR2(400),
  logged_ts   DATE DEFAULT SYSDATE
);

CREATE GLOBAL TEMPORARY TABLE erp_gtt_stage (
  product_id NUMBER,
  qty        NUMBER
) ON COMMIT PRESERVE ROWS;

-- ---------------------------------------------------------------- indexes
CREATE INDEX ix_sol_order ON erp_sales_order_line(order_id);
CREATE INDEX ix_sol_prod ON erp_sales_order_line(product_id);
CREATE INDEX ix_so_cust ON erp_sales_order(customer_id);
CREATE INDEX ix_inv_prod ON erp_inventory(product_id);
CREATE INDEX ix_dmd_prod ON erp_demand(product_id);
CREATE INDEX ix_bc_bom ON erp_bom_component(bom_id);
CREATE INDEX ix_ge_acct ON erp_gl_entry(account_id);
CREATE INDEX ix_il_inv ON erp_invoice_line(invoice_id);
CREATE INDEX ix_mi_wo ON erp_material_issue(wo_id);
CREATE INDEX ix_pol_po ON erp_purchase_order_line(po_id);

-- ---------------------------------------------------------------- seed data
INSERT INTO erp_config(config_key,config_value) VALUES ('MRP_HORIZON_DAYS','30');
INSERT INTO erp_config(config_key,config_value) VALUES ('SAFETY_STOCK_PCT','15');
INSERT INTO erp_config(config_key,config_value) VALUES ('BASE_CURRENCY','USD');
INSERT INTO erp_config(config_key,config_value) VALUES ('GL_BATCH_LIMIT','1000');

INSERT INTO erp_currency(currency_id,code,name) VALUES (1,'USD','US Dollar');
INSERT INTO erp_currency(currency_id,code,name) VALUES (2,'EUR','Euro');
INSERT INTO erp_currency(currency_id,code,name) VALUES (3,'GBP','Pound');
INSERT INTO erp_currency(currency_id,code,name) VALUES (4,'JPY','Yen');

INSERT INTO erp_exchange_rate(rate_id,currency_id,rate,rate_date) VALUES (1,1,1.0,SYSDATE-1);
INSERT INTO erp_exchange_rate(rate_id,currency_id,rate,rate_date) VALUES (2,1,1.0,SYSDATE);
INSERT INTO erp_exchange_rate(rate_id,currency_id,rate,rate_date) VALUES (3,2,1.08,SYSDATE-1);
INSERT INTO erp_exchange_rate(rate_id,currency_id,rate,rate_date) VALUES (4,2,1.08,SYSDATE);
INSERT INTO erp_exchange_rate(rate_id,currency_id,rate,rate_date) VALUES (5,3,1.27,SYSDATE-1);
INSERT INTO erp_exchange_rate(rate_id,currency_id,rate,rate_date) VALUES (6,3,1.27,SYSDATE);
INSERT INTO erp_exchange_rate(rate_id,currency_id,rate,rate_date) VALUES (7,4,0.0068,SYSDATE-1);
INSERT INTO erp_exchange_rate(rate_id,currency_id,rate,rate_date) VALUES (8,4,0.0068,SYSDATE);

INSERT INTO erp_customer(customer_id,customer_name,currency_id) VALUES (1,'Acme Corp',1);
INSERT INTO erp_customer(customer_id,customer_name,currency_id) VALUES (2,'Globex',1);
INSERT INTO erp_customer(customer_id,customer_name,currency_id) VALUES (3,'Initech',2);
INSERT INTO erp_customer(customer_id,customer_name,currency_id) VALUES (4,'Umbrella',3);
INSERT INTO erp_customer(customer_id,customer_name,currency_id) VALUES (5,'Wayne Ent',4);

INSERT INTO erp_product(product_id,sku,std_cost,list_price,make_buy) VALUES (1,'ASSY-100',120,199,'M');
INSERT INTO erp_product(product_id,sku,std_cost,list_price,make_buy) VALUES (2,'ASSY-200',210,349,'M');
INSERT INTO erp_product(product_id,sku,std_cost,list_price,make_buy) VALUES (3,'SUB-300',60,99,'M');
INSERT INTO erp_product(product_id,sku,std_cost,list_price,make_buy) VALUES (4,'SUB-400',45,79,'M');
INSERT INTO erp_product(product_id,sku,std_cost,list_price,make_buy) VALUES (5,'PART-500',12,25,'B');
INSERT INTO erp_product(product_id,sku,std_cost,list_price,make_buy) VALUES (6,'PART-600',8,18,'B');
INSERT INTO erp_product(product_id,sku,std_cost,list_price,make_buy) VALUES (7,'PART-700',3,9,'B');
INSERT INTO erp_product(product_id,sku,std_cost,list_price,make_buy) VALUES (8,'PART-800',22,40,'B');
INSERT INTO erp_product(product_id,sku,std_cost,list_price,make_buy) VALUES (9,'RAW-900',1.5,4,'B');
INSERT INTO erp_product(product_id,sku,std_cost,list_price,make_buy) VALUES (10,'RAW-950',0.75,2,'B');

INSERT INTO erp_warehouse(warehouse_id,wh_code,name) VALUES (1,'WH-A','Main');
INSERT INTO erp_warehouse(warehouse_id,wh_code,name) VALUES (2,'WH-B','West');
INSERT INTO erp_warehouse(warehouse_id,wh_code,name) VALUES (3,'WH-C','East');

INSERT INTO erp_inventory(inv_id,product_id,warehouse_id,qty_on_hand,qty_reserved) VALUES (1,1,2,15,2);
INSERT INTO erp_inventory(inv_id,product_id,warehouse_id,qty_on_hand,qty_reserved) VALUES (2,2,3,30,4);
INSERT INTO erp_inventory(inv_id,product_id,warehouse_id,qty_on_hand,qty_reserved) VALUES (3,3,1,45,6);
INSERT INTO erp_inventory(inv_id,product_id,warehouse_id,qty_on_hand,qty_reserved) VALUES (4,4,2,60,8);
INSERT INTO erp_inventory(inv_id,product_id,warehouse_id,qty_on_hand,qty_reserved) VALUES (5,5,3,75,10);
INSERT INTO erp_inventory(inv_id,product_id,warehouse_id,qty_on_hand,qty_reserved) VALUES (6,6,1,90,12);
INSERT INTO erp_inventory(inv_id,product_id,warehouse_id,qty_on_hand,qty_reserved) VALUES (7,7,2,105,14);
INSERT INTO erp_inventory(inv_id,product_id,warehouse_id,qty_on_hand,qty_reserved) VALUES (8,8,3,120,16);
INSERT INTO erp_inventory(inv_id,product_id,warehouse_id,qty_on_hand,qty_reserved) VALUES (9,9,1,135,18);
INSERT INTO erp_inventory(inv_id,product_id,warehouse_id,qty_on_hand,qty_reserved) VALUES (10,10,2,150,20);

INSERT INTO erp_sales_order(order_id,customer_id,status) VALUES (100,1,'NEW');
INSERT INTO erp_sales_order(order_id,customer_id,status) VALUES (101,2,'NEW');
INSERT INTO erp_sales_order(order_id,customer_id,status) VALUES (102,3,'NEW');
INSERT INTO erp_sales_order(order_id,customer_id,status) VALUES (103,4,'NEW');
INSERT INTO erp_sales_order(order_id,customer_id,status) VALUES (104,5,'NEW');

INSERT INTO erp_sales_order_line(line_id,order_id,product_id,qty,unit_price,line_amount) VALUES (1,100,1,4,199,796);
INSERT INTO erp_sales_order_line(line_id,order_id,product_id,qty,unit_price,line_amount) VALUES (2,100,2,5,349,1745);
INSERT INTO erp_sales_order_line(line_id,order_id,product_id,qty,unit_price,line_amount) VALUES (3,100,3,6,99,594);
INSERT INTO erp_sales_order_line(line_id,order_id,product_id,qty,unit_price,line_amount) VALUES (4,101,1,5,199,995);
INSERT INTO erp_sales_order_line(line_id,order_id,product_id,qty,unit_price,line_amount) VALUES (5,101,2,6,349,2094);
INSERT INTO erp_sales_order_line(line_id,order_id,product_id,qty,unit_price,line_amount) VALUES (6,101,3,7,99,693);
INSERT INTO erp_sales_order_line(line_id,order_id,product_id,qty,unit_price,line_amount) VALUES (7,102,1,6,199,1194);
INSERT INTO erp_sales_order_line(line_id,order_id,product_id,qty,unit_price,line_amount) VALUES (8,102,2,7,349,2443);
INSERT INTO erp_sales_order_line(line_id,order_id,product_id,qty,unit_price,line_amount) VALUES (9,102,3,1,99,99);
INSERT INTO erp_sales_order_line(line_id,order_id,product_id,qty,unit_price,line_amount) VALUES (10,103,1,7,199,1393);
INSERT INTO erp_sales_order_line(line_id,order_id,product_id,qty,unit_price,line_amount) VALUES (11,103,2,1,349,349);
INSERT INTO erp_sales_order_line(line_id,order_id,product_id,qty,unit_price,line_amount) VALUES (12,103,3,2,99,198);
INSERT INTO erp_sales_order_line(line_id,order_id,product_id,qty,unit_price,line_amount) VALUES (13,104,1,1,199,199);
INSERT INTO erp_sales_order_line(line_id,order_id,product_id,qty,unit_price,line_amount) VALUES (14,104,2,2,349,698);
INSERT INTO erp_sales_order_line(line_id,order_id,product_id,qty,unit_price,line_amount) VALUES (15,104,3,3,99,297);

INSERT INTO erp_supplier(supplier_id,supplier_name) VALUES (1,'Global Metals');
INSERT INTO erp_supplier(supplier_id,supplier_name) VALUES (2,'FastCircuits');
INSERT INTO erp_supplier(supplier_id,supplier_name) VALUES (3,'BoltCo');

INSERT INTO erp_purchase_order(po_id,supplier_id,status) VALUES (200,1,'OPEN');
INSERT INTO erp_purchase_order(po_id,supplier_id,status) VALUES (201,2,'OPEN');
INSERT INTO erp_purchase_order(po_id,supplier_id,status) VALUES (202,3,'OPEN');

INSERT INTO erp_purchase_order_line(pol_id,po_id,product_id,qty,unit_cost) VALUES (1,200,5,50,12);
INSERT INTO erp_purchase_order_line(pol_id,po_id,product_id,qty,unit_cost) VALUES (2,200,6,60,8);
INSERT INTO erp_purchase_order_line(pol_id,po_id,product_id,qty,unit_cost) VALUES (3,200,7,70,3);
INSERT INTO erp_purchase_order_line(pol_id,po_id,product_id,qty,unit_cost) VALUES (4,201,5,50,12);
INSERT INTO erp_purchase_order_line(pol_id,po_id,product_id,qty,unit_cost) VALUES (5,201,6,60,8);
INSERT INTO erp_purchase_order_line(pol_id,po_id,product_id,qty,unit_cost) VALUES (6,201,7,70,3);
INSERT INTO erp_purchase_order_line(pol_id,po_id,product_id,qty,unit_cost) VALUES (7,202,5,50,12);
INSERT INTO erp_purchase_order_line(pol_id,po_id,product_id,qty,unit_cost) VALUES (8,202,6,60,8);
INSERT INTO erp_purchase_order_line(pol_id,po_id,product_id,qty,unit_cost) VALUES (9,202,7,70,3);

INSERT INTO erp_bom_header(bom_id,product_id,active_flag) VALUES (1,1,'Y');
INSERT INTO erp_bom_header(bom_id,product_id,active_flag) VALUES (2,2,'Y');
INSERT INTO erp_bom_header(bom_id,product_id,active_flag) VALUES (3,3,'Y');
INSERT INTO erp_bom_header(bom_id,product_id,active_flag) VALUES (4,4,'Y');

INSERT INTO erp_bom_component(bom_comp_id,bom_id,component_id,qty_per) VALUES (1,1,3,2);
INSERT INTO erp_bom_component(bom_comp_id,bom_id,component_id,qty_per) VALUES (2,1,5,4);
INSERT INTO erp_bom_component(bom_comp_id,bom_id,component_id,qty_per) VALUES (3,2,4,1);
INSERT INTO erp_bom_component(bom_comp_id,bom_id,component_id,qty_per) VALUES (4,2,6,3);
INSERT INTO erp_bom_component(bom_comp_id,bom_id,component_id,qty_per) VALUES (5,3,5,2);
INSERT INTO erp_bom_component(bom_comp_id,bom_id,component_id,qty_per) VALUES (6,3,9,5);
INSERT INTO erp_bom_component(bom_comp_id,bom_id,component_id,qty_per) VALUES (7,4,7,1);
INSERT INTO erp_bom_component(bom_comp_id,bom_id,component_id,qty_per) VALUES (8,4,10,6);

INSERT INTO erp_work_center(work_center_id,wc_code,capacity_hrs) VALUES (1,'CUT',8);
INSERT INTO erp_work_center(work_center_id,wc_code,capacity_hrs) VALUES (2,'WELD',8);
INSERT INTO erp_work_center(work_center_id,wc_code,capacity_hrs) VALUES (3,'ASSY',16);

INSERT INTO erp_routing(routing_id,product_id) VALUES (1,1);
INSERT INTO erp_routing(routing_id,product_id) VALUES (2,2);
INSERT INTO erp_routing(routing_id,product_id) VALUES (3,3);

INSERT INTO erp_routing_operation(op_id,routing_id,work_center_id,seq_no,run_time_min) VALUES (1,1,1,10,15);
INSERT INTO erp_routing_operation(op_id,routing_id,work_center_id,seq_no,run_time_min) VALUES (2,1,2,20,30);
INSERT INTO erp_routing_operation(op_id,routing_id,work_center_id,seq_no,run_time_min) VALUES (3,1,3,30,45);
INSERT INTO erp_routing_operation(op_id,routing_id,work_center_id,seq_no,run_time_min) VALUES (4,2,1,40,15);
INSERT INTO erp_routing_operation(op_id,routing_id,work_center_id,seq_no,run_time_min) VALUES (5,2,2,50,30);
INSERT INTO erp_routing_operation(op_id,routing_id,work_center_id,seq_no,run_time_min) VALUES (6,2,3,60,45);
INSERT INTO erp_routing_operation(op_id,routing_id,work_center_id,seq_no,run_time_min) VALUES (7,3,1,70,15);
INSERT INTO erp_routing_operation(op_id,routing_id,work_center_id,seq_no,run_time_min) VALUES (8,3,2,80,30);
INSERT INTO erp_routing_operation(op_id,routing_id,work_center_id,seq_no,run_time_min) VALUES (9,3,3,90,45);

INSERT INTO erp_work_order(wo_id,product_id,qty_planned,status) VALUES (300,1,50,'RELEASED');
INSERT INTO erp_work_order(wo_id,product_id,qty_planned,status) VALUES (301,2,30,'RELEASED');
INSERT INTO erp_work_order(wo_id,product_id,qty_planned,status) VALUES (302,3,80,'RELEASED');

INSERT INTO erp_material_issue(issue_id,wo_id,component_id,qty_issued) VALUES (1,300,3,9);
INSERT INTO erp_material_issue(issue_id,wo_id,component_id,qty_issued) VALUES (2,300,5,15);
INSERT INTO erp_material_issue(issue_id,wo_id,component_id,qty_issued) VALUES (3,301,4,12);
INSERT INTO erp_material_issue(issue_id,wo_id,component_id,qty_issued) VALUES (4,302,5,15);

INSERT INTO erp_demand(demand_id,product_id,source_type,source_id,qty,need_date) VALUES (1,1,'SALES_ORDER',101,20,SYSDATE+5);
INSERT INTO erp_demand(demand_id,product_id,source_type,source_id,qty,need_date) VALUES (2,2,'SALES_ORDER',102,40,SYSDATE+10);
INSERT INTO erp_demand(demand_id,product_id,source_type,source_id,qty,need_date) VALUES (3,3,'SALES_ORDER',103,60,SYSDATE+15);
INSERT INTO erp_demand(demand_id,product_id,source_type,source_id,qty,need_date) VALUES (4,4,'SALES_ORDER',104,80,SYSDATE+20);

INSERT INTO erp_forecast(forecast_id,product_id,qty,forecast_date) VALUES (1,1,40,SYSDATE);
INSERT INTO erp_forecast(forecast_id,product_id,qty,forecast_date) VALUES (2,2,80,SYSDATE);
INSERT INTO erp_forecast(forecast_id,product_id,qty,forecast_date) VALUES (3,3,120,SYSDATE);

INSERT INTO erp_mrp_run(mrp_run_id,status,planned_count) VALUES (99000,'DONE',3);
INSERT INTO erp_mrp_plan(plan_id,mrp_run_id,product_id,action,qty) VALUES (1,99000,1,'MAKE',10);
INSERT INTO erp_mrp_plan(plan_id,mrp_run_id,product_id,action,qty) VALUES (2,99000,2,'MAKE',20);
INSERT INTO erp_mrp_plan(plan_id,mrp_run_id,product_id,action,qty) VALUES (3,99000,5,'BUY',50);
INSERT INTO erp_mrp_plan(plan_id,mrp_run_id,product_id,action,qty) VALUES (4,99000,6,'BUY',60);

INSERT INTO erp_invoice(invoice_id,customer_id,amount,status,invoice_date) VALUES (400,1,1200,'OPEN',SYSDATE-10);
INSERT INTO erp_invoice(invoice_id,customer_id,amount,status,invoice_date) VALUES (401,2,800,'OPEN',SYSDATE-20);
INSERT INTO erp_invoice(invoice_id,customer_id,amount,status,invoice_date) VALUES (402,3,450,'PAID',SYSDATE-30);

INSERT INTO erp_invoice_line(invoice_line_id,invoice_id,product_id,amount) VALUES (1,400,1,150);
INSERT INTO erp_invoice_line(invoice_line_id,invoice_id,product_id,amount) VALUES (2,400,2,300);
INSERT INTO erp_invoice_line(invoice_line_id,invoice_id,product_id,amount) VALUES (3,401,1,150);
INSERT INTO erp_invoice_line(invoice_line_id,invoice_id,product_id,amount) VALUES (4,401,2,300);
INSERT INTO erp_invoice_line(invoice_line_id,invoice_id,product_id,amount) VALUES (5,402,1,150);
INSERT INTO erp_invoice_line(invoice_line_id,invoice_id,product_id,amount) VALUES (6,402,2,300);

INSERT INTO erp_shipment(shipment_id,order_id) VALUES (500,100);
INSERT INTO erp_shipment(shipment_id,order_id) VALUES (501,101);
INSERT INTO erp_shipment_line(ship_line_id,shipment_id,product_id,qty_shipped) VALUES (1,500,1,3);
INSERT INTO erp_shipment_line(ship_line_id,shipment_id,product_id,qty_shipped) VALUES (2,500,2,6);
INSERT INTO erp_shipment_line(ship_line_id,shipment_id,product_id,qty_shipped) VALUES (3,501,1,3);
INSERT INTO erp_shipment_line(ship_line_id,shipment_id,product_id,qty_shipped) VALUES (4,501,2,6);

INSERT INTO erp_gl_account(account_id,account_type,name) VALUES (1,'ASSET','Inventory');
INSERT INTO erp_gl_account(account_id,account_type,name) VALUES (2,'REVENUE','Sales');
INSERT INTO erp_gl_account(account_id,account_type,name) VALUES (3,'EXPENSE','COGS');
INSERT INTO erp_gl_account(account_id,account_type,name) VALUES (4,'LIABILITY','AP');

INSERT INTO erp_gl_batch(batch_id,status) VALUES (600,'OPEN');
INSERT INTO erp_gl_entry(entry_id,batch_id,account_id,debit,credit) VALUES (1,600,1,1000,0);
INSERT INTO erp_gl_entry(entry_id,batch_id,account_id,debit,credit) VALUES (2,600,2,0,1000);
INSERT INTO erp_gl_entry(entry_id,batch_id,account_id,debit,credit) VALUES (3,600,3,500,0);
INSERT INTO erp_gl_entry(entry_id,batch_id,account_id,debit,credit) VALUES (4,600,4,0,500);

INSERT INTO erp_price_list(price_list_id,list_name,active_flag) VALUES (1,'STANDARD','Y');
INSERT INTO erp_price_list(price_list_id,list_name,active_flag) VALUES (2,'PROMO','Y');
INSERT INTO erp_product_price(product_price_id,price_list_id,product_id,unit_price) VALUES (1,1,1,199);
INSERT INTO erp_product_price(product_price_id,price_list_id,product_id,unit_price) VALUES (2,1,2,349);
INSERT INTO erp_product_price(product_price_id,price_list_id,product_id,unit_price) VALUES (3,1,3,99);
INSERT INTO erp_product_price(product_price_id,price_list_id,product_id,unit_price) VALUES (4,2,1,199);
INSERT INTO erp_product_price(product_price_id,price_list_id,product_id,unit_price) VALUES (5,2,2,349);
INSERT INTO erp_product_price(product_price_id,price_list_id,product_id,unit_price) VALUES (6,2,3,99);
COMMIT;

-- ---------------------------------------------------------------- views
CREATE OR REPLACE VIEW erp_v_order_summary AS
SELECT so.order_id, so.customer_id, c.customer_name,
       NVL(SUM(sol.line_amount),0) AS order_total,
       COUNT(sol.line_id)          AS line_count
FROM erp_sales_order so
JOIN erp_customer c        ON c.customer_id = so.customer_id
LEFT JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
GROUP BY so.order_id, so.customer_id, c.customer_name;
/

CREATE OR REPLACE VIEW erp_v_inventory_value AS
SELECT i.product_id, p.sku, w.wh_code,
       (i.qty_on_hand - NVL(i.qty_reserved,0)) * NVL(p.std_cost,0) AS ext_value
FROM erp_inventory i
JOIN erp_product p   ON p.product_id = i.product_id
JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id;
/

CREATE OR REPLACE VIEW erp_v_gl_trial_balance AS
SELECT a.account_type, SUM(e.debit) AS total_debit, SUM(e.credit) AS total_credit,
       SUM(e.debit) - SUM(e.credit) AS net
FROM erp_gl_entry e
JOIN erp_gl_account a ON a.account_id = e.account_id
GROUP BY a.account_type;
/

-- ---------------------------------------------------------------- materialized views
BEGIN EXECUTE IMMEDIATE 'DROP MATERIALIZED VIEW erp_mv_product_sales'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
CREATE MATERIALIZED VIEW erp_mv_product_sales
BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
SELECT sol.product_id, p.sku, SUM(sol.qty) AS total_qty, SUM(sol.line_amount) AS total_amount
FROM erp_sales_order_line sol
JOIN erp_product p ON p.product_id = sol.product_id
GROUP BY sol.product_id, p.sku;
/

BEGIN EXECUTE IMMEDIATE 'DROP MATERIALIZED VIEW erp_mv_demand_by_prod'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
CREATE MATERIALIZED VIEW erp_mv_demand_by_prod
BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
SELECT d.product_id, SUM(d.qty) AS total_demand, COUNT(*) AS demand_rows
FROM erp_demand d GROUP BY d.product_id;
/

-- ---------------------------------------------------------------- functions
CREATE OR REPLACE FUNCTION erp_fn_get_config(p_key IN VARCHAR2) RETURN VARCHAR2 IS
  v_val erp_config.config_value%TYPE;
BEGIN
  SELECT config_value INTO v_val FROM erp_config WHERE config_key = p_key;
  RETURN v_val;
EXCEPTION
  WHEN NO_DATA_FOUND THEN RETURN NULL;
  WHEN OTHERS THEN RETURN NULL;
END erp_fn_get_config;
/

CREATE OR REPLACE FUNCTION erp_fn_fx_rate(p_currency_id IN NUMBER) RETURN NUMBER IS
  v_rate NUMBER := 1;
BEGIN
  SELECT rate INTO v_rate FROM (
    SELECT rate FROM erp_exchange_rate WHERE currency_id = p_currency_id ORDER BY rate_date DESC
  ) WHERE ROWNUM = 1;
  RETURN v_rate;
EXCEPTION
  WHEN NO_DATA_FOUND THEN RETURN 1;
END erp_fn_fx_rate;
/

CREATE OR REPLACE FUNCTION erp_fn_order_total(p_order_id IN NUMBER) RETURN NUMBER IS
  v_total NUMBER := 0;
BEGIN
  SELECT NVL(SUM(line_amount),0) INTO v_total FROM erp_sales_order_line WHERE order_id = p_order_id;
  RETURN v_total;
END erp_fn_order_total;
/

CREATE OR REPLACE FUNCTION erp_fn_onhand_qty(p_product_id IN NUMBER) RETURN NUMBER IS
  v_qty NUMBER := 0;
BEGIN
  SELECT NVL(SUM(qty_on_hand - NVL(qty_reserved,0)),0) INTO v_qty
    FROM erp_inventory WHERE product_id = p_product_id;
  RETURN v_qty;
EXCEPTION
  WHEN OTHERS THEN RETURN 0;
END erp_fn_onhand_qty;
/

CREATE OR REPLACE FUNCTION erp_fn_safety_stock(p_product_id IN NUMBER) RETURN NUMBER IS
  v_pct NUMBER := 0; v_base NUMBER := 0;
BEGIN
  v_pct := TO_NUMBER(NVL(erp_fn_get_config('SAFETY_STOCK_PCT'),'0'));
  SELECT NVL(AVG(qty),0) INTO v_base FROM erp_demand WHERE product_id = p_product_id;
  RETURN ROUND(v_base * v_pct / 100, 2);
EXCEPTION
  WHEN OTHERS THEN RETURN 0;
END erp_fn_safety_stock;
/

CREATE OR REPLACE FUNCTION erp_fn_gross_requirement(p_product_id IN NUMBER) RETURN NUMBER IS
  v_qty NUMBER := 0;
BEGIN
  SELECT NVL(SUM(qty),0) INTO v_qty FROM erp_demand
   WHERE product_id = p_product_id AND need_date <= SYSDATE + 90;
  RETURN v_qty;
EXCEPTION
  WHEN OTHERS THEN RETURN 0;
END erp_fn_gross_requirement;
/

CREATE OR REPLACE FUNCTION erp_fn_net_requirement(p_product_id IN NUMBER) RETURN NUMBER IS
  v_gross NUMBER := 0; v_avail NUMBER := 0; v_safe NUMBER := 0; v_net NUMBER := 0;
BEGIN
  v_gross := erp_fn_gross_requirement(p_product_id);
  v_avail := erp_fn_onhand_qty(p_product_id);
  v_safe  := erp_fn_safety_stock(p_product_id);
  v_net   := v_gross - v_avail + v_safe;
  IF v_net < 0 THEN v_net := 0; END IF;
  RETURN v_net;
END erp_fn_net_requirement;
/

CREATE OR REPLACE FUNCTION erp_fn_bom_component_count(p_product_id IN NUMBER) RETURN NUMBER IS
  v_cnt NUMBER := 0;
BEGIN
  SELECT COUNT(*) INTO v_cnt
  FROM erp_bom_header bh
  JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
  WHERE bh.product_id = p_product_id;
  RETURN v_cnt;
EXCEPTION
  WHEN OTHERS THEN RETURN 0;
END erp_fn_bom_component_count;
/

CREATE OR REPLACE FUNCTION erp_fn_supplier_spend(p_supplier_id IN NUMBER) RETURN NUMBER IS
  v_spend NUMBER := 0;
BEGIN
  SELECT NVL(SUM(pol.qty * pol.unit_cost),0) INTO v_spend
  FROM erp_purchase_order po
  JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
  WHERE po.supplier_id = p_supplier_id AND po.status <> 'CANCELLED';
  RETURN v_spend;
EXCEPTION
  WHEN OTHERS THEN RETURN 0;
END erp_fn_supplier_spend;
/

CREATE OR REPLACE FUNCTION erp_fn_wo_completion_pct(p_wo_id IN NUMBER) RETURN NUMBER IS
  v_planned NUMBER := 0; v_done NUMBER := 0;
BEGIN
  SELECT NVL(qty_planned,0), NVL(qty_done,0) INTO v_planned, v_done
  FROM erp_work_order WHERE wo_id = p_wo_id;
  IF v_planned = 0 THEN RETURN 0; END IF;
  RETURN ROUND(v_done / v_planned * 100, 2);
EXCEPTION
  WHEN NO_DATA_FOUND THEN RETURN 0;
  WHEN OTHERS THEN RETURN 0;
END erp_fn_wo_completion_pct;
/

CREATE OR REPLACE FUNCTION erp_fn_gl_balance(p_account_type IN VARCHAR2) RETURN NUMBER IS
  v_bal NUMBER := 0;
BEGIN
  SELECT NVL(SUM(e.debit) - SUM(e.credit),0) INTO v_bal
  FROM erp_gl_entry e
  JOIN erp_gl_account a ON a.account_id = e.account_id
  WHERE a.account_type = p_account_type;
  RETURN v_bal;
EXCEPTION
  WHEN OTHERS THEN RETURN 0;
END erp_fn_gl_balance;
/

CREATE OR REPLACE FUNCTION erp_fn_open_invoice_total(p_customer_id IN NUMBER) RETURN NUMBER IS
  v_tot NUMBER := 0;
BEGIN
  SELECT NVL(SUM(amount),0) INTO v_tot
  FROM erp_invoice WHERE customer_id = p_customer_id AND status = 'OPEN';
  RETURN v_tot;
EXCEPTION
  WHEN OTHERS THEN RETURN 0;
END erp_fn_open_invoice_total;
/

CREATE OR REPLACE FUNCTION erp_fn_demand_qty(p_product_id IN NUMBER) RETURN NUMBER IS
  v_qty NUMBER := 0;
BEGIN
  SELECT NVL(SUM(qty),0) INTO v_qty FROM erp_demand WHERE product_id = p_product_id;
  RETURN v_qty;
EXCEPTION
  WHEN OTHERS THEN RETURN 0;
END erp_fn_demand_qty;
/

-- ---------------------------------------------------------------- audit + edge
CREATE OR REPLACE PROCEDURE erp_usp_write_audit_trail(
  p_event   IN VARCHAR2,
  p_object  IN VARCHAR2,
  p_message IN VARCHAR2
) IS
  PRAGMA AUTONOMOUS_TRANSACTION;
  v_id  NUMBER;
  v_cnt NUMBER := 0;
BEGIN
  v_id := erp_audit_seq.NEXTVAL;
  INSERT INTO erp_audit_log(audit_id, event_type, object_name, message)
  VALUES (v_id, p_event, p_object, SUBSTR(p_message,1,400));
  BEGIN
    SELECT COUNT(*) INTO v_cnt FROM erp_audit_log;
    IF v_cnt > 500000 THEN
      DELETE FROM erp_audit_log WHERE audit_id < (v_id - 400000);
    END IF;
  EXCEPTION WHEN OTHERS THEN NULL;
  END;
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
END erp_usp_write_audit_trail;
/

CREATE OR REPLACE PROCEDURE erp_usp_edge_recalc(
  p_mrp_run_id IN NUMBER
) IS
  v_i     NUMBER := 0;
  v_grade VARCHAR2(10);
  v_total NUMBER := 0;
  v_lvls  NUMBER := 0;
BEGIN
  erp_usp_write_audit_trail('EDGE_START','ERP_EDGE','recalc run '||p_mrp_run_id);
  SAVEPOINT sp_edge_start;
  WHILE v_i < 5 LOOP
    v_i := v_i + 1;
    CASE
      WHEN v_i <= 1 THEN v_grade := 'LOW';
      WHEN v_i <= 3 THEN v_grade := 'MED';
      ELSE v_grade := 'HIGH';
    END CASE;
    INSERT INTO erp_audit_log(audit_id, event_type, object_name, message)
    VALUES (erp_audit_seq.NEXTVAL,'EDGE_ITER','ERP_EDGE','iter '||v_i||' grade '||v_grade);
  END LOOP;
  ROLLBACK TO SAVEPOINT sp_edge_start;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_total, v_lvls FROM bom_tree;
  EXCEPTION WHEN OTHERS THEN
    v_total := 0; v_lvls := 0;
  END;
  erp_usp_write_audit_trail('EDGE_END','ERP_EDGE',
    'recalc done rows='||v_total||' maxlvl='||v_lvls);
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    erp_usp_write_audit_trail('EDGE_ERR','ERP_EDGE',SQLERRM);
END erp_usp_edge_recalc;
/

-- ---------------------------------------------------------------- direct self-recursion
CREATE OR REPLACE PROCEDURE erp_usp_explode_bom(
  p_product_id IN NUMBER,
  p_depth      IN NUMBER DEFAULT 0,
  p_max_depth  IN NUMBER DEFAULT 10
) IS
  -- Directly self-recursive: walks the BOM tree, calling itself for every
  -- child component. Terminates on the depth guard OR when no children remain.
  v_children NUMBER := 0;
BEGIN
  IF p_depth > p_max_depth THEN
    erp_usp_write_audit_trail('BOM_GUARD','ERP_BOM_COMPONENT',
      'max depth '||p_max_depth||' hit at product '||p_product_id);
    RETURN;
  END IF;

  FOR r IN (
    SELECT bc.component_id, bc.qty_per
    FROM erp_bom_header bh
    JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
    WHERE bh.product_id = p_product_id AND bh.active_flag = 'Y'
  ) LOOP
    v_children := v_children + 1;
    -- RECURSIVE CALL to self, one level deeper
    erp_usp_explode_bom(r.component_id, p_depth + 1, p_max_depth);
  END LOOP;

  erp_usp_write_audit_trail('BOM_EXPLODE','ERP_BOM_COMPONENT',
    'product='||p_product_id||' depth='||p_depth||' children='||v_children);
EXCEPTION
  WHEN OTHERS THEN
    erp_usp_write_audit_trail('BOM_EXPLODE_ERR','ERP_BOM_COMPONENT',
      'product='||p_product_id||' '||SQLERRM);
END erp_usp_explode_bom;
/

-- ---------------------------------------------------------------- package (spec + body)
CREATE OR REPLACE PACKAGE erp_pkg_planner AS
  g_last_context  VARCHAR2(60);
  g_last_run_id   NUMBER := 0;
  g_call_count    NUMBER := 0;

  TYPE t_prod_rec IS RECORD (product_id NUMBER, sku VARCHAR2(40), qty NUMBER);
  TYPE t_prod_tab IS TABLE OF t_prod_rec;

  TYPE t_num_varray IS VARRAY(100) OF NUMBER;
  TYPE t_str_map    IS TABLE OF VARCHAR2(200) INDEX BY VARCHAR2(60);

  PROCEDURE set_context(p_ctx IN VARCHAR2, p_run_id IN NUMBER);

  FUNCTION compute_total(p_seed IN NUMBER) RETURN NUMBER;
  FUNCTION compute_total(p_seed IN NUMBER, p_mode IN VARCHAR2) RETURN NUMBER;

  FUNCTION describe(p_id IN NUMBER)     RETURN VARCHAR2;
  FUNCTION describe(p_code IN VARCHAR2) RETURN VARCHAR2;

  FUNCTION explode_products(p_seed IN NUMBER) RETURN t_prod_tab PIPELINED;

  PROCEDURE open_demand_cursor(p_rc OUT SYS_REFCURSOR);

  -- MUTUAL / CIRCULAR recursion pair (depth guarded). walk_down calls walk_up,
  -- walk_up calls walk_down; both stop when p_depth reaches p_limit.
  PROCEDURE walk_down(p_depth IN NUMBER, p_limit IN NUMBER);
  PROCEDURE walk_up(p_depth IN NUMBER, p_limit IN NUMBER);
END erp_pkg_planner;
/

CREATE OR REPLACE PACKAGE BODY erp_pkg_planner AS

  PROCEDURE set_context(p_ctx IN VARCHAR2, p_run_id IN NUMBER) IS
  BEGIN
    g_last_context := p_ctx;
    g_last_run_id  := p_run_id;
    g_call_count   := g_call_count + 1;
  END set_context;

  FUNCTION compute_total(p_seed IN NUMBER) RETURN NUMBER IS
    v_tot NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(line_amount),0) INTO v_tot FROM erp_sales_order_line;
    RETURN v_tot + NVL(p_seed,0);
  EXCEPTION WHEN OTHERS THEN RETURN NVL(p_seed,0);
  END compute_total;

  FUNCTION compute_total(p_seed IN NUMBER, p_mode IN VARCHAR2) RETURN NUMBER IS
    v_tot NUMBER := 0; v_rate NUMBER := 1;
  BEGIN
    IF p_mode = 'BASE' THEN
      FOR r IN (SELECT c.currency_id, SUM(sol.line_amount) amt
                FROM erp_customer c
                JOIN erp_sales_order so ON so.customer_id = c.customer_id
                JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
                GROUP BY c.currency_id) LOOP
        v_rate := erp_fn_fx_rate(r.currency_id);
        v_tot  := v_tot + NVL(r.amt,0) * NVL(v_rate,1);
      END LOOP;
    ELSE
      v_tot := compute_total(p_seed);
    END IF;
    RETURN v_tot + NVL(p_seed,0);
  END compute_total;

  FUNCTION describe(p_id IN NUMBER) RETURN VARCHAR2 IS
    v_sku erp_product.sku%TYPE;
  BEGIN
    SELECT sku INTO v_sku FROM erp_product WHERE product_id = p_id;
    RETURN 'PRODUCT#'||p_id||'='||v_sku;
  EXCEPTION WHEN NO_DATA_FOUND THEN RETURN 'PRODUCT#'||p_id||'=?';
  END describe;

  FUNCTION describe(p_code IN VARCHAR2) RETURN VARCHAR2 IS
    v_name erp_currency.name%TYPE;
  BEGIN
    SELECT name INTO v_name FROM erp_currency WHERE code = p_code;
    RETURN 'CURRENCY '||p_code||'='||v_name;
  EXCEPTION WHEN NO_DATA_FOUND THEN RETURN 'CURRENCY '||p_code||'=?';
  END describe;

  FUNCTION explode_products(p_seed IN NUMBER) RETURN t_prod_tab PIPELINED IS
    v_rec t_prod_rec;
  BEGIN
    FOR r IN (SELECT p.product_id, p.sku, NVL(SUM(d.qty),0) AS qty
              FROM erp_product p
              LEFT JOIN erp_demand d ON d.product_id = p.product_id
              GROUP BY p.product_id, p.sku
              ORDER BY p.product_id) LOOP
      v_rec.product_id := r.product_id;
      v_rec.sku        := r.sku;
      v_rec.qty        := r.qty + NVL(p_seed,0);
      PIPE ROW(v_rec);
    END LOOP;
    RETURN;
  END explode_products;

  PROCEDURE open_demand_cursor(p_rc OUT SYS_REFCURSOR) IS
  BEGIN
    OPEN p_rc FOR
      SELECT d.product_id, p.sku, d.qty, d.need_date
      FROM erp_demand d
      JOIN erp_product p ON p.product_id = d.product_id
      ORDER BY d.need_date;
  END open_demand_cursor;

  -- ------- MUTUAL / CIRCULAR recursion (A -> B -> A ...) ----------
  PROCEDURE walk_down(p_depth IN NUMBER, p_limit IN NUMBER) IS
  BEGIN
    IF p_depth >= p_limit THEN
      erp_usp_write_audit_trail('WALK_DOWN','ERP_PKG_PLANNER','limit at depth '||p_depth);
      RETURN;
    END IF;
    erp_usp_write_audit_trail('WALK_DOWN','ERP_PKG_PLANNER','depth '||p_depth);
    -- call the partner routine (mutual recursion)
    walk_up(p_depth + 1, p_limit);
  END walk_down;

  PROCEDURE walk_up(p_depth IN NUMBER, p_limit IN NUMBER) IS
  BEGIN
    IF p_depth >= p_limit THEN
      erp_usp_write_audit_trail('WALK_UP','ERP_PKG_PLANNER','limit at depth '||p_depth);
      RETURN;
    END IF;
    erp_usp_write_audit_trail('WALK_UP','ERP_PKG_PLANNER','depth '||p_depth);
    -- call back into walk_down, closing the A <-> B cycle
    walk_down(p_depth + 1, p_limit);
  END walk_up;

END erp_pkg_planner;
/

-- ---------------------------------------------------------------- chain (deepest first)
CREATE OR REPLACE PROCEDURE erp_usp_finalize_gl_batch(p_batch_id IN NUMBER DEFAULT 600) IS
  v_net NUMBER := 0;
BEGIN
  erp_usp_write_audit_trail('GL_FINALIZE','ERP_GL_BATCH','enter erp_usp_finalize_gl_batch');
  SAVEPOINT sp_finalize_gl_batch_begin;
  -- [1200] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1200 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1200 '||SQLERRM);
  END;
  -- [1201] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1201 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1201 '||SQLERRM);
  END;
  -- [1202] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1202 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1202 '||SQLERRM);
  END;
  -- [1203] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1203);
    v_tot := erp_pkg_planner.compute_total(1203);
    v_tot := v_tot + erp_pkg_planner.compute_total(1203, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1203))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1203 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1203 '||SQLERRM);
  END;
  -- [1204] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1204 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1204 '||SQLERRM);
  END;
  -- [1205] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1205 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1205;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1205 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1205 '||SQLERRM);
  END;
  -- [1206] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1206 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1206 '||SQLERRM);
  END;
  -- [1207] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1207;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1207 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1207 '||SQLERRM);
  END;
  -- [1208] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1208 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1208 '||SQLERRM);
  END;
  -- [1209] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1209 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1209 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1209 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1209 '||SQLERRM);
  END;
  -- [1210] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1210 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1210 '||SQLERRM);
  END;
  -- [1211] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1211 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1211 '||SQLERRM);
  END;
  -- [1212] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1212 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1212 '||SQLERRM);
  END;
  -- [1213] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1213 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1213 '||SQLERRM);
  END;
  -- [1214] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1214 IS REF CURSOR;
    rc      t_rc_1214;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1214 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1214 '||SQLERRM);
  END;
  -- [1215] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1215')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1215', 1215, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1215 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1215 '||SQLERRM);
  END;
  -- [1216] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1216 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1216;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1216 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1216 '||SQLERRM);
  END;
  -- [1217] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1217 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1217 '||SQLERRM);
  END;
  -- [1218] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1218 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1218 '||SQLERRM);
  END;
  -- [1219] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1219;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1219;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1219 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1219;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1219 '||SQLERRM);
  END;
  -- [1220] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1220 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1220 '||SQLERRM);
  END;
  -- [1221] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1221 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1221 '||SQLERRM);
  END;
  -- [1222] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1222 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1222 '||SQLERRM);
  END;
  -- [1223] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1223 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1223 '||SQLERRM);
  END;
  -- [1224] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1224 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1224 '||SQLERRM);
  END;
  -- [1225] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1225 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1225 '||SQLERRM);
  END;
  -- [1226] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1226 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1226 '||SQLERRM);
  END;
  -- [1227] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1227 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1227 '||SQLERRM);
  END;
  -- [1228] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1228 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1228 '||SQLERRM);
  END;
  -- [1229] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1229 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1229 '||SQLERRM);
  END;
  -- [1230] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1230 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1230 '||SQLERRM);
  END;
  -- [1231] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1231);
    v_tot := erp_pkg_planner.compute_total(1231);
    v_tot := v_tot + erp_pkg_planner.compute_total(1231, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1231))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1231 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1231 '||SQLERRM);
  END;
  -- [1232] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1232 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1232 '||SQLERRM);
  END;
  -- [1233] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1233 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1233;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1233 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1233 '||SQLERRM);
  END;
  -- [1234] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1234 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1234 '||SQLERRM);
  END;
  -- [1235] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1235;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1235 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1235 '||SQLERRM);
  END;
  -- [1236] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1236 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1236 '||SQLERRM);
  END;
  -- [1237] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1237 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1237 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1237 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1237 '||SQLERRM);
  END;
  -- [1238] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1238 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1238 '||SQLERRM);
  END;
  -- [1239] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1239 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1239 '||SQLERRM);
  END;
  -- [1240] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1240 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1240 '||SQLERRM);
  END;
  -- [1241] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1241 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1241 '||SQLERRM);
  END;
  -- [1242] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1242 IS REF CURSOR;
    rc      t_rc_1242;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1242 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1242 '||SQLERRM);
  END;
  -- [1243] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1243')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1243', 1243, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1243 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1243 '||SQLERRM);
  END;
  -- [1244] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1244 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1244;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1244 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1244 '||SQLERRM);
  END;
  -- [1245] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1245 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1245 '||SQLERRM);
  END;
  -- [1246] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1246 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1246 '||SQLERRM);
  END;
  -- [1247] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1247;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1247;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1247 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1247;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1247 '||SQLERRM);
  END;
  -- [1248] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1248 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1248 '||SQLERRM);
  END;
  -- [1249] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1249 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1249 '||SQLERRM);
  END;
  -- [1250] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1250 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1250 '||SQLERRM);
  END;
  -- [1251] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1251 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1251 '||SQLERRM);
  END;
  -- [1252] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1252 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1252 '||SQLERRM);
  END;
  -- [1253] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1253 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1253 '||SQLERRM);
  END;
  -- [1254] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1254 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1254 '||SQLERRM);
  END;
  -- [1255] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1255 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1255 '||SQLERRM);
  END;
  -- [1256] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1256 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1256 '||SQLERRM);
  END;
  -- [1257] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1257 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1257 '||SQLERRM);
  END;
  -- [1258] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1258 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1258 '||SQLERRM);
  END;
  -- [1259] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1259);
    v_tot := erp_pkg_planner.compute_total(1259);
    v_tot := v_tot + erp_pkg_planner.compute_total(1259, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1259))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1259 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1259 '||SQLERRM);
  END;
  -- [1260] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1260 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1260 '||SQLERRM);
  END;
  -- [1261] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1261 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1261;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1261 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1261 '||SQLERRM);
  END;
  -- [1262] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1262 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1262 '||SQLERRM);
  END;
  -- [1263] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1263;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1263 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1263 '||SQLERRM);
  END;
  -- [1264] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1264 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1264 '||SQLERRM);
  END;
  -- [1265] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1265 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1265 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1265 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1265 '||SQLERRM);
  END;
  -- [1266] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1266 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1266 '||SQLERRM);
  END;
  -- [1267] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1267 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1267 '||SQLERRM);
  END;
  -- [1268] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1268 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1268 '||SQLERRM);
  END;
  -- [1269] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1269 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1269 '||SQLERRM);
  END;
  -- [1270] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1270 IS REF CURSOR;
    rc      t_rc_1270;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1270 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1270 '||SQLERRM);
  END;
  -- [1271] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1271')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1271', 1271, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1271 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1271 '||SQLERRM);
  END;

  erp_usp_write_audit_trail('GL_FINALIZE_DONE','ERP_GL_BATCH','gl batch finalized');
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK TO SAVEPOINT sp_finalize_gl_batch_begin;
    erp_usp_write_audit_trail('GL_FINALIZE_ERR','ERP_GL_BATCH',SQLERRM);
END erp_usp_finalize_gl_batch;
/

CREATE OR REPLACE PROCEDURE erp_usp_post_gl_entries(p_run_id IN NUMBER) IS
  v_posted NUMBER := 0;
BEGIN
  erp_usp_write_audit_trail('GL_POST','ERP_GL_ENTRY','enter erp_usp_post_gl_entries');
  SAVEPOINT sp_post_gl_entries_begin;
  -- [1272] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1272 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1272;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1272 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1272 '||SQLERRM);
  END;
  -- [1273] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1273 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1273 '||SQLERRM);
  END;
  -- [1274] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1274 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1274 '||SQLERRM);
  END;
  -- [1275] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1275;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1275;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1275 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1275;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1275 '||SQLERRM);
  END;
  -- [1276] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1276 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1276 '||SQLERRM);
  END;
  -- [1277] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1277 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1277 '||SQLERRM);
  END;
  -- [1278] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1278 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1278 '||SQLERRM);
  END;
  -- [1279] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1279 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1279 '||SQLERRM);
  END;
  -- [1280] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1280 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1280 '||SQLERRM);
  END;
  -- [1281] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1281 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1281 '||SQLERRM);
  END;
  -- [1282] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1282 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1282 '||SQLERRM);
  END;
  -- [1283] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1283 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1283 '||SQLERRM);
  END;
  -- [1284] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1284 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1284 '||SQLERRM);
  END;
  -- [1285] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1285 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1285 '||SQLERRM);
  END;
  -- [1286] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1286 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1286 '||SQLERRM);
  END;
  -- [1287] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1287);
    v_tot := erp_pkg_planner.compute_total(1287);
    v_tot := v_tot + erp_pkg_planner.compute_total(1287, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1287))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1287 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1287 '||SQLERRM);
  END;
  -- [1288] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1288 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1288 '||SQLERRM);
  END;
  -- [1289] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1289 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1289;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1289 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1289 '||SQLERRM);
  END;
  -- [1290] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1290 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1290 '||SQLERRM);
  END;
  -- [1291] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1291;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1291 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1291 '||SQLERRM);
  END;
  -- [1292] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1292 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1292 '||SQLERRM);
  END;
  -- [1293] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1293 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1293 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1293 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1293 '||SQLERRM);
  END;
  -- [1294] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1294 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1294 '||SQLERRM);
  END;
  -- [1295] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1295 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1295 '||SQLERRM);
  END;
  -- [1296] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1296 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1296 '||SQLERRM);
  END;
  -- [1297] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1297 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1297 '||SQLERRM);
  END;
  -- [1298] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1298 IS REF CURSOR;
    rc      t_rc_1298;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1298 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1298 '||SQLERRM);
  END;
  -- [1299] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1299')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1299', 1299, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1299 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1299 '||SQLERRM);
  END;
  -- [1300] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1300 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1300;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1300 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1300 '||SQLERRM);
  END;
  -- [1301] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1301 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1301 '||SQLERRM);
  END;
  -- [1302] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1302 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1302 '||SQLERRM);
  END;
  -- [1303] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1303;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1303;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1303 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1303;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1303 '||SQLERRM);
  END;
  -- [1304] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1304 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1304 '||SQLERRM);
  END;
  -- [1305] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1305 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1305 '||SQLERRM);
  END;
  -- [1306] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1306 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1306 '||SQLERRM);
  END;
  -- [1307] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1307 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1307 '||SQLERRM);
  END;
  -- [1308] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1308 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1308 '||SQLERRM);
  END;
  -- [1309] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1309 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1309 '||SQLERRM);
  END;
  -- [1310] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1310 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1310 '||SQLERRM);
  END;
  -- [1311] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1311 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1311 '||SQLERRM);
  END;
  -- [1312] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1312 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1312 '||SQLERRM);
  END;
  -- [1313] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1313 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1313 '||SQLERRM);
  END;
  -- [1314] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1314 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1314 '||SQLERRM);
  END;
  -- [1315] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1315);
    v_tot := erp_pkg_planner.compute_total(1315);
    v_tot := v_tot + erp_pkg_planner.compute_total(1315, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1315))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1315 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1315 '||SQLERRM);
  END;
  -- [1316] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1316 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1316 '||SQLERRM);
  END;
  -- [1317] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1317 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1317;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1317 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1317 '||SQLERRM);
  END;
  -- [1318] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1318 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1318 '||SQLERRM);
  END;
  -- [1319] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1319;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1319 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1319 '||SQLERRM);
  END;
  -- [1320] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1320 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1320 '||SQLERRM);
  END;
  -- [1321] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1321 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1321 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1321 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1321 '||SQLERRM);
  END;
  -- [1322] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1322 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1322 '||SQLERRM);
  END;
  -- [1323] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1323 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1323 '||SQLERRM);
  END;
  -- [1324] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1324 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1324 '||SQLERRM);
  END;
  -- [1325] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1325 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1325 '||SQLERRM);
  END;
  -- [1326] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1326 IS REF CURSOR;
    rc      t_rc_1326;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1326 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1326 '||SQLERRM);
  END;
  -- [1327] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1327')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1327', 1327, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1327 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1327 '||SQLERRM);
  END;
  -- [1328] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1328 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1328;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1328 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1328 '||SQLERRM);
  END;
  -- [1329] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1329 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1329 '||SQLERRM);
  END;
  -- [1330] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1330 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1330 '||SQLERRM);
  END;
  -- [1331] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1331;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1331;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1331 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1331;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1331 '||SQLERRM);
  END;
  -- [1332] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1332 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1332 '||SQLERRM);
  END;
  -- [1333] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1333 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1333 '||SQLERRM);
  END;
  -- [1334] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1334 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1334 '||SQLERRM);
  END;
  -- [1335] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1335 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1335 '||SQLERRM);
  END;
  -- [1336] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1336 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1336 '||SQLERRM);
  END;
  -- [1337] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1337 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1337 '||SQLERRM);
  END;
  -- [1338] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1338 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1338 '||SQLERRM);
  END;
  -- [1339] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1339 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1339 '||SQLERRM);
  END;
  -- [1340] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1340 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1340 '||SQLERRM);
  END;
  -- [1341] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1341 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1341 '||SQLERRM);
  END;
  -- [1342] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1342 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1342 '||SQLERRM);
  END;
  -- [1343] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1343);
    v_tot := erp_pkg_planner.compute_total(1343);
    v_tot := v_tot + erp_pkg_planner.compute_total(1343, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1343))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1343 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1343 '||SQLERRM);
  END;

  -- descend one level deeper in the ERP cycle call chain
  erp_usp_finalize_gl_batch(600);

  erp_usp_write_audit_trail('GL_POST_DONE','ERP_GL_ENTRY','gl entries posted');
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK TO SAVEPOINT sp_post_gl_entries_begin;
    erp_usp_write_audit_trail('GL_POST_ERR','ERP_GL_ENTRY',SQLERRM);
END erp_usp_post_gl_entries;
/

CREATE OR REPLACE PROCEDURE erp_usp_generate_invoice(p_run_id IN NUMBER) IS
  v_inv NUMBER := 0;
BEGIN
  erp_usp_write_audit_trail('INVOICE_GEN','ERP_INVOICE','enter erp_usp_generate_invoice');
  SAVEPOINT sp_generate_invoice_begin;
  -- [1344] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1344 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1344 '||SQLERRM);
  END;
  -- [1345] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1345 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1345;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1345 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1345 '||SQLERRM);
  END;
  -- [1346] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1346 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1346 '||SQLERRM);
  END;
  -- [1347] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1347;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1347 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1347 '||SQLERRM);
  END;
  -- [1348] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1348 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1348 '||SQLERRM);
  END;
  -- [1349] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1349 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1349 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1349 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1349 '||SQLERRM);
  END;
  -- [1350] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1350 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1350 '||SQLERRM);
  END;
  -- [1351] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1351 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1351 '||SQLERRM);
  END;
  -- [1352] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1352 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1352 '||SQLERRM);
  END;
  -- [1353] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1353 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1353 '||SQLERRM);
  END;
  -- [1354] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1354 IS REF CURSOR;
    rc      t_rc_1354;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1354 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1354 '||SQLERRM);
  END;
  -- [1355] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1355')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1355', 1355, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1355 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1355 '||SQLERRM);
  END;
  -- [1356] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1356 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1356;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1356 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1356 '||SQLERRM);
  END;
  -- [1357] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1357 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1357 '||SQLERRM);
  END;
  -- [1358] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1358 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1358 '||SQLERRM);
  END;
  -- [1359] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1359;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1359;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1359 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1359;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1359 '||SQLERRM);
  END;
  -- [1360] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1360 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1360 '||SQLERRM);
  END;
  -- [1361] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1361 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1361 '||SQLERRM);
  END;
  -- [1362] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1362 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1362 '||SQLERRM);
  END;
  -- [1363] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1363 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1363 '||SQLERRM);
  END;
  -- [1364] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1364 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1364 '||SQLERRM);
  END;
  -- [1365] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1365 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1365 '||SQLERRM);
  END;
  -- [1366] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1366 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1366 '||SQLERRM);
  END;
  -- [1367] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1367 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1367 '||SQLERRM);
  END;
  -- [1368] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1368 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1368 '||SQLERRM);
  END;
  -- [1369] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1369 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1369 '||SQLERRM);
  END;
  -- [1370] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1370 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1370 '||SQLERRM);
  END;
  -- [1371] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1371);
    v_tot := erp_pkg_planner.compute_total(1371);
    v_tot := v_tot + erp_pkg_planner.compute_total(1371, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1371))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1371 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1371 '||SQLERRM);
  END;
  -- [1372] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1372 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1372 '||SQLERRM);
  END;
  -- [1373] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1373 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1373;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1373 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1373 '||SQLERRM);
  END;
  -- [1374] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1374 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1374 '||SQLERRM);
  END;
  -- [1375] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1375;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1375 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1375 '||SQLERRM);
  END;
  -- [1376] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1376 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1376 '||SQLERRM);
  END;
  -- [1377] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1377 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1377 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1377 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1377 '||SQLERRM);
  END;
  -- [1378] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1378 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1378 '||SQLERRM);
  END;
  -- [1379] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1379 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1379 '||SQLERRM);
  END;
  -- [1380] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1380 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1380 '||SQLERRM);
  END;
  -- [1381] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1381 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1381 '||SQLERRM);
  END;
  -- [1382] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1382 IS REF CURSOR;
    rc      t_rc_1382;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1382 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1382 '||SQLERRM);
  END;
  -- [1383] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1383')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1383', 1383, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1383 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1383 '||SQLERRM);
  END;
  -- [1384] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1384 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1384;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1384 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1384 '||SQLERRM);
  END;
  -- [1385] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1385 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1385 '||SQLERRM);
  END;
  -- [1386] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1386 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1386 '||SQLERRM);
  END;
  -- [1387] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1387;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1387;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1387 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1387;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1387 '||SQLERRM);
  END;
  -- [1388] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1388 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1388 '||SQLERRM);
  END;
  -- [1389] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1389 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1389 '||SQLERRM);
  END;
  -- [1390] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1390 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1390 '||SQLERRM);
  END;
  -- [1391] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1391 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1391 '||SQLERRM);
  END;
  -- [1392] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1392 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1392 '||SQLERRM);
  END;
  -- [1393] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1393 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1393 '||SQLERRM);
  END;
  -- [1394] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1394 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1394 '||SQLERRM);
  END;
  -- [1395] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1395 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1395 '||SQLERRM);
  END;
  -- [1396] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1396 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1396 '||SQLERRM);
  END;
  -- [1397] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1397 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1397 '||SQLERRM);
  END;
  -- [1398] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1398 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1398 '||SQLERRM);
  END;
  -- [1399] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1399);
    v_tot := erp_pkg_planner.compute_total(1399);
    v_tot := v_tot + erp_pkg_planner.compute_total(1399, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1399))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1399 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1399 '||SQLERRM);
  END;
  -- [1400] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1400 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1400 '||SQLERRM);
  END;
  -- [1401] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1401 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1401;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1401 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1401 '||SQLERRM);
  END;
  -- [1402] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1402 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1402 '||SQLERRM);
  END;
  -- [1403] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1403;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1403 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1403 '||SQLERRM);
  END;
  -- [1404] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1404 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1404 '||SQLERRM);
  END;
  -- [1405] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1405 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1405 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1405 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1405 '||SQLERRM);
  END;
  -- [1406] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1406 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1406 '||SQLERRM);
  END;
  -- [1407] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1407 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1407 '||SQLERRM);
  END;
  -- [1408] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1408 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1408 '||SQLERRM);
  END;
  -- [1409] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1409 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1409 '||SQLERRM);
  END;
  -- [1410] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1410 IS REF CURSOR;
    rc      t_rc_1410;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1410 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1410 '||SQLERRM);
  END;
  -- [1411] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1411')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1411', 1411, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1411 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1411 '||SQLERRM);
  END;
  -- [1412] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1412 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1412;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1412 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1412 '||SQLERRM);
  END;
  -- [1413] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1413 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1413 '||SQLERRM);
  END;
  -- [1414] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1414 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1414 '||SQLERRM);
  END;
  -- [1415] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1415;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1415;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1415 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1415;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1415 '||SQLERRM);
  END;

  -- descend one level deeper in the ERP cycle call chain
  erp_usp_post_gl_entries(p_run_id);

  erp_usp_write_audit_trail('INVOICE_GEN_DONE','ERP_INVOICE','invoices generated');
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK TO SAVEPOINT sp_generate_invoice_begin;
    erp_usp_write_audit_trail('INVOICE_GEN_ERR','ERP_INVOICE',SQLERRM);
END erp_usp_generate_invoice;
/

CREATE OR REPLACE PROCEDURE erp_usp_ship_order(p_run_id IN NUMBER) IS
  v_ship NUMBER := 0;
BEGIN
  erp_usp_write_audit_trail('SHIP','ERP_SHIPMENT','enter erp_usp_ship_order');
  SAVEPOINT sp_ship_order_begin;
  -- [1416] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1416 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1416 '||SQLERRM);
  END;
  -- [1417] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1417 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1417 '||SQLERRM);
  END;
  -- [1418] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1418 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1418 '||SQLERRM);
  END;
  -- [1419] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1419 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1419 '||SQLERRM);
  END;
  -- [1420] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1420 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1420 '||SQLERRM);
  END;
  -- [1421] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1421 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1421 '||SQLERRM);
  END;
  -- [1422] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1422 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1422 '||SQLERRM);
  END;
  -- [1423] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1423 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1423 '||SQLERRM);
  END;
  -- [1424] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1424 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1424 '||SQLERRM);
  END;
  -- [1425] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1425 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1425 '||SQLERRM);
  END;
  -- [1426] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1426 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1426 '||SQLERRM);
  END;
  -- [1427] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1427);
    v_tot := erp_pkg_planner.compute_total(1427);
    v_tot := v_tot + erp_pkg_planner.compute_total(1427, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1427))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1427 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1427 '||SQLERRM);
  END;
  -- [1428] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1428 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1428 '||SQLERRM);
  END;
  -- [1429] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1429 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1429;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1429 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1429 '||SQLERRM);
  END;
  -- [1430] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1430 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1430 '||SQLERRM);
  END;
  -- [1431] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1431;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1431 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1431 '||SQLERRM);
  END;
  -- [1432] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1432 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1432 '||SQLERRM);
  END;
  -- [1433] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1433 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1433 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1433 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1433 '||SQLERRM);
  END;
  -- [1434] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1434 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1434 '||SQLERRM);
  END;
  -- [1435] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1435 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1435 '||SQLERRM);
  END;
  -- [1436] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1436 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1436 '||SQLERRM);
  END;
  -- [1437] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1437 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1437 '||SQLERRM);
  END;
  -- [1438] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1438 IS REF CURSOR;
    rc      t_rc_1438;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1438 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1438 '||SQLERRM);
  END;
  -- [1439] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1439')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1439', 1439, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1439 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1439 '||SQLERRM);
  END;
  -- [1440] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1440 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1440;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1440 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1440 '||SQLERRM);
  END;
  -- [1441] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1441 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1441 '||SQLERRM);
  END;
  -- [1442] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1442 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1442 '||SQLERRM);
  END;
  -- [1443] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1443;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1443;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1443 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1443;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1443 '||SQLERRM);
  END;
  -- [1444] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1444 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1444 '||SQLERRM);
  END;
  -- [1445] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1445 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1445 '||SQLERRM);
  END;
  -- [1446] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1446 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1446 '||SQLERRM);
  END;
  -- [1447] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1447 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1447 '||SQLERRM);
  END;
  -- [1448] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1448 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1448 '||SQLERRM);
  END;
  -- [1449] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1449 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1449 '||SQLERRM);
  END;
  -- [1450] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1450 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1450 '||SQLERRM);
  END;
  -- [1451] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1451 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1451 '||SQLERRM);
  END;
  -- [1452] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1452 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1452 '||SQLERRM);
  END;
  -- [1453] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1453 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1453 '||SQLERRM);
  END;
  -- [1454] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1454 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1454 '||SQLERRM);
  END;
  -- [1455] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1455);
    v_tot := erp_pkg_planner.compute_total(1455);
    v_tot := v_tot + erp_pkg_planner.compute_total(1455, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1455))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1455 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1455 '||SQLERRM);
  END;
  -- [1456] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1456 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1456 '||SQLERRM);
  END;
  -- [1457] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1457 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1457;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1457 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1457 '||SQLERRM);
  END;
  -- [1458] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1458 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1458 '||SQLERRM);
  END;
  -- [1459] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1459;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1459 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1459 '||SQLERRM);
  END;
  -- [1460] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1460 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1460 '||SQLERRM);
  END;
  -- [1461] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1461 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1461 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1461 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1461 '||SQLERRM);
  END;
  -- [1462] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1462 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1462 '||SQLERRM);
  END;
  -- [1463] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1463 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1463 '||SQLERRM);
  END;
  -- [1464] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1464 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1464 '||SQLERRM);
  END;
  -- [1465] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1465 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1465 '||SQLERRM);
  END;
  -- [1466] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1466 IS REF CURSOR;
    rc      t_rc_1466;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1466 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1466 '||SQLERRM);
  END;
  -- [1467] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1467')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1467', 1467, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1467 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1467 '||SQLERRM);
  END;
  -- [1468] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1468 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1468;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1468 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1468 '||SQLERRM);
  END;
  -- [1469] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1469 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1469 '||SQLERRM);
  END;
  -- [1470] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1470 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1470 '||SQLERRM);
  END;
  -- [1471] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1471;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1471;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1471 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1471;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1471 '||SQLERRM);
  END;
  -- [1472] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1472 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1472 '||SQLERRM);
  END;
  -- [1473] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1473 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1473 '||SQLERRM);
  END;
  -- [1474] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1474 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1474 '||SQLERRM);
  END;
  -- [1475] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1475 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1475 '||SQLERRM);
  END;
  -- [1476] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1476 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1476 '||SQLERRM);
  END;
  -- [1477] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1477 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1477 '||SQLERRM);
  END;
  -- [1478] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1478 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1478 '||SQLERRM);
  END;
  -- [1479] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1479 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1479 '||SQLERRM);
  END;
  -- [1480] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1480 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1480 '||SQLERRM);
  END;
  -- [1481] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1481 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1481 '||SQLERRM);
  END;
  -- [1482] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1482 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1482 '||SQLERRM);
  END;
  -- [1483] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1483);
    v_tot := erp_pkg_planner.compute_total(1483);
    v_tot := v_tot + erp_pkg_planner.compute_total(1483, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1483))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1483 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1483 '||SQLERRM);
  END;
  -- [1484] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1484 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1484 '||SQLERRM);
  END;
  -- [1485] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1485 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1485;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1485 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1485 '||SQLERRM);
  END;
  -- [1486] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1486 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1486 '||SQLERRM);
  END;
  -- [1487] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1487;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1487 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1487 '||SQLERRM);
  END;

  -- descend one level deeper in the ERP cycle call chain
  erp_usp_generate_invoice(p_run_id);

  erp_usp_write_audit_trail('SHIP_DONE','ERP_SHIPMENT','orders shipped');
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK TO SAVEPOINT sp_ship_order_begin;
    erp_usp_write_audit_trail('SHIP_ERR','ERP_SHIPMENT',SQLERRM);
END erp_usp_ship_order;
/

CREATE OR REPLACE PROCEDURE erp_usp_complete_work_order(p_run_id IN NUMBER) IS
  v_done NUMBER := 0;
BEGIN
  erp_usp_write_audit_trail('WO_COMPLETE','ERP_WORK_ORDER','enter erp_usp_complete_work_order');
  SAVEPOINT sp_complete_work_order_begin;
  -- [1488] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1488 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1488 '||SQLERRM);
  END;
  -- [1489] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1489 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1489 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1489 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1489 '||SQLERRM);
  END;
  -- [1490] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1490 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1490 '||SQLERRM);
  END;
  -- [1491] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1491 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1491 '||SQLERRM);
  END;
  -- [1492] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1492 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1492 '||SQLERRM);
  END;
  -- [1493] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1493 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1493 '||SQLERRM);
  END;
  -- [1494] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1494 IS REF CURSOR;
    rc      t_rc_1494;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1494 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1494 '||SQLERRM);
  END;
  -- [1495] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1495')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1495', 1495, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1495 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1495 '||SQLERRM);
  END;
  -- [1496] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1496 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1496;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1496 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1496 '||SQLERRM);
  END;
  -- [1497] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1497 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1497 '||SQLERRM);
  END;
  -- [1498] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1498 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1498 '||SQLERRM);
  END;
  -- [1499] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1499;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1499;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1499 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1499;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1499 '||SQLERRM);
  END;
  -- [1500] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1500 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1500 '||SQLERRM);
  END;
  -- [1501] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1501 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1501 '||SQLERRM);
  END;
  -- [1502] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1502 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1502 '||SQLERRM);
  END;
  -- [1503] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1503 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1503 '||SQLERRM);
  END;
  -- [1504] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1504 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1504 '||SQLERRM);
  END;
  -- [1505] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1505 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1505 '||SQLERRM);
  END;
  -- [1506] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1506 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1506 '||SQLERRM);
  END;
  -- [1507] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1507 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1507 '||SQLERRM);
  END;
  -- [1508] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1508 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1508 '||SQLERRM);
  END;
  -- [1509] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1509 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1509 '||SQLERRM);
  END;
  -- [1510] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1510 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1510 '||SQLERRM);
  END;
  -- [1511] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1511);
    v_tot := erp_pkg_planner.compute_total(1511);
    v_tot := v_tot + erp_pkg_planner.compute_total(1511, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1511))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1511 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1511 '||SQLERRM);
  END;
  -- [1512] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1512 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1512 '||SQLERRM);
  END;
  -- [1513] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1513 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1513;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1513 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1513 '||SQLERRM);
  END;
  -- [1514] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1514 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1514 '||SQLERRM);
  END;
  -- [1515] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1515;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1515 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1515 '||SQLERRM);
  END;
  -- [1516] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1516 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1516 '||SQLERRM);
  END;
  -- [1517] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1517 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1517 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1517 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1517 '||SQLERRM);
  END;
  -- [1518] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1518 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1518 '||SQLERRM);
  END;
  -- [1519] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1519 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1519 '||SQLERRM);
  END;
  -- [1520] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1520 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1520 '||SQLERRM);
  END;
  -- [1521] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1521 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1521 '||SQLERRM);
  END;
  -- [1522] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1522 IS REF CURSOR;
    rc      t_rc_1522;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1522 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1522 '||SQLERRM);
  END;
  -- [1523] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1523')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1523', 1523, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1523 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1523 '||SQLERRM);
  END;
  -- [1524] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1524 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1524;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1524 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1524 '||SQLERRM);
  END;
  -- [1525] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1525 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1525 '||SQLERRM);
  END;
  -- [1526] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1526 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1526 '||SQLERRM);
  END;
  -- [1527] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1527;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1527;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1527 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1527;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1527 '||SQLERRM);
  END;
  -- [1528] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1528 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1528 '||SQLERRM);
  END;
  -- [1529] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1529 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1529 '||SQLERRM);
  END;
  -- [1530] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1530 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1530 '||SQLERRM);
  END;
  -- [1531] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1531 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1531 '||SQLERRM);
  END;
  -- [1532] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1532 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1532 '||SQLERRM);
  END;
  -- [1533] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1533 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1533 '||SQLERRM);
  END;
  -- [1534] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1534 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1534 '||SQLERRM);
  END;
  -- [1535] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1535 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1535 '||SQLERRM);
  END;
  -- [1536] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1536 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1536 '||SQLERRM);
  END;
  -- [1537] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1537 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1537 '||SQLERRM);
  END;
  -- [1538] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1538 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1538 '||SQLERRM);
  END;
  -- [1539] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1539);
    v_tot := erp_pkg_planner.compute_total(1539);
    v_tot := v_tot + erp_pkg_planner.compute_total(1539, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1539))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1539 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1539 '||SQLERRM);
  END;
  -- [1540] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1540 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1540 '||SQLERRM);
  END;
  -- [1541] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1541 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1541;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1541 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1541 '||SQLERRM);
  END;
  -- [1542] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1542 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1542 '||SQLERRM);
  END;
  -- [1543] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1543;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1543 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1543 '||SQLERRM);
  END;
  -- [1544] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1544 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1544 '||SQLERRM);
  END;
  -- [1545] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1545 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1545 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1545 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1545 '||SQLERRM);
  END;
  -- [1546] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1546 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1546 '||SQLERRM);
  END;
  -- [1547] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1547 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1547 '||SQLERRM);
  END;
  -- [1548] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1548 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1548 '||SQLERRM);
  END;
  -- [1549] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1549 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1549 '||SQLERRM);
  END;
  -- [1550] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1550 IS REF CURSOR;
    rc      t_rc_1550;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1550 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1550 '||SQLERRM);
  END;
  -- [1551] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1551')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1551', 1551, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1551 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1551 '||SQLERRM);
  END;
  -- [1552] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1552 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1552;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1552 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1552 '||SQLERRM);
  END;
  -- [1553] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1553 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1553 '||SQLERRM);
  END;
  -- [1554] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1554 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1554 '||SQLERRM);
  END;
  -- [1555] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1555;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1555;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1555 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1555;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1555 '||SQLERRM);
  END;
  -- [1556] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1556 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1556 '||SQLERRM);
  END;
  -- [1557] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1557 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1557 '||SQLERRM);
  END;
  -- [1558] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1558 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1558 '||SQLERRM);
  END;
  -- [1559] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1559 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1559 '||SQLERRM);
  END;

  -- descend one level deeper in the ERP cycle call chain
  erp_usp_ship_order(p_run_id);

  erp_usp_write_audit_trail('WO_COMPLETE_DONE','ERP_WORK_ORDER','work orders completed');
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK TO SAVEPOINT sp_complete_work_order_begin;
    erp_usp_write_audit_trail('WO_COMPLETE_ERR','ERP_WORK_ORDER',SQLERRM);
END erp_usp_complete_work_order;
/

CREATE OR REPLACE PROCEDURE erp_usp_release_work_orders(p_run_id IN NUMBER) IS
  v_rel NUMBER := 0;
BEGIN
  erp_usp_write_audit_trail('WO_RELEASE','ERP_WORK_ORDER','enter erp_usp_release_work_orders');
  SAVEPOINT sp_release_work_orders_begin;
  -- [1560] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1560 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1560 '||SQLERRM);
  END;
  -- [1561] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1561 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1561 '||SQLERRM);
  END;
  -- [1562] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1562 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1562 '||SQLERRM);
  END;
  -- [1563] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1563 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1563 '||SQLERRM);
  END;
  -- [1564] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1564 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1564 '||SQLERRM);
  END;
  -- [1565] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1565 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1565 '||SQLERRM);
  END;
  -- [1566] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1566 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1566 '||SQLERRM);
  END;
  -- [1567] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1567);
    v_tot := erp_pkg_planner.compute_total(1567);
    v_tot := v_tot + erp_pkg_planner.compute_total(1567, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1567))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1567 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1567 '||SQLERRM);
  END;
  -- [1568] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1568 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1568 '||SQLERRM);
  END;
  -- [1569] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1569 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1569;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1569 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1569 '||SQLERRM);
  END;
  -- [1570] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1570 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1570 '||SQLERRM);
  END;
  -- [1571] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1571;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1571 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1571 '||SQLERRM);
  END;
  -- [1572] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1572 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1572 '||SQLERRM);
  END;
  -- [1573] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1573 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1573 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1573 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1573 '||SQLERRM);
  END;
  -- [1574] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1574 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1574 '||SQLERRM);
  END;
  -- [1575] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1575 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1575 '||SQLERRM);
  END;
  -- [1576] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1576 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1576 '||SQLERRM);
  END;
  -- [1577] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1577 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1577 '||SQLERRM);
  END;
  -- [1578] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1578 IS REF CURSOR;
    rc      t_rc_1578;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1578 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1578 '||SQLERRM);
  END;
  -- [1579] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1579')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1579', 1579, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1579 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1579 '||SQLERRM);
  END;
  -- [1580] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1580 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1580;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1580 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1580 '||SQLERRM);
  END;
  -- [1581] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1581 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1581 '||SQLERRM);
  END;
  -- [1582] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1582 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1582 '||SQLERRM);
  END;
  -- [1583] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1583;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1583;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1583 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1583;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1583 '||SQLERRM);
  END;
  -- [1584] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1584 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1584 '||SQLERRM);
  END;
  -- [1585] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1585 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1585 '||SQLERRM);
  END;
  -- [1586] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1586 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1586 '||SQLERRM);
  END;
  -- [1587] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1587 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1587 '||SQLERRM);
  END;
  -- [1588] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1588 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1588 '||SQLERRM);
  END;
  -- [1589] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1589 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1589 '||SQLERRM);
  END;
  -- [1590] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1590 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1590 '||SQLERRM);
  END;
  -- [1591] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1591 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1591 '||SQLERRM);
  END;
  -- [1592] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1592 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1592 '||SQLERRM);
  END;
  -- [1593] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1593 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1593 '||SQLERRM);
  END;
  -- [1594] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1594 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1594 '||SQLERRM);
  END;
  -- [1595] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1595);
    v_tot := erp_pkg_planner.compute_total(1595);
    v_tot := v_tot + erp_pkg_planner.compute_total(1595, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1595))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1595 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1595 '||SQLERRM);
  END;
  -- [1596] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1596 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1596 '||SQLERRM);
  END;
  -- [1597] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1597 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1597;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1597 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1597 '||SQLERRM);
  END;
  -- [1598] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1598 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1598 '||SQLERRM);
  END;
  -- [1599] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1599;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1599 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1599 '||SQLERRM);
  END;
  -- [1600] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1600 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1600 '||SQLERRM);
  END;
  -- [1601] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1601 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1601 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1601 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1601 '||SQLERRM);
  END;
  -- [1602] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1602 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1602 '||SQLERRM);
  END;
  -- [1603] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1603 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1603 '||SQLERRM);
  END;
  -- [1604] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1604 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1604 '||SQLERRM);
  END;
  -- [1605] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1605 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1605 '||SQLERRM);
  END;
  -- [1606] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1606 IS REF CURSOR;
    rc      t_rc_1606;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1606 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1606 '||SQLERRM);
  END;
  -- [1607] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1607')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1607', 1607, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1607 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1607 '||SQLERRM);
  END;
  -- [1608] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1608 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1608;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1608 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1608 '||SQLERRM);
  END;
  -- [1609] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1609 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1609 '||SQLERRM);
  END;
  -- [1610] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1610 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1610 '||SQLERRM);
  END;
  -- [1611] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1611;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1611;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1611 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1611;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1611 '||SQLERRM);
  END;
  -- [1612] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1612 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1612 '||SQLERRM);
  END;
  -- [1613] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1613 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1613 '||SQLERRM);
  END;
  -- [1614] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1614 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1614 '||SQLERRM);
  END;
  -- [1615] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1615 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1615 '||SQLERRM);
  END;
  -- [1616] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1616 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1616 '||SQLERRM);
  END;
  -- [1617] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1617 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1617 '||SQLERRM);
  END;
  -- [1618] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1618 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1618 '||SQLERRM);
  END;
  -- [1619] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1619 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1619 '||SQLERRM);
  END;
  -- [1620] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1620 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1620 '||SQLERRM);
  END;
  -- [1621] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1621 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1621 '||SQLERRM);
  END;
  -- [1622] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1622 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1622 '||SQLERRM);
  END;
  -- [1623] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1623);
    v_tot := erp_pkg_planner.compute_total(1623);
    v_tot := v_tot + erp_pkg_planner.compute_total(1623, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1623))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1623 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1623 '||SQLERRM);
  END;
  -- [1624] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1624 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1624 '||SQLERRM);
  END;
  -- [1625] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1625 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1625;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1625 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1625 '||SQLERRM);
  END;
  -- [1626] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1626 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1626 '||SQLERRM);
  END;
  -- [1627] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1627;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1627 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1627 '||SQLERRM);
  END;
  -- [1628] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1628 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1628 '||SQLERRM);
  END;
  -- [1629] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1629 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1629 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1629 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1629 '||SQLERRM);
  END;
  -- [1630] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1630 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1630 '||SQLERRM);
  END;
  -- [1631] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1631 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1631 '||SQLERRM);
  END;

  -- descend one level deeper in the ERP cycle call chain
  erp_usp_complete_work_order(p_run_id);

  erp_usp_write_audit_trail('WO_RELEASE_DONE','ERP_WORK_ORDER','work orders released');
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK TO SAVEPOINT sp_release_work_orders_begin;
    erp_usp_write_audit_trail('WO_RELEASE_ERR','ERP_WORK_ORDER',SQLERRM);
END erp_usp_release_work_orders;
/

CREATE OR REPLACE PROCEDURE erp_usp_compute_net_requirements(p_run_id IN NUMBER) IS
  v_net NUMBER := 0;
BEGIN
  erp_usp_write_audit_trail('NETREQ','ERP_MRP_PLAN','enter erp_usp_compute_net_requirements');
  SAVEPOINT sp_compute_net_requirements_begin;
  -- [1632] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1632 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1632 '||SQLERRM);
  END;
  -- [1633] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1633 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1633 '||SQLERRM);
  END;
  -- [1634] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1634 IS REF CURSOR;
    rc      t_rc_1634;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1634 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1634 '||SQLERRM);
  END;
  -- [1635] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1635')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1635', 1635, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1635 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1635 '||SQLERRM);
  END;
  -- [1636] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1636 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1636;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1636 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1636 '||SQLERRM);
  END;
  -- [1637] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1637 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1637 '||SQLERRM);
  END;
  -- [1638] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1638 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1638 '||SQLERRM);
  END;
  -- [1639] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1639;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1639;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1639 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1639;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1639 '||SQLERRM);
  END;
  -- [1640] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1640 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1640 '||SQLERRM);
  END;
  -- [1641] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1641 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1641 '||SQLERRM);
  END;
  -- [1642] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1642 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1642 '||SQLERRM);
  END;
  -- [1643] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1643 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1643 '||SQLERRM);
  END;
  -- [1644] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1644 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1644 '||SQLERRM);
  END;
  -- [1645] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1645 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1645 '||SQLERRM);
  END;
  -- [1646] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1646 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1646 '||SQLERRM);
  END;
  -- [1647] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1647 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1647 '||SQLERRM);
  END;
  -- [1648] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1648 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1648 '||SQLERRM);
  END;
  -- [1649] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1649 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1649 '||SQLERRM);
  END;
  -- [1650] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1650 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1650 '||SQLERRM);
  END;
  -- [1651] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1651);
    v_tot := erp_pkg_planner.compute_total(1651);
    v_tot := v_tot + erp_pkg_planner.compute_total(1651, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1651))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1651 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1651 '||SQLERRM);
  END;
  -- [1652] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1652 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1652 '||SQLERRM);
  END;
  -- [1653] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1653 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1653;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1653 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1653 '||SQLERRM);
  END;
  -- [1654] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1654 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1654 '||SQLERRM);
  END;
  -- [1655] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1655;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1655 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1655 '||SQLERRM);
  END;
  -- [1656] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1656 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1656 '||SQLERRM);
  END;
  -- [1657] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1657 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1657 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1657 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1657 '||SQLERRM);
  END;
  -- [1658] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1658 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1658 '||SQLERRM);
  END;
  -- [1659] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1659 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1659 '||SQLERRM);
  END;
  -- [1660] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1660 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1660 '||SQLERRM);
  END;
  -- [1661] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1661 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1661 '||SQLERRM);
  END;
  -- [1662] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1662 IS REF CURSOR;
    rc      t_rc_1662;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1662 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1662 '||SQLERRM);
  END;
  -- [1663] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1663')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1663', 1663, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1663 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1663 '||SQLERRM);
  END;
  -- [1664] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1664 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1664;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1664 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1664 '||SQLERRM);
  END;
  -- [1665] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1665 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1665 '||SQLERRM);
  END;
  -- [1666] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1666 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1666 '||SQLERRM);
  END;
  -- [1667] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1667;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1667;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1667 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1667;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1667 '||SQLERRM);
  END;
  -- [1668] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1668 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1668 '||SQLERRM);
  END;
  -- [1669] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1669 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1669 '||SQLERRM);
  END;
  -- [1670] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1670 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1670 '||SQLERRM);
  END;
  -- [1671] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1671 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1671 '||SQLERRM);
  END;
  -- [1672] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1672 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1672 '||SQLERRM);
  END;
  -- [1673] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1673 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1673 '||SQLERRM);
  END;
  -- [1674] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1674 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1674 '||SQLERRM);
  END;
  -- [1675] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1675 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1675 '||SQLERRM);
  END;
  -- [1676] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1676 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1676 '||SQLERRM);
  END;
  -- [1677] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1677 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1677 '||SQLERRM);
  END;
  -- [1678] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1678 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1678 '||SQLERRM);
  END;
  -- [1679] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1679);
    v_tot := erp_pkg_planner.compute_total(1679);
    v_tot := v_tot + erp_pkg_planner.compute_total(1679, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1679))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1679 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1679 '||SQLERRM);
  END;
  -- [1680] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1680 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1680 '||SQLERRM);
  END;
  -- [1681] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1681 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1681;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1681 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1681 '||SQLERRM);
  END;
  -- [1682] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1682 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1682 '||SQLERRM);
  END;
  -- [1683] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1683;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1683 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1683 '||SQLERRM);
  END;
  -- [1684] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1684 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1684 '||SQLERRM);
  END;
  -- [1685] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1685 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1685 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1685 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1685 '||SQLERRM);
  END;
  -- [1686] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1686 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1686 '||SQLERRM);
  END;
  -- [1687] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1687 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1687 '||SQLERRM);
  END;
  -- [1688] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1688 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1688 '||SQLERRM);
  END;
  -- [1689] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1689 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1689 '||SQLERRM);
  END;
  -- [1690] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1690 IS REF CURSOR;
    rc      t_rc_1690;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1690 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1690 '||SQLERRM);
  END;
  -- [1691] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1691')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1691', 1691, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1691 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1691 '||SQLERRM);
  END;
  -- [1692] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1692 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1692;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1692 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1692 '||SQLERRM);
  END;
  -- [1693] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1693 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1693 '||SQLERRM);
  END;
  -- [1694] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1694 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1694 '||SQLERRM);
  END;
  -- [1695] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1695;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1695;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1695 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1695;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1695 '||SQLERRM);
  END;
  -- [1696] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1696 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1696 '||SQLERRM);
  END;
  -- [1697] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1697 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1697 '||SQLERRM);
  END;
  -- [1698] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1698 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1698 '||SQLERRM);
  END;
  -- [1699] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1699 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1699 '||SQLERRM);
  END;
  -- [1700] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1700 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1700 '||SQLERRM);
  END;
  -- [1701] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1701 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1701 '||SQLERRM);
  END;
  -- [1702] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1702 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1702 '||SQLERRM);
  END;
  -- [1703] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1703 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1703 '||SQLERRM);
  END;

  -- descend one level deeper in the ERP cycle call chain
  erp_usp_release_work_orders(p_run_id);

  erp_usp_write_audit_trail('NETREQ_DONE','ERP_MRP_PLAN','net requirements computed');
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK TO SAVEPOINT sp_compute_net_requirements_begin;
    erp_usp_write_audit_trail('NETREQ_ERR','ERP_MRP_PLAN',SQLERRM);
END erp_usp_compute_net_requirements;
/

CREATE OR REPLACE PROCEDURE erp_usp_explode_demand(p_run_id IN NUMBER) IS
  v_exp NUMBER := 0;
BEGIN
  erp_usp_write_audit_trail('EXPLODE','ERP_DEMAND','enter erp_usp_explode_demand');
  SAVEPOINT sp_explode_demand_begin;
  -- [1704] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1704 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1704 '||SQLERRM);
  END;
  -- [1705] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1705 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1705 '||SQLERRM);
  END;
  -- [1706] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1706 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1706 '||SQLERRM);
  END;
  -- [1707] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1707);
    v_tot := erp_pkg_planner.compute_total(1707);
    v_tot := v_tot + erp_pkg_planner.compute_total(1707, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1707))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1707 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1707 '||SQLERRM);
  END;
  -- [1708] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1708 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1708 '||SQLERRM);
  END;
  -- [1709] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1709 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1709;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1709 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1709 '||SQLERRM);
  END;
  -- [1710] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1710 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1710 '||SQLERRM);
  END;
  -- [1711] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1711;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1711 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1711 '||SQLERRM);
  END;
  -- [1712] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1712 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1712 '||SQLERRM);
  END;
  -- [1713] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1713 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1713 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1713 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1713 '||SQLERRM);
  END;
  -- [1714] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1714 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1714 '||SQLERRM);
  END;
  -- [1715] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1715 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1715 '||SQLERRM);
  END;
  -- [1716] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1716 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1716 '||SQLERRM);
  END;
  -- [1717] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1717 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1717 '||SQLERRM);
  END;
  -- [1718] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1718 IS REF CURSOR;
    rc      t_rc_1718;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1718 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1718 '||SQLERRM);
  END;
  -- [1719] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1719')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1719', 1719, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1719 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1719 '||SQLERRM);
  END;
  -- [1720] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1720 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1720;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1720 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1720 '||SQLERRM);
  END;
  -- [1721] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1721 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1721 '||SQLERRM);
  END;
  -- [1722] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1722 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1722 '||SQLERRM);
  END;
  -- [1723] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1723;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1723;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1723 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1723;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1723 '||SQLERRM);
  END;
  -- [1724] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1724 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1724 '||SQLERRM);
  END;
  -- [1725] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1725 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1725 '||SQLERRM);
  END;
  -- [1726] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1726 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1726 '||SQLERRM);
  END;
  -- [1727] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1727 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1727 '||SQLERRM);
  END;
  -- [1728] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1728 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1728 '||SQLERRM);
  END;
  -- [1729] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1729 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1729 '||SQLERRM);
  END;
  -- [1730] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1730 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1730 '||SQLERRM);
  END;
  -- [1731] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1731 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1731 '||SQLERRM);
  END;
  -- [1732] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1732 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1732 '||SQLERRM);
  END;
  -- [1733] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1733 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1733 '||SQLERRM);
  END;
  -- [1734] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1734 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1734 '||SQLERRM);
  END;
  -- [1735] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1735);
    v_tot := erp_pkg_planner.compute_total(1735);
    v_tot := v_tot + erp_pkg_planner.compute_total(1735, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1735))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1735 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1735 '||SQLERRM);
  END;
  -- [1736] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1736 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1736 '||SQLERRM);
  END;
  -- [1737] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1737 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1737;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1737 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1737 '||SQLERRM);
  END;
  -- [1738] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1738 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1738 '||SQLERRM);
  END;
  -- [1739] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1739;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1739 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1739 '||SQLERRM);
  END;
  -- [1740] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1740 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1740 '||SQLERRM);
  END;
  -- [1741] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1741 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1741 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1741 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1741 '||SQLERRM);
  END;
  -- [1742] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1742 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1742 '||SQLERRM);
  END;
  -- [1743] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1743 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1743 '||SQLERRM);
  END;
  -- [1744] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1744 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1744 '||SQLERRM);
  END;
  -- [1745] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1745 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1745 '||SQLERRM);
  END;
  -- [1746] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1746 IS REF CURSOR;
    rc      t_rc_1746;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1746 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1746 '||SQLERRM);
  END;
  -- [1747] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1747')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1747', 1747, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1747 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1747 '||SQLERRM);
  END;
  -- [1748] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1748 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1748;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1748 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1748 '||SQLERRM);
  END;
  -- [1749] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1749 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1749 '||SQLERRM);
  END;
  -- [1750] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1750 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1750 '||SQLERRM);
  END;
  -- [1751] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1751;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1751;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1751 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1751;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1751 '||SQLERRM);
  END;
  -- [1752] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1752 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1752 '||SQLERRM);
  END;
  -- [1753] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1753 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1753 '||SQLERRM);
  END;
  -- [1754] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1754 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1754 '||SQLERRM);
  END;
  -- [1755] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1755 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1755 '||SQLERRM);
  END;
  -- [1756] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1756 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1756 '||SQLERRM);
  END;
  -- [1757] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1757 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1757 '||SQLERRM);
  END;
  -- [1758] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1758 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1758 '||SQLERRM);
  END;
  -- [1759] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1759 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1759 '||SQLERRM);
  END;
  -- [1760] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1760 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1760 '||SQLERRM);
  END;
  -- [1761] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1761 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1761 '||SQLERRM);
  END;
  -- [1762] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1762 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1762 '||SQLERRM);
  END;
  -- [1763] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1763);
    v_tot := erp_pkg_planner.compute_total(1763);
    v_tot := v_tot + erp_pkg_planner.compute_total(1763, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1763))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1763 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1763 '||SQLERRM);
  END;
  -- [1764] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1764 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1764 '||SQLERRM);
  END;
  -- [1765] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1765 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1765;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1765 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1765 '||SQLERRM);
  END;
  -- [1766] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1766 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1766 '||SQLERRM);
  END;
  -- [1767] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1767;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1767 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1767 '||SQLERRM);
  END;
  -- [1768] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1768 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1768 '||SQLERRM);
  END;
  -- [1769] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1769 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1769 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1769 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1769 '||SQLERRM);
  END;
  -- [1770] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1770 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1770 '||SQLERRM);
  END;
  -- [1771] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1771 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1771 '||SQLERRM);
  END;
  -- [1772] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1772 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1772 '||SQLERRM);
  END;
  -- [1773] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1773 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1773 '||SQLERRM);
  END;
  -- [1774] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1774 IS REF CURSOR;
    rc      t_rc_1774;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1774 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1774 '||SQLERRM);
  END;
  -- [1775] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1775')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1775', 1775, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1775 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1775 '||SQLERRM);
  END;

  -- descend one level deeper in the ERP cycle call chain
  erp_usp_compute_net_requirements(p_run_id);

  erp_usp_write_audit_trail('EXPLODE_DONE','ERP_DEMAND','demand exploded');
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK TO SAVEPOINT sp_explode_demand_begin;
    erp_usp_write_audit_trail('EXPLODE_ERR','ERP_DEMAND',SQLERRM);
END erp_usp_explode_demand;
/

CREATE OR REPLACE PROCEDURE erp_usp_collect_demand(p_run_id IN NUMBER) IS
  v_coll NUMBER := 0;
BEGIN
  erp_usp_write_audit_trail('COLLECT','ERP_DEMAND','enter erp_usp_collect_demand');
  SAVEPOINT sp_collect_demand_begin;
  -- [1776] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1776 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1776;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1776 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1776 '||SQLERRM);
  END;
  -- [1777] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1777 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1777 '||SQLERRM);
  END;
  -- [1778] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1778 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1778 '||SQLERRM);
  END;
  -- [1779] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1779;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1779;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1779 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1779;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1779 '||SQLERRM);
  END;
  -- [1780] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1780 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1780 '||SQLERRM);
  END;
  -- [1781] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1781 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1781 '||SQLERRM);
  END;
  -- [1782] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1782 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1782 '||SQLERRM);
  END;
  -- [1783] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1783 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1783 '||SQLERRM);
  END;
  -- [1784] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1784 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1784 '||SQLERRM);
  END;
  -- [1785] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1785 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1785 '||SQLERRM);
  END;
  -- [1786] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1786 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1786 '||SQLERRM);
  END;
  -- [1787] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1787 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1787 '||SQLERRM);
  END;
  -- [1788] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1788 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1788 '||SQLERRM);
  END;
  -- [1789] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1789 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1789 '||SQLERRM);
  END;
  -- [1790] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1790 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1790 '||SQLERRM);
  END;
  -- [1791] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1791);
    v_tot := erp_pkg_planner.compute_total(1791);
    v_tot := v_tot + erp_pkg_planner.compute_total(1791, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1791))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1791 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1791 '||SQLERRM);
  END;
  -- [1792] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1792 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1792 '||SQLERRM);
  END;
  -- [1793] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1793 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1793;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1793 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1793 '||SQLERRM);
  END;
  -- [1794] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1794 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1794 '||SQLERRM);
  END;
  -- [1795] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1795;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1795 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1795 '||SQLERRM);
  END;
  -- [1796] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1796 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1796 '||SQLERRM);
  END;
  -- [1797] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1797 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1797 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1797 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1797 '||SQLERRM);
  END;
  -- [1798] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1798 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1798 '||SQLERRM);
  END;
  -- [1799] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1799 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1799 '||SQLERRM);
  END;
  -- [1800] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1800 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1800 '||SQLERRM);
  END;
  -- [1801] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1801 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1801 '||SQLERRM);
  END;
  -- [1802] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1802 IS REF CURSOR;
    rc      t_rc_1802;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1802 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1802 '||SQLERRM);
  END;
  -- [1803] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1803')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1803', 1803, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1803 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1803 '||SQLERRM);
  END;
  -- [1804] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1804 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1804;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1804 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1804 '||SQLERRM);
  END;
  -- [1805] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1805 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1805 '||SQLERRM);
  END;
  -- [1806] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1806 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1806 '||SQLERRM);
  END;
  -- [1807] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1807;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1807;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1807 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1807;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1807 '||SQLERRM);
  END;
  -- [1808] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1808 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1808 '||SQLERRM);
  END;
  -- [1809] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1809 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1809 '||SQLERRM);
  END;
  -- [1810] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1810 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1810 '||SQLERRM);
  END;
  -- [1811] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1811 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1811 '||SQLERRM);
  END;
  -- [1812] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1812 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1812 '||SQLERRM);
  END;
  -- [1813] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1813 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1813 '||SQLERRM);
  END;
  -- [1814] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1814 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1814 '||SQLERRM);
  END;
  -- [1815] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1815 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1815 '||SQLERRM);
  END;
  -- [1816] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1816 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1816 '||SQLERRM);
  END;
  -- [1817] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1817 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1817 '||SQLERRM);
  END;
  -- [1818] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1818 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1818 '||SQLERRM);
  END;
  -- [1819] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1819);
    v_tot := erp_pkg_planner.compute_total(1819);
    v_tot := v_tot + erp_pkg_planner.compute_total(1819, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1819))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1819 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1819 '||SQLERRM);
  END;
  -- [1820] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1820 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1820 '||SQLERRM);
  END;
  -- [1821] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1821 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1821;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1821 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1821 '||SQLERRM);
  END;
  -- [1822] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1822 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1822 '||SQLERRM);
  END;
  -- [1823] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1823;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1823 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1823 '||SQLERRM);
  END;
  -- [1824] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1824 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1824 '||SQLERRM);
  END;
  -- [1825] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1825 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1825 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1825 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1825 '||SQLERRM);
  END;
  -- [1826] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1826 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1826 '||SQLERRM);
  END;
  -- [1827] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1827 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1827 '||SQLERRM);
  END;
  -- [1828] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1828 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1828 '||SQLERRM);
  END;
  -- [1829] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1829 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1829 '||SQLERRM);
  END;
  -- [1830] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1830 IS REF CURSOR;
    rc      t_rc_1830;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1830 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1830 '||SQLERRM);
  END;
  -- [1831] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1831')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1831', 1831, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1831 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1831 '||SQLERRM);
  END;
  -- [1832] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1832 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1832;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1832 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1832 '||SQLERRM);
  END;
  -- [1833] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1833 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1833 '||SQLERRM);
  END;
  -- [1834] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1834 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1834 '||SQLERRM);
  END;
  -- [1835] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1835;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1835;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1835 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1835;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1835 '||SQLERRM);
  END;
  -- [1836] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1836 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1836 '||SQLERRM);
  END;
  -- [1837] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1837 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1837 '||SQLERRM);
  END;
  -- [1838] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1838 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1838 '||SQLERRM);
  END;
  -- [1839] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1839 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1839 '||SQLERRM);
  END;
  -- [1840] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1840 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1840 '||SQLERRM);
  END;
  -- [1841] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1841 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1841 '||SQLERRM);
  END;
  -- [1842] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1842 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1842 '||SQLERRM);
  END;
  -- [1843] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1843 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1843 '||SQLERRM);
  END;
  -- [1844] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1844 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1844 '||SQLERRM);
  END;
  -- [1845] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1845 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1845 '||SQLERRM);
  END;
  -- [1846] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1846 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1846 '||SQLERRM);
  END;
  -- [1847] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1847);
    v_tot := erp_pkg_planner.compute_total(1847);
    v_tot := v_tot + erp_pkg_planner.compute_total(1847, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1847))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1847 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1847 '||SQLERRM);
  END;

  -- descend one level deeper in the ERP cycle call chain
  erp_usp_explode_demand(p_run_id);

  erp_usp_write_audit_trail('COLLECT_DONE','ERP_DEMAND','demand collected');
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK TO SAVEPOINT sp_collect_demand_begin;
    erp_usp_write_audit_trail('COLLECT_ERR','ERP_DEMAND',SQLERRM);
END erp_usp_collect_demand;
/

CREATE OR REPLACE PROCEDURE erp_usp_validate_master_data(p_run_id IN NUMBER) IS
  v_bad NUMBER := 0;
BEGIN
  erp_usp_write_audit_trail('VALIDATE','ERP_PRODUCT','enter erp_usp_validate_master_data');
  SAVEPOINT sp_validate_master_data_begin;
  -- [1848] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1848 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1848 '||SQLERRM);
  END;
  -- [1849] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1849 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1849;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1849 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1849 '||SQLERRM);
  END;
  -- [1850] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1850 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1850 '||SQLERRM);
  END;
  -- [1851] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1851;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1851 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1851 '||SQLERRM);
  END;
  -- [1852] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1852 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1852 '||SQLERRM);
  END;
  -- [1853] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1853 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1853 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1853 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1853 '||SQLERRM);
  END;
  -- [1854] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1854 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1854 '||SQLERRM);
  END;
  -- [1855] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1855 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1855 '||SQLERRM);
  END;
  -- [1856] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1856 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1856 '||SQLERRM);
  END;
  -- [1857] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1857 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1857 '||SQLERRM);
  END;
  -- [1858] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1858 IS REF CURSOR;
    rc      t_rc_1858;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1858 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1858 '||SQLERRM);
  END;
  -- [1859] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1859')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1859', 1859, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1859 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1859 '||SQLERRM);
  END;
  -- [1860] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1860 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1860;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1860 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1860 '||SQLERRM);
  END;
  -- [1861] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1861 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1861 '||SQLERRM);
  END;
  -- [1862] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1862 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1862 '||SQLERRM);
  END;
  -- [1863] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1863;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1863;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1863 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1863;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1863 '||SQLERRM);
  END;
  -- [1864] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1864 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1864 '||SQLERRM);
  END;
  -- [1865] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1865 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1865 '||SQLERRM);
  END;
  -- [1866] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1866 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1866 '||SQLERRM);
  END;
  -- [1867] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1867 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1867 '||SQLERRM);
  END;
  -- [1868] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1868 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1868 '||SQLERRM);
  END;
  -- [1869] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1869 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1869 '||SQLERRM);
  END;
  -- [1870] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1870 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1870 '||SQLERRM);
  END;
  -- [1871] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1871 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1871 '||SQLERRM);
  END;
  -- [1872] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1872 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1872 '||SQLERRM);
  END;
  -- [1873] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1873 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1873 '||SQLERRM);
  END;
  -- [1874] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1874 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1874 '||SQLERRM);
  END;
  -- [1875] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1875);
    v_tot := erp_pkg_planner.compute_total(1875);
    v_tot := v_tot + erp_pkg_planner.compute_total(1875, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1875))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1875 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1875 '||SQLERRM);
  END;
  -- [1876] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1876 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1876 '||SQLERRM);
  END;
  -- [1877] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1877 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1877;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1877 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1877 '||SQLERRM);
  END;
  -- [1878] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1878 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1878 '||SQLERRM);
  END;
  -- [1879] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1879;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1879 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1879 '||SQLERRM);
  END;
  -- [1880] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1880 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1880 '||SQLERRM);
  END;
  -- [1881] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1881 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1881 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1881 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1881 '||SQLERRM);
  END;
  -- [1882] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1882 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1882 '||SQLERRM);
  END;
  -- [1883] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1883 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1883 '||SQLERRM);
  END;
  -- [1884] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1884 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1884 '||SQLERRM);
  END;
  -- [1885] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1885 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1885 '||SQLERRM);
  END;
  -- [1886] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1886 IS REF CURSOR;
    rc      t_rc_1886;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1886 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1886 '||SQLERRM);
  END;
  -- [1887] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1887')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1887', 1887, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1887 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1887 '||SQLERRM);
  END;
  -- [1888] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1888 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1888;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1888 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1888 '||SQLERRM);
  END;
  -- [1889] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1889 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1889 '||SQLERRM);
  END;
  -- [1890] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1890 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1890 '||SQLERRM);
  END;
  -- [1891] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1891;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1891;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1891 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1891;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1891 '||SQLERRM);
  END;
  -- [1892] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1892 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1892 '||SQLERRM);
  END;
  -- [1893] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1893 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1893 '||SQLERRM);
  END;
  -- [1894] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1894 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1894 '||SQLERRM);
  END;
  -- [1895] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1895 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1895 '||SQLERRM);
  END;
  -- [1896] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1896 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1896 '||SQLERRM);
  END;
  -- [1897] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1897 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1897 '||SQLERRM);
  END;
  -- [1898] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1898 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1898 '||SQLERRM);
  END;
  -- [1899] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1899 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1899 '||SQLERRM);
  END;
  -- [1900] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1900 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1900 '||SQLERRM);
  END;
  -- [1901] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1901 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1901 '||SQLERRM);
  END;
  -- [1902] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1902 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1902 '||SQLERRM);
  END;
  -- [1903] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1903);
    v_tot := erp_pkg_planner.compute_total(1903);
    v_tot := v_tot + erp_pkg_planner.compute_total(1903, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1903))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1903 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1903 '||SQLERRM);
  END;
  -- [1904] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1904 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1904 '||SQLERRM);
  END;
  -- [1905] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1905 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1905;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1905 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1905 '||SQLERRM);
  END;
  -- [1906] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1906 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1906 '||SQLERRM);
  END;
  -- [1907] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1907;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1907 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1907 '||SQLERRM);
  END;
  -- [1908] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1908 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1908 '||SQLERRM);
  END;
  -- [1909] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1909 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1909 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1909 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1909 '||SQLERRM);
  END;
  -- [1910] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1910 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1910 '||SQLERRM);
  END;
  -- [1911] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1911 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1911 '||SQLERRM);
  END;
  -- [1912] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1912 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1912 '||SQLERRM);
  END;
  -- [1913] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1913 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1913 '||SQLERRM);
  END;
  -- [1914] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1914 IS REF CURSOR;
    rc      t_rc_1914;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1914 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1914 '||SQLERRM);
  END;
  -- [1915] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1915')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1915', 1915, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1915 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1915 '||SQLERRM);
  END;
  -- [1916] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1916 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1916;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1916 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1916 '||SQLERRM);
  END;
  -- [1917] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1917 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1917 '||SQLERRM);
  END;
  -- [1918] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1918 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1918 '||SQLERRM);
  END;
  -- [1919] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1919;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1919;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1919 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1919;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1919 '||SQLERRM);
  END;

  -- exercise DIRECT self-recursion over each active BOM root
  FOR r IN (SELECT product_id FROM erp_bom_header WHERE active_flag = 'Y') LOOP
    erp_usp_explode_bom(r.product_id, 0, 10);
  END LOOP;

  -- exercise MUTUAL / CIRCULAR recursion (walk_down <-> walk_up)
  erp_pkg_planner.walk_down(0, 6);

  -- descend one level deeper in the ERP cycle call chain
  erp_usp_collect_demand(p_run_id);

  erp_usp_write_audit_trail('VALIDATE_DONE','ERP_PRODUCT','master data validated');
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK TO SAVEPOINT sp_validate_master_data_begin;
    erp_usp_write_audit_trail('VALIDATE_ERR','ERP_PRODUCT',SQLERRM);
END erp_usp_validate_master_data;
/

CREATE OR REPLACE PROCEDURE erp_usp_init_mrp_run(
  p_mrp_run_id OUT NUMBER
) IS
  v_run_id  NUMBER;
  v_horizon NUMBER;
  v_cfg_row erp_config%ROWTYPE;
BEGIN
  v_run_id := erp_mrp_seq.NEXTVAL;
  v_horizon := TO_NUMBER(NVL(erp_fn_get_config('MRP_HORIZON_DAYS'),'30'));
  BEGIN
    SELECT * INTO v_cfg_row FROM erp_config WHERE config_key = 'BASE_CURRENCY';
  EXCEPTION WHEN NO_DATA_FOUND THEN NULL; END;
  erp_usp_write_audit_trail('INIT_START','ERP_MRP_RUN','Run '||v_run_id||' horizon '||v_horizon);

  INSERT INTO erp_mrp_run(mrp_run_id, run_date, status, planned_count)
  VALUES (v_run_id, SYSDATE, 'RUNNING', 0);

  DELETE FROM erp_demand WHERE source_type IN ('SALES_ORDER','FORECAST','DEPENDENT');

  p_mrp_run_id := v_run_id;
  SAVEPOINT sp_init_begin;
  -- [1920] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1920 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1920 '||SQLERRM);
  END;
  -- [1921] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1921 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1921 '||SQLERRM);
  END;
  -- [1922] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1922 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1922 '||SQLERRM);
  END;
  -- [1923] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1923 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1923 '||SQLERRM);
  END;
  -- [1924] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1924 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1924 '||SQLERRM);
  END;
  -- [1925] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1925 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1925 '||SQLERRM);
  END;
  -- [1926] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1926 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1926 '||SQLERRM);
  END;
  -- [1927] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1927 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1927 '||SQLERRM);
  END;
  -- [1928] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1928 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1928 '||SQLERRM);
  END;
  -- [1929] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1929 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1929 '||SQLERRM);
  END;
  -- [1930] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1930 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1930 '||SQLERRM);
  END;
  -- [1931] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1931);
    v_tot := erp_pkg_planner.compute_total(1931);
    v_tot := v_tot + erp_pkg_planner.compute_total(1931, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1931))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1931 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1931 '||SQLERRM);
  END;
  -- [1932] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1932 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1932 '||SQLERRM);
  END;
  -- [1933] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1933 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1933;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1933 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1933 '||SQLERRM);
  END;
  -- [1934] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1934 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1934 '||SQLERRM);
  END;
  -- [1935] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1935;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1935 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1935 '||SQLERRM);
  END;
  -- [1936] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1936 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1936 '||SQLERRM);
  END;
  -- [1937] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1937 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1937 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1937 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1937 '||SQLERRM);
  END;
  -- [1938] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1938 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1938 '||SQLERRM);
  END;
  -- [1939] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1939 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1939 '||SQLERRM);
  END;
  -- [1940] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1940 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1940 '||SQLERRM);
  END;
  -- [1941] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1941 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1941 '||SQLERRM);
  END;
  -- [1942] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1942 IS REF CURSOR;
    rc      t_rc_1942;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1942 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1942 '||SQLERRM);
  END;
  -- [1943] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1943')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1943', 1943, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1943 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1943 '||SQLERRM);
  END;
  -- [1944] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1944 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1944;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1944 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1944 '||SQLERRM);
  END;
  -- [1945] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1945 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1945 '||SQLERRM);
  END;
  -- [1946] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1946 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1946 '||SQLERRM);
  END;
  -- [1947] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1947;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1947;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1947 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1947;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1947 '||SQLERRM);
  END;
  -- [1948] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1948 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1948 '||SQLERRM);
  END;
  -- [1949] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1949 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1949 '||SQLERRM);
  END;
  -- [1950] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1950 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1950 '||SQLERRM);
  END;
  -- [1951] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1951 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1951 '||SQLERRM);
  END;
  -- [1952] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1952 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1952 '||SQLERRM);
  END;
  -- [1953] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1953 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1953 '||SQLERRM);
  END;
  -- [1954] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1954 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1954 '||SQLERRM);
  END;
  -- [1955] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1955 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1955 '||SQLERRM);
  END;
  -- [1956] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1956 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1956 '||SQLERRM);
  END;
  -- [1957] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1957 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1957 '||SQLERRM);
  END;
  -- [1958] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1958 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1958 '||SQLERRM);
  END;
  -- [1959] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1959);
    v_tot := erp_pkg_planner.compute_total(1959);
    v_tot := v_tot + erp_pkg_planner.compute_total(1959, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1959))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1959 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1959 '||SQLERRM);
  END;
  -- [1960] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1960 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1960 '||SQLERRM);
  END;
  -- [1961] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1961 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1961;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1961 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1961 '||SQLERRM);
  END;
  -- [1962] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1962 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1962 '||SQLERRM);
  END;
  -- [1963] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1963;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1963 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1963 '||SQLERRM);
  END;
  -- [1964] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1964 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1964 '||SQLERRM);
  END;
  -- [1965] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1965 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1965 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1965 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1965 '||SQLERRM);
  END;
  -- [1966] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1966 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1966 '||SQLERRM);
  END;
  -- [1967] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1967 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1967 '||SQLERRM);
  END;
  -- [1968] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1968 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1968 '||SQLERRM);
  END;
  -- [1969] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1969 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1969 '||SQLERRM);
  END;
  -- [1970] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1970 IS REF CURSOR;
    rc      t_rc_1970;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1970 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1970 '||SQLERRM);
  END;
  -- [1971] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1971')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1971', 1971, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1971 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1971 '||SQLERRM);
  END;
  -- [1972] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_1972 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_1972;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk1972 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk1972 '||SQLERRM);
  END;
  -- [1973] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk1973 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk1973 '||SQLERRM);
  END;
  -- [1974] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk1974 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk1974 '||SQLERRM);
  END;
  -- [1975] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_1975;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_1975;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk1975 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_1975;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk1975 '||SQLERRM);
  END;
  -- [1976] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk1976 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk1976 '||SQLERRM);
  END;
  -- [1977] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk1977 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk1977 '||SQLERRM);
  END;
  -- [1978] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk1978 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk1978 '||SQLERRM);
  END;
  -- [1979] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk1979 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk1979 '||SQLERRM);
  END;
  -- [1980] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk1980 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk1980 '||SQLERRM);
  END;
  -- [1981] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk1981 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk1981 '||SQLERRM);
  END;
  -- [1982] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk1982 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk1982 '||SQLERRM);
  END;
  -- [1983] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk1983 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk1983 '||SQLERRM);
  END;
  -- [1984] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk1984 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk1984 '||SQLERRM);
  END;
  -- [1985] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk1985 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk1985 '||SQLERRM);
  END;
  -- [1986] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk1986 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk1986 '||SQLERRM);
  END;
  -- [1987] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 1987);
    v_tot := erp_pkg_planner.compute_total(1987);
    v_tot := v_tot + erp_pkg_planner.compute_total(1987, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(1987))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk1987 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk1987 '||SQLERRM);
  END;
  -- [1988] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk1988 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk1988 '||SQLERRM);
  END;
  -- [1989] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_1989 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_1989;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk1989 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk1989 '||SQLERRM);
  END;
  -- [1990] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk1990 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk1990 '||SQLERRM);
  END;
  -- [1991] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 1991;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk1991 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk1991 '||SQLERRM);
  END;

  -- descend one level deeper in the ERP cycle call chain
  erp_usp_validate_master_data(v_run_id);

  UPDATE erp_mrp_run SET status = 'PLANNED' WHERE mrp_run_id = v_run_id;
  erp_usp_write_audit_trail('INIT_DONE','ERP_MRP_RUN','mrp run initialized '||v_run_id);
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK TO SAVEPOINT sp_init_begin;
    erp_usp_write_audit_trail('INIT_ERR','ERP_MRP_RUN',SQLERRM);
    RAISE;
END erp_usp_init_mrp_run;
/

-- ---------------------------------------------------------------- analytics helper procs
CREATE OR REPLACE PROCEDURE erp_usp_recost_products IS
  v_n NUMBER := 0;
BEGIN
  FOR r IN (
    SELECT bh.product_id, SUM(bc.qty_per * NVL(cp.std_cost,0)) AS rolled
    FROM erp_bom_header bh
    JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
    JOIN erp_product cp       ON cp.product_id = bc.component_id
    WHERE bh.active_flag = 'Y'
    GROUP BY bh.product_id
  ) LOOP
    UPDATE erp_product SET std_cost = GREATEST(NVL(std_cost,0), r.rolled)
     WHERE product_id = r.product_id;
    v_n := v_n + 1;
  END LOOP;
  erp_usp_write_audit_trail('RECOST','ERP_PRODUCT','recosted products='||v_n);
EXCEPTION
  WHEN OTHERS THEN
    erp_usp_write_audit_trail('RECOST_ERR','ERP_PRODUCT',SQLERRM);
END erp_usp_recost_products;
/

CREATE OR REPLACE PROCEDURE erp_usp_snapshot_inventory IS
  TYPE t_pid IS TABLE OF erp_product.product_id%TYPE;
  l_pids t_pid;
  v_val NUMBER := 0;
BEGIN
  SELECT product_id BULK COLLECT INTO l_pids FROM erp_product ORDER BY product_id;
  FOR i IN 1 .. l_pids.COUNT LOOP
    v_val := v_val + erp_fn_onhand_qty(l_pids(i)) * erp_fn_demand_qty(l_pids(i));
  END LOOP;
  erp_usp_write_audit_trail('INV_SNAP','ERP_INVENTORY','snapshot weight='||v_val||' items='||l_pids.COUNT);
EXCEPTION
  WHEN OTHERS THEN
    erp_usp_write_audit_trail('INV_SNAP_ERR','ERP_INVENTORY',SQLERRM);
END erp_usp_snapshot_inventory;
/

CREATE OR REPLACE PROCEDURE erp_usp_reconcile_suppliers IS
  v_tot NUMBER := 0;
BEGIN
  FOR r IN (SELECT supplier_id FROM erp_supplier) LOOP
    v_tot := v_tot + erp_fn_supplier_spend(r.supplier_id);
  END LOOP;
  erp_usp_write_audit_trail('SUP_RECON','ERP_SUPPLIER','total spend='||v_tot);
EXCEPTION
  WHEN OTHERS THEN
    erp_usp_write_audit_trail('SUP_RECON_ERR','ERP_SUPPLIER',SQLERRM);
END erp_usp_reconcile_suppliers;
/

CREATE OR REPLACE PROCEDURE erp_usp_evaluate_work_orders IS
  v_pct NUMBER := 0; v_n NUMBER := 0;
BEGIN
  FOR r IN (SELECT wo_id FROM erp_work_order) LOOP
    v_pct := v_pct + erp_fn_wo_completion_pct(r.wo_id);
    v_n := v_n + 1;
  END LOOP;
  IF v_n > 0 THEN
    erp_usp_write_audit_trail('WO_EVAL','ERP_WORK_ORDER','avg pct='||ROUND(v_pct/v_n,2));
  END IF;
EXCEPTION
  WHEN OTHERS THEN
    erp_usp_write_audit_trail('WO_EVAL_ERR','ERP_WORK_ORDER',SQLERRM);
END erp_usp_evaluate_work_orders;
/

CREATE OR REPLACE PROCEDURE erp_usp_check_receivables IS
  v_open NUMBER := 0;
BEGIN
  FOR r IN (SELECT customer_id FROM erp_customer) LOOP
    v_open := v_open + erp_fn_open_invoice_total(r.customer_id);
  END LOOP;
  erp_usp_write_audit_trail('AR_CHECK','ERP_INVOICE','open receivables='||v_open);
EXCEPTION
  WHEN OTHERS THEN
    erp_usp_write_audit_trail('AR_CHECK_ERR','ERP_INVOICE',SQLERRM);
END erp_usp_check_receivables;
/

CREATE OR REPLACE PROCEDURE erp_usp_close_gl_period IS
  v_asset  NUMBER := 0;
  v_rev    NUMBER := 0;
  v_exp    NUMBER := 0;
BEGIN
  SAVEPOINT sp_close_period;
  v_asset := erp_fn_gl_balance('ASSET');
  v_rev   := erp_fn_gl_balance('REVENUE');
  v_exp   := erp_fn_gl_balance('EXPENSE');
  UPDATE erp_gl_batch SET status = 'REVIEWED' WHERE status = 'OPEN';
  erp_usp_write_audit_trail('GL_CLOSE','ERP_GL_BATCH',
    'asset='||v_asset||' rev='||v_rev||' exp='||v_exp);
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK TO SAVEPOINT sp_close_period;
    erp_usp_write_audit_trail('GL_CLOSE_ERR','ERP_GL_BATCH',SQLERRM);
END erp_usp_close_gl_period;
/

CREATE OR REPLACE PROCEDURE erp_usp_flag_shortages IS
  v_short NUMBER := 0;
BEGIN
  FOR r IN (SELECT product_id FROM erp_product WHERE make_buy IN ('M','B')) LOOP
    IF erp_fn_net_requirement(r.product_id) > 0 THEN
      v_short := v_short + 1;
    END IF;
  END LOOP;
  erp_usp_write_audit_trail('SHORTAGE','ERP_PRODUCT','short items='||v_short);
EXCEPTION
  WHEN OTHERS THEN
    erp_usp_write_audit_trail('SHORTAGE_ERR','ERP_PRODUCT',SQLERRM);
END erp_usp_flag_shortages;
/

CREATE OR REPLACE PROCEDURE erp_usp_refresh_price_variance IS
  v_var NUMBER := 0;
BEGIN
  FOR r IN (
    SELECT p.product_id, ABS(NVL(pp.unit_price,0) - NVL(p.list_price,0)) AS d
    FROM erp_product p
    JOIN erp_product_price pp ON pp.product_id = p.product_id
    JOIN erp_price_list pl    ON pl.price_list_id = pp.price_list_id
    WHERE pl.active_flag = 'Y'
  ) LOOP
    v_var := v_var + r.d;
  END LOOP;
  erp_usp_write_audit_trail('PRICE_REFRESH','ERP_PRODUCT_PRICE','variance='||v_var);
EXCEPTION
  WHEN OTHERS THEN
    erp_usp_write_audit_trail('PRICE_REFRESH_ERR','ERP_PRODUCT_PRICE',SQLERRM);
END erp_usp_refresh_price_variance;
/

CREATE OR REPLACE PROCEDURE erp_usp_run_analytics(
  p_run_id IN NUMBER
) IS
BEGIN
  erp_usp_write_audit_trail('ANALYTICS','ERP_ANALYTICS','enter erp_usp_run_analytics run '||p_run_id);
  erp_usp_recost_products;
  erp_usp_snapshot_inventory;
  erp_usp_reconcile_suppliers;
  erp_usp_evaluate_work_orders;
  erp_usp_check_receivables;
  erp_usp_close_gl_period;
  erp_usp_flag_shortages;
  erp_usp_refresh_price_variance;
  erp_usp_write_audit_trail('ANALYTICS_DONE','ERP_ANALYTICS','analytics complete run '||p_run_id);
EXCEPTION
  WHEN OTHERS THEN
    erp_usp_write_audit_trail('ANALYTICS_ERR','ERP_ANALYTICS',SQLERRM);
END erp_usp_run_analytics;
/

-- ---------------------------------------------------------------- view/mv touch proc
CREATE OR REPLACE PROCEDURE erp_usp_touch_views IS
  v_n NUMBER := 0;
BEGIN
  SELECT COUNT(*) INTO v_n FROM erp_v_order_summary;
  erp_usp_write_audit_trail('TOUCH_VIEW','ERP_V_ORDER_SUMMARY','rows='||v_n);
  SELECT COUNT(*) INTO v_n FROM erp_v_inventory_value;
  erp_usp_write_audit_trail('TOUCH_VIEW','ERP_V_INVENTORY_VALUE','rows='||v_n);
  SELECT COUNT(*) INTO v_n FROM erp_v_gl_trial_balance;
  erp_usp_write_audit_trail('TOUCH_VIEW','ERP_V_GL_TRIAL_BALANCE','rows='||v_n);
  SELECT COUNT(*) INTO v_n FROM erp_mv_product_sales;
  erp_usp_write_audit_trail('TOUCH_VIEW','ERP_MV_PRODUCT_SALES','rows='||v_n);
  SELECT COUNT(*) INTO v_n FROM erp_mv_demand_by_prod;
  erp_usp_write_audit_trail('TOUCH_VIEW','ERP_MV_DEMAND_BY_PROD','rows='||v_n);
EXCEPTION
  WHEN OTHERS THEN
    erp_usp_write_audit_trail('TOUCH_VIEW_ERR','ERP_VIEWS',SQLERRM);
END erp_usp_touch_views;
/

-- ------------------------------------------------------ cross-schema touch proc
-- References objects owned by the SECOND schema/user ANALYZER_REF (see
-- db-setup/seed/oracle_ref.sql). Requires the cross-schema grants there.
CREATE OR REPLACE PROCEDURE erp_usp_xschema_touch IS
  v_cnt    NUMBER := 0;
  v_active NUMBER := 0;
  v_name   VARCHAR2(60);
  v_id     NUMBER;
BEGIN
  -- (a) cross-schema CALL into ANALYZER_REF procedure
  ANALYZER_REF.USP_REF_LOG('XSCHEMA_TOUCH', 'ANALYZER entered cross-schema touch');

  -- (b) cross-schema reads: base table + view + function
  SELECT COUNT(*) INTO v_cnt   FROM ANALYZER_REF.REF_REGION;
  SELECT COUNT(*) INTO v_active FROM ANALYZER_REF.VW_REGION;
  v_name := ANALYZER_REF.FN_REGION_NAME(1);
  erp_usp_write_audit_trail('XSCHEMA_READ','ANALYZER_REF.REF_REGION',
    'regions='||v_cnt||' active='||v_active||' first='||v_name);

  -- (c) cross-schema INSERT (fires ANALYZER_REF.TRG_REF_REGION)
  v_id := 900 + MOD(TRUNC(DBMS_UTILITY.GET_TIME), 100);
  BEGIN
    INSERT INTO ANALYZER_REF.REF_REGION (region_id, region_name, active)
    VALUES (v_id, 'X-'||v_id, 1);
  EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN NULL;
  END;

  ANALYZER_REF.USP_REF_LOG('XSCHEMA_DONE', 'ANALYZER finished cross-schema touch');
EXCEPTION
  WHEN OTHERS THEN
    erp_usp_write_audit_trail('XSCHEMA_ERR','ANALYZER_REF',SQLERRM);
END erp_usp_xschema_touch;
/

-- ---------------------------------------------------------------- ENTRY PROCEDURE
CREATE OR REPLACE PROCEDURE erp_usp_run_cycle IS
  v_run_id  NUMBER;
  v_orders  NUMBER := 0;
  v_revenue NUMBER := 0;
  v_rc      SYS_REFCURSOR;
  v_pid     NUMBER;
  v_sku     erp_product.sku%TYPE;
  v_qty     NUMBER;
  v_need    DATE;
BEGIN
  erp_usp_write_audit_trail('CYCLE_START','ERP_RUN_CYCLE','Starting full ERP cycle');

  -- read every view + materialized view so they enter the reachable call-tree
  erp_usp_touch_views;

  -- cross-schema touch: reaches objects owned by ANALYZER_REF
  erp_usp_xschema_touch;

  -- kick off the deep planning chain (depth 12 from here)
  erp_usp_init_mrp_run(v_run_id);

  BEGIN
    DBMS_MVIEW.REFRESH('ERP_MV_PRODUCT_SALES','C');
    DBMS_MVIEW.REFRESH('ERP_MV_DEMAND_BY_PROD','C');
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MVIEW_WARN','ERP_MV_PRODUCT_SALES',SQLERRM);
  END;

  SELECT COUNT(DISTINCT so.order_id), NVL(SUM(sol.line_amount),0)
    INTO v_orders, v_revenue
  FROM erp_sales_order so
  JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
  JOIN erp_customer c ON c.customer_id = so.customer_id;

  UPDATE erp_mrp_run SET status = 'DONE' WHERE mrp_run_id = v_run_id;

  BEGIN
    erp_pkg_planner.open_demand_cursor(v_rc);
    LOOP
      FETCH v_rc INTO v_pid, v_sku, v_qty, v_need;
      EXIT WHEN v_rc%NOTFOUND;
      NULL;
    END LOOP;
    CLOSE v_rc;
  EXCEPTION
    WHEN OTHERS THEN
      IF v_rc%ISOPEN THEN CLOSE v_rc; END IF;
      erp_usp_write_audit_trail('REFCUR_WARN','ERP_DEMAND',SQLERRM);
  END;
  -- [1992] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk1992 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk1992 '||SQLERRM);
  END;
  -- [1993] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_1993 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_1993 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk1993 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk1993 '||SQLERRM);
  END;
  -- [1994] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk1994 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk1994 '||SQLERRM);
  END;
  -- [1995] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk1995 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk1995 '||SQLERRM);
  END;
  -- [1996] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk1996 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk1996 '||SQLERRM);
  END;
  -- [1997] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk1997 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk1997 '||SQLERRM);
  END;
  -- [1998] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_1998 IS REF CURSOR;
    rc      t_rc_1998;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk1998 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk1998 '||SQLERRM);
  END;
  -- [1999] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_1999')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_1999', 1999, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk1999 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk1999 '||SQLERRM);
  END;
  -- [2000] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_2000 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_2000;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk2000 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk2000 '||SQLERRM);
  END;
  -- [2001] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk2001 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk2001 '||SQLERRM);
  END;
  -- [2002] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk2002 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk2002 '||SQLERRM);
  END;
  -- [2003] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_2003;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_2003;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk2003 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_2003;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk2003 '||SQLERRM);
  END;
  -- [2004] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk2004 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk2004 '||SQLERRM);
  END;
  -- [2005] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk2005 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk2005 '||SQLERRM);
  END;
  -- [2006] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk2006 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk2006 '||SQLERRM);
  END;
  -- [2007] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk2007 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk2007 '||SQLERRM);
  END;
  -- [2008] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk2008 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk2008 '||SQLERRM);
  END;
  -- [2009] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk2009 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk2009 '||SQLERRM);
  END;
  -- [2010] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk2010 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk2010 '||SQLERRM);
  END;
  -- [2011] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk2011 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk2011 '||SQLERRM);
  END;
  -- [2012] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk2012 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk2012 '||SQLERRM);
  END;
  -- [2013] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk2013 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk2013 '||SQLERRM);
  END;
  -- [2014] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk2014 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk2014 '||SQLERRM);
  END;
  -- [2015] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 2015);
    v_tot := erp_pkg_planner.compute_total(2015);
    v_tot := v_tot + erp_pkg_planner.compute_total(2015, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(2015))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk2015 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk2015 '||SQLERRM);
  END;
  -- [2016] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk2016 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk2016 '||SQLERRM);
  END;
  -- [2017] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_2017 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_2017;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk2017 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk2017 '||SQLERRM);
  END;
  -- [2018] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk2018 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk2018 '||SQLERRM);
  END;
  -- [2019] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 2019;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk2019 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk2019 '||SQLERRM);
  END;
  -- [2020] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk2020 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk2020 '||SQLERRM);
  END;
  -- [2021] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_2021 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_2021 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk2021 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk2021 '||SQLERRM);
  END;
  -- [2022] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk2022 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk2022 '||SQLERRM);
  END;
  -- [2023] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk2023 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk2023 '||SQLERRM);
  END;
  -- [2024] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk2024 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk2024 '||SQLERRM);
  END;
  -- [2025] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk2025 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk2025 '||SQLERRM);
  END;
  -- [2026] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_2026 IS REF CURSOR;
    rc      t_rc_2026;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk2026 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk2026 '||SQLERRM);
  END;
  -- [2027] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_2027')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_2027', 2027, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk2027 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk2027 '||SQLERRM);
  END;
  -- [2028] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_2028 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_2028;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk2028 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk2028 '||SQLERRM);
  END;
  -- [2029] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk2029 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk2029 '||SQLERRM);
  END;
  -- [2030] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk2030 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk2030 '||SQLERRM);
  END;
  -- [2031] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_2031;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_2031;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk2031 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_2031;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk2031 '||SQLERRM);
  END;
  -- [2032] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk2032 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk2032 '||SQLERRM);
  END;
  -- [2033] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk2033 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk2033 '||SQLERRM);
  END;
  -- [2034] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk2034 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk2034 '||SQLERRM);
  END;
  -- [2035] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk2035 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk2035 '||SQLERRM);
  END;
  -- [2036] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk2036 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk2036 '||SQLERRM);
  END;
  -- [2037] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk2037 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk2037 '||SQLERRM);
  END;
  -- [2038] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk2038 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk2038 '||SQLERRM);
  END;
  -- [2039] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk2039 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk2039 '||SQLERRM);
  END;
  -- [2040] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk2040 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk2040 '||SQLERRM);
  END;
  -- [2041] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk2041 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk2041 '||SQLERRM);
  END;
  -- [2042] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk2042 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk2042 '||SQLERRM);
  END;
  -- [2043] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 2043);
    v_tot := erp_pkg_planner.compute_total(2043);
    v_tot := v_tot + erp_pkg_planner.compute_total(2043, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(2043))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk2043 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk2043 '||SQLERRM);
  END;
  -- [2044] material issue variance (material_issue+work_order+product)
  DECLARE
    v_iss NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT mi.component_id, mi.qty_issued, wo.qty_planned, p.sku
      FROM erp_material_issue mi
      JOIN erp_work_order wo ON wo.wo_id = mi.wo_id
      JOIN erp_product p     ON p.product_id = mi.component_id
    ) LOOP
      v_iss := v_iss + NVL(r.qty_issued,0);
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('MAT_ISS','ERP_MATERIAL_ISSUE',
      'blk2044 issued='||v_iss||' rows='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MAT_ISS_ERR','ERP_MATERIAL_ISSUE','blk2044 '||SQLERRM);
  END;
  -- [2045] exchange rate refresh (exchange_rate+currency, BULK COLLECT + FORALL)
  DECLARE
    TYPE t_cid_2045 IS TABLE OF erp_currency.currency_id%TYPE;
    l_cids t_cid_2045;
    v_rate NUMBER;
  BEGIN
    SELECT currency_id BULK COLLECT INTO l_cids FROM erp_currency;
    FORALL i IN 1 .. l_cids.COUNT
      UPDATE erp_currency SET last_seen = SYSDATE WHERE currency_id = l_cids(i);
    FOR i IN 1 .. l_cids.COUNT LOOP
      v_rate := erp_fn_fx_rate(l_cids(i));
      NULL;
    END LOOP;
    erp_usp_write_audit_trail('FX_REFRESH','ERP_EXCHANGE_RATE',
      'blk2045 currencies='||l_cids.COUNT||' lastrate='||NVL(v_rate,1));
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FX_REFRESH_ERR','ERP_EXCHANGE_RATE','blk2045 '||SQLERRM);
  END;
  -- [2046] invoice aging (invoice+invoice_line+customer, LEFT JOIN, correlated subquery)
  DECLARE
    v_open NUMBER := 0;
    v_cnt  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT inv.invoice_id, inv.amount, inv.status, c.customer_name,
             SYSDATE - inv.invoice_date AS age,
             (SELECT NVL(SUM(il.amount),0) FROM erp_invoice_line il
               WHERE il.invoice_id = inv.invoice_id) AS line_total
      FROM erp_invoice inv
      JOIN erp_customer c ON c.customer_id = inv.customer_id
      LEFT JOIN erp_invoice_line il ON il.invoice_id = inv.invoice_id
      WHERE inv.status = 'OPEN'
    ) LOOP
      v_open := v_open + NVL(r.amount,0);
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('INV_AGING','ERP_INVOICE',
      'blk2046 open='||v_open||' cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_AGING_ERR','ERP_INVOICE','blk2046 '||SQLERRM);
  END;
  -- [2047] demand horizon summary (demand+product, dynamic WHERE via EXECUTE IMMEDIATE + USING)
  DECLARE
    v_sql VARCHAR2(600);
    v_qty NUMBER := 0;
    v_h   NUMBER := 30 + 2047;
  BEGIN
    v_sql := 'SELECT NVL(SUM(d.qty),0) FROM erp_demand d '||
             'JOIN erp_product p ON p.product_id = d.product_id '||
             'WHERE d.need_date <= SYSDATE + :h';
    EXECUTE IMMEDIATE v_sql INTO v_qty USING v_h;
    erp_usp_write_audit_trail('DMD_HORIZON','ERP_DEMAND',
      'blk2047 horizon_qty='||v_qty);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DMD_HORIZON_ERR','ERP_DEMAND','blk2047 '||SQLERRM);
  END;
  -- [2048] MRP plan action tally (mrp_plan+mrp_run+product, CASE, IF/ELSIF)
  DECLARE
    v_make NUMBER := 0;
    v_buy  NUMBER := 0;
    v_lbl  VARCHAR2(20);
  BEGIN
    FOR r IN (
      SELECT mp.action, COUNT(*) c
      FROM erp_mrp_plan mp
      JOIN erp_mrp_run mr ON mr.mrp_run_id = mp.mrp_run_id
      JOIN erp_product p  ON p.product_id = mp.product_id
      GROUP BY mp.action
    ) LOOP
      CASE r.action
        WHEN 'MAKE' THEN v_make := v_make + r.c; v_lbl := 'M';
        WHEN 'BUY'  THEN v_buy  := v_buy  + r.c; v_lbl := 'B';
        ELSE v_lbl := 'X';
      END CASE;
      IF v_lbl = 'X' THEN NULL; END IF;
    END LOOP;
    erp_usp_write_audit_trail('MRP_TALLY','ERP_MRP_PLAN',
      'blk2048 make='||v_make||' buy='||v_buy);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('MRP_TALLY_ERR','ERP_MRP_PLAN','blk2048 '||SQLERRM);
  END;
  -- [2049] inventory valuation scan (inventory+product+warehouse, explicit CURSOR, fn_onhand_qty)
  DECLARE
    v_val    NUMBER := 0;
    v_cnt    NUMBER := 0;
    v_avail  NUMBER := 0;
    CURSOR c_inv_2049 IS
      SELECT i.product_id, i.qty_on_hand, i.qty_reserved, p.std_cost, w.wh_code
      FROM erp_inventory i
      JOIN erp_product p   ON p.product_id = i.product_id
      JOIN erp_warehouse w ON w.warehouse_id = i.warehouse_id
      WHERE i.qty_on_hand >= 0;
  BEGIN
    FOR r IN c_inv_2049 LOOP
      v_val   := v_val + (r.qty_on_hand - NVL(r.qty_reserved,0)) * NVL(r.std_cost,0);
      v_avail := erp_fn_onhand_qty(r.product_id);
      v_cnt   := v_cnt + 1;
    END LOOP;
    IF v_cnt > 0 THEN
      erp_usp_write_audit_trail('INV_VAL','ERP_INVENTORY',
        'blk2049 valuation='||v_val||' rows='||v_cnt||' avail='||v_avail);
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('INV_VAL_ERR','ERP_INVENTORY','blk2049 '||SQLERRM);
  END;
  -- [2050] customer revenue aggregation (order+line+customer+currency, fn_fx_rate, COALESCE)
  DECLARE
    v_rev  NUMBER := 0;
    v_base NUMBER := 0;
    v_rate NUMBER := 1;
  BEGIN
    FOR r IN (
      SELECT c.customer_id, c.currency_id,
             COALESCE(SUM(sol.line_amount),0) AS rev
      FROM erp_customer c
      JOIN erp_sales_order so       ON so.customer_id = c.customer_id
      JOIN erp_sales_order_line sol ON sol.order_id = so.order_id
      JOIN erp_currency cur         ON cur.currency_id = c.currency_id
      GROUP BY c.customer_id, c.currency_id
    ) LOOP
      v_rate := erp_fn_fx_rate(r.currency_id);
      v_rev  := v_rev + r.rev;
      v_base := v_base + r.rev * NVL(v_rate,1);
    END LOOP;
    erp_usp_write_audit_trail('CUST_REV','ERP_SALES_ORDER',
      'blk2050 rev='||v_rev||' base='||v_base);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CUST_REV_ERR','ERP_SALES_ORDER','blk2050 '||SQLERRM);
  END;
  -- [2051] BOM component rollup (bom_header+bom_component+product, fn_bom_component_count)
  DECLARE
    v_qty  NUMBER := 0;
    v_comp NUMBER := 0;
    v_bom  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT bh.product_id, bc.component_id, bc.qty_per, cp.std_cost
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      JOIN erp_product cp       ON cp.product_id = bc.component_id
      WHERE bh.active_flag = 'Y'
    ) LOOP
      v_qty := v_qty + r.qty_per * NVL(r.std_cost,0);
      v_bom := erp_fn_bom_component_count(r.product_id);
      v_comp := v_comp + 1;
    END LOOP;
    erp_usp_write_audit_trail('BOM_ROLL','ERP_BOM_COMPONENT',
      'blk2051 extcost='||v_qty||' comps='||v_comp||' bomcnt='||v_bom);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_ROLL_ERR','ERP_BOM_COMPONENT','blk2051 '||SQLERRM);
  END;
  -- [2052] dynamic SQL aggregate over table set (EXECUTE IMMEDIATE, collection iteration)
  DECLARE
    v_sql VARCHAR2(1000);
    v_cnt NUMBER := 0;
    v_tab VARCHAR2(60);
  BEGIN
    FOR t IN (SELECT column_value AS tn FROM TABLE(SYS.ODCIVARCHAR2LIST(
                'ERP_SALES_ORDER','ERP_PURCHASE_ORDER','ERP_WORK_ORDER','ERP_INVOICE'))) LOOP
      v_tab := t.tn;
      v_sql := 'SELECT COUNT(*) FROM '||v_tab;
      EXECUTE IMMEDIATE v_sql INTO v_cnt;
      erp_usp_write_audit_trail('DYN_CNT',v_tab,'blk2052 rows='||v_cnt);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('DYN_CNT_ERR','ERP_CONFIG','blk2052 '||SQLERRM);
  END;
  -- [2053] DBMS_SQL dynamic fetch (parse/define/execute/fetch/close)
  DECLARE
    v_cur  INTEGER;
    v_rows INTEGER;
    v_pid  NUMBER;
    v_sum  NUMBER := 0;
  BEGIN
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur,
      'SELECT product_id FROM erp_product WHERE make_buy = ''M'' ORDER BY product_id',
      DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cur, 1, v_pid);
    v_rows := DBMS_SQL.EXECUTE(v_cur);
    LOOP
      EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cur) = 0;
      DBMS_SQL.COLUMN_VALUE(v_cur, 1, v_pid);
      v_sum := v_sum + NVL(v_pid,0);
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cur);
    erp_usp_write_audit_trail('DBMS_SQL','ERP_PRODUCT','blk2053 pidsum='||v_sum);
  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cur) THEN DBMS_SQL.CLOSE_CURSOR(v_cur); END IF;
      erp_usp_write_audit_trail('DBMS_SQL_ERR','ERP_PRODUCT','blk2053 '||SQLERRM);
  END;
  -- [2054] REF CURSOR (OPEN-FOR dynamic) demand scan
  DECLARE
    TYPE t_rc_2054 IS REF CURSOR;
    rc      t_rc_2054;
    v_pid   NUMBER;
    v_qty   NUMBER;
    v_tot   NUMBER := 0;
  BEGIN
    OPEN rc FOR
      'SELECT product_id, qty FROM erp_demand WHERE qty > :m'
      USING 0;
    LOOP
      FETCH rc INTO v_pid, v_qty;
      EXIT WHEN rc%NOTFOUND;
      v_tot := v_tot + NVL(v_qty,0);
    END LOOP;
    CLOSE rc;
    erp_usp_write_audit_trail('REFCUR','ERP_DEMAND','blk2054 tot='||v_tot);
  EXCEPTION
    WHEN OTHERS THEN
      IF rc%ISOPEN THEN CLOSE rc; END IF;
      erp_usp_write_audit_trail('REFCUR_ERR','ERP_DEMAND','blk2054 '||SQLERRM);
  END;
  -- [2055] MERGE forecast into demand staging (MERGE + inline view + SEQUENCE)
  DECLARE
    v_merged NUMBER := 0;
  BEGIN
    MERGE INTO erp_demand d
    USING (
      SELECT f.product_id, SUM(f.qty) AS qty
      FROM erp_forecast f
      JOIN erp_product p ON p.product_id = f.product_id
      GROUP BY f.product_id
    ) src
    ON (d.product_id = src.product_id AND d.source_type = 'FCST_MRG_2055')
    WHEN MATCHED THEN UPDATE SET d.qty = src.qty
    WHEN NOT MATCHED THEN
      INSERT (demand_id, product_id, source_type, source_id, qty, need_date)
      VALUES (erp_mrp_seq.NEXTVAL, src.product_id, 'FCST_MRG_2055', 2055, src.qty, SYSDATE+30);
    v_merged := SQL%ROWCOUNT;
    erp_usp_write_audit_trail('FCST_MERGE','ERP_DEMAND','blk2055 merged='||v_merged);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FCST_MERGE_ERR','ERP_DEMAND','blk2055 '||SQLERRM);
  END;
  -- [2056] BULK COLLECT net-requirement scan (fn_net_requirement + fn_gross_requirement)
  DECLARE
    TYPE t_pid_2056 IS TABLE OF erp_product.product_id%TYPE;
    l_pids t_pid_2056;
    v_net   NUMBER := 0;
    v_gross NUMBER := 0;
  BEGIN
    SELECT product_id BULK COLLECT INTO l_pids
    FROM erp_product WHERE make_buy IN ('M','B');
    FOR i IN 1 .. l_pids.COUNT LOOP
      v_gross := v_gross + erp_fn_gross_requirement(l_pids(i));
      v_net   := v_net   + erp_fn_net_requirement(l_pids(i));
    END LOOP;
    erp_usp_write_audit_trail('NETREQ_SCAN','ERP_PRODUCT',
      'blk2056 gross='||v_gross||' net='||v_net||' items='||l_pids.COUNT);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NETREQ_ERR','ERP_PRODUCT','blk2056 '||SQLERRM);
  END;
  -- [2057] purchasing analysis (purchase_order+line+supplier+product)
  DECLARE
    v_spend NUMBER := 0;
    v_lines NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT s.supplier_id, s.supplier_name, pol.qty, pol.unit_cost, p.sku
      FROM erp_purchase_order po
      JOIN erp_purchase_order_line pol ON pol.po_id = po.po_id
      JOIN erp_supplier s              ON s.supplier_id = po.supplier_id
      JOIN erp_product p               ON p.product_id = pol.product_id
      WHERE po.status <> 'CANCELLED'
    ) LOOP
      v_spend := v_spend + NVL(r.qty,0) * NVL(r.unit_cost,0);
      v_lines := v_lines + 1;
    END LOOP;
    erp_usp_write_audit_trail('PO_ANALYSIS','ERP_PURCHASE_ORDER',
      'blk2057 spend='||v_spend||' lines='||v_lines);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PO_ANALYSIS_ERR','ERP_PURCHASE_ORDER','blk2057 '||SQLERRM);
  END;
  -- [2058] routing capacity check (routing+operation+work_center+product)
  DECLARE
    v_load NUMBER := 0;
    v_cap  NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT rt.product_id, ro.run_time_min, wc.capacity_hrs, wc.wc_code
      FROM erp_routing rt
      JOIN erp_routing_operation ro ON ro.routing_id = rt.routing_id
      JOIN erp_work_center wc       ON wc.work_center_id = ro.work_center_id
      JOIN erp_product p            ON p.product_id = rt.product_id
    ) LOOP
      v_load := v_load + NVL(r.run_time_min,0);
      v_cap  := v_cap + NVL(r.capacity_hrs,0) * 60;
    END LOOP;
    erp_usp_write_audit_trail('CAP_CHECK','ERP_WORK_CENTER',
      'blk2058 loadmin='||v_load||' capmin='||v_cap);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('CAP_CHECK_ERR','ERP_WORK_CENTER','blk2058 '||SQLERRM);
  END;
  -- [2059] order total reconciliation (sales_order + fn_order_total, UPDATE, SAVEPOINT)
  DECLARE
    v_stored NUMBER := 0;
    v_calc   NUMBER := 0;
    v_diff   NUMBER := 0;
  BEGIN
    SAVEPOINT sp_recon_2059;
    FOR r IN (SELECT order_id, total_amount FROM erp_sales_order) LOOP
      v_calc := erp_fn_order_total(r.order_id);
      IF NVL(r.total_amount,0) <> v_calc THEN
        UPDATE erp_sales_order SET total_amount = v_calc WHERE order_id = r.order_id;
        v_diff := v_diff + 1;
      END IF;
      v_stored := v_stored + v_calc;
    END LOOP;
    IF v_diff > 100000 THEN
      ROLLBACK TO SAVEPOINT sp_recon_2059;
    END IF;
    erp_usp_write_audit_trail('ORD_RECON','ERP_SALES_ORDER',
      'blk2059 total='||v_stored||' fixed='||v_diff);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT sp_recon_2059;
      erp_usp_write_audit_trail('ORD_RECON_ERR','ERP_SALES_ORDER','blk2059 '||SQLERRM);
  END;
  -- [2060] safety stock evaluation (inventory + fn_safety_stock + fn_get_config)
  DECLARE
    v_pct   VARCHAR2(20);
    v_below NUMBER := 0;
    v_safe  NUMBER := 0;
  BEGIN
    v_pct := erp_fn_get_config('SAFETY_STOCK_PCT');
    FOR r IN (SELECT product_id, qty_on_hand FROM erp_inventory) LOOP
      v_safe := erp_fn_safety_stock(r.product_id);
      IF r.qty_on_hand < v_safe THEN
        v_below := v_below + 1;
      END IF;
    END LOOP;
    erp_usp_write_audit_trail('SAFETY_EVAL','ERP_INVENTORY',
      'blk2060 pct='||NVL(v_pct,'?')||' below='||v_below);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SAFETY_EVAL_ERR','ERP_INVENTORY','blk2060 '||SQLERRM);
  END;
  -- [2061] GL trial balance scan (gl_entry+gl_account, GROUP BY, analytic guard)
  DECLARE
    v_dr NUMBER := 0;
    v_cr NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT a.account_type, SUM(e.debit) dr, SUM(e.credit) cr,
             RANK() OVER (ORDER BY SUM(e.debit) DESC) rnk
      FROM erp_gl_entry e
      JOIN erp_gl_account a ON a.account_id = e.account_id
      GROUP BY a.account_type
    ) LOOP
      v_dr := v_dr + NVL(r.dr,0);
      v_cr := v_cr + NVL(r.cr,0);
    END LOOP;
    erp_usp_write_audit_trail('GL_TB','ERP_GL_ENTRY',
      'blk2061 debit='||v_dr||' credit='||v_cr);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GL_TB_ERR','ERP_GL_ENTRY','blk2061 '||SQLERRM);
  END;
  -- [2062] shipment fulfilment ratio (shipment+shipment_line+sales_order_line)
  DECLARE
    v_ship NUMBER := 0;
    v_ord  NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(sl.qty_shipped),0) INTO v_ship
    FROM erp_shipment sh
    JOIN erp_shipment_line sl ON sl.shipment_id = sh.shipment_id;
    SELECT NVL(SUM(qty),0) INTO v_ord FROM erp_sales_order_line;
    erp_usp_write_audit_trail('FULFIL','ERP_SHIPMENT',
      'blk2062 shipped='||v_ship||' ordered='||v_ord);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('FULFIL_ERR','ERP_SHIPMENT','blk2062 '||SQLERRM);
  END;
  -- [2063] price list variance (price_list+product_price+product)
  DECLARE
    v_var NUMBER := 0;
    v_n   NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT pl.list_name, pp.unit_price, p.list_price
      FROM erp_price_list pl
      JOIN erp_product_price pp ON pp.price_list_id = pl.price_list_id
      JOIN erp_product p        ON p.product_id = pp.product_id
      WHERE pl.active_flag = 'Y'
    ) LOOP
      v_var := v_var + ABS(NVL(r.unit_price,0) - NVL(r.list_price,0));
      v_n := v_n + 1;
    END LOOP;
    erp_usp_write_audit_trail('PRICE_VAR','ERP_PRODUCT_PRICE',
      'blk2063 var='||v_var||' n='||v_n);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PRICE_VAR_ERR','ERP_PRODUCT_PRICE','blk2063 '||SQLERRM);
  END;
  -- [2064] recursive WITH CTE over BOM tree (RECURSIVE, UNION ALL, depth guard)
  DECLARE
    v_rows NUMBER := 0;
    v_lvls NUMBER := 0;
  BEGIN
    WITH bom_tree(product_id, component_id, lvl) AS (
      SELECT bh.product_id, bc.component_id, 1
      FROM erp_bom_header bh
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      UNION ALL
      SELECT t.product_id, bc.component_id, t.lvl + 1
      FROM bom_tree t
      JOIN erp_bom_header bh    ON bh.product_id = t.component_id
      JOIN erp_bom_component bc ON bc.bom_id = bh.bom_id
      WHERE t.lvl < 10
    )
    SELECT COUNT(*), NVL(MAX(lvl),0) INTO v_rows, v_lvls FROM bom_tree;
    erp_usp_write_audit_trail('BOM_CTE','ERP_BOM_COMPONENT',
      'blk2064 rows='||v_rows||' maxlvl='||v_lvls);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CTE_ERR','ERP_BOM_COMPONENT','blk2064 '||SQLERRM);
  END;
  -- [2065] hierarchical CONNECT BY over BOM explosion
  DECLARE
    v_nodes NUMBER := 0;
    v_depth NUMBER := 0;
  BEGIN
    SELECT COUNT(*), NVL(MAX(LEVEL),0) INTO v_nodes, v_depth
    FROM erp_bom_component bc
    START WITH bc.bom_id IN (SELECT bom_id FROM erp_bom_header WHERE active_flag = 'Y')
    CONNECT BY NOCYCLE PRIOR bc.component_id = bc.bom_id
    AND LEVEL <= 10;
    erp_usp_write_audit_trail('BOM_CONNECT','ERP_BOM_COMPONENT',
      'blk2065 nodes='||v_nodes||' depth='||v_depth);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('BOM_CONNECT_ERR','ERP_BOM_COMPONENT','blk2065 '||SQLERRM);
  END;
  -- [2066] analytic / window functions over sales order lines
  DECLARE
    v_top NUMBER := 0;
    v_cnt NUMBER := 0;
  BEGIN
    FOR r IN (
      SELECT sol.product_id, sol.line_amount,
             SUM(sol.line_amount) OVER (PARTITION BY sol.product_id) AS prod_tot,
             ROW_NUMBER() OVER (PARTITION BY sol.product_id
                                ORDER BY sol.line_amount DESC) AS rn,
             LAG(sol.line_amount,1,0) OVER (ORDER BY sol.line_id) AS prev_amt
      FROM erp_sales_order_line sol
    ) LOOP
      IF r.rn = 1 THEN v_top := v_top + NVL(r.prod_tot,0); END IF;
      v_cnt := v_cnt + 1;
    END LOOP;
    erp_usp_write_audit_trail('ANALYTIC','ERP_SALES_ORDER_LINE',
      'blk2066 top='||v_top||' rows='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('ANALYTIC_ERR','ERP_SALES_ORDER_LINE','blk2066 '||SQLERRM);
  END;
  -- [2067] self-join on BOM (component that is also a parent) + nested subquery
  DECLARE
    v_pairs NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_pairs
    FROM erp_bom_component c1
    JOIN erp_bom_component c2 ON c2.bom_id = c1.bom_id AND c2.component_id <> c1.component_id
    WHERE c1.component_id IN (
      SELECT bh.product_id FROM erp_bom_header bh
      WHERE bh.product_id IN (SELECT product_id FROM erp_product WHERE make_buy = 'M')
    );
    erp_usp_write_audit_trail('SELF_JOIN','ERP_BOM_COMPONENT','blk2067 pairs='||v_pairs);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('SELF_JOIN_ERR','ERP_BOM_COMPONENT','blk2067 '||SQLERRM);
  END;
  -- [2068] FULL/RIGHT/CROSS join variety + NVL/COALESCE across product & inventory
  DECLARE
    v_a NUMBER := 0;
    v_b NUMBER := 0;
    v_c NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_a
    FROM erp_product p
    FULL OUTER JOIN erp_inventory i ON i.product_id = p.product_id;
    SELECT COUNT(*) INTO v_b
    FROM erp_inventory i
    RIGHT OUTER JOIN erp_product p ON p.product_id = i.product_id;
    SELECT COUNT(*) INTO v_c
    FROM erp_warehouse w CROSS JOIN erp_currency cur;
    erp_usp_write_audit_trail('JOIN_VARIETY','ERP_PRODUCT',
      'blk2068 full='||v_a||' right='||v_b||' cross='||v_c);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('JOIN_VARIETY_ERR','ERP_PRODUCT','blk2068 '||SQLERRM);
  END;
  -- [2069] GLOBAL TEMPORARY TABLE staging + INSERT/SELECT + DELETE
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    DELETE FROM erp_gtt_stage;
    INSERT INTO erp_gtt_stage(product_id, qty)
    SELECT d.product_id, SUM(d.qty)
    FROM erp_demand d
    GROUP BY d.product_id;
    SELECT COUNT(*) INTO v_cnt FROM erp_gtt_stage;
    erp_usp_write_audit_trail('GTT_STAGE','ERP_GTT_STAGE','blk2069 staged='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('GTT_STAGE_ERR','ERP_GTT_STAGE','blk2069 '||SQLERRM);
  END;
  -- [2070] deeply nested subqueries (3 levels) + correlated EXISTS
  DECLARE
    v_cnt NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO v_cnt
    FROM erp_product p
    WHERE p.product_id IN (
      SELECT d.product_id FROM erp_demand d
      WHERE d.qty > (
        SELECT NVL(AVG(d2.qty),0) FROM erp_demand d2
        WHERE d2.product_id IN (
          SELECT sol.product_id FROM erp_sales_order_line sol
          WHERE EXISTS (SELECT 1 FROM erp_sales_order so
                        WHERE so.order_id = sol.order_id AND so.status <> 'CANCELLED')
        )
      )
    );
    erp_usp_write_audit_trail('NESTED_SUBQ','ERP_PRODUCT','blk2070 cnt='||v_cnt);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('NESTED_SUBQ_ERR','ERP_PRODUCT','blk2070 '||SQLERRM);
  END;
  -- [2071] package call: overloaded API + pipelined table function + global var
  DECLARE
    v_tot NUMBER := 0;
    v_rows NUMBER := 0;
  BEGIN
    erp_pkg_planner.set_context('CYCLE', 2071);
    v_tot := erp_pkg_planner.compute_total(2071);
    v_tot := v_tot + erp_pkg_planner.compute_total(2071, 'BASE');
    FOR r IN (SELECT * FROM TABLE(erp_pkg_planner.explode_products(2071))) LOOP
      v_rows := v_rows + 1;
    END LOOP;
    erp_usp_write_audit_trail('PKG_CALL','ERP_PKG_PLANNER',
      'blk2071 tot='||v_tot||' piped='||v_rows||' ctx='||erp_pkg_planner.g_last_context);
  EXCEPTION
    WHEN OTHERS THEN
      erp_usp_write_audit_trail('PKG_CALL_ERR','ERP_PKG_PLANNER','blk2071 '||SQLERRM);
  END;

  erp_usp_touch_views;

  erp_usp_run_analytics(v_run_id);

  erp_usp_edge_recalc(v_run_id);

  erp_usp_write_audit_trail('CYCLE_END','ERP_RUN_CYCLE',
    'Cycle done. run='||v_run_id||' orders='||v_orders||' revenue='||v_revenue);
EXCEPTION
  WHEN OTHERS THEN
    erp_usp_write_audit_trail('CYCLE_ERROR','ERP_RUN_CYCLE',SQLERRM);
    RAISE;
END erp_usp_run_cycle;
/

-- ---------------------------------------------------------------- triggers
CREATE OR REPLACE TRIGGER erp_trg_sol_biu
BEFORE INSERT OR UPDATE ON erp_sales_order_line
FOR EACH ROW
BEGIN
  IF :NEW.line_id IS NULL THEN
    :NEW.line_id := erp_doc_seq.NEXTVAL;
  END IF;
  :NEW.line_amount := NVL(:NEW.qty,0) * NVL(:NEW.unit_price,0);
END erp_trg_sol_biu;
/

CREATE OR REPLACE TRIGGER erp_trg_so_stmt_ad
AFTER DELETE ON erp_sales_order
BEGIN
  erp_usp_write_audit_trail('SO_DELETE','ERP_SALES_ORDER','statement-level delete fired');
END erp_trg_so_stmt_ad;
/

CREATE OR REPLACE TRIGGER erp_trg_inv_compound
FOR INSERT OR UPDATE ON erp_inventory
COMPOUND TRIGGER
  TYPE t_ids IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
  g_touched t_ids;
  g_n       PLS_INTEGER := 0;

  BEFORE EACH ROW IS
  BEGIN
    IF :NEW.qty_on_hand < 0 THEN
      :NEW.qty_on_hand := 0;
    END IF;
  END BEFORE EACH ROW;

  AFTER EACH ROW IS
  BEGIN
    g_n := g_n + 1;
    g_touched(g_n) := :NEW.product_id;
  END AFTER EACH ROW;

  AFTER STATEMENT IS
  BEGIN
    erp_usp_write_audit_trail('INV_TOUCH','ERP_INVENTORY','rows touched='||g_n);
  END AFTER STATEMENT;
END erp_trg_inv_compound;
/

CREATE OR REPLACE TRIGGER erp_trg_wo_biu
BEFORE INSERT OR UPDATE ON erp_work_order
FOR EACH ROW
WHEN (NEW.status IS NOT NULL)
BEGIN
  IF :NEW.wo_id IS NULL THEN
    :NEW.wo_id := erp_doc_seq.NEXTVAL;
  END IF;
  IF INSERTING AND :NEW.status IS NULL THEN
    :NEW.status := 'PLANNED';
  END IF;
END erp_trg_wo_biu;
/

-- ---------------------------------------------------------------- end of seed
EXIT
