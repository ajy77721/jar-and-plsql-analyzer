CREATE OR REPLACE PACKAGE BODY                                     PG_RPGE_LISTING
IS
    
      /******************************************************************************
      NAME:       PG_RPGE_LISTING
      PURPOSE:    ACCOUNTING REPORTS

      REVISIONS:
      Ver        Date        Author           Description
      ---------  ----------  ---------------  ------------------------------------
      1.0        15/10/2019   GANGARAM         Red mine 106281 : Auto Commission payment constant modified to CR>CP
      2.00       30/08/2019   Debanjan         Redmine#119949 : FN_RPAC_ACBATCHLIST :: for Transaction Type 'debit note',
                                               duplicate record was coming as table ACDB_GL joined in parent query.
      2.01       04/09/2019   Debanjan         Redmine#119949 : FN_RPAC_ACBATCHLIST :: Unnecessary join with view DMAG_VI_AGENT in query.
                                               Removed as per DBA Review feedback.
      2.02       24/02/2020   GANGARAM         Red mine 128863 : VEH_NO,CNOTE_NO making null for CR>CP and No Statment                              
      3.00       14/04/2020   Shabbir          Redmine # 130371 : Thunderhead : Outsource payment listing report issues
      4.00       24/04/2020   Shabbir          Redmine # 129304 : Thunderhead : Notice of Cancellation - Period of Cover and Premium warranty due date should calculate based on Endorsement Effective Date
      5.00       19/08/2020   Vipin Vince      Redmine # 141697 : Accounting: Outstanding transaction in statement-RI Name
      6.00       28/08/2020   Vipin Vince      Redmine # 142108 : Auto_comm_listing_add_columns    
      7.00       17/09/2020   RAMAKRISHNA.M    Redmine # 135333 : To cater UBB and ad-hoc reporting requirement as to make sure risks can only be accepted after completion of fire surveys (Fire  products)
      8.0        16/10/2020   Kahfina          Redmine # 135333 : Change SURVEY_PRODUCT to SURVEY_PRODUCT2
      8.01       05/11/2020   RAMAKRISHNA.M    Redmine # 143819 : ODM, AGIC OPUS : Revised Referred and Decline Risk in OPUS  Notice (UBB)
      9.00       19/10/2020   Vipin Vince      Redmine # 144913 : Accounting: Outstanding transaction in statement- NO data found
      10.00      11/01/2021   ASEEF BABA       Enhancement#130292 AGIC OPUS > NMCN > Non-Motor Cover Note Number Update
      11.00      11/01/2021   JACELINE         Enhancement#84167 Installment IFRS17: Outstanding Transaction In statement - Include Installment
      12.00      21/01/2020   GANGARAM         Redmine 84167 : OPUS-I  | IFRS 17 Requirement- To built in Installment Module in Underwriting.
      13.00      22/02/2021   JACELINE         Enhancement#109730 AGIC OPUS > Accounting > Refund Premium Automation
      14.00      25/02/2021   CHANDRA          Enhancement#109730 AGIC OPUS > Accounting > Refund Premium Automation 
      15.00      05/03/2021   CHANDRA          Enhancement#109730 AGIC OPUS > Accounting > Refund Premium Automation
      16.00      07/07/2021   SAI PAVAN        APP216465 # Outstanding Survey Report issue
      17.00      13/11/2021   JACELINE         # Assign null value of BENEFICIARY LISTING REPORT
      18.00      29/12/2021   JACELINE         Enhancement#175366: WithHolding Tax On Payment made to agent etc
      19.00         23/2/2022    JACELINE           Enhancement#AGIC-735: To allow WHT agent to pay by total payable/nett/Other amount
      20.00      24/02/2022   GANGARAM         #AGIC-890 Unable to retrieve RI Inward data
      21.00      18/03/2022   GANGARAM         INC11570808  Unable to retrieve RI Agents data.
      22.00      25/03/2022   GANGARAM         INC10587071  Unable to retrieve RI Agents data reverting version 21.00 changes.
      23.00      26/04/2022   GANGARAM         INC11933419  WHT report displaying deleted PV number.
      24.00      24/03/2022   GANGARAM         AGIC-735 AGIC-753_WHTPymtToAgent
      25.00      20/05/2022   GANGARAM         #AGIC-1893 WHT applies on NETT commission recovery payment
      26.00      08/06/2022   SHABBIR            Enhancement#170969 : AGIC OPUS > 070414 PerlindunganKu Allianz4All
      27.00      22/06/2022   Gangaram         Bug#AGIC-3966 - WHT Report - to display 2 rows when multiple matching
      28.00      18/07/2022   GANGARAM         Enhancement AGIC-960 MT PTV
      29.00         26/08/2022      Atiqah           AGIC-4592_Property Underwriting Guideline
      30.00      27/04/2023      GANGARAM         Bug #AGIC-14482 AGIC OPUS: Accounting Banca Payment matched listing displaying duplicate rows in Report
      31.00      29/05/2023    GANGARAM        BUG AGIC-14470 WHT reporting list agent code changed to KO tables.
      32.00      25/09/2023   Nayarayanarao    BUG AGIC-13190 To prevent print Outstanding transactions with zero amount.
      33.00      21/12/2023   Nayarayanarao    BUG AGIC-15690 Query modified as dynamic filters.
      34.00      25/03/2024   Nayarayanarao    Enhancement AGIC-26549 : E-Invoice Status Report
      35.00      02/05/2024   Gangaram         AGIC-30991 Property underwriting guideline 2024 impact system changes.
      37.00      14/08/2024   Gangaram         CR010 Einvoice status report for SBNM irbm type
      36.00      15/08/2024   Nayarayanarao    AGIC-36416 - CR031 Included Fail status in E-Invoice Status dropdowan to filter fail records in E-INVOICE STATUS REPORT
      38.00      20/09/2024   Jaceline           bug fix : eInvoice status report
   ******************************************************************************/
   
   g_k_V_PackageName_v   CONSTANT VARCHAR2 (50) := 'PG_RPGE_LISTING';

    procedure pc_upd_prn_ind (p_StrBranchCode in varchar2, p_StrAccountCodeFrom in varchar2, 
   p_StrAccountCodeTo in varchar2, p_StrPolicy in varchar2)
   is
      v_ProcName_v   VARCHAR2 (30) := 'pc_upd_prn_ind';
      v_Step_v       VARCHAR2 (5) := '000';
      v_sql_up        VARCHAR (3000);
      pragma autonomous_transaction;

   begin
        
      v_sql_up :=
        'Update ACST_MAST SET PW_PRN_DATE = '''||TRUNC(SYSDATE)||''', PW_PRN_IND = ''1'' ' 
        ||'WHERE UKEY_MAST IN ( SELECT UKEY_MAST FROM((( '
        ||'CMGE_CODE  INNER JOIN ACST_MAST C1 ON  CMGE_CODE.CODE_CD = C1.PRODUCT_CONFIG_CODE  AND  CMGE_CODE.CAT_CODE LIKE ''%PRODUCT'') '
        ||'LEFT OUTER JOIN DMAG_VI_AGENT ON C1.AGENT_CODE = DMAG_VI_AGENT.AGENTCODE ) '
        ||'LEFT OUTER JOIN CMDM_BRANCH ON DMAG_VI_AGENT.BRANCH_CODE = CMDM_BRANCH.BRANCH_CODE ) '
        ||'LEFT OUTER JOIN CMGE_POSTCODE ON CMDM_BRANCH.POSTCODE = CMGE_POSTCODE.POSTCODE '
        ||'WHERE DMAG_VI_AGENT.BRANCH_CODE = '''||p_StrBranchCode||'''  AND (C1.ST_TYPE = ''PL'' or C1.ST_TYPE=''EN'') '
        ||'AND C1.AGENT_CAT_TYPE=''PW'' AND C1.BAL_AMT <> 0 ';
        
        
        
         IF(p_StrAccountCodeFrom IS NOT NULL AND p_StrAccountCodeTo IS NOT NULL ) THEN
         v_sql_up := v_sql_up ||' AND ACST_MAST.AGENT_CODE BETWEEN '''||p_StrAccountCodeFrom||''' AND '''||p_StrAccountCodeTo||''' ';
         END IF;
         
         IF(p_StrPolicy IS  NULL) THEN
         v_sql_up := v_sql_up ||' ) ';
         END IF;
        
         IF(p_StrPolicy IS NOT NULL) THEN 
         v_sql_up := v_sql_up ||' AND ACST_MAST.ST_DOC = '''||p_StrPolicy||'''  ) ';
         END IF;
         
         DBMS_OUTPUT.put_line ('3.v_sql_up =' || v_sql_up);
         EXECUTE IMMEDIATE v_sql_up;
     
       DBMS_OUTPUT.put_line ('After E - lStrSQL = '|| v_sql_up);
      --EXECUTE IMMEDIATE lStrSQL;    
    COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
        DBMS_OUTPUT.put_line ('ERROR - = '||v_ProcName_v|| ' - '|| SQLERRM);
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   end pc_upd_prn_ind;
   
   --BANCA - NON MOTOR MATCHED LISTING
   FUNCTION FN_GEN_BANCA_NM_MATCHED (p_ParmPymtType          VARCHAR2,
                                     p_ParmBatchCode         VARCHAR2,
                                     p_ParmUploadDateFrom    VARCHAR2,
                                     p_ParmUploadDateTo      VARCHAR2,
                                     p_ParmBankInDateFrom    VARCHAR2,
                                     p_ParmBankInDateTo      VARCHAR2,
                                     p_ParmRcptDateFrom      VARCHAR2,
                                     p_ParmRcptDateTo        VARCHAR2
                                    )
      RETURN BANCA_NM_MATCHED_TAB
      PIPELINED
   IS
      v_ProcName_v    VARCHAR2 (30) := 'FN_GEN_BANCA_NM_MATCHED';
      v_Step_v        VARCHAR2 (5) := '000';
      r_banca_row     BANCA_NM_MATCHED_REC;
      row_count       NUMBER := 1;
      v_cpPartId_n    uwge_policy_bases.cp_part_id%TYPE;
      v_cpPartVer_n   uwge_policy_bases.cp_version%TYPE;
   BEGIN
      FOR r
         IN (  SELECT ACRC_PYMT_UPLOAD.PYMT_REFNO, ACST_MAST.BAL_AMT,
                      ACRC_PYMT_UPLOAD.PYMT_AMT, 0 CN_PYMT_AMT,
                      ACRC_PYMT_UPLOAD.COLL_NO, ACRC_PYMT_UPLOAD.LOAN_ACCT_NO
                      , ACRC_PYMT_UPLOAD.UPLOAD_DATETIME, ACRC_PYMT_UPLOAD.RCPT_NO
                      , ACRC_PYMT_UPLOAD.RCPT_DATE, ACRC_PYMT_UPLOAD.BANK_CODE
                      , ACRC_PYMT_UPLOAD.TAX_DOC_NO, ACRC_PYMT_UPLOAD.ADD_REMARKS
                      , ACRC_PYMT_UPLOAD.ENDT_REMARKS, ACRC_PYMT_UPLOAD.CN_AMT
                      , ACST_MAST.PRODUCT_CONFIG_CODE, ACRC_PYMT_UPLOAD.DEL_IND
                      , ACST_MAST.ST_TYPE, ACST_MAST.ST_DOC,ACRC_PYMT_UPLOAD.BATCH_TYPE
                 FROM (ACST_MAST
                       INNER JOIN ACRC_PYMT_UPLOAD
                          ON ACST_MAST.ST_DOC = ACRC_PYMT_UPLOAD.PYMT_REFNO)
                      INNER JOIN
                      ACRC_BATCH_MAST
                         ON ACRC_PYMT_UPLOAD.BATCH_CODE =
                               ACRC_BATCH_MAST.BATCH_CODE
                WHERE     ACRC_PYMT_UPLOAD.PYMT_AMT > 0
                      AND ACRC_PYMT_UPLOAD.PYMT_AMT =
                             ACRC_PYMT_UPLOAD.MATCH_AMT
                      AND ((p_ParmUploadDateFrom is null and p_ParmUploadDateTo is null) 
                          or(TRUNC(ACRC_PYMT_UPLOAD.UPLOAD_DATETIME) BETWEEN TO_DATE( p_ParmUploadDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmUploadDateTo, 'DD/MM/YYYY')))
                      AND ((p_ParmBankInDateFrom is null and p_ParmBankInDateTo is null)
                          or (ACRC_PYMT_UPLOAD.PYMT_DATE BETWEEN TO_DATE( p_ParmBankInDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmBankInDateTo, 'DD/MM/YYYY')))
                      AND ((p_ParmRcptDateFrom is null and p_ParmRcptDateTo is null)
                          or (ACRC_PYMT_UPLOAD.RCPT_DATE BETWEEN TO_DATE( p_ParmRcptDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmRcptDateTo, 'DD/MM/YYYY')))
                      AND (p_ParmBatchCode is null or ACRC_PYMT_UPLOAD.BATCH_CODE=p_ParmBatchCode)
             ORDER BY ACRC_PYMT_UPLOAD.PYMT_REFNO)
      LOOP
         r_banca_row.PYMT_REFNO        := NULL;
         r_banca_row.COLLATERAL_NO     := NULL;
         r_banca_row.LOAN_AC_NO        := NULL;
         r_banca_row.AGENT             := NULL;
         r_banca_row.POL_DATE          := NULL;
         r_banca_row.CLIENT_NAME       := NULL;
         r_banca_row.NETT_PREM         := NULL;
         r_banca_row.COMM              := NULL;
         r_banca_row.AFTCOMM_PREM      := NULL;
         r_banca_row.BAL_AMT           := NULL;
         r_banca_row.PYMT_AMT          := NULL;
         r_banca_row.CN_PYMT_AMT       := NULL;
         r_banca_row.COLL_NO           := NULL;
         r_banca_row.LOAN_ACCT_NO      := NULL;
         r_banca_row.UPLOAD_DATETIME   := NULL;
         r_banca_row.RCPT_NO           := NULL;
         r_banca_row.RCPT_DATE         := NULL;
         r_banca_row.BANK_CODE         := NULL;
         r_banca_row.TAX_DOC_NO        := NULL;
         r_banca_row.ADD_REMARKS       := NULL;
         r_banca_row.ENDT_REMARKS      := NULL;
         v_cpPartId_n                  := NULL;
         v_cpPartVer_n                 := NULL;

         r_banca_row.PYMT_REFNO        := r.PYMT_REFNO;

         IF r.ST_TYPE = 'PL'
         THEN
            BEGIN
               SELECT COLLATERAL_NO, LOAN_AC_NO
                 INTO r_banca_row.COLLATERAL_NO, r_banca_row.LOAN_AC_NO
                 FROM UWGE_POLICY_BANCA bn
                WHERE     bn.version_no = 1
                      AND EXISTS
                             (SELECT 1
                                FROM ocp_policy_bases opb
                               WHERE     opb.contract_id = bn.contract_id
                                     AND opb.version_no = 1
                                     AND opb.policy_ref = r.st_doc);
            EXCEPTION
               WHEN OTHERS
               THEN
                  NULL;
            END;

            BEGIN
               SELECT agent_code, NETT_PREM,
                      COMM_AMT, cp_part_id,
                      cp_Version
                 INTO r_banca_row.AGENT, r_banca_row.NETT_PREM,
                      r_banca_row.COMM, v_cpPartId_n,
                      v_cpPartVer_n
                 FROM uwge_policy_bases bn
                WHERE     bn.version_no = 1
                      AND EXISTS
                             (SELECT 1
                                FROM ocp_policy_bases opb
                               WHERE     opb.contract_id = bn.contract_id
                                     AND opb.version_no = 1
                                     AND opb.policy_ref = r.st_doc);
            EXCEPTION
               WHEN OTHERS
               THEN
                  NULL;
            END;

            BEGIN
               SELECT issue_date
                 INTO r_banca_row.POL_DATE
                 FROM uwge_policy_contracts bn
                WHERE EXISTS
                         (SELECT 1
                            FROM ocp_policy_bases opb
                           WHERE     opb.contract_id = bn.contract_id
                                 AND opb.version_no = 1
                                 AND opb.policy_ref = r.st_doc);
            EXCEPTION
               WHEN OTHERS
               THEN
                  NULL;
            END;
         ELSE
            BEGIN
               SELECT COLLATERAL_NO, LOAN_AC_NO
                 INTO r_banca_row.COLLATERAL_NO, r_banca_row.LOAN_AC_NO
                 FROM UWGE_POLICY_BANCA bn
                WHERE EXISTS
                         (SELECT 1
                            FROM uwge_policy_versions upv
                           WHERE     upv.contract_id = bn.contract_id
                                 AND upv.version_no = bn.version_no
                                 AND upv.endt_no = r.st_doc);
            EXCEPTION
               WHEN OTHERS
               THEN
                  NULL;
            END;

            BEGIN
               SELECT agent_code, NETT_PREM,
                      COMM_AMT, NETT_PREM,
                      cp_part_id, cp_Version
                 INTO r_banca_row.AGENT, r_banca_row.NETT_PREM,
                      r_banca_row.COMM, r_banca_row.AFTCOMM_PREM,
                      v_cpPartId_n, v_cpPartVer_n
                 FROM uwge_policy_bases bn
                WHERE EXISTS
                         (SELECT 1
                            FROM uwge_policy_versions upv
                           WHERE     upv.contract_id = bn.contract_id
                                 AND upv.version_no = bn.version_no
                                 AND upv.endt_no = r.st_doc);
            EXCEPTION
               WHEN OTHERS
               THEN
                  NULL;
            END;

            BEGIN
               SELECT issue_date
                 INTO r_banca_row.POL_DATE
                 FROM uwge_policy_versions bn
                WHERE bn.endt_no = r.st_doc;
            EXCEPTION
               WHEN OTHERS
               THEN
                  NULL;
            END;
         END IF;

         IF v_cpPartId_n IS NOT NULL AND v_cpPartVer_n IS NOT NULL
         THEN
            SELECT name_ext
              INTO r_banca_row.CLIENT_NAME
              FROM cpge_vi_partners_all cp
             WHERE cp.part_id = v_cpPartId_n AND cp.version = v_cpPartVer_n;
         END IF;

         r_banca_row.BAL_AMT           := r.BAL_AMT;
         r_banca_row.PYMT_AMT          := r.PYMT_AMT;
         r_banca_row.CN_PYMT_AMT       := r.CN_PYMT_AMT;
         r_banca_row.DIFF              := r.CN_PYMT_AMT - r.PYMT_AMT;
         r_banca_row.COLL_NO           := r.COLL_NO;
         r_banca_row.LOAN_ACCT_NO      := r.LOAN_ACCT_NO;
         r_banca_row.UPLOAD_DATETIME   := r.UPLOAD_DATETIME;
         r_banca_row.RCPT_NO           := r.RCPT_NO;
         r_banca_row.RCPT_DATE         := r.RCPT_DATE;
         r_banca_row.BANK_CODE         := r.BANK_CODE;
         r_banca_row.TAX_DOC_NO        := r.TAX_DOC_NO;
         r_banca_row.ADD_REMARKS       := r.ADD_REMARKS;
         r_banca_row.ENDT_REMARKS      := r.ENDT_REMARKS;

         DBMS_OUTPUT.put_line (r_banca_row.PYMT_REFNO);
         PIPE ROW (r_banca_row);
      END LOOP;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   END FN_GEN_BANCA_NM_MATCHED;

   --BANCA - NON MOTOR UNMATCHED LISTING
   FUNCTION FN_GEN_BANCA_NM_UNMATCHED (p_ParmPymtType          VARCHAR2,
                                       p_ParmBatchCode         VARCHAR2,
                                       p_ParmUploadDateFrom    VARCHAR2,
                                       p_ParmUploadDateTo      VARCHAR2,
                                       p_ParmBankInDateFrom    VARCHAR2,
                                       p_ParmBankInDateTo      VARCHAR2,
                                       p_ParmRcptDateFrom      VARCHAR2,
                                       p_ParmRcptDateTo        VARCHAR2
                                      )
      RETURN BANCA_NM_MATCHED_TAB
      PIPELINED
   IS
      v_ProcName_v    VARCHAR2 (30) := 'FN_GEN_BANCA_NM_UNMATCHED';
      v_Step_v        VARCHAR2 (5) := '000';
      r_banca_row     BANCA_NM_MATCHED_REC;
      row_count       NUMBER := 1;
      v_cpPartId_n    uwge_policy_bases.cp_part_id%TYPE;
      v_cpPartVer_n   uwge_policy_bases.cp_version%TYPE;
   BEGIN
      FOR r
         IN (  SELECT ACRC_PYMT_UPLOAD.PYMT_REFNO, ACST_MAST.BAL_AMT,
                      ACRC_PYMT_UPLOAD.PYMT_AMT, 0 CN_PYMT_AMT,
                      ACRC_PYMT_UPLOAD.COLL_NO, ACRC_PYMT_UPLOAD.LOAN_ACCT_NO
                      , ACRC_PYMT_UPLOAD.UPLOAD_DATETIME, ACRC_PYMT_UPLOAD.RCPT_NO
                      , ACRC_PYMT_UPLOAD.RCPT_DATE, ACRC_PYMT_UPLOAD.BANK_CODE
                      , ACRC_PYMT_UPLOAD.TAX_DOC_NO, ACRC_PYMT_UPLOAD.ADD_REMARKS
                      , ACRC_PYMT_UPLOAD.ENDT_REMARKS, ACRC_PYMT_UPLOAD.CN_AMT
                      , ACST_MAST.PRODUCT_CONFIG_CODE, ACRC_PYMT_UPLOAD.DEL_IND
                      , ACST_MAST.ST_TYPE, ACST_MAST.ST_DOC,
                      ACRC_PYMT_UPLOAD.UNMATCH_REMARKS
                 FROM (ACST_MAST
                       INNER JOIN ACRC_PYMT_UPLOAD
                          ON ACST_MAST.ST_DOC = ACRC_PYMT_UPLOAD.PYMT_REFNO)
                      INNER JOIN
                      ACRC_BATCH_MAST
                         ON ACRC_PYMT_UPLOAD.BATCH_CODE =
                               ACRC_BATCH_MAST.BATCH_CODE
                WHERE (   ACRC_PYMT_UPLOAD.PYMT_AMT <>
                             ACRC_PYMT_UPLOAD.MATCH_AMT
                       OR ACRC_PYMT_UPLOAD.PYMT_AMT = 0)
                       AND ((p_ParmUploadDateFrom is null and p_ParmUploadDateTo is null) 
                          or(TRUNC(ACRC_PYMT_UPLOAD.UPLOAD_DATETIME) BETWEEN TO_DATE( p_ParmUploadDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmUploadDateTo, 'DD/MM/YYYY')))
                       AND ((p_ParmBankInDateFrom is null and p_ParmBankInDateTo is null)
                          or (ACRC_PYMT_UPLOAD.PYMT_DATE BETWEEN TO_DATE( p_ParmBankInDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmBankInDateTo, 'DD/MM/YYYY')))
                       AND ((p_ParmRcptDateFrom is null and p_ParmRcptDateTo is null)
                          or (ACRC_PYMT_UPLOAD.RCPT_DATE BETWEEN TO_DATE( p_ParmRcptDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmRcptDateTo, 'DD/MM/YYYY')))
                       AND (p_ParmBatchCode is null or ACRC_PYMT_UPLOAD.BATCH_CODE=p_ParmBatchCode)
             ORDER BY ACRC_PYMT_UPLOAD.PYMT_REFNO)
      LOOP
         r_banca_row.PYMT_REFNO        := NULL;
         r_banca_row.COLLATERAL_NO     := NULL;
         r_banca_row.LOAN_AC_NO        := NULL;
         r_banca_row.AGENT             := NULL;
         r_banca_row.POL_DATE          := NULL;
         r_banca_row.CLIENT_NAME       := NULL;
         r_banca_row.NETT_PREM         := NULL;
         r_banca_row.COMM              := NULL;
         r_banca_row.AFTCOMM_PREM      := NULL;
         r_banca_row.BAL_AMT           := NULL;
         r_banca_row.PYMT_AMT          := NULL;
         r_banca_row.CN_PYMT_AMT       := NULL;
         r_banca_row.COLL_NO           := NULL;
         r_banca_row.LOAN_ACCT_NO      := NULL;
         r_banca_row.UPLOAD_DATETIME   := NULL;
         r_banca_row.RCPT_NO           := NULL;
         r_banca_row.RCPT_DATE         := NULL;
         r_banca_row.BANK_CODE         := NULL;
         r_banca_row.TAX_DOC_NO        := NULL;
         r_banca_row.ADD_REMARKS       := NULL;
         r_banca_row.ENDT_REMARKS      := NULL;
         r_banca_row.UNMATCH_REMARKS   := NULL;
         v_cpPartId_n                  := NULL;
         v_cpPartVer_n                 := NULL;

         r_banca_row.PYMT_REFNO        := r.PYMT_REFNO;

         IF r.ST_TYPE = 'PL'
         THEN
            BEGIN
               SELECT COLLATERAL_NO, LOAN_AC_NO
                 INTO r_banca_row.COLLATERAL_NO, r_banca_row.LOAN_AC_NO
                 FROM UWGE_POLICY_BANCA bn
                WHERE     bn.version_no = 1
                      AND EXISTS
                             (SELECT 1
                                FROM ocp_policy_bases opb
                               WHERE     opb.contract_id = bn.contract_id
                                     AND opb.version_no = 1
                                     AND opb.policy_ref = r.st_doc);
            EXCEPTION
               WHEN OTHERS
               THEN
                  NULL;
            END;

            BEGIN
               SELECT agent_code, NETT_PREM,
                      COMM_AMT, cp_part_id,
                      cp_Version
                 INTO r_banca_row.AGENT, r_banca_row.NETT_PREM,
                      r_banca_row.COMM, v_cpPartId_n,
                      v_cpPartVer_n
                 FROM uwge_policy_bases bn
                WHERE     bn.version_no = 1
                      AND EXISTS
                             (SELECT 1
                                FROM ocp_policy_bases opb
                               WHERE     opb.contract_id = bn.contract_id
                                     AND opb.version_no = 1
                                     AND opb.policy_ref = r.st_doc);
            EXCEPTION
               WHEN OTHERS
               THEN
                  NULL;
            END;

            BEGIN
               SELECT issue_date
                 INTO r_banca_row.POL_DATE
                 FROM uwge_policy_contracts bn
                WHERE EXISTS
                         (SELECT 1
                            FROM ocp_policy_bases opb
                           WHERE     opb.contract_id = bn.contract_id
                                 AND opb.version_no = 1
                                 AND opb.policy_ref = r.st_doc);
            EXCEPTION
               WHEN OTHERS
               THEN
                  NULL;
            END;
         ELSE
            BEGIN
               SELECT COLLATERAL_NO, LOAN_AC_NO
                 INTO r_banca_row.COLLATERAL_NO, r_banca_row.LOAN_AC_NO
                 FROM UWGE_POLICY_BANCA bn
                WHERE EXISTS
                         (SELECT 1
                            FROM uwge_policy_versions upv
                           WHERE     upv.contract_id = bn.contract_id
                                 AND upv.version_no = bn.version_no
                                 AND upv.endt_no = r.st_doc);
            EXCEPTION
               WHEN OTHERS
               THEN
                  NULL;
            END;

            BEGIN
               SELECT agent_code, NETT_PREM,
                      COMM_AMT, NETT_PREM,
                      cp_part_id, cp_Version
                 INTO r_banca_row.AGENT, r_banca_row.NETT_PREM,
                      r_banca_row.COMM, r_banca_row.AFTCOMM_PREM,
                      v_cpPartId_n, v_cpPartVer_n
                 FROM uwge_policy_bases bn
                WHERE EXISTS
                         (SELECT 1
                            FROM uwge_policy_versions upv
                           WHERE     upv.contract_id = bn.contract_id
                                 AND upv.version_no = bn.version_no
                                 AND upv.endt_no = r.st_doc);
            EXCEPTION
               WHEN OTHERS
               THEN
                  NULL;
            END;

            BEGIN
               SELECT issue_date
                 INTO r_banca_row.POL_DATE
                 FROM uwge_policy_versions bn
                WHERE bn.endt_no = r.st_doc;
            EXCEPTION
               WHEN OTHERS
               THEN
                  NULL;
            END;
         END IF;

         IF v_cpPartId_n IS NOT NULL AND v_cpPartVer_n IS NOT NULL
         THEN
            SELECT name_ext
              INTO r_banca_row.CLIENT_NAME
              FROM cpge_vi_partners_all cp
             WHERE cp.part_id = v_cpPartId_n AND cp.version = v_cpPartVer_n;
         END IF;

         r_banca_row.BAL_AMT           := r.BAL_AMT;
         r_banca_row.PYMT_AMT          := r.PYMT_AMT;
         r_banca_row.CN_PYMT_AMT       := r.CN_PYMT_AMT;
         r_banca_row.COLL_NO           := r.COLL_NO;
         r_banca_row.LOAN_ACCT_NO      := r.LOAN_ACCT_NO;
         r_banca_row.UPLOAD_DATETIME   := r.UPLOAD_DATETIME;
         r_banca_row.RCPT_NO           := r.RCPT_NO;
         r_banca_row.RCPT_DATE         := r.RCPT_DATE;
         r_banca_row.BANK_CODE         := r.BANK_CODE;
         r_banca_row.TAX_DOC_NO        := r.TAX_DOC_NO;
         r_banca_row.ADD_REMARKS       := r.ADD_REMARKS;
         r_banca_row.ENDT_REMARKS      := r.ENDT_REMARKS;
         r_banca_row.UNMATCH_REMARKS   := r.UNMATCH_REMARKS;

         PIPE ROW (r_banca_row);
      END LOOP;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   END FN_GEN_BANCA_NM_UNMATCHED;

   --CALL CENTER MATCHED LISTING
   --  33.00  start
   FUNCTION FN_GEN_CALL_CENTER_MATCHED (p_ParmPymtCat           VARCHAR2,
                                     p_ParmPymtType          VARCHAR2,
                                     p_ParmBatchCode         VARCHAR2,
                                     p_ParmCnoteBranch       VARCHAR2,
                                     p_ParmUploadDateFrom    VARCHAR2,
                                     p_ParmUploadDateTo      VARCHAR2,
                                     p_ParmBankInDateFrom    VARCHAR2,
                                     p_ParmBankInDateTo      VARCHAR2,
                                     p_ParmRcptDateFrom      VARCHAR2,
                                     p_ParmRcptDateTo        VARCHAR2
                                )
      RETURN CALL_CENTER_MATCHED_TAB
      PIPELINED
   IS
     v_ProcName_v    VARCHAR2 (30) := 'FN_GEN_CALL_CENTER_MATCHED';
      v_Step_v        VARCHAR2 (5) := '000';
      r_banca_row     PG_RPGE_LISTING.CALL_CENTER_MATCHED_REC;
      row_count       NUMBER := 1;
      v_cpPartId_n    uwge_policy_bases.cp_part_id%TYPE;
      v_cpPartVer_n   uwge_policy_bases.cp_version%TYPE;
      TYPE v_cursor_type IS REF CURSOR;   
      v_cursor       v_cursor_type;
      lStrSQL             VARCHAR2(32767);
   BEGIN
    /* FOR r
         IN (SELECT   
                a.PYMT_REFNO, a.INSURED_NAME,  a.BATCH_TYPE,
                a.BANK_BRANCH, a.PYMT_DATE, a.PYMT_AMT, a.DEL_IND, a.PYMT_MODE_DESCP, 
                a.PREM_DUE as PYMT_PREM_DUE, a.MATCH_AMT, a.PYMT_NO,
                a.ADD_REMARKS, a.UPLOAD_DATETIME, a.BATCH_CODE,a.MANUAL_IND,
                -- start 12.00
                --b.MATCH_DOC,
                CASE WHEN ACGC_PYMT_MATCH_INST.INST_CYCLE IS NOT NULL THEN b.MATCH_DOC||':'||ACGC_PYMT_MATCH_INST.INST_CYCLE ELSE b.MATCH_DOC END as MATCH_DOC, -- end 12.00
                b.RCPT_NO, b.RCPT_DATE,  b.MATCH_DATE, 
                --b.MATCH_AMT as MH_MATCH_AMT,
                CASE WHEN ACGC_PYMT_MATCH_INST.INST_CYCLE IS NOT NULL THEN ACGC_PYMT_MATCH_INST.MATCH_AMT ELSE b.MATCH_AMT END as MH_MATCH_AMT, -- 12.00                
                b.MATCH_TYPE,
                ( COALESCE((select py.match_amt from acrc_pymt_match_det py where py.pymt_no=b.PYMT_NO and py.match_cn = b.MATCH_DOC and SUBSTR(py.match_doc,LENGTH(py.match_doc)-4,LENGTH(py.match_doc))='95054'), 0) ) As ROADTAX, 
                ( COALESCE((select py.match_amt from acrc_pymt_match_det py where py.pymt_no=b.PYMT_NO and py.match_cn=b.MATCH_DOC and SUBSTR(py.match_doc,LENGTH(py.match_doc)-4,LENGTH(py.match_doc))='63093'), 0) ) As MYEG,
                c.CNOTE_DATE, c.STATUS, d.JPJ_STATUS, c.CNOTE_BRANCH_CODE, c.AGENT_CODE, 
                ( select NAME from DMAG_VI_AGENT where AGENTCODE = c.AGENT_CODE ) As AGENT_NAME,
                coalesce(c.COMM_AMT, 0)  As COMM_AMT, c.NAME as CLIENT_NAME, d.VEH_NO, 
                c.PREM_DUE as CN_PREM_DUE, c.NETT_PREM, a.SRC_NO, a.SRC_NAME, a.CARD_NO, a.EDC_BATCH_NO, a.SETTLE_DATE, a.STMT_BATCH_NO,a.MATCH_ADV_AMT,c.POL_NO,c.EFF_DATE AS POL_EDATE    
                FROM ACRC_PYMT_UPLOAD a, ACRC_PYMT_MATCH b
                LEFT JOIN ACGC_PYMT_MATCH_INST ON b.MATCH_DOC = ACGC_PYMT_MATCH_INST.MATCH_DOC --  12.00
                , CNGE_NOTE c, CNGE_NOTE_MT d 
                WHERE  b.PYMT_NO = a.PYMT_NO and b.match_doc = c.CNOTE_NO and c.CNOTE_NO = d.CNOTE_NO
                AND ((p_ParmUploadDateFrom is null and p_ParmUploadDateTo is null) 
                or(TRUNC(a.UPLOAD_DATETIME) BETWEEN TO_DATE( p_ParmUploadDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmUploadDateTo, 'DD/MM/YYYY')))
                AND ((p_ParmBankInDateFrom is null and p_ParmBankInDateTo is null)
                or (a.PYMT_DATE BETWEEN TO_DATE( p_ParmBankInDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmBankInDateTo, 'DD/MM/YYYY')))
                AND ((p_ParmRcptDateFrom is null and p_ParmRcptDateTo is null)
                or (b.RCPT_DATE BETWEEN TO_DATE( p_ParmRcptDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmRcptDateTo, 'DD/MM/YYYY')))
                AND (p_ParmBatchCode is null or a.BATCH_CODE = p_ParmBatchCode)
                AND (p_ParmPymtType is null or a.BATCH_TYPE = p_ParmPymtType)
                AND (p_ParmCnoteBranch is null or c.CNOTE_BRANCH_CODE=p_ParmCnoteBranch)
                AND a.MATCH_AMT > 0 
                --and a.MATCH_AMT = a.PYMT_AMT
                AND a.PYMT_AMT = a.MATCH_AMT+NVL(a.MATCH_ADV_AMT,0)  
                AND  (a.DEL_IND is  null  or a.DEL_IND <> 'Y')  AND b.MATCH_TYPE <> 'GL'
                UNION
                SELECT 
                a.PYMT_REFNO, a.INSURED_NAME, a.BATCH_TYPE,
                a.BANK_BRANCH, a.PYMT_DATE, b.PYMT_AMT, a.DEL_IND, a.PYMT_MODE_DESCP, 
                a.PREM_DUE as PYMT_PREM_DUE, b.MATCH_AMT, a.PYMT_NO, 
                a.ADD_REMARKS, a.UPLOAD_DATETIME, a.BATCH_CODE, a.MANUAL_IND,
                --NVL(c.CNOTE_NO,b.MATCH_DOC) AS MATCH_DOC , -- start 12.00
                NVL(c.CNOTE_NO,CASE WHEN ACGC_PYMT_MATCH_INST.INST_CYCLE IS NOT NULL THEN b.MATCH_DOC||':'||ACGC_PYMT_MATCH_INST.INST_CYCLE ELSE b.MATCH_DOC END) AS MATCH_DOC , -- 12.00
                b.RCPT_NO, b.RCPT_DATE,  b.MATCH_DATE, 
                --b.MATCH_AMT as MH_MATCH_AMT, 
                CASE WHEN ACGC_PYMT_MATCH_INST.INST_CYCLE IS NOT NULL THEN ACGC_PYMT_MATCH_INST.MATCH_AMT ELSE b.MATCH_AMT END as MH_MATCH_AMT, -- 12.00
                b.MATCH_TYPE,
                ( COALESCE((select py.match_amt from acrc_pymt_match_det py where py.pymt_no=b.PYMT_NO and py.match_cn = b.MATCH_DOC and SUBSTR(py.match_doc,LENGTH(py.match_doc)-4,LENGTH(py.match_doc))='95054'), 0) ) As ROADTAX,
                ( COALESCE((select py.match_amt from acrc_pymt_match_det py where py.pymt_no=b.PYMT_NO and py.match_cn=b.MATCH_DOC and SUBSTR(py.match_doc,LENGTH(py.match_doc)-4,LENGTH(py.match_doc))='63093'), 0) ) As MYEG,
                c.CNOTE_DATE, c.POLICY_STATUS, '',  c.ISSUE_OFFICE, c.AGENT_CODE, 
                c.AGENT_NAME, 
                --c.COMM_AMT,
                B.PYMT_COMM_AMT,--Redmine 95747 
                c.CLIENT_NAME,
                (SELECT VEH_NO FROM UWGE_RISK_VEH WHERE CONTRACT_ID = C.CONTRACT_ID and TOP_INDICATOR='Y') AS VEH_NO  ,
                --c.CN_PREM_DUE, 
                B.PYMT_PREM_AMT as CN_PREM_DUE,--Redmine 95747 
                --c.NETT_PREM, 
                (B.PYMT_PREM_AMT  - COALESCE (B.PYMT_COMM_AMT, 0)  - COALESCE (B.PYMT_GST_COMM_AMT, 0))   AS NETT_PREM,--Redmine 95747 
                a.SRC_NO, a.SRC_NAME, a.CARD_NO, a.EDC_BATCH_NO, a.SETTLE_DATE, a.STMT_BATCH_NO,a.MATCH_ADV_AMT,c.POL_NO,c.TRAN_DATE
                FROM ACRC_PYMT_UPLOAD a, ACRC_PYMT_MATCH b
                LEFT JOIN ACGC_PYMT_MATCH_INST ON b.MATCH_DOC = ACGC_PYMT_MATCH_INST.MATCH_DOC  -- start 12.00
                AND b.PYMT_NO = ACGC_PYMT_MATCH_INST.PYMT_NO --30.00
                , (SELECT UB.CNOTE_NO,OP.POLICY_REF AS POL_NO,UB.CNOTE_DATE,OP.ACTION_CODE AS POLICY_STATUS,UB.ISSUE_OFFICE,UB.AGENT_CODE,
                ( select NAME from DMAG_VI_AGENT where AGENTCODE = UB.AGENT_CODE ) As AGENT_NAME,
                coalesce(UB.COMM_AMT, 0) As COMM_AMT,
                ( select NAME_EXT from CPGE_VI_PARTNERS where PART_ID = UB.CP_PART_ID) as CLIENT_NAME,
                UB.PREM_DUE as CN_PREM_DUE,
                (UB.PREM_DUE - coalesce(UB.COMM_AMT, 0)-coalesce(UB.GST_COMM_AMT, 0)) as NETT_PREM,
                UV.ISSUE_DATE AS TRAN_DATE,OP.CONTRACT_ID
                FROM OCP_POLICY_BASES OP,UWGE_POLICY_BASES UB,UWGE_POLICY_VERSIONS UV
                WHERE OP.CONTRACT_ID = UB.CONTRACT_ID(+)
                AND OP.CONTRACT_ID = UV.CONTRACT_ID(+)
                AND UV.VERSION_NO = UB.VERSION_NO--Redmine 95747 
                AND OP.VERSION_NO = UV.VERSION_NO--Redmine 95747 
                )  c
                WHERE  b.PYMT_NO = a.PYMT_NO and b.match_doc = c.POL_NO                
                AND ((p_ParmUploadDateFrom is null and p_ParmUploadDateTo is null) 
                or(TRUNC(a.UPLOAD_DATETIME) BETWEEN TO_DATE( p_ParmUploadDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmUploadDateTo, 'DD/MM/YYYY')))
                AND ((p_ParmBankInDateFrom is null and p_ParmBankInDateTo is null)
                or (a.PYMT_DATE BETWEEN TO_DATE( p_ParmBankInDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmBankInDateTo, 'DD/MM/YYYY')))
                AND ((p_ParmRcptDateFrom is null and p_ParmRcptDateTo is null)
                or (b.RCPT_DATE BETWEEN TO_DATE( p_ParmRcptDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmRcptDateTo, 'DD/MM/YYYY')))
                AND (p_ParmBatchCode is null or a.BATCH_CODE = p_ParmBatchCode)
                AND (p_ParmPymtType is null or a.BATCH_TYPE = p_ParmPymtType)
                AND (p_ParmCnoteBranch is null or c.ISSUE_OFFICE=p_ParmCnoteBranch)
                AND b.MATCH_AMT > 0 
                --and b.MATCH_AMT = a.PYMT_AMT
                AND a.PYMT_AMT = a.MATCH_AMT+NVL(a.MATCH_ADV_AMT,0)  
                AND  (a.DEL_IND is  null  or a.DEL_IND <> 'Y')  AND b.MATCH_TYPE <> 'GL'
                )
      LOOP

         r_banca_row.MATCH_DOC         := NULL;
         r_banca_row.CNOTE_DATE        := NULL;
         r_banca_row.STATUS            := NULL;
         r_banca_row.JPJ_STATUS        := NULL;
         r_banca_row.AGENT             := NULL;
         r_banca_row.AGENT_NAME        := NULL;
         r_banca_row.CLIENT_NAME       := NULL;
         r_banca_row.VEH_NO            := NULL;
         r_banca_row.CN_PREM_DUE       := NULL;
         r_banca_row.COMM_AMT          := NULL;
         r_banca_row.NETT_PREM         := NULL;
         r_banca_row.SRC_NO            := NULL;
         r_banca_row.SRC_NAME          := NULL;
         r_banca_row.CARD_NO           := NULL;
         r_banca_row.PYMT_REFNO        := NULL;
         r_banca_row.PYMT_DATE         := NULL;
         r_banca_row.PYMT_AMT          := NULL;
         r_banca_row.MH_MATCH_AMT      := NULL;
         r_banca_row.ROADTAX           := NULL;
         r_banca_row.MYEG              := NULL;
         r_banca_row.UPLOAD_DATETIME   := NULL;
         r_banca_row.RCPT_NO           := NULL;
         r_banca_row.RCPT_DATE         := NULL;
         r_banca_row.MANUAL_IND        := NULL;
         

         r_banca_row.MATCH_DOC         := r.MATCH_DOC;
         r_banca_row.CNOTE_DATE        := r.CNOTE_DATE;
         r_banca_row.STATUS            := r.STATUS;
         r_banca_row.JPJ_STATUS        := r.JPJ_STATUS;
         r_banca_row.AGENT             := r.AGENT_CODE;
         r_banca_row.AGENT_NAME        := r.AGENT_NAME;
         r_banca_row.VEH_NO            := r.VEH_NO;
         r_banca_row.CN_PREM_DUE       := r.CN_PREM_DUE;
         r_banca_row.COMM_AMT          := r.COMM_AMT;
         r_banca_row.NETT_PREM         := r.NETT_PREM;
         r_banca_row.SRC_NO            := r.SRC_NO;
         r_banca_row.SRC_NAME          := r.SRC_NAME;
         r_banca_row.CARD_NO           := r.CARD_NO;
         r_banca_row.PYMT_REFNO        := r.PYMT_REFNO;
         r_banca_row.PYMT_DATE         := r.PYMT_DATE;
         r_banca_row.PYMT_AMT          := r.PYMT_AMT;
         r_banca_row.MH_MATCH_AMT      := r.MH_MATCH_AMT;
         r_banca_row.ROADTAX           := r.ROADTAX;
         r_banca_row.MYEG              := r.MYEG;
         r_banca_row.UPLOAD_DATETIME   := r.UPLOAD_DATETIME;
         r_banca_row.RCPT_NO           := r.RCPT_NO;
         r_banca_row.RCPT_DATE         := r.RCPT_DATE;
         r_banca_row.MANUAL_IND        := r.MANUAL_IND;
         r_banca_row.CLIENT_NAME       := r.CLIENT_NAME;
         PIPE ROW (r_banca_row);
      END LOOP; */ 
    lStrSQL      := 'Select MATCH_DOC,CNOTE_DATE,STATUS,JPJ_STATUS, AGENT, AGENT_NAME,VEH_NO,CN_PREM_DUE, COMM_AMT,NETT_PREM,SRC_NO,SRC_NAME,CARD_NO, PYMT_REFNO,PYMT_DATE,PYMT_AMT, MH_MATCH_AMT, ROADTAX,MYEG,UPLOAD_DATETIME,RCPT_NO,RCPT_DATE,MANUAL_IND,CLIENT_NAME  from (SELECT a.PYMT_REFNO, a.INSURED_NAME,  a.BATCH_TYPE, '
                    ||' a.BANK_BRANCH, a.PYMT_DATE, a.PYMT_AMT, a.DEL_IND, a.PYMT_MODE_DESCP,  '
                    ||' a.PREM_DUE as PYMT_PREM_DUE, a.MATCH_AMT, a.PYMT_NO, '
                    ||' a.ADD_REMARKS, a.UPLOAD_DATETIME, a.BATCH_CODE,a.MANUAL_IND, '
                        -- start 12.00
                        --b.MATCH_DOC,
                    ||' CASE WHEN ACGC_PYMT_MATCH_INST.INST_CYCLE IS NOT NULL THEN b.MATCH_DOC||'':''||ACGC_PYMT_MATCH_INST.INST_CYCLE ELSE b.MATCH_DOC END as MATCH_DOC, '  -- end 12.00
                    ||'  b.RCPT_NO, b.RCPT_DATE,  b.MATCH_DATE, '
                    ||' CASE WHEN ACGC_PYMT_MATCH_INST.INST_CYCLE IS NOT NULL THEN ACGC_PYMT_MATCH_INST.MATCH_AMT ELSE b.MATCH_AMT END as MH_MATCH_AMT,b.MATCH_TYPE, ' -- 12.00
                    ||' ( COALESCE((select py.match_amt from acrc_pymt_match_det py where py.pymt_no=b.PYMT_NO and py.match_cn = b.MATCH_DOC and SUBSTR(py.match_doc,LENGTH(py.match_doc)-4,LENGTH(py.match_doc))=''95054''), 0) ) As ROADTAX,  '
                    ||' ( COALESCE((select py.match_amt from acrc_pymt_match_det py where py.pymt_no=b.PYMT_NO and py.match_cn=b.MATCH_DOC and SUBSTR(py.match_doc,LENGTH(py.match_doc)-4,LENGTH(py.match_doc))=''63093''), 0) ) As MYEG, '
                    ||' c.CNOTE_DATE, c.STATUS, d.JPJ_STATUS, c.CNOTE_BRANCH_CODE, c.AGENT_CODE as AGENT, ( select NAME from DMAG_VI_AGENT where AGENTCODE = c.AGENT_CODE ) As AGENT_NAME, '
                    ||' coalesce(c.COMM_AMT, 0)  As COMM_AMT, c.NAME as CLIENT_NAME, d.VEH_NO,'
                    ||' c.PREM_DUE as CN_PREM_DUE, c.NETT_PREM, a.SRC_NO, a.SRC_NAME, a.CARD_NO, a.EDC_BATCH_NO, a.SETTLE_DATE, a.STMT_BATCH_NO,a.MATCH_ADV_AMT,c.POL_NO,c.EFF_DATE AS POL_EDATE '
                      ||' FROM ACRC_PYMT_UPLOAD a, ACRC_PYMT_MATCH b '
                      ||'  LEFT JOIN ACGC_PYMT_MATCH_INST ON b.MATCH_DOC = ACGC_PYMT_MATCH_INST.MATCH_DOC '  --  12.00
                      ||'  , CNGE_NOTE c, CNGE_NOTE_MT d  '
                      ||'  WHERE  b.PYMT_NO = a.PYMT_NO and b.match_doc = c.CNOTE_NO and c.CNOTE_NO = d.CNOTE_NO ';
                      
         if(p_ParmPymtType is not null) then
             lStrSQL :=lStrSQL ||'  AND a.BATCH_TYPE  ='''||p_ParmPymtType||'''';
          END IF;
        if(p_ParmBatchCode is not null) then
             lStrSQL :=lStrSQL ||'  AND a.BATCH_CODE  ='''||p_ParmBatchCode||'''';
          END IF;
        if(p_ParmCnoteBranch is not null) then
             lStrSQL :=lStrSQL ||'  AND c.CNOTE_BRANCH_CODE  ='''||p_ParmCnoteBranch||'''';
          END IF;
        if(p_ParmUploadDateFrom is not null and p_ParmUploadDateTo is not null) then
             lStrSQL :=lStrSQL ||'  AND TRUNC(a.UPLOAD_DATETIME) BETWEEN TO_DATE('''|| p_ParmUploadDateFrom||''', ''DD/MM/YYYY'' )  AND TO_DATE('''|| p_ParmUploadDateTo||''', ''DD/MM/YYYY'' ) ';
          END IF;
        if(p_ParmBankInDateFrom is not null and p_ParmBankInDateTo is not null) then
             lStrSQL :=lStrSQL ||'  AND a.PYMT_DATE BETWEEN TO_DATE('''|| p_ParmBankInDateFrom||''', ''DD/MM/YYYY'' )  AND TO_DATE('''|| p_ParmBankInDateTo||''', ''DD/MM/YYYY'' ) ';
          END IF;
        if(p_ParmRcptDateFrom is not null and p_ParmRcptDateTo is not null) then
             lStrSQL :=lStrSQL ||'  AND b.RCPT_DATE BETWEEN TO_DATE('''|| p_ParmRcptDateFrom||''', ''DD/MM/YYYY'' )  AND TO_DATE('''|| p_ParmRcptDateTo||''', ''DD/MM/YYYY'' ) ';
          END IF;
         lStrSQL :=lStrSQL ||'  AND a.MATCH_AMT > 0 AND  a.PYMT_AMT = a.MATCH_AMT+NVL(a.MATCH_ADV_AMT,0)  ';
         lStrSQL :=lStrSQL ||' AND  (a.DEL_IND is  null  or a.DEL_IND <> ''Y'')  AND b.MATCH_TYPE <> ''GL'' ';        
         
         lStrSQL  :=lStrSQL||' UNION SELECT  a.PYMT_REFNO, a.INSURED_NAME, a.BATCH_TYPE, a.BANK_BRANCH, a.PYMT_DATE, b.PYMT_AMT, a.DEL_IND, a.PYMT_MODE_DESCP, '
                    ||' a.PREM_DUE as PYMT_PREM_DUE, b.MATCH_AMT, a.PYMT_NO,  a.ADD_REMARKS, a.UPLOAD_DATETIME, a.BATCH_CODE, a.MANUAL_IND,  '
                      --NVL(c.CNOTE_NO,b.MATCH_DOC) AS MATCH_DOC , -- start 12.00
                    ||' NVL(c.CNOTE_NO,CASE WHEN ACGC_PYMT_MATCH_INST.INST_CYCLE IS NOT NULL THEN b.MATCH_DOC||'':''||ACGC_PYMT_MATCH_INST.INST_CYCLE ELSE b.MATCH_DOC END) AS MATCH_DOC , ' -- 12.00
                    ||' b.RCPT_NO, b.RCPT_DATE,  b.MATCH_DATE, '
                        --b.MATCH_AMT as MH_MATCH_AMT, 
                    ||' CASE WHEN ACGC_PYMT_MATCH_INST.INST_CYCLE IS NOT NULL THEN ACGC_PYMT_MATCH_INST.MATCH_AMT ELSE b.MATCH_AMT END as MH_MATCH_AMT,b.MATCH_TYPE, '  -- end 12.00
                    ||'  ( COALESCE((select py.match_amt from acrc_pymt_match_det py where py.pymt_no=b.PYMT_NO and py.match_cn = b.MATCH_DOC and SUBSTR(py.match_doc,LENGTH(py.match_doc)-4,LENGTH(py.match_doc))=''95054''), 0) ) As ROADTAX, '
                    ||' ( COALESCE((select py.match_amt from acrc_pymt_match_det py where py.pymt_no=b.PYMT_NO and py.match_cn=b.MATCH_DOC and SUBSTR(py.match_doc,LENGTH(py.match_doc)-4,LENGTH(py.match_doc))=''63093''), 0) ) As MYEG, ' -- 12.00
                    ||' c.CNOTE_DATE, c.POLICY_STATUS, '''',  c.ISSUE_OFFICE, c.AGENT_CODE as AGENT, c.AGENT_NAME,  '
                     --c.COMM_AMT,
                    ||'  B.PYMT_COMM_AMT, ' --Redmine 95747
                    ||' c.CLIENT_NAME, (SELECT VEH_NO FROM UWGE_RISK_VEH WHERE CONTRACT_ID = C.CONTRACT_ID and TOP_INDICATOR=''Y'') AS VEH_NO  , '
                    --c.CN_PREM_DUE,
                    ||'  B.PYMT_PREM_AMT as CN_PREM_DUE, '  --Redmine 95747
                    --c.NETT_PREM, 
                    ||' (B.PYMT_PREM_AMT  - COALESCE (B.PYMT_COMM_AMT, 0)  - COALESCE (B.PYMT_GST_COMM_AMT, 0))   AS NETT_PREM, ' --Redmine 95747 
                      ||' a.SRC_NO, a.SRC_NAME, a.CARD_NO, a.EDC_BATCH_NO, a.SETTLE_DATE, a.STMT_BATCH_NO,a.MATCH_ADV_AMT,c.POL_NO,c.TRAN_DATE '
                      ||'  FROM ACRC_PYMT_UPLOAD a, ACRC_PYMT_MATCH b '   
                      ||'  LEFT JOIN ACGC_PYMT_MATCH_INST ON b.MATCH_DOC = ACGC_PYMT_MATCH_INST.MATCH_DOC '  --start  12.00
                      ||'  , (SELECT UB.CNOTE_NO,OP.POLICY_REF AS POL_NO,UB.CNOTE_DATE,OP.ACTION_CODE AS POLICY_STATUS,UB.ISSUE_OFFICE,UB.AGENT_CODE, '  --  12.00
                      ||'  ( select NAME from DMAG_VI_AGENT where AGENTCODE = UB.AGENT_CODE ) As AGENT_NAME, '  
                      ||'  coalesce(UB.COMM_AMT, 0) As COMM_AMT, '   
                      ||'  ( select NAME_EXT from CPGE_VI_PARTNERS where PART_ID = UB.CP_PART_ID) as CLIENT_NAME, '   
                      ||'   UV.ISSUE_DATE AS TRAN_DATE,OP.CONTRACT_ID '   
                      ||'  FROM OCP_POLICY_BASES OP,UWGE_POLICY_BASES UB,UWGE_POLICY_VERSIONS UV '   
                      ||'  WHERE OP.CONTRACT_ID = UB.CONTRACT_ID(+) '  
                      ||' AND OP.CONTRACT_ID = UV.CONTRACT_ID(+)  '
                      ||'   AND UV.VERSION_NO = UB.VERSION_NO '  --Redmine 95747 
                      ||'  AND OP.VERSION_NO = UV.VERSION_NO '  --Redmine 95747 
                      ||'   )  c '
                      ||'   WHERE  b.PYMT_NO = a.PYMT_NO and b.match_doc = c.POL_NO ' 
                      ||'  AND b.PYMT_NO = ACGC_PYMT_MATCH_INST.PYMT_NO ' ; --30.00
                      
                if(p_ParmUploadDateFrom is not null and p_ParmUploadDateTo is not null) then
                      lStrSQL :=lStrSQL ||'  AND TRUNC(a.UPLOAD_DATETIME) BETWEEN TO_DATE('''|| p_ParmUploadDateFrom||''', ''DD/MM/YYYY'' )  AND TO_DATE('''|| p_ParmUploadDateTo||''', ''DD/MM/YYYY'' ) ';
                END IF; 
                if(p_ParmBankInDateFrom is not null and p_ParmBankInDateTo is not null) then
                      lStrSQL :=lStrSQL ||'  AND a.PYMT_DATE BETWEEN TO_DATE('''|| p_ParmBankInDateFrom||''', ''DD/MM/YYYY'' )  AND TO_DATE('''|| p_ParmBankInDateTo||''', ''DD/MM/YYYY'' ) ';
                END IF;
                if(p_ParmRcptDateFrom is not null and p_ParmRcptDateTo is not null) then
                      lStrSQL :=lStrSQL ||'  AND b.RCPT_DATE BETWEEN TO_DATE('''|| p_ParmRcptDateFrom||''', ''DD/MM/YYYY'' )  AND TO_DATE('''|| p_ParmRcptDateTo||''', ''DD/MM/YYYY'' ) ';
                END IF;   
                if(p_ParmBatchCode is not null) then
                      lStrSQL :=lStrSQL ||'  AND a.BATCH_CODE  ='''||p_ParmBatchCode||'''';
                END IF;
                   if(p_ParmPymtType is not null) then
                      lStrSQL :=lStrSQL ||'  AND a.BATCH_TYPE  ='''||p_ParmPymtType||'''';
                END IF; 
                if(p_ParmCnoteBranch is not null) then
                      lStrSQL :=lStrSQL ||'  AND c.ISSUE_OFFICE  ='''||p_ParmCnoteBranch||'''';
                END IF;
         lStrSQL :=lStrSQL ||'  AND b.MATCH_AMT > 0  ';
         lStrSQL :=lStrSQL ||' AND a.PYMT_AMT = a.MATCH_AMT+NVL(a.MATCH_ADV_AMT,0)  ';
         lStrSQL :=lStrSQL ||' AND  (a.DEL_IND is  null  or a.DEL_IND <> ''Y'')  AND b.MATCH_TYPE <> ''GL'' )  ';
                DBMS_OUTPUT.put_line ('lStrSQL - ' || lStrSQL);        
                OPEN v_cursor FOR lStrSQL;
                    LOOP
                    FETCH v_cursor
                        INTO 
                            r_banca_row.MATCH_DOC,r_banca_row.CNOTE_DATE,r_banca_row.STATUS,r_banca_row.JPJ_STATUS, r_banca_row.AGENT, r_banca_row.AGENT_NAME,r_banca_row.VEH_NO,r_banca_row.CN_PREM_DUE,  
                            r_banca_row.COMM_AMT,r_banca_row.NETT_PREM,r_banca_row.SRC_NO,r_banca_row.SRC_NAME,r_banca_row.CARD_NO, r_banca_row.PYMT_REFNO,r_banca_row.PYMT_DATE,r_banca_row.PYMT_AMT,       
                            r_banca_row.MH_MATCH_AMT, r_banca_row.ROADTAX,r_banca_row.MYEG,r_banca_row.UPLOAD_DATETIME,r_banca_row.RCPT_NO,r_banca_row.RCPT_DATE,r_banca_row.MANUAL_IND,r_banca_row.CLIENT_NAME;                               
           exit when v_cursor%NOTFOUND;
           PIPE ROW (r_banca_row);
         END LOOP;                             
       RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   END FN_GEN_CALL_CENTER_MATCHED;
    --  33.00  End   
  --BANCA MOTOR PAYMENT MATCHED LISTING
   FUNCTION FN_GEN_BANCA_MT_MATCHED (p_ParmPymtCat           VARCHAR2,
                                     p_ParmPymtType          VARCHAR2,
                                     p_ParmBatchCode         VARCHAR2,
                                     p_ParmCnoteBranch       VARCHAR2,
                                     p_ParmUploadDateFrom    VARCHAR2,
                                     p_ParmUploadDateTo      VARCHAR2,
                                     p_ParmBankInDateFrom    VARCHAR2,
                                     p_ParmBankInDateTo      VARCHAR2,
                                     p_ParmRcptDateFrom      VARCHAR2,
                                     p_ParmRcptDateTo        VARCHAR2
                                    )
      RETURN BANCA_MT_MATCHED_TAB
      PIPELINED
   IS
      v_ProcName_v    VARCHAR2 (30) := 'FN_GEN_BANCA_MT_MATCHED';
      v_Step_v        VARCHAR2 (5) := '000';
      r_banca_row     BANCA_MT_MATCHED_REC;
      row_count       NUMBER := 1;
      v_cpPartId_n    uwge_policy_bases.cp_part_id%TYPE;
      v_cpPartVer_n   uwge_policy_bases.cp_version%TYPE;
   BEGIN
      FOR r
         IN (SELECT
a.PYMT_REFNO,
a.INSURED_NAME,
a.BATCH_TYPE,
a.BANK_BRANCH,
a.PYMT_DATE,
a.PYMT_AMT,
a.DEL_IND,
a.PYMT_MODE_DESCP,
a.PREM_DUE as PYMT_PREM_DUE,
a.MATCH_AMT,
a.PYMT_NO,
a.ADD_REMARKS,
a.UPLOAD_DATETIME,
a.BATCH_CODE,
a.MANUAL_IND,
b.MATCH_DOC,
b.RCPT_NO,
b.RCPT_DATE,
b.MATCH_DATE,
b.MATCH_AMT as MH_MATCH_AMT,
b.MATCH_TYPE,
( COALESCE((select py.match_amt from acrc_pymt_match_det py where py.pymt_no=b.PYMT_NO and py.match_cn = b.MATCH_DOC and SUBSTR(py.match_doc,LENGTH(py.match_doc)-4,LENGTH(py.match_doc))='95054'), 0) ) As ROADTAX,
( COALESCE((select py.match_amt from acrc_pymt_match_det py where py.pymt_no=b.PYMT_NO and py.match_cn=b.MATCH_DOC and SUBSTR(py.match_doc,LENGTH(py.match_doc)-4,LENGTH(py.match_doc))='63093'), 0) ) As MYEG,
c.ISSUE_DATE AS CNOTE_DATE,
c.CONTRACT_STATUS AS STATUS,
(SELECT JPJ_STATUS FROM CNGE_NOTE_MT WHERE CNOTE_NO = c.CONTRACT_NO) AS JPJ_STATUS,
DMAG.branch_code  AS CNOTE_BRANCH,
c.AGENT_CODE,
DMAG.NAME As AGENT_NAME,
coalesce(C.COMM_AMT, 0) As COMM_AMT,
(SELECT NAME FROM CNGE_NOTE WHERE CNOTE_NO = c.CONTRACT_NO) as CLIENT_NAME,
(SELECT VEH_NO FROM CNGE_NOTE_MT WHERE CNOTE_NO = c.CONTRACT_NO ) AS VEH_NO,
c.PREM_DUE as CN_PREM_DUE,
c.NETT_PREM,
a.SRC_NO,
a.SRC_NAME,
a.CARD_NO,
a.EDC_BATCH_NO,
a.SETTLE_DATE,
a.STMT_BATCH_NO,
a.MATCH_ADV_AMT,
c.POL_NO,
c.EFF_DATE
FROM ACRC_PYMT_UPLOAD a, ACRC_PYMT_MATCH b, CNUW_BASES c,DMAG_VI_AGENT DMAG
WHERE b.PYMT_NO = a.PYMT_NO
and b.match_doc = c.CONTRACT_NO
AND DMAG.agentcode=c.agent_code
AND ((p_ParmUploadDateFrom is null and p_ParmUploadDateTo is null) 
or(TRUNC(a.UPLOAD_DATETIME) BETWEEN TO_DATE( p_ParmUploadDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmUploadDateTo, 'DD/MM/YYYY')))
AND ((p_ParmBankInDateFrom is null and p_ParmBankInDateTo is null)
or (a.PYMT_DATE BETWEEN TO_DATE( p_ParmBankInDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmBankInDateTo, 'DD/MM/YYYY')))
AND ((p_ParmRcptDateFrom is null and p_ParmRcptDateTo is null)
or (b.RCPT_DATE BETWEEN TO_DATE( p_ParmRcptDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmRcptDateTo, 'DD/MM/YYYY')))
AND (p_ParmBatchCode is null or a.BATCH_CODE = p_ParmBatchCode)
AND (p_ParmPymtType is null or a.BATCH_TYPE = p_ParmPymtType)
AND (p_ParmCnoteBranch is null or DMAG.BRANCH_CODE=p_ParmCnoteBranch)
UNION
SELECT
a.PYMT_REFNO,
a.INSURED_NAME,
a.BATCH_TYPE,
a.BANK_BRANCH,
a.PYMT_DATE,
b.PYMT_AMT,
a.DEL_IND,
a.PYMT_MODE_DESCP,
a.PREM_DUE as PYMT_PREM_DUE,
b.MATCH_AMT,
a.PYMT_NO,
a.ADD_REMARKS,
a.UPLOAD_DATETIME,
a.BATCH_CODE,
a.MANUAL_IND,
b.MATCH_DOC,
b.RCPT_NO,
b.RCPT_DATE,
b.MATCH_DATE,
b.MATCH_AMT as MH_MATCH_AMT,
b.MATCH_TYPE,
( COALESCE((select py.match_amt from acrc_pymt_match_det py where py.pymt_no=b.PYMT_NO and py.match_cn = b.MATCH_DOC and SUBSTR(py.match_doc,LENGTH(py.match_doc)-4,LENGTH(py.match_doc))='95054'), 0) ) As ROADTAX,
( COALESCE((select py.match_amt from acrc_pymt_match_det py where py.pymt_no=b.PYMT_NO and py.match_cn=b.MATCH_DOC and SUBSTR(py.match_doc,LENGTH(py.match_doc)-4,LENGTH(py.match_doc))='63093'), 0) ) As MYEG,
c.CNOTE_DATE,
c.POLICY_STATUS AS STATUS,
'',
c.ISSUE_OFFICE,
c.AGENT_CODE,
C.AGENT_NAME,
C.COMM_AMT,
C.CLIENT_NAME,
'',
C.CN_PREM_DUE,
c.NETT_PREM,
a.SRC_NO,
a.SRC_NAME,
a.CARD_NO,
a.EDC_BATCH_NO,
a.SETTLE_DATE,
a.STMT_BATCH_NO,
a.MATCH_ADV_AMT,
c.POL_NO,
c.TRAN_DATE
FROM ACRC_PYMT_UPLOAD a, ACRC_PYMT_MATCH b, 
(SELECT OP.POLICY_REF AS POL_NO,UB.CNOTE_DATE,OP.ACTION_CODE AS POLICY_STATUS,UB.ISSUE_OFFICE,UB.AGENT_CODE,
( select NAME from DMAG_VI_AGENT where AGENTCODE = UB.AGENT_CODE ) As AGENT_NAME,
coalesce(UB.COMM_AMT, 0) As COMM_AMT,
( select NAME_EXT from CPGE_VI_PARTNERS where PART_ID = UB.CP_PART_ID) as CLIENT_NAME,
UB.PREM_DUE as CN_PREM_DUE,
(UB.PREM_DUE - coalesce(UB.COMM_AMT, 0)-coalesce(UB.GST_COMM_AMT, 0)) as NETT_PREM,
UV.ISSUE_DATE AS TRAN_DATE
FROM OCP_POLICY_BASES OP,UWGE_POLICY_BASES UB,UWGE_POLICY_VERSIONS UV
WHERE OP.CONTRACT_ID = UB.CONTRACT_ID(+)
AND OP.CONTRACT_ID = UV.CONTRACT_ID(+))  c
WHERE b.PYMT_NO = a.PYMT_NO
and b.match_doc = c.POL_NO
AND ((p_ParmUploadDateFrom is null and p_ParmUploadDateTo is null) 
or(TRUNC(a.UPLOAD_DATETIME) BETWEEN TO_DATE( p_ParmUploadDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmUploadDateTo, 'DD/MM/YYYY')))
AND ((p_ParmBankInDateFrom is null and p_ParmBankInDateTo is null)
or (a.PYMT_DATE BETWEEN TO_DATE( p_ParmBankInDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmBankInDateTo, 'DD/MM/YYYY')))
AND ((p_ParmRcptDateFrom is null and p_ParmRcptDateTo is null)
or (b.RCPT_DATE BETWEEN TO_DATE( p_ParmRcptDateFrom, 'DD/MM/YYYY')  AND TO_DATE( p_ParmRcptDateTo, 'DD/MM/YYYY')))
AND (p_ParmBatchCode is null or a.BATCH_CODE = p_ParmBatchCode)
AND (p_ParmPymtType is null or a.BATCH_TYPE = p_ParmPymtType)
AND (p_ParmCnoteBranch is null or c.ISSUE_OFFICE=p_ParmCnoteBranch)
)
      LOOP
         --            VIEW_PYMTS_UPLOAD

         r_banca_row.MATCH_DOC         := NULL;
         r_banca_row.CNOTE_DATE        := NULL;
         r_banca_row.STATUS            := NULL;
         r_banca_row.JPJ_STATUS        := NULL;
         r_banca_row.AGENT             := NULL;
         r_banca_row.AGENT_NAME        := NULL;
         r_banca_row.CLIENT_NAME       := NULL;
         r_banca_row.VEH_NO            := NULL;
         r_banca_row.CN_PREM_DUE       := NULL;
         r_banca_row.COMM_AMT          := NULL;
         r_banca_row.NETT_PREM         := NULL;
         r_banca_row.SRC_NO            := NULL;
         r_banca_row.SRC_NAME          := NULL;
         r_banca_row.CARD_NO           := NULL;
         r_banca_row.PYMT_REFNO        := NULL;
         r_banca_row.PYMT_DATE         := NULL;
         r_banca_row.PYMT_AMT          := NULL;
         r_banca_row.MH_MATCH_AMT      := NULL;
         r_banca_row.ROADTAX           := NULL;
         r_banca_row.MYEG              := NULL;
         r_banca_row.UPLOAD_DATETIME   := NULL;
         r_banca_row.RCPT_NO           := NULL;
         r_banca_row.RCPT_DATE         := NULL;
         r_banca_row.MANUAL_IND        := NULL;
         

         r_banca_row.MATCH_DOC         := r.MATCH_DOC;
         r_banca_row.CNOTE_DATE        := r.CNOTE_DATE;
         r_banca_row.STATUS            := r.STATUS;
         r_banca_row.JPJ_STATUS        := r.JPJ_STATUS;
         r_banca_row.AGENT             := r.AGENT_CODE;
         r_banca_row.AGENT_NAME        := r.AGENT_NAME;
         r_banca_row.VEH_NO            := r.VEH_NO;
         r_banca_row.CN_PREM_DUE       := r.CN_PREM_DUE;
         r_banca_row.COMM_AMT          := r.COMM_AMT;
         r_banca_row.NETT_PREM         := r.NETT_PREM;
         r_banca_row.SRC_NO            := r.SRC_NO;
         r_banca_row.SRC_NAME          := r.SRC_NAME;
         r_banca_row.CARD_NO           := r.CARD_NO;
         r_banca_row.PYMT_REFNO        := r.PYMT_REFNO;
         r_banca_row.PYMT_DATE         := r.PYMT_DATE;
         r_banca_row.PYMT_AMT          := r.PYMT_AMT;
         r_banca_row.MH_MATCH_AMT      := r.MH_MATCH_AMT;
         r_banca_row.ROADTAX           := r.ROADTAX;
         r_banca_row.MYEG              := r.MYEG;
         r_banca_row.UPLOAD_DATETIME   := r.UPLOAD_DATETIME;
         r_banca_row.RCPT_NO           := r.RCPT_NO;
         r_banca_row.RCPT_DATE         := r.RCPT_DATE;
         r_banca_row.MANUAL_IND        := r.MANUAL_IND;
         r_banca_row.CLIENT_NAME       := r.CLIENT_NAME;
         PIPE ROW (r_banca_row);
      END LOOP;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   END FN_GEN_BANCA_MT_MATCHED;


   FUNCTION FN_RPAC_PREMCOLLECT (p_StrProcMonth     VARCHAR2,
                                 p_StrProcYear      VARCHAR2,
                                 p_StrRegionFrom    VARCHAR2,
                                 p_StrRegionTo      VARCHAR2,
                                 p_StrBranchFrom    VARCHAR2,
                                 p_StrBranchTo      VARCHAR2,
                                 p_StrAcctSource    VARCHAR2,
                                 p_StrAgentFrom     VARCHAR2,
                                 p_StrAgentTo       VARCHAR2,
                                 p_gSummaryInd      VARCHAR2
                                )
      RETURN RPAC_PREMCOLLECT_TAB
      PIPELINED
   IS
      v_ProcName_v   VARCHAR2 (30) := 'FN_RPAC_PREMCOLLECT';
      v_Step_v       VARCHAR2 (5) := '000';
      r_row          RPAC_PREMCOLLECT_REC;
      TYPE v_cursor_type IS REF CURSOR;   
      v_cursor       v_cursor_type;
      lStrSQL varchar2(32000);
      v_branch_from  VARCHAR2(2);
      v_branch_to    VARCHAR2(10);
      v_ac_source    VARCHAR2(2);
      v_proc_mth     VARCHAR2(2);
      v_proc_yr      VARCHAR2(4);
      v_region_from  VARCHAR2(10);
      v_region_to    VARCHAR2(10);
      v_agent_from   VARCHAR2(20);
      v_agent_to     VARCHAR2(20);
      v_sum_id          VARCHAR2(10);
      
   BEGIN
    v_branch_from := p_StrBranchFrom;
    v_branch_to := p_StrBranchTo;
    v_ac_source := p_StrAcctSource;
    v_proc_mth := p_StrProcMonth;
    v_proc_yr := p_StrProcYear;

    IF p_StrRegionFrom IS NULL THEN 
        v_region_from := ''; 
    ELSE 
        v_region_from := p_StrRegionFrom;
    END IF;

    IF p_StrRegionTo IS NULL THEN 
        v_region_to := ''; 
    ELSE 
        v_region_to := p_StrRegionTo;
    END IF;

    IF p_StrAgentFrom IS NULL THEN 
        v_agent_from := ''; 
    ELSE 
        v_agent_from := p_StrAgentFrom;
    END IF;

    IF p_StrAgentTo IS NULL THEN 
        v_agent_to := ''; 
    ELSE 
        v_agent_to := p_StrAgentTo;
    END IF;

    IF p_gSummaryInd IS NULL THEN 
        v_sum_id := ''; 
    ELSE 
        v_sum_id := p_gSummaryInd;
    END IF;

      IF v_ac_source = 'A'
               THEN
            
    lStrSQL      := 'SELECT TBL.AGENT_ID, AGENT_CAT_TYPE,AGENT_CODE, BRANCH_CODE, REGION_CODE, BRANCH_NAME, REGION_DESCP, AGENT_NAME, '
                    ||' SUM (NVL ((CASE WHEN AGE_DAYS <= 14 THEN NVL (DOC_AMT, 0) END), 0)) AGE0_14DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS BETWEEN 15 AND 30 THEN NVL (DOC_AMT, 0) END), 0)) AGE15_30DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS BETWEEN 31 AND 60 THEN NVL (DOC_AMT, 0) END), 0)) AGE31_60DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS BETWEEN 61 AND 90 THEN NVL (DOC_AMT, 0) END), 0)) AGE61_90DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS BETWEEN 91 AND 120 THEN NVL (DOC_AMT, 0) END), 0)) AGE91_120DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS BETWEEN 121 AND 150 THEN NVL (DOC_AMT, 0) END), 0)) AGE121_150DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS BETWEEN 151 AND 180 THEN NVL (DOC_AMT, 0) END), 0)) AGE151_180DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS BETWEEN 181 AND 330 THEN NVL (DOC_AMT, 0) END), 0)) AGE181_330DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS BETWEEN 331 AND 360 THEN NVL (DOC_AMT, 0) END), 0)) AGE331_360DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS > 360 THEN NVL (DOC_AMT, 0) END), 0)) ABOVE360DAYS '
                    ||' , (SUM (NVL ((CASE WHEN AGE_DAYS IS NULL OR NVL (AGE_DAYS, 0) < 0 THEN NVL (DOC_AMT, 0) END), 0)) + COALESCE(VI.AC_CR_AMT,0)) AS UNIDENTIFIEDAMT '
                    ||' FROM VIEW_PREM_STMT TBL LEFT JOIN (SELECT GL.AC_AGENT_ID, COALESCE(SUM(GL.AC_CR_AMT),0) AS AC_CR_AMT '
                      ||'  FROM ACRC_RCPT RC LEFT JOIN ACRC_GL GL ON RC.AC_NO=GL.AC_NO '
                      ||'  WHERE RC.PROC_MTH= TO_NUMBER ('|| v_proc_mth ||')  '
                      ||' AND RC.PROC_YR= TO_NUMBER ('|| v_proc_yr ||')  '
                      ||' AND GL.KO_IND = ''N'' '
                      ||' GROUP BY GL.AC_AGENT_ID '
                     ||' ) VI ON VI.AC_AGENT_ID = TBL.AGENT_ID '
                    ||' WHERE AGT_CAT = ''AG''  '
                    ||' AND PROC_YR = TO_NUMBER ('|| v_proc_yr ||')  '
                    ||' AND PROC_MTH = TO_NUMBER ('|| v_proc_mth ||')  ' 
                    ||' AND (( ''' || v_branch_from ||''' IS NOT NULL AND BRANCH_CODE BETWEEN '''|| v_branch_from ||''' AND '''|| v_branch_to||''') OR '''|| v_branch_from ||''' IS NULL) '; 
                    
                    
        IF v_region_from <> '' AND  v_region_to <> '' THEN
                lStrSQL :=lStrSQL ||' AND (('''||v_region_from||''' IS NOT NULL AND region_code BETWEEN '''|| v_region_from||''' AND ''' ||v_region_to ||''') OR '''||v_region_from ||''' IS NULL) ';
        END IF;  
        IF v_agent_from <> '' AND  v_agent_to <> '' THEN
                lStrSQL :=lStrSQL ||' AND (('''||v_agent_from||''' IS NOT NULL AND agent_code BETWEEN '''||v_agent_from||''' AND '''||v_agent_to||''') OR '''||v_agent_from||' IS NULL) ';
        END IF;
        lStrSQL :=lStrSQL ||' GROUP BY AGENT_ID, AGENT_CODE,AGENT_CAT_TYPE,AGENT_CODE, BRANCH_CODE, REGION_CODE, BRANCH_NAME, REGION_DESCP, AGENT_NAME,AC_CR_AMT ORDER BY AGENT_ID ';

                    DBMS_OUTPUT.put_line ('lStrSQL'||lStrSQL);        
                OPEN v_cursor FOR lStrSQL;
                    LOOP
                    FETCH v_cursor
                        INTO 
                            r_row.GL_AGENT_ID, r_row.AC_AGENT_CAT_TYPE, r_row.AGENT_CODE, r_row.BRANCH_CODE,r_row.REGION_CODE, r_row.BRANCH_NAME,r_row.REGION_NAME,r_row.AGENT_NAME,
                            r_row.AGE0_14DAYSAMT, r_row.AGE15_30DAYSAMT, r_row.AGE31_60DAYSAMT, r_row.AGE61_90DAYSAMT, r_row.AGE91_120DAYSAMT, r_row.AGE121_150DAYSAMT,
                            r_row.AGE151_180DAYSAMT,r_row.AGE181_330DAYSAMT,r_row.AGE331_360DAYSAMT,r_row.ABOVE360DAYSAMT,r_row.UNIDENTIFIEDAMT;
                            EXIT WHEN v_cursor%NOTFOUND;

            PIPE ROW (r_row);
         END LOOP;
                CLOSE v_cursor;
      ELSE
        lStrSQL      := 'SELECT TBL.AGENT_ID, AGENT_CAT_TYPE,AGENT_CODE, BRANCH_CODE, REGION_CODE, BRANCH_NAME, REGION_DESCP, AGENT_NAME, '
                    ||' SUM (NVL ((CASE WHEN AGE_DAYS <= 14 THEN NVL (DOC_AMT, 0) END), 0)) AGE0_14DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS BETWEEN 15 AND 30 THEN NVL (DOC_AMT, 0) END), 0)) AGE15_30DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS BETWEEN 31 AND 60 THEN NVL (DOC_AMT, 0) END), 0)) AGE31_60DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS BETWEEN 61 AND 90 THEN NVL (DOC_AMT, 0) END), 0)) AGE61_90DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS BETWEEN 91 AND 120 THEN NVL (DOC_AMT, 0) END), 0)) AGE91_120DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS BETWEEN 121 AND 150 THEN NVL (DOC_AMT, 0) END), 0)) AGE121_150DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS BETWEEN 151 AND 180 THEN NVL (DOC_AMT, 0) END), 0)) AGE151_180DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS BETWEEN 181 AND 330 THEN NVL (DOC_AMT, 0) END), 0)) AGE181_330DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS BETWEEN 331 AND 360 THEN NVL (DOC_AMT, 0) END), 0)) AGE331_360DAYS '
                    ||' , SUM (NVL ((CASE WHEN AGE_DAYS > 360 THEN NVL (DOC_AMT, 0) END), 0)) ABOVE360DAYS '
                    ||' , (SUM (NVL ((CASE WHEN AGE_DAYS IS NULL OR NVL (AGE_DAYS, 0) < 0 THEN NVL (DOC_AMT, 0) END), 0)) + COALESCE(VI.AC_CR_AMT,0)) AS UNIDENTIFIEDAMT '
                    ||' FROM VIEW_PREM_STMT TBL LEFT JOIN (SELECT GL.AC_AGENT_ID, COALESCE(SUM(GL.AC_CR_AMT),0) AS AC_CR_AMT '
                      ||'  FROM ACRC_RCPT RC LEFT JOIN ACRC_GL GL ON RC.AC_NO=GL.AC_NO '
                      ||'  WHERE RC.PROC_MTH= TO_NUMBER ('|| v_proc_mth ||')  '
                      ||' AND RC.PROC_YR= TO_NUMBER ('|| v_proc_yr ||')  '
                      ||' AND GL.KO_IND = ''N'' '
                      ||' GROUP BY GL.AC_AGENT_ID '
                     ||' ) VI ON VI.AC_AGENT_ID = TBL.AGENT_ID '
                    ||' WHERE AGT_CAT = ''RI''  '
                    ||' AND PROC_YR = TO_NUMBER ('|| v_proc_yr ||')  '
                    ||' AND PROC_MTH = TO_NUMBER ('|| v_proc_mth ||')  ' 
                    ||' AND (( ''' || v_branch_from ||''' IS NOT NULL AND BRANCH_CODE BETWEEN '''|| v_branch_from ||''' AND '''|| v_branch_to||''') OR '''|| v_branch_from ||''' IS NULL) '; 


        IF v_region_from <> '' AND  v_region_to <> '' THEN
                lStrSQL :=lStrSQL ||' AND (('''||v_region_from||''' IS NOT NULL AND region_code BETWEEN '''|| v_region_from||''' AND ''' ||v_region_to ||''') OR '''||v_region_from ||''' IS NULL) ';
        END IF;  
        IF v_agent_from <> '' AND  v_agent_to <> '' THEN
                lStrSQL :=lStrSQL ||' AND (('''||v_agent_from||''' IS NOT NULL AND agent_code BETWEEN '''||v_agent_from||''' AND '''||v_agent_to||''') OR '''||v_agent_from||' IS NULL) ';
        END IF;
        lStrSQL :=lStrSQL ||' GROUP BY AGENT_ID, AGENT_CODE,AGENT_CAT_TYPE,AGENT_CODE, BRANCH_CODE, REGION_CODE, BRANCH_NAME, REGION_DESCP, AGENT_NAME,AC_CR_AMT ORDER BY AGENT_ID ';

                    DBMS_OUTPUT.put_line ('lStrSQL'||lStrSQL);        
                OPEN v_cursor FOR lStrSQL;
                    LOOP
                    FETCH v_cursor
                        INTO 
                            r_row.GL_AGENT_ID, r_row.AC_AGENT_CAT_TYPE, r_row.AGENT_CODE, r_row.BRANCH_CODE,r_row.REGION_CODE, r_row.BRANCH_NAME,r_row.REGION_NAME,r_row.AGENT_NAME,
                            r_row.AGE0_14DAYSAMT, r_row.AGE15_30DAYSAMT, r_row.AGE31_60DAYSAMT, r_row.AGE61_90DAYSAMT, r_row.AGE91_120DAYSAMT, r_row.AGE121_150DAYSAMT,
                            r_row.AGE151_180DAYSAMT,r_row.AGE181_330DAYSAMT,r_row.AGE331_360DAYSAMT,r_row.ABOVE360DAYSAMT,r_row.UNIDENTIFIEDAMT;
                            EXIT WHEN v_cursor%NOTFOUND;

            PIPE ROW (r_row);
         END LOOP;
                CLOSE v_cursor;
      END IF;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   END FN_RPAC_PREMCOLLECT;

   --RPAC_OFFRECEIPT_LIST Official Receipt Listing (SUMMARY)
   FUNCTION FN_RPAC_OFFRECEIPT_SUMM (p_lStrBankCodeFrom    VARCHAR2,
                                     p_lStrBankCodeTo      VARCHAR2,
                                     p_lStrBatchNoFrom     VARCHAR2,
                                     p_lStrBatchNoTo       VARCHAR2,
                                     p_lStrTranDateFrom    VARCHAR2,
                                     p_lStrTranDateTo      VARCHAR2,
                                     p_lStrProcYear        VARCHAR2,
                                     p_lStrProcMth         VARCHAR2
                                    )
      RETURN RPAC_OFFRECEIPT_SUMM_TAB
      PIPELINED
   IS
      v_ProcName_v   VARCHAR2 (30) := 'FN_RPAC_OFFRECEIPT_SUMM';
      v_Step_v       VARCHAR2 (5) := '000';
      r_row          RPAC_OFFRECEIPT_SUMM_REC;
      v_date         DATE;
      v_date2        DATE;
   BEGIN
      v_date    := TO_DATE ( p_lStrTranDateFrom, 'DD/MM/YYYY');

      v_date2   := TO_DATE ( p_lStrTranDateTo, 'DD/MM/YYYY');

      FOR R
         IN (  SELECT T1.OWN_BANK, SUM (T1.AMOUNT) AS AMT,
                      SUM (T1.CASH) AS CASH_QTY, SUM (T1.CHQ) AS CHQ_QTY,
                      SUM (T1.MTCASH_TOTAL) AS MT_CASH_TOTAL, SUM (T1.MTCHQ_TOTAL) AS MT_CHQ_TOTAL
                      , SUM (T1.NMCASH_TOTAL) AS NM_CASH_TOTAL, SUM (T1.NMCHQ_TOTAL) AS NM_CHQ_TOTAL
                      , SUM (T1.PWCASH_TOTAL) AS PW_CASH_TOTAL, SUM (T1.PWCHQ_TOTAL) AS PW_CHQ_TOTAL
                      , SUM (T1.OTCASH_TOTAL) AS OT_CASH_TOTAL, SUM (T1.OTCHQ_TOTAL) AS OT_CHQ_TOTAL
                 FROM (SELECT ACRC_RCPT.OWN_BANK,
                              ACRC_RCPT.AMOUNT,
                              (SELECT MIN (SEQ_NO)
                                 FROM ACRC_BANK
                                WHERE     ACRC_BANK.AC_NO = ACRC_RCPT.AC_NO
                                      AND ACRC_BANK.CHQ_NO IS NULL
                                      AND ACRC_BANK.CHQ_NO IS NULL)
                                 AS CASH,
                              (SELECT MIN (SEQ_NO)
                                 FROM ACRC_BANK
                                WHERE     ACRC_BANK.AC_NO = ACRC_RCPT.AC_NO
                                      AND ACRC_BANK.CHQ_NO IS NOT NULL
                                      AND ACRC_BANK.CHQ_NO IS NOT NULL)
                                 AS CHQ,
                              (SELECT SUM (ACRC_BANK.CHQ_AMT)
                                 FROM ACRC_BANK
                                WHERE     ACRC_BANK.AC_NO = ACRC_RCPT.AC_NO
                                      AND ACRC_RCPT.AGENT_CAT_TYPE = 'MT'
                                      AND ACRC_BANK.CHQ_NO IS NULL)
                                 AS MTCASH_TOTAL,
                              (SELECT SUM (ACRC_BANK.CHQ_AMT)
                                 FROM ACRC_BANK
                                WHERE     ACRC_BANK.AC_NO = ACRC_RCPT.AC_NO
                                      AND ACRC_RCPT.AGENT_CAT_TYPE = 'MT'
                                      AND ACRC_BANK.CHQ_NO IS NOT NULL)
                                 AS MTCHQ_TOTAL,
                              (SELECT SUM (ACRC_BANK.CHQ_AMT)
                                 FROM ACRC_BANK
                                WHERE     ACRC_BANK.AC_NO = ACRC_RCPT.AC_NO
                                      AND ACRC_RCPT.AGENT_CAT_TYPE = 'NM'
                                      AND ACRC_BANK.CHQ_NO IS NULL)
                                 AS NMCASH_TOTAL,
                              (SELECT SUM (ACRC_BANK.CHQ_AMT)
                                 FROM ACRC_BANK
                                WHERE     ACRC_BANK.AC_NO = ACRC_RCPT.AC_NO
                                      AND ACRC_RCPT.AGENT_CAT_TYPE = 'NM'
                                      AND ACRC_BANK.CHQ_NO IS NOT NULL)
                                 AS NMCHQ_TOTAL,
                              (SELECT SUM (ACRC_BANK.CHQ_AMT)
                                 FROM ACRC_BANK
                                WHERE     ACRC_BANK.AC_NO = ACRC_RCPT.AC_NO
                                      AND ACRC_RCPT.AGENT_CAT_TYPE = 'PW'
                                      AND ACRC_BANK.CHQ_NO IS NULL)
                                 AS PWCASH_TOTAL,
                              (SELECT SUM (ACRC_BANK.CHQ_AMT)
                                 FROM ACRC_BANK
                                WHERE     ACRC_BANK.AC_NO = ACRC_RCPT.AC_NO
                                      AND ACRC_RCPT.AGENT_CAT_TYPE = 'PW'
                                      AND ACRC_BANK.CHQ_NO IS NOT NULL)
                                 AS PWCHQ_TOTAL,
                              (SELECT SUM (ACRC_BANK.CHQ_AMT)
                                 FROM ACRC_BANK
                                WHERE     ACRC_BANK.AC_NO = ACRC_RCPT.AC_NO
                                      AND ACRC_RCPT.AGENT_CAT_TYPE NOT IN
                                             ('MT', 'NM', 'PW')
                                      AND ACRC_BANK.CHQ_NO IS NULL)
                                 AS OTCASH_TOTAL,
                              (SELECT SUM (ACRC_BANK.CHQ_AMT)
                                 FROM ACRC_BANK
                                WHERE     ACRC_BANK.AC_NO = ACRC_RCPT.AC_NO
                                      AND (ACRC_RCPT.AGENT_CAT_TYPE NOT IN
                                             ('MT', 'NM', 'PW') OR ACRC_RCPT.AGENT_CAT_TYPE is null)
                                      AND ACRC_BANK.CHQ_NO IS NOT NULL)
                                 AS OTCHQ_TOTAL
                         FROM ACRC_RCPT
                        WHERE     TRUNC(ACRC_RCPT.TRAN_DATE) BETWEEN v_date --p_lStrTranDateFrom
                                                          AND v_date2 --p_lStrTranDateTo
                              AND ACRC_RCPT.BATCH_NO BETWEEN p_lStrBatchNoFrom
                                                         AND p_lStrBatchNoTo
                              AND ACRC_RCPT.PROC_YR =
                                     TO_NUMBER (p_lStrProcYear)
                              AND ACRC_RCPT.PROC_MTH =
                                     TO_NUMBER (p_lStrProcMth)
                              AND (ACRC_RCPT.OWN_BANK BETWEEN NVL(p_lStrBankCodeFrom,ACRC_RCPT.OWN_BANK) AND nvl(p_lStrBankCodeTo,ACRC_RCPT.OWN_BANK))) T1

             GROUP BY T1.OWN_BANK)
      LOOP
         r_row.BANK_CODE        := NULL;
         r_row.AMOUNT           := NULL;
         r_row.COUNT_CASH       := NULL;
         r_row.COUNT_CHQ        := NULL;
         r_row.MT_TOTAL_CASH    := NULL;
         r_row.MT_TOTAL_CHQ     := NULL;
         r_row.NM_TOTAL_CASH    := NULL;
         r_row.NM_TOTAL_CHQ     := NULL;
         r_row.PW_TOTAL_CASH    := NULL;
         r_row.PW_TOTAL_CHQ     := NULL;
         r_row.OTH_TOTAL_CASH   := NULL;
         r_row.OTH_TOTAL_CHQ    := NULL;

         r_row.BANK_CODE        := R.OWN_BANK;
         r_row.AMOUNT           := R.AMT;
         r_row.COUNT_CASH       := R.CASH_QTY;
         r_row.COUNT_CHQ        := R.CHQ_QTY;
         r_row.MT_TOTAL_CASH    := R.MT_CASH_TOTAL;
         r_row.MT_TOTAL_CHQ     := R.MT_CHQ_TOTAL;
         r_row.NM_TOTAL_CASH    := R.NM_CASH_TOTAL;
         r_row.NM_TOTAL_CHQ     := R.NM_CHQ_TOTAL;
         r_row.PW_TOTAL_CASH    := R.PW_CASH_TOTAL;
         r_row.PW_TOTAL_CHQ     := R.PW_CHQ_TOTAL;
         r_row.OTH_TOTAL_CASH   := R.OT_CASH_TOTAL;
         r_row.OTH_TOTAL_CHQ    := R.OT_CHQ_TOTAL;

         PIPE ROW (r_row);
      END LOOP;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   END FN_RPAC_OFFRECEIPT_SUMM;

   --RPAC_OFFRECEIPT_LIST Official Receipt Listing (LIST)
   FUNCTION FN_RPAC_OFFRECEIPT_LIST (
    p_lstrbankcodefrom   VARCHAR2,
    p_lstrbankcodeto     VARCHAR2,
    p_lstrbatchnofrom    VARCHAR2,
    p_lstrbatchnoto      VARCHAR2,
    p_lstrtrandatefrom   VARCHAR2,
    p_lstrtrandateto     VARCHAR2,
    p_lstrprocyear       VARCHAR2,
    p_lstrprocmth        VARCHAR2,
    p_lstrsortby         VARCHAR2,
    p_lStrSearchBy         VARCHAR2
) RETURN rpac_offreceipt_list_tab
    PIPELINED
IS
      v_ProcName_v   VARCHAR2 (30) := 'FN_RPAC_OFFRECEIPT_LIST';
      v_Step_v       VARCHAR2 (5) := '000';
      r_row          RPAC_OFFRECEIPT_LIST_REC;
      v_date         DATE;
      v_date2        DATE;
   BEGIN
      v_date    := TO_DATE ( p_lStrTranDateFrom, 'DD/MM/YYYY');

      v_date2   := TO_DATE ( p_lStrTranDateTo, 'DD/MM/YYYY');
     DBMS_OUTPUT.put_line ('p_lStrBankCodeFrom'||p_lStrBankCodeFrom);
      IF p_lStrBankCodeFrom IS NOT NULL
      THEN
         FOR R
            IN (  SELECT ACRC_RCPT.OWN_BANK, ACRC_RCPT.BANK_IN_NO
                    FROM ACRC_RCPT
                   WHERE     ACRC_RCPT.TRAN_DATE BETWEEN v_date --p_lStrTranDateFrom
                                                               AND v_date2 --p_lStrTranDateTo
                         AND ACRC_RCPT.OWN_BANK BETWEEN p_lStrBankCodeFrom
                                                    AND p_lStrBankCodeTo
                         AND ACRC_RCPT.PROC_YR = p_lStrProcYear
                         AND ACRC_RCPT.PROC_MTH = p_lStrProcMth
                GROUP BY ACRC_RCPT.OWN_BANK, ACRC_RCPT.BANK_IN_NO
                ORDER BY ACRC_RCPT.OWN_BANK, ACRC_RCPT.BANK_IN_NO)
         LOOP
            FOR R2
               IN (SELECT ACRC_RCPT.BATCH_NO,
                          ACRC_RCPT.AC_NO,
                          (SELECT COUNT (*)
                             FROM ACRC_BANK
                            WHERE ACRC_BANK.AC_NO = ACRC_RCPT.AC_NO)
                             AS BANK_CNT,
                          (SELECT COUNT (*)
                             FROM ACRC_GL
                            WHERE ACRC_GL.AC_NO = ACRC_RCPT.AC_NO)
                             AS GL_CNT
                     FROM ACRC_RCPT
                    WHERE     ACRC_RCPT.OWN_BANK = R.OWN_BANK
                          AND ACRC_RCPT.BANK_IN_NO = R.BANK_IN_NO
                          AND ACRC_RCPT.PROC_YR = p_lStrProcYear
                          AND ACRC_RCPT.PROC_MTH = p_lStrProcMth)
            LOOP
            --DBMS_OUTPUT.put_line (R2.GL_CNT);
               
                  FOR R3
                     IN (SELECT ACRC_RCPT.OWN_BANK,
                                ACRC_RCPT.BATCH_NO,
                                ACRC_RCPT.TRAN_DATE,
                                ACRC_RCPT.AC_NO,
                                ACRC_RCPT.AMOUNT,
                                ACRC_GL.AC_AGENT_ID,
                                ACRC_RCPT.CBC_DATE,
                                (SELECT NAME FROM DMAG_VI_AGENT   WHERE DMAG_VI_AGENT.AGENTCODE = ACRC_GL.AC_AGENT_CODE) AS AGENT_NAME,
                                ACRC_RCPT.NAME,
                                ACRC_GL.AC_GLCODE,
                                ACRC_RCPT.GST_TAX_INV_REFNO,
                                (SELECT DESCP
                                   FROM ACGL_LEDGER
                                  WHERE UKEY = ACRC_GL.AC_GLCODE)
                                   AS GL_DESCP,
                                ACRC_RCPT.STMT_DESCP,
                                ACRC_GL.AC_DB_AMT,
                                ACRC_GL.AC_CR_AMT,
                                ACRC_RCPT.BANK_IN_NO,
                                ACRC_RCPT.BANK_IN_DATE,
                                ACRC_BANK.CHQ_BANK AS PAYER_BANK,
                                ACRC_BANK.CHQ_NO AS CHEQUE_NO,
                                ACRC_BANK.CHQ_DATE AS  CHEQUE_DATE
                                --(SELECT ACRC_BANK.CHQ_BANK FROM ACRC_BANK WHERE ACRC_BANK.AC_NO=ACRC_RCPT.AC_NO AND ACRC_BANK.AC_NO=R2.AC_NO) AS --PAYER_BANK,
                                --(SELECT ACRC_BANK.CHQ_NO FROM ACRC_BANK WHERE ACRC_BANK.AC_NO=ACRC_RCPT.AC_NO AND ACRC_BANK.AC_NO=R2.AC_NO) AS --CHEQUE_NO,
                               -- (SELECT ACRC_BANK.CHQ_DATE FROM ACRC_BANK WHERE ACRC_BANK.AC_NO=ACRC_RCPT.AC_NO AND ACRC_BANK.AC_NO=R2.AC_NO) AS --CHEQUE_DATE
                           FROM ACRC_RCPT
                                LEFT OUTER JOIN ACRC_GL
                                   ON (ACRC_GL.AC_NO = ACRC_RCPT.AC_NO)
                                LEFT OUTER JOIN ACRC_BANK 
                                   ON  (ACRC_BANK.AC_NO=ACRC_RCPT.AC_NO)
                          WHERE     ACRC_RCPT.OWN_BANK = R.OWN_BANK
                                AND ACRC_RCPT.BATCH_NO = R2.BATCH_NO
                                AND ACRC_RCPT.AC_NO = R2.AC_NO
                                AND ACRC_RCPT.BANK_IN_NO = R.BANK_IN_NO
                                AND ACRC_RCPT.PROC_YR = p_lStrProcYear
                                AND ACRC_RCPT.PROC_MTH = p_lStrProcMth)
                  LOOP
                     r_row.BANK_CODE        := NULL;
                     r_row.BATCH_NO         := NULL;
                     r_row.TRAN_DATE        := NULL;
                     r_row.CBC_DATE         := NULL;
                     r_row.RECEIPT_NO       := NULL;
                     r_row.TAX_INVOICE_NO   := NULL;
                     r_row.AMOUNT           := NULL;
                     r_row.PAYER_BANK       := NULL;
                     r_row.CHEQUE_NO        := NULL;
                     r_row.CHEQUE_DATE      := NULL;
                     r_row.PAYEE_NAME       := NULL;
                     r_row.GL_CODE          := NULL;
                     r_row.AGENT_ID         := NULL;
                     r_row.AGENT_CODE       := NULL;
                     r_row.AGENT_NAME       := NULL;
                     r_row.GL_DESCP         := NULL;
                     r_row.PARTICULAR       := NULL;
                     r_row.AMOUNT_DT        := NULL;
                     r_row.AMOUNT_CR        := NULL;
                     r_row.BANK_SLIP        := NULL;
                     r_row.BANK_IN_DATE     := NULL;

                     r_row.BANK_CODE        := R.OWN_BANK;
                     r_row.BATCH_NO         := R2.BATCH_NO;
                     r_row.TRAN_DATE        := R3.TRAN_DATE;
                     r_row.CBC_DATE         := R3.CBC_DATE;
                     r_row.RECEIPT_NO       := R3.AC_NO;
                     r_row.TAX_INVOICE_NO   := R3.GST_TAX_INV_REFNO;
                     r_row.AMOUNT           := R3.AMOUNT;
                     r_row.PAYER_BANK       := R3.PAYER_BANK;
                     r_row.CHEQUE_NO        := R3.CHEQUE_NO;
                     r_row.CHEQUE_DATE      := R3.CHEQUE_DATE;
                     r_row.PAYEE_NAME       := R3.NAME;
                     r_row.GL_CODE          := R3.AC_GLCODE;
                     r_row.AGENT_ID         := R3.AC_AGENT_ID;
                     r_row.AGENT_CODE       := NULL;
                     r_row.AGENT_NAME       := R3.AGENT_NAME;
                     r_row.GL_DESCP         := R3.GL_DESCP;
                     r_row.PARTICULAR       := R3.STMT_DESCP;
                     r_row.AMOUNT_DT        := R3.AC_DB_AMT;
                     r_row.AMOUNT_CR        := R3.AC_CR_AMT;
                     r_row.BANK_SLIP        := R3.BANK_IN_NO;
                     r_row.BANK_IN_DATE     := R3.BANK_IN_DATE;

                     PIPE ROW (r_row);
                  END LOOP;

            END LOOP;
         END LOOP;
      ELSIF p_lStrBatchNoFrom IS NOT NULL
      THEN
         IF p_lStrSortBy ='B'
         THEN
         FOR R
        IN(SELECT ACRC_RCPT.BATCH_NO, ACRC_RCPT.BANK_IN_NO   FROM ACRC_RCPT 
         WHERE ACRC_RCPT.TRAN_DATE BETWEEN v_date AND v_date2 
         AND ACRC_RCPT.BATCH_NO BETWEEN p_lStrBatchNoFrom AND p_lStrBatchNoTo 
         AND ACRC_RCPT.PROC_YR = p_lStrProcYear 
         AND ACRC_RCPT.PROC_MTH = p_lStrProcMth
         GROUP BY ACRC_RCPT.BATCH_NO,  ACRC_RCPT.BANK_IN_NO
         ORDER BY ACRC_RCPT.BATCH_NO,  ACRC_RCPT.BANK_IN_NO)
         LOOP
         FOR R2
           IN (SELECT ACRC_RCPT.OWN_BANK, ACRC_RCPT.AC_NO,
               (SELECT COUNT(*) FROM ACRC_BANK WHERE ACRC_BANK.AC_NO = ACRC_RCPT.AC_NO) AS BANK_CNT,
               (SELECT COUNT(*) FROM ACRC_GL WHERE ACRC_GL.AC_NO = ACRC_RCPT.AC_NO) AS GL_CNT
               FROM ACRC_RCPT
           WHERE ACRC_RCPT.BATCH_NO = R.BATCH_NO
           AND ACRC_RCPT.BANK_IN_NO = R.BANK_IN_NO
           AND ACRC_RCPT.PROC_YR = p_lStrProcYear 
           AND ACRC_RCPT.PROC_MTH = p_lStrProcMth)
           LOOP 
                            
              FOR R3
              IN (SELECT ACRC_RCPT.OWN_BANK,
                                ACRC_RCPT.BATCH_NO,
                                ACRC_RCPT.TRAN_DATE,
                                ACRC_RCPT.AC_NO,
                                ACRC_RCPT.AMOUNT,
                                ACRC_GL.AC_AGENT_ID,
                                ACRC_RCPT.CBC_DATE,
                                (SELECT NAME FROM DMAG_VI_AGENT   WHERE DMAG_VI_AGENT.AGENTCODE = ACRC_GL.AC_AGENT_CODE) AS AGENT_NAME,
                                ACRC_RCPT.NAME,
                                ACRC_GL.AC_GLCODE,
                                ACRC_RCPT.GST_TAX_INV_REFNO,
                                (SELECT DESCP
                                   FROM ACGL_LEDGER
                                  WHERE UKEY = ACRC_GL.AC_GLCODE)
                                   AS GL_DESCP,
                                ACRC_RCPT.STMT_DESCP,
                                ACRC_GL.AC_DB_AMT,
                                ACRC_GL.AC_CR_AMT,
                                ACRC_RCPT.BANK_IN_NO,
                                ACRC_RCPT.BANK_IN_DATE,
                                ACRC_BANK.CHQ_BANK AS PAYER_BANK,
                               ACRC_BANK.CHQ_NO AS CHEQUE_NO,
                                ACRC_BANK.CHQ_DATE AS  CHEQUE_DATE
                                -- (SELECT ACRC_BANK.CHQ_BANK FROM ACRC_BANK WHERE ACRC_BANK.AC_NO=ACRC_RCPT.AC_NO AND ACRC_BANK.AC_NO=R2.AC_NO) AS --PAYER_BANK,
                                --(SELECT ACRC_BANK.CHQ_NO FROM ACRC_BANK WHERE ACRC_BANK.AC_NO=ACRC_RCPT.AC_NO AND ACRC_BANK.AC_NO=R2.AC_NO) AS --CHEQUE_NO,
                               -- (SELECT ACRC_BANK.CHQ_DATE FROM ACRC_BANK WHERE ACRC_BANK.AC_NO=ACRC_RCPT.AC_NO AND ACRC_BANK.AC_NO=R2.AC_NO) AS --CHEQUE_DATE
                           FROM ACRC_RCPT
                                LEFT OUTER JOIN ACRC_GL
                                   ON (ACRC_GL.AC_NO = ACRC_RCPT.AC_NO)
                                LEFT OUTER JOIN ACRC_BANK 
                                   ON  (ACRC_BANK.AC_NO=ACRC_RCPT.AC_NO)
                          WHERE     ACRC_RCPT.OWN_BANK = R2.OWN_BANK
                                AND ACRC_RCPT.BATCH_NO = R.BATCH_NO
                                AND ACRC_RCPT.AC_NO = R2.AC_NO
                                AND ACRC_RCPT.BANK_IN_NO = R.BANK_IN_NO
                                AND ACRC_RCPT.PROC_YR = p_lStrProcYear
                                AND ACRC_RCPT.PROC_MTH = p_lStrProcMth)
                  LOOP
                     r_row.BANK_CODE        := NULL;
                     r_row.BATCH_NO         := NULL;
                     r_row.TRAN_DATE        := NULL;
                     r_row.CBC_DATE         := NULL;
                     r_row.RECEIPT_NO       := NULL;
                     r_row.TAX_INVOICE_NO   := NULL;
                     r_row.AMOUNT           := NULL;
                     r_row.PAYER_BANK       := NULL;
                     r_row.CHEQUE_NO        := NULL;
                     r_row.CHEQUE_DATE      := NULL;
                     r_row.PAYEE_NAME       := NULL;
                     r_row.GL_CODE          := NULL;
                     r_row.AGENT_ID         := NULL;
                     r_row.AGENT_CODE       := NULL;
                     r_row.AGENT_NAME       := NULL;
                     r_row.GL_DESCP         := NULL;
                     r_row.PARTICULAR       := NULL;
                     r_row.AMOUNT_DT        := NULL;
                     r_row.AMOUNT_CR        := NULL;
                     r_row.BANK_SLIP        := NULL;
                     r_row.BANK_IN_DATE     := NULL;

                     r_row.BANK_CODE        := R2.OWN_BANK;
                     r_row.BATCH_NO         := R.BATCH_NO;
                     r_row.TRAN_DATE        := R3.TRAN_DATE;
                     r_row.CBC_DATE         := R3.CBC_DATE;
                     r_row.RECEIPT_NO       := R3.AC_NO;
                     r_row.TAX_INVOICE_NO   := R3.GST_TAX_INV_REFNO;
                     r_row.AMOUNT           := R3.AMOUNT;
                     r_row.PAYER_BANK       := R3.PAYER_BANK;
                     r_row.CHEQUE_NO        := R3.CHEQUE_NO;
                     r_row.CHEQUE_DATE      := R3.CHEQUE_DATE;
                     r_row.PAYEE_NAME       := R3.NAME;
                     r_row.GL_CODE          := R3.AC_GLCODE;
                     r_row.AGENT_ID         := R3.AC_AGENT_ID;
                     r_row.AGENT_CODE       := NULL;
                     r_row.AGENT_NAME       := R3.AGENT_NAME;
                     r_row.GL_DESCP         := R3.GL_DESCP;
                     r_row.PARTICULAR       := R3.STMT_DESCP;
                     r_row.AMOUNT_DT        := R3.AC_DB_AMT;
                     r_row.AMOUNT_CR        := R3.AC_CR_AMT;
                     r_row.BANK_SLIP        := R3.BANK_IN_NO;
                     r_row.BANK_IN_DATE     := R3.BANK_IN_DATE;

                     PIPE ROW (r_row);
                  END LOOP;
                  

         END LOOP;  
         END LOOP;  
         --PIPE ROW (r_row);--94476
     DBMS_OUTPUT.put_line (p_lStrSortBy);
      ELSIF p_lStrSortBy ='R'
       
      THEN
      FOR S
      IN (SELECT ACRC_RCPT.BATCH_NO, ACRC_RCPT.AC_NO
       FROM ACRC_RCPT 
       WHERE ACRC_RCPT.TRAN_DATE BETWEEN v_date AND v_date2 
       AND ACRC_RCPT.BATCH_NO BETWEEN p_lStrBatchNoFrom AND p_lStrBatchNoTo 
       AND ACRC_RCPT.PROC_YR = p_lStrProcYear 
       AND ACRC_RCPT.PROC_MTH = p_lStrProcMth
       GROUP BY ACRC_RCPT.BATCH_NO,  ACRC_RCPT.AC_NO
       ORDER BY ACRC_RCPT.BATCH_NO,  ACRC_RCPT.AC_NO)
      LOOP
      FOR S2
      IN (SELECT ACRC_RCPT.OWN_BANK, ACRC_RCPT.AC_NO,
               (SELECT COUNT(*) FROM ACRC_BANK WHERE ACRC_BANK.AC_NO = ACRC_RCPT.AC_NO) AS BANK_CNT,
               (SELECT COUNT(*) FROM ACRC_GL WHERE ACRC_GL.AC_NO = ACRC_RCPT.AC_NO) AS GL_CNT
       FROM ACRC_RCPT
       WHERE ACRC_RCPT.BATCH_NO = S.BATCH_NO
       AND ACRC_RCPT.AC_NO = S.AC_NO
       AND ACRC_RCPT.PROC_YR = p_lStrProcYear 
       AND ACRC_RCPT.PROC_MTH = p_lStrProcMth) 
       LOOP
               
                  FOR S3
                  IN (SELECT ACRC_RCPT.OWN_BANK,
                                ACRC_RCPT.BATCH_NO,
                                ACRC_RCPT.TRAN_DATE,
                                ACRC_RCPT.AC_NO,
                                ACRC_RCPT.AMOUNT,
                                ACRC_GL.AC_AGENT_ID,   
                                ACRC_RCPT.CBC_DATE,
                                (SELECT NAME FROM DMAG_VI_AGENT   WHERE DMAG_VI_AGENT.AGENTCODE = ACRC_GL.AC_AGENT_CODE) AS AGENT_NAME,
                                ACRC_RCPT.NAME,
                                ACRC_GL.AC_GLCODE,
                                ACRC_RCPT.GST_TAX_INV_REFNO,
                                (SELECT DESCP
                                   FROM ACGL_LEDGER
                                  WHERE UKEY = ACRC_GL.AC_GLCODE)
                                   AS GL_DESCP,
                                ACRC_RCPT.STMT_DESCP,
                                ACRC_GL.AC_DB_AMT,
                                ACRC_GL.AC_CR_AMT,
                                ACRC_RCPT.BANK_IN_NO,
                                ACRC_RCPT.BANK_IN_DATE,
                                ACRC_BANK.CHQ_BANK AS PAYER_BANK,
                                ACRC_BANK.CHQ_NO AS CHEQUE_NO,
                                ACRC_BANK.CHQ_DATE AS  CHEQUE_DATE
                                -- (SELECT ACRC_BANK.CHQ_BANK FROM ACRC_BANK WHERE ACRC_BANK.AC_NO=ACRC_RCPT.AC_NO AND ACRC_BANK.AC_NO=S2.AC_NO) AS --PAYER_BANK,
                                --(SELECT ACRC_BANK.CHQ_NO FROM ACRC_BANK WHERE ACRC_BANK.AC_NO=ACRC_RCPT.AC_NO AND ACRC_BANK.AC_NO=S2.AC_NO) AS --CHEQUE_NO,
                               -- (SELECT ACRC_BANK.CHQ_DATE FROM ACRC_BANK WHERE ACRC_BANK.AC_NO=ACRC_RCPT.AC_NO AND ACRC_BANK.AC_NO=S2.AC_NO) AS --CHEQUE_DATE
                           FROM ACRC_RCPT
                                LEFT OUTER JOIN ACRC_GL
                                   ON (ACRC_GL.AC_NO = ACRC_RCPT.AC_NO)
                                LEFT OUTER JOIN ACRC_BANK 
                                   ON  (ACRC_BANK.AC_NO=ACRC_RCPT.AC_NO)
                          WHERE     ACRC_RCPT.OWN_BANK = S2.OWN_BANK
                                AND ACRC_RCPT.BATCH_NO = S.BATCH_NO
                                AND ACRC_RCPT.AC_NO = S2.AC_NO                             
                                AND ACRC_RCPT.PROC_YR = p_lStrProcYear
                                AND ACRC_RCPT.PROC_MTH = p_lStrProcMth)
                  LOOP
                     r_row.BANK_CODE        := NULL;
                     r_row.BATCH_NO         := NULL;
                     r_row.TRAN_DATE        := NULL;
                     r_row.CBC_DATE         := NULL;
                     r_row.RECEIPT_NO       := NULL;
                     r_row.TAX_INVOICE_NO   := NULL;
                     r_row.AMOUNT           := NULL;
                     r_row.PAYER_BANK       := NULL;
                     r_row.CHEQUE_NO        := NULL;
                     r_row.CHEQUE_DATE      := NULL;
                     r_row.PAYEE_NAME       := NULL;
                     r_row.GL_CODE          := NULL;
                     r_row.AGENT_ID         := NULL;
                     r_row.AGENT_CODE       := NULL;
                     r_row.AGENT_NAME       := NULL;
                     r_row.GL_DESCP         := NULL;
                     r_row.PARTICULAR       := NULL;
                     r_row.AMOUNT_DT        := NULL;
                     r_row.AMOUNT_CR        := NULL;
                     r_row.BANK_SLIP        := NULL;
                     r_row.BANK_IN_DATE     := NULL;

                     r_row.BANK_CODE        := S2.OWN_BANK;
                     r_row.BATCH_NO         := S.BATCH_NO;
                     r_row.TRAN_DATE        := S3.TRAN_DATE;
                     r_row.CBC_DATE         := S3.CBC_DATE;
                     r_row.RECEIPT_NO       := S3.AC_NO;
                     r_row.TAX_INVOICE_NO   := S3.GST_TAX_INV_REFNO;
                     r_row.AMOUNT           := S3.AMOUNT;
                     r_row.PAYER_BANK       := S3.PAYER_BANK;
                     r_row.CHEQUE_NO        := S3.CHEQUE_NO;
                     r_row.CHEQUE_DATE      := S3.CHEQUE_DATE;
                     r_row.PAYEE_NAME       := S3.NAME;
                     r_row.GL_CODE          := S3.AC_GLCODE;
                     r_row.AGENT_ID         := S3.AC_AGENT_ID;
                     r_row.AGENT_CODE       := NULL;
                     r_row.AGENT_NAME       := S3.AGENT_NAME;
                     r_row.GL_DESCP         := S3.GL_DESCP;
                     r_row.PARTICULAR       := S3.STMT_DESCP;
                     r_row.AMOUNT_DT        := S3.AC_DB_AMT;
                     r_row.AMOUNT_CR        := s3.AC_CR_AMT;
                     r_row.BANK_SLIP        := s3.BANK_IN_NO;
                     r_row.BANK_IN_DATE     := S3.BANK_IN_DATE;

                     PIPE ROW (r_row);
                  END LOOP;
                                
            END LOOP;
         END LOOP;
         --PIPE ROW (r_row);
         END IF;
         END IF;
        -- PIPE ROW (r_row);
    
        
      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   END FN_RPAC_OFFRECEIPT_LIST;
   --RPAC_ACBATCHLIST Accounting Batch Listing (LIST)
   FUNCTION FN_RPAC_ACBATCHLIST (p_lStrBranchCodeFrom    VARCHAR2,
                                 p_lStrBranchCodeTo      VARCHAR2,
                                 p_lStrSourceFrom        VARCHAR2,
                                 p_lStrSourceTo          VARCHAR2,
                                 p_lStrBatchNoFrom       VARCHAR2,
                                 p_lStrBatchNoTo         VARCHAR2,
                                 p_lStrDocumentFrom      VARCHAR2,
                                 p_lStrDocumentTo        VARCHAR2,
                                 p_lIntProcYear          VARCHAR2,
                                 p_lIntProcMth           VARCHAR2,
                                 p_lStrTranType          VARCHAR2
                                )
      RETURN RPAC_ACBATCHLIST_TAB
      PIPELINED
   IS
      v_ProcName_v   VARCHAR2 (30) := 'FN_RPAC_ACBATCHLIST';
      v_Step_v       VARCHAR2 (5) := '000';
      r_row          RPAC_ACBATCHLIST_REC;
   BEGIN
      IF p_lStrTranType = 'P'
      THEN
         FOR r
            IN (SELECT ACPY_PYMT.BATCH_NO, ACPY_PYMT.TRAN_DATE,
                       ACPY_PYMT.AC_NO, ACPY_PYMT.AMOUNT,
                       ACPY_PYMT.CHQ_NO, ACPY_PYMT.EXP_CODE,
                       ACPY_PYMT.EXP_AMT, ACPY_PYMT.STMT_DESCP
                  FROM ACPY_PYMT
                       LEFT OUTER JOIN
                       DMAG_VI_AGCAT_SOURCE
                          ON (DMAG_VI_AGCAT_SOURCE.AGENT_ID =
                                 ACPY_PYMT.AGENT_ID)
                      -- start redmine 119949
                      -- LEFT OUTER JOIN DMAG_VI_AGENT
                         -- ON (DMAG_VI_AGENT.AGENTCODE = ACPY_PYMT.AGENT_CODE)
                      --end redmine 119949
                 WHERE  ((p_lStrBranchCodeFrom IS NULL AND p_lStrBranchCodeTo IS NULL) OR
                          ACPY_PYMT.ISSUE_OFFICE BETWEEN p_lStrBranchCodeFrom AND p_lStrBranchCodeTo)
                       AND ACPY_PYMT.PROC_YR = NVL(p_lIntProcYear,ACPY_PYMT.PROC_YR)
                       AND ACPY_PYMT.PROC_MTH = NVL(p_lIntProcMth,ACPY_PYMT.PROC_MTH)
                       AND (   p_lStrBatchNoFrom IS NULL
                            OR (    p_lStrBatchNoFrom IS NOT NULL
                                AND ACPY_PYMT.BATCH_NO BETWEEN p_lStrBatchNoFrom
                                                           AND p_lStrBatchNoTo))
                       AND (   p_lStrSourceFrom IS NULL
                            OR (    p_lStrSourceFrom IS NOT NULL
                                AND DMAG_VI_AGCAT_SOURCE.SOURCE_ID BETWEEN p_lStrSourceFrom
                                                                       AND p_lStrSourceTo))
                       AND (   p_lStrDocumentFrom IS NULL
                            OR (    p_lStrDocumentFrom IS NOT NULL
                                AND ACPY_PYMT.AC_NO BETWEEN p_lStrDocumentFrom
                                                        AND p_lStrDocumentTo)))
         LOOP
            r_row.BATCHNO         := NULL;
            r_row.TRANDATE        := NULL;
            r_row.PAYMENTNO       := NULL;
            r_row.RECEIPTNO       := NULL;
            r_row.JOURNALNO       := NULL;
            r_row.AMOUNT          := NULL;
            r_row.CHEQUENO        := NULL;
            r_row.EXPCODE         := NULL;
            r_row.EXPAMOUNT       := NULL;
            r_row.STATEMENTDESC   := NULL;
            r_row.ACNO            := NULL;

            r_row.BATCHNO         := r.BATCH_NO;
            r_row.TRANDATE        := r.TRAN_DATE;
            r_row.PAYMENTNO       := r.AC_NO;
            r_row.RECEIPTNO       := NULL;
            r_row.JOURNALNO       := NULL;
            r_row.AMOUNT          := r.AMOUNT;
            r_row.CHEQUENO        := r.CHQ_NO;
            r_row.EXPCODE         := r.EXP_CODE;
            r_row.EXPAMOUNT       := r.EXP_AMT;
            r_row.STATEMENTDESC   := r.STMT_DESCP;
            r_row.ACNO            := r.AC_NO;

            PIPE ROW (r_row);
         END LOOP;
      --BATCH TOTAL
      --         SELECT COUNT(ACGC_BATCH.TOT_DOC) AS TOTAL_DOC, SUM(ACGC_BATCH.TOT_AMT) AS TOTAL_AMT
      --              FROM ACGC_BATCH
      --             WHERE ACGC_BATCH.BATCH_NO = :lStrHidBatchNo
      --
      --BANK TOTAL
      --             SELECT CMUW_MORTGAGEE.GEN_LEDG, CMUW_MORTGAGEE.DESCP, GL.AC_DB_AMT  FROM CMUW_MORTGAGEE,
      --               (SELECT ACPY_GL.AC_GLCODE, SUM(ACPY_GL.AC_DB_AMT - ACPY_GL.AC_CR_AMT) AS AC_DB_AMT
      --               FROM ACPY_GL WHERE ACPY_GL.AC_NO IN (SELECT ACPY_PYMT.AC_NO FROM ACPY_PYMT
      --             LEFT OUTER JOIN MKAG_PROFILE ON (MKAG_PROFILE.AGENT_CODE = ACPY_PYMT.AGENT_CODE)
      --             WHERE :?MKAG_PROFILE_PTOBRANCH
      --             AND ACPY_PYMT.BATCH_NO = :lStrHidBatchNo
      --             AND ACPY_PYMT.PROC_YR = :lStrProcYear
      --             AND ACPY_PYMT.PROC_MTH = :lStrProcMth)
      --             GROUP BY ACPY_GL.AC_GLCODE) GL
      --            WHERE CMUW_MORTGAGEE.GEN_LEDG = GL.AC_GLCODE

      ELSIF p_lStrTranType = 'R'
      THEN
         FOR r
            IN (SELECT ACRC_RCPT.BATCH_NO, ACRC_RCPT.TRAN_DATE,
                       ACRC_RCPT.AC_NO, ACRC_RCPT.AMOUNT,
                       ACRC_BANK.CHQ_NO, ACRC_RCPT.STMT_DESCP
                  FROM ACRC_RCPT
                       LEFT OUTER JOIN
                       DMAG_VI_AGCAT_SOURCE
                          ON (DMAG_VI_AGCAT_SOURCE.AGENT_ID =
                                 ACRC_RCPT.AGENT_ID)
                       -- start redmine 119949
                       --LEFT OUTER JOIN DMAG_VI_AGENT
                         -- ON (DMAG_VI_AGENT.AGENTCODE = ACRC_RCPT.AGENT_CODE)
                       -- end redmine 119949
                       LEFT OUTER JOIN ACRC_BANK
                          ON (ACRC_BANK.AC_NO = ACRC_RCPT.AC_NO)
                 WHERE     ((p_lStrBranchCodeFrom IS NULL AND p_lStrBranchCodeTo IS NULL) OR
                          ACRC_RCPT.ISSUE_OFFICE BETWEEN p_lStrBranchCodeFrom AND p_lStrBranchCodeTo)
                       AND ACRC_RCPT.PROC_YR = NVL(p_lIntProcYear,ACRC_RCPT.PROC_YR)
                       AND ACRC_RCPT.PROC_MTH = NVL(p_lIntProcMth,ACRC_RCPT.PROC_MTH)
                       AND (   p_lStrBatchNoFrom IS NULL
                            OR (    p_lStrBatchNoFrom IS NOT NULL
                                AND ACRC_RCPT.BATCH_NO BETWEEN p_lStrBatchNoFrom
                                                           AND p_lStrBatchNoTo))
                       AND (   p_lStrSourceFrom IS NULL
                            OR (    p_lStrSourceFrom IS NOT NULL
                                AND DMAG_VI_AGCAT_SOURCE.SOURCE_ID BETWEEN p_lStrSourceFrom
                                                                       AND p_lStrSourceTo))
                       AND (   p_lStrDocumentFrom IS NULL
                            OR (    p_lStrDocumentFrom IS NOT NULL
                                AND ACRC_RCPT.AC_NO BETWEEN p_lStrDocumentFrom
                                                        AND p_lStrDocumentTo)))
         LOOP
            r_row.BATCHNO         := NULL;
            r_row.TRANDATE        := NULL;
            r_row.PAYMENTNO       := NULL;
            r_row.RECEIPTNO       := NULL;
            r_row.JOURNALNO       := NULL;
            r_row.AMOUNT          := NULL;
            r_row.CHEQUENO        := NULL;
            r_row.EXPCODE         := NULL;
            r_row.EXPAMOUNT       := NULL;
            r_row.STATEMENTDESC   := NULL;
            r_row.ACNO            := NULL;
            r_row.ACNO_COUNT      := 0;

            r_row.BATCHNO         := r.BATCH_NO;
            r_row.TRANDATE        := r.TRAN_DATE;
            r_row.PAYMENTNO       := NULL;
            r_row.RECEIPTNO       := r.AC_NO;
            r_row.JOURNALNO       := NULL;
            r_row.AMOUNT          := r.AMOUNT;
            r_row.CHEQUENO        := r.CHQ_NO;
            r_row.EXPCODE         := NULL;
            r_row.EXPAMOUNT       := NULL;
            r_row.STATEMENTDESC   := r.STMT_DESCP;
            r_row.ACNO            := r.AC_NO;
            
            begin
            SELECT COUNT (ACRC_RCPT.AC_NO) INTO r_row.ACNO_COUNT
                FROM ACRC_RCPT
               WHERE ACRC_RCPT.BATCH_NO = r.BATCH_NO; 
               exception WHEN others THEN
               null;
             end;

            PIPE ROW (r_row);
         END LOOP;
      --BATCH TOTAL
      --         SELECT COUNT(ACGC_BATCH.TOT_DOC) AS TOTAL_DOC, SUM(ACGC_BATCH.TOT_AMT) AS TOTAL_AMT
      --          FROM ACGC_BATCH
      --         WHERE ACGC_BATCH.BATCH_NO = :lStrHidBatchNo
      --
      --BANK TOTAL
      --         SELECT CMUW_MORTGAGEE.GEN_LEDG, CMUW_MORTGAGEE.DESCP, GL.AC_DB_AMT
      --        FROM CMUW_MORTGAGEE,
      --           (SELECT ACRC_GL.AC_GLCODE, SUM(ACRC_GL.AC_CR_AMT - ACRC_GL.AC_DB_AMT) AS AC_DB_AMT
      --              FROM ACRC_GL WHERE ACRC_GL.AC_NO IN (SELECT ACRC_RCPT.AC_NO FROM ACRC_RCPT
      --                                                                                                LEFT OUTER JOIN MKAG_PROFILE ON (MKAG_PROFILE.AGENT_CODE = ACRC_RCPT.AGENT_CODE)
      --                                                                                                WHERE :?MKAG_PROFILE_RTOBRANCH AND ACRC_RCPT.BATCH_NO = :lStrHidBatchNo
      --                                                      AND ACRC_RCPT.PROC_YR = :lStrProcYear
      --                                                      AND ACRC_RCPT.PROC_MTH = :lStrProcMth)
      --             GROUP BY ACRC_GL.AC_GLCODE) GL
      --        WHERE CMUW_MORTGAGEE.GEN_LEDG = GL.AC_GLCODE
      --

      ELSIF p_lStrTranType = 'J'
      THEN
         FOR r
            IN (SELECT ACJN_JOUR.BATCH_NO, ACJN_JOUR.TRAN_DATE,
                       ACJN_JOUR.AC_NO, ACJN_JOUR.EXP_CODE,
                       ACJN_JOUR.EXP_AMT, ACJN_JOUR.STMT_DESCP
                  FROM ACJN_JOUR
                       LEFT OUTER JOIN
                       DMAG_VI_AGCAT_SOURCE
                          ON (DMAG_VI_AGCAT_SOURCE.AGENT_ID =
                                 ACJN_JOUR.AGENT_ID)
                     -- start redmine 119949
                      -- LEFT OUTER JOIN DMAG_VI_AGENT
                        --  ON (DMAG_VI_AGENT.AGENTCODE = ACJN_JOUR.AGENT_CODE)
                      -- end redmine 119949
                 WHERE     ((p_lStrBranchCodeFrom IS NULL AND p_lStrBranchCodeTo IS NULL) OR
                          ACJN_JOUR.ISSUE_OFFICE BETWEEN p_lStrBranchCodeFrom AND p_lStrBranchCodeTo)
                       AND ACJN_JOUR.PROC_YR = NVL(p_lIntProcYear,ACJN_JOUR.PROC_YR)
                       AND ACJN_JOUR.PROC_MTH = NVL(p_lIntProcMth,ACJN_JOUR.PROC_MTH)
                       AND (   p_lStrBatchNoFrom IS NULL
                            OR (    p_lStrBatchNoFrom IS NOT NULL
                                AND ACJN_JOUR.BATCH_NO BETWEEN p_lStrBatchNoFrom
                                                           AND p_lStrBatchNoTo))
                       AND (   p_lStrSourceFrom IS NULL
                            OR (    p_lStrSourceFrom IS NOT NULL
                                AND DMAG_VI_AGCAT_SOURCE.SOURCE_ID BETWEEN p_lStrSourceFrom
                                                                       AND p_lStrSourceTo))
                       AND (   p_lStrDocumentFrom IS NULL
                            OR (    p_lStrDocumentFrom IS NOT NULL
                                AND ACJN_JOUR.AC_NO BETWEEN p_lStrDocumentFrom
                                                        AND p_lStrDocumentTo)))
         LOOP
            r_row.BATCHNO         := NULL;
            r_row.TRANDATE        := NULL;
            r_row.PAYMENTNO       := NULL;
            r_row.RECEIPTNO       := NULL;
            r_row.JOURNALNO       := NULL;
            r_row.AMOUNT          := NULL;
            r_row.CHEQUENO        := NULL;
            r_row.EXPCODE         := NULL;
            r_row.EXPAMOUNT       := NULL;
            r_row.STATEMENTDESC   := NULL;
            r_row.ACNO            := NULL;

            r_row.BATCHNO         := r.BATCH_NO;
            r_row.TRANDATE        := r.TRAN_DATE;
            r_row.PAYMENTNO       := NULL;
            r_row.RECEIPTNO       := NULL;
            r_row.JOURNALNO       := r.AC_NO;
            r_row.AMOUNT          := NULL;
            r_row.CHEQUENO        := NULL;
            r_row.EXPCODE         := r.EXP_CODE;
            r_row.EXPAMOUNT       := r.EXP_AMT;
            r_row.STATEMENTDESC   := r.STMT_DESCP;
            r_row.ACNO            := r.AC_NO;

            PIPE ROW (r_row);
         END LOOP;
      --BATCH TOTAL
      --         SELECT COUNT(ACGC_BATCH.TOT_DOC) AS TOTAL_DOC, SUM(ACGC_BATCH.TOT_AMT) AS TOTAL_AMT
      --          FROM ACGC_BATCH
      --         WHERE ACGC_BATCH.BATCH_NO = :lStrHidBatchNo
      --        --BANK TOTAL
      --        SELECT CMUW_MORTGAGEE.GEN_LEDG, CMUW_MORTGAGEE.DESCP, GL.AC_DB_AMT
      --          FROM CMUW_MORTGAGEE,
      --               (SELECT ACJN_GL.AC_GLCODE, SUM(ACJN_GL.AC_CR_AMT - ACJN_GL.AC_DB_AMT) AS AC_DB_AMT
      --                  FROM ACJN_GL WHERE ACJN_GL.AC_NO IN (SELECT ACJN_JOUR.AC_NO FROM ACJN_JOUR
      --                                                                                                    LEFT OUTER JOIN MKAG_PROFILE ON (MKAG_PROFILE.AGENT_CODE = ACJN_JOUR.AGENT_CODE)
      --                                                                                                    WHERE :?MKAG_PROFILE_JTOBRANCH AND ACJN_JOUR.BATCH_NO = :lStrHidBatchNo
      --                                                          AND ACJN_JOUR.PROC_YR = :lStrProcYear
      --                                                          AND ACJN_JOUR.PROC_MTH = :lStrProcMth)
      --                 GROUP BY ACJN_GL.AC_GLCODE) GL
      --        WHERE CMUW_MORTGAGEE.GEN_LEDG = GL.AC_GLCODE

      ELSIF p_lStrTranType = 'D'
      THEN
         FOR r
            IN (SELECT ACDB_NDB.BATCH_NO,
                       ACDB_NDB.TRAN_DATE,
                       ACDB_NDB.AC_NO,
                       ACDB_NDB.AMOUNT,
                       ACDB_NDB.EXP_CODE,
                       ACDB_NDB.EXP_AMT,
                    -- start redmine 119949
                      -- ACDB_GL.AC_AGENT_ID,
                     --  ACDB_GL.AC_GLCODE,
                     --  (SELECT DESCP
                     --     FROM ACGL_LEDGER
                      --   WHERE UKEY = ACDB_GL.AC_GLCODE)
                     --     AS GL_DESC,
                     --  ACDB_GL.AC_DB_AMT,
                      -- ACDB_GL.AC_CR_AMT,
                      -- ACDB_KO.DOC_NO,
                      -- ACDB_KO.DOC_AMT,
                      -- end redmine 119949
                       ACDB_NDB.STMT_DESCP
                  -- start redmine 119949
                  --FROM (((ACDB_NDB
                  FROM ACDB_NDB
                  -- end redmine 119949
                          LEFT OUTER JOIN
                          DMAG_VI_AGCAT_SOURCE
                          -- start redmine 119949
                           --ON (DMAG_VI_AGCAT_SOURCE.AGENT_ID =
                          --          ACDB_NDB.AGENT_ID))
                        -- LEFT OUTER JOIN
                         --DMAG_VI_AGENT
                          --  ON (DMAG_VI_AGENT.AGENTCODE = ACDB_NDB.AGENT_CODE))
                        --LEFT OUTER JOIN ACDB_GL
                         --  ON (ACDB_GL.AC_NO = ACDB_NDB.AC_NO))
                       --LEFT OUTER JOIN ACDB_KO
                       --   ON (ACDB_KO.AC_NO = ACDB_NDB.AC_NO)
                             ON DMAG_VI_AGCAT_SOURCE.AGENT_ID =
                                    ACDB_NDB.AGENT_ID
                        -- end redmine 119949
                 WHERE     ((p_lStrBranchCodeFrom IS NULL AND p_lStrBranchCodeTo IS NULL) OR
                          ACDB_NDB.ISSUE_OFFICE BETWEEN p_lStrBranchCodeFrom AND p_lStrBranchCodeTo)
                       AND ACDB_NDB.PROC_YR = NVL(p_lIntProcYear,ACDB_NDB.PROC_YR)
                       AND ACDB_NDB.PROC_MTH = NVL(p_lIntProcMth,ACDB_NDB.PROC_MTH)
                       AND (   p_lStrBatchNoFrom IS NULL
                            OR (    p_lStrBatchNoFrom IS NOT NULL
                                AND ACDB_NDB.BATCH_NO BETWEEN p_lStrBatchNoFrom
                                                          AND p_lStrBatchNoTo))
                       AND (   p_lStrSourceFrom IS NULL
                            OR (    p_lStrSourceFrom IS NOT NULL
                                AND DMAG_VI_AGCAT_SOURCE.SOURCE_ID BETWEEN p_lStrSourceFrom
                                                                       AND p_lStrSourceTo))
                       AND (   p_lStrDocumentFrom IS NULL
                            OR (    p_lStrDocumentFrom IS NOT NULL
                                AND ACDB_NDB.AC_NO BETWEEN p_lStrDocumentFrom
                                                       AND p_lStrDocumentTo)))
         LOOP
            r_row.BATCHNO         := NULL;
            r_row.TRANDATE        := NULL;
            r_row.PAYMENTNO       := NULL;
            r_row.RECEIPTNO       := NULL;
            r_row.JOURNALNO       := NULL;
            r_row.AMOUNT          := NULL;
            r_row.CHEQUENO        := NULL;
            r_row.EXPCODE         := NULL;
            r_row.EXPAMOUNT       := NULL;
            r_row.STATEMENTDESC   := NULL;
            r_row.ACNO            := NULL;

            r_row.BATCHNO         := r.BATCH_NO;
            r_row.TRANDATE        := r.TRAN_DATE;
            r_row.PAYMENTNO       := r.AC_NO;
            r_row.RECEIPTNO       := NULL;
            r_row.JOURNALNO       := NULL;
            r_row.AMOUNT          := r.AMOUNT;
            r_row.CHEQUENO        := NULL;
            r_row.EXPCODE         := r.EXP_CODE;
            r_row.EXPAMOUNT       := r.EXP_AMT;
            r_row.STATEMENTDESC   := r.STMT_DESCP;
            r_row.ACNO            := r.AC_NO;

            PIPE ROW (r_row);
         END LOOP;
      --         --BATCH TOTAL
      --            SELECT COUNT(ACGC_BATCH.TOT_DOC) AS TOTAL_DOC, SUM(ACGC_BATCH.TOT_AMT) AS TOTAL_AMT
      --              FROM ACGC_BATCH
      --             WHERE ACGC_BATCH.BATCH_NO = :lStrHidBatchNo
      --          --BANK TOTAL
      --          SELECT CMUW_MORTGAGEE.GEN_LEDG, CMUW_MORTGAGEE.DESCP, GL.AC_DB_AMT
      --  FROM CMUW_MORTGAGEE,
      --       (SELECT ACDB_GL.AC_GLCODE, SUM(ACDB_GL.AC_CR_AMT - ACDB_GL.AC_DB_AMT) AS AC_DB_AMT
      --          FROM ACDB_GL WHERE ACDB_GL.AC_NO IN (SELECT ACDB_NDB.AC_NO FROM ACDB_NDB
      --                                                                                            LEFT OUTER JOIN MKAG_PROFILE ON (MKAG_PROFILE.AGENT_CODE = ACDB_NDB.AGENT_CODE)
      --                                                                                            WHERE :?MKAG_PROFILE_DTOBRANCH AND ACDB_NDB.BATCH_NO = :lStrHidBatchNo
      --                                                  AND ACDB_NDB.PROC_YR = :lStrProcYear
      --                                                  AND ACDB_NDB.PROC_MTH = :lStrProcMth)
      --         GROUP BY ACDB_GL.AC_GLCODE) GL
      --WHERE CMUW_MORTGAGEE.GEN_LEDG = GL.AC_GLCODE

      ELSIF p_lStrTranType = 'C'
      THEN
         FOR r
            IN (SELECT ACCR_NCR.BATCH_NO, ACCR_NCR.TRAN_DATE,
                       ACCR_NCR.AC_NO, ACCR_NCR.AMOUNT,
                       ACCR_NCR.EXP_CODE, ACCR_NCR.EXP_AMT,
                       ACCR_NCR.STMT_DESCP
                  FROM ACCR_NCR
                       LEFT OUTER JOIN
                       DMAG_VI_AGCAT_SOURCE
                          ON (DMAG_VI_AGCAT_SOURCE.AGENT_ID =
                                 ACCR_NCR.AGENT_ID)
                     -- start redmine 119949
                     -- LEFT OUTER JOIN DMAG_VI_AGENT
                    --      ON (DMAG_VI_AGENT.AGENTCODE = ACCR_NCR.AGENT_CODE)
                    -- end redmine 119949
                 WHERE     ((p_lStrBranchCodeFrom IS NULL AND p_lStrBranchCodeTo IS NULL) OR
                          ACCR_NCR.ISSUE_OFFICE BETWEEN p_lStrBranchCodeFrom AND p_lStrBranchCodeTo)
                       AND ACCR_NCR.PROC_YR = NVL(p_lIntProcYear,ACCR_NCR.PROC_YR)
                       AND ACCR_NCR.PROC_MTH = NVL(p_lIntProcMth,ACCR_NCR.PROC_MTH)
                       AND (   p_lStrBatchNoFrom IS NULL
                            OR (    p_lStrBatchNoFrom IS NOT NULL
                                AND ACCR_NCR.BATCH_NO BETWEEN p_lStrBatchNoFrom
                                                          AND p_lStrBatchNoTo))
                       AND (   p_lStrSourceFrom IS NULL
                            OR (    p_lStrSourceFrom IS NOT NULL
                                AND DMAG_VI_AGCAT_SOURCE.SOURCE_ID BETWEEN p_lStrSourceFrom
                                                                       AND p_lStrSourceTo))
                       AND (   p_lStrDocumentFrom IS NULL
                            OR (    p_lStrDocumentFrom IS NOT NULL
                                AND ACCR_NCR.AC_NO BETWEEN p_lStrDocumentFrom
                                                       AND p_lStrDocumentTo)))
         LOOP
            r_row.BATCHNO         := NULL;
            r_row.TRANDATE        := NULL;
            r_row.PAYMENTNO       := NULL;
            r_row.RECEIPTNO       := NULL;
            r_row.JOURNALNO       := NULL;
            r_row.AMOUNT          := NULL;
            r_row.CHEQUENO        := NULL;
            r_row.EXPCODE         := NULL;
            r_row.EXPAMOUNT       := NULL;
            r_row.STATEMENTDESC   := NULL;
            r_row.ACNO            := NULL;

            r_row.BATCHNO         := r.BATCH_NO;
            r_row.TRANDATE        := r.TRAN_DATE;
            r_row.PAYMENTNO       := r.AC_NO;
            r_row.RECEIPTNO       := NULL;
            r_row.JOURNALNO       := NULL;
            r_row.AMOUNT          := r.AMOUNT;
            r_row.CHEQUENO        := NULL;
            r_row.EXPCODE         := r.EXP_CODE;
            r_row.EXPAMOUNT       := r.EXP_AMT;
            r_row.STATEMENTDESC   := r.STMT_DESCP;
            r_row.ACNO            := r.AC_NO;

            PIPE ROW (r_row);
         END LOOP;
      --BATCH TOTAL
      --         SELECT COUNT(ACGC_BATCH.TOT_DOC) AS TOTAL_DOC, SUM(ACGC_BATCH.TOT_AMT) AS TOTAL_AMT
      --  FROM ACGC_BATCH
      -- WHERE ACGC_BATCH.BATCH_NO = :lStrHidBatchNo
      --
      -- --BANK TOTAL
      -- SELECT CMUW_MORTGAGEE.GEN_LEDG, CMUW_MORTGAGEE.DESCP, GL.AC_DB_AMT
      --  FROM CMUW_MORTGAGEE,
      --       (SELECT ACCR_GL.AC_GLCODE, SUM(ACCR_GL.AC_CR_AMT - ACCR_GL.AC_DB_AMT) AS AC_DB_AMT
      --          FROM ACCR_GL WHERE ACCR_GL.AC_NO IN (SELECT ACCR_NCR.AC_NO FROM ACCR_NCR
      --                                                                                            LEFT OUTER JOIN MKAG_PROFILE ON (MKAG_PROFILE.AGENT_CODE = ACCR_NCR.AGENT_CODE)
      --                                                                                            WHERE :?MKAG_PROFILE_CTOBRANCH AND ACCR_NCR.BATCH_NO = :lStrHidBatchNo
      --                                                  AND ACCR_NCR.PROC_YR = :lStrProcYear
      --                                                  AND ACCR_NCR.PROC_MTH = :lStrProcMth)
      --         GROUP BY ACCR_GL.AC_GLCODE) GL
      --WHERE CMUW_MORTGAGEE.GEN_LEDG = GL.AC_GLCODE

      ELSIF p_lStrTranType = 'SP'
      THEN
         FOR r
            IN (SELECT ACPY_RE_PYMT.BATCH_NO, ACPY_RE_PYMT.TRAN_DATE,
                       ACPY_RE_PYMT.AC_NO, ACPY_RE_PYMT.AMOUNT,
                       ACPY_RE_PYMT.CHQ_NO, ACPY_RE_PYMT.EXP_CODE,
                       ACPY_RE_PYMT.EXP_AMT, ACPY_RE_PYMT.STMT_DESCP
                  FROM ACPY_RE_PYMT
                       LEFT OUTER JOIN
                       DMAG_VI_AGCAT_SOURCE
                          ON (DMAG_VI_AGCAT_SOURCE.AGENT_ID =
                                 ACPY_RE_PYMT.AGENT_ID)
                -- start redmine 119949
                --    LEFT OUTER JOIN
                  --     DMAG_VI_AGENT
                  --        ON (DMAG_VI_AGENT.AGENTCODE =
                        --         ACPY_RE_PYMT.AGENT_CODE)
                -- end redmine 119949
                 WHERE     ((p_lStrBranchCodeFrom IS NULL AND p_lStrBranchCodeTo IS NULL) OR
                          ACPY_RE_PYMT.ISSUE_OFFICE BETWEEN p_lStrBranchCodeFrom AND p_lStrBranchCodeTo)
                       AND ACPY_RE_PYMT.PROC_YR = NVL(p_lIntProcYear,ACPY_RE_PYMT.PROC_YR)
                       AND ACPY_RE_PYMT.PROC_MTH = NVL(p_lIntProcMth,ACPY_RE_PYMT.PROC_MTH)
                       AND (   p_lStrBatchNoFrom IS NULL
                            OR (    p_lStrBatchNoFrom IS NOT NULL
                                AND ACPY_RE_PYMT.BATCH_NO BETWEEN p_lStrBatchNoFrom
                                                              AND p_lStrBatchNoTo))
                       AND (   p_lStrSourceFrom IS NULL
                            OR (    p_lStrSourceFrom IS NOT NULL
                                AND DMAG_VI_AGCAT_SOURCE.SOURCE_ID BETWEEN p_lStrSourceFrom
                                                                       AND p_lStrSourceTo))
                       AND (   p_lStrDocumentFrom IS NULL
                            OR (    p_lStrDocumentFrom IS NOT NULL
                                AND ACPY_RE_PYMT.AC_NO BETWEEN p_lStrDocumentFrom
                                                           AND p_lStrDocumentTo)))
         LOOP
            r_row.BATCHNO         := NULL;
            r_row.TRANDATE        := NULL;
            r_row.PAYMENTNO       := NULL;
            r_row.RECEIPTNO       := NULL;
            r_row.JOURNALNO       := NULL;
            r_row.AMOUNT          := NULL;
            r_row.CHEQUENO        := NULL;
            r_row.EXPCODE         := NULL;
            r_row.EXPAMOUNT       := NULL;
            r_row.STATEMENTDESC   := NULL;
            r_row.ACNO            := NULL;

            r_row.BATCHNO         := r.BATCH_NO;
            r_row.TRANDATE        := r.TRAN_DATE;
            r_row.PAYMENTNO       := r.AC_NO;
            r_row.RECEIPTNO       := NULL;
            r_row.JOURNALNO       := NULL;
            r_row.AMOUNT          := r.AMOUNT;
            r_row.CHEQUENO        := r.CHQ_NO;
            r_row.EXPCODE         := r.EXP_CODE;
            r_row.EXPAMOUNT       := r.EXP_AMT;
            r_row.STATEMENTDESC   := r.STMT_DESCP;
            r_row.ACNO            := r.AC_NO;

            PIPE ROW (r_row);
         END LOOP;
      --        SELECT COUNT(ACGC_BATCH.TOT_DOC) AS TOTAL_DOC, SUM(ACGC_BATCH.TOT_AMT) AS TOTAL_AMT
      --        FROM ACGC_BATCH
      --        WHERE ACGC_BATCH.BATCH_NO = :lStrHidBatchNo
      --
      --        SELECT CMUW_MORTGAGEE.GEN_LEDG, CMUW_MORTGAGEE.DESCP, GL.AC_DB_AMT
      --        FROM CMUW_MORTGAGEE,
      --        (SELECT ACPY_RE_GL.AC_GLCODE, SUM(ACPY_RE_GL.AC_CR_AMT - ACPY_RE_GL.AC_DB_AMT) AS AC_DB_AMT
      --        FROM ACPY_RE_GL WHERE ACPY_RE_GL.AC_NO IN (SELECT ACPY_RE_PYMT.AC_NO FROM ACPY_RE_PYMT
      --                                                                                        LEFT OUTER JOIN MKAG_PROFILE ON (MKAG_PROFILE.AGENT_CODE = ACPY_RE_PYMT.AGENT_CODE)
      --                                                                                        WHERE :?MKAG_PROFILE_SPTOBRANCH AND ACPY_RE_PYMT.BATCH_NO = :lStrHidBatchNo
      --                                              AND ACPY_RE_PYMT.PROC_YR = :lStrProcYear
      --                                              AND ACPY_RE_PYMT.PROC_MTH = :lStrProcMth)
      --        GROUP BY ACPY_RE_GL.AC_GLCODE) GL
      --        WHERE CMUW_MORTGAGEE.GEN_LEDG = GL.AC_GLCODE

      ELSIF p_lStrTranType = 'SJ'
      THEN
         FOR r
            IN (SELECT ACJN_RE_JOUR.BATCH_NO, ACJN_RE_JOUR.TRAN_DATE,
                       ACJN_RE_JOUR.AC_NO, ACJN_RE_JOUR.AMOUNT,
                       ACJN_RE_JOUR.EXP_CODE, ACJN_RE_JOUR.EXP_AMT,
                       ACJN_RE_JOUR.STMT_DESCP
                  FROM ACJN_RE_JOUR
                 WHERE     ACJN_RE_JOUR.PROC_YR = NVL(p_lIntProcYear,ACJN_RE_JOUR.PROC_YR)
                       AND ACJN_RE_JOUR.PROC_MTH = NVL(p_lIntProcMth,ACJN_RE_JOUR.PROC_MTH)
                       AND (   p_lStrBatchNoFrom IS NULL
                            OR (    p_lStrBatchNoFrom IS NOT NULL
                                AND ACJN_RE_JOUR.BATCH_NO BETWEEN p_lStrBatchNoFrom
                                                              AND p_lStrBatchNoTo))
                       AND (   p_lStrDocumentFrom IS NULL
                            OR (    p_lStrDocumentFrom IS NOT NULL
                                AND ACJN_RE_JOUR.AC_NO BETWEEN p_lStrDocumentFrom
                                                           AND p_lStrDocumentTo)))
         LOOP
            r_row.BATCHNO         := NULL;
            r_row.TRANDATE        := NULL;
            r_row.PAYMENTNO       := NULL;
            r_row.RECEIPTNO       := NULL;
            r_row.JOURNALNO       := NULL;
            r_row.AMOUNT          := NULL;
            r_row.CHEQUENO        := NULL;
            r_row.EXPCODE         := NULL;
            r_row.EXPAMOUNT       := NULL;
            r_row.STATEMENTDESC   := NULL;
            r_row.ACNO            := NULL;

            r_row.BATCHNO         := r.BATCH_NO;
            r_row.TRANDATE        := r.TRAN_DATE;
            r_row.PAYMENTNO       := NULL;
            r_row.RECEIPTNO       := NULL;
            r_row.JOURNALNO       := r.AC_NO;
            r_row.AMOUNT          := r.AMOUNT;
            r_row.CHEQUENO        := NULL;
            r_row.EXPCODE         := r.EXP_CODE;
            r_row.EXPAMOUNT       := r.EXP_AMT;
            r_row.STATEMENTDESC   := r.STMT_DESCP;
            r_row.ACNO            := r.AC_NO;

            PIPE ROW (r_row);
         END LOOP;
      END IF;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   END FN_RPAC_ACBATCHLIST;

   --RPAC_ACBATCHLIST Accounting Batch Listing (LIST) GL
   FUNCTION FN_RPAC_ACBATCHLIST_GL ( p_acNo VARCHAR2, p_lStrTranType VARCHAR2,p_chequeNo VARCHAR2)
      RETURN RPAC_ACBATCHLIST_GL_TAB
      PIPELINED
   IS
      v_ProcName_v   VARCHAR2 (30) := 'FN_RPAC_ACBATCHLIST_GL';
      v_Step_v       VARCHAR2 (5) := '000';
      r_row          RPAC_ACBATCHLIST_GL_REC;
      v_nett_amt     NUMBER (22, 2);
   BEGIN
      IF p_lStrTranType = 'P'
      THEN
         FOR r IN (SELECT ACPY_GL.AC_AGENT_ID,
                          ACPY_GL.AC_GLCODE,
                          (SELECT DESCP
                             FROM ACGL_LEDGER
                            WHERE UKEY = ACPY_GL.AC_GLCODE)
                             AS GL_DESC,
                          ACPY_GL.AC_DB_AMT,
                          ACPY_GL.AC_CR_AMT,
                          ACPY_GL.AC_NO,
                          ACPY_GL.GL_SEQ_NO
                     FROM ACPY_GL
                    WHERE ACPY_GL.AC_NO = p_acNo ORDER BY AC_GLCODE)
         LOOP
            r_row.ACNO         := NULL;
            r_row.GL_SEQ_NO    := NULL;
            r_row.ACCTCODE     := NULL;
            r_row.LEDGERTYPE   := NULL;
            r_row.GLCODE       := NULL;
            r_row.GLDESC       := NULL;
            r_row.DEBITAMT     := NULL;
            r_row.CREDITAMT    := NULL;

            r_row.ACNO         := r.AC_NO;
            r_row.GL_SEQ_NO    := r.GL_SEQ_NO;
            r_row.ACCTCODE     := r.AC_AGENT_ID;
            r_row.LEDGERTYPE   := NULL;
            r_row.GLCODE       := r.AC_GLCODE;
            r_row.GLDESC       := r.GL_DESC;
            r_row.DEBITAMT     := r.AC_DB_AMT;
            r_row.CREDITAMT    := r.AC_CR_AMT;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'R'
      THEN
         FOR r IN (SELECT ACRC_GL.AC_AGENT_ID,
                          ACRC_GL.AC_GLCODE,
                          ACRC_GL.AC_DB_AMT,
                          ACRC_GL.AC_CR_AMT,
                          (SELECT DESCP
                             FROM ACGL_LEDGER
                            WHERE UKEY = ACRC_GL.AC_GLCODE)
                             AS GL_DESC,
                          ACRC_GL.AC_NO,
                          ACRC_GL.GL_SEQ_NO
                     FROM ACRC_GL--,ACRC_BANK --96593
                    WHERE ACRC_GL.AC_NO = p_acNo
                    --and ACRC_BANK.UKEY_BANK=ACRC_GL.UKEY_GL  --96593
                    ORDER BY AC_GLCODE)
         LOOP
            r_row.ACNO         := NULL;
            r_row.GL_SEQ_NO    := NULL;
            r_row.ACCTCODE     := NULL;
            r_row.LEDGERTYPE   := NULL;
            r_row.GLCODE       := NULL;
            r_row.GLDESC       := NULL;
            r_row.DEBITAMT     := NULL;
            r_row.CREDITAMT    := NULL;
            v_nett_amt         := NULL;
            
                        ----96593
            begin
            SELECT B.NET_AMT INTO v_nett_amt FROM ACRC_BANK b WHERE B.AC_NO=p_acNo
            AND B.CHQ_NO=p_chequeNo;
            exception WHEN others THEN
            NULL;
            END;
            ----96593

            r_row.ACNO         := r.AC_NO;
            r_row.GL_SEQ_NO    := r.GL_SEQ_NO;
            r_row.ACCTCODE     := r.AC_AGENT_ID;
            r_row.LEDGERTYPE   := NULL;
            r_row.GLCODE       := r.AC_GLCODE;
            r_row.GLDESC       := r.GL_DESC;
            --r_row.DEBITAMT     := r.AC_DB_AMT;
            --r_row.CREDITAMT    := r.AC_CR_AMT;
            
            if p_chequeNo is not null then
            IF r.AC_DB_AMT >0 THEN ----96593
            r_row.DEBITAMT     := v_nett_amt;
            ELSE
            r_row.DEBITAMT     :=0;
            END IF;
            if r.AC_CR_AMT >0 then
            r_row.CREDITAMT    := v_nett_amt;
            ELSE
            r_row.CREDITAMT    :=0;
              END IF;
            else
              r_row.DEBITAMT     := r.AC_DB_AMT;
              r_row.CREDITAMT    := r.AC_CR_AMT;
            end if;
            
             ----96593

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'J'
      THEN
         FOR r IN (SELECT ACJN_GL.AC_AGENT_ID,
                          ACJN_GL.LEDG_TYPE,
                          ACJN_GL.AC_GLCODE,
                          (SELECT DESCP
                             FROM ACGL_LEDGER
                            WHERE UKEY = ACJN_GL.AC_GLCODE)
                             AS GL_DESC,
                          ACJN_GL.AC_DB_AMT,
                          ACJN_GL.AC_CR_AMT,
                          ACJN_GL.AC_NO,
                          ACJN_GL.GL_SEQ_NO
                     FROM ACJN_GL
                    WHERE ACJN_GL.AC_NO = p_acNo ORDER BY AC_GLCODE)
         LOOP
            r_row.ACNO         := NULL;
            r_row.GL_SEQ_NO    := NULL;
            r_row.ACCTCODE     := NULL;
            r_row.LEDGERTYPE   := NULL;
            r_row.GLCODE       := NULL;
            r_row.GLDESC       := NULL;
            r_row.DEBITAMT     := NULL;
            r_row.CREDITAMT    := NULL;

            r_row.ACNO         := r.AC_NO;
            r_row.GL_SEQ_NO    := r.GL_SEQ_NO;
            r_row.ACCTCODE     := r.AC_AGENT_ID;
            r_row.LEDGERTYPE   := r.LEDG_TYPE;
            r_row.GLCODE       := r.AC_GLCODE;
            r_row.GLDESC       := r.GL_DESC;
            r_row.DEBITAMT     := r.AC_DB_AMT;
            r_row.CREDITAMT    := r.AC_CR_AMT;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'D'
      THEN
         FOR r IN (SELECT ACDB_GL.AC_AGENT_ID,
                          ACDB_GL.AC_GLCODE,
                          (SELECT DESCP
                             FROM ACGL_LEDGER
                            WHERE UKEY = ACDB_GL.AC_GLCODE)
                             AS GL_DESC,
                          ACDB_GL.AC_DB_AMT,
                          ACDB_GL.AC_CR_AMT,
                          ACDB_GL.AC_NO,
                          ACDB_GL.GL_SEQ_NO
                     FROM ACDB_GL
                    WHERE ACDB_GL.AC_NO = p_acNo ORDER BY AC_GLCODE)
         LOOP
            r_row.ACNO         := NULL;
            r_row.GL_SEQ_NO    := NULL;
            r_row.ACCTCODE     := NULL;
            r_row.LEDGERTYPE   := NULL;
            r_row.GLCODE       := NULL;
            r_row.GLDESC       := NULL;
            r_row.DEBITAMT     := NULL;
            r_row.CREDITAMT    := NULL;

            r_row.ACNO         := r.AC_NO;
            r_row.GL_SEQ_NO    := r.GL_SEQ_NO;
            r_row.ACCTCODE     := r.AC_AGENT_ID;
            r_row.LEDGERTYPE   := NULL;
            r_row.GLCODE       := r.AC_GLCODE;
            r_row.GLDESC       := r.GL_DESC;
            r_row.DEBITAMT     := r.AC_DB_AMT;
            r_row.CREDITAMT    := r.AC_CR_AMT;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'C'
      THEN
         FOR r IN (SELECT ACCR_GL.AC_AGENT_ID,
                          ACCR_GL.AC_GLCODE,
                          ACCR_GL.AC_DB_AMT,
                          ACCR_GL.AC_CR_AMT,
                          (SELECT DESCP
                             FROM ACGL_LEDGER
                            WHERE UKEY = ACCR_GL.AC_GLCODE)
                             AS GL_DESC,
                          ACCR_GL.AC_NO,
                          ACCR_GL.GL_SEQ_NO
                     FROM ACCR_GL
                    WHERE ACCR_GL.AC_NO = p_acNo  ORDER BY AC_GLCODE)
         LOOP
            r_row.ACNO         := NULL;
            r_row.GL_SEQ_NO    := NULL;
            r_row.ACCTCODE     := NULL;
            r_row.LEDGERTYPE   := NULL;
            r_row.GLCODE       := NULL;
            r_row.GLDESC       := NULL;
            r_row.DEBITAMT     := NULL;
            r_row.CREDITAMT    := NULL;

            r_row.ACNO         := r.AC_NO;
            r_row.GL_SEQ_NO    := r.GL_SEQ_NO;
            r_row.ACCTCODE     := r.AC_AGENT_ID;
            r_row.LEDGERTYPE   := NULL;
            r_row.GLCODE       := r.AC_GLCODE;
            r_row.GLDESC       := r.GL_DESC;
            r_row.DEBITAMT     := r.AC_DB_AMT;
            r_row.CREDITAMT    := r.AC_CR_AMT;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'SP'
      THEN
         FOR r IN (SELECT ACPY_RE_GL.AC_AGENT_ID,
                          ACPY_RE_GL.AC_GLCODE,
                          (SELECT DESCP
                             FROM ACGL_LEDGER
                            WHERE UKEY = ACPY_RE_GL.AC_GLCODE)
                             AS GL_DESC,
                          ACPY_RE_GL.AC_DB_AMT,
                          ACPY_RE_GL.AC_CR_AMT,
                          ACPY_RE_GL.AC_NO,
                          ACPY_RE_GL.GL_SEQ_NO
                     FROM ACPY_RE_GL
                    WHERE ACPY_RE_GL.AC_NO = p_acNo  ORDER BY AC_GLCODE )
         LOOP
            r_row.ACNO         := NULL;
            r_row.GL_SEQ_NO    := NULL;
            r_row.ACCTCODE     := NULL;
            r_row.LEDGERTYPE   := NULL;
            r_row.GLCODE       := NULL;
            r_row.GLDESC       := NULL;
            r_row.DEBITAMT     := NULL;
            r_row.CREDITAMT    := NULL;

            r_row.ACNO         := r.AC_NO;
            r_row.GL_SEQ_NO    := r.GL_SEQ_NO;
            r_row.ACCTCODE     := r.AC_AGENT_ID;
            r_row.LEDGERTYPE   := NULL;
            r_row.GLCODE       := r.AC_GLCODE;
            r_row.GLDESC       := r.GL_DESC;
            r_row.DEBITAMT     := r.AC_DB_AMT;
            r_row.CREDITAMT    := r.AC_CR_AMT;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'SJ'
      THEN
         FOR r IN (SELECT ACJN_RE_GL.AC_AGENT_ID,
                          ACJN_RE_GL.AC_GLCODE,
                          (SELECT DESCP
                             FROM ACGL_LEDGER
                            WHERE UKEY = ACJN_RE_GL.AC_GLCODE)
                             AS GL_DESC,
                          ACJN_RE_GL.LEDG_TYPE,
                          ACJN_RE_GL.AC_DB_AMT,
                          ACJN_RE_GL.AC_CR_AMT,
                          ACJN_RE_GL.AC_NO,
                          ACJN_RE_GL.GL_SEQ_NO
                     FROM ACJN_RE_GL
                    WHERE ACJN_RE_GL.AC_NO = p_acNo ORDER BY AC_GLCODE)
         LOOP
            r_row.ACNO         := NULL;
            r_row.GL_SEQ_NO    := NULL;
            r_row.ACCTCODE     := NULL;
            r_row.LEDGERTYPE   := NULL;
            r_row.GLCODE       := NULL;
            r_row.GLDESC       := NULL;
            r_row.DEBITAMT     := NULL;
            r_row.CREDITAMT    := NULL;

            r_row.ACNO         := r.AC_NO;
            r_row.GL_SEQ_NO    := r.GL_SEQ_NO;
            r_row.ACCTCODE     := r.AC_AGENT_ID;
            r_row.LEDGERTYPE   := r.LEDG_TYPE;
            r_row.GLCODE       := r.AC_GLCODE;
            r_row.GLDESC       := r.GL_DESC;
            r_row.DEBITAMT     := r.AC_DB_AMT;
            r_row.CREDITAMT    := r.AC_CR_AMT;

            PIPE ROW (r_row);
         END LOOP;
      END IF;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   END FN_RPAC_ACBATCHLIST_GL;

   --RPAC_ACBATCHLIST Accounting Batch Listing (LIST) GLKO
   FUNCTION FN_RPAC_ACBATCHLIST_GLKO ( p_acNo VARCHAR2, p_glSeqNo NUMBER, p_lStrTranType VARCHAR2)
      RETURN RPAC_ACBATCHLIST_GLKO_TAB
      PIPELINED
   IS
      v_ProcName_v   VARCHAR2 (30) := 'FN_RPAC_ACBATCHLIST_GLKO';
      v_Step_v       VARCHAR2 (5) := '000';
      r_row          RPAC_ACBATCHLIST_GLKO_REC;
   BEGIN
      IF p_lStrTranType = 'P'
      THEN
         FOR r
            IN (SELECT ACPY_KO.DOC_NO, ACPY_KO.DOC_AMT,
                       ACPY_KO.ST_SEQ_NO, ACPY_KO.AC_NO,
                       ACPY_KO.GL_SEQ_NO
                  FROM ACPY_KO
                 WHERE     ACPY_KO.AC_NO = p_acNo
                       AND ACPY_KO.GL_SEQ_NO = p_glSeqNo)
         LOOP
            r_row.ACNO         := NULL;
            r_row.GL_SEQ_NO    := NULL;
            r_row.POLICYNO     := NULL;
            r_row.TRANAMOUNT   := NULL;
            r_row.KOFFSEQNO    := NULL;

            r_row.ACNO         := r.AC_NO;
            r_row.GL_SEQ_NO    := r.GL_SEQ_NO;
            r_row.POLICYNO     := r.DOC_NO;
            r_row.TRANAMOUNT   := r.DOC_AMT;
            r_row.KOFFSEQNO    := r.ST_SEQ_NO;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'R'
      THEN
         FOR r
            IN (SELECT ACRC_KO.DOC_NO, ACRC_KO.DOC_AMT,
                       ACRC_KO.ST_SEQ_NO, ACRC_KO.AC_NO,
                       ACRC_KO.GL_SEQ_NO
                  FROM ACRC_KO
                 WHERE     ACRC_KO.AC_NO = p_acNo
                       AND ACRC_KO.GL_SEQ_NO = p_glSeqNo)
         LOOP
            r_row.ACNO         := NULL;
            r_row.GL_SEQ_NO    := NULL;
            r_row.POLICYNO     := NULL;
            r_row.TRANAMOUNT   := NULL;
            r_row.KOFFSEQNO    := NULL;

            r_row.ACNO         := r.AC_NO;
            r_row.GL_SEQ_NO    := r.GL_SEQ_NO;
            r_row.POLICYNO     := r.DOC_NO;
            r_row.TRANAMOUNT   := r.DOC_AMT;
            r_row.KOFFSEQNO    := r.ST_SEQ_NO;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'J'
      THEN
         FOR r
            IN (SELECT ACJN_KO.DOC_NO, ACJN_KO.DOC_AMT,
                       ACJN_KO.ST_SEQ_NO, ACJN_KO.AC_NO,
                       ACJN_KO.GL_SEQ_NO
                  FROM ACJN_KO
                 WHERE     ACJN_KO.AC_NO = p_acNo
                       AND ACJN_KO.GL_SEQ_NO = p_glSeqNo)
         LOOP
            r_row.ACNO         := NULL;
            r_row.GL_SEQ_NO    := NULL;
            r_row.POLICYNO     := NULL;
            r_row.TRANAMOUNT   := NULL;
            r_row.KOFFSEQNO    := NULL;

            r_row.ACNO         := r.AC_NO;
            r_row.GL_SEQ_NO    := r.GL_SEQ_NO;
            r_row.POLICYNO     := r.DOC_NO;
            r_row.TRANAMOUNT   := r.DOC_AMT;
            r_row.KOFFSEQNO    := r.ST_SEQ_NO;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'D'
      THEN
         FOR r
            IN (SELECT ACDB_KO.DOC_NO, ACDB_KO.DOC_AMT,
                       ACDB_KO.ST_SEQ_NO, ACDB_KO.AC_NO,
                       ACDB_KO.GL_SEQ_NO
                  FROM ACDB_KO
                 WHERE     ACDB_KO.AC_NO = p_acNo
                       AND ACDB_KO.GL_SEQ_NO = p_glSeqNo)
         LOOP
            r_row.ACNO         := NULL;
            r_row.GL_SEQ_NO    := NULL;
            r_row.POLICYNO     := NULL;
            r_row.TRANAMOUNT   := NULL;
            r_row.KOFFSEQNO    := NULL;

            r_row.ACNO         := r.AC_NO;
            r_row.GL_SEQ_NO    := r.GL_SEQ_NO;
            r_row.POLICYNO     := r.DOC_NO;
            r_row.TRANAMOUNT   := r.DOC_AMT;
            r_row.KOFFSEQNO    := r.ST_SEQ_NO;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'C'
      THEN
         FOR r
            IN (SELECT ACCR_KO.DOC_NO, ACCR_KO.DOC_AMT,
                       ACCR_KO.ST_SEQ_NO, ACCR_KO.AC_NO,
                       ACCR_KO.GL_SEQ_NO
                  FROM ACCR_KO
                 WHERE     ACCR_KO.AC_NO = p_acNo
                       AND ACCR_KO.GL_SEQ_NO = p_glSeqNo)
         LOOP
            r_row.ACNO         := NULL;
            r_row.GL_SEQ_NO    := NULL;
            r_row.POLICYNO     := NULL;
            r_row.TRANAMOUNT   := NULL;
            r_row.KOFFSEQNO    := NULL;

            r_row.ACNO         := r.AC_NO;
            r_row.GL_SEQ_NO    := r.GL_SEQ_NO;
            r_row.POLICYNO     := r.DOC_NO;
            r_row.TRANAMOUNT   := r.DOC_AMT;
            r_row.KOFFSEQNO    := r.ST_SEQ_NO;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'SP'
      THEN
         NULL;
      ELSIF p_lStrTranType = 'SJ'
      THEN
         NULL;
      END IF;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   END FN_RPAC_ACBATCHLIST_GLKO;

   --RPAC_ACBATCHLIST Accounting Batch Listing (LIST) GLretro
   FUNCTION FN_RPAC_ACBATCHLIST_GLRETRO ( p_acNo VARCHAR2, p_glSeqNo NUMBER, p_lStrTranType VARCHAR2)
      RETURN RPAC_ACBATCHLIST_GLRETRO_TAB
      PIPELINED
   IS
      v_ProcName_v   VARCHAR2 (30) := 'FN_RPAC_ACBATCHLIST_GLRETRO';
      v_Step_v       VARCHAR2 (5) := '000';
      r_row          RPAC_ACBATCHLIST_GLRETRO_REC;
   BEGIN
      IF p_lStrTranType = 'J'
      THEN
         FOR r
            IN (SELECT ACJN_RETRO.CLS, ACJN_RETRO.SOURCE_ID,
                       ACJN_RETRO.AC_NO, ACJN_RETRO.GL_SEQ_NO
                  FROM ACJN_RETRO
                 WHERE     ACJN_RETRO.AC_NO = p_acNo
                       AND ACJN_RETRO.GL_SEQ_NO = p_glSeqNo)
         LOOP
            r_row.ACNO         := NULL;
            r_row.GL_SEQ_NO    := NULL;
            r_row.CLASSCODE    := NULL;
            r_row.SOURCECODE   := NULL;

            r_row.ACNO         := r.AC_NO;
            r_row.GL_SEQ_NO    := r.GL_SEQ_NO;
            r_row.CLASSCODE    := r.CLS;
            r_row.SOURCECODE   := r.SOURCE_ID;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'SJ'
      THEN
         FOR r
            IN (SELECT ACJN_RE_RETRO.CLS, ACJN_RE_RETRO.SOURCE_ID,
                       ACJN_RE_RETRO.AC_NO, ACJN_RE_RETRO.GL_SEQ_NO
                  FROM ACJN_RE_RETRO
                 WHERE     ACJN_RE_RETRO.AC_NO = p_acNo
                       AND ACJN_RE_RETRO.GL_SEQ_NO = p_glSeqNo)
         LOOP
            r_row.ACNO         := NULL;
            r_row.GL_SEQ_NO    := NULL;
            r_row.CLASSCODE    := NULL;
            r_row.SOURCECODE   := NULL;

            r_row.ACNO         := r.AC_NO;
            r_row.GL_SEQ_NO    := r.GL_SEQ_NO;
            r_row.CLASSCODE    := r.CLS;
            r_row.SOURCECODE   := r.SOURCE_ID;

            PIPE ROW (r_row);
         END LOOP;
      END IF;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   END FN_RPAC_ACBATCHLIST_GLRETRO;

   --RPAC_ACBATCHLIST Accounting Batch Listing (LIST) - BANK TOTAL
   FUNCTION FN_RPAC_ACBATCHLIST_BANK (p_BATCHNO               ACPY_PYMT.BATCH_NO%TYPE,
                                      p_lStrBranchCodeFrom    VARCHAR2,
                                      p_lStrBranchCodeTo      VARCHAR2,
                                      p_lStrSourceFrom        VARCHAR2,
                                      p_lStrSourceTo          VARCHAR2,
                                      p_lStrDocumentFrom      VARCHAR2,
                                      p_lStrDocumentTo        VARCHAR2,
                                      p_lIntProcYear          VARCHAR2,
                                      p_lIntProcMth           VARCHAR2,
                                      p_lStrTranType          VARCHAR2
                                     )
      RETURN RPAC_ACBATCHLIST_TOTAL_T
      PIPELINED
   IS
      v_ProcName_v     VARCHAR2 (30) := 'FN_RPAC_ACBATCHLIST_BANK';
      v_Step_v         VARCHAR2 (5) := '000';
      r_row            RPAC_ACBATCHLIST_TOTAL_R;
      v_totalDoc       NUMBER (10) := 0;
      v_totalAmt       NUMBER (22, 2) := 0;
      v_compTotalDoc   NUMBER (10) := 0;
      v_compTotalAmt   NUMBER (22, 2) := 0;
   BEGIN
      FOR r IN (SELECT COUNT (ACGC_BATCH.TOT_DOC) AS TOTAL_DOC, SUM (ACGC_BATCH.TOT_AMT) AS TOTAL_AMT
                  FROM ACGC_BATCH
                 WHERE ACGC_BATCH.BATCH_NO = p_BATCHNO)
      LOOP
         r_row.BATCHNO          := NULL;
         r_row.BATCHTOTAL       := NULL;
         r_row.BATCHCOMPTOTAL   := NULL;
         r_row.BATCHAMT         := NULL;
         r_row.BATCHCOMPAMT     := NULL;
         r_row.GENLEDGE         := NULL;
         r_row.BANKTOTALAMT     := NULL;

         v_totalDoc             := r.TOTAL_DOC;
         v_totalAmt             := r.TOTAL_AMT;

         r_row.BATCHNO          := p_BATCHNO;
         r_row.BATCHTOTAL       := r.TOTAL_DOC;
         r_row.BATCHCOMPTOTAL   := v_compTotalDoc;
         r_row.BATCHAMT         := r.TOTAL_AMT;
         r_row.BATCHCOMPAMT     := v_compTotalAmt;
         r_row.GENLEDGE         := NULL;
         r_row.BANKTOTALAMT     := NULL;

        -- PIPE ROW (r_row);
      END LOOP;

      IF p_lStrTranType = 'P'
      THEN
         FOR r
            IN (SELECT CMUW_MORTGAGEE.GEN_LEDG, CMUW_MORTGAGEE.DESCP,
                       GL.AC_DB_AMT
                  FROM CMUW_MORTGAGEE,
                       (  SELECT ACPY_GL.AC_GLCODE, SUM (ACPY_GL.AC_DB_AMT - ACPY_GL.AC_CR_AMT) AS AC_DB_AMT
                            FROM ACPY_GL
                           WHERE ACPY_GL.AC_NO IN
                                    (SELECT ACPY_PYMT.AC_NO
                                       FROM ACPY_PYMT
                                            LEFT OUTER JOIN
                                            DMAG_VI_AGENT
                                               ON (DMAG_VI_AGENT.AGENTCODE =
                                                      ACPY_PYMT.AGENT_CODE)
                                      WHERE     ((p_lStrBranchCodeFrom is null AND p_lStrBranchCodeTo is null)  OR (DMAG_VI_AGENT.BRANCH_CODE BETWEEN p_lStrBranchCodeFrom
                                                                              AND p_lStrBranchCodeTo))
                                            AND ACPY_PYMT.BATCH_NO = p_BATCHNO
                                            AND ACPY_PYMT.PROC_YR =
                                                   p_lIntProcYear
                                            AND ACPY_PYMT.PROC_MTH =
                                                   p_lIntProcMth)
                        GROUP BY ACPY_GL.AC_GLCODE) GL
                 WHERE CMUW_MORTGAGEE.GEN_LEDG = GL.AC_GLCODE)
         LOOP
            r_row.BATCHNO          := NULL;
            r_row.BATCHTOTAL       := NULL;
            r_row.BATCHCOMPTOTAL   := NULL;
            r_row.BATCHAMT         := NULL;
            r_row.BATCHCOMPAMT     := NULL;
            r_row.GENLEDGE         := NULL;
            r_row.BANKTOTALAMT     := NULL;

            r_row.BATCHNO          := p_BATCHNO;
            r_row.BATCHTOTAL       := v_totalDoc;
            r_row.BATCHCOMPTOTAL   := v_compTotalDoc;
            r_row.BATCHAMT         := v_totalAmt;
            r_row.BATCHCOMPAMT     := v_compTotalAmt;
            r_row.GENLEDGE         := r.GEN_LEDG || ' ' || r.DESCP;
            r_row.BANKTOTALAMT     := r.AC_DB_AMT;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'R'
      THEN
         FOR r
            IN (SELECT CMUW_MORTGAGEE.GEN_LEDG, CMUW_MORTGAGEE.DESCP,
                       GL.AC_DB_AMT
                  FROM CMUW_MORTGAGEE,
                       (  SELECT ACRC_GL.AC_GLCODE, SUM (ACRC_GL.AC_CR_AMT - ACRC_GL.AC_DB_AMT) AS AC_DB_AMT
                            FROM ACRC_GL
                           WHERE ACRC_GL.AC_NO IN
                                    (SELECT ACRC_RCPT.AC_NO
                                       FROM ACRC_RCPT
                                            LEFT OUTER JOIN
                                            DMAG_VI_AGENT
                                               ON (DMAG_VI_AGENT.AGENTCODE =
                                                      ACRC_RCPT.AGENT_CODE)
                                      WHERE     ((p_lStrBranchCodeFrom is null AND p_lStrBranchCodeTo is null)  OR (DMAG_VI_AGENT.BRANCH_CODE BETWEEN p_lStrBranchCodeFrom
                                                                              AND p_lStrBranchCodeTo))
                                            AND ACRC_RCPT.BATCH_NO = p_BATCHNO
                                            AND ACRC_RCPT.PROC_YR =
                                                   p_lIntProcYear
                                            AND ACRC_RCPT.PROC_MTH =
                                                   p_lIntProcMth)
                        GROUP BY ACRC_GL.AC_GLCODE) GL
                 WHERE CMUW_MORTGAGEE.GEN_LEDG = GL.AC_GLCODE)
         LOOP
            r_row.BATCHNO          := NULL;
            r_row.BATCHTOTAL       := NULL;
            r_row.BATCHCOMPTOTAL   := NULL;
            r_row.BATCHAMT         := NULL;
            r_row.BATCHCOMPAMT     := NULL;
            r_row.GENLEDGE         := NULL;
            r_row.BANKTOTALAMT     := NULL;

            r_row.BATCHNO          := p_BATCHNO;
            r_row.BATCHTOTAL       := v_totalDoc;
            r_row.BATCHCOMPTOTAL   := v_compTotalDoc;
            r_row.BATCHAMT         := v_totalAmt;
            r_row.BATCHCOMPAMT     := v_compTotalAmt;
            r_row.GENLEDGE         := r.GEN_LEDG || ' ' || r.DESCP;
            r_row.BANKTOTALAMT     := r.AC_DB_AMT;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'J'
      THEN
         FOR r
            IN (SELECT CMUW_MORTGAGEE.GEN_LEDG, CMUW_MORTGAGEE.DESCP,
                       GL.AC_DB_AMT
                  FROM CMUW_MORTGAGEE,
                       (  SELECT ACJN_GL.AC_GLCODE, SUM (ACJN_GL.AC_CR_AMT - ACJN_GL.AC_DB_AMT) AS AC_DB_AMT
                            FROM ACJN_GL
                           WHERE ACJN_GL.AC_NO IN
                                    (SELECT ACJN_JOUR.AC_NO
                                       FROM ACJN_JOUR
                                            LEFT OUTER JOIN
                                            DMAG_VI_AGENT
                                               ON (DMAG_VI_AGENT.AGENTCODE =
                                                      ACJN_JOUR.AGENT_CODE)
                                      WHERE     ((p_lStrBranchCodeFrom is null AND p_lStrBranchCodeTo is null)  OR (DMAG_VI_AGENT.BRANCH_CODE BETWEEN p_lStrBranchCodeFrom
                                                                              AND p_lStrBranchCodeTo))
                                            AND ACJN_JOUR.BATCH_NO = p_BATCHNO
                                            AND ACJN_JOUR.PROC_YR =
                                                   p_lIntProcYear
                                            AND ACJN_JOUR.PROC_MTH =
                                                   p_lIntProcMth)
                        GROUP BY ACJN_GL.AC_GLCODE) GL
                 WHERE CMUW_MORTGAGEE.GEN_LEDG = GL.AC_GLCODE)
         LOOP
            r_row.BATCHNO          := NULL;
            r_row.BATCHTOTAL       := NULL;
            r_row.BATCHCOMPTOTAL   := NULL;
            r_row.BATCHAMT         := NULL;
            r_row.BATCHCOMPAMT     := NULL;
            r_row.GENLEDGE         := NULL;
            r_row.BANKTOTALAMT     := NULL;

            r_row.BATCHNO          := p_BATCHNO;
            r_row.BATCHTOTAL       := v_totalDoc;
            r_row.BATCHCOMPTOTAL   := v_compTotalDoc;
            r_row.BATCHAMT         := v_totalAmt;
            r_row.BATCHCOMPAMT     := v_compTotalAmt;
            r_row.GENLEDGE         := r.GEN_LEDG || ' ' || r.DESCP;
            r_row.BANKTOTALAMT     := r.AC_DB_AMT;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'D'
      THEN
         FOR r
            IN (SELECT CMUW_MORTGAGEE.GEN_LEDG, CMUW_MORTGAGEE.DESCP,
                       GL.AC_DB_AMT
                  FROM CMUW_MORTGAGEE,
                       (  SELECT ACDB_GL.AC_GLCODE, SUM (ACDB_GL.AC_CR_AMT - ACDB_GL.AC_DB_AMT) AS AC_DB_AMT
                            FROM ACDB_GL
                           WHERE ACDB_GL.AC_NO IN
                                    (SELECT ACDB_NDB.AC_NO
                                       FROM ACDB_NDB
                                            LEFT OUTER JOIN
                                            DMAG_VI_AGENT
                                               ON (DMAG_VI_AGENT.AGENTCODE =
                                                      ACDB_NDB.AGENT_CODE)
                                      WHERE     ((p_lStrBranchCodeFrom is null AND p_lStrBranchCodeTo is null)  OR (DMAG_VI_AGENT.BRANCH_CODE BETWEEN p_lStrBranchCodeFrom
                                                                              AND p_lStrBranchCodeTo))
                                            AND ACDB_NDB.BATCH_NO = p_BATCHNO
                                            AND ACDB_NDB.PROC_YR =
                                                   p_lIntProcYear
                                            AND ACDB_NDB.PROC_MTH =
                                                   p_lIntProcMth)
                        GROUP BY ACDB_GL.AC_GLCODE) GL
                 WHERE CMUW_MORTGAGEE.GEN_LEDG = GL.AC_GLCODE)
         LOOP
            r_row.BATCHNO          := NULL;
            r_row.BATCHTOTAL       := NULL;
            r_row.BATCHCOMPTOTAL   := NULL;
            r_row.BATCHAMT         := NULL;
            r_row.BATCHCOMPAMT     := NULL;
            r_row.GENLEDGE         := NULL;
            r_row.BANKTOTALAMT     := NULL;

            r_row.BATCHNO          := p_BATCHNO;
            r_row.BATCHTOTAL       := v_totalDoc;
            r_row.BATCHCOMPTOTAL   := v_compTotalDoc;
            r_row.BATCHAMT         := v_totalAmt;
            r_row.BATCHCOMPAMT     := v_compTotalAmt;
            r_row.GENLEDGE         := r.GEN_LEDG || ' ' || r.DESCP;
            r_row.BANKTOTALAMT     := r.AC_DB_AMT;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'C'
      THEN
         FOR r
            IN (SELECT CMUW_MORTGAGEE.GEN_LEDG, CMUW_MORTGAGEE.DESCP,
                       GL.AC_DB_AMT
                  FROM CMUW_MORTGAGEE,
                       (  SELECT ACCR_GL.AC_GLCODE, SUM (ACCR_GL.AC_CR_AMT - ACCR_GL.AC_DB_AMT) AS AC_DB_AMT
                            FROM ACCR_GL
                           WHERE ACCR_GL.AC_NO IN
                                    (SELECT ACCR_NCR.AC_NO
                                       FROM ACCR_NCR
                                            LEFT OUTER JOIN
                                            DMAG_VI_AGENT
                                               ON (DMAG_VI_AGENT.AGENTCODE =
                                                      ACCR_NCR.AGENT_CODE)
                                      WHERE     ((p_lStrBranchCodeFrom is null AND p_lStrBranchCodeTo is null)  OR (DMAG_VI_AGENT.BRANCH_CODE BETWEEN p_lStrBranchCodeFrom
                                                                              AND p_lStrBranchCodeTo))
                                            AND ACCR_NCR.BATCH_NO = p_BATCHNO
                                            AND ACCR_NCR.PROC_YR =
                                                   p_lIntProcYear
                                            AND ACCR_NCR.PROC_MTH =
                                                   p_lIntProcMth)
                        GROUP BY ACCR_GL.AC_GLCODE) GL
                 WHERE CMUW_MORTGAGEE.GEN_LEDG = GL.AC_GLCODE)
         LOOP
            r_row.BATCHNO          := NULL;
            r_row.BATCHTOTAL       := NULL;
            r_row.BATCHCOMPTOTAL   := NULL;
            r_row.BATCHAMT         := NULL;
            r_row.BATCHCOMPAMT     := NULL;
            r_row.GENLEDGE         := NULL;
            r_row.BANKTOTALAMT     := NULL;

            r_row.BATCHNO          := p_BATCHNO;
            r_row.BATCHTOTAL       := v_totalDoc;
            r_row.BATCHCOMPTOTAL   := v_compTotalDoc;
            r_row.BATCHAMT         := v_totalAmt;
            r_row.BATCHCOMPAMT     := v_compTotalAmt;
            r_row.GENLEDGE         := r.GEN_LEDG || ' ' || r.DESCP;
            r_row.BANKTOTALAMT     := r.AC_DB_AMT;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'SP'
      THEN
         FOR r
            IN (SELECT CMUW_MORTGAGEE.GEN_LEDG, CMUW_MORTGAGEE.DESCP,
                       GL.AC_DB_AMT
                  FROM CMUW_MORTGAGEE,
                       (  SELECT ACPY_RE_GL.AC_GLCODE, SUM (ACPY_RE_GL.AC_CR_AMT - ACPY_RE_GL.AC_DB_AMT) AS AC_DB_AMT
                            FROM ACPY_RE_GL
                           WHERE ACPY_RE_GL.AC_NO IN
                                    (SELECT ACPY_RE_PYMT.AC_NO
                                       FROM ACPY_RE_PYMT
                                            LEFT OUTER JOIN
                                            DMAG_VI_AGENT
                                               ON (DMAG_VI_AGENT.AGENTCODE =
                                                      ACPY_RE_PYMT.AGENT_CODE)
                                      WHERE     ((p_lStrBranchCodeFrom is null AND p_lStrBranchCodeTo is null)  OR (DMAG_VI_AGENT.BRANCH_CODE BETWEEN p_lStrBranchCodeFrom
                                                                              AND p_lStrBranchCodeTo))
                                            AND ACPY_RE_PYMT.BATCH_NO =
                                                   p_BATCHNO
                                            AND ACPY_RE_PYMT.PROC_YR =
                                                   p_lIntProcYear
                                            AND ACPY_RE_PYMT.PROC_MTH =
                                                   p_lIntProcMth)
                        GROUP BY ACPY_RE_GL.AC_GLCODE) GL
                 WHERE CMUW_MORTGAGEE.GEN_LEDG = GL.AC_GLCODE)
         LOOP
            r_row.BATCHNO          := NULL;
            r_row.BATCHTOTAL       := NULL;
            r_row.BATCHCOMPTOTAL   := NULL;
            r_row.BATCHAMT         := NULL;
            r_row.BATCHCOMPAMT     := NULL;
            r_row.GENLEDGE         := NULL;
            r_row.BANKTOTALAMT     := NULL;

            r_row.BATCHNO          := p_BATCHNO;
            r_row.BATCHTOTAL       := v_totalDoc;
            r_row.BATCHCOMPTOTAL   := v_compTotalDoc;
            r_row.BATCHAMT         := v_totalAmt;
            r_row.BATCHCOMPAMT     := v_compTotalAmt;
            r_row.GENLEDGE         := r.GEN_LEDG || ' ' || r.DESCP;
            r_row.BANKTOTALAMT     := r.AC_DB_AMT;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_lStrTranType = 'SJ'
      THEN
         FOR r
            IN (SELECT CMUW_MORTGAGEE.GEN_LEDG, CMUW_MORTGAGEE.DESCP,
                       GL.AC_DB_AMT
                  FROM CMUW_MORTGAGEE,
                       (  SELECT ACJN_RE_GL.AC_GLCODE, SUM (ACJN_RE_GL.AC_CR_AMT - ACJN_RE_GL.AC_DB_AMT) AS AC_DB_AMT
                            FROM ACJN_RE_GL
                           WHERE ACJN_RE_GL.AC_NO IN
                                    (SELECT ACJN_RE_JOUR.AC_NO
                                       FROM ACJN_RE_JOUR
                                      WHERE     ACJN_RE_JOUR.BATCH_NO =
                                                   p_BATCHNO
                                            AND ACJN_RE_JOUR.PROC_YR =
                                                   p_lIntProcYear
                                            AND ACJN_RE_JOUR.PROC_MTH =
                                                   p_lIntProcMth)
                        GROUP BY ACJN_RE_GL.AC_GLCODE) GL
                 WHERE CMUW_MORTGAGEE.GEN_LEDG = GL.AC_GLCODE)
         LOOP
            r_row.BATCHNO          := NULL;
            r_row.BATCHTOTAL       := NULL;
            r_row.BATCHCOMPTOTAL   := NULL;
            r_row.BATCHAMT         := NULL;
            r_row.BATCHCOMPAMT     := NULL;
            r_row.GENLEDGE         := NULL;
            r_row.BANKTOTALAMT     := NULL;

            r_row.BATCHNO          := p_BATCHNO;
            r_row.BATCHTOTAL       := v_totalDoc;
            r_row.BATCHCOMPTOTAL   := v_compTotalDoc;
            r_row.BATCHAMT         := v_totalAmt;
            r_row.BATCHCOMPAMT     := v_compTotalAmt;
            r_row.GENLEDGE         := r.GEN_LEDG || ' ' || r.DESCP;
            r_row.BANKTOTALAMT     := r.AC_DB_AMT;

            PIPE ROW (r_row);
         END LOOP;
      END IF;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   END FN_RPAC_ACBATCHLIST_BANK;

   --RPAC_OUTSOURCE_SUMMLIST Outsource Payment Listing - SUMM
   FUNCTION FN_RPAC_OUTSOURCE_SUMMLST_SUMM (p_lStrBatchNoFrom    VARCHAR2,
                                            p_lStrBatchNoTo      VARCHAR2,
                                            p_lStrPlatform       VARCHAR2,
                                            p_lStrPlatformAll    VARCHAR2,
                                            p_lStrListType       VARCHAR2,
                                            p_lStrPTypeCM        VARCHAR2,
                                            p_lStrPTypePP        VARCHAR2,
                                            p_lStrPTypeOT        VARCHAR2,
                                            p_lStrSplit          VARCHAR2,
                                            p_lStrSplitAmt       VARCHAR2,
                                            p_lStrProcMth        VARCHAR2,
                                            p_lStrProcYr         VARCHAR2,
                                            p_lStrBranchCode     VARCHAR2
                                           )
      RETURN RPAC_OUTSOURCE_SUMMLIST_SUMM_T
      PIPELINED
   IS
      v_ProcName_v   VARCHAR2 (30) := 'FN_RPAC_OUTSOURCE_SUMMLST_SUMM';
      v_Step_v       VARCHAR2 (5) := '000';
      r_row          RPAC_OUTSOURCE_SUMMLIST_SUMM_R;
      v_lStrPTypeCM VARCHAR(5);
      v_lStrPTypePP VARCHAR(5);
      v_lStrPTypeOT VARCHAR(5);
      v_lStrSplitAmt NUMBER;
      v_account_code VARCHAR2(500);
      v_nric VARCHAR2(200);
      v_profile_code VARCHAR2(100);
      v_buss_reg VARCHAR2(20);
      v_new_nric VARCHAR2(20);
      v_old_nric VARCHAR2(20);
      v_pol_army VARCHAR2(20);
      v_buss_reg_ind VARCHAR2(10);
      v_new_nric_ind VARCHAR2(10);
      v_old_nric_ind VARCHAR2(10);
      v_pol_army_ind VARCHAR2(10);
      v_creditor VARCHAR2(10);
      v_agent VARCHAR2(10);
      v_ri VARCHAR2(10);
      v_ext_typ VARCHAR2(50);
      V_BANK_ACC_NO VARCHAR2(100); --26/08/2018
      v_beneName VARCHAR2(200);
   BEGIN
   v_lStrPTypeCM := p_lStrPTypeCM;
   v_lStrPTypePP := p_lStrPTypePP;
   v_lStrPTypeOT := p_lStrPTypeOT;
   if(p_lStrPTypeCM is null) then
    v_lStrPTypeCM := 'N';
   end if;
   
   if(p_lStrPTypePP is null) then
    v_lStrPTypePP := 'N';
   end if;
   
   if(p_lStrPTypeOT is null) then
    v_lStrPTypeOT := 'N';
   end if;
   
   if(p_lStrSplit = 'N' or p_lStrSplit is null) then
    v_lStrSplitAmt := null;
   end if;

   SELECT CODE INTO v_creditor FROM SAPM_NEWUTIL WHERE UKEY = 'AC_ACSRC_CREDITOR';
   SELECT CODE INTO v_agent FROM SAPM_NEWUTIL WHERE UKEY = 'AC_ACSRC_AGENT';
   SELECT CODE INTO v_ri FROM SAPM_NEWUTIL WHERE UKEY = 'AC_ACSRC_RI';     
   
   FOR r IN (SELECT ACPY_PAYLINK.BANK,
            (SELECT DESCP FROM CMUW_MORTGAGEE WHERE CODE = ACPY_PAYLINK.BANK) AS BANK_DESC, ACPY_PAYLINK.PAY_TYPE,
            (SELECT DESCP FROM SAPM_SC_DET WHERE SCTYPE = 'AC_PAYTY' AND SCCODE = ACPY_PAYLINK.PAY_TYPE) AS AC_PAY_TYPE,
            ACPY_PAYLINK.TRAN_DATE,ACPY_PAYLINK.BATCH_NO,ACPY_PAYLINK.PLATFORM, (SELECT DESCP  FROM CMCL_PLATFORM WHERE CODE = ACPY_PAYLINK.PLATFORM) AS PLATFORM_DESC,ACPY_PAYLINK.AC_NO,
            ACPY_PAYLINK.AGENT_ID,--COALESCE(CREDITOR,'') AS CREDITOR,COALESCE(CLIENT,'') AS CLIENT,
            ACPY_PAYLINK.NAME,ACPY_PAYLINK.AMOUNT,ACPY_PAYLINK.STMT_DESCP,ACPY_PAYLINK.FCURR,ACPY_PAYLINK.FAMT,ACPY_PAYLINK.XRATE,ACPY_PAYLINK.CHQ_NO,
            ACPY_PAYLINK.DELIVERY_MTD,ACPY_PAYLINK.DELIVERY_TO,ACPY_PAYLINK.ZONE,ACPY_PAYLINK.AGENT_CODE,ACPY_PAYLINK.AC_SRC,ACPY_PAYLINK.PROFILE_CODE,ACPY_PAYLINK.PROFILE_KEY, 
            CPGE_PARTNERS_BENE.BANK_ACC_NO, CPGE_PARTNERS_BENE.BENE_NAME, CPGE_PARTNERS_BENE.IBG_VERIFY_TYPE,
            --(SELECT ACPY_PAYLINK_GST.TAX_INV_NO FROM ACPY_PAYLINK_GST WHERE ACPY_PAYLINK_GST.AC_NO = ACPY_PAYLINK.AC_NO) AS INV_NO,
            '' INV_NO,ACPY_PAYLINK.CREDITOR,--26/08/2018
            CPGE_PARTNERS_BENE.TRANSFER_TYPE,CPGE_PARTNERS_BENE.BANK_CODE,ACPY_PAYLINK.BENE_ID, ACPY_PAYLINK.BENE_VERSION, ACPY_PAYLINK.PART_ID, ACPY_PAYLINK.PART_VERSION,ACPY_PAYLINK.EXT_PART_ID, ACPY_PAYLINK.EXT_PART_VERSION
            FROM ACPY_PAYLINK LEFT JOIN CPGE_PARTNERS_BENE ON (ACPY_PAYLINK.BENE_ID = CPGE_PARTNERS_BENE.BENE_ID AND ACPY_PAYLINK.BENE_VERSION=CPGE_PARTNERS_BENE.BENE_VERSION)
            WHERE ((p_lStrBatchNoFrom IS NULL AND p_lStrBatchNoTo IS NULL) OR
                    ACPY_PAYLINK.BATCH_NO BETWEEN p_lStrBatchNoFrom AND p_lStrBatchNoTo)
                    AND (P_lStrBranchCode IS NULL OR ACPY_PAYLINK.ISSUE_OFFICE = P_lStrBranchCode) 
                    AND ACPY_PAYLINK.PROC_YR = TO_NUMBER(p_lStrProcYr)
                    AND ACPY_PAYLINK.PROC_MTH = TO_NUMBER(p_lStrProcMth)
                    
                AND 1 =
                        (CASE
                          WHEN p_lStrPlatformAll = 'Y' AND p_lStrPlatform IS NULL AND ACPY_PAYLINK.PLATFORM IN ('IB','OS','TT')                                           
                            THEN 1
                          WHEN
                            p_lStrPlatform IS NOT NULL AND ACPY_PAYLINK.PLATFORM = p_lStrPlatform
                            THEN 1
                          ELSE
                            0
                          END)
                AND 1=
                      (CASE
                        WHEN v_lStrPTypeCM='Y' AND v_lStrPTypePP='Y' AND v_lStrPTypeOT='Y' AND ACPY_PAYLINK.PAY_TYPE IN ('CM','OT','RE')
                          THEN 1
                        WHEN v_lStrPTypeCM='Y' AND v_lStrPTypeOT='Y' AND v_lStrPTypePP='N' AND ACPY_PAYLINK.PAY_TYPE IN ('CM','OT')
                          THEN 1
                        WHEN v_lStrPTypeCM='Y' AND v_lStrPTypeOT='N' AND v_lStrPTypePP='Y' AND ACPY_PAYLINK.PAY_TYPE IN ('CM','RE')
                          THEN 1
                        WHEN v_lStrPTypeCM='N' AND v_lStrPTypeOT='Y' AND v_lStrPTypePP='Y' AND ACPY_PAYLINK.PAY_TYPE IN ('OT','RE')
                          THEN 1
                        WHEN v_lStrPTypeCM='Y' AND v_lStrPTypeOT='N' AND v_lStrPTypePP='N' AND ACPY_PAYLINK.PAY_TYPE = 'CM'
                          THEN 1                        
                        WHEN v_lStrPTypeCM='N' AND v_lStrPTypeOT='Y' AND v_lStrPTypePP='N' AND ACPY_PAYLINK.PAY_TYPE = 'OT'
                          THEN 1
                        WHEN v_lStrPTypeCM='N' AND v_lStrPTypeOT='N' AND v_lStrPTypePP='Y' AND ACPY_PAYLINK.PAY_TYPE = 'RE'
                          THEN 1
                        WHEN v_lStrPTypeCM='N' AND v_lStrPTypeOT='N' AND v_lStrPTypePP='N'
                          THEN 1
                        ELSE
                          0
                        END)
                AND ACPY_PAYLINK.DEL_DATE IS NULL
                AND ((p_lStrSplitAmt is null AND p_lStrSplit='N') OR ( p_lStrSplit ='L' AND ACPY_PAYLINK.AMOUNT <= TO_NUMBER(p_lStrSplitAmt))
                      OR ( p_lStrSplit ='M' AND ACPY_PAYLINK.AMOUNT > TO_NUMBER(p_lStrSplitAmt)))           
            ORDER BY BANK,PAY_TYPE, ACPY_PAYLINK.BATCH_NO , ACPY_PAYLINK.AC_NO )
      LOOP
        r_row.BANK              := null;
        r_row.BANKDESCP         := null;
        r_row.PYMTDATE          := null;
        r_row.BATCHNO           := null;
        r_row.BENENAME          :=  null;
        r_row.PAYTYPEPLATFORM   := null;
        r_row.PVNO              := null;
        r_row.ACCOUNTCODE       := null;
        r_row.CLIENTCODE        := null;
        r_row.PAYEENAME         := null;
        r_row.GLCODE            := null;
        r_row.GLDESCP           := null;
        r_row.AMOUNT            := null;
        r_row.PYMTDESCP         := null;
        r_row.INVNO             := null;
        r_row.FORNCURR          := null;
        r_row.FORNAMT           := null;
        r_row.EXCRATE           := null;
        r_row.BANKMODE          := null;
        r_row.IDNO              := null;
        r_row.PAY_TYPE          := null;
        r_row.AC_SRC            := NULL;
        v_nric                  := NULL; ---APP177846
        ---r_row.BANK_ACC_NO       := NULL; --APP178519
        V_BANK_ACC_NO := NULL;

        
        r_row.BANK              := r.BANK;
        r_row.BANKDESCP         := r.BANK_DESC;
        r_row.PYMTDATE          := r.TRAN_DATE;
        r_row.BATCHNO           := r.BATCH_NO;
        --r_row.BENENAME           := r.BENE_NAME;
        v_beneName              := r.BENE_NAME;
        IF(r.PLATFORM='IB') then
          r_row.PAYTYPEPLATFORM   := r.AC_PAY_TYPE||'_'||'IBG';
        ELSE
          r_row.PAYTYPEPLATFORM   := r.AC_PAY_TYPE||'_'||r.PLATFORM;
        END IF;
        r_row.PVNO              := r.AC_NO;        
        r_row.PAYEENAME         := r.NAME;
        r_row.PAY_TYPE          := r.PAY_TYPE;
        r_row.AC_SRC            := r.AC_SRC;
        r_row.FORNAMT           := r.FAMT;
        r_row.FORNCURR          := r.FCURR;
        r_row.EXCRATE           := r.XRATE;
        r_row.INVNO             := r.INV_NO;
        v_account_code :='';
               
        if(r.AGENT_ID is not null) then
          v_account_code := r.AGENT_ID||'/';
        end if;
        
        if(r.PROFILE_KEY is not null) then
          v_account_code := v_account_code||r.PROFILE_KEY||'/';
        end if;      
        
        IF(r.CREDITOR IS NOT NULL) THEN --26/08/2018
            v_account_code := v_account_code||r.CREDITOR||'/';
         END IF; --26/08/2018
         
        v_account_code := v_account_code||r.NAME;
        --v_account_code := v_account_code||r.NAME;
        
        r_row.CLIENTCODE        := v_account_code;      
        
        if('OS'=r.PLATFORM) then
          r_row.BANKMODE           := r.PLATFORM||' '||r.DELIVERY_MTD||' '||r.DELIVERY_TO;--26/08/2018
        else
          
        --26/08/2018  
        BEGIN
          SELECT substr(B.BANK_CODE,1,4) INTO r_row.BANKMODE FROM CPGE_PARTNERS_BENE_HIST B WHERE r.BENE_ID = B.BENE_ID AND r.BENE_VERSION = B.BENE_VERSION;
          r_row.BANKMODE := r.PLATFORM||' '||r_row.BANKMODE;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
        
        BEGIN--26/08/2018
        SELECT  B.BANK_ACC_NO  
          INTO V_BANK_ACC_NO
         FROM CPGE_PARTNERS_BENE_HIST B WHERE r.BENE_ID = B.BENE_ID AND r.BENE_VERSION = B.BENE_VERSION;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
         r_row.BANKMODE :=  r_row.BANKMODE ||' '||V_BANK_ACC_NO;
        --26/08/2018
        
       /* BEGIN
          if (r.AC_SRC=v_agent) Then
            SELECT ID_VALUE1 INTO v_nric FROM DMAG_VI_AGENT WHERE AGENTCODE = r.AGENT_CODE;
          elsif(r.AC_SRC=v_creditor) then
            SELECT CP.ID_VALUE1 INTO v_nric FROM CPGE_VI_PARTNER_DETAILS CP WHERE CP.PART_ID = r.PART_ID AND CP.PART_VERSION = r. PART_VERSION;
          else
           SELECT CPGE_VI_PARTNER_EXTERNAL.ID_VALUE1 INTO v_nric FROM CPGE_VI_PARTNER_EXTERNAL 
           WHERE CPGE_VI_PARTNER_EXTERNAL.EXT_PART_ID = r.EXT_PART_ID AND CPGE_VI_PARTNER_EXTERNAL.EXT_PART_VERSION = r.EXT_PART_VERSION;
          end if;
          EXCEPTION WHEN OTHERS THEN NULL;
         END;
         */
        end if;
        
       --IF v_nric IS NULL OR R.PROFILE_KEY IS NOT NULL THEN ---APP177846
          BEGIN
          SELECT  B.IBG_VERIFY_ID
            INTO v_nric
           FROM CPGE_PARTNERS_BENE_HIST B WHERE r.BENE_ID = B.BENE_ID AND r.BENE_VERSION = B.BENE_VERSION AND B.IBG_VERIFY_IND ='Y';
           EXCEPTION WHEN OTHERS THEN NULL;
          END;         
        IF v_beneName IS NULL THEN
           BEGIN
          SELECT  B.BENE_NAME
            INTO v_beneName
           FROM CPGE_PARTNERS_BENE_HIST B WHERE r.BENE_ID = B.BENE_ID AND r.BENE_VERSION = B.BENE_VERSION ;
           EXCEPTION WHEN OTHERS THEN NULL;
          END;        
        END IF;
        r_row.BENENAME           := v_beneName;        
     -- end if; --26/08/2018 
        r_row.IDNO := v_nric;
        BEGIN
          SELECT ACPY_PAYLINK_GL.AC_GLCODE
          INTO r_row.GLCODE
          FROM (ACPY_PAYLINK LEFT OUTER JOIN ACPY_PAYLINK_GL ON ACPY_PAYLINK.AC_NO = ACPY_PAYLINK_GL.AC_NO) 
          LEFT OUTER JOIN CMUW_MORTGAGEE ON ACPY_PAYLINK.BANK = CMUW_MORTGAGEE.CODE 
          WHERE ACPY_PAYLINK.AC_NO = r.AC_NO
          AND ACPY_PAYLINK_GL.GL_SEQ_NO=1 ---Redmine 99609
          AND ACPY_PAYLINK_GL.AC_GLCODE <> CMUW_MORTGAGEE.GEN_LEDG;
        EXCEPTION WHEN OTHERS
               THEN
                  NULL;
          END;
          
            BEGIN
           SELECT (SELECT ACGL_LEDGER.DESCP
                     FROM ACGL_LEDGER
                    WHERE ACGL_LEDGER.UKEY = ACPY_PAYLINK_GL.AC_GLCODE)
             INTO r_row.GLDESCP
             FROM (ACPY_PAYLINK
                   LEFT OUTER JOIN ACPY_PAYLINK_GL
                      ON ACPY_PAYLINK.AC_NO = ACPY_PAYLINK_GL.AC_NO)
                  LEFT OUTER JOIN CMUW_MORTGAGEE
                     ON ACPY_PAYLINK.BANK = CMUW_MORTGAGEE.CODE
            WHERE     ACPY_PAYLINK.AC_NO = r.AC_NO
                  AND ACPY_PAYLINK_GL.AC_GLCODE <> CMUW_MORTGAGEE.GEN_LEDG
                  AND ACPY_PAYLINK_GL.GL_SEQ_NO = 1;
        EXCEPTION
           WHEN OTHERS
           THEN
              NULL;
        END;
        --r_row.GLDESCP           := r.AC_PAY_TYPE;  ---Redmine 99609 start end
        r_row.AMOUNT            := r.AMOUNT;
        r_row.PYMTDESCP         := r.STMT_DESCP;
        PIPE ROW (r_row);
      END LOOP;
      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   END FN_RPAC_OUTSOURCE_SUMMLST_SUMM;

   --RPAC_OUTSOURCE_SUMMLIST Outsource Payment Listing - SUMM
   FUNCTION FN_RPAC_OUTSOURCE_SUMMLST_LST (p_lStrBatchNoFrom    VARCHAR2,
                                               p_lStrBatchNoTo      VARCHAR2,
                                               p_lStrPlatform       VARCHAR2,
                                               p_lStrPlatformAll    VARCHAR2,
                                               p_lStrListType       VARCHAR2,
                                               p_lStrPTypeCM        VARCHAR2,
                                               p_lStrPTypePP        VARCHAR2,
                                               p_lStrPTypeOT        VARCHAR2,
                                               p_lStrSplit          VARCHAR2,
                                               p_lStrSplitAmt       VARCHAR2,
                                               p_lStrProcMth        VARCHAR2,
                                               p_lStrProcYr         VARCHAR2,
                                               p_lStrBranchCode     VARCHAR2
                                              )
          RETURN RPAC_OUTSOURCE_SUMMLIST_RDET_T
          PIPELINED
       IS
          v_ProcName_v   VARCHAR2 (30) := 'FN_RPAC_OUTSOURCE_SUMMLST_LST';
          v_Step_v       VARCHAR2 (5) := '000';
          r_row          RPAC_OUTSOURCE_SUMMLIST_RDET;
          v_account_code VARCHAR2(500);
          v_beneName VARCHAR2(200);
       BEGIN

        PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'p_lStrBatchNoFrom: ' || p_lStrBatchNoFrom ||': '|| SQLERRM);
        PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'p_lStrBatchNoTo: ' || p_lStrBatchNoTo ||': '|| SQLERRM);
        PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'p_lStrPlatform: ' || p_lStrPlatform ||': '|| SQLERRM);
        PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'p_lStrPlatformAll: ' || p_lStrPlatformAll ||': '|| SQLERRM);
        PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'p_lStrListType: ' || p_lStrListType ||': '|| SQLERRM);
        PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'p_lStrPTypeCM: ' || p_lStrPTypeCM ||': '|| SQLERRM);
        PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'p_lStrPTypePP: ' || p_lStrPTypePP ||': '|| SQLERRM);
        PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'p_lStrPTypeOT: ' || p_lStrPTypeOT ||': '|| SQLERRM);
        PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'p_lStrSplit: ' || p_lStrSplit ||': '|| SQLERRM);
        PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'p_lStrSplitAmt: ' || p_lStrSplitAmt ||': '|| SQLERRM);
        PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'p_lStrProcMth: ' || p_lStrProcMth ||': '|| SQLERRM);
        PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'p_lStrProcYr: ' || p_lStrProcYr ||': '|| SQLERRM);
        PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'p_lStrBranchCode: ' || p_lStrBranchCode ||': '|| SQLERRM);
        if(p_lStrPTypeOT = 'Y') then
          FOR r IN (SELECT CMUW_MORTGAGEE.GEN_LEDG, CMUW_MORTGAGEE.DESCP, 
                    ACPY_PAYLINK.AC_NO, ACPY_PAYLINK.BATCH_NO, ACPY_PAYLINK.DEL_DATE, ACPY_PAYLINK.TRAN_DATE, ACPY_PAYLINK.TRANSFER_IND, 
                    (SELECT DESCP FROM SAPM_SC_DET WHERE SCTYPE = 'AC_PAYTY' AND SCCODE = ACPY_PAYLINK.PAY_TYPE) AS PAY_TYPE_DESCP,
                    ACPY_PAYLINK.PAY_TYPE, ACPY_PAYLINK.AMOUNT, ACPY_PAYLINK.BANK,  --ACPY_PAYLINK.BANK_ROUTE_GIRO,
                    (select a.tran_type from acgc_batch a where a.batch_no = ACPY_PAYLINK.BATCH_NO AND rownum=1) as TRAN_TYPE,
                    ACPY_PAYLINK.PLATFORM, ACPY_PAYLINK.ISSUE_OFFICE,
                    ACPY_PAYLINK.DELIVERY_MTD,    
                    (select descp from sapm_sc_det where sccode = ACPY_PAYLINK.DELIVERY_MTD and sctype = 'AC_DELIVERY_MTD_CTB_HQ' AND rownum=1) AS DM_OS_03_HQ_DESC,
                    (select descp from sapm_sc_det where sccode = ACPY_PAYLINK.DELIVERY_MTD and sctype = 'AC_DELIVERY_MTD_CTB' AND rownum=1) AS DM_OS_03_NONHQ_DESC,
                    (SELECT descp FROM sapm_sc_det WHERE sccode = ACPY_PAYLINK.DELIVERY_MTD AND sctype = 'AC_DELIVERY_MTD_SCB' AND ROWNUM=1) AS DM_OS_09_DESC,
                    ACPY_PAYLINK.DELIVERY_TO,ACPY_PAYLINK.CREDITOR,--Redmine 91757
                    (select descp from cmgc_branch where code = ACPY_PAYLINK.DELIVERY_TO AND rownum=1) AS DT_03_OS_C_DESC,
                    (select descp from sapm_sc_det where sccode = ACPY_PAYLINK.DELIVERY_TO and sctype = 'AC_DELIVERY_TO' AND rownum=1) AS DT_03_OS_M_OR_RET_DESC,
                    (select descp from sapm_sc_det where sccode = ACPY_PAYLINK.DELIVERY_TO and sctype = 'CL_DELIVERY_TO' AND rownum=1) AS DT_09_DESC,
                    CPGE_PARTNERS_BENE.BANK_ACC_NO,
                    (SELECT B.BENE_NAME  FROM CPGE_PARTNERS_BENE B WHERE ACPY_PAYLINK.BENE_ID = B.BENE_ID AND ACPY_PAYLINK.BENE_VERSION=B.BENE_VERSION AND rownum=1) AS BENE_NAME, 
                    ACPY_PAYLINK.PROFILE_KEY,ACPY_PAYLINK.NAME,ACPY_PAYLINK.AGENT_ID,ACPY_PAYLINK.BENE_ID, ACPY_PAYLINK.BENE_VERSION, ACPY_PAYLINK.FCURR,ACPY_PAYLINK.FAMT,ACPY_PAYLINK.XRATE
                    FROM ACPY_PAYLINK LEFT OUTER JOIN CMUW_MORTGAGEE ON ACPY_PAYLINK.BANK = CMUW_MORTGAGEE.CODE
                        LEFT JOIN CPGE_PARTNERS_BENE ON (ACPY_PAYLINK.BENE_ID = CPGE_PARTNERS_BENE.BENE_ID AND ACPY_PAYLINK.BENE_VERSION=CPGE_PARTNERS_BENE.BENE_VERSION)
                    WHERE ((p_lStrBatchNoFrom IS NULL AND p_lStrBatchNoTo IS NULL) OR ACPY_PAYLINK.BATCH_NO BETWEEN p_lStrBatchNoFrom AND p_lStrBatchNoTo)
                    AND ACPY_PAYLINK.PAY_TYPE = 'OT'
                    AND ACPY_PAYLINK.DEL_DATE IS NULL
                    AND (P_lStrBranchCode IS NULL OR ACPY_PAYLINK.ISSUE_OFFICE = P_lStrBranchCode) 
                    AND ACPY_PAYLINK.PROC_YR = TO_NUMBER(p_lStrProcYr)
                    AND ACPY_PAYLINK.PROC_MTH = TO_NUMBER(p_lStrProcMth)
                    AND 1=
                      (CASE
                        WHEN (p_lStrPlatformAll = 'Y' AND p_lStrPlatform IS NULL AND ACPY_PAYLINK.PLATFORM IN ('IB','OS','TT')) then 1
                        WHEN (p_lStrPlatform IS NOT NULL AND ACPY_PAYLINK.PLATFORM = p_lStrPlatform) THEN 1
                        ELSE 0
                        END
                        )
                    AND ((p_lStrSplitAmt is null AND p_lStrSplit='N') OR ( p_lStrSplit ='L' AND ACPY_PAYLINK.AMOUNT <= TO_NUMBER(p_lStrSplitAmt))
                      OR ( p_lStrSplit ='M' AND ACPY_PAYLINK.AMOUNT > TO_NUMBER(p_lStrSplitAmt)))
                    ORDER BY ACPY_PAYLINK.BATCH_NO, ACPY_PAYLINK.AC_NO
                    )
           LOOP
            r_row.BANK          := null;
            r_row.BANKDESCP     := null;
            r_row.GENLEDG       := null;
            r_row.PYMTTYPE      := null;
            r_row.PYMTDATE      := null;
            r_row.BATCHNO       := null;
            r_row.PVNO          := null;
            r_row.ACCOUNTCODE   := null;
            r_row.CLIENTCODE    := null;
            r_row.PAYEENAME     := null;
            r_row.PYMTDESCP     := null;
            r_row.FORNCURR      := null;
            r_row.FORNAMT       := null;
            r_row.EXCRATE       := null;
            v_beneName          := null;
            
            r_row.BANK          := r.BANK;
            r_row.BANKDESCP     := r.DESCP;
            r_row.GENLEDG       := r.GEN_LEDG;
            r_row.PYMTTYPE      := r.PAY_TYPE_DESCP;
            r_row.PYMTDATE      := r.TRAN_DATE;
            r_row.BATCHNO       := r.BATCH_NO;
            r_row.PVNO          := r.AC_NO;
            r_row.PYMTDESCP     := null;
            r_row.FORNAMT       := r.FAMT;
            r_row.FORNCURR      := r.FCURR;
            r_row.EXCRATE       := r.XRATE;
            r_row.PAYEENAME     := r.NAME;
            v_beneName          := r.BENE_NAME;
            v_account_code :='';
            if(r.AGENT_ID is not null) then
              v_account_code := r.AGENT_ID||'/';
            end if;
            if(r.PROFILE_KEY is not null) then
              v_account_code := v_account_code||r.PROFILE_KEY||'/';
            end if;      
            IF(r.CREDITOR IS NOT NULL) THEN --Redmine 91757
              v_account_code := v_account_code||r.CREDITOR||'/';
            END IF; 
            IF v_beneName IS NULL THEN
                    BEGIN
                      SELECT  B.BENE_NAME
                        INTO v_beneName
                       FROM CPGE_PARTNERS_BENE_HIST B WHERE r.BENE_ID = B.BENE_ID AND r.BENE_VERSION = B.BENE_VERSION ;
                       EXCEPTION WHEN OTHERS THEN 
                       PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'step 1.0 :  BENE_ID, BENE_VERSION : ' || r.BENE_ID||'/ '||  r.BENE_VERSION ||': '|| SQLERRM); -- 21082024 added log
                       NULL;
                    END;        
            END IF;
            r_row.BENENAME           := v_beneName;            

            v_account_code := v_account_code||r.NAME;
            
            r_row.CLIENTCODE        := v_account_code; 
            PIPE ROW (r_row);
          END LOOP;
        end if;
        IF(p_lStrPTypePP = 'Y') THEN
          FOR r IN (SELECT   DISTINCT  CMUW_MORTGAGEE.DESCP, CMUW_MORTGAGEE.GEN_LEDG, ACPY_PAYLINK.BATCH_NO,ACPY_PAYLINK.AC_NO,  ACPY_PAYLINK.CREDITOR,--Redmine 91757
                    (SELECT TRAN_DATE FROM ACPY_PAYLINK  A WHERE A.BATCH_NO=ACPY_PAYLINK.BATCH_NO AND rownum=1)  As TR_DATE, 
                    (SELECT B.BENE_NAME  FROM CPGE_PARTNERS_BENE B WHERE ACPY_PAYLINK.BENE_ID = B.BENE_ID AND ACPY_PAYLINK.BENE_VERSION=B.BENE_VERSION AND rownum=1) AS BENE_NAME,
                    ACPY_PAYLINK.BANK,ACPY_PAYLINK.PROFILE_KEY,ACPY_PAYLINK.NAME, ACPY_PAYLINK.BENE_ID, ACPY_PAYLINK.BENE_VERSION, ACPY_PAYLINK.AGENT_ID,ACPY_PAYLINK.FCURR,ACPY_PAYLINK.FAMT,ACPY_PAYLINK.XRATE
                    FROM ( ACPY_PAYLINK LEFT OUTER JOIN ACPY_PAYLINK_GL ON ACPY_PAYLINK.AC_NO = ACPY_PAYLINK_GL.AC_NO )  LEFT OUTER JOIN CMUW_MORTGAGEE ON ACPY_PAYLINK.BANK = CMUW_MORTGAGEE.CODE
                    WHERE ((p_lStrBatchNoFrom IS NULL AND p_lStrBatchNoTo IS NULL) OR ACPY_PAYLINK.BATCH_NO BETWEEN p_lStrBatchNoFrom AND p_lStrBatchNoTo)
                    AND ACPY_PAYLINK.PAY_TYPE = 'RE' AND ACPY_PAYLINK.DEL_DATE IS NULL AND ACPY_PAYLINK_GL.AC_GLCODE <> CMUW_MORTGAGEE.GEN_LEDG
                    AND (P_lStrBranchCode IS NULL OR ACPY_PAYLINK.ISSUE_OFFICE = P_lStrBranchCode) 
                    AND ACPY_PAYLINK.PROC_YR = TO_NUMBER(p_lStrProcYr)
                    AND ACPY_PAYLINK.PROC_MTH = TO_NUMBER(p_lStrProcMth)
                    AND 1 =
                      (CASE
                        WHEN (p_lStrPlatformAll='Y' AND p_lStrPlatform IS NULL AND ACPY_PAYLINK.PLATFORM IN ('IB','OS','TT')) THEN 1
                        WHEN (p_lStrPlatform IS NOT NULL AND ACPY_PAYLINK.PLATFORM= p_lStrPlatform) THEN 1
                        ELSE 0
                        END                 
                      )
                    AND ((p_lStrSplitAmt is null AND p_lStrSplit='N') OR ( p_lStrSplit ='L' AND ACPY_PAYLINK.AMOUNT <= TO_NUMBER(p_lStrSplitAmt))
                      OR ( p_lStrSplit ='M' AND ACPY_PAYLINK.AMOUNT > TO_NUMBER(p_lStrSplitAmt)))
                    ORDER BY ACPY_PAYLINK.BATCH_NO
                    )
          LOOP
            r_row.BANK          := null;
            r_row.BANKDESCP     := null;
            r_row.GENLEDG       := null;
            r_row.PYMTTYPE      := null;
            r_row.PYMTDATE      := null;
            r_row.BATCHNO       := null;
            r_row.PVNO          := null;
            r_row.ACCOUNTCODE   := null;
            r_row.CLIENTCODE    := null;
            r_row.PAYEENAME     := null;
            r_row.PYMTDESCP     := null;
            r_row.FORNCURR      := null;
            r_row.FORNAMT       := null;
            r_row.EXCRATE       := null;
            v_beneName          := null;
            
            r_row.BANK          := r.BANK;
            r_row.BANKDESCP     := r.DESCP;
            r_row.GENLEDG       := r.GEN_LEDG;
            r_row.PYMTTYPE      := 'PP';
            r_row.PYMTDATE      := r.TR_DATE;
            r_row.BATCHNO       := r.BATCH_NO;
            r_row.PVNO          := r.AC_NO;
            r_row.PYMTDESCP     := null;
            r_row.FORNAMT       := r.FAMT;
            r_row.FORNCURR      := r.FCURR;
            r_row.EXCRATE       := r.XRATE;
            r_row.PAYEENAME     := r.NAME;
            v_beneName          := r.BENE_NAME;
            v_account_code :='';
            if(r.AGENT_ID is not null) then
              v_account_code := r.AGENT_ID||'/';
            end if;
            
            if(r.PROFILE_KEY is not null) then
              v_account_code := v_account_code||r.PROFILE_KEY||'/';
            END IF;
            if(r.CREDITOR is not null) then --Redmine 91757
              v_account_code := v_account_code||r.CREDITOR||'/';
            END IF;      
        IF v_beneName IS NULL THEN
                BEGIN
                  SELECT  B.BENE_NAME
                    INTO v_beneName
                   FROM CPGE_PARTNERS_BENE_HIST B WHERE r.BENE_ID = B.BENE_ID AND r.BENE_VERSION = B.BENE_VERSION ;
                   EXCEPTION WHEN OTHERS THEN 
                   PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'step 1.1 :  BENE_ID, BENE_VERSION : ' || r.BENE_ID||'/ '||  r.BENE_VERSION ||': '|| SQLERRM); -- 21082024 added log
                   NULL;
                  END;        
                END IF;
        r_row.BENENAME           := v_beneName;            

            v_account_code := v_account_code||r.NAME;
            
            r_row.CLIENTCODE        := v_account_code; 
            
            PIPE ROW (r_row);
          END LOOP;
        end if;
        IF(p_lStrPTypeCM = 'Y') THEN
            FOR r IN (SELECT CMUW_MORTGAGEE.GEN_LEDG, CMUW_MORTGAGEE.DESCP, ACPY_PAYLINK.BANK, ACPY_PAYLINK.CREDITOR,--Redmine 91757
                    ACPY_PAYLINK.TRAN_DATE,ACPY_PAYLINK.PROFILE_KEY,ACPY_PAYLINK.NAME, ACPY_PAYLINK.AGENT_ID,
                    ACPY_PAYLINK.AC_NO, ACPY_PAYLINK.BENE_ID, ACPY_PAYLINK.BENE_VERSION,  ACPY_PAYLINK.BATCH_NO, ACPY_PAYLINK.TRANSFER_IND, ACPY_PAYLINK.DEL_DATE, ACPY_PAYLINK.AMOUNT, ACPY_PAYLINK.PAY_TYPE,
                    (SELECT B.BENE_NAME  FROM CPGE_PARTNERS_BENE B WHERE ACPY_PAYLINK.BENE_ID = B.BENE_ID AND ACPY_PAYLINK.BENE_VERSION=B.BENE_VERSION AND rownum=1) AS BENE_NAME,
                    ACPY_PAYLINK.FCURR,ACPY_PAYLINK.FAMT,ACPY_PAYLINK.XRATE
                    FROM ACPY_PAYLINK LEFT OUTER JOIN CMUW_MORTGAGEE ON ACPY_PAYLINK.BANK = CMUW_MORTGAGEE.CODE
                    WHERE 0=0 AND ((p_lStrBatchNoFrom is null AND p_lStrBatchNoTo IS NULL) OR
                    (ACPY_PAYLINK.BATCH_NO BETWEEN p_lStrBatchNoFrom AND p_lStrBatchNoTo))
                    AND ACPY_PAYLINK.PAY_TYPE = 'CM'
                    AND ACPY_PAYLINK.DEL_DATE IS NULL
                    AND (P_lStrBranchCode IS NULL OR ACPY_PAYLINK.ISSUE_OFFICE = P_lStrBranchCode) 
                    AND ACPY_PAYLINK.PROC_YR = TO_NUMBER(p_lStrProcYr)
                    AND ACPY_PAYLINK.PROC_MTH = TO_NUMBER(p_lStrProcMth)
                    AND 1=
                      (CASE
                        WHEN (p_lStrPlatformAll='Y' AND p_lStrPlatform is null AND ACPY_PAYLINK.PLATFORM IN ('IB','OS','TT')) THEN 1
                        WHEN (p_lStrPlatform is not null AND ACPY_PAYLINK.PLATFORM=p_lStrPlatform) THEN 1
                        ELSE 0
                        END
                      )
                    AND ((p_lStrSplitAmt is null AND p_lStrSplit='N') OR ( p_lStrSplit ='L' AND ACPY_PAYLINK.AMOUNT <= TO_NUMBER(p_lStrSplitAmt))
                      OR ( p_lStrSplit ='M' AND ACPY_PAYLINK.AMOUNT > TO_NUMBER(p_lStrSplitAmt)))
                    ORDER BY ACPY_PAYLINK.BATCH_NO, ACPY_PAYLINK.AC_NO
                    )
          LOOP
            r_row.BANK          := null;
            r_row.BANKDESCP     := null;
            r_row.GENLEDG       := null;
            r_row.PYMTTYPE      := null;
            r_row.PYMTDATE      := null;
            r_row.BATCHNO       := null;
            r_row.PVNO          := null;
            r_row.ACCOUNTCODE   := null;
            r_row.CLIENTCODE    := null;
            r_row.PAYEENAME     := null;
            r_row.PYMTDESCP     := null;
            r_row.FORNCURR      := null;
            r_row.FORNAMT       := null;
            r_row.EXCRATE       := NULL;
            v_beneName          := NULL;
            
            r_row.BANK          := r.BANK;
            r_row.BANKDESCP     := r.DESCP;
            r_row.GENLEDG       := r.GEN_LEDG;
            r_row.PYMTTYPE      := r.PAY_TYPE;
            r_row.PYMTDATE      := r.TRAN_DATE;
            r_row.BATCHNO       := r.BATCH_NO;
            r_row.PVNO          := r.AC_NO;
            r_row.PYMTDESCP     := null;
            r_row.FORNAMT       := r.FAMT;
            r_row.FORNCURR      := r.FCURR;
            r_row.EXCRATE       := r.XRATE;
            r_row.PAYEENAME     := r.NAME;
            v_beneName          := r.BENE_NAME;
            v_account_code :='';
            if(r.AGENT_ID is not null) then
              v_account_code := r.AGENT_ID||'/';
            end if;
            if(r.PROFILE_KEY is not null) then
              v_account_code := v_account_code||r.PROFILE_KEY||'/';
            END IF; 
            if(r.CREDITOR is not null) then --Redmine 91757
              v_account_code := v_account_code||r.CREDITOR||'/';
            end if;      
            IF v_beneName IS NULL THEN
               BEGIN
                  SELECT  B.BENE_NAME
                    INTO v_beneName
                   FROM CPGE_PARTNERS_BENE_HIST B WHERE r.BENE_ID = B.BENE_ID AND r.BENE_VERSION = B.BENE_VERSION ;
                   EXCEPTION WHEN OTHERS THEN 
                    PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'step 1.2 :  BENE_ID, BENE_VERSION : ' || r.BENE_ID||'/ '||  r.BENE_VERSION ||': '|| SQLERRM); -- 21082024 added log
                   NULL;
              END;        
        END IF;
            r_row.BENENAME           := v_beneName;
            v_account_code := v_account_code||r.NAME;
 
            r_row.CLIENTCODE        := v_account_code; 
            

            PIPE ROW (r_row);
          END LOOP;
        end if;
          RETURN;
       EXCEPTION
          WHEN OTHERS
          THEN
             PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v ||'.' || v_ProcName_v, 1, SQLERRM);
    END FN_RPAC_OUTSOURCE_SUMMLST_LST;

    
    
FUNCTION FN_RPAC_OUTSOURCE_SUMM_SUMMGL (p_pvNo VARCHAR2)
      RETURN RPAC_OUTSRC_SUMMLIST_RDET_GL_T PIPELINED 
      IS
      v_ProcName_v    VARCHAR2 (30) := 'FN_RPAC_OUTSOURCE_SUMM_SUMMGL';
      r_row RPAC_OUTSRC_SUMMLIST_RDET_GL;
      v_account_code VARCHAR2(500);
      v_pv_no  VARCHAR2(400);
      v_inv_ind      VARCHAR2(10);
      v_ko_ind       VARCHAR2(10);
      v_paylink_name VARCHAR2(400);
      v_pymt_descp   VARCHAR2(400);
      v_gl_seq_no    NUMBER;
      v_gl_amt      NUMBER;
      v_ac_db_amt      NUMBER;
      v_ac_cr_amt      NUMBER;
      v_agent_id     VARCHAR(80);
      v_profile_key  VARCHAR(80);
      v_pymt_name    VARCHAR(300);
      is_print_amt   boolean;
      v_nric VARCHAR2(200);
      v_profile_code VARCHAR2(100);
      v_buss_reg VARCHAR2(20);
      v_new_nric VARCHAR2(20);
      v_old_nric VARCHAR2(20);
      v_pol_army VARCHAR2(20);
      v_buss_reg_ind VARCHAR2(10);
      v_new_nric_ind VARCHAR2(10);
      v_old_nric_ind VARCHAR2(10);
      v_pol_army_ind VARCHAR2(10);
      v_creditor VARCHAR2(10);
      v_agent VARCHAR2(10);
      v_ri VARCHAR2(10);
      v_ext_typ VARCHAR2(50);
BEGIN
    SELECT CODE INTO v_creditor FROM SAPM_NEWUTIL WHERE UKEY = 'AC_ACSRC_CREDITOR';
    SELECT CODE INTO v_agent FROM SAPM_NEWUTIL WHERE UKEY = 'AC_ACSRC_AGENT';
    SELECT CODE INTO v_ri FROM SAPM_NEWUTIL WHERE UKEY = 'AC_ACSRC_RI';
    

    
    FOR r IN (
                SELECT SUM (AC_DB_AMT) AMOUNT,
                       AC_NO,
                       NULL AS AC_AGENT_ID,
                       NAME,
                       GL_DESC,
                       AC_GLCODE,
                       SUM (AC_DB_AMT) AC_DB_AMT,
                       SUM (AC_CR_AMT)AC_CR_AMT,
                       STMT_DESCP,
                       DOC_NO,
                       DOC_AMT,
                       KO_XIND,
                       INV_XIND,
                       NULL AS GL_SEQ_NO,
                       NULL AS ITM_NO_GL,
                       NULL AS ITM_NO_KO,
                       PLATFORM,
                       DELIVERY_MTD,
                       DELIVERY_TO,
                       ZONE,
                       PROFILE_CODE,
                       PROFILE_KEY,
                       FCURR,
                       FAMT,
                       XRATE,
                       AC_SRC,
                       AGENT_CODE,
                       BENE_ID,
                       BENE_VERSION,
                       PART_ID,
                       PART_VERSION,
                       EXT_PART_ID,
                       EXT_PART_VERSION
                    FROM (  SELECT ACPY_PAYLINK.AMOUNT,
                                   ACPY_PAYLINK.AC_NO,
                                   ACPY_PAYLINK_GL.AC_AGENT_ID,
                                   ACPY_PAYLINK.NAME,
                                   (SELECT ACGL_LEDGER.DESCP
                                      FROM ACGL_LEDGER
                                     WHERE ACGL_LEDGER.UKEY = ACPY_PAYLINK_GL.AC_GLCODE)
                                      AS GL_DESC,
                                   ACPY_PAYLINK_GL.AC_GLCODE,
                                   ACPY_PAYLINK_GL.AC_DB_AMT,
                                   ACPY_PAYLINK_GL.AC_CR_AMT,
                                   ACPY_PAYLINK.STMT_DESCP,
                                   NULL AS DOC_NO,
                                   NULL AS DOC_AMT,
                                   ACPY_PAYLINK.KO_XIND,
                                   ACPY_PAYLINK.INV_XIND,
                                   ACPY_PAYLINK_GL.GL_SEQ_NO,
                                   ACPY_PAYLINK_GL.ITM_NO AS ITM_NO_GL,
                                   NULL AS ITM_NO_KO,
                                   ACPY_PAYLINK.PLATFORM,
                                   ACPY_PAYLINK.DELIVERY_MTD,
                                   ACPY_PAYLINK.DELIVERY_TO,
                                   ACPY_PAYLINK.ZONE,
                                   ACPY_PAYLINK.PROFILE_CODE,
                                   ACPY_PAYLINK.PROFILE_KEY,
                                   ACPY_PAYLINK.FCURR,
                                   ACPY_PAYLINK.FAMT,
                                   ACPY_PAYLINK.XRATE,
                                   ACPY_PAYLINK.AC_SRC,
                                   ACPY_PAYLINK.AGENT_CODE,
                                   ACPY_PAYLINK.BENE_ID,
                                   ACPY_PAYLINK.BENE_VERSION,
                                   ACPY_PAYLINK.PART_ID,
                                   ACPY_PAYLINK.PART_VERSION,
                                   ACPY_PAYLINK.EXT_PART_ID,
                                   ACPY_PAYLINK.EXT_PART_VERSION
                              FROM (ACPY_PAYLINK
                                    LEFT OUTER JOIN ACPY_PAYLINK_GL
                                       ON ACPY_PAYLINK.AC_NO = ACPY_PAYLINK_GL.AC_NO)
                                   LEFT OUTER JOIN CMUW_MORTGAGEE
                                      ON ACPY_PAYLINK.BANK = CMUW_MORTGAGEE.CODE
                             WHERE     ACPY_PAYLINK.AC_NO = p_pvNo
                                   AND ACPY_PAYLINK_GL.AC_GLCODE <> CMUW_MORTGAGEE.GEN_LEDG
                          ORDER BY ACPY_PAYLINK_GL.ITM_NO) GL
                GROUP BY AC_NO,
                         AC_GLCODE,
                         NAME,
                         GL_DESC,
                         AC_GLCODE,
                         AC_CR_AMT,
                         STMT_DESCP,
                         DOC_NO,
                         DOC_AMT,
                         KO_XIND,
                         INV_XIND,
                         PLATFORM,
                         DELIVERY_MTD,
                         DELIVERY_TO,
                         ZONE,
                         PROFILE_CODE,
                         PROFILE_KEY,
                         FCURR,
                         FAMT,
                         XRATE,
                         AC_SRC,
                         AGENT_CODE,
                         BENE_ID,
                         BENE_VERSION,
                         PART_ID,
                         PART_VERSION,
                         EXT_PART_ID,
                         EXT_PART_VERSION) 
       
    LOOP
      is_print_amt := true;
      r_row.PVNO      := null;
      r_row.GLCODE    := null;
      r_row.GLDESCP   := null;
      r_row.AMOUNT          := null;
      r_row.ACCOUNTCODE     := null;
      r_row.CLIENTCODE      := null;
      r_row.PAYEENAME       := null;
      r_row.PYMTDESCP       := null;
      r_row.FORNCURR        := null;
      r_row.FORNAMT         := null;
      r_row.EXCRATE         := null;
      r_row.BANKMODE        := NULL;
      r_row.IDNO            := NULL;
      v_nric                := NULL;--APP177846
      r_row.BANK_ACC_NO     := NULL; --APP178519

      
      r_row.PVNO      := r.AC_NO;
      r_row.GLCODE    := r.AC_GLCODE;
      r_row.GLDESCP   := r.GL_DESC;
      r_row.AMOUNT    := r.AC_DB_AMT; --r.AMOUNT; APP177846
      r_row.PAYEENAME := r.NAME;
      r_row.PYMTDESCP := r.STMT_DESCP;      
      r_row.FORNCURR  := r.FCURR;
      r_row.FORNAMT   := r.FAMT;
      r_row.EXCRATE   := r.XRATE;

      
      if (r.PLATFORM='OS') Then
        v_nric := '';
        r_row.BANKMODE          := r.PLATFORM||' '||r.DELIVERY_MTD||' '||r.DELIVERY_TO;
      else
        BEGIN
          SELECT substr(B.BANK_CODE,1,4) INTO r_row.BANKMODE FROM CPGE_PARTNERS_BENE_HIST B WHERE r.BENE_ID = B.BENE_ID AND r.BENE_VERSION = B.BENE_VERSION;
          r_row.BANKMODE := r.PLATFORM||' '||r_row.BANKMODE;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
            BEGIN
              if (r.AC_SRC=v_agent) Then
                SELECT ID_VALUE1 INTO v_nric FROM DMAG_VI_AGENT WHERE AGENTCODE = r.AGENT_CODE;
              elsif(r.AC_SRC=v_ri) then
                SELECT CP.ID_VALUE1 INTO v_nric FROM CPGE_VI_PARTNER_DETAILS CP WHERE CP.PART_ID = r.PART_ID AND CP.PART_VERSION = r. PART_VERSION;
              else
               SELECT CPGE_VI_PARTNER_EXTERNAL.ID_VALUE1 INTO v_nric FROM CPGE_VI_PARTNER_EXTERNAL 
               WHERE CPGE_VI_PARTNER_EXTERNAL.EXT_PART_ID = r.EXT_PART_ID AND CPGE_VI_PARTNER_EXTERNAL.EXT_PART_VERSION = r.EXT_PART_VERSION;
              end if;
            EXCEPTION WHEN OTHERS THEN NULL;
            END; 
         --APP178519   
            BEGIN
            SELECT  B.BANK_ACC_NO  
              INTO r_row.BANK_ACC_NO
             FROM CPGE_PARTNERS_BENE_HIST B WHERE r.BENE_ID = B.BENE_ID AND r.BENE_VERSION = B.BENE_VERSION;
             EXCEPTION WHEN OTHERS THEN NULL;
            END; 
      END IF;--APP178519
      
      
      IF v_nric IS NULL THEN-- Redmine 91757      
        BEGIN
        SELECT  B.IBG_VERIFY_ID
          INTO v_nric
         FROM CPGE_PARTNERS_BENE_HIST B WHERE r.BENE_ID = B.BENE_ID AND r.BENE_VERSION = B.BENE_VERSION;
         EXCEPTION WHEN OTHERS THEN NULL;
        END;         
      end if;
      

      r_row.IDNO  := v_nric;
       v_account_code :='';
      
      if(r.AC_AGENT_ID is not null) then
        v_account_code := r.AC_AGENT_ID||'/';
      end if;
      
      if(r.PROFILE_KEY is not null) then
        v_account_code := v_account_code||r.PROFILE_KEY||'/';
      end if;      
      v_account_code := v_account_code||r.NAME;
      
      if(v_pv_no = r.AC_NO) then
          r_row.PVNO := '';
      else
          v_pv_no := r.AC_NO;
      end if;
        
      v_inv_ind := r.INV_XIND;
      v_ko_ind  := r.KO_XIND;
      v_ac_db_amt := r.AC_DB_AMT;
      v_ac_cr_amt := r.AC_CR_AMT;
      
      if(v_ac_db_amt is null) then
        v_ac_db_amt := 0.00;
      end if;
      if(v_ac_cr_amt is null) then
        v_ac_cr_amt := 0.00;
      end if;
      r_row.GL_AMOUNT   := (v_ac_db_amt-v_ac_cr_amt);

      if((v_inv_ind is not null AND v_inv_ind ='Y') OR (v_ko_ind is not null AND v_ko_ind = 'Y')) 
      then
        if(v_paylink_name=r.NAME) then
          r_row.PAYEENAME := '';
        end if;        
        if(v_pymt_descp=r.STMT_DESCP) then
          r_row.PYMTDESCP := '';
        end if;
        if(v_agent_id = r.AC_AGENT_ID) then
          v_account_code := '';
          is_print_amt := false;
        end if;
        if(is_print_amt = true) then
          if(v_gl_amt = (v_ac_db_amt-v_ac_cr_amt)) then
            if(v_gl_seq_no = r.GL_SEQ_NO) then
              r_row.GL_AMOUNT    := 0.00;
            else
             r_row.GL_AMOUNT   := (v_ac_db_amt-v_ac_cr_amt);             
            end if;
          else
            r_row.GL_AMOUNT   := (v_ac_db_amt-v_ac_cr_amt);
          end if;
        else
          r_row.GL_AMOUNT   := (v_ac_db_amt-v_ac_cr_amt);
        end if;
      else
        if(v_paylink_name !=r.NAME) then
          r_row.PAYEENAME := r.NAME;
        end if;
        if(v_pymt_descp!=r.STMT_DESCP) then
          r_row.PYMTDESCP := r.STMT_DESCP;
        end if;
        if(v_agent_id != r.AC_AGENT_ID) then
          v_account_code := r.AC_AGENT_ID;
        end if;
      end if;

      r_row.CLIENTCODE    := v_account_code;
      
      v_paylink_name  := r.NAME;
      v_pymt_descp    := r.STMT_DESCP;
      v_agent_id      := r.AC_AGENT_ID;
      v_ac_db_amt     := r.AC_DB_AMT;
      v_ac_cr_amt     := r.AC_CR_AMT;
      
      v_gl_seq_no     := r.GL_SEQ_NO;
      v_gl_amt        :=  v_ac_db_amt-v_ac_cr_amt;
      
      pipe row(r_row);
    END LOOP;
    
  RETURN ;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( v_ProcName_v || '.' || v_ProcName_v, 1, SQLERRM);
END FN_RPAC_OUTSOURCE_SUMM_SUMMGL;
    

   --RPAC_OUTSOURCE_SUMMLIST Outsource Payment Listing - SUMM
FUNCTION FN_RPAC_OUTSOURCE_SUMMLST_GL (p_pvNo IN VARCHAR2) 
      RETURN RPAC_OUTSRC_SUMMLIST_RDET_GL_T PIPELINED 
      IS
      v_ProcName_v    VARCHAR2 (30) := 'FN_RPAC_OUTSOURCE_SUMMLST_GL';
      r_row RPAC_OUTSRC_SUMMLIST_RDET_GL;
      v_account_code VARCHAR2(500);
      v_pv_no  VARCHAR2(400);
      v_inv_ind      VARCHAR2(10);
      v_ko_ind       VARCHAR2(10);
      v_paylink_name VARCHAR2(400);
      v_pymt_descp   VARCHAR2(400);
      v_gl_seq_no    NUMBER;
      v_gl_amt      NUMBER;
      v_ac_db_amt      NUMBER;
      v_ac_cr_amt      NUMBER;
      v_agent_id     VARCHAR(80);
      v_profile_key  VARCHAR(80);
      v_pymt_name    VARCHAR(300);
      is_print_amt   boolean;
      v_nric VARCHAR2(200);
      v_profile_code VARCHAR2(100);
      v_buss_reg VARCHAR2(20);
      v_new_nric VARCHAR2(20);
      v_old_nric VARCHAR2(20);
      v_pol_army VARCHAR2(20);
      v_buss_reg_ind VARCHAR2(10);
      v_new_nric_ind VARCHAR2(10);
      v_old_nric_ind VARCHAR2(10);
      v_pol_army_ind VARCHAR2(10);
      v_creditor VARCHAR2(10);
      v_agent VARCHAR2(10);
      v_ri VARCHAR2(10);
      v_ext_typ VARCHAR2(50);
BEGIN

PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'p_pvNo: ' || p_pvNo ||': '|| SQLERRM);

    SELECT CODE INTO v_creditor FROM SAPM_NEWUTIL WHERE UKEY = 'AC_ACSRC_CREDITOR';
    SELECT CODE INTO v_agent FROM SAPM_NEWUTIL WHERE UKEY = 'AC_ACSRC_AGENT';
    SELECT CODE INTO v_ri FROM SAPM_NEWUTIL WHERE UKEY = 'AC_ACSRC_RI';     
    FOR r IN (SELECT ACPY_PAYLINK.AMOUNT, ACPY_PAYLINK.AC_NO, ACPY_PAYLINK_GL.AC_AGENT_ID, ACPY_PAYLINK.NAME,  -- 3.00 start
          ( SELECT ACGL_LEDGER.DESCP FROM ACGL_LEDGER WHERE ACGL_LEDGER.UKEY = ACPY_PAYLINK_GL.AC_GLCODE ) As GL_DESC, 
          ACPY_PAYLINK_GL.AC_GLCODE, ACPY_PAYLINK_GL.AC_DB_AMT ,ACPY_PAYLINK_GL.AC_GLAMT, ACPY_PAYLINK_GL.AC_CR_AMT, ACPY_PAYLINK.STMT_DESCP, NULL AS DOC_NO, -- 3.00
          NULL AS DOC_AMT, ACPY_PAYLINK.KO_XIND, ACPY_PAYLINK.INV_XIND, ACPY_PAYLINK_GL.GL_SEQ_NO, ACPY_PAYLINK_GL.ITM_NO AS ITM_NO_GL, 
          NULL AS ITM_NO_KO, ACPY_PAYLINK.PLATFORM, ACPY_PAYLINK.DELIVERY_MTD, ACPY_PAYLINK.DELIVERY_TO, ACPY_PAYLINK.ZONE, 
          ACPY_PAYLINK.PROFILE_CODE, ACPY_PAYLINK.PROFILE_KEY, --ACPY_PAYLINK.CREDITOR, ACPY_PAYLINK.CLIENT, 
          ACPY_PAYLINK.FCURR, ACPY_PAYLINK.FAMT, ACPY_PAYLINK.XRATE, ACPY_PAYLINK.AC_SRC, ACPY_PAYLINK.AGENT_CODE,
          ACPY_PAYLINK.BENE_ID, ACPY_PAYLINK.BENE_VERSION, ACPY_PAYLINK.PART_ID, ACPY_PAYLINK.PART_VERSION,ACPY_PAYLINK.EXT_PART_ID, ACPY_PAYLINK.EXT_PART_VERSION
          FROM (ACPY_PAYLINK LEFT OUTER JOIN ACPY_PAYLINK_GL ON ACPY_PAYLINK.AC_NO = ACPY_PAYLINK_GL.AC_NO) 
          --LEFT OUTER JOIN ACPY_PAYLINK_KO ON ACPY_PAYLINK_KO.AC_NO = ACPY_PAYLINK_GL.AC_NO AND ACPY_PAYLINK_KO.GL_SEQ_NO = ACPY_PAYLINK_GL.GL_SEQ_NO) 
          LEFT OUTER JOIN CMUW_MORTGAGEE ON ACPY_PAYLINK.BANK = CMUW_MORTGAGEE.CODE
          WHERE    ACPY_PAYLINK.AC_NO = p_pvNo AND 
          ACPY_PAYLINK_GL.AC_GLCODE <> CMUW_MORTGAGEE.GEN_LEDG
          ORDER BY ACPY_PAYLINK_GL.ITM_NO) -- 3.00 end
         -- ACPY_PAYLINK_KO.ITM_NO )
    LOOP
      is_print_amt := true;
      r_row.PVNO      := null;
      r_row.GLCODE    := null;
      r_row.GLDESCP   := null;
      r_row.AMOUNT          := null;
      r_row.ACCOUNTCODE     := null;
      r_row.CLIENTCODE      := null;
      r_row.PAYEENAME       := null;
      r_row.PYMTDESCP       := null;
      r_row.FORNCURR        := null;
      r_row.FORNAMT         := null;
      r_row.EXCRATE         := null;
      r_row.BANKMODE        := null;
      r_row.IDNO            := NULL;
      v_nric                := NULL;--APP177846
      r_row.BANK_ACC_NO     := NULL; --APP178519

      
      r_row.PVNO      := r.AC_NO;
      r_row.GLCODE    := r.AC_GLCODE;
      r_row.GLDESCP   := r.GL_DESC;
      --r_row.AMOUNT    := r.AC_DB_AMT; -- 3.00 start
      r_row.AMOUNT    := r.AC_GLAMT; -- 3.00 end
      r_row.PAYEENAME := r.NAME;
      r_row.PYMTDESCP := r.STMT_DESCP;      
      r_row.FORNCURR  := r.FCURR;
      r_row.FORNAMT   := r.FAMT;
      r_row.EXCRATE   := r.XRATE;

      
      if (r.PLATFORM='OS') Then
        v_nric := '';
        r_row.BANKMODE          := r.PLATFORM||' '||r.DELIVERY_MTD||' '||r.DELIVERY_TO;
      else
        BEGIN
          SELECT substr(B.BANK_CODE,1,4) INTO r_row.BANKMODE FROM CPGE_PARTNERS_BENE_HIST B WHERE r.BENE_ID = B.BENE_ID AND r.BENE_VERSION = B.BENE_VERSION;
          r_row.BANKMODE := r.PLATFORM||' '||r_row.BANKMODE;
        EXCEPTION WHEN OTHERS THEN 
        PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'step 1.0 :  BENE_ID, BENE_VERSION : ' || r.BENE_ID||'/ '||  r.BENE_VERSION ||': '|| SQLERRM); -- 21082024 added log
        NULL;
        END;
        --APP189704
       /* BEGIN
          if (r.AC_SRC=v_agent) Then
            SELECT ID_VALUE1 INTO v_nric FROM DMAG_VI_AGENT WHERE AGENTCODE = r.AGENT_CODE;
          elsif(r.AC_SRC=v_ri) then
            SELECT CP.ID_VALUE1 INTO v_nric FROM CPGE_VI_PARTNER_DETAILS CP WHERE CP.PART_ID = r.PART_ID AND CP.PART_VERSION = r. PART_VERSION;
          else
           SELECT CPGE_VI_PARTNER_EXTERNAL.ID_VALUE1 INTO v_nric FROM CPGE_VI_PARTNER_EXTERNAL 
           WHERE CPGE_VI_PARTNER_EXTERNAL.EXT_PART_ID = r.EXT_PART_ID AND CPGE_VI_PARTNER_EXTERNAL.EXT_PART_VERSION = r.EXT_PART_VERSION;
          end if;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;*/
        
          --APP178519   
          BEGIN
          SELECT  B.BANK_ACC_NO  
            INTO r_row.BANK_ACC_NO
           FROM CPGE_PARTNERS_BENE_HIST B WHERE r.BENE_ID = B.BENE_ID AND r.BENE_VERSION = B.BENE_VERSION;
           EXCEPTION WHEN OTHERS THEN 
            PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'step 1.1 :  BENE_ID, BENE_VERSION : ' || r.BENE_ID||'/ '||  r.BENE_VERSION ||': '|| SQLERRM); -- 21082024 added log
           NULL;
          END; 
       END IF;
      
      -- APP189704
     -- IF v_nric IS NULL OR R.PROFILE_KEY IS NOT NULL THEN 
        BEGIN
        SELECT  B.IBG_VERIFY_ID
          INTO v_nric
         FROM CPGE_PARTNERS_BENE_HIST B WHERE r.BENE_ID = B.BENE_ID AND r.BENE_VERSION = B.BENE_VERSION AND B.IBG_VERIFY_IND ='Y' ;
         EXCEPTION WHEN OTHERS THEN 
          PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'step 1.2 :  BENE_ID, BENE_VERSION : ' || r.BENE_ID||'/ '||  r.BENE_VERSION ||': '|| SQLERRM); -- 21082024 added log
         NULL;
        END;         
     -- end if;
      

      r_row.IDNO  := v_nric;
      v_account_code :='';
      
      if(r.AC_AGENT_ID is not null) then
        v_account_code := r.AC_AGENT_ID||'/';
      end if;
      
      if(r.PROFILE_KEY is not null) then
        v_account_code := v_account_code||r.PROFILE_KEY||'/';
      end if;      
      v_account_code := v_account_code||r.NAME;
      
      if(v_pv_no = r.AC_NO) then
          r_row.PVNO := '';
      else
          v_pv_no := r.AC_NO;
      end if;
        
      v_inv_ind := r.INV_XIND;
      v_ko_ind  := r.KO_XIND;
      v_ac_db_amt := r.AC_DB_AMT;
      v_ac_cr_amt := r.AC_CR_AMT;
      
      if(v_ac_db_amt is null) then
        v_ac_db_amt := 0.00;
      end if;
      if(v_ac_cr_amt is null) then
        v_ac_cr_amt := 0.00;
      end if;
      r_row.GL_AMOUNT   := (v_ac_db_amt-v_ac_cr_amt);

      if((v_inv_ind is not null AND v_inv_ind ='Y') OR (v_ko_ind is not null AND v_ko_ind = 'Y')) 
      then
        if(v_paylink_name=r.NAME) then
          r_row.PAYEENAME := '';
        end if;        
        if(v_pymt_descp=r.STMT_DESCP) then
          r_row.PYMTDESCP := '';
        end if;
        if(v_agent_id = r.AC_AGENT_ID) then
          v_account_code := '';
          is_print_amt := false;
        end if;
        if(is_print_amt = true) then
          if(v_gl_amt = (v_ac_db_amt-v_ac_cr_amt)) then
            if(v_gl_seq_no = r.GL_SEQ_NO) then
              r_row.GL_AMOUNT    := 0.00;
            else
             r_row.GL_AMOUNT   := (v_ac_db_amt-v_ac_cr_amt);             
            end if;
          else
            r_row.GL_AMOUNT   := (v_ac_db_amt-v_ac_cr_amt);
          end if;
        else
          r_row.GL_AMOUNT   := (v_ac_db_amt-v_ac_cr_amt);
        end if;
      else
        if(v_paylink_name !=r.NAME) then
          r_row.PAYEENAME := r.NAME;
        end if;
        if(v_pymt_descp!=r.STMT_DESCP) then
          r_row.PYMTDESCP := r.STMT_DESCP;
        end if;
        if(v_agent_id != r.AC_AGENT_ID) then
          v_account_code := r.AC_AGENT_ID;
        end if;
      end if;

      r_row.CLIENTCODE    := v_account_code;
      
      v_paylink_name  := r.NAME;
      v_pymt_descp    := r.STMT_DESCP;
      v_agent_id      := r.AC_AGENT_ID;
      v_ac_db_amt     := r.AC_DB_AMT;
      v_ac_cr_amt     := r.AC_CR_AMT;
      
      v_gl_seq_no     := r.GL_SEQ_NO;
      v_gl_amt        :=  v_ac_db_amt-v_ac_cr_amt;
      
      
      pipe row(r_row);
    END LOOP;
    
  RETURN ;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( v_ProcName_v || '.' || v_ProcName_v, 1, SQLERRM);
END FN_RPAC_OUTSOURCE_SUMMLST_GL;

   --RPAC_OUTSOURCE_SUMMLIST Outsource Payment Listing -
   FUNCTION FN_RPAC_OUTSOURCE_SUMMLST_INV (p_pvNo VARCHAR2)
      RETURN RPAC_OUTSRC_SUMMLST_RDET_INV_T
      PIPELINED
   IS
      v_ProcName_v   VARCHAR2 (30) := 'FN_RPAC_OUTSOURCE_SUMMLST_INV';
      v_Step_v       VARCHAR2 (5) := '000';
      r_row          RPAC_OUTSRC_SUMMLIST_RDET_INV;
   BEGIN
   
   PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'p_pvNo: ' || p_pvNo ||': '|| SQLERRM);
   
   FOR r IN (SELECT ACPY_PAYLINK_INV.AC_NO, ACPY_PAYLINK_INV.INV_NO 
              FROM ACPY_PAYLINK ACPY_PAYLINK 
              INNER JOIN ACPY_PAYLINK_INV ON ACPY_PAYLINK.AC_NO = ACPY_PAYLINK_INV.AC_NO 
              WHERE ACPY_PAYLINK.AC_NO = p_pvNo )
     LOOP
      r_row.PVNO       := null;
      r_row.INVNO      := null;
      r_row.PYMTMODE   := null;
      r_row.BANKMODE   := null;
      r_row.IDNO       := null;
      
      r_row.PVNO       := r.AC_NO;
      r_row.INVNO      := r.INV_NO;
      r_row.PYMTMODE   := null;
      r_row.BANKMODE   := null;
      r_row.IDNO       := null;      
      PIPE ROW(r_row);
     END LOOP;
      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   END FN_RPAC_OUTSOURCE_SUMMLST_INV;

   
   FUNCTION FN_RPAC_OUTSOURCE_SUMMLST_CLM (p_pvNo VARCHAR2)
      RETURN RPAC_OUTSOURCE_SUMMLST_CLM_T
      PIPELINED
   IS
      v_ProcName_v   VARCHAR2 (30) := 'FN_RPAC_OUTSOURCE_SUMMLST_CLM';
      v_Step_v       VARCHAR2 (5) := '000';
      r_row          RPAC_OUTSOURCE_SUMMLST_CLM;
   BEGIN
   PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'p_pvNo: ' || p_pvNo ||': '|| SQLERRM);
   
   FOR r IN (SELECT    ACPY_PAYLINK_CLM.CLM_NO, ACPY_PAYLINK_CLM.CLM_AMT, ACPY_PAYLINK_CLM.CLM_PYMT_NO
                --, ACPY_PAYLINK.CREDITOR, ACPY_PAYLINK.CLIENT, ACPY_PAYLINK.PROFILE_CODE, ACPY_PAYLINK.PROFILE_KEY, ACPY_PAYLINK.PLATFORM, ACPY_PAYLINK.AC_SRC, ACPY_PAYLINK.BEN_VERIFC_ID
                 FROM     ACPY_PAYLINK 
                INNER JOIN ACPY_PAYLINK_CLM ON ACPY_PAYLINK.AC_NO = ACPY_PAYLINK_CLM.AC_NO
                WHERE    ACPY_PAYLINK.AC_NO = p_pvNo)
     LOOP
      r_row.CLMNO       := null;
      r_row.CLMAMT      := null;
      r_row.CLMPYMTNO   := null;

      
      r_row.CLMNO       := R.CLM_NO;
      r_row.CLMAMT      := R.CLM_AMT;
      r_row.CLMPYMTNO   := R.CLM_PYMT_NO;
      
      PIPE ROW(r_row);
     END LOOP;
      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   END FN_RPAC_OUTSOURCE_SUMMLST_CLM;
   

   --RPAC_OUTSOURCE_SUMMLIST Outsource Payment Listing -
   FUNCTION FN_RPAC_OUTSOURCE_SUMMLST_KO (p_pvNo VARCHAR2)
      RETURN RPAC_OUTSRC_SUMMLIST_RDET_KO_T
      PIPELINED
   IS
      v_ProcName_v   VARCHAR2 (30) := 'FN_RPAC_OUTSOURCE_SUMMLST_KO';
      v_Step_v       VARCHAR2 (5) := '000';
      r_row          RPAC_OUTSRC_SUMMLIST_RDET_KO;

   BEGIN
   
   PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, 'p_pvNo: ' || p_pvNo ||': '|| SQLERRM);
   
    FOR r IN (SELECT   ACPY_PAYLINK.AC_NO, 
              CASE WHEN ACGC_KO_INST.INST_CYCLE IS NOT NULL THEN ACPY_PAYLINK_KO.DOC_NO||':'||ACGC_KO_INST.INST_CYCLE ELSE ACPY_PAYLINK_KO.DOC_NO END as DOC_NO, -- 12.00
              CASE WHEN ACGC_KO_INST.INST_CYCLE IS NOT NULL THEN ACGC_KO_INST.DOC_AMT ELSE ACPY_PAYLINK_KO.DOC_AMT END as DOC_AMT, -- 12.00
              ACPY_PAYLINK_NARR.NARR,
              ACPY_PAYLINK_NARR.NARR_AMT, ACPY_PAYLINK_CLM.CLM_PYMT_NO, ACPY_PAYLINK_CLM.CLM_AMT
              FROM (ACPY_PAYLINK 
              LEFT OUTER JOIN ACPY_PAYLINK_KO ON ACPY_PAYLINK.AC_NO = ACPY_PAYLINK_KO.AC_NO and ACPY_PAYLINK.KO_XIND = 'Y')
              LEFT OUTER JOIN ACGC_KO_INST ON ACPY_PAYLINK_KO.AC_NO = ACGC_KO_INST.AC_NO AND ACGC_KO_INST.DOC_NO = ACPY_PAYLINK_KO.DOC_NO -- 12.00
              LEFT OUTER JOIN ACPY_PAYLINK_NARR ON ACPY_PAYLINK.AC_NO = ACPY_PAYLINK_NARR.AC_NO and ACPY_PAYLINK.NARR_XIND='Y' 
              and (ACPY_PAYLINK.KO_XIND='N' or ACPY_PAYLINK.KO_XIND is null or ACPY_PAYLINK.KO_XIND ='')  
              and (ACPY_PAYLINK.CLM_XIND='N' or ACPY_PAYLINK.CLM_XIND is null or  ACPY_PAYLINK.CLM_XIND='')  
              LEFT OUTER JOIN ACPY_PAYLINK_CLM ON ACPY_PAYLINK.AC_NO = ACPY_PAYLINK_CLM.AC_NO and ACPY_PAYLINK.CLM_XIND='Y' 
              and ((ACPY_PAYLINK.NARR_XIND='N' or ACPY_PAYLINK.NARR_XIND is null or ACPY_PAYLINK.NARR_XIND = '') or ACPY_PAYLINK.NARR_XIND='Y')
              and (ACPY_PAYLINK.KO_XIND='N' or ACPY_PAYLINK.KO_XIND is null or  ACPY_PAYLINK.KO_XIND = '')
              WHERE    ACPY_PAYLINK.AC_NO = p_pvNo)
    LOOP
      r_row.PVNO      :=null;
      r_row.KOFFNO    :=null;
      r_row.KOFFAMT   :=null;
      r_row.CPVNO     :=null;
      r_row.CPVAMT    :=null;
      r_row.NARR      :=null;
      r_row.NARRAMT   :=null;

      r_row.PVNO      :=r.AC_NO;
      r_row.KOFFNO    :=r.DOC_NO;
      r_row.KOFFAMT   :=r.DOC_AMT;
      r_row.CPVNO     :=r.CLM_PYMT_NO;
      r_row.CPVAMT    :=r.CLM_AMT;
      r_row.NARR      := TO_CHAR(dbms_lob.substr(r.NARR,length(r.NARR))); --Redmine 91757
      
      PG_UTIL_LOG_ERROR.PC_INS_log_error (v_ProcName_v , 0, ' r_row.NARR : ' ||  r_row.NARR  ||': '|| SQLERRM);
      
      r_row.NARRAMT   :=r.NARR_AMT;
      pipe row(r_row);
    END LOOP;
      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   END FN_RPAC_OUTSOURCE_SUMMLST_KO;
   
   FUNCTION FN_RPAC_CCARD_LISTING 
(
  p_Typ IN VARCHAR2  
, p_Branch_From IN VARCHAR2  
, p_Branch_To IN VARCHAR2  
, p_Date_From IN VARCHAR2  
, p_Date_To IN VARCHAR2  
, p_Date_Typ IN VARCHAR2  
)  RETURN PG_RPGE_LISTING.RPAC_CRDTCARD_LIST_T PIPELINED  
  IS
      v_ProcName_v    VARCHAR2 (30) := 'FN_RPAC_CRDTCARD_LIST';
      r_row           PG_RPGE_LISTING.RPAC_CRDTCARD_LIST;
      v_Columns       VARCHAR2 (1000);
      v_Where         VARCHAR2 (1000);
      v_Order         VARCHAR2 (1000);
      v_From          VARCHAR2 (1000);
      TYPE RptCurTyp  IS REF CURSOR;
      v_rpt_cursor    RptCurTyp;
      v_Step_v        VARCHAR2 (5) := '000';
   BEGIN
    v_Order := ' ORDER BY UWCC_CARD.BRANCH, UWCC_CARD_DET_RCPT.RCPT_NO, UWCC_CARD.TRAN_DATE, UWCC_CARD_DET.MAINCLS, UWCC_CARD_DET.CLS, UWCC_CARD.CODE ';
    v_From := ' FROM ((( UWCC_CARD LEFT OUTER JOIN UWCC_CARD_DET ON UWCC_CARD.CODE = UWCC_CARD_DET.CODE ) LEFT OUTER JOIN UWCC_CARD_DET_RCPT ON UWCC_CARD_DET.CODE = UWCC_CARD_DET_RCPT.CODE ) LEFT OUTER JOIN UWCC_CARD_DET_PYMT ON UWCC_CARD_DET.CODE = UWCC_CARD_DET_PYMT.CODE )  LEFT OUTER JOIN UWCC_CARD_DET_SYSJOUR ON UWCC_CARD_DET.CODE = UWCC_CARD_DET_SYSJOUR.CODE , CMDM_BRANCH ';
    v_Columns := 'SELECT UWCC_CARD.CODE, UWCC_CARD.TRAN_DATE, UWCC_CARD.CRCARD_NO, UWCC_CARD.CRCARD_EXP_DATE, UWCC_CARD.TOT_AMT_DUE,((SELECT CODE FROM SAPM_NEWUTIL WHERE UKEY=''AC_CC_PCT'') ) As NET_AMT, UWCC_CARD.CLIENT_NAME, UWCC_CARD_DET.MAINCLS, UWCC_CARD_DET.CLS, UWCC_CARD_DET.SUBCLS, UWCC_CARD_DET.CNOTE_ID, UWCC_CARD.DOWNLOAD_DATE, UWCC_CARD.CANCEL_DATE, UWCC_CARD.RESP_CODE, UWCC_CARD_DET_SYSJOUR.SYSJOUR_NO, UWCC_CARD_DET_RCPT.RCPT_NO, UWCC_CARD_DET_PYMT.PYMT_NO, UWCC_CARD_DET.DOC_NO, UWCC_CARD.AGENT, UWCC_CARD.UPLOAD_DATE, UWCC_CARD.BRANCH,CMDM_BRANCH.BRANCH_CODE, UWCC_CARD.REASON_CODE ';
   
    if(p_Typ='AL') then
        v_Where := ' WHERE UWCC_CARD.RESP_CODE=''A'' AND UWCC_CARD.MERCHANT_TYPE=''REC'' AND CMDM_BRANCH.BRANCH_PREFIX = UWCC_CARD.BRANCH ';
    end if; 
    if(p_Typ='KL') then
        v_Where := ' WHERE (UWCC_CARD.RESP_CODE='''' OR UWCC_CARD.RESP_CODE IS NULL) AND UWCC_CARD.MERCHANT_TYPE=''REC'' AND UWCC_CARD.DEL_TRAN_DATE IS NULL AND CMDM_BRANCH.BRANCH_PREFIX = UWCC_CARD.BRANCH ';
    end if;
    if(p_Typ='CL') then
      v_Where := ' WHERE UWCC_CARD.RESP_CODE=''A'' AND UWCC_CARD.CANCEL_IND=''Y'' AND UWCC_CARD.MERCHANT_TYPE=''REC'' AND CMDM_BRANCH.BRANCH_PREFIX = UWCC_CARD.BRANCH ';
    end if;
    if(p_Typ ='RL') then
      v_Where := ' WHERE UWCC_CARD.RESP_CODE=''D'' AND UWCC_CARD.MERCHANT_TYPE=''REC'' AND CMDM_BRANCH.BRANCH_PREFIX = UWCC_CARD.BRANCH ';
    end if;

    if (p_Date_From is not null and p_Date_To is not null) then
      if p_Date_Typ='ID' then
        v_Where := v_Where||' AND UWCC_CARD.TRAN_DATE BETWEEN TO_DATE ('''|| p_Date_From||''', ''DD/MM/YYYY'') AND TO_DATE ('''|| p_Date_To||''', ''DD/MM/YYYY'')';
      end if;
      if (p_Date_Typ='DD') then
        v_Where := v_Where||' AND UWCC_CARD.DOWNLOAD_DATE BETWEEN TO_DATE ('''|| p_Date_From||''', ''DD/MM/YYYY'') AND TO_DATE ('''|| p_Date_To||''', ''DD/MM/YYYY'')';
      end if;
      if (p_Date_Typ='CD') then
        v_Where := v_Where||' AND UWCC_CARD.CANCEL_DATE BETWEEN TO_DATE ('''|| p_Date_From||''', ''DD/MM/YYYY'') AND TO_DATE ('''|| p_Date_To||''', ''DD/MM/YYYY'')';
      end if;
    end if;
    
    if(p_Branch_From is not null and p_Branch_To is not null) then 
      v_Where := v_Where||' AND CMDM_BRANCH.BRANCH_CODE BETWEEN '''||p_Branch_From||''' AND '''||p_Branch_To||'''';
    end if;
   
    open  v_rpt_cursor for v_Columns||v_From||v_Where||v_Order;
    loop
      fetch v_rpt_cursor into r_row;
      exit when v_rpt_cursor%NOTFOUND;
      pipe row (r_row);
    end loop;

   RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( v_ProcName_v || '.' || v_ProcName_v, 1, SQLERRM);
    END FN_RPAC_CCARD_LISTING;
    
    FUNCTION FN_RPAC_OUTRANSTATE 
    (
      P_EDATEFROM IN VARCHAR2  
    , P_EDATETO IN VARCHAR2  
    , P_AGEFROM IN VARCHAR2  
    , P_AGETO IN VARCHAR2  
    , P_BRANCHCODE IN VARCHAR2  
    , P_AGENTIDFROM IN VARCHAR2  
    , P_AGENTIDTO IN VARCHAR2  
    , P_SORTBY IN VARCHAR2  
    , P_MKTFROM IN VARCHAR2  
    , P_MKTTO IN VARCHAR2  
    , P_ACCTSRC IN VARCHAR2  
    , P_ACCTTYPFROM IN VARCHAR2  
    , P_ACCTTYPTO IN VARCHAR2  
    , P_SUBCHNNL IN VARCHAR2  
    , P_CHNNL IN VARCHAR2
    , P_IS_FAC IN VARCHAR2
    ) 
    RETURN PG_RPGE_LISTING.RPAC_OUTRANSTATE_T PIPELINED IS
          v_ProcName_v    VARCHAR2 (30) := 'FN_RPAC_OUTRANSTATE';
          r_out_tran PG_RPGE_LISTING.RPAC_OUTRANSTATE_REC;
          v_Select VARCHAR2(32767);
          TYPE RptCurTyp  IS REF CURSOR;
          v_rpt_cursor    RptCurTyp;
    BEGIN
    v_Select := 'SELECT  DMAG_VI_AGENT.NAME AS AGENT_NAME, ADDR.ADDRESS_LINE1, ADDR.ADDRESS_LINE2, ADDR.ADDRESS_LINE3, ADDR.POSTCODE, '
      ||' (SELECT DESCRIPTION FROM CMGE_POSTCODE WHERE CMGE_POSTCODE.POSTCODE = ADDR.POSTCODE) AS POST_DESCP, '
      ||' (SELECT ge.CODE_DESC FROM CMGE_POSTCODE ps, CMGE_CODE ge WHERE ge.CAT_CODE = ''STATE'' AND ge.CODE_CD = ps.STATE AND ps.POSTCODE = ADDR.POSTCODE) AS STATE_DESCP, '
      ||' ACST_MAST.AGENT_ID, ACST_MAST.ST_SEQ_NO, ACST_MAST.UKEY_MAST, ACST_MAST.ST_DOC, ACST_MAST.POL_NO, ACST_MAST.INSURED, ACST_MAST.EDATE, ACST_MAST.AGE_DAYS, ACST_MAST.NETT, '
      ||' ACST_MAST.MATCH_DOC, ACST_MAST.MATCH_TYPE, ACST_MAST.MATCH_AMT, ACST_MAST.BAL_AMT, ACST_MAST.PRODUCT_CONFIG_CODE, ACST_MAST.COMM1, ACST_MAST.COMM2, ACST_MAST.GROSS, '
      ||' ACST_MAST.AGENT_CODE, ACST_MAST.ST_AMT, ACST_MAST.GL_SEQ_NO, ACST_MAST.ST_TYPE, ACST_MAST.MATCH_AGENT_ID, ACST_MAST.MATCH_GL_SEQ_NO, '
      ||' ACST_MAST.MATCH_ST_SEQ_NO, ACST_MAST.AGENT_CAT_TYPE, ACST_MAST.CLM_NO AS CLAIM_NO, '
      ||' (CASE '
      ||' WHEN (ACST_MAST.AGENT_CAT_TYPE = ''CL'' AND SUBSTR(ACST_MAST.PRODUCT_CONFIG_CODE,0,2) = ''08'') '
      ||' THEN (SELECT CLMT_PYMT.CLM_NO FROM CLMT_PYMT WHERE CLMT_PYMT.PYMT_NO = ACST_MAST.ST_DOC AND ACST_MAST.AGENT_CAT_TYPE = ''CL'' AND SUBSTR(ACST_MAST.PRODUCT_CONFIG_CODE,0,2) = ''08'') '
      ||' WHEN (ACST_MAST.AGENT_CAT_TYPE = ''CL'' AND SUBSTR(ACST_MAST.PRODUCT_CONFIG_CODE, 0,2) <> ''08'') '
      ||' THEN (SELECT CLNM_PYMT.CLM_NO FROM CLNM_PYMT WHERE CLNM_PYMT.PYMT_NO = ACST_MAST.ST_DOC AND ACST_MAST.AGENT_CAT_TYPE = ''CL'' AND SUBSTR(ACST_MAST.PRODUCT_CONFIG_CODE,0,2) <> ''08'') '
      ||' WHEN (ACST_MAST.AGENT_CAT_TYPE IN (''RI'',''CI'')) '
      ||' THEN (SELECT I.INW_POL_NO FROM UWGE_POLICY_INW I LEFT JOIN OCP_POLICY_BASES B ON I.CONTRACT_ID=B.CONTRACT_ID WHERE B.POLICY_REF=ACST_MAST.POL_NO and B.TOP_INDICATOR=''Y'') '
      ||' ELSE '
      ||' '''' '
      ||' END) AS CLM_NO, ACST_MAST.TRAN_DATE, ACST_MAST.CNOTE_NO, ACST_MAST.ST_DESC, ACST_MAST.GST_COMM, ACST_MAST.GST_COMM2, ACST_MAST.BAL_AMT_PREM, ACST_MAST.BAL_AMT_COMM, ACST_MAST.GST, ' 
      ||' ACST_MAST.VEH_NO ,CMDM_BRANCH.BRANCH_NAME, (SELECT DESCP from CMGC_SUBCLS where MAINCLS||CLS||SUBCLS = ACST_MAST.PRODUCT_CONFIG_CODE) SUBCLS_DESCP, ACST_MATCH.TRAN_DATE AS MTC_TRAN_DATE , '
      ||' ACST_MATCH.MATCH_DOC_NO AS MTC_MATCH_DOC_NO, ACST_MATCH.MATCH_DOC_AMT AS MTC_DOC_AMT, ACST_MATCH.MATCH_ST_SEQ_NO AS MTC_MATCH_ST_SEQ_NO, ACST_MATCH.ST_SEQ_NO AS MTC_ST_SEQ_NO, ACST_MATCH.DOC_NO AS MTC_DOC_NO ';
      
      if(P_IS_FAC = 'Y') then
            v_Select := v_Select||' FROM  (  ( ACST_MAST LEFT OUTER JOIN DMAG_VI_AGENT ON ACST_MAST.AGENT_CODE = DMAG_VI_AGENT.AGENTCODE LEFT JOIN CMDM_BRANCH ON DMAG_VI_AGENT.BRANCH_CODE=CMDM_BRANCH.BRANCH_CODE)  )    '||
                    ' LEFT OUTER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (DMAG_VI_AGENT.OFF_ADD_ID, DMAG_VI_AGENT.OFF_ADD_VERSION)) ADDR  '||
                    ' ON DMAG_VI_AGENT.OFF_ADD_ID = ADDR.ADD_ID AND DMAG_VI_AGENT.OFF_ADD_VERSION = ADDR.VERSION  '||
                    ' LEFT OUTER JOIN RIAG_SOURCE ON ACST_MAST.AGENT_CODE = RIAG_SOURCE.RI_CODE LEFT JOIN ACST_MATCH ON (ACST_MATCH.MATCH_ST_SEQ_NO = ACST_MAST.MATCH_ST_SEQ_NO AND ACST_MATCH.MATCH_AGENT_ID = ACST_MAST.MATCH_AGENT_ID AND ACST_MATCH.MATCH_DOC_NO = ACST_MAST.ST_DOC) '||
                    ' WHERE 1=1 ';
            
            if(P_AGENTIDFROM is not null and P_AGENTIDTO is not null) then
                v_Select := v_Select||' AND ACST_MAST.AGENT_ID between '''||P_AGENTIDFROM||''' and '''||P_AGENTIDTO||'''';
            end if;        
            if(P_BRANCHCODE is not null) then
              v_Select := v_Select||' AND DMAG_VI_AGENT.BRANCH_CODE='''||P_BRANCHCODE||'''';
            end if;
            if(P_CHNNL is not null) then
              v_Select := v_Select||' AND  DMAG_VI_AGENT.CHANNEL='''||P_CHNNL||'''';
            end if;
            if(P_SUBCHNNL is not null) then
              v_Select := v_Select||' AND DMAG_VI_AGENT.SUBCHANNEL='''||P_SUBCHNNL||'''';
            end if;
            if(P_EDATEFROM is not null and P_EDATETO is not null) then
              v_Select := v_Select||' AND ACST_MAST.EDATE BETWEEN '''||P_EDATEFROM||''' AND '||P_EDATETO||'''';
            end if;
            if(P_AGEFROM is not null and P_AGETO is not null) then
              v_Select := v_Select||' AND ACST_MAST.AGE_DAYS  BETWEEN '''||P_AGEFROM||''' AND '''||P_AGETO||'''';
            end if;
            
            if('A' = P_SORTBY ) then
                v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 order by ACST_MAST.AGE_DAYS desc, ACST_MAST.AGENT_ID asc, ACST_MAST.ST_SEQ_NO asc ';            
            elsif ('C' = P_SORTBY) then
                v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 order by ACST_MAST.PRODUCT_CONFIG_CODE desc, ACST_MAST.AGENT_ID asc, ACST_MAST.ST_SEQ_NO ascc ';    
            elsif('CO' = P_SORTBY) then
                v_Select := v_Select||' AND (ACST_MAST.COMM1+ACST_MAST.COMM2) <> 0 and ACST_MAST.MATCH_AMT <> 0 and (((ACST_MAST.BAL_AMT > 0) AND ((ACST_MAST.ST_AMT< 0) OR (ACST_MAST.ST_AMT > 0))) OR ((ACST_MAST.BAL_AMT <0) and ((ACST_MAST.ST_AMT < 0) OR (ACST_MAST.ST_AMT > 0)))) '
                  ||' order by (ACST_MAST.COMM1+ACST_MAST.COMM2) desc, ACST_MAST.AGENT_ID asc, ACST_MAST.ST_SEQ_NO asc ';
            elsif('P' = P_SORTBY) then
                v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 and RIAG_SOURCE.SOURCE_ID in (''3-F01'',''4-F01'',''5-F01'',''6-F01'',''7-F01'',''3-C01'',''4-C01'',''5-C01'')    order by ACST_MAST.POL_NO , ACST_MAST.AGENT_ID asc, ACST_MAST.ST_SEQ_NO asc ';            
            elsif('I' = P_SORTBY) then
                v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 order by ACST_MAST.INSURED, ACST_MAST.AGENT_ID, ACST_MAST.ST_SEQ_NO ';            
            end if;
            
            open  v_rpt_cursor for v_Select;
            loop
              fetch v_rpt_cursor into r_out_tran.AGENT_NAME,r_out_tran.ADDRESS_LINE1, r_out_tran.ADDRESS_LINE2, r_out_tran.ADDRESS_LINE3, r_out_tran.POSTCODE, 
                                      r_out_tran.POST_DESCP, r_out_tran.STATE_DESCP, r_out_tran.AGENT_ID, r_out_tran.ST_SEQ_NO, r_out_tran.UKEY_MAST, r_out_tran.ST_DOC, 
                                      r_out_tran.POL_NO, r_out_tran.INSURED, r_out_tran.EDATE, r_out_tran.AGE_DAYS, r_out_tran.NETT, r_out_tran.MATCH_DOC, r_out_tran.MATCH_TYPE, 
                                      r_out_tran.MATCH_AMT, r_out_tran.BAL_AMT, r_out_tran.PRODUCT_CONFIG_CODE, r_out_tran.COMM1, r_out_tran.COMM2, r_out_tran.GROSS, 
                                      r_out_tran.AGENT_CODE, r_out_tran.ST_AMT, r_out_tran.GL_SEQ_NO, r_out_tran.ST_TYPE, r_out_tran.MATCH_AGENT_ID, r_out_tran.MATCH_GL_SEQ_NO, 
                                      r_out_tran.MATCH_ST_SEQ_NO, r_out_tran.AGENT_CAT_TYPE, r_out_tran.CLAIM_NO, r_out_tran.CLM_NO, r_out_tran.TRAN_DATE, r_out_tran.CNOTE_NO, 
                                      r_out_tran.ST_DESC, r_out_tran.GST_COMM, r_out_tran.GST_COMM2, r_out_tran.BAL_AMT_PREM, r_out_tran.BAL_AMT_COMM, r_out_tran.GST,  
                                      r_out_tran.VEH_NO,r_out_tran.BRANCH_NAME, r_out_tran.SUBCLS_DESCP, r_out_tran.MTC_TRAN_DATE,    r_out_tran.MTC_MATCH_DOC_NO,    
                                      r_out_tran.MTC_DOC_AMT,r_out_tran.MATCH_ST_SEQ_NO,r_out_tran.MTC_ST_SEQ_NO,r_out_tran.MTC_DOC_NO;
              exit when v_rpt_cursor%NOTFOUND;
              pipe row (r_out_tran);
            end loop;
      else
            v_Select := v_Select||' , A.SERVICED_BY,(select HEADER_DESCP from cmge_coderel_hdr where cat_code= ''CHNL'' and HEADER_CODE=DMAG_VI_AGENT.CHANNEL) AS CHANNEL_DESCP,  '||
                      '  (select DETAIL_DESCP from cmge_coderel_det where cat_code= ''CHNL'' and DETAIL_CODE =DMAG_VI_AGENT.SUBCHANNEL) AS SUBCHANNEL_DESCP  ';
            if ('A' != P_ACCTSRC) then
              v_Select := v_Select||'  FROM  ACST_MAST LEFT OUTER JOIN RIAG_PROFILE ON ACST_MAST.AGENT_CODE = RIAG_PROFILE.RI_CODE    ' ||
                    '  LEFT OUTER JOIN DMAG_VI_AGENT ON ACST_MAST.AGENT_CODE = DMAG_VI_AGENT.AGENTCODE LEFT JOIN CMDM_BRANCH ON DMAG_VI_AGENT.BRANCH_CODE=CMDM_BRANCH.BRANCH_CODE  ' ||
                    '  LEFT OUTER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (DMAG_VI_AGENT.OFF_ADD_ID, DMAG_VI_AGENT.OFF_ADD_VERSION)) ADDR  ' ||
                    '  ON DMAG_VI_AGENT.OFF_ADD_ID = ADDR.ADD_ID AND DMAG_VI_AGENT.OFF_ADD_VERSION = ADDR.VERSION LEFT JOIN DMAG_AGENTS A on A.INT_ID = DMAG_VI_AGENT.INT_ID '||
                    '  LEFT JOIN ACST_MATCH ON (ACST_MATCH.MATCH_ST_SEQ_NO = ACST_MAST.MATCH_ST_SEQ_NO AND ACST_MATCH.MATCH_AGENT_ID = ACST_MAST.MATCH_AGENT_ID AND ACST_MATCH.MATCH_DOC_NO = ACST_MAST.ST_DOC) WHERE 1=1  ';
            else
              v_Select := v_Select||'  FROM     (  ( ACST_MAST  ' ||
                    '  LEFT OUTER JOIN DMAG_VI_AGENT ON ACST_MAST.AGENT_CODE = DMAG_VI_AGENT.AGENTCODE LEFT JOIN CMDM_BRANCH ON DMAG_VI_AGENT.BRANCH_CODE=CMDM_BRANCH.BRANCH_CODE )  )   ' ||
                    '  LEFT OUTER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (DMAG_VI_AGENT.OFF_ADD_ID, DMAG_VI_AGENT.OFF_ADD_VERSION)) ADDR  ' ||
                    '  ON DMAG_VI_AGENT.OFF_ADD_ID = ADDR.ADD_ID AND DMAG_VI_AGENT.OFF_ADD_VERSION = ADDR.VERSION LEFT JOIN DMAG_AGENTS A on A.INT_ID = DMAG_VI_AGENT.INT_ID '||
                    '  LEFT JOIN ACST_MATCH ON (ACST_MATCH.MATCH_ST_SEQ_NO = ACST_MAST.MATCH_ST_SEQ_NO AND ACST_MATCH.MATCH_AGENT_ID = ACST_MAST.MATCH_AGENT_ID AND ACST_MATCH.MATCH_DOC_NO = ACST_MAST.ST_DOC) WHERE 1=1  ';
            end if;        
            
            if(P_ACCTTYPFROM is not null and P_ACCTTYPTO is not null) then
              v_Select := v_Select||' AND ACST_MAST.AGENT_CAT_TYPE BETWEEN '''||P_ACCTTYPFROM||''' and '''||P_ACCTTYPTO||'''';
            end if;
            if(P_AGENTIDFROM is not null and P_AGENTIDTO is not null) then
              v_Select := v_Select||' AND ACST_MAST.AGENT_CODE BETWEEN '''||P_AGENTIDFROM||''' AND '''||P_AGENTIDTO||'''';
            end if;
            if(P_BRANCHCODE is not null) then
              v_Select := v_Select||' AND DMAG_VI_AGENT.BRANCH_CODE='''||P_BRANCHCODE||'''';
            end if;
            if(P_CHNNL is not null) then
              v_Select := v_Select||' AND  DMAG_VI_AGENT.CHANNEL='''||P_CHNNL||'''';
            end if;
            if(P_SUBCHNNL is not null) then
              v_Select := v_Select||' AND DMAG_VI_AGENT.SUBCHANNEL='''||P_SUBCHNNL||'''';
            end if;
            if(P_EDATEFROM is not null and P_EDATETO is not null) then
              v_Select := v_Select||' AND ACST_MAST.EDATE BETWEEN '''||P_EDATEFROM||''' AND '''||P_EDATETO||'''';
            end if;
            if(P_AGEFROM is not null and P_AGETO is not null) then
              v_Select := v_Select||' AND ACST_MAST.AGE_DAYS  BETWEEN '''||P_AGEFROM||' AND '''||P_AGETO||'''';
            end if;
            if(P_MKTFROM is not null and P_MKTTO is not null) then
             v_Select := v_Select||' AND A.SERVICED_BY IN (Select USER_ID from SASC_USER_PROFILE where USER_ID BETWEEN '''||P_MKTFROM||''' AND '''||P_MKTTO||''')' ;
            end if;
                
          if ('A' = P_ACCTSRC) then            
            if ('A' = P_SORTBY) then
                        v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 '||
                ' ORDER BY ACST_MAST.AGENT_CAT_TYPE, ACST_MAST.AGENT_ID, ACST_MAST.AGE_DAYS desc, ACST_MAST.AGENT_CODE asc, ACST_MAST.ST_SEQ_NO asc ';            
                    elsif('C' = P_SORTBY)then
                        v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 '||
                ' ORDER BY  ACST_MAST.AGENT_CAT_TYPE, ACST_MAST.AGENT_ID, ACST_MAST.PRODUCT_CONFIG_CODE, ACST_MAST.AGENT_CODE asc, ACST_MAST.ST_SEQ_NO asc ';            
                    elsif('CO' = P_SORTBY)then
                        v_Select := v_Select||' AND (ACST_MAST.COMM1+ACST_MAST.COMM2) <> 0 and ACST_MAST.MATCH_AMT <> 0 and (((ACST_MAST.BAL_AMT > 0) AND ((ACST_MAST.ST_AMT< 0) OR (ACST_MAST.ST_AMT > 0))) OR ((ACST_MAST.BAL_AMT <0) and ((ACST_MAST.ST_AMT < 0) OR (ACST_MAST.ST_AMT > 0)))) '||
                ' ORDER BY ACST_MAST.AGENT_CAT_TYPE, ACST_MAST.AGENT_ID,(ACST_MAST.COMM1+ACST_MAST.COMM2) desc, ACST_MAST.AGENT_CODE asc, ACST_MAST.ST_SEQ_NO asc ';
                    elsif('P' = P_SORTBY)then
                        v_Select := v_Select||' AND  ACST_MAST.BAL_AMT <> 0 order by ACST_MAST.AGENT_CAT_TYPE, ACST_MAST.AGENT_ID, ACST_MAST.POL_NO , ACST_MAST.AGENT_CODE asc, ACST_MAST.ST_SEQ_NO asc ';            
                    elsif('I' = P_SORTBY)then
                        v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 order by ACST_MAST.AGENT_CAT_TYPE, ACST_MAST.AGENT_ID,ACST_MAST.INSURED, ACST_MAST.AGENT_CODE asc, ACST_MAST.ST_SEQ_NO asc ';            
                    end if;
                else
                    if ('R' = P_SORTBY) then
                        v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 order by  ACST_MAST.AGE_DAYS desc, RIAG_PROFILE.RI_CODE asc, ACST_MAST.ST_SEQ_NO asc ';            
                    elsif('C' = P_SORTBY)then
                        v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 order by ACST_MAST.PRODUCT_CONFIG_CODE, RIAG_PROFILE.RI_CODE, ACST_MAST.ST_SEQ_NO asc ';            
                    elsif('CO' = P_SORTBY)then
                        v_Select := v_Select||' AND (ACST_MAST.COMM1+ACST_MAST.COMM2) <> 0 and ACST_MAST.MATCH_AMT <> 0 and (((ACST_MAST.BAL_AMT > 0) AND ((ACST_MAST.ST_AMT< 0) OR (ACST_MAST.ST_AMT > 0))) OR ((ACST_MAST.BAL_AMT <0) and ((ACST_MAST.ST_AMT < 0) OR (ACST_MAST.ST_AMT > 0)))) '
                        ||' order by (ACST_MAST.COMM1+ACST_MAST.COMM2) desc, RIAG_PROFILE.RI_CODE asc, ACST_MAST.ST_SEQ_NO asc ';
                    elsif('P' = P_SORTBY)then
                        v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 order by ACST_MAST.POL_NO , RIAG_PROFILE.RI_CODE, ACST_MAST.ST_SEQ_NO asc ';            
                    elsif('I' = P_SORTBY)then
                        v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 order by ACST_MAST.INSURED, RIAG_PROFILE.RI_CODE, ACST_MAST.ST_SEQ_NO asc ';            
                    end if;
                end if;
          open  v_rpt_cursor for v_Select;
            loop
              fetch v_rpt_cursor into r_out_tran.AGENT_NAME,r_out_tran.ADDRESS_LINE1, r_out_tran.ADDRESS_LINE2, r_out_tran.ADDRESS_LINE3, r_out_tran.POSTCODE,
                                      r_out_tran.POST_DESCP, r_out_tran.STATE_DESCP, r_out_tran.AGENT_ID, r_out_tran.ST_SEQ_NO, r_out_tran.UKEY_MAST, r_out_tran.ST_DOC, 
                                      r_out_tran.POL_NO, r_out_tran.INSURED, r_out_tran.EDATE, r_out_tran.AGE_DAYS, r_out_tran.NETT, r_out_tran.MATCH_DOC, r_out_tran.MATCH_TYPE, 
                                      r_out_tran.MATCH_AMT, r_out_tran.BAL_AMT, r_out_tran.PRODUCT_CONFIG_CODE, r_out_tran.COMM1, r_out_tran.COMM2, r_out_tran.GROSS, 
                                      r_out_tran.AGENT_CODE, r_out_tran.ST_AMT, r_out_tran.GL_SEQ_NO, r_out_tran.ST_TYPE, r_out_tran.MATCH_AGENT_ID, r_out_tran.MATCH_GL_SEQ_NO, 
                                      r_out_tran.MATCH_ST_SEQ_NO, r_out_tran.AGENT_CAT_TYPE, r_out_tran.CLAIM_NO, r_out_tran.CLM_NO, r_out_tran.TRAN_DATE, r_out_tran.CNOTE_NO, 
                                      r_out_tran.ST_DESC, r_out_tran.GST_COMM, r_out_tran.GST_COMM2, r_out_tran.BAL_AMT_PREM, r_out_tran.BAL_AMT_COMM, r_out_tran.GST,  
                                      r_out_tran.VEH_NO,r_out_tran.BRANCH_NAME, r_out_tran.SUBCLS_DESCP, r_out_tran.MTC_TRAN_DATE,    r_out_tran.MTC_MATCH_DOC_NO,    
                                      r_out_tran.MTC_DOC_AMT,r_out_tran.MATCH_ST_SEQ_NO,r_out_tran.MTC_ST_SEQ_NO,r_out_tran.MTC_DOC_NO, r_out_tran.SERVICED_BY, r_out_tran.CHANNEL_DESCP, r_out_tran.SUBCHANNEL_DESCP;
              exit when v_rpt_cursor%NOTFOUND;
              pipe row (r_out_tran);
            end loop;
      end if;    
      RETURN;
       EXCEPTION
          WHEN OTHERS
          THEN
             PG_UTIL_LOG_ERROR.PC_INS_log_error ( v_ProcName_v || '.' || v_ProcName_v, 1, SQLERRM);
    END FN_RPAC_OUTRANSTATE;

    
    FUNCTION FN_RPAC_UWRICL_MGTEXP 
(
  P_PROCMTH IN NUMBER
, P_PROCYEAR IN NUMBER  
) RETURN PG_RPGE_LISTING.RPAC_UWRICL_MGTEXP_T PIPELINED 
IS
  v_ProcName_v    VARCHAR2 (30) := 'FN_RPAC_CRDTCARD_LIST';
  o_monthEnd PG_RPGE_LISTING.RPAC_UWRICL_MGTEXP_REC;
BEGIN
  FOR r 
      IN (SELECT SUM(GROSS_PREMX - REBATE_AMT) ALLGPREM, SUM(COMM_AMT    + COMM2_AMT)  AS ALLCOMM 
              FROM (  SELECT GROSS_PREM GROSS_PREMX, REBATE_AMT, COMM_AMT, COMM2_AMT 
                      FROM   UWGE_POLICY_BASES 
                          WHERE  UW_MTH = P_PROCMTH AND UW_YEAR = P_PROCYEAR AND  VERSION_NO = 1 
                  UNION ALL 
                      SELECT DIFF_GROSS_PREM GROSS_PREMX, DIFF_REBATE_AMT REBATE_AMT, DIFF_COMM_AMT COMM_AMT, DIFF_COMM2_AMT  COMM2_AMT
                      FROM   UWGE_POLICY_BASES  
                          WHERE UW_MTH = P_PROCMTH AND UW_YEAR = P_PROCYEAR AND  VERSION_NO > 1 ))
    LOOP
      o_monthEnd.ALLGPREM := null;
      o_monthEnd.GPREM := null;
      o_monthEnd.ALLGCOMM := null;
      o_monthEnd.GCOMM := null;
      
      o_monthEnd.ALLGPREM := r.allgprem;
      o_monthEnd.GPREM := r.allgprem;
      o_monthEnd.ALLGCOMM := r.allcomm;
      o_monthEnd.GCOMM := r.allcomm;
      --PIPE ROW(o_monthEnd);
    END LOOP;
    
    FOR s IN (SELECT SUM(COPREM) SUM_OF_COPREM
              FROM (SELECT C.RI_GROSS_PREM COPREM 
                            FROM   RIGE_POLICY_CONTRACT A
                            LEFT JOIN RIGE_POLICY_VERSION B ON A.CONTRACT_ID = B.CONTRACT_ID
                            LEFT JOIN RIGE_BROKER C ON C.CONTRACT_ID = B.CONTRACT_ID AND C.VERSION_NO = B.VERSION_NO
                            WHERE  B.PROC_YEAR = P_PROCYEAR
                            AND B.PROC_MTH = P_PROCMTH
                            AND C.VERSION_NO = 1
                            AND C.RI_CAT_TYPE = 'CO'                                                          
                            UNION ALL 
                            SELECT C.DIFF_RI_GROSS_PREM COPREM 
                            FROM   RIGE_POLICY_CONTRACT A
                            LEFT JOIN RIGE_POLICY_VERSION B ON A.CONTRACT_ID = B.CONTRACT_ID
                            LEFT JOIN RIGE_BROKER C ON C.CONTRACT_ID = B.CONTRACT_ID AND C.VERSION_NO = B.VERSION_NO
                            WHERE  B.PROC_YEAR = P_PROCYEAR
                            AND B.PROC_MTH = P_PROCMTH
                            AND C.VERSION_NO > 1
                            AND C.RI_CAT_TYPE = 'CO') RI_COPREM)
      LOOP
        o_monthEnd.ALLCOPREM := null;
        o_monthEnd.RPREM := null; 
       
        o_monthEnd.RPREM := s.SUM_OF_COPREM;
        o_monthEnd.ALLCOPREM := s.SUM_OF_COPREM;
      END LOOP;
    
    FOR a IN(SELECT SUM(RPREM) SUM_OF_RPREM
            FROM   (SELECT C.RI_GROSS_PREM RPREM 
                            FROM   RIGE_POLICY_CONTRACT A
                            LEFT JOIN RIGE_POLICY_VERSION B ON A.CONTRACT_ID = B.CONTRACT_ID
                            LEFT JOIN RIGE_BROKER C ON C.CONTRACT_ID = B.CONTRACT_ID AND C.VERSION_NO = B.VERSION_NO
                            WHERE  B.PROC_YEAR = P_PROCYEAR
                            AND B.PROC_MTH = P_PROCMTH
                            AND C.VERSION_NO = 1
                            AND C.RI_CAT_TYPE != 'CO'                                                        
                            UNION ALL 
                            SELECT C.DIFF_RI_GROSS_PREM RPREM 
                            FROM   RIGE_POLICY_CONTRACT A
                            LEFT JOIN RIGE_POLICY_VERSION B ON A.CONTRACT_ID = B.CONTRACT_ID
                            LEFT JOIN RIGE_BROKER C ON C.CONTRACT_ID = B.CONTRACT_ID AND C.VERSION_NO = B.VERSION_NO
                            WHERE  B.PROC_YEAR = P_PROCYEAR
                            AND B.PROC_MTH = P_PROCMTH
                            AND C.VERSION_NO > 1
                            AND C.RI_CAT_TYPE != 'CO') RI_RPREM)
    LOOP
        o_monthEnd.RPREM := null;
        o_monthEnd.RPREM := a.SUM_OF_RPREM;
    END LOOP; 
   
   FOR b IN (SELECT SUM(RCOMM+RCOMM2) SUM_OF_RCOMM, SUM(SERVC) SUM_OF_SERVC
            FROM (SELECT C.RI_COMM_AMT RCOMM, C.RI_COMM2_AMT RCOMM2, C.RI_SCHRG_AMT SERVC 
            FROM   RIGE_POLICY_CONTRACT A
            LEFT JOIN RIGE_POLICY_VERSION B ON A.CONTRACT_ID = B.CONTRACT_ID
            LEFT JOIN RIGE_BROKER C ON C.CONTRACT_ID = B.CONTRACT_ID AND C.VERSION_NO = B.VERSION_NO
            WHERE  B.PROC_YEAR = P_PROCYEAR
            AND B.PROC_MTH = P_PROCMTH
            AND C.VERSION_NO = 1
            UNION ALL 
            SELECT C.DIFF_RI_COMM_AMT  RCOMM, C.DIFF_RI_COMM2_AMT RCOMM2, C.DIFF_RI_SCHRG_AMT SERVC 
            FROM   RIGE_POLICY_CONTRACT A
            LEFT JOIN RIGE_POLICY_VERSION B ON A.CONTRACT_ID = B.CONTRACT_ID
            LEFT JOIN RIGE_BROKER C ON C.CONTRACT_ID = B.CONTRACT_ID AND C.VERSION_NO = B.VERSION_NO
            WHERE  B.PROC_YEAR = P_PROCYEAR
            AND B.PROC_MTH = P_PROCMTH
            AND C.VERSION_NO > 1) RI_RCOMM_SERVC)
    LOOP 
        o_monthEnd.RCOMM := null;
        o_monthEnd.SERVC := null;
       
        o_monthEnd.RCOMM := b.SUM_OF_RCOMM;
        o_monthEnd.SERVC := b.SUM_OF_SERVC;
    END LOOP; 
   
    FOR t IN (SELECT SUM(TOT)   GADJ, 
       AVG((SELECT SUM(TOT) 
            FROM   (SELECT CASE 
                             WHEN REGGROSS IS NULL THEN 0 
                             ELSE REGGROSS 
                           END AS TOT 
                    FROM  (SELECT SUM(OD_AMT + TPD_AMT + TPI_AMT + ADJ_AMT + 
                                      SOL_AMT) 
                                  REGGROSS 
                           FROM   CLMT_MAST_BH 
                           WHERE  PROC_YR = P_PROCYEAR 
                                  AND PROC_MTH = P_PROCMTH 
                                  AND DOC_TYPE = 'R') AA 
                    UNION ALL 
                    SELECT CASE 
                             WHEN REGGROSS IS NULL THEN 0 
                             ELSE REGGROSS 
                           END AS TOT 
                    FROM  (SELECT SUM(RESV_AMT + ADJ_AMT + SOL_AMT) REGGROSS 
                           FROM   CLNM_MAST_BH 
                           WHERE  PROC_YR = P_PROCYEAR 
                                  AND PROC_MTH = P_PROCMTH 
                                  AND DOC_TYPE = 'R') BB)TBLREGGROSS)) AS GREG, 
       AVG((SELECT SUM(RI_AMT_ALL) 
            FROM   (SELECT B.RI_AMT RI_AMT_ALL 
                    FROM   CLMT_MAST_BH A, 
                           CLMT_MAST_RI_BH B, 
                           RIAG_SOURCE C 
                    WHERE  A.UKEY = B.UKEY_BH 
                           AND B.RI_COMP = C.RI_CODE 
                           AND A.DOC_TYPE = 'R' 
                           AND PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH 
                           AND C.SOURCE_ID = '7-C01' 
                    UNION ALL 
                    SELECT B.RI_AMT RI_AMT_ALL 
                    FROM   CLNM_MAST_BH A, 
                           CLNM_MAST_RI_BH B, 
                           RIAG_SOURCE C 
                    WHERE  A.UKEY = B.UKEY_BH 
                           AND B.RI_COMP = C.RI_CODE 
                           AND A.DOC_TYPE = 'R' 
                           AND PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH 
                           AND C.SOURCE_ID = '7-C01') COREG_TBL))      AS COREG, 
       AVG((SELECT SUM(RI_AMT_ALL2) 
            FROM   (SELECT B.RI_AMT RI_AMT_ALL2 
                    FROM   CLMT_MAST_BH A, 
                           CLMT_MAST_RI_BH B, 
                           RIAG_SOURCE C 
                    WHERE  A.UKEY = B.UKEY_BH 
                           AND B.RI_COMP = C.RI_CODE 
                           AND A.DOC_TYPE = 'R' 
                           AND PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH 
                           AND C.SOURCE_ID != '7-C01' 
                    UNION ALL 
                    SELECT B.RI_AMT RI_AMT_ALL2 
                    FROM   CLNM_MAST_BH A, 
                           CLNM_MAST_RI_BH B, 
                           RIAG_SOURCE C 
                    WHERE  A.UKEY = B.UKEY_BH 
                           AND B.RI_COMP = C.RI_CODE 
                           AND A.DOC_TYPE = 'R' 
                           AND PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH 
                           AND C.SOURCE_ID != '7-C01') RREG_TBL))      AS RREG, 
       AVG((SELECT SUM(PAY) 
            FROM   (SELECT PYMT_AMT PAY 
                    FROM   CLMT_PYMT 
                    WHERE  PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH 
                    UNION ALL 
                    SELECT PYMT_AMT PAY 
                    FROM   CLNM_PYMT 
                    WHERE  PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH) GPAY_TBL))        AS GPAY, 
       AVG((SELECT SUM(COADJX) 
            FROM   (SELECT B.DIFF_RI_AMT COADJX 
                    FROM   CLMT_MAST_AH A, 
                           CLMT_MAST_DIFF_RI_AH B, 
                           RIAG_SOURCE C 
                    WHERE  A.UKEY = B.UKEY_AH 
                           AND B.DIFF_RI_COMP = C.RI_CODE 
                           AND ( A.DOC_TYPE = 'A' 
                                  OR A.DOC_TYPE = 'C' ) 
                           AND PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH 
                           AND C.SOURCE_ID = '7-C01' 
                    UNION ALL 
                    SELECT B.DIFF_RI_AMT COADJX 
                    FROM   CLNM_MAST_AH A, 
                           CLNM_MAST_DIFF_RI_AH B, 
                           RIAG_SOURCE C 
                    WHERE  A.UKEY = B.UKEY_AH 
                           AND B.DIFF_RI_COMP = C.RI_CODE 
                           AND ( A.DOC_TYPE = 'A' 
                                  OR A.DOC_TYPE = 'C' ) 
                           AND PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH 
                           AND C.SOURCE_ID = '7-C01' 
                    UNION ALL 
                    SELECT B.DIFF_RI_DET COADJX 
                    FROM   CLMT_PYMT A, 
                           CLMT_PYMT_RI B, 
                           RIAG_SOURCE C 
                    WHERE  A.PYMT_NO = B.PYMT_NO 
                           AND B.RI_COMP = C.RI_CODE 
                           AND PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH 
                           AND C.SOURCE_ID = '7-C01' 
                    UNION ALL 
                    SELECT B.DIFF_RI_DET COADJX 
                    FROM   CLNM_PYMT A, 
                           CLNM_PYMT_RI B, 
                           RIAG_SOURCE C 
                    WHERE  A.PYMT_NO = B.PYMT_NO 
                           AND B.RI_COMP = C.RI_CODE 
                           AND PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH 
                           AND C.SOURCE_ID = '7-C01') COADJ_TBL))      AS COADJ, 
       AVG((SELECT SUM(COPAYX) 
            FROM   (SELECT B.RI_AMT COPAYX 
                    FROM   CLMT_PYMT A, 
                           CLMT_PYMT_RI B, 
                           RIAG_SOURCE C 
                    WHERE  A.PYMT_NO = B.PYMT_NO 
                           AND B.RI_COMP = C.RI_CODE 
                           AND PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH 
                           AND C.SOURCE_ID = '7-C01' 
                    UNION ALL 
                    SELECT B.RI_AMT COPAYX 
                    FROM   CLNM_PYMT A, 
                           CLNM_PYMT_RI B, 
                           RIAG_SOURCE C 
                    WHERE  A.PYMT_NO = B.PYMT_NO 
                           AND B.RI_COMP = C.RI_CODE 
                           AND PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH 
                           AND C.SOURCE_ID = '7-C01') COPAY_TBL))      AS COPAY, 
       AVG((SELECT SUM(RADJX) 
            FROM   (SELECT B.DIFF_RI_AMT RADJX 
                    FROM   CLMT_MAST_AH A, 
                           CLMT_MAST_DIFF_RI_AH B, 
                           RIAG_SOURCE C 
                    WHERE  A.UKEY = B.UKEY_AH 
                           AND B.DIFF_RI_COMP = C.RI_CODE 
                           AND ( A.DOC_TYPE = 'A' 
                                  OR A.DOC_TYPE = 'C' ) 
                           AND PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH 
                           AND C.SOURCE_ID != '7-C01' 
                    UNION ALL 
                    SELECT B.DIFF_RI_AMT RADJX 
                    FROM   CLNM_MAST_AH A, 
                           CLNM_MAST_DIFF_RI_AH B, 
                           RIAG_SOURCE C 
                    WHERE  A.UKEY = B.UKEY_AH 
                           AND B.DIFF_RI_COMP = C.RI_CODE 
                           AND ( A.DOC_TYPE = 'A' 
                                  OR A.DOC_TYPE = 'C' ) 
                           AND PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH 
                           AND C.SOURCE_ID != '7-C01' 
                    UNION ALL 
                    SELECT B.DIFF_RI_DET RADJX 
                    FROM   CLMT_PYMT A, 
                           CLMT_PYMT_RI B, 
                           RIAG_SOURCE C 
                    WHERE  A.PYMT_NO = B.PYMT_NO 
                           AND B.RI_COMP = C.RI_CODE 
                           AND PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH 
                           AND C.SOURCE_ID != '7-C01' 
                    UNION ALL 
                    SELECT B.DIFF_RI_DET RADJX 
                    FROM   CLNM_PYMT A, 
                           CLNM_PYMT_RI B, 
                           RIAG_SOURCE C 
                    WHERE  A.PYMT_NO = B.PYMT_NO 
                           AND B.RI_COMP = C.RI_CODE 
                           AND PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH 
                           AND C.SOURCE_ID != '7-C01') RADJ_TBL))      AS RADJ, 
       AVG((SELECT SUM(RPAYX) 
            FROM   (SELECT B.RI_AMT RPAYX 
                    FROM   CLMT_PYMT A, 
                           CLMT_PYMT_RI B, 
                           RIAG_SOURCE C 
                    WHERE  A.PYMT_NO = B.PYMT_NO 
                           AND B.RI_COMP = C.RI_CODE 
                           AND PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH 
                           AND C.SOURCE_ID != '7-C01' 
                    UNION ALL 
                    SELECT B.RI_AMT RPAYX 
                    FROM   CLNM_PYMT A, 
                           CLNM_PYMT_RI B, 
                           RIAG_SOURCE C 
                    WHERE  A.PYMT_NO = B.PYMT_NO 
                           AND B.RI_COMP = C.RI_CODE 
                           AND PROC_YR = P_PROCYEAR 
                           AND PROC_MTH = P_PROCMTH 
                           AND C.SOURCE_ID != '7-C01') RPAY_TBL))      AS RPAY 
        FROM   (SELECT CASE 
                         WHEN ADJGROSS IS NULL THEN 0 
                         ELSE ADJGROSS 
                       END AS TOT 
                FROM   (SELECT SUM(DIFF_OD + DIFF_TPD + DIFF_TPI + DIFF_ADJ + DIFF_SOL) 
                               ADJGROSS 
                        FROM   CLMT_MAST_AH 
                        WHERE  PROC_YR = P_PROCYEAR 
                               AND PROC_MTH = P_PROCMTH 
                               AND DOC_TYPE IN ( 'A', 'C' )) AA 
                UNION ALL 
                SELECT CASE 
                         WHEN ADJGROSS IS NULL THEN 0 
                         ELSE ADJGROSS 
                       END AS TOT 
                FROM   (SELECT SUM(DIFF_RESV + DIFF_ADJ + DIFF_SOL) ADJGROSS 
                        FROM   CLNM_MAST_AH 
                        WHERE  PROC_YR = P_PROCYEAR 
                               AND PROC_MTH = P_PROCMTH 
                               AND DOC_TYPE IN ( 'A', 'C' )) BB 
                UNION ALL 
                SELECT CASE 
                         WHEN ADJGROSS IS NULL THEN 0 
                         ELSE ADJGROSS 
                       END AS TOT 
                FROM   (SELECT SUM(DIFF_OD_AMT + DIFF_TPD_AMT + DIFF_TPI_AMT 
                                   + DIFF_ADJ_AMT + DIFF_SOL_AMT) ADJGROSS 
                        FROM   CLMT_PYMT 
                        WHERE  PROC_YR = P_PROCYEAR 
                               AND PROC_MTH = P_PROCMTH) CC 
                UNION ALL 
                SELECT CASE 
                         WHEN ADJGROSS IS NULL THEN 0 
                         ELSE ADJGROSS 
                       END AS TOT 
                FROM   (SELECT SUM(DIFF_RESV_AMT + DIFF_ADJ_AMT + DIFF_SOL_AMT) ADJGROSS 
                        FROM   CLNM_PYMT 
                        WHERE  PROC_YR = P_PROCYEAR 
                               AND PROC_MTH = P_PROCMTH) DD) CLNM_MAST_AH_CLNM_PYMT )
    LOOP
        o_monthEnd.GADJ := null;
        o_monthEnd.GREG := null;
        o_monthEnd.COREG := null;
        o_monthEnd.RREG := null;
        o_monthEnd.GPAY := null;
        o_monthEnd.COADJ := null;
        o_monthEnd.COPAY := null;
        o_monthEnd.RADJ := null;
        o_monthEnd.RPAY := null;        
        o_monthEnd.GADJ := t.GADJ;
        o_monthEnd.GREG := t.GREG;
        o_monthEnd.COREG := t.COREG;
        o_monthEnd.RREG := t.RREG;
        o_monthEnd.GPAY := t.GPAY;
        o_monthEnd.COADJ := t.COADJ;
        o_monthEnd.COPAY := t.COPAY;
        o_monthEnd.RADJ := t.RADJ;
        o_monthEnd.RPAY := t.RPAY;
        --PIPE ROW(o_monthEnd);
      END LOOP;
      
   FOR u IN (SELECT SUM(ACDBAMT - ACCRAMT)                               MGTEXPAMT, 
       AVG((SELECT SUM(ACDBAMT - ACCRAMT) 
            FROM   (SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACRC_RCPT A 
                           LEFT OUTER JOIN ACRC_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '10000' 
                           AND B.COA <= '11992' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACPY_PYMT A 
                           LEFT OUTER JOIN ACPY_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '10000' 
                           AND B.COA <= '11992' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_JOUR A 
                           LEFT OUTER JOIN ACJN_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '10000' 
                           AND B.COA <= '11992' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACCR_NCR A 
                           LEFT OUTER JOIN ACCR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '10000' 
                           AND B.COA <= '11992' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACDB_NDB A 
                           LEFT OUTER JOIN ACDB_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '10000' 
                           AND B.COA <= '11992' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_SYSJOUR A 
                           LEFT OUTER JOIN ACJN_SYSJOUR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '10000' 
                           AND B.COA <= '11992' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR) AA)) GPREM, 
       AVG((SELECT SUM(ACDBAMT - ACCRAMT) 
            FROM   (SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACRC_RCPT A 
                           LEFT OUTER JOIN ACRC_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '12001' 
                           AND B.COA <= '12095' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACPY_PYMT A 
                           LEFT OUTER JOIN ACPY_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '12001' 
                           AND B.COA <= '12095' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_JOUR A 
                           LEFT OUTER JOIN ACJN_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '12001' 
                           AND B.COA <= '12095' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACCR_NCR A 
                           LEFT OUTER JOIN ACCR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '12001' 
                           AND B.COA <= '12095' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACDB_NDB A 
                           LEFT OUTER JOIN ACDB_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '12001' 
                           AND B.COA <= '12095' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_SYSJOUR A 
                           LEFT OUTER JOIN ACJN_SYSJOUR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '12001' 
                           AND B.COA <= '12095' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR) AA)) COINOUTPREM, 
       AVG((SELECT SUM(ACDBAMT - ACCRAMT) 
            FROM   (SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACRC_RCPT A 
                           LEFT OUTER JOIN ACRC_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '12100' 
                           AND B.COA <= '14898' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACPY_PYMT A 
                           LEFT OUTER JOIN ACPY_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '12100' 
                           AND B.COA <= '14898' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_JOUR A 
                           LEFT OUTER JOIN ACJN_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '12100' 
                           AND B.COA <= '14898' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACCR_NCR A 
                           LEFT OUTER JOIN ACCR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '12100' 
                           AND B.COA <= '14898' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACDB_NDB A 
                           LEFT OUTER JOIN ACDB_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '12100' 
                           AND B.COA <= '14898' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_SYSJOUR A 
                           LEFT OUTER JOIN ACJN_SYSJOUR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '12100' 
                           AND B.COA <= '14898' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR) AA)) RIOUTPREM, 
       AVG((SELECT SUM(ACDBAMT - ACCRAMT) 
            FROM   (SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACRC_RCPT A 
                           LEFT OUTER JOIN ACRC_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '20000' 
                           AND B.COA <= '22000' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACPY_PYMT A 
                           LEFT OUTER JOIN ACPY_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '20000' 
                           AND B.COA <= '22000' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_JOUR A 
                           LEFT OUTER JOIN ACJN_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '20000' 
                           AND B.COA <= '22000' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACCR_NCR A 
                           LEFT OUTER JOIN ACCR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '20000' 
                           AND B.COA <= '22000' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACDB_NDB A 
                           LEFT OUTER JOIN ACDB_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '20000' 
                           AND B.COA <= '22000' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_SYSJOUR A 
                           LEFT OUTER JOIN ACJN_SYSJOUR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '20000' 
                           AND B.COA <= '22000' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR) AA)) COMMAMT, 
       AVG((SELECT SUM(ACDBAMT - ACCRAMT) 
            FROM   (SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACRC_RCPT A 
                           LEFT OUTER JOIN ACRC_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '25000' 
                           AND B.COA <= '25695' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACPY_PYMT A 
                           LEFT OUTER JOIN ACPY_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '25000' 
                           AND B.COA <= '25695' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_JOUR A 
                           LEFT OUTER JOIN ACJN_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '25000' 
                           AND B.COA <= '25695' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACCR_NCR A 
                           LEFT OUTER JOIN ACCR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '25000' 
                           AND B.COA <= '25695' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACDB_NDB A 
                           LEFT OUTER JOIN ACDB_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '25000' 
                           AND B.COA <= '25695' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_SYSJOUR A 
                           LEFT OUTER JOIN ACJN_SYSJOUR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '25000' 
                           AND B.COA <= '25695' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR) AA)) COMMAMTMINUSPROFCOMM 
       , 
       AVG((SELECT SUM(ACDBAMT - ACCRAMT) 
            FROM   (SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACRC_RCPT A 
                           LEFT OUTER JOIN ACRC_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '22001' 
                           AND B.COA <= '24899' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACPY_PYMT A 
                           LEFT OUTER JOIN ACPY_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '22001' 
                           AND B.COA <= '24899' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_JOUR A 
                           LEFT OUTER JOIN ACJN_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '22001' 
                           AND B.COA <= '24899' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACCR_NCR A 
                           LEFT OUTER JOIN ACCR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '22001' 
                           AND B.COA <= '24899' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACDB_NDB A 
                           LEFT OUTER JOIN ACDB_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '22001' 
                           AND B.COA <= '24899' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_SYSJOUR A 
                           LEFT OUTER JOIN ACJN_SYSJOUR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '22001' 
                           AND B.COA <= '24899' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR) AA)) RICOMMAMT, 
       AVG((SELECT SUM(ACDBAMT - ACCRAMT) 
            FROM   (SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACRC_RCPT A 
                           LEFT OUTER JOIN ACRC_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '27600' 
                           AND B.COA <= '29000' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACPY_PYMT A 
                           LEFT OUTER JOIN ACPY_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '27600' 
                           AND B.COA <= '29000' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_JOUR A 
                           LEFT OUTER JOIN ACJN_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '27600' 
                           AND B.COA <= '29000' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACCR_NCR A 
                           LEFT OUTER JOIN ACCR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '27600' 
                           AND B.COA <= '29000' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACDB_NDB A 
                           LEFT OUTER JOIN ACDB_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '27600' 
                           AND B.COA <= '29000' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_SYSJOUR A 
                           LEFT OUTER JOIN ACJN_SYSJOUR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '27600' 
                           AND B.COA <= '29000' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR) AA)) 
       RICOMMAMTMINUSPROFCOMM, 
       AVG((SELECT SUM(ACDBAMT - ACCRAMT) 
            FROM   (SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACRC_RCPT A 
                           LEFT OUTER JOIN ACRC_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '51201' 
                           AND B.COA <= '52997' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACPY_PYMT A 
                           LEFT OUTER JOIN ACPY_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '51201' 
                           AND B.COA <= '52997' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_JOUR A 
                           LEFT OUTER JOIN ACJN_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '51201' 
                           AND B.COA <= '52997' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACCR_NCR A 
                           LEFT OUTER JOIN ACCR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '51201' 
                           AND B.COA <= '52997' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACDB_NDB A 
                           LEFT OUTER JOIN ACDB_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '51201' 
                           AND B.COA <= '52997' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_SYSJOUR A 
                           LEFT OUTER JOIN ACJN_SYSJOUR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '51201' 
                           AND B.COA <= '52997' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR) AA)) RISERVCHRG, 
       AVG((SELECT SUM(ACDBAMT - ACCRAMT) 
            FROM   (SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACRC_RCPT A 
                           LEFT OUTER JOIN ACRC_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '35007' 
                           AND B.COA <= '36997' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACPY_PYMT A 
                           LEFT OUTER JOIN ACPY_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '35007' 
                           AND B.COA <= '36997' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_JOUR A 
                           LEFT OUTER JOIN ACJN_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '35007' 
                           AND B.COA <= '36997' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACCR_NCR A 
                           LEFT OUTER JOIN ACCR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '35007' 
                           AND B.COA <= '36997' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACDB_NDB A 
                           LEFT OUTER JOIN ACDB_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '35007' 
                           AND B.COA <= '36997' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_SYSJOUR A 
                           LEFT OUTER JOIN ACJN_SYSJOUR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '35007' 
                           AND B.COA <= '36997' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR) AA)) GCLMOUTSTDG, 
       AVG((SELECT SUM(ACDBAMT - ACCRAMT) 
            FROM   (SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACRC_RCPT A 
                           LEFT OUTER JOIN ACRC_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '37006' 
                           AND B.COA <= '37093' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACPY_PYMT A 
                           LEFT OUTER JOIN ACPY_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '37006' 
                           AND B.COA <= '37093' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_JOUR A 
                           LEFT OUTER JOIN ACJN_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '37006' 
                           AND B.COA <= '37093' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACCR_NCR A 
                           LEFT OUTER JOIN ACCR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '37006' 
                           AND B.COA <= '37093' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACDB_NDB A 
                           LEFT OUTER JOIN ACDB_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '37006' 
                           AND B.COA <= '37093' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_SYSJOUR A 
                           LEFT OUTER JOIN ACJN_SYSJOUR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '37006' 
                           AND B.COA <= '37093' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR) AA)) COINCLMOUTSTDG, 
       AVG((SELECT SUM(ACDBAMT - ACCRAMT) 
            FROM   (SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACRC_RCPT A 
                           LEFT OUTER JOIN ACRC_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '30000' 
                           AND B.COA <= '31996' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACPY_PYMT A 
                           LEFT OUTER JOIN ACPY_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '30000' 
                           AND B.COA <= '31996' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_JOUR A 
                           LEFT OUTER JOIN ACJN_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '30000' 
                           AND B.COA <= '31996' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACCR_NCR A 
                           LEFT OUTER JOIN ACCR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '30000' 
                           AND B.COA <= '31996' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACDB_NDB A 
                           LEFT OUTER JOIN ACDB_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '30000' 
                           AND B.COA <= '31996' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_SYSJOUR A 
                           LEFT OUTER JOIN ACJN_SYSJOUR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '30000' 
                           AND B.COA <= '31996' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR) AA)) GCLMPAID, 
       AVG((SELECT SUM(ACDBAMT - ACCRAMT) 
            FROM   (SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACRC_RCPT A 
                           LEFT OUTER JOIN ACRC_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '32000' 
                           AND B.COA <= '32092' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACPY_PYMT A 
                           LEFT OUTER JOIN ACPY_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '32000' 
                           AND B.COA <= '32092' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_JOUR A 
                           LEFT OUTER JOIN ACJN_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '32000' 
                           AND B.COA <= '32092' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACCR_NCR A 
                           LEFT OUTER JOIN ACCR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '32000' 
                           AND B.COA <= '32092' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACDB_NDB A 
                           LEFT OUTER JOIN ACDB_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '32000' 
                           AND B.COA <= '32092' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_SYSJOUR A 
                           LEFT OUTER JOIN ACJN_SYSJOUR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '32000' 
                           AND B.COA <= '32092' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR) AA)) COINCLMPAID, 
       AVG((SELECT SUM(ACDBAMT - ACCRAMT) 
            FROM   (SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACRC_RCPT A 
                           LEFT OUTER JOIN ACRC_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '37103' 
                           AND B.COA <= '39896' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACPY_PYMT A 
                           LEFT OUTER JOIN ACPY_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '37103' 
                           AND B.COA <= '39896' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_JOUR A 
                           LEFT OUTER JOIN ACJN_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '37103' 
                           AND B.COA <= '39896' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACCR_NCR A 
                           LEFT OUTER JOIN ACCR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '37103' 
                           AND B.COA <= '39896' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACDB_NDB A 
                           LEFT OUTER JOIN ACDB_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '37103' 
                           AND B.COA <= '39896' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_SYSJOUR A 
                           LEFT OUTER JOIN ACJN_SYSJOUR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '37103' 
                           AND B.COA <= '39896' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR) AA)) RICLMOS, 
       AVG((SELECT SUM(ACDBAMT - ACCRAMT) 
            FROM   (SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACRC_RCPT A 
                           LEFT OUTER JOIN ACRC_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '32102' 
                           AND B.COA <= '34895' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACPY_PYMT A 
                           LEFT OUTER JOIN ACPY_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '32102' 
                           AND B.COA <= '34895' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_JOUR A 
                           LEFT OUTER JOIN ACJN_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '32102' 
                           AND B.COA <= '34895' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACCR_NCR A 
                           LEFT OUTER JOIN ACCR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '32102' 
                           AND B.COA <= '34895' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACDB_NDB A 
                           LEFT OUTER JOIN ACDB_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '32102' 
                           AND B.COA <= '34895' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR 
                    UNION ALL 
                    SELECT B.AC_DB_AMT ACDBAMT, 
                           B.AC_CR_AMT ACCRAMT 
                    FROM   ACJN_SYSJOUR A 
                           LEFT OUTER JOIN ACJN_SYSJOUR_GL B 
                                        ON A.AC_NO = B.AC_NO 
                    WHERE  B.COA >= '32102' 
                           AND B.COA <= '34895' 
                           AND A.PROC_MTH = P_PROCMTH 
                           AND A.PROC_YR = P_PROCYEAR) AA)) RICLMPYMT 
FROM   (SELECT B.AC_DB_AMT ACDBAMT, 
               B.AC_CR_AMT ACCRAMT 
        FROM   ACRC_RCPT A 
               LEFT OUTER JOIN ACRC_GL B 
                            ON A.AC_NO = B.AC_NO 
        WHERE  B.COA >= '60000' 
               AND B.COA <= '69999' 
               AND A.PROC_MTH = P_PROCMTH 
               AND A.PROC_YR = P_PROCYEAR 
        UNION ALL 
        SELECT B.AC_DB_AMT ACDBAMT, 
               B.AC_CR_AMT ACCRAMT 
        FROM   ACPY_PYMT A 
               LEFT OUTER JOIN ACPY_GL B 
                            ON A.AC_NO = B.AC_NO 
        WHERE  B.COA >= '60000' 
               AND B.COA <= '69999' 
               AND A.PROC_MTH = P_PROCMTH 
               AND A.PROC_YR = P_PROCYEAR 
        UNION ALL 
        SELECT B.AC_DB_AMT ACDBAMT, 
               B.AC_CR_AMT ACCRAMT 
        FROM   ACJN_JOUR A 
               LEFT OUTER JOIN ACJN_GL B 
                            ON A.AC_NO = B.AC_NO 
        WHERE  B.COA >= '60000' 
               AND B.COA <= '69999' 
               AND A.PROC_MTH = P_PROCMTH 
               AND A.PROC_YR = P_PROCYEAR 
        UNION ALL 
        SELECT B.AC_DB_AMT ACDBAMT, 
               B.AC_CR_AMT ACCRAMT 
        FROM   ACCR_NCR A 
               LEFT OUTER JOIN ACCR_GL B 
                            ON A.AC_NO = B.AC_NO 
        WHERE  B.COA >= '60000' 
               AND B.COA <= '69999' 
               AND A.PROC_MTH = P_PROCMTH 
               AND A.PROC_YR = P_PROCYEAR 
        UNION ALL 
        SELECT B.AC_DB_AMT ACDBAMT, 
               B.AC_CR_AMT ACCRAMT 
        FROM   ACDB_NDB A 
               LEFT OUTER JOIN ACDB_GL B 
                            ON A.AC_NO = B.AC_NO 
        WHERE  B.COA >= '60000' 
               AND B.COA <= '69999' 
               AND A.PROC_MTH = P_PROCMTH 
               AND A.PROC_YR = P_PROCYEAR 
        UNION ALL 
        SELECT B.AC_DB_AMT ACDBAMT, 
               B.AC_CR_AMT ACCRAMT 
        FROM   ACJN_SYSJOUR A 
               LEFT OUTER JOIN ACJN_SYSJOUR_GL B 
                            ON A.AC_NO = B.AC_NO 
        WHERE  B.COA >= '60000' 
               AND B.COA <= '69999' 
               AND A.PROC_MTH = P_PROCMTH 
               AND A.PROC_YR = P_PROCYEAR) MGTEXP ) 
    LOOP
      o_monthEnd.MGTEXPAMT := null;
      o_monthEnd.GPREM := null;
      o_monthEnd.COINOUTPREM := null;
      o_monthEnd.RIOUTPREM := null;
      o_monthEnd.COMMAMT := null;
      o_monthEnd.COMMAMTMINUSPROFCOMM := null;
      o_monthEnd.RICOMMAMT := null;
      o_monthEnd.RICOMMAMTMINUSPROFCOMM := null;
      o_monthEnd.RISERVCHRG := null;
      o_monthEnd.GCLMOUTSTDG := null;
      o_monthEnd.COINCLMOUTSTDG := null;
      o_monthEnd.GCLMPAID := null;
      o_monthEnd.COINCLMPAID := null;
      o_monthEnd.RICLMOS := null;
      o_monthEnd.RICLMPYMT := null;
      
      o_monthEnd.MGTEXPAMT := u.MGTEXPAMT;
      o_monthEnd.GPREM := u.GPREM;
      o_monthEnd.COINOUTPREM := u.COINOUTPREM;
      o_monthEnd.RIOUTPREM := u.RIOUTPREM;
      o_monthEnd.COMMAMT := u.COMMAMT;
      o_monthEnd.COMMAMTMINUSPROFCOMM := u.COMMAMTMINUSPROFCOMM;
      o_monthEnd.RICOMMAMT := u.RICOMMAMT;
      o_monthEnd.RICOMMAMTMINUSPROFCOMM := u.RICOMMAMTMINUSPROFCOMM;
      o_monthEnd.RISERVCHRG := u.RISERVCHRG;
      o_monthEnd.GCLMOUTSTDG := u.GCLMOUTSTDG;
      o_monthEnd.COINCLMOUTSTDG := u.COINCLMOUTSTDG;
      o_monthEnd.GCLMPAID := u.GCLMPAID;
      o_monthEnd.COINCLMPAID := u.COINCLMPAID;
      o_monthEnd.RICLMOS := u.RICLMOS;
      o_monthEnd.RICLMPYMT := u.RICLMPYMT;      
    END LOOP;
    PIPE ROW(o_monthEnd);
  RETURN;
    EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( v_ProcName_v || '.' || v_ProcName_v, 1, SQLERRM);
    END FN_RPAC_UWRICL_MGTEXP;
    
    FUNCTION FN_RPAC_OUTRANSTATE_MAST (
      P_EDATEFROM IN VARCHAR2  
    , P_EDATETO IN VARCHAR2  
    , P_AGEFROM IN VARCHAR2  
    , P_AGETO IN VARCHAR2  
    , P_BRANCHCODE IN VARCHAR2  
    , P_AGENTIDFROM IN VARCHAR2  
    , P_AGENTIDTO IN VARCHAR2  
    , P_SORTBY IN VARCHAR2  
    , P_MKTFROM IN VARCHAR2  
    , P_MKTTO IN VARCHAR2  
    , P_ACCTSRC IN VARCHAR2  
    , P_ACCTTYPFROM IN VARCHAR2  
    , P_ACCTTYPTO IN VARCHAR2  
    , P_SUBCHNNL IN VARCHAR2  
    , P_CHNNL IN VARCHAR2
    , P_IS_FAC IN VARCHAR2
    ) 
    RETURN PG_RPGE_LISTING.RPAC_OUTRANSTATE_T PIPELINED IS
          v_ProcName_v    VARCHAR2 (30) := 'FN_RPAC_OUTRANSTATE_MAST';
          r_out_tran PG_RPGE_LISTING.RPAC_OUTRANSTATE_REC;
          v_Select VARCHAR2(32767);
          TYPE RptCurTyp  IS REF CURSOR;
          v_rpt_cursor    RptCurTyp;
    BEGIN
    ---<<9.00 >> start
    
    /* Changes for 11.00 start*/
    IF('N'=P_IS_FAC) AND ('A'=P_ACCTSRC) THEN
        v_Select := 'SELECT * FROM( ';
    END IF;
    /*Changes for 11.00 end*/
    
   -- v_Select := 'SELECT ACST_MAST.AGENT_ID, ACST_MAST.ST_SEQ_NO, ACST_MAST.UKEY_MAST, ACST_MAST.ST_DOC, ACST_MAST.POL_NO, ACST_MAST.INSURED, ACST_MAST.EDATE, ACST_MAST.AGE_DAYS, ACST_MAST.NETT, ' -- comment by 11.00
    V_SELECT:=V_SELECT||'SELECT ACST_MAST.AGENT_ID, ACST_MAST.ST_SEQ_NO, ACST_MAST.UKEY_MAST, ACST_MAST.ST_DOC, ACST_MAST.POL_NO, ACST_MAST.INSURED, ACST_MAST.EDATE, ACST_MAST.AGE_DAYS, ACST_MAST.NETT, ' -- added by 11.00
                ||' ACST_MAST.MATCH_DOC, ACST_MAST.MATCH_TYPE, ACST_MAST.MATCH_AMT, ACST_MAST.BAL_AMT, ACST_MAST.PRODUCT_CONFIG_CODE, ACST_MAST.COMM1, ACST_MAST.COMM2, ACST_MAST.GROSS, '
                ||' ACST_MAST.AGENT_CODE,  '
                ||' ACST_MAST.ST_AMT, ACST_MAST.GL_SEQ_NO, ACST_MAST.ST_TYPE, ACST_MAST.MATCH_AGENT_ID, ACST_MAST.MATCH_GL_SEQ_NO, '
                ||' ACST_MAST.MATCH_ST_SEQ_NO, ACST_MAST.AGENT_CAT_TYPE, ACST_MAST.CLM_NO AS CLAIM_NO, '
                ||' (CASE '
                ||' WHEN (ACST_MAST.AGENT_CAT_TYPE =''CL'' AND SUBSTR(ACST_MAST.PRODUCT_CONFIG_CODE,0,2) =''08'') '
                ||' THEN (SELECT CLMT_PYMT.CLM_NO FROM CLMT_PYMT WHERE CLMT_PYMT.PYMT_NO = ACST_MAST.ST_DOC AND ACST_MAST.AGENT_CAT_TYPE =''CL'' AND SUBSTR(ACST_MAST.PRODUCT_CONFIG_CODE,0,2) =''08'') '
                ||' WHEN (ACST_MAST.AGENT_CAT_TYPE =''CL'' AND SUBSTR(ACST_MAST.PRODUCT_CONFIG_CODE, 0,2) <>''08'') '
                ||' THEN (SELECT CLNM_PYMT.CLM_NO FROM CLNM_PYMT WHERE CLNM_PYMT.PYMT_NO = ACST_MAST.ST_DOC AND ACST_MAST.AGENT_CAT_TYPE =''CL'' AND SUBSTR(ACST_MAST.PRODUCT_CONFIG_CODE,0,2) <>''08'') '
                ||' WHEN (ACST_MAST.AGENT_CAT_TYPE IN (''RI'',''CI'')) '
                ||' THEN (SELECT I.INW_POL_NO FROM UWGE_POLICY_INW I LEFT JOIN OCP_POLICY_BASES B ON I.CONTRACT_ID=B.CONTRACT_ID WHERE B.POLICY_REF=ACST_MAST.POL_NO AND B.TOP_INDICATOR=''Y'' AND I.TOP_INDICATOR=''Y'') '
                ||' ELSE '
                ||' '''' '
                ||' END) AS CLM_NO, ACST_MAST.TRAN_DATE, ACST_MAST.CNOTE_NO, ACST_MAST.ST_DESC, ACST_MAST.GST_COMM, ACST_MAST.GST_COMM2, ACST_MAST.BAL_AMT_PREM, ACST_MAST.BAL_AMT_COMM, ACST_MAST.GST,  ' --26.00 --28.00
              --  ||' END) AS CLM_NO, ACST_MAST.TRAN_DATE, ACST_MAST.CNOTE_NO, ACST_MAST.ST_DESC || CUSTOMER.PG_SOA_UTILS.FN_GET_PTV(ACST_MAST.ST_DOC,ACST_MAST.AGENT_CAT_TYPE,ACST_MAST.ST_TYPE), ACST_MAST.GST_COMM, ACST_MAST.GST_COMM2, ACST_MAST.BAL_AMT_PREM, ACST_MAST.BAL_AMT_COMM, ACST_MAST.GST,  ' -- 26.00 --28.00
                ||' ACST_MAST.VEH_NO,(SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE IN (''BG_PRODUCT'',''EG_PRODUCT'',''FI_PRODUCT'',''LB_PRODUCT'',''MA_PRODUCT'',''MS_PRODUCT'',''MT_PRODUCT'',''PL_PRODUCT'',''PP_PRODUCT'',''WM_PRODUCT'') AND CODE_CD = ACST_MAST.PRODUCT_CONFIG_CODE) SUBCLS_DESCP, '
                ||' P.NAME AS AGENT_NAME, ADDR.ADDRESS_LINE1, ADDR.ADDRESS_LINE2, ADDR.ADDRESS_LINE3, ADDR.POSTCODE,  '
                ||' (SELECT DESCRIPTION FROM CMGE_POSTCODE WHERE CMGE_POSTCODE.POSTCODE = ADDR.POSTCODE) AS POST_DESCP,  '
                ||' (SELECT ge.CODE_DESC FROM CMGE_POSTCODE ps, CMGE_CODE ge WHERE ge.CAT_CODE = ''STATE'' AND ge.CODE_CD = ps.STATE AND ps.POSTCODE = ADDR.POSTCODE) AS STATE_DESCP ';
                ---<<9.00 >> end
    if('Y'=P_IS_FAC) then
              V_SELECT:=V_SELECT||' ,(select HEADER_DESCP from cmge_coderel_hdr where cat_code= ''CHNL'' and HEADER_CODE=P.CHANNEL) AS CHANNEL_DESCP '
                                ||' ,(select DETAIL_DESCP from cmge_coderel_det where cat_code= ''CHNL'' and DETAIL_CODE =P.SUBCHANNEL) AS SUBCHANNEL_DESCP, '
                                ||' ( NVL(NVL((select uwge_policy_banca.BANK_BRANCH_CODE || ''-'' ||cmuw_bankbranch.BANK_BRANCH_DESCP '
||' from OCP_POLICY_BASES , uwge_policy_banca, cmuw_bankbranch '
||' WHERE ACST_MAST.POL_NO = OCP_POLICY_BASES.policy_ref '
||' AND OCP_POLICY_BASES.CONTRACT_ID = uwge_policy_banca.CONTRACT_ID'
||' AND uwge_policy_banca.BANK_TYPE =cmuw_bankbranch.BANK_CODE'
||' AND uwge_policy_banca.BANK_BRANCH_CODE = cmuw_bankbranch.BANK_BRANCH_CODE'
||' AND uwge_policy_banca.BANK_CHANNEL = cmuw_bankbranch.BANK_CHANNEL'
||' AND uwge_policy_banca.BANK_SUB_CHANNEL = cmuw_bankbranch.BANK_SUB_CHANNEL'
||' and rownum = 1),(select uwge_policy_banca.BANK_BRANCH_CODE|| ''-'' || cmuw_bankbranch.BANK_BRANCH_DESCP '
||' from UWGE_POLICY_VERSIONS , uwge_policy_banca, cmuw_bankbranch'
||' WHERE ACST_MAST.ST_DOC = UWGE_POLICY_VERSIONS.ENDT_NO'
||' AND UWGE_POLICY_VERSIONS.CONTRACT_ID = uwge_policy_banca.CONTRACT_ID'
||' AND uwge_policy_banca.BANK_TYPE =cmuw_bankbranch.BANK_CODE'
||' AND uwge_policy_banca.BANK_BRANCH_CODE = cmuw_bankbranch.BANK_BRANCH_CODE'
||' AND uwge_policy_banca.BANK_CHANNEL = cmuw_bankbranch.BANK_CHANNEL'
||' AND uwge_policy_banca.BANK_SUB_CHANNEL = cmuw_bankbranch.BANK_SUB_CHANNEL AND ROWNUM = 1)),''''))  AS BRANCH_NAME '
-- CMDM_BRANCH.BRANCH_NAME '
||' , CUSTOMER.PG_SOA_UTILS.FN_GET_PTV(ACST_MAST.ST_DOC,ACST_MAST.AGENT_CAT_TYPE,ACST_MAST.ST_TYPE) AS PTV_DESC ' --28.00
                                ||' FROM  ACST_MAST LEFT OUTER JOIN DMAG_VI_AGENT P ON ACST_MAST.AGENT_CODE = P.AGENTCODE '
                                ||' LEFT OUTER JOIN CMDM_BRANCH ON P.BRANCH_CODE=CMDM_BRANCH.BRANCH_CODE      '
                                ||' LEFT OUTER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (P.OFF_ADD_ID, P.OFF_ADD_VERSION)) ADDR  '
                                ||' ON P.OFF_ADD_ID = ADDR.ADD_ID AND P.OFF_ADD_VERSION = ADDR.VERSION  '
                                ||' LEFT OUTER JOIN RIAG_SOURCE ON ACST_MAST.AGENT_CODE = RIAG_SOURCE.RI_CODE WHERE 1=1 ';
                                
        if(P_AGENTIDFROM is not null and P_AGENTIDTO is not null) then
                v_Select := v_Select||' AND ACST_MAST.AGENT_ID between '''||P_AGENTIDFROM||''' and '''||P_AGENTIDTO||'''';
            end if;        
            if(P_BRANCHCODE is not null) then
              v_Select := v_Select||' AND P.BRANCH_CODE='''||P_BRANCHCODE||'''';
            end if;
            if(P_CHNNL is not null) then
              v_Select := v_Select||' AND P.CHANNEL='''||P_CHNNL||'''';
            end if;
            if(P_SUBCHNNL is not null) then
              v_Select := v_Select||' AND P.SUBCHANNEL='''||P_SUBCHNNL||'''';
            end if;
            if(P_EDATEFROM is not null and P_EDATETO is not null) then
              v_Select := v_Select||' AND ACST_MAST.EDATE BETWEEN TO_DATE ('''||P_EDATEFROM||''',''DD/MM/YYYY'') AND TO_DATE ('''||P_EDATETO||''',''DD/MM/YYYY'') ';
            end if;
            if(P_AGEFROM is not null and P_AGETO is not null) then
              v_Select := v_Select||' AND ACST_MAST.AGE_DAYS  BETWEEN '''||P_AGEFROM||''' AND '''||P_AGETO||'''';
            end if;
            
            if('A' = P_SORTBY ) then
                v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 order by ACST_MAST.AGE_DAYS desc, ACST_MAST.AGENT_ID asc, ACST_MAST.ST_SEQ_NO asc ';            
            elsif ('C' = P_SORTBY) then
                v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 order by ACST_MAST.PRODUCT_CONFIG_CODE desc, ACST_MAST.AGENT_ID asc, ACST_MAST.ST_SEQ_NO ascc ';    
            elsif('CO' = P_SORTBY) then
                v_Select := v_Select||' AND (ACST_MAST.COMM1+ACST_MAST.COMM2) <> 0 and ACST_MAST.MATCH_AMT <> 0 and (((ACST_MAST.BAL_AMT > 0) AND ((ACST_MAST.ST_AMT< 0) OR (ACST_MAST.ST_AMT > 0))) OR ((ACST_MAST.BAL_AMT <0) and ((ACST_MAST.ST_AMT < 0) OR (ACST_MAST.ST_AMT > 0)))) '
                  ||' order by (ACST_MAST.COMM1+ACST_MAST.COMM2) desc, ACST_MAST.AGENT_ID asc, ACST_MAST.ST_SEQ_NO asc ';
            elsif('P' = P_SORTBY) then
                v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 and RIAG_SOURCE.SOURCE_ID in (''3-F01'',''4-F01'',''5-F01'',''6-F01'',''7-F01'',''3-C01'',''4-C01'',''5-C01'') order by ACST_MAST.POL_NO , ACST_MAST.AGENT_ID asc, ACST_MAST.ST_SEQ_NO asc ';            
            elsif('I' = P_SORTBY) then
                v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 order by ACST_MAST.INSURED, ACST_MAST.AGENT_ID, ACST_MAST.ST_SEQ_NO ';            
            end if;
            open  v_rpt_cursor for v_Select;
            loop
              fetch v_rpt_cursor into r_out_tran.AGENT_ID, r_out_tran.ST_SEQ_NO, r_out_tran.UKEY_MAST, r_out_tran.ST_DOC, 
                                      r_out_tran.POL_NO, r_out_tran.INSURED, r_out_tran.EDATE, r_out_tran.AGE_DAYS, r_out_tran.NETT, r_out_tran.MATCH_DOC, r_out_tran.MATCH_TYPE, 
                                      r_out_tran.MATCH_AMT, r_out_tran.BAL_AMT, r_out_tran.PRODUCT_CONFIG_CODE, r_out_tran.COMM1, r_out_tran.COMM2, r_out_tran.GROSS, 
                                      r_out_tran.AGENT_CODE, r_out_tran.ST_AMT, r_out_tran.GL_SEQ_NO, r_out_tran.ST_TYPE, r_out_tran.MATCH_AGENT_ID, r_out_tran.MATCH_GL_SEQ_NO, 
                                      r_out_tran.MATCH_ST_SEQ_NO, r_out_tran.AGENT_CAT_TYPE, r_out_tran.CLAIM_NO, r_out_tran.CLM_NO, r_out_tran.TRAN_DATE, r_out_tran.CNOTE_NO, 
                                      r_out_tran.ST_DESC, r_out_tran.GST_COMM, r_out_tran.GST_COMM2, r_out_tran.BAL_AMT_PREM, r_out_tran.BAL_AMT_COMM, r_out_tran.GST,  
                                      r_out_tran.VEH_NO, r_out_tran.SUBCLS_DESCP, r_out_tran.AGENT_NAME,r_out_tran.ADDRESS_LINE1, r_out_tran.ADDRESS_LINE2, r_out_tran.ADDRESS_LINE3, r_out_tran.POSTCODE,
                                      r_out_tran.POST_DESCP, r_out_tran.STATE_DESCP, r_out_tran.CHANNEL_DESCP, r_out_tran.SUBCHANNEL_DESCP,r_out_tran.BRANCH_NAME
                                      ,r_out_tran.PTV_DESC;--28.00
              exit when v_rpt_cursor%NOTFOUND;
              pipe row (r_out_tran);
            end loop;
    else  -- P_IS_FAC = N
      if('A'=P_ACCTSRC) then
          v_Select:=v_Select||',(select HEADER_DESCP from cmge_coderel_hdr where cat_code= ''CHNL'' and HEADER_CODE=P.CHANNEL) AS CHANNEL_DESCP '
                       ||' ,(select DETAIL_DESCP from cmge_coderel_det where cat_code= ''CHNL'' and DETAIL_CODE =P.SUBCHANNEL) AS SUBCHANNEL_DESCP '
                        ||' ,( NVL(NVL((select uwge_policy_banca.BANK_BRANCH_CODE || ''-'' ||cmuw_bankbranch.BANK_BRANCH_DESCP '
||' from OCP_POLICY_BASES , uwge_policy_banca, cmuw_bankbranch '
||' WHERE ACST_MAST.POL_NO = OCP_POLICY_BASES.policy_ref '
||' AND OCP_POLICY_BASES.CONTRACT_ID = uwge_policy_banca.CONTRACT_ID'
||' AND uwge_policy_banca.BANK_TYPE =cmuw_bankbranch.BANK_CODE'
||' AND uwge_policy_banca.BANK_BRANCH_CODE = cmuw_bankbranch.BANK_BRANCH_CODE'
||' AND uwge_policy_banca.BANK_CHANNEL = cmuw_bankbranch.BANK_CHANNEL'
||' AND uwge_policy_banca.BANK_SUB_CHANNEL = cmuw_bankbranch.BANK_SUB_CHANNEL'
||' and rownum = 1),(select uwge_policy_banca.BANK_BRANCH_CODE|| ''-'' || cmuw_bankbranch.BANK_BRANCH_DESCP '
||' from UWGE_POLICY_VERSIONS , uwge_policy_banca, cmuw_bankbranch'
||' WHERE ACST_MAST.ST_DOC = UWGE_POLICY_VERSIONS.ENDT_NO'
||' AND UWGE_POLICY_VERSIONS.CONTRACT_ID = uwge_policy_banca.CONTRACT_ID'
||' AND uwge_policy_banca.BANK_TYPE =cmuw_bankbranch.BANK_CODE'
||' AND uwge_policy_banca.BANK_BRANCH_CODE = cmuw_bankbranch.BANK_BRANCH_CODE'
||' AND uwge_policy_banca.BANK_CHANNEL = cmuw_bankbranch.BANK_CHANNEL'
||' AND uwge_policy_banca.BANK_SUB_CHANNEL = cmuw_bankbranch.BANK_SUB_CHANNEL AND ROWNUM = 1)),''''))  AS BRANCH_NAME ,'
                        ||' DMAG_AGENTS.SERVICED_BY '
                ||' , CUSTOMER.PG_SOA_UTILS.FN_GET_PTV(ACST_MAST.ST_DOC,ACST_MAST.AGENT_CAT_TYPE,ACST_MAST.ST_TYPE) AS PTV_DESC ' --28.00
                       ||' FROM  ACST_MAST '
                       ||' LEFT JOIN  DMAG_VI_DIR_INW P ON ACST_MAST.AGENT_CODE = P.AGENTCODE '
                       ||' LEFT OUTER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (P.ADDR_ID, P.ADDR_VERSION)) ADDR  ON P.ADDR_ID = ADDR.ADD_ID AND P.ADDR_VERSION = ADDR.VERSION '
                       ||' LEFT OUTER JOIN DMAG_AGENTS ON DMAG_AGENTS.INT_ID = P.INT_ID '
                       ||' LEFT OUTER JOIN  CMDM_BRANCH ON P.BRANCH_CODE=CMDM_BRANCH.BRANCH_CODE WHERE 1=1 ';
        if(P_BRANCHCODE is not null) then
          v_Select := v_Select||' AND P.BRANCH_CODE='''||P_BRANCHCODE||'''';
          end if; 
          
            else  -- P_ACCTSRC <> A
          --<5.00 start>
          select replace(v_Select,'P.NAME',' (SELECT CP.NAME_EXT  FROM CPGE_VI_PARTNERS_ALL CP WHERE CP.PART_ID = P.PART_ID AND CP.VERSION = P.PART_VERSION)  ') INTO v_Select FROM DUAL ;--<5.00 end>
          v_Select:=v_Select||' ,( NVL(NVL((select uwge_policy_banca.BANK_BRANCH_CODE || ''-'' ||cmuw_bankbranch.BANK_BRANCH_DESCP '
                        ||' from OCP_POLICY_BASES , uwge_policy_banca, cmuw_bankbranch '
                        ||' WHERE ACST_MAST.POL_NO = OCP_POLICY_BASES.policy_ref '
                        ||' AND OCP_POLICY_BASES.CONTRACT_ID = uwge_policy_banca.CONTRACT_ID'
                        ||' AND uwge_policy_banca.BANK_TYPE =cmuw_bankbranch.BANK_CODE'
                        ||' AND uwge_policy_banca.BANK_BRANCH_CODE = cmuw_bankbranch.BANK_BRANCH_CODE'
                        ||' AND uwge_policy_banca.BANK_CHANNEL = cmuw_bankbranch.BANK_CHANNEL'
                        ||' AND uwge_policy_banca.BANK_SUB_CHANNEL = cmuw_bankbranch.BANK_SUB_CHANNEL'
                        ||' and rownum = 1),(select uwge_policy_banca.BANK_BRANCH_CODE|| ''-'' || cmuw_bankbranch.BANK_BRANCH_DESCP '
                        ||' from UWGE_POLICY_VERSIONS , uwge_policy_banca, cmuw_bankbranch'
                        ||' WHERE ACST_MAST.ST_DOC = UWGE_POLICY_VERSIONS.ENDT_NO'
                        ||' AND UWGE_POLICY_VERSIONS.CONTRACT_ID = uwge_policy_banca.CONTRACT_ID'
                        ||' AND uwge_policy_banca.BANK_TYPE =cmuw_bankbranch.BANK_CODE'
                        ||' AND uwge_policy_banca.BANK_BRANCH_CODE = cmuw_bankbranch.BANK_BRANCH_CODE'
                        ||' AND uwge_policy_banca.BANK_CHANNEL = cmuw_bankbranch.BANK_CHANNEL'
                        ||' AND uwge_policy_banca.BANK_SUB_CHANNEL = cmuw_bankbranch.BANK_SUB_CHANNEL AND ROWNUM = 1)),''''))  AS BRANCH_NAME '
                        ||' ,CUSTOMER.PG_SOA_UTILS.FN_GET_PTV(ACST_MAST.ST_DOC,ACST_MAST.AGENT_CAT_TYPE,ACST_MAST.ST_TYPE) AS PTV_DESC ' --28.00
          --', NVL( select cmuw_bankbranch.BANK_BRANCH_DESCP from OCP_POLICY_BASES, uwge_policy_banca, cmuw_bankbranch WHERE ACST_MAST.POL_NO = OCP_POLICY_BASES.POLICY_REF '
                                --||' AND OCP_POLICY_BASES.CONTRACT_ID = UWGE_POLICY_BANCA.CONTRACT_ID AND UWGE_POLICY_BANCA.BANK_TYPE =CMUW_BANKBRANCH.BANK_CODE AND UWGE_POLICY_BANCA.BANK_BRANCH_CODE = CMUW_BANKBRANCH.BANK_BRANCH_CODE '
                                --||' AND uwge_policy_banca.BANK_CHANNEL = cmuw_bankbranch.BANK_CHANNEL AND UWGE_POLICY_BANCA.BANK_SUB_CHANNEL = CMUW_BANKBRANCH.BANK_SUB_CHANNEL, '''')  AS BRANCH_NAME '
                       ||' FROM  ACST_MAST '
                       ||' LEFT OUTER JOIN RIAG_PROFILE P ON ACST_MAST.AGENT_CODE = P.RI_CODE ' --#20.00 table name changed to DMAG_VI_DIR_INW  /* reverted 20.00 changes 22.00*/
                       --||' LEFT OUTER JOIN (SELECT RI_CODE AS AGENTCODE, ADDR_ID,ADDR_VERSION, BRANCH AS BRANCH_CODE, PART_ID, PART_VERSION FROM RIAG_PROFILE UNION ALL ' --start 21.00 union with rige_profile table --commneted22.00
                      -- ||' SELECT  AGENTCODE, ADDR_ID,ADDR_VERSION, BRANCH_CODE, PART_ID, PART_VERSION FROM DMAG_VI_DIR_INW ) P ON ACST_MAST.AGENT_CODE = P.AGENTCODE ' --end  21.00 --commneted22 22.00
                       ||' LEFT OUTER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (P.ADDR_ID, P.ADDR_VERSION)) ADDR  ON P.ADDR_ID = ADDR.ADD_ID AND P.ADDR_VERSION = ADDR.VERSION '
                       ||' LEFT OUTER JOIN  CMDM_BRANCH ON P.BRANCH=CMDM_BRANCH.BRANCH_CODE WHERE 1=1 '; -- modified to branch_code by 20.00 /* reverted 20.00 changes 22.00*/
        if(P_BRANCHCODE is not null) then
          v_Select := v_Select||' AND P.BRANCH='''||P_BRANCHCODE||''''; -- modified to branch_code by 20.00 /* reverted 20.00 changes 22.00*/
         end if; 
          
            end if; -- P_ACCTSRC END
      
      if(P_ACCTTYPFROM is not null and P_ACCTTYPTO is not null) then
          v_Select := v_Select||' AND ACST_MAST.AGENT_CAT_TYPE BETWEEN '''||P_ACCTTYPFROM||''' and '''||P_ACCTTYPTO||'''';
      end if;
      if(P_AGENTIDFROM is not null and P_AGENTIDTO is not null) then
          v_Select := v_Select||' AND ACST_MAST.AGENT_CODE BETWEEN '''||P_AGENTIDFROM||''' AND '''||P_AGENTIDTO||'''';
      end if;
           
      if(P_CHNNL is not null) then
          v_Select := v_Select||' AND P.CHANNEL='''||P_CHNNL||'''';
      end if;
      if(P_SUBCHNNL is not null) then
          v_Select := v_Select||' AND P.SUBCHANNEL='''||P_SUBCHNNL||'''';
      end if;
      if(P_EDATEFROM is not null and P_EDATETO is not null) then
          v_Select := v_Select||' AND ACST_MAST.EDATE BETWEEN TO_DATE ('''||P_EDATEFROM||''',''DD/MM/YYYY'') AND TO_DATE ('''||P_EDATETO||''',''DD/MM/YYYY'') ';
      end if;
      if(P_AGEFROM is not null and P_AGETO is not null) then
          v_Select := v_Select||' AND ACST_MAST.AGE_DAYS  BETWEEN '''||P_AGEFROM||''' AND '''||P_AGETO||''''; -- modified by 20.00
      end if;
      if(P_MKTFROM is not null and P_MKTTO is not null) then
          v_Select := v_Select||' AND DMAG_AGENTS.SERVICED_BY IN (Select USER_ID from SASC_USER_PROFILE where USER_ID BETWEEN '''||P_MKTFROM||''' AND '''||P_MKTTO||''')' ;
      end if;
                
      if ('A' = P_ACCTSRC) then            
                        
                    if ('A' = P_SORTBY) then
                        --v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 '|| --commneted by 11.00
                        v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 AND (ACST_MAST.INST_IND = ''N'' OR ACST_MAST.INST_IND IS NULL) '; -- Added by 11.00
                            --' ORDER BY ACST_MAST.AGENT_ID, ACST_MAST.AGE_DAYS desc, ACST_MAST.AGENT_CODE asc, ACST_MAST.ST_SEQ_NO asc '; --commneted by 11.00
                    elsif('C' = P_SORTBY)then
                        --v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 '|| --commneted by 11.00
                        v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 AND (ACST_MAST.INST_IND = ''N'' OR ACST_MAST.INST_IND IS NULL) '; -- Added by 11.00
                            --' ORDER BY ACST_MAST.AGENT_ID, ACST_MAST.PRODUCT_CONFIG_CODE, ACST_MAST.AGENT_CODE asc, ACST_MAST.ST_SEQ_NO asc ';   -- commented by 11.00
                    elsif('CO' = P_SORTBY)then
                        --v_Select := v_Select||' AND (ACST_MAST.COMM1+ACST_MAST.COMM2) <> 0 and ACST_MAST.MATCH_AMT <> 0 and (((ACST_MAST.BAL_AMT > 0) AND ((ACST_MAST.ST_AMT< 0) OR (ACST_MAST.ST_AMT > 0))) OR ((ACST_MAST.BAL_AMT <0) and ((ACST_MAST.ST_AMT < 0) OR (ACST_MAST.ST_AMT > 0)))) '|| --commneted by 11.00
                        v_Select := v_Select||' AND (ACST_MAST.COMM1+ACST_MAST.COMM2) <> 0 and ACST_MAST.MATCH_AMT <> 0 and (((ACST_MAST.BAL_AMT > 0) AND ((ACST_MAST.ST_AMT< 0) OR (ACST_MAST.ST_AMT > 0))) OR ((ACST_MAST.BAL_AMT <0) and ((ACST_MAST.ST_AMT < 0) OR (ACST_MAST.ST_AMT > 0)))) AND ACST_MAST.INST_IND = ''N'' '; -- Added by 11.00
                            --' ORDER BY ACST_MAST.AGENT_ID,(ACST_MAST.COMM1+ACST_MAST.COMM2) desc, ACST_MAST.AGENT_CODE asc, ACST_MAST.ST_SEQ_NO asc '; -- commented by 11.00
                    elsif('P' = P_SORTBY)then
                        --v_Select := v_Select||' AND  ACST_MAST.BAL_AMT <> 0 order by  ACST_MAST.AGENT_ID, ACST_MAST.POL_NO , ACST_MAST.AGENT_CODE asc, ACST_MAST.ST_SEQ_NO asc ';  --commneted by 11.00          
                        v_Select := v_Select||' AND  ACST_MAST.BAL_AMT <> 0 AND (ACST_MAST.INST_IND = ''N'' OR ACST_MAST.INST_IND IS NULL) '; -- Added by 11.00   
                                        --'order by  ACST_MAST.AGENT_ID, ACST_MAST.POL_NO , ACST_MAST.AGENT_CODE asc, ACST_MAST.ST_SEQ_NO asc ';  --commented by 11.00
                    elsif('I' = P_SORTBY)then
                        --v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 order by  ACST_MAST.AGENT_ID,ACST_MAST.INSURED, ACST_MAST.AGENT_CODE asc, ACST_MAST.ST_SEQ_NO asc '; --commneted by 11.00          
                        v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 AND (ACST_MAST.INST_IND = ''N'' OR ACST_MAST.INST_IND IS NULL) '; -- added by 11.00
                                    --'order by  ACST_MAST.AGENT_ID,ACST_MAST.INSURED, ACST_MAST.AGENT_CODE asc, ACST_MAST.ST_SEQ_NO asc ';  -- commneted by 11.00       
                    end if;
                    /* Changes for 11.00 start*/
                        v_Select := v_Select||' UNION ALL SELECT ACST_INST.AGENT_ID, ACST_INST.ST_SEQ_NO,ACST_INST.UKEY_INST AS UKEY_MAST,ACST_INST.ST_DOC_INST AS ST_DOC, '
                                        ||' ACST_INST.POL_NO,ACST_INST.INSURED,ACST_INST.INST_DATE, ACST_INST.AGE_DAYS, ACST_INST.NETT, ACST_INST.MATCH_DOC,  '
                                        ||' ACST_INST.MATCH_TYPE, ACST_INST.MATCH_AMT, ACST_INST.BAL_AMT, ACST_INST.PRODUCT_CONFIG_CODE, '
                                        ||' ACST_INST.COMM1, ACST_INST.COMM2, ACST_INST.GROSS, ACST_INST.AGENT_CODE, ACST_INST.ST_AMT,  '
                                        ||' ACST_INST.GL_SEQ_NO, ACST_INST.ST_TYPE || ''I'', ACST_INST.MATCH_AGENT_ID, ACST_INST.MATCH_GL_SEQ_NO,  ' 
                                        ||' ACST_INST.MATCH_ST_SEQ_NO, ACST_INST.AGENT_CAT_TYPE, ACST_INST.CLM_NO AS CLAIM_NO, '
                                        ||' (CASE  WHEN (ACST_INST.AGENT_CAT_TYPE =''CL'' AND SUBSTR(ACST_INST.PRODUCT_CONFIG_CODE,0,2) =''08'')  THEN (SELECT CLMT_PYMT.CLM_NO FROM CLMT_PYMT WHERE CLMT_PYMT.PYMT_NO = ACST_INST.ST_DOC AND ACST_INST.AGENT_CAT_TYPE =''CL'' AND SUBSTR(ACST_INST.PRODUCT_CONFIG_CODE,0,2) =''08'')  WHEN (ACST_INST.AGENT_CAT_TYPE =''CL'' AND SUBSTR(ACST_INST.PRODUCT_CONFIG_CODE, 0,2) <>''08'')  THEN (SELECT CLNM_PYMT.CLM_NO FROM CLNM_PYMT WHERE CLNM_PYMT.PYMT_NO = ACST_INST.ST_DOC AND ACST_INST.AGENT_CAT_TYPE =''CL'' AND SUBSTR(ACST_INST.PRODUCT_CONFIG_CODE,0,2) <>''08'')  WHEN (ACST_INST.AGENT_CAT_TYPE IN (''RI'',''CI''))  THEN (SELECT I.INW_POL_NO FROM UWGE_POLICY_INW I LEFT JOIN OCP_POLICY_BASES B ON I.CONTRACT_ID=B.CONTRACT_ID WHERE B.POLICY_REF=ACST_INST.POL_NO AND B.TOP_INDICATOR=''Y'' AND I.TOP_INDICATOR=''Y'')  ELSE  ''''  END) AS CLM_NO,  '
                                        ||' ACST_INST.TRAN_DATE, ACST_INST.CNOTE_NO, ACST_INST.ST_DESC || '' ('' || ACST_INST.INST_CYCLE||''/''||CNT.CNT_ST_DOC||'')'' AS ST_DESC, ACST_INST.GST_COMM, ACST_INST.GST_COMM2,  ' --26.00 -28.00
                                        --||' ACST_INST.TRAN_DATE, ACST_INST.CNOTE_NO, ACST_INST.ST_DESC || '' ('' || ACST_INST.INST_CYCLE||''/''||CNT.CNT_ST_DOC||'')''|| CUSTOMER.PG_SOA_UTILS.FN_GET_PTV(ACST_INST.ST_DOC,ACST_INST.AGENT_CAT_TYPE,ACST_INST.ST_TYPE) AS ST_DESC, ACST_INST.GST_COMM, ACST_INST.GST_COMM2,  ' --26.00  --28.00
                                        ||' ACST_INST.BAL_AMT_PREM, ACST_INST.BAL_AMT_COMM, ACST_INST.GST, ACST_INST.VEH_NO, '
                                        ||' (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE IN (''BG_PRODUCT'',''EG_PRODUCT'',''FI_PRODUCT'',''LB_PRODUCT'',''MA_PRODUCT'',''MS_PRODUCT'',''MT_PRODUCT'',''PL_PRODUCT'',''PP_PRODUCT'',''WM_PRODUCT'') AND CODE_CD = ACST_INST.PRODUCT_CONFIG_CODE) SUBCLS_DESCP, '
                                        ||' P.NAME AS AGENT_NAME, ADDR.ADDRESS_LINE1, ADDR.ADDRESS_LINE2, ADDR.ADDRESS_LINE3, ADDR.POSTCODE,  '
                                        ||' (SELECT DESCRIPTION FROM CMGE_POSTCODE WHERE CMGE_POSTCODE.POSTCODE = ADDR.POSTCODE) AS POST_DESCP, '
                                        ||' (SELECT GE.CODE_DESC FROM CMGE_POSTCODE PS, CMGE_CODE GE WHERE GE.CAT_CODE = ''STATE'' AND GE.CODE_CD = PS.STATE AND PS.POSTCODE = ADDR.POSTCODE) AS STATE_DESCP , '
                                        ||' (SELECT HEADER_DESCP FROM CMGE_CODEREL_HDR WHERE CAT_CODE= ''CHNL'' AND HEADER_CODE=P.CHANNEL) AS CHANNEL_DESCP  , '
                                        ||' (SELECT DETAIL_DESCP FROM CMGE_CODEREL_DET WHERE CAT_CODE= ''CHNL'' AND DETAIL_CODE =P.SUBCHANNEL) AS SUBCHANNEL_DESCP  ,( NVL(NVL((SELECT UWGE_POLICY_BANCA.BANK_BRANCH_CODE || ''-'' ||CMUW_BANKBRANCH.BANK_BRANCH_DESCP  FROM OCP_POLICY_BASES , UWGE_POLICY_BANCA, CMUW_BANKBRANCH  WHERE ACST_INST.POL_NO = OCP_POLICY_BASES.POLICY_REF  AND OCP_POLICY_BASES.CONTRACT_ID = UWGE_POLICY_BANCA.CONTRACT_ID AND UWGE_POLICY_BANCA.BANK_TYPE =CMUW_BANKBRANCH.BANK_CODE AND UWGE_POLICY_BANCA.BANK_BRANCH_CODE = CMUW_BANKBRANCH.BANK_BRANCH_CODE AND UWGE_POLICY_BANCA.BANK_CHANNEL = CMUW_BANKBRANCH.BANK_CHANNEL AND UWGE_POLICY_BANCA.BANK_SUB_CHANNEL = CMUW_BANKBRANCH.BANK_SUB_CHANNEL AND ROWNUM = 1),(SELECT UWGE_POLICY_BANCA.BANK_BRANCH_CODE|| ''-'' || CMUW_BANKBRANCH.BANK_BRANCH_DESCP  FROM UWGE_POLICY_VERSIONS , UWGE_POLICY_BANCA, CMUW_BANKBRANCH WHERE ACST_INST.ST_DOC = UWGE_POLICY_VERSIONS.ENDT_NO AND UWGE_POLICY_VERSIONS.CONTRACT_ID = UWGE_POLICY_BANCA.CONTRACT_ID AND UWGE_POLICY_BANCA.BANK_TYPE =CMUW_BANKBRANCH.BANK_CODE AND UWGE_POLICY_BANCA.BANK_BRANCH_CODE = CMUW_BANKBRANCH.BANK_BRANCH_CODE AND UWGE_POLICY_BANCA.BANK_CHANNEL = CMUW_BANKBRANCH.BANK_CHANNEL AND UWGE_POLICY_BANCA.BANK_SUB_CHANNEL = CMUW_BANKBRANCH.BANK_SUB_CHANNEL AND ROWNUM = 1)),''''))  AS BRANCH_NAME , DMAG_AGENTS.SERVICED_BY  '
                                        ||' , CUSTOMER.PG_SOA_UTILS.FN_GET_PTV(ACST_INST.ST_DOC,ACST_INST.AGENT_CAT_TYPE,ACST_INST.ST_TYPE) AS PTV_DESC ' --28.00
                                        ||' FROM ACST_INST '
                                        ||' LEFT JOIN  DMAG_VI_DIR_INW P ON ACST_INST.AGENT_CODE = P.AGENTCODE   '
                                        ||' LEFT OUTER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (P.ADDR_ID, P.ADDR_VERSION)) ADDR  ON P.ADDR_ID = ADDR.ADD_ID AND P.ADDR_VERSION = ADDR.VERSION '
                                        ||' LEFT OUTER JOIN DMAG_AGENTS ON DMAG_AGENTS.INT_ID = P.INT_ID   '
                                       -- ||' LEFT JOIN  CMDM_BRANCH ON P.BRANCH_CODE=CMDM_BRANCH.BRANCH_CODE  '
                                        ||' LEFT OUTER JOIN (SELECT UPV.CONTRACT_ID, UPV.ENDT_CODE, UPV.ENDT_NO FROM UWGE_POLICY_VERSIONS UPV WHERE UPV.ENDT_CODE = ''96'') ENDT_CAN ON ENDT_CAN.ENDT_NO = ACST_INST.ST_DOC ' -- add
                                        ||' LEFT OUTER JOIN (SELECT UPV.CONTRACT_ID, UPV.ENDT_CODE, OCP.POLICY_REF FROM UWGE_POLICY_VERSIONS UPV, OCP_POLICY_BASES OCP WHERE UPV.CONTRACT_ID = OCP.CONTRACT_ID AND UPV.ENDT_CODE = ''96'' AND UPV.VERSION_NO <> 1) POL_CAN ON POL_CAN.POLICY_REF = ACST_INST.ST_DOC '
                                        ||' LEFT OUTER JOIN  (SELECT COUNT(A.ST_DOC) CNT_ST_DOC,A.ST_DOC FROM ACST_INST A GROUP BY A.ST_DOC) CNT ON CNT.ST_DOC =  ACST_INST.ST_DOC'
                                        ||' LEFT OUTER JOIN (SELECT COUNT (DOC_NO) AS CNT_DOC_NO, DOC, AGENT_ID FROM (SELECT ACST_MATCH.DOC_NO , ACGC_KO_INST.AGENT_ID, ACGC_KO_INST.DOC_NO ||'':''|| ACGC_KO_INST.INST_CYCLE AS DOC '
                                        ||' FROM ACGC_KO_INST JOIN ACST_MATCH '
                                        ||' ON ACST_MATCH.DOC_NO = ACGC_KO_INST.AC_NO '
                                        ||' AND ACST_MATCH.AGENT_CODE = ACGC_KO_INST.AGENT_CODE '
                                        ||' AND ACST_MATCH.AGENT_CAT_TYPE = ACGC_KO_INST.AGENT_CAT_TYPE '
                                        ||' AND ACST_MATCH.GL_SEQ_NO = ACGC_KO_INST.GL_SEQ_NO '
                                        ||' AND ACST_MATCH.MATCH_DOC_NO = ACGC_KO_INST.DOC_NO '
                                        ||' WHERE '
                                        ||' EXISTS( '
                                        ||' SELECT 1 FROM ACST_MAST WHERE (INST_IND = ''Y'') AND ST_DOC = ACST_MATCH.MATCH_DOC_NO))GROUP BY DOC,AGENT_ID) match ON match.AGENT_ID = ACST_INST.AGENT_ID AND  match.DOC = ACST_INST.ST_DOC_INST '
                                        ||' WHERE 1=1 '
                                        ||' AND ( '
                                        ||' ((ACST_INST.BAL_AMT_PREM <>0 OR ACST_INST.BAL_AMT_COMM <>0) ' -- 32.00 removed condition (ACST_INST.BAL_AMT <> 0) 
                                        ||' AND ACST_INST.INST_DATE < sysdate '
                                        ||' AND match.CNT_DOC_NO > 0 AND ((ENDT_CAN.ENDT_CODE IS NULL OR ENDT_CAN.ENDT_CODE <>''96'') OR (POL_CAN.ENDT_CODE IS NULL OR POL_CAN.ENDT_CODE <>''96'')) '
                                        ||' ) '
                                        ||' OR '
                                        ||' ((ACST_INST.BAL_AMT_PREM <>0 OR ACST_INST.BAL_AMT_COMM <>0) ' -- 32.00 removed condition (ACST_INST.BAL_AMT <> 0) 
                                        ||' AND ACST_INST.INST_DATE < sysdate AND match.CNT_DOC_NO > 0  AND ((ENDT_CAN.ENDT_CODE IS NULL OR ENDT_CAN.ENDT_CODE <>''96'') OR (POL_CAN.ENDT_CODE IS NULL OR POL_CAN.ENDT_CODE <>''96'')) '
                                        ||' ) '
                                        ||' OR '
                                        ||' ((ACST_INST.BAL_AMT_PREM <>0 OR ACST_INST.BAL_AMT_COMM <>0) ' -- 32.00 removed condition (ACST_INST.BAL_AMT <> 0) 
                                        ||' AND match.CNT_DOC_NO > 0 '  
                                        ||' AND ACST_INST.INST_DATE > sysdate) '
                                        ||' OR '
                                        ||' ((ACST_INST.BAL_AMT_PREM <>0 OR ACST_INST.BAL_AMT_COMM <>0) '  -- 32.00 removed condition (ACST_INST.BAL_AMT <> 0)  
                                        ||' AND (ENDT_CAN.ENDT_CODE =''96'' OR POL_CAN.ENDT_CODE = ''96'')) '
                                        ||' OR ' 
                                        ||' (ACST_INST.BAL_AMT = ACST_INST.ST_AMT AND ACST_INST.INST_DATE < sysdate) '
                                        ||' ) AND ACST_INST.BAL_AMT <> 0  ';  -- 32.00

                                         if(P_BRANCHCODE is not null) then
                                                v_Select := v_Select||' AND P.BRANCH_CODE='''||P_BRANCHCODE||'''';
                                         end if; 
                                         
                                         if(P_ACCTTYPFROM is not null and P_ACCTTYPTO is not null) then
                                              v_Select := v_Select||' AND ACST_INST.AGENT_CAT_TYPE BETWEEN '''||P_ACCTTYPFROM||''' and '''||P_ACCTTYPTO||'''';
                                          end if;
                                          if(P_AGENTIDFROM is not null and P_AGENTIDTO is not null) then
                                              v_Select := v_Select||' AND ACST_INST.AGENT_CODE BETWEEN '''||P_AGENTIDFROM||''' AND '''||P_AGENTIDTO||'''';
                                          end if;
                                               
                                          if(P_CHNNL is not null) then
                                              v_Select := v_Select||' AND P.CHANNEL='''||P_CHNNL||'''';
                                          end if;
                                          if(P_SUBCHNNL is not null) then
                                              v_Select := v_Select||' AND P.SUBCHANNEL='''||P_SUBCHNNL||'''';
                                          end if;
                                          if(P_EDATEFROM is not null and P_EDATETO is not null) then
                                              v_Select := v_Select||' AND ACST_INST.EDATE BETWEEN TO_DATE ('''||P_EDATEFROM||''',''DD/MM/YYYY'') AND TO_DATE ('''||P_EDATETO||''',''DD/MM/YYYY'') ';
                                          end if;
                                          if(P_AGEFROM is not null and P_AGETO is not null) then
                                              v_Select := v_Select||' AND ACST_INST.AGE_DAYS  BETWEEN '''||P_AGEFROM||' AND '''||P_AGETO||'''';
                                          end if;
                                          if(P_MKTFROM is not null and P_MKTTO is not null) then
                                              v_Select := v_Select||' AND DMAG_AGENTS.SERVICED_BY IN (Select USER_ID from SASC_USER_PROFILE where USER_ID BETWEEN '''||P_MKTFROM||''' AND '''||P_MKTTO||''')' ;
                                          end if;
                                          
        if ('A' = P_SORTBY) then
                                                                            v_Select := v_Select||') ORDER BY AGENT_ID, AGE_DAYS desc, AGENT_CODE asc, ST_SEQ_NO asc '; 
        elsif('C' = P_SORTBY)then
                                                                            v_Select := v_Select||') ORDER BY AGENT_ID, PRODUCT_CONFIG_CODE, AGENT_CODE asc, ST_SEQ_NO asc '; 
        elsif('CO' = P_SORTBY)then
                                                                            v_Select := v_Select||') ORDER BY AGENT_ID,(COMM1+COMM2) desc, AGENT_CODE asc, ST_SEQ_NO asc '; 
        elsif('P' = P_SORTBY)then
                                                                            v_Select := v_Select||') ORDER BY  AGENT_ID, POL_NO , AGENT_CODE asc, ST_SEQ_NO asc '; 
        elsif('I' = P_SORTBY)then
                                                                            v_Select := v_Select||') ORDER BY  AGENT_ID,INSURED, AGENT_CODE asc, ST_SEQ_NO asc ';
        end if;
                        /* Changes for 11.00 end*/
                        
      else
        if ('A' = P_SORTBY) then
            v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 order by  ACST_MAST.AGE_DAYS desc, P.RI_CODE asc, ACST_MAST.ST_SEQ_NO asc ';  -- modified to agentcode by 20.00   /* reverted 20.00 changes 22.00*/     
        elsif('C' = P_SORTBY)then
            v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 order by ACST_MAST.PRODUCT_CONFIG_CODE, P.RI_CODE, ACST_MAST.ST_SEQ_NO asc '; -- modified to agentcode by 20.00           /* reverted 20.00 changes 22.00*/
        elsif('CO' = P_SORTBY)then
            v_Select := v_Select||' AND (ACST_MAST.COMM1+ACST_MAST.COMM2) <> 0 and ACST_MAST.MATCH_AMT <> 0 and (((ACST_MAST.BAL_AMT > 0) AND ((ACST_MAST.ST_AMT< 0) OR (ACST_MAST.ST_AMT > 0))) OR ((ACST_MAST.BAL_AMT <0) and ((ACST_MAST.ST_AMT < 0) OR (ACST_MAST.ST_AMT > 0)))) '
                        ||' order by (ACST_MAST.COMM1+ACST_MAST.COMM2) desc, P.RI_CODE asc, ACST_MAST.ST_SEQ_NO asc '; -- modified to agentcode by 20.00 /* reverted 20.00 changes 22.00*/
        elsif('P' = P_SORTBY)then
            v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 order by ACST_MAST.POL_NO , P.RI_CODE, ACST_MAST.ST_SEQ_NO asc ';  -- modified to agentcode by 20.00     /* reverted 20.00 changes 22.00*/       
        elsif('I' = P_SORTBY)then
            v_Select := v_Select||' AND ACST_MAST.BAL_AMT <> 0 order by ACST_MAST.INSURED, P.RI_CODE, ACST_MAST.ST_SEQ_NO asc '; -- modified to agentcode by 20.00     /* reverted 20.00 changes 22.00*/       
        end if;
      end if;
          
         
      dbms_output.put_line('v_Select Query - ' || v_Select);
      if('A'=P_ACCTSRC) then
      open  v_rpt_cursor for v_Select;
            loop
              fetch v_rpt_cursor into r_out_tran.AGENT_ID, r_out_tran.ST_SEQ_NO, r_out_tran.UKEY_MAST, r_out_tran.ST_DOC, 
                                      r_out_tran.POL_NO, r_out_tran.INSURED, r_out_tran.EDATE, r_out_tran.AGE_DAYS, r_out_tran.NETT, r_out_tran.MATCH_DOC, r_out_tran.MATCH_TYPE, 
                                      r_out_tran.MATCH_AMT, r_out_tran.BAL_AMT, r_out_tran.PRODUCT_CONFIG_CODE, r_out_tran.COMM1, r_out_tran.COMM2, r_out_tran.GROSS, 
                                      r_out_tran.AGENT_CODE, r_out_tran.ST_AMT, r_out_tran.GL_SEQ_NO, r_out_tran.ST_TYPE, r_out_tran.MATCH_AGENT_ID, r_out_tran.MATCH_GL_SEQ_NO, 
                                      r_out_tran.MATCH_ST_SEQ_NO, r_out_tran.AGENT_CAT_TYPE, r_out_tran.CLAIM_NO, r_out_tran.CLM_NO, r_out_tran.TRAN_DATE, r_out_tran.CNOTE_NO, 
                                      r_out_tran.ST_DESC, r_out_tran.GST_COMM, r_out_tran.GST_COMM2, r_out_tran.BAL_AMT_PREM, r_out_tran.BAL_AMT_COMM, r_out_tran.GST,  
                                      r_out_tran.VEH_NO, r_out_tran.SUBCLS_DESCP, r_out_tran.AGENT_NAME,r_out_tran.ADDRESS_LINE1, r_out_tran.ADDRESS_LINE2, r_out_tran.ADDRESS_LINE3, r_out_tran.POSTCODE,
                                      r_out_tran.POST_DESCP, r_out_tran.STATE_DESCP, r_out_tran.CHANNEL_DESCP, r_out_tran.SUBCHANNEL_DESCP,r_out_tran.BRANCH_NAME, r_out_tran.SERVICED_BY
                                      , r_out_tran.PTV_DESC; --28.00
                                      
              exit when v_rpt_cursor%NOTFOUND;
              pipe row (r_out_tran);
            end loop;
      else
      open  v_rpt_cursor for v_Select;
            loop
              fetch v_rpt_cursor into r_out_tran.AGENT_ID, r_out_tran.ST_SEQ_NO, r_out_tran.UKEY_MAST, r_out_tran.ST_DOC, 
                                      r_out_tran.POL_NO, r_out_tran.INSURED, r_out_tran.EDATE, r_out_tran.AGE_DAYS, r_out_tran.NETT, r_out_tran.MATCH_DOC, r_out_tran.MATCH_TYPE, 
                                      r_out_tran.MATCH_AMT, r_out_tran.BAL_AMT, r_out_tran.PRODUCT_CONFIG_CODE, r_out_tran.COMM1, r_out_tran.COMM2, r_out_tran.GROSS, 
                                      r_out_tran.AGENT_CODE, r_out_tran.ST_AMT, r_out_tran.GL_SEQ_NO, r_out_tran.ST_TYPE, r_out_tran.MATCH_AGENT_ID, r_out_tran.MATCH_GL_SEQ_NO, 
                                      r_out_tran.MATCH_ST_SEQ_NO, r_out_tran.AGENT_CAT_TYPE, r_out_tran.CLAIM_NO, r_out_tran.CLM_NO, r_out_tran.TRAN_DATE, r_out_tran.CNOTE_NO, 
                                      r_out_tran.ST_DESC, r_out_tran.GST_COMM, r_out_tran.GST_COMM2, r_out_tran.BAL_AMT_PREM, r_out_tran.BAL_AMT_COMM, r_out_tran.GST,  
                                      r_out_tran.VEH_NO, r_out_tran.SUBCLS_DESCP, r_out_tran.AGENT_NAME,r_out_tran.ADDRESS_LINE1, r_out_tran.ADDRESS_LINE2, r_out_tran.ADDRESS_LINE3, r_out_tran.POSTCODE,
                                      r_out_tran.POST_DESCP, r_out_tran.STATE_DESCP,r_out_tran.BRANCH_NAME
                                      , r_out_tran.PTV_DESC; --28.00
              exit when v_rpt_cursor%NOTFOUND;
              pipe row (r_out_tran);
            end loop;
      end if;

    end if;   -- P_IS_FAC END
        
      RETURN ;
       EXCEPTION
          WHEN OTHERS
          THEN
             PG_UTIL_LOG_ERROR.PC_INS_log_error ( v_ProcName_v || '.' || v_ProcName_v, 1, SQLERRM);
    END FN_RPAC_OUTRANSTATE_MAST;

    FUNCTION FN_RPAC_OUTRANSTATE_DET 
    (
      P_ST_SEQ_NO IN NUMBER  
    , P_AGENT_ID IN VARCHAR2  
    , P_MATCH_DOC_NO IN VARCHAR2  
    ) RETURN PG_RPGE_LISTING.RPAC_OUTRANSTATE_T PIPELINED IS 
          v_ProcName_v    VARCHAR2 (30) := 'FN_RPAC_OUTRANSTATE_DET';
          r_out_tran PG_RPGE_LISTING.RPAC_OUTRANSTATE_REC;
    BEGIN
    FOR r IN (SELECT ACST_MATCH.TRAN_DATE, ACST_MATCH.MATCH_DOC_NO,  
              ACST_MATCH.MATCH_DOC_AMT, ACST_MATCH.MATCH_ST_SEQ_NO, ACST_MATCH.ST_SEQ_NO, ACST_MATCH.DOC_NO
              FROM  ACST_MATCH     WHERE ACST_MATCH.MATCH_ST_SEQ_NO = P_ST_SEQ_NO
                AND ACST_MATCH.MATCH_AGENT_ID = P_AGENT_ID AND ACST_MATCH.MATCH_DOC_NO = P_MATCH_DOC_NO
                 --start 12.00
                UNION ALL
                /* SELECT NULL as TRAN_DATE, ACGC_KO_INST.DOC_NO as MATCH_DOC_NO,
                ACGC_KO_INST.DOC_AMT AS MATCH_DOC_AMT, ACGC_KO_INST.DOC_GL_SEQ_NO AS MATCH_ST_SEQ_NO, ACGC_KO_INST.ST_SEQ_NO, ACGC_KO_INST.DOC_NO
                FROM ACGC_KO_INST, ACST_INST 
                WHERE ACST_INST.ST_DOC = ACGC_KO_INST.DOC_NO 
                AND ACST_INST.MATCH_DOC = ACGC_KO_INST.AC_NO 
                AND ACST_INST.INST_CYCLE = ACGC_KO_INST.INST_CYCLE 
                AND ACGC_KO_INST.DOC_GL_SEQ_NO = P_ST_SEQ_NO
                AND ACGC_KO_INST.AGENT_ID = P_AGENT_ID AND ACST_INST.ST_DOC_INST = P_MATCH_DOC_NO */                
                SELECT ACST_MATCH.TRAN_DATE, ACGC_KO_INST.DOC_NO ||':' ||ACGC_KO_INST.INST_CYCLE as MATCH_DOC_NO, 
                ACGC_KO_INST.DOC_AMT AS MATCH_DOC_AMT, ACST_MATCH.MATCH_ST_SEQ_NO AS MATCH_ST_SEQ_NO, ACST_MATCH.ST_SEQ_NO, ACST_MATCH.DOC_NO                
                FROM ACGC_KO_INST JOIN ACST_MATCH
                ON ACST_MATCH.DOC_NO = ACGC_KO_INST.AC_NO
                AND ACST_MATCH.AGENT_CODE = ACGC_KO_INST.AGENT_CODE
                AND ACST_MATCH.AGENT_CAT_TYPE = ACGC_KO_INST.AGENT_CAT_TYPE
                AND ACST_MATCH.MATCH_DOC_NO = ACGC_KO_INST.DOC_NO
                AND ACST_MATCH.GL_SEQ_NO = ACGC_KO_INST.GL_SEQ_NO
                WHERE ACGC_KO_INST.AGENT_ID =  P_AGENT_ID
                AND ACGC_KO_INST.DOC_NO ||':'|| ACGC_KO_INST.INST_CYCLE = P_MATCH_DOC_NO AND EXISTS(
                SELECT 1 FROM ACST_MAST WHERE (INST_IND = 'Y') AND ST_DOC = ACST_MATCH.MATCH_DOC_NO)
              ) -- end 12.00
          LOOP
          r_out_tran.MTC_DOC_NO := null;
          r_out_tran.MTC_MATCH_DOC_NO    := null;
          r_out_tran.MTC_ST_SEQ_NO    := null;
          r_out_tran.MTC_DOC_AMT    := null;
          r_out_tran.MTC_MATCH_ST_SEQ_NO    := null;
          r_out_tran.MTC_TRAN_DATE    := null;      
          r_out_tran.MTC_DOC_NO := r.DOC_NO;
          r_out_tran.MTC_MATCH_DOC_NO    := r.MATCH_DOC_NO;
          r_out_tran.MTC_ST_SEQ_NO    := r.ST_SEQ_NO;
          r_out_tran.MTC_DOC_AMT    := r.MATCH_DOC_AMT;
          r_out_tran.MTC_MATCH_ST_SEQ_NO    := r.MATCH_ST_SEQ_NO;
          r_out_tran.MTC_TRAN_DATE    := r.TRAN_DATE;
          pipe row (r_out_tran);
          END LOOP;
      RETURN ;
       EXCEPTION
          WHEN OTHERS
          THEN
             PG_UTIL_LOG_ERROR.PC_INS_log_error ( v_ProcName_v || '.' || v_ProcName_v, 1, SQLERRM);
             
             dbms_output.put_line('SQLERRM - ' || SQLERRM);
    END FN_RPAC_OUTRANSTATE_DET;
    
    FUNCTION fn_rcpt_trans_rpt (p_proc_year      VARCHAR2,
                                p_proc_mth          VARCHAR2,
                                p_src_id_from       VARCHAR2,
                                p_src_id_to         VARCHAR2,
                                p_agent_id_from     VARCHAR2,
                                p_agent_id_to       VARCHAR2,
                                p_branch_from       VARCHAR2,
                                p_branch_to         VARCHAR2,
                                p_tran_date_from    VARCHAR2,
                                p_tran_date_to      VARCHAR2,
                                p_batch_no_from     VARCHAR2,
                                p_batch_no_to       VARCHAR2)
       RETURN rcpt_trans_rpt_det_tab
       PIPELINED
    IS
       v_ProcName_v   VARCHAR2 (30) := 'fn_rcpt_trans_rpt';
       v_Step_v       VARCHAR2 (5) := '000';
       r_row          rcpt_trans_rpt_det;
    BEGIN
       FOR r
          IN (  SELECT DISTINCT
                       ACRC_RCPT.AGENT_ID,
                       DMAG_VI_AGENT.NAME,
                       ACRC_RCPT.GST_TAX_INV_REFNO
                  FROM ACRC_RCPT
                       LEFT OUTER JOIN
                       DMAG_VI_AGCAT_SOURCE
                          ON     SUBSTR (ACRC_RCPT.AGENT_ID, 0, 7) =
                                    DMAG_VI_AGCAT_SOURCE.AGENT_CODE
                             AND SUBSTR (ACRC_RCPT.AGENT_ID, 8, 2) =
                                    DMAG_VI_AGCAT_SOURCE.AGENT_CAT_TYPE
                       LEFT OUTER JOIN DMAG_VI_AGENT
                          ON ACRC_RCPT.AGENT_CODE = DMAG_VI_AGENT.AGENTCODE
                 WHERE     ACRC_RCPT.PROC_MTH = p_proc_mth
                       AND ACRC_RCPT.PROC_YR = p_proc_year
                       AND (   (p_src_id_from IS NULL AND p_src_id_to IS NULL)
                            OR DMAG_VI_AGCAT_SOURCE.SOURCE_ID BETWEEN p_src_id_from
                                                                  AND p_src_id_to)
                       AND (   (p_agent_id_from IS NULL AND p_agent_id_to IS NULL)
                            OR ACRC_RCPT.AGENT_ID BETWEEN p_agent_id_from
                                                      AND p_agent_id_to)
                       AND (   (p_branch_from IS NULL AND p_branch_to IS NULL)
                            OR DMAG_VI_AGENT.BRANCH_CODE BETWEEN p_branch_from
                                                             AND p_branch_to)
                       --Updated for Redmine # 98399
                       AND (   (    p_batch_no_from IS NULL
                                      AND p_batch_no_to IS NULL)
                                  OR ACRC_RCPT.BATCH_NO BETWEEN p_batch_no_from
                                                            AND p_batch_no_to)
                       AND (   (    p_tran_date_from IS NULL
                                      AND p_tran_date_to IS NULL)
                                  OR TRUNC (ACRC_RCPT.TRAN_DATE) BETWEEN TO_DATE (
                                                                            p_tran_date_from,
                                                                            'DD/MM/YYYY')
                                                                     AND TO_DATE (
                                                                            p_tran_date_to,
                                                                            'DD/MM/YYYY'))                                                                
              ORDER BY ACRC_RCPT.AGENT_ID)
       LOOP
          r_row.agent_id := NULL;
          r_row.agent_name := NULL;
          r_row.gst_tax_inv_refno := NULL;
    
          r_row.tran_date := NULL;
          r_row.ac_no := NULL;
          r_row.descp := NULL;
          r_row.pay_type := NULL;
          r_row.chq_no := NULL;
          r_row.chq_amt := NULL;
          r_row.amount := NULL;
          r_row.bank_in_date := NULL;
          r_row.stmt_descp := NULL;
          r_row.name := NULL;
    
    
          -- r_row.agent_id := r.agent_id;
          -- r_row.agent_name := r.name;
    
    
          IF r.agent_id IS NOT NULL
          THEN
             FOR r2
                IN (  SELECT ACRC_RCPT.GST_TAX_INV_REFNO,
                             ACRC_RCPT.TRAN_DATE,
                             ACRC_RCPT.AC_NO,
                             CMAC_PAYMODE.DESCP,
                             ACRC_BANK.PAY_TYPE,
                             ACRC_BANK.CHQ_NO,
                             ACRC_BANK.CHQ_AMT,
                             ACRC_RCPT.AMOUNT,
                             ACRC_RCPT.BANK_IN_DATE,
                             ACRC_RCPT.STMT_DESCP,
                             ACRC_RCPT.NAME
                        FROM (ACRC_RCPT
                              LEFT OUTER JOIN ACRC_BANK
                                 ON ACRC_RCPT.AC_NO = ACRC_BANK.AC_NO)
                             LEFT OUTER JOIN CMAC_PAYMODE
                                ON ACRC_BANK.PAY_TYPE = CMAC_PAYMODE.CODE
                             LEFT OUTER JOIN DMAG_VI_AGENT
                                ON ACRC_RCPT.AGENT_CODE = DMAG_VI_AGENT.AGENTCODE
                             LEFT OUTER JOIN
                             DMAG_VI_AGCAT_SOURCE
                                ON     SUBSTR (ACRC_RCPT.AGENT_ID, 0, 7) =
                                          DMAG_VI_AGCAT_SOURCE.agent_code
                                   AND SUBSTR (ACRC_RCPT.AGENT_ID, 8, 2) =
                                          DMAG_VI_AGCAT_SOURCE.AGENT_CAT_TYPE
                       WHERE     ACRC_RCPT.PROC_MTH = p_proc_mth
                             AND ACRC_RCPT.PROC_YR = p_proc_year
                             AND ACRC_RCPT.AGENT_ID = r.agent_id
                             AND (   (p_src_id_from IS NULL AND p_src_id_to IS NULL)
                                  OR DMAG_VI_AGCAT_SOURCE.SOURCE_ID BETWEEN p_src_id_from
                                                                        AND p_src_id_to)
                             AND (   (    p_agent_id_from IS NULL
                                      AND p_agent_id_to IS NULL)
                                  OR ACRC_RCPT.AGENT_ID BETWEEN p_agent_id_from
                                                            AND p_agent_id_to)
                             AND (   (p_branch_from IS NULL AND p_branch_to IS NULL)
                                  OR DMAG_VI_AGENT.BRANCH_CODE BETWEEN p_branch_from
                                                                   AND p_branch_to)
                             AND (   (    p_batch_no_from IS NULL
                                      AND p_batch_no_to IS NULL)
                                  OR ACRC_RCPT.BATCH_NO BETWEEN p_batch_no_from
                                                            AND p_batch_no_to)
                             AND (   (    p_tran_date_from IS NULL
                                      AND p_tran_date_to IS NULL)
                                  OR TRUNC (ACRC_RCPT.TRAN_DATE) BETWEEN TO_DATE (
                                                                            p_tran_date_from,
                                                                            'DD/MM/YYYY')
                                                                     AND TO_DATE (
                                                                            p_tran_date_to,
                                                                            'DD/MM/YYYY'))
                    ORDER BY ACRC_RCPT.AGENT_CODE, ACRC_RCPT.AC_NO)
             LOOP
                r_row.agent_id := r.agent_id;
                r_row.agent_name := r.name;
                
                
                r_row.gst_tax_inv_refno := r2.gst_tax_inv_refno;
                r_row.tran_date := r2.tran_date;
                r_row.ac_no := r2.ac_no;
                r_row.descp := r2.descp;
                r_row.pay_type := r2.pay_type;
                r_row.chq_no := r2.chq_no;
                r_row.chq_amt := r2.chq_amt;
                r_row.amount := r2.amount;
                r_row.bank_in_date := r2.bank_in_date;
                r_row.stmt_descp := r2.stmt_descp;
                r_row.name := r2.name;
    
    
    
                PIPE ROW (r_row);
             END LOOP;
          END IF;
       END LOOP;
    
       RETURN;
    EXCEPTION
       WHEN OTHERS
       THEN
          PG_UTIL_LOG_ERROR.PC_INS_log_error (
             g_k_V_PackageName_v || '.' || v_ProcName_v,
             1,
             SQLERRM);
    END fn_rcpt_trans_rpt;
 
   
             
      
      FUNCTION fn_pymt_summ_detail (p_type VARCHAR2,
                                   P_batch_no_from VARCHAR2,
                                   p_batch_no_to  VARCHAR2,
                                   p_branch_from  VARCHAR2,
                                   --p_branch_to  VARCHAR2,
                                   p_bank_code VARCHAR2,
                                   p_platform VARCHAR2,
                                   p_tran_date_from VARCHAR2,
                                   p_tran_date_to VARCHAR2,
                                   p_auth_date_from VARCHAR2,
                                   p_auth_date_to VARCHAR2,
                                   p_operator VARCHAR2,
                                   p_convInd VARCHAR2,
                                   p_userId VARCHAR2,
                                   p_userDept VARCHAR2
                                   )
         RETURN pymt_summ_detail_det_tab
         PIPELINED
      IS
         v_ProcName_v   VARCHAR2 (30) := 'fn_pymt_summ_detail';
         v_Step_v       VARCHAR2 (5) := '000';
         r_row          pymt_summ_detail_det;
         r_temp         pymt_summ_detail_det;
         v_none_cnt     NUMBER;
         v_full_cnt     NUMBER;
         v_detail_cnt   NUMBER;
         TYPE RptCurTyp IS REF CURSOR;
         v_cursor       RptCurTyp;
         sumSql         VARCHAR2(20000);         
      BEGIN
         IF p_type = 'SUMM'
         THEN
             /*FOR r
               IN (SELECT X.BRANCH, X.BATCH_NO, x.BANK
                     FROM ACGC_BATCH X
                    WHERE     X.TRAN_TYPE IN ('CLMT', 'CLNM', 'OS')
                          AND X.TRAN_DATE > (SELECT TO_DATE (VALUE, 'YYYY-MM-DD')
                                               FROM SAPM_PARAM
                                              WHERE KEY = 'FTP_AUTO_CUTOFF')
                          AND ( (SELECT COUNT (1)
                                   FROM CUSTOMER_I.ACPY_PAYLINK H,CUSTOMER.ACPY_PAYLINK_VI A
                                  WHERE     H.BATCH_NO = X.BATCH_NO
                                        AND H.DEL_DATE IS NULL AND (   (    p_tran_date_from IS NULL
                                      AND p_tran_date_to IS NULL)
                                  OR TRUNC (H.TRAN_DATE) BETWEEN TO_DATE (
                                                                            p_tran_date_from,
                                                                            'DD/MM/YYYY')
                                                                     AND TO_DATE (
                                                                            p_tran_date_to,
                                                                            'DD/MM/YYYY'))
                                    AND H.BATCH_NO = A.BATCH_NO
                                    AND H.AC_NO = A.AC_NO
                                    AND NVL(p_branch_from,A.FILTER_BRANCH) = A.FILTER_BRANCH
                                    AND (A.FILTER_DEPT =  (case when (p_userDept IS NOT NULL AND 'FA' <> p_userDept)  THEN  'NA' 
                                                              ELSE  p_userDept END))
 

                        --AND (H.operator =  (case when (p_userDept IS NOT NULL AND 'FA' <> p_userDept)  THEN  p_userId
                        
                        --               else null END) )

                        
                        AND (   (    p_auth_date_from IS NULL
                                      AND p_auth_date_to IS NULL)

                            OR TRUNC (NVL(H.TRANSFER2_DATE, H.TRANSFER_DATE)) BETWEEN TO_DATE (
                                                                            p_auth_date_from,
                                                                            'DD/MM/YYYY')
                                                                     AND TO_DATE (
                                                                            p_auth_date_to,
                                                                            'DD/MM/YYYY'))) > 0)
                                 AND (   (    P_batch_no_from IS NULL
                                      AND p_batch_no_to IS NULL)
                                  OR X.BATCH_NO BETWEEN P_batch_no_from
                                                            AND p_batch_no_to) 
                                                            
                                      AND (   (    p_branch_from IS NULL
                                      )
                                  OR X.BRANCH = p_branch_from
                                                            )                         
                                    AND ( NVL(p_bank_code,x.BANK)=x.BANK )   
                                        ) */
                                        
                     sumSql := 'SELECT X.BRANCH, X.BATCH_NO, x.BANK FROM ACGC_BATCH X  '
                      || '  WHERE     X.TRAN_TYPE IN (''CLMT'', ''CLNM'', ''OS'') '
                      || '  AND X.TRAN_DATE > (SELECT TO_DATE (VALUE, ''YYYY-MM-DD'') '
                      || '  FROM SAPM_PARAM   WHERE KEY = ''FTP_AUTO_CUTOFF'') '
                      || '  AND ( (SELECT COUNT (1)'
                      || '  FROM CUSTOMER_I.ACPY_PAYLINK H,CUSTOMER.ACPY_PAYLINK_VI A '
                      || '  WHERE  H.BATCH_NO = X.BATCH_NO AND H.BATCH_NO = A.BATCH_NO '
                      || '    AND H.AC_NO = A.AC_NO '
                      || '  AND H.DEL_DATE IS NULL  ';
                     -- || '    AND (A.FILTER_DEPT =(case when ('''||p_userDept||''' IS NOT NULL AND ''FA'' <> '''||p_userDept||''')  THEN  '''||p_userDept||''' ' 
                     -- || '  ELSE  ''NA''  END)) ';
                    
                                if (p_branch_from is not null) THEN
                                    sumSql :=  sumSql || ' AND H.ISSUE_OFFICE = '''||p_branch_from ||''' ';
                                
                                end if;
                    
                                IF(p_branch_from is not null AND 'HQ'= p_branch_from AND 'FA' != p_userDept) THEN
                                   sumSql :=  sumSql || ' AND A.FILTER_DEPT = '''||p_userDept||''' ';
                                END IF;
                        
                                if(p_tran_date_from is not null and p_tran_date_to is not null ) THEN                                
                                sumSql :=  sumSql || ' AND TRUNC (H.TRAN_DATE) BETWEEN TO_DATE ('''||p_tran_date_from||''',''DD/MM/YYYY'')'
                                                  || ' AND TO_DATE ('''||p_tran_date_to||''',''DD/MM/YYYY'') ';
                                
                                end if;
                                
                                
                        
                                if( p_auth_date_from IS NOT NULL     AND p_auth_date_to IS NOT NULL) THEN
                                sumSql :=  sumSql || ' AND TRUNC (NVL(H.TRANSFER2_DATE, H.TRANSFER_DATE)) BETWEEN TO_DATE ('''||p_auth_date_from||''',''DD/MM/YYYY'') '
                                                  || ' AND TO_DATE ('''||p_auth_date_to||''',''DD/MM/YYYY'') ';                                                            
                                 
                                 end if;
                                 
                                 if('Y' = p_convInd) THEN
                                  sumSql :=  sumSql || ' AND H.DOWNLOAD_DATE  IS NOT NULL ' ;
                                  ELSE 
                                  sumSql :=  sumSql || ' AND H.DOWNLOAD_DATE  IS  NULL ' ;
                                  END IF;
                                 
                                
                                 
                                sumSql :=  sumSql || ' ) > 0) ' ;
                                 
                                 if( p_batch_no_from IS NOT NULL  AND p_batch_no_to IS NOT NULL)THEN
                                  sumSql :=  sumSql || ' AND X.BATCH_NO BETWEEN '''||p_batch_no_from ||'''  AND '''||p_batch_no_to||''' ';
                                                                             
                                     end if;
                                    
                                 if( p_bank_code IS NOT NULL )THEN
                                    sumSql :=  sumSql || ' AND x.BANK = '''||p_bank_code||'''  ';
                                  
                              end if;    
                              
                   DBMS_OUTPUT.put_line('SUMMARY QUERY  -=-' || sumSql);
                   OPEN v_cursor FOR sumSql;                                    
            LOOP
            
            FETCH v_cursor
                    INTO r_temp.issue_office,r_temp.BATCH_NO,r_temp.BANK;
                    EXIT WHEN v_cursor%NOTFOUND;          
                
              
               r_row.issue_office := NULL;
               r_row.batch_no := NULL;
               r_row.bank_code := NULL;
               r_row.batch_record := NULL;
               r_row.batch_amounT := NULL;
               r_row.auth_record := NULL;
               r_row.auth_amount := NULL;
               v_none_cnt := 0;
               v_full_cnt := 0;
      
               r_row.issue_office := r_temp.issue_office;
               r_row.batch_no := r_temp.BATCH_NO;
               r_row.bank_code := r_temp.BANK;
      
      
      
               SELECT COUNT (1)
                 INTO r_row.batch_record
                 FROM ACPY_PAYLINK A
                WHERE A.BATCH_NO = r_row.BATCH_NO AND A.DEL_DATE IS NULL;
      
      
               SELECT SUM (B.AMOUNT)
                 INTO r_row.batch_amount
                 FROM ACPY_PAYLINK B
                WHERE B.BATCH_NO = r_row.BATCH_NO AND B.DEL_DATE IS NULL;
      
      
               SELECT COUNT (1)
                 INTO r_row.auth_record
                 FROM ACPY_PAYLINK C
                WHERE     C.BATCH_NO = r_row.BATCH_NO
                      AND C.TRANSFER_IND = 'Y'
                      AND C.TRANSFER2_IND = 'Y'
                      AND C.DEL_DATE IS NULL;
      
      
      
               SELECT SUM (D.AMOUNT)
                 INTO r_row.auth_amount
                 FROM ACPY_PAYLINK D
                WHERE     D.BATCH_NO = r_row.BATCH_NO
                      AND D.TRANSFER_IND = 'Y'
                      AND D.TRANSFER2_IND = 'Y'
                      AND D.DEL_DATE IS NULL;
      
      
      
               SELECT COUNT (1)
                 INTO v_none_cnt
                 FROM ACPY_PAYLINK F
                WHERE     F.BATCH_NO = r_row.BATCH_NO
                      AND (   F.TRANSFER_IND IS NULL
                           OR F.TRANSFER_IND = 'N'
                           OR F.TRANSFER_IND = '')
                      AND (   F.TRANSFER2_IND IS NULL
                           OR F.TRANSFER2_IND = 'N'
                           OR F.TRANSFER2_IND = '')
                      AND F.DEL_DATE IS NULL;
      
      
               SELECT COUNT (1)
                 INTO v_full_cnt
                 FROM ACPY_PAYLINK H
                WHERE     H.BATCH_NO = r_row.BATCH_NO
                      AND H.TRANSFER_IND = 'Y'
                      AND H.TRANSFER2_IND = 'Y'
                      AND H.DEL_DATE IS NULL;
      
      
               IF r_row.batch_record = v_none_cnt
               THEN
                  r_row.auth_status := 'NONE';
               ELSIF r_row.batch_record = v_full_cnt
               THEN
                  r_row.auth_status := 'FULL';
               ELSE
                  r_row.auth_status := 'PARTIAL';
               END IF;
      
      
      
               PIPE ROW (r_row);
            END LOOP;
            CLOSE v_cursor; 
         ELSE                                                         --------Detail
            -- v_detail_cnt := 0;
      
      
      
            /*FOR r
               IN (SELECT A.ISSUE_OFFICE,
                          A.BANK,
                          A.PLATFORM,
                          A.TRAN_DATE,
                          A.FILENAME,
                          A.DOWNLOAD_DATE,
                          A.BATCH_NO,
                          A.AC_NO,
                          A.NAME,
                          A.STMT_DESCP,
                          A.AMOUNT,
                          A.OPERATOR,
                          A.TRANSFER_IND,
                          A.TRANSFER_OPERATOR,
                          A.TRANSFER_DATE,
                          A.TRANSFER2_IND,
                          A.TRANSFER2_OPERATOR,
                          A.TRANSFER2_DATE,
                          (CASE
                              WHEN A.TRANSFER2_DATE IS NOT NULL
                              THEN
                                 A.TRANSFER2_DATE
                              ELSE
                                 A.TRANSFER_DATE
                           END)
                             AS AUTH_DATE
                     -- (SELECT B.AUTH_STATUS
                     --  FROM VIEW_PYMT_SUMM B
                     -- WHERE B.BATCH_NO = A.BATCH_NO)
                     --AS AUTH_STATUS
                     FROM ACPY_PAYLINK A,ACPY_PAYLINK_VI VI
                    WHERE     A.DATCONV_DATE IS NULL
                          AND A.DEL_DATE IS NULL
                          AND A.TRAN_DATE > (SELECT TO_DATE (VALUE, 'YYYY-MM-DD')
                                               FROM SAPM_PARAM
                                              WHERE KEY = 'FTP_AUTO_CUTOFF')
                                              
                        AND (   (    P_batch_no_from IS NULL
                                      AND p_batch_no_to IS NULL)
                                  OR A.BATCH_NO BETWEEN P_batch_no_from
                                                            AND p_batch_no_to) 
                                                            
                                                                                                          
                        --AND (   (    p_branch_from IS NULL )
                         --         OR A.ISSUE_OFFICE = p_branch_from) 
                            AND  VI.BATCH_NO = A.BATCH_NO
                            AND VI.AC_NO = A.AC_NO
                            AND NVL(p_branch_from,VI.FILTER_BRANCH) = VI.FILTER_BRANCH --HQ
                            --AND  A.FILTER_DEPT = 'NA'
                            --AND (A.FILTER_DEPT =  (case when (p_userDept IS NOT NULL AND 'FA' <> p_userDept)  THEN  p_userId
                            AND VI.FILTER_DEPT =  (case when ('FA' IS NOT NULL AND 'FA' <> 'FA')  THEN  'NA' 
                                                      ELSE  p_userDept END)
                                                            
                        AND (NVL(p_bank_code,A.BANK)=A.BANK)  
                        AND (NVL(p_platform,A.PLATFORM)=A.PLATFORM)
                        --AND (A.operator =  (case when (p_userDept IS NOT NULL AND 'FA' <> p_userDept)  THEN  p_userId
                         --              else null END) ) 
                       AND (   (    p_tran_date_from IS NULL
                                      AND p_tran_date_to IS NULL)
                                  OR TRUNC (A.TRAN_DATE) BETWEEN TO_DATE (
                                                                            p_tran_date_from,
                                                                            'DD/MM/YYYY')
                                                                     AND TO_DATE (
                                                                            p_tran_date_to,
                                                                            'DD/MM/YYYY'))
                                                        
                        AND (   (    p_auth_date_from IS NULL
                                      AND p_auth_date_to IS NULL)

                            OR TRUNC (NVL(A.TRANSFER2_DATE, A.TRANSFER_DATE)) BETWEEN TO_DATE (
                                                                            p_auth_date_from,
                                                                            'DD/MM/YYYY')
                                                                     AND TO_DATE (
                                                                            p_auth_date_to,
                                                                            'DD/MM/YYYY')) 
                                                                            
                            AND(NVL(p_operator,A.OPERATOR)=A.OPERATOR)
                                                                            
       
                             
                                              
                                              
                                              
                                              
                                              
                                              )*/
                         sumSql := ' SELECT A.ISSUE_OFFICE,A.BANK,A.PLATFORM,A.TRAN_DATE,A.FILENAME,A.DOWNLOAD_DATE,'                             
                          || '  A.BATCH_NO,A.AC_NO,A.NAME,A.STMT_DESCP,   '
                          || '  A.AMOUNT,A.OPERATOR,A.TRANSFER_IND,A.TRANSFER_OPERATOR,A.TRANSFER_DATE,A.TRANSFER2_IND,'
                          || '  A.TRANSFER2_OPERATOR,A.TRANSFER2_DATE,'
                          || ' (CASE  WHEN A.TRANSFER2_DATE IS NOT NULL THEN  A.TRANSFER2_DATE   ELSE'
                          || '  A.TRANSFER_DATE  END) AS AUTH_DATE ' 
                          || '  FROM ACPY_PAYLINK A,(SELECT AC_NO,BATCH_NO,FILTER_DEPT FROM ACPY_PAYLINK_VI GROUP BY AC_NO,BATCH_NO,FILTER_DEPT) VI  '
                          || '  WHERE     A.DATCONV_DATE IS NULL    AND A.DEL_DATE IS NULL '
                          || '  AND A.TRAN_DATE > (SELECT TO_DATE (VALUE, ''YYYY-MM-DD'') FROM SAPM_PARAM   WHERE KEY = ''FTP_AUTO_CUTOFF'') '
                          || '  AND  VI.BATCH_NO = A.BATCH_NO  AND VI.AC_NO = A.AC_NO ';
                          --|| '  AND VI.FILTER_DEPT =  (case when ('''||p_userDept||''' IS NOT NULL AND ''FA'' <> '''||p_userDept||''')  THEN  --'''||p_userDept||''' ' 
                         -- || '  ELSE  ''NA'' END) ' ;
                          
                          if (p_branch_from is not null) THEN
                                    sumSql :=  sumSql || ' AND A.ISSUE_OFFICE = '''||p_branch_from ||''' ';
                                
                             end if;
                    
                         IF(p_branch_from is not null AND 'HQ'= p_branch_from AND 'FA' != p_userDept) THEN
                                   sumSql :=  sumSql || ' AND VI.FILTER_DEPT = '''||p_userDept||''' ';
                              END IF;
                                
                                
                         IF (p_batch_no_from IS NOT NULL    AND p_batch_no_to IS NOT NULL) THEN
                                  sumSql := sumSql || ' AND A.BATCH_NO BETWEEN '''||p_batch_no_from||'''  AND '''||p_batch_no_to||''' ';
                           END IF;                                 
                                                                                                          
                                               
                            
                        IF (p_bank_code  IS NOT NULL) THEN
                        sumSql := sumSql || ' AND A.BANK = '''|| p_bank_code || ''' ';
                        END IF;
                        
                        IF(p_platform IS NOT NULL) THEN
                           sumSql := sumSql || ' AND A.PLATFORM = '''||p_bank_code||''' ' ;
                        END IF;
                        
                       IF (p_tran_date_from IS NOT NULL  AND p_tran_date_to IS NOT NULL) THEN
                               sumSql := sumSql || ' AND  TRUNC (A.TRAN_DATE) BETWEEN TO_DATE ('''||p_tran_date_from||''',''DD/MM/YYYY'') '
                                                || ' AND TO_DATE ('''||p_tran_date_to||''',''DD/MM/YYYY'') ';
                        END IF; 
                        
                        IF (p_auth_date_from IS NOT NULL AND p_auth_date_to IS NOT NULL) THEN
                        sumSql := sumSql || ' AND TRUNC (NVL(A.TRANSFER2_DATE, A.TRANSFER_DATE)) BETWEEN TO_DATE ('''||p_auth_date_from||''',''DD/MM/YYYY'') '
                                         || ' AND TO_DATE ('''||p_auth_date_to||''',''DD/MM/YYYY'') ';
                         END IF;
                         
                         IF (p_operator IS NOT NULL)THEN                     
                            sumSql := sumSql || ' AND A.OPERATOR = '''|| p_operator ||''' ';
                            END IF;
                            
                            if('Y' = p_convInd) THEN
                                  sumSql :=  sumSql || ' AND A.DOWNLOAD_DATE  IS NOT NULL ' ;
                                  ELSE 
                                  sumSql :=  sumSql || ' AND A.DOWNLOAD_DATE  IS  NULL ' ;
                                  END IF;
                                  
                                 DBMS_OUTPUT.put_line('DETAIL QUERY  -=- ' || sumSql); 
                             
            OPEN v_cursor FOR sumSql;                                    
            LOOP
            
            FETCH v_cursor
                    INTO   r_temp.ISSUE_OFFICE,         
                           r_temp.BANK,
                           r_temp.PLATFORM,
                           r_temp.TRAN_DATE,
                           r_temp.FILENAME,
                           r_temp.DOWNLOAD_DATE,
                           r_temp.BATCH_NO,
                           r_temp.AC_NO,
                           r_temp.NAME,
                           r_temp.STMT_DESCP,
                           r_temp.AMOUNT,
                           r_temp.OPERATOR,
                           r_temp.TRANSFER_IND,
                           r_temp.TRANSFER_OPERATOR,
                           r_temp.TRANSFER_DATE,
                           r_temp.TRANSFER2_IND,
                           r_temp.TRANSFER2_OPERATOR,
                           r_temp.TRANSFER2_DATE,
                           r_temp.AUTH_DATE;
                    EXIT WHEN v_cursor%NOTFOUND;          
                
               r_row.issue_office := NULL;
               r_row.bank := NULL;
               r_row.platform := NULL;
               r_row.tran_date := NULL;
               r_row.filename := NULL;
               r_row.download_date := NULL;
               r_row.batch_no := NULL;
               r_row.ac_no := NULL;
               r_row.NAME := NULL;
               r_row.STMT_DESCP := NULL;
               r_row.AMOUNT := NULL;
               r_row.OPERATOR := NULL;
               r_row.TRANSFER_IND := NULL;
               r_row.TRANSFER_OPERATOR := NULL;
               r_row.TRANSFER_DATE := NULL;
               r_row.TRANSFER2_IND := NULL;
               r_row.TRANSFER2_OPERATOR := NULL;
               r_row.TRANSFER2_DATE := NULL;
               r_row.AUTH_DATE := NULL;
               r_row.AUTH_STATUS := NULL;
               v_detail_cnt :=0;
      
      
      
                          r_row.issue_office := r_temp.ISSUE_OFFICE;                          
                          r_row.bank := r_temp.BANK;
                          r_row.platform := r_temp.PLATFORM;
                          r_row.tran_date := r_temp.TRAN_DATE;
                          r_row.filename := r_temp.FILENAME;
                          r_row.download_date := r_temp.DOWNLOAD_DATE;
                          r_row.batch_no := r_temp.BATCH_NO;
                          r_row.ac_no := r_temp.AC_NO;
                          r_row.NAME := r_temp.NAME;
                          r_row.STMT_DESCP := r_temp.STMT_DESCP;
                          r_row.AMOUNT := r_temp.AMOUNT;
                          r_row.OPERATOR := r_temp.OPERATOR;
                          r_row.TRANSFER_IND := r_temp.TRANSFER_IND;
                          r_row.TRANSFER_OPERATOR := r_temp.TRANSFER_OPERATOR;
                          r_row.TRANSFER_DATE := r_temp.TRANSFER_DATE;
                          r_row.TRANSFER2_IND := r_temp.TRANSFER2_IND;
                          r_row.TRANSFER2_OPERATOR := r_temp.TRANSFER2_OPERATOR;
                          r_row.TRANSFER2_DATE := r_temp.TRANSFER2_DATE;
                          r_row.AUTH_DATE := r_temp.AUTH_DATE;                         
                         -- PIPE ROW (r_row);
              
              
                  begin
                   SELECT COUNT (1)
                     INTO v_detail_cnt
                     FROM ACGC_BATCH X
                    WHERE     X.TRAN_TYPE IN ('CLMT', 'CLNM', 'OS')
                          AND X.BATCH_NO = r_row.BATCH_NO
                          AND X.TRAN_DATE > (SELECT TO_DATE (VALUE, 'YYYY-MM-DD')
                                               FROM SAPM_PARAM
                                              WHERE KEY = 'FTP_AUTO_CUTOFF')
                          AND ( 
                        (SELECT COUNT (1)
                               FROM ACPY_PAYLINK H
                                WHERE H.BATCH_NO = X.BATCH_NO AND H.DEL_DATE IS NULL)
                                 --r_row.batch_record 
                                  >   0);
          
                      EXCEPTION    WHEN OTHERS  THEN   NULL;     END;
          
                   IF v_detail_cnt > 0
                   THEN
                           Begin
                            SELECT COUNT (1)
                             INTO r_row.batch_record
                             FROM ACPY_PAYLINK A
                              WHERE A.BATCH_NO = r_row.BATCH_NO AND A.DEL_DATE IS NULL;
                              EXCEPTION    WHEN OTHERS  THEN   NULL;     END;
                          
                  
                              begin
                              SELECT COUNT (1)
                                INTO v_none_cnt
                                FROM ACPY_PAYLINK F
                               WHERE     F.BATCH_NO = r_row.BATCH_NO
                                     AND (   F.TRANSFER_IND IS NULL
                                          OR F.TRANSFER_IND = 'N'
                                          OR F.TRANSFER_IND = '')
                                     AND (   F.TRANSFER2_IND IS NULL
                                          OR F.TRANSFER2_IND = 'N'
                                          OR F.TRANSFER2_IND = '')
                                     AND F.DEL_DATE IS NULL;
                                     EXCEPTION    WHEN OTHERS  THEN   NULL;     END;
                  
                              begin
                              SELECT COUNT (1)
                                INTO v_full_cnt
                                FROM ACPY_PAYLINK H
                               WHERE     H.BATCH_NO = r_row.BATCH_NO
                                     AND H.TRANSFER_IND = 'Y'
                                     AND H.TRANSFER2_IND = 'Y'
                                     AND H.DEL_DATE IS NULL;
                               EXCEPTION    WHEN OTHERS  THEN   NULL;     END;      
                  
                  
                              IF r_row.batch_record = v_none_cnt
                              THEN
                                 r_row.auth_status := 'NONE';
                              ELSIF r_row.batch_record = v_full_cnt
                              THEN
                                 r_row.auth_status := 'FULL';
                              ELSE
                                 r_row.auth_status := 'PARTIAL';
                              END IF;
                         
                            
              ---end Summary
              
                          /*r_row.issue_office := r_temp.ISSUE_OFFICE;                          
                          r_row.bank := r_temp.BANK;
                          r_row.platform := r_temp.PLATFORM;
                          r_row.tran_date := r_temp.TRAN_DATE;
                          r_row.filename := r_temp.FILENAME;
                          r_row.download_date := r_temp.DOWNLOAD_DATE;
                          r_row.batch_no := r_temp.BATCH_NO;
                          r_row.ac_no := r_temp.AC_NO;
                          r_row.NAME := r_temp.NAME;
                          r_row.STMT_DESCP := r_temp.STMT_DESCP;
                          r_row.AMOUNT := r_temp.AMOUNT;
                          r_row.OPERATOR := r_temp.OPERATOR;
                          r_row.TRANSFER_IND := r_temp.TRANSFER_IND;
                          r_row.TRANSFER_OPERATOR := r_temp.TRANSFER_OPERATOR;
                          r_row.TRANSFER_DATE := r_temp.TRANSFER_DATE;
                          r_row.TRANSFER2_IND := r_temp.TRANSFER2_IND;
                          r_row.TRANSFER2_OPERATOR := r_temp.TRANSFER2_OPERATOR;
                          r_row.TRANSFER2_DATE := r_temp.TRANSFER2_DATE;
                          r_row.AUTH_DATE := r_temp.AUTH_DATE;
                          --r_row.AUTH_STATUS := r.AUTH_STATUS;
                          --r_row.CNT := v_detail_cnt;
                          PIPE ROW (r_row);*/
                          
                          PIPE ROW (r_row);
              END IF;
            END LOOP;
            CLOSE v_cursor; 
         END IF;
      
      
      
         RETURN;
      EXCEPTION
         WHEN OTHERS
         THEN
            PG_UTIL_LOG_ERROR.PC_INS_log_error (
               g_k_V_PackageName_v || '.' || v_ProcName_v,
               1,
               SQLERRM);
      END fn_pymt_summ_detail;
      FUNCTION FN_PREMWARR_CANCE_NOTICE (p_StrReportType     VARCHAR2,
                                 p_StrBranchCode          VARCHAR2,
                                 p_StrAccountCodeFrom     VARCHAR2,
                                 p_StrAccountCodeTo       VARCHAR2,
                                 p_StrPolicy              VARCHAR2,
                                 p_StrAgeDaysFrom         VARCHAR2,
                                 p_StrAgeDaysTo           VARCHAR2,                                
                                 p_StrPrintDate           VARCHAR2
                                )
      RETURN RPAC_PREMWARR_CANCE_NOTICE_TAB
      PIPELINED
      IS
      v_ProcName_v   VARCHAR2 (30) := 'FN_PREMWARR_CANCE_NOTICE';
      v_Step_v       VARCHAR2 (5) := '000';
      r_row          RPAC_PREMWARR_CANCE_NOTICE_REC;
      r_tmp          RPAC_PREMWARR_CANCE_NOTICE_REC;
      v_sql_v       VARCHAR2(20000);
      v_sql_cl      VARCHAR2(20000);
      v_sql_up      VARCHAR2(20000);
      TYPE RptCurTyp  IS REF CURSOR;
      v_cursor1    RptCurTyp;
      TYPE RptCurTyp1  IS REF CURSOR;
      v_cursor2    RptCurTyp1;
      v_cursor_cnt number := 1;
      BEGIN
      DBMS_OUTPUT.put_line ('Starting Function =' || p_StrReportType);
      ---<<9.00 >> start
      v_sql_v      :=
           'SELECT CC.CODE_DESC,DMAG_VI_AGENT.NAME,CMGE_POSTCODE.DESCRIPTION,C1.CODE_DESC AS CLCODE_DESC, '     
          ||'CMDM_BRANCH.ADDRESS_LINE1, CMDM_BRANCH.ADDRESS_LINE2,CMDM_BRANCH.ADDRESS_LINE3, '
          ||'CMDM_BRANCH.PHONE_NO, CMDM_BRANCH.POSTCODE,CMDM_BRANCH.FAX_NO,ACST_MAST.ST_DESC,  '        
          ||'ACST_MAST.ST_DOC, ACST_MAST.AGENT_ID,UB.CP_PART_ID,  '
          ||'ACST_MAST.BAL_AMT, SUBSTR(ACST_MAST.PRODUCT_CONFIG_CODE,1,2) AS MAINCLS,SUBSTR(ACST_MAST.PRODUCT_CONFIG_CODE,3,2) AS CLASS,   '
          ||'ACST_MAST.ENDT_NO, ACST_MAST.PW_PRN_IND,  '
          ||'ACST_MAST.POL_EDATE, ACST_MAST.ENDT_EDATE, ACST_MAST.AGE_DAYS,  '
          ||'ACST_MAST.STAX, ACST_MAST.SCHRG, ACST_MAST.TRF_FEE, ACST_MAST.PCHRG, ACST_MAST.BCHRG,  '
          ||'ACST_MAST.MISC_AMT, ACST_MAST.STAMP, ACST_MAST.GROSS, ACST_MAST.COMM1, ACST_MAST.COMM2,  '
          ||'ACST_MAST.EDATE, ACST_MAST.GST,UB.EFF_DATE,UB.EXP_DATE, ACST_MAST.ST_TYPE '  -- 4.00
          ||'FROM  (  (  (  ACST_MAST INNER JOIN CMGE_CODE C1 ON  C1.CODE_CD = ACST_MAST.PRODUCT_CONFIG_CODE  AND  C1.CAT_CODE IN (''BG_PRODUCT'',''EG_PRODUCT'',''FI_PRODUCT'',''LB_PRODUCT'',''MA_PRODUCT'',''MS_PRODUCT'',''MT_PRODUCT'',''PL_PRODUCT'',''PP_PRODUCT'',''WM_PRODUCT'')) '
          ||'LEFT OUTER JOIN DMAG_VI_AGENT ON ACST_MAST.AGENT_CODE = DMAG_VI_AGENT.AGENTCODE ) '
          ||'LEFT OUTER JOIN OCP_POLICY_BASES OCP ON  OCP.POLICY_REF = ACST_MAST.POL_NO '
          ||'LEFT OUTER JOIN UWGE_POLICY_BASES UB ON  UB.CONTRACT_ID = OCP.CONTRACT_ID AND UB.TOP_INDICATOR=''Y'' '
          ||'LEFT OUTER JOIN CMDM_BRANCH ON DMAG_VI_AGENT.BRANCH_CODE = CMDM_BRANCH.BRANCH_CODE ) '
          ||'LEFT OUTER JOIN CMGE_CODE CC ON CMDM_BRANCH.CITY = CC.CODE_CD AND CC.CAT_CODE = ''CITY''  '
          ||'LEFT OUTER JOIN CMGE_POSTCODE ON CMDM_BRANCH.POSTCODE = CMGE_POSTCODE.POSTCODE '
          ||'WHERE  ( ACST_MAST.ST_TYPE = ''PL'' OR ACST_MAST.ST_TYPE=''EN'' )  '
          ||'AND ACST_MAST.AGENT_CAT_TYPE=''PW'' AND ACST_MAST.BAL_AMT <> 0    '
          ||'AND ACST_MAST.GROSS + ACST_MAST.STAX + ACST_MAST.SCHRG + ACST_MAST.TRF_FEE +  '
          ||'ACST_MAST.PCHRG + ACST_MAST.BCHRG + ACST_MAST.MISC_AMT + ACST_MAST.STAMP + ACST_MAST.GST > 0 ';
        
        ---<<9.00 >> end
        IF('PA'= p_StrReportType)THEN
         v_sql_v := v_sql_v ||' AND ACST_MAST.PW_PRN_IND IS NULL  ';
        END IF;
        
        IF('RA'= p_StrReportType OR 'RP'= p_StrReportType)THEN
         v_sql_v := v_sql_v ||' AND ACST_MAST.PW_PRN_IND IS NOT NULL ';
        END IF;
        
        IF(p_StrBranchCode IS NOT NULL) THEN
         v_sql_v := v_sql_v ||' AND DMAG_VI_AGENT.BRANCH_CODE = '''||p_StrBranchCode||''' '; 
        END IF;
        
        IF(p_StrAccountCodeFrom IS NOT NULL AND p_StrAccountCodeTo IS NOT NULL ) THEN
         v_sql_v := v_sql_v ||' AND ACST_MAST.AGENT_CODE BETWEEN '''||p_StrAccountCodeFrom||''' AND '''||p_StrAccountCodeTo||''' '; 
        END IF;
        
        IF(p_StrPolicy IS NOT NULL) THEN 
         v_sql_v := v_sql_v ||' AND ACST_MAST.ST_DOC = '''||p_StrPolicy||'''  ';
        END IF;
        
        IF(p_StrAgeDaysFrom IS NOT NULL AND p_StrAgeDaysTo IS NOT NULL) THEN 
         v_sql_v := v_sql_v ||' AND TRUNC(SYSDATE) - TRUNC(ACST_MAST.EDATE) >= '''||p_StrAgeDaysFrom||''' AND TRUNC(SYSDATE) - TRUNC(ACST_MAST.EDATE) <= '''||p_StrAgeDaysTo||''' ';
        END IF;
        
        IF(p_StrPrintDate IS NOT NULL) THEN
         v_sql_v := v_sql_v ||' AND TRUNC(ACST_MAST.PW_PRN_DATE) = TO_DATE ('''||p_StrPrintDate||''',''DD/MM/YYYY'')  '; 
        END IF;
        
         v_sql_v := v_sql_v ||' ORDER BY ACST_MAST.AGENT_ID, ACST_MAST.POL_NO  ';        
        
        DBMS_OUTPUT.put_line ('1.v_sql_v =' || v_sql_v);
        
        OPEN v_cursor1 FOR v_sql_v;
        
        
        
        LOOP
        FETCH v_cursor1
                    INTO r_tmp.CODE_DESC,r_tmp.NAME,r_tmp.DESCRIPTION,r_tmp.CLCODE_DESC,r_tmp.ADDRESS_LINE1,r_tmp.ADDRESS_LINE2,    
                    r_tmp.ADDRESS_LINE3,r_tmp.PHONE_NO,r_tmp.POSTCODE,r_tmp.FAX_NO,r_tmp.ST_DESC,r_tmp.ST_DOC,r_tmp.AGENT_ID,    
                    r_tmp.CLIENT,r_tmp.BAL_AMT,r_tmp.MAINCLS,r_tmp.CLASS,r_tmp.ENDT_NO,r_tmp.PW_PRN_IND,r_tmp.POL_EDATE,r_tmp.ENDT_EDATE,        
                    r_tmp.AGE_DAYS,r_tmp.STAX,r_tmp.SCHRG,r_tmp.TRF_FEE,r_tmp.PCHRG,r_tmp.BCHRG,r_tmp.MISC_AMT,r_tmp.STAMP,r_tmp.GROSS,
                    r_tmp.COMM1,r_tmp.COMM2,r_tmp.EDATE, r_tmp.GST,r_tmp.EFF_DATE,r_tmp.EXP_DATE,r_tmp.ST_TYPE; -- 4.00
                    EXIT WHEN v_cursor1%NOTFOUND;
                    DBMS_OUTPUT.put_line ('1.1. v_sql_v =' || v_sql_v);
                r_row.CODE_DESC := NULL;
                r_row.NAME := NULL;
                r_row.DESCRIPTION := NULL;
                r_row.CLCODE_DESC    := NULL;
                r_row.ADDRESS_LINE1    := NULL;
                r_row.ADDRESS_LINE2    := NULL;    
                r_row.ADDRESS_LINE3    := NULL;
                r_row.PHONE_NO := NULL;
                r_row.POSTCODE := NULL;
                r_row.FAX_NO := NULL;
                r_row.ST_DESC := NULL;
                r_row.ST_DOC := NULL;
                r_row.AGENT_ID := NULL;  
                r_row.CLIENT := NULL;
                r_row.BAL_AMT := NULL;
                r_row.MAINCLS := NULL;
                r_row.CLASS := NULL;
                r_row.ENDT_NO := NULL;
                r_row.PW_PRN_IND :=  NULL;  
                r_row.POL_EDATE :=  NULL;  
                r_row.ENDT_EDATE  := NULL ;     
                r_row.AGE_DAYS :=  NULL ;  
                r_row.STAX :=  NULL;  
                r_row.SCHRG :=  NULL;  
                r_row.TRF_FEE :=  NULL;  
                r_row.PCHRG :=  NULL;  
                r_row.BCHRG :=  NULL;  
                r_row.MISC_AMT :=  NULL;  
                r_row.STAMP :=  NULL;  
                r_row.GROSS :=  NULL;  
                r_row.COMM1 :=  NULL;  
                r_row.COMM2 :=  NULL; 
                r_row.EDATE :=  NULL;  
                r_row.GST :=  NULL;  
                r_row.EFF_DATE :=   NULL;
                r_row.EXP_DATE :=    NULL;    
                r_row.ST_TYPE :=    NULL;  -- 4.00
                
       IF(v_cursor1%NOTFOUND) THEN        
         v_cursor_cnt := 0;
         DBMS_OUTPUT.put_line ('v_cursor_cnt =' || v_cursor_cnt);
         END IF;
                    
                r_row.CODE_DESC := r_tmp.CODE_DESC;
                r_row.NAME := r_tmp.NAME;
                r_row.DESCRIPTION := r_tmp.DESCRIPTION;
                r_row.CLCODE_DESC    := r_tmp.CLCODE_DESC;
                r_row.ADDRESS_LINE1    := r_tmp.ADDRESS_LINE1;
                r_row.ADDRESS_LINE2    := r_tmp.ADDRESS_LINE2;    
                r_row.ADDRESS_LINE3    := r_tmp.ADDRESS_LINE3;
                r_row.PHONE_NO := r_tmp.PHONE_NO;
                r_row.POSTCODE := r_tmp.POSTCODE;
                r_row.FAX_NO := r_tmp.FAX_NO;
                r_row.ST_DESC := r_tmp.ST_DESC;
                r_row.ST_DOC := r_tmp.ST_DOC;
                r_row.AGENT_ID := r_tmp.AGENT_ID;  
                r_row.CLIENT := TO_CHAR(NVL(r_tmp.CLIENT,0));
                r_row.BAL_AMT := r_tmp.BAL_AMT;
                r_row.MAINCLS := r_tmp.MAINCLS;
                r_row.CLASS := r_tmp.CLASS;
                r_row.ENDT_NO := r_tmp.ENDT_NO;
                r_row.PW_PRN_IND :=  r_tmp.PW_PRN_IND;  
                r_row.POL_EDATE :=  r_tmp.POL_EDATE;  
                r_row.ENDT_EDATE  := r_tmp.ENDT_EDATE ;     
                r_row.AGE_DAYS :=  r_tmp.AGE_DAYS ;  
                r_row.STAX :=  r_tmp.STAX;  
                r_row.SCHRG :=  r_tmp.SCHRG;  
                r_row.TRF_FEE :=  r_tmp.TRF_FEE;  
                r_row.PCHRG :=  r_tmp.PCHRG;  
                r_row.BCHRG :=  r_tmp.BCHRG;  
                r_row.MISC_AMT :=  r_tmp.MISC_AMT;  
                r_row.STAMP :=  r_tmp.STAMP;  
                r_row.GROSS :=  r_tmp.GROSS;  
                r_row.COMM1 :=  r_tmp.COMM1;  
                r_row.COMM2 :=  r_tmp.COMM2; 
                r_row.EDATE :=  r_tmp.EDATE;  
                r_row.GST :=  r_tmp.GST;  
                r_row.EFF_DATE :=   r_tmp.EFF_DATE;
                r_row.EXP_DATE :=    r_tmp.EXP_DATE;                
                r_row.ST_TYPE :=    r_tmp.ST_TYPE; -- 4.00
                --DBMS_OUTPUT.put_line ('1.v_sql_v VALUE =' || r_row.EXP_DATE); -- 4.00
                      v_sql_cl  :=
                                'SELECT CS.CODE_DESC AS STATE_DESC,CC.CODE_DESC AS CITY_DESC, '
                                ||'CP.ID_TYPE1,CP.NAME_EXT,CAD.ADDRESS_LINE1,CAD.ADDRESS_LINE2,CAD.ADDRESS_LINE2,CAD.POSTCODE ' 
                                ||'FROM CPGE_VI_PARTNERS_ALL CP  LEFT JOIN  CPGE_VI_ADDRESS_ALL CAD ON  CP.PART_ID = CAD.PART_ID ' 
                                ||'LEFT JOIN CMGE_CODE CS ON CAD.STATE = CS.CODE_CD AND CS.CAT_CODE = ''STATE'' '
                                ||'LEFT JOIN CMGE_CODE CC ON CAD.CITY = CC.CODE_CD AND CC.CAT_CODE = ''CITY'' ' 
                                ||'WHERE CP.TOP_INDICATOR =''Y'' AND CAD.DEFAULT_ADDR=''Y'' AND CAD.TOP_INDICATOR=''Y''  AND ROWNUM = 1 ';
                 
                 IF(r_row.CLIENT IS NOT NULL) THEN
                  v_sql_cl := v_sql_cl ||' AND CP.PART_ID = '''||r_row.CLIENT||''' ';
                 END IF;                
                DBMS_OUTPUT.put_line ('2.v_sql_cl =' || v_sql_cl);
                OPEN v_cursor2 FOR v_sql_cl;                

                LOOP
                    FETCH v_cursor2
                    INTO r_tmp.STATE_DESC,r_tmp.CITY_DESC,r_tmp.ID_TYPE1,r_tmp.NAME_EXT,r_tmp.CPADDRESS_LINE1,r_tmp.CPADDRESS_LINE2,    
                    r_tmp.CPADDRESS_LINE3,r_tmp.CPPOSTCODE;
                    
                    --IF(v_cursor2%NOTFOUND) THEN
                    --pipe row (r_row);
                    --END IF;
                    
                    EXIT WHEN v_cursor2%NOTFOUND;
                    DBMS_OUTPUT.put_line ('2.1. v_sql_cl =' || v_sql_cl);
                    r_row.STATE_DESC := NULL;
                    r_row.CITY_DESC := NULL;
                    r_row.ID_TYPE1 := NULL;
                    r_row.NAME_EXT := NULL;
                    r_row.CPADDRESS_LINE1 := NULL;
                    r_row.CPADDRESS_LINE2 := NULL;
                    r_row.CPADDRESS_LINE3 := NULL;
                    r_row.CPPOSTCODE := NULL;
                    
                    r_row.STATE_DESC := r_tmp.STATE_DESC;
                    r_row.CITY_DESC := r_tmp.CITY_DESC;
                    r_row.ID_TYPE1 := r_tmp.ID_TYPE1;
                    r_row.NAME_EXT := r_tmp.NAME_EXT;
                    r_row.CPADDRESS_LINE1 := r_tmp.CPADDRESS_LINE1;
                    r_row.CPADDRESS_LINE2 := r_tmp.CPADDRESS_LINE2;
                    r_row.CPADDRESS_LINE3 := r_tmp.CPADDRESS_LINE3;
                    r_row.CPPOSTCODE := r_tmp.CPPOSTCODE;
                    DBMS_OUTPUT.put_line ('2.v_sql_cl VALUE =' || r_row.ID_TYPE1);
                    pipe row (r_row);
                    
               END LOOP;    
               CLOSE v_cursor2;
       
        END LOOP;    
        
        
            
        CLOSE v_cursor1;
        
        IF('PA'= p_StrReportType) THEN
        DBMS_OUTPUT.put_line ('INSIDE UPDATE =' || p_StrReportType);
        IF(v_cursor_cnt > 0) THEN
        DBMS_OUTPUT.put_line ('INSIDE CURSER1 =' || v_cursor_cnt);       
        DBMS_OUTPUT.put_line ('INSIDE ELSE =' || p_StrReportType);
        BEGIN
                        DBMS_OUTPUT.put_line ('INSIDE UPDATE CAL - ' ||v_cursor_cnt);
                        pc_upd_prn_ind (p_StrBranchCode  , p_StrAccountCodeFrom  , 
                        p_StrAccountCodeTo  , p_StrPolicy  );
                    EXCEPTION
                    WHEN OTHERS
                    THEN
                      -- NULL;
                       DBMS_OUTPUT.put_line ('ERROR pc_upd_prn_ind : ' ||  SQLERRM );
                    END;
                    DBMS_OUTPUT.put_line ('pc_upd_prn_ind - end');
                
         
    END IF;
    END IF;
    
        RETURN ;
       EXCEPTION
          WHEN OTHERS
          THEN
             PG_UTIL_LOG_ERROR.PC_INS_log_error ( v_ProcName_v || '.' || v_ProcName_v, 1, SQLERRM);
    END FN_PREMWARR_CANCE_NOTICE;

    
-- POLICY AND ENDT LISTING FUNCTION
FUNCTION FN_POL_ENDT_UW_LISTING (p_StrReportType         VARCHAR2,
                                 p_StrLob                VARCHAR2,
                                 p_StrPrnBy              VARCHAR2,
                                 p_StrBranchAll          VARCHAR2,
                                 p_StrBranchFrom         VARCHAR2,
                                 p_StrBranchTo           VARCHAR2,
                                 p_StrRegionAll          VARCHAR2,
                                 p_StrRegionFrom         VARCHAR2,
                                 p_StrRegionTo           VARCHAR2,
                                 p_StrTransDtFrom        VARCHAR2,
                                 p_StrTransDtTo          VARCHAR2,
                                 p_StrRiInd              VARCHAR2,
                                 p_StrAmtAll             VARCHAR2,
                                 p_StrGrossPrem          VARCHAR2,
                                 p_StrSIAmtAll           VARCHAR2,
                                 p_StrSumInsured         VARCHAR2,
                                 p_StrFireEclude         VARCHAR2,
                                 p_StrPLExclude          VARCHAR2,
                                 p_StrProcYear           VARCHAR2,
                                 p_StrProcMth            VARCHAR2
                                )
      RETURN RPAC_POL_ENDT_UW_LISTING_TAB
      PIPELINED
      IS
      v_ProcName_v   VARCHAR2 (30) := 'FN_POL_ENDT_UW_LISTING';
      v_Step_v       VARCHAR2 (5)  := '000';
      r_row          RPAC_POL_ENDT_UW_LISTING_REC;
      r_tmp          RPAC_POL_ENDT_UW_LISTING_REC;
      v_sql_v       VARCHAR2(20000);      
      TYPE RptCurTyp  IS REF CURSOR;
      v_cursor1    RptCurTyp;   
      v_lob              VARCHAR2(2);
      lStrPiamCodeEx  VARCHAR2(100) := '(''400100'',''400500'',''400600'',''400800'')';
      lStrPAEx        VARCHAR2(500) := '(''070130'',''070230'',''070105'',''070217'',''070126'',''070226'',''070127'',''070227'',''070128'',''070228'',''070405'')';
      BEGIN
      DBMS_OUTPUT.put_line ('Starting Function execution =' || p_StrReportType);
      FOR r IN
      (SELECT  CODE_CD,CODE_DESC  FROM  CMGE_CODE WHERE CAT_CODE ='MAINCLS' ORDER BY CMGE_CODE.CODE_CD)
      LOOP
      v_lob  :='';
      if('01' = r.CODE_CD) THEN
      v_lob  :='MS';
      elsif('02' = r.CODE_CD) THEN
      v_lob  :='BG';
      elsif('03' = r.CODE_CD) THEN
      v_lob  :='EG';
      elsif('04' = r.CODE_CD) THEN
      v_lob  :='FI';
      elsif('05' = r.CODE_CD) THEN
      v_lob  :='LB';
      elsif('06' = r.CODE_CD) THEN
      v_lob  :='MA';
      elsif('07' = r.CODE_CD) THEN
      v_lob  :='PL';
      elsif('08' = r.CODE_CD) THEN
      v_lob  :='MT';
      elsif('09' = r.CODE_CD) THEN
      v_lob  :='WM';
      elsif('12' = r.CODE_CD) THEN
       v_lob  :='PP';
      end if;
      
      r_row.LOB_CODE := NULL;
      r_row.LOB_DESCP := NULL;
      
      r_row.LOB_CODE := v_lob;
      r_row.LOB_DESCP := r.CODE_DESC;
      
      
      v_sql_v      :='';
      v_sql_v      :=
           'SELECT UWGE_POLICY_BASES.ISSUE_OFFICE, CMDM_BRANCH.BRANCH_NAME,';
           IF('POL' = p_StrReportType) THEN
           v_sql_v := v_sql_v  || ' OCP_POLICY_BASES.POLICY_REF,'
                               || ' '''' ,';
           ELSE
           v_sql_v := v_sql_v  || ' '''' ,'
                            || ' UWGE_POLICY_VERSIONS.ENDT_NO,' ;
           END IF;
           
        v_sql_v := v_sql_v  || ' UWGE_POLICY_BASES.AGENT_CODE||UWGE_POLICY_BASES.AGENT_CAT_TYPE AS AGENT_ID, '
        || ' (SELECT NAME_EXT FROM CPGE_VI_PARTNERS_ALL CP WHERE CP.PART_ID = UWGE_POLICY_BASES.CP_PART_ID AND CP.VERSION = UWGE_POLICY_BASES.CP_VERSION '
        || ' AND ROWNUM = 1) XNAME,'
        || ' UWGE_POLICY_CONTRACTS.PRODUCT_CONFIG_CODE,'
        || ' UWGE_POLICY_BASES.EFF_DATE, UWGE_POLICY_BASES.EXP_DATE, UWGE_POLICY_BASES.RI_IND,';
        
        if(v_lob ='MS' OR v_lob ='FI') THEN 
        if('ENDT' = p_StrReportType) THEN
         v_sql_v := v_sql_v  || ' UWGE_POLICY_BASES.DIFF_POL_SI AS BILL_SI , UWGE_POLICY_BASES.DIFF_GROSS_PREM AS GROSS_PREM, ' ;
        ELSE 
        v_sql_v := v_sql_v  || ' UWGE_POLICY_BASES.BILL_SI AS BILL_SI , UWGE_POLICY_BASES.GROSS_PREM AS GROSS_PREM, ' ;      
        END IF;
        ELSE
        v_sql_v := v_sql_v  || ' UWGE_POLICY_BASES.BILL_SI AS BILL_SI , UWGE_POLICY_BASES.GROSS_PREM AS GROSS_PREM , ' ;   
        END IF;
        
        IF(v_lob ='MS' OR v_lob ='FI' ) THEN 
        v_sql_v := v_sql_v  || ' (UWFI_RISK_LOCATION.ADDRESS_NO || '' '' || CMUW_RISK_ACCUM.ADDRESS_LINE1 || '' '' || '
         || '  CMUW_RISK_ACCUM.ADDRESS_LINE2 || '' '' || CMUW_RISK_ACCUM.ADDRESS_LINE3 || '' '' || CMUW_RISK_ACCUM.POSTCODE ) '
         || '  AS RISK_LOC_DESC,  UWFI_RISK_LOCATION.TERRITORIAL_LIMIT ' 
         || '  AS GEN_DESC,UWFI_RISK_LOCATION.OCCP_CODE AS PIAM, ';
        ELSE
        v_sql_v := v_sql_v  || ' '''','''', '''', ';
        END IF;
        
        IF(v_lob ='MA') THEN
        v_sql_v := v_sql_v  || ' UWMA_RISK.VESSEL_NAME AS VESSEL_NAME ,UWMA_RISK_CARGO.VOYAGE_DETAILS AS VOYAGE_DETAILS,';
        ELSE 
         v_sql_v := v_sql_v  || ' '''', '''', ' ;
        END IF;
        
        v_sql_v := v_sql_v  ||  ' UWGE_POLICY_BASES.POL_SI, ';
        
        if(v_lob ='FI') THEN
        v_sql_v := v_sql_v  ||  ' UWFI_RISK_LOCATION.OCCP_DESCP ';
        ELSE
        v_sql_v := v_sql_v  ||  ' UWGE_POLICY_BASES.OCCUP_DESC ';
        END IF;
        
        IF ('POL' = p_StrReportType) THEN
        v_sql_v := v_sql_v || ' FROM OCP_POLICY_BASES, UWGE_POLICY_CONTRACTS, CMDM_BRANCH, UWGE_POLICY_BASES';
        ELSE 
        v_sql_v := v_sql_v || ' FROM UWGE_POLICY_CONTRACTS, CMDM_BRANCH, UWGE_POLICY_VERSIONS, UWGE_POLICY_BASES';
        END IF;
        
        IF(v_lob ='MS' OR v_lob ='FI') THEN        
        v_sql_v := v_sql_v || ' LEFT OUTER JOIN UWFI_RISK_LOCATION ON UWFI_RISK_LOCATION.CONTRACT_ID = UWGE_POLICY_BASES.CONTRACT_ID AND UWFI_RISK_LOCATION.FLOATING_RISK_IND IS NULL '
                            
                            || ' LEFT OUTER JOIN CMUW_RISK_ACCUM ON CMUW_RISK_ACCUM.RISK_ACCM_CODE = UWFI_RISK_LOCATION.RISK_CODE  ';
         END IF;
         
         IF(v_lob ='MA') THEN         
         v_sql_v := v_sql_v || ' LEFT OUTER JOIN UWMA_RISK ON UWMA_RISK.CONTRACT_ID = UWGE_POLICY_BASES.CONTRACT_ID'
                            || ' LEFT OUTER JOIN UWMA_RISK_CARGO ON UWGE_POLICY_BASES.CONTRACT_ID = UWMA_RISK_CARGO.CONTRACT_ID';
         END IF;
         
              
        IF('POL' = p_StrReportType) THEN
         v_sql_v := v_sql_v  || ' WHERE  UWGE_POLICY_BASES.CONTRACT_ID = OCP_POLICY_BASES.CONTRACT_ID AND UWGE_POLICY_BASES.VERSION_NO = 1 AND OCP_POLICY_BASES.VERSION_NO = 1 AND UWGE_POLICY_BASES.CONTRACT_ID = UWGE_POLICY_CONTRACTS.CONTRACT_ID'
                             || ' AND UWGE_POLICY_BASES.ISSUE_OFFICE = CMDM_BRANCH.BRANCH_CODE';
        ELSE 
         v_sql_v := v_sql_v  || ' WHERE  UWGE_POLICY_VERSIONS.CONTRACT_ID = UWGE_POLICY_BASES.CONTRACT_ID AND UWGE_POLICY_VERSIONS.VERSION_NO = UWGE_POLICY_BASES.VERSION_NO '
                             || ' AND UWGE_POLICY_BASES.CONTRACT_ID = UWGE_POLICY_CONTRACTS.CONTRACT_ID'
                             || ' AND UWGE_POLICY_VERSIONS.VERSION_NO > ''1'' '
                             || ' AND UWGE_POLICY_BASES.ISSUE_OFFICE = CMDM_BRANCH.BRANCH_CODE';
        END IF;
        
         v_sql_v := v_sql_v  || ' AND UWGE_POLICY_CONTRACTS.LOB = '''|| v_lob|| ''' '
        || ' AND UWGE_POLICY_BASES.UW_MTH = TO_NUMBER ('''|| p_StrProcMth ||''') '
        || ' AND UWGE_POLICY_BASES.UW_YEAR = TO_NUMBER('''|| p_StrProcYear || ''') ';
        
        
        IF(p_StrRegionAll IS NOT NULL AND p_StrRegionFrom  IS NULL and p_StrRegionTo IS  NULL)THEN
         v_sql_v := v_sql_v ||' AND CMDM_BRANCH.BRANCH_CODE IN (SELECT DISTINCT CMDM_BRANCH.BRANCH_CODE FROM CMDM_BRANCH WHERE CMDM_BRANCH)';
        END IF;
        
         IF(p_StrRegionAll IS NULL AND p_StrRegionFrom IS NOT NULL AND p_StrRegionTo IS NOT NULL)THEN
         v_sql_v := v_sql_v ||' AND CMDM_BRANCH.BRANCH_CODE IN (SELECT CMDM_BRANCH.BRANCH_CODE FROM CMDM_BRANCH WHERE CMDM_BRANCH.REGION_CODE BETWEEN ''' || p_StrRegionFrom||'''  AND '''||p_StrRegionTo||''' )';
        END IF;
        
        IF(p_StrBranchAll IS NOT NULL AND p_StrBranchFrom  IS NULL AND p_StrBranchTo  IS NULL )THEN
         v_sql_v := v_sql_v ||' AND CMDM_BRANCH.BRANCH_CODE  IN (SELECT DISTINCT CMDM_BRANCH.BRANCH_CODE FROM CMDM_BRANCH)  ';
        END IF;
        
        IF(p_StrBranchAll IS NULL AND p_StrBranchFrom IS NOT NULL AND p_StrBranchTo IS NOT NULL )THEN
         v_sql_v := v_sql_v ||' AND CMDM_BRANCH.BRANCH_CODE  BETWEEN '''|| p_StrBranchFrom ||'''  AND '''|| p_StrBranchTo ||'''  ';
        END IF;
        
        IF(p_StrTransDtFrom IS NOT NULL AND p_StrTransDtTo IS NOT NULL) THEN
         v_sql_v := v_sql_v ||' AND TRUNC(UWGE_POLICY_CONTRACTS.ISSUE_DATE) BETWEEN  TO_DATE ('''||p_StrTransDtFrom||''',''DD/MM/YYYY'')  AND TO_DATE ('''||p_StrTransDtTo||''',''DD/MM/YYYY'') '; 
        END IF;
        
        IF(p_StrRiInd IS NOT NULL ) THEN
         v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.RI_IND = '''|| p_StrRiInd || ''' ' ; 
        END IF;
        
        IF('POL' = p_StrReportType) THEN
        if (p_StrAmtAll='1') Then
          v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.GROSS_PREM = TO_NUMBER('''||p_StrGrossPrem||''') ';
        elsif (p_StrAmtAll='2') Then
          v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.GROSS_PREM < TO_NUMBER('''||p_StrGrossPrem||''') ';
        elsif (p_StrAmtAll='3') Then
          v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.GROSS_PREM <= TO_NUMBER('''||p_StrGrossPrem||''') ';
        elsif (p_StrAmtAll='4') Then
          v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.GROSS_PREM > TO_NUMBER ('''||p_StrGrossPrem||''') ';
        elsif (p_StrAmtAll='5') Then
          v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.GROSS_PREM >= TO_NUMBER ('''||p_StrGrossPrem||''') ';
        end if;
        ELSE
         if (p_StrAmtAll='1') Then
          v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.DIFF_GROSS_PREM = TO_NUMBER('''||p_StrGrossPrem||''') ';
        elsif (p_StrAmtAll='2') Then
          v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.DIFF_GROSS_PREM < TO_NUMBER('''||p_StrGrossPrem||''') ';
        elsif (p_StrAmtAll='3') Then
          v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.DIFF_GROSS_PREM <= TO_NUMBER('''||p_StrGrossPrem||''') ';
        elsif (p_StrAmtAll='4') Then
          v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.DIFF_GROSS_PREM > TO_NUMBER ('''||p_StrGrossPrem||''') ';
        elsif (p_StrAmtAll='5') Then
          v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.DIFF_GROSS_PREM >= TO_NUMBER ('''||p_StrGrossPrem||''') ';
          end if;
        END IF;
        
        IF('POL' = p_StrReportType) THEN
        if (p_StrSIAmtAll='1') Then
         v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.BILL_SI = TO_NUMBER ('''||p_StrSumInsured||''') ';
        elsif (p_StrSIAmtAll='2') Then
         v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.BILL_SI < TO_NUMBER ('''||p_StrSumInsured||''') ';
        elsif (p_StrSIAmtAll='3') Then
         v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.BILL_SI <= TO_NUMBER ('''||p_StrSumInsured||''') ';
        elsif (p_StrSIAmtAll='4') Then
         v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.BILL_SI >  TO_NUMBER ('''||p_StrSumInsured||''') ';
        elsif (p_StrSIAmtAll='5') Then
         v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.BILL_SI >= TO_NUMBER ('''||p_StrSumInsured||''') ';
        end if;
          ELSE
          if (p_StrSIAmtAll='1') Then
         v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.DIFF_POL_SI = TO_NUMBER ('''||p_StrSumInsured||''') ';
        elsif (p_StrSIAmtAll='2') Then
         v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.DIFF_POL_SI < TO_NUMBER ('''||p_StrSumInsured||''') ';
        elsif (p_StrSIAmtAll='3') Then
         v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.DIFF_POL_SI <= TO_NUMBER ('''||p_StrSumInsured||''') ';
        elsif (p_StrSIAmtAll='4') Then
         v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.DIFF_POL_SI >  TO_NUMBER ('''||p_StrSumInsured||''') ';
        elsif (p_StrSIAmtAll='5') Then
         v_sql_v := v_sql_v ||' AND UWGE_POLICY_BASES.DIFF_POL_SI >= TO_NUMBER ('''||p_StrSumInsured||''') ';
         end if;
         END IF;
        
        IF(v_lob ='MS') THEN
        v_sql_v := v_sql_v ||' AND SUBSTR(UWGE_POLICY_CONTRACTS.PRODUCT_CONFIG_CODE, 5) <> ''31'' ';
        END IF;
        
         IF(v_lob ='FI' and p_StrFireEclude IS NOT NULL) THEN
        v_sql_v := v_sql_v ||' AND  (UWFI_RISK_LOCATION.OCCP_CODE NOT IN '||lStrPiamCodeEx||' '
                           ||' OR UWFI_RISK_LOCATION.OCCP_CODE  IN '||lStrPiamCodeEx||'  AND  UWGE_POLICY_BASES.BILL_SI >= 10000000) ';
        END IF;
        
        IF(v_lob ='PL' AND p_StrPLExclude IS NOT NULL) THEN
        v_sql_v := v_sql_v ||' AND UWGE_POLICY_CONTRACTS.PRODUCT_CONFIG_CODE NOT IN '||lStrPAEx||' ';
        END IF;
        
        IF ('POL' = p_StrReportType) THEN
        v_sql_v := v_sql_v ||' ORDER BY OCP_POLICY_BASES.POLICY_REF  ';
        ELSE
        v_sql_v := v_sql_v ||' ORDER BY  UWGE_POLICY_VERSIONS.ENDT_NO ';
        END IF;         
        DBMS_OUTPUT.put_line ('1.v_sql_v =' || v_sql_v);
        
        
        OPEN v_cursor1 FOR v_sql_v;  
        
        
        LOOP
        FETCH v_cursor1
                    INTO r_tmp.ISSUE_OFFICE,r_tmp.BRANCH_NAME,r_tmp.POLICY_REF,r_tmp.ENDT_NO,r_tmp.AGENT_ID,r_tmp.XNAME,r_tmp.PRODUCT_CONFIG_CODE,r_tmp.EFF_DATE,    
                    r_tmp.EXP_DATE,r_tmp.RI_IND,r_tmp.BILL_SI,r_tmp.GROSS_PREM,r_tmp.RISK_LOC_DESC,r_tmp.GEN_DESC,r_tmp.PIAM,r_tmp.VESSEL_NAME,r_tmp.VOYAGE_DETAILS,r_tmp.POL_SI,    
                    r_tmp.OCCUP_DESC;
                    EXIT WHEN v_cursor1%NOTFOUND;
      
         
                r_row.ISSUE_OFFICE := NULL;
                r_row.BRANCH_NAME := NULL;
                r_row.POLICY_REF := NULL;
                r_row.ENDT_NO     := NULL;
                r_row.AGENT_ID    := NULL;
                r_row.XNAME    := NULL;
                r_row.PRODUCT_CONFIG_CODE    := NULL;    
                r_row.EFF_DATE    := NULL;
                r_row.EXP_DATE := NULL;
                r_row.BILL_SI := NULL;
                r_row.GROSS_PREM := NULL;
                r_row.RI_IND := NULL;
                r_row.RISK_LOC_DESC := NULL;        
                r_row.GEN_DESC := NULL;
                r_row.PIAM := NULL;
                r_row.VESSEL_NAME := NULL;
                r_row.VOYAGE_DETAILS := NULL;
                r_row.POL_SI := NULL;
                r_row.OCCUP_DESC := NULL;        
      
                    
                r_row.ISSUE_OFFICE := r_tmp.ISSUE_OFFICE;
                r_row.BRANCH_NAME := r_tmp.BRANCH_NAME;
                r_row.POLICY_REF := r_tmp.POLICY_REF;
                r_row.ENDT_NO     := r_tmp.ENDT_NO;
                r_row.AGENT_ID    := r_tmp.AGENT_ID;
                r_row.XNAME    := r_tmp.XNAME;
                r_row.PRODUCT_CONFIG_CODE    := r_tmp.PRODUCT_CONFIG_CODE;    
                r_row.EFF_DATE    := r_tmp.EFF_DATE;
                r_row.EXP_DATE := r_tmp.EXP_DATE;
                r_row.BILL_SI := r_tmp.BILL_SI;
                r_row.GROSS_PREM := r_tmp.GROSS_PREM;
                r_row.RI_IND := r_tmp.RI_IND;
                r_row.RISK_LOC_DESC := r_tmp.RISK_LOC_DESC;
                r_row.GEN_DESC := r_tmp.GEN_DESC;
                r_row.PIAM := r_tmp.PIAM;
                r_row.VESSEL_NAME := r_tmp.VESSEL_NAME;
                r_row.VOYAGE_DETAILS := r_tmp.VOYAGE_DETAILS;
                r_row.POL_SI := r_tmp.POL_SI;
                r_row.OCCUP_DESC := r_tmp.OCCUP_DESC;
                
                PIPE ROW (r_row);
                       
              
       
        END LOOP;            
        CLOSE v_cursor1;  
       -- PIPE ROW (r_row);        
       
    END LOOP;
        RETURN ;
       EXCEPTION
          WHEN OTHERS
          THEN
             PG_UTIL_LOG_ERROR.PC_INS_log_error ( v_ProcName_v || '.' || v_ProcName_v, 1, SQLERRM);
    END FN_POL_ENDT_UW_LISTING;
--END PG_RPGE_LISTING;



--RPAC_PARTICULAR_GL_CODE Accounting 
   FUNCTION FN_RPAC_PARTICULAR_GL_CODE ( p_StrReportType VARCHAR2, p_yearMonthFrom VARCHAR2, p_yearMonthTo VARCHAR2, p_glPrefixFrom VARCHAR2, p_glPrefixTo VARCHAR2, p_glCode VARCHAR2)
      RETURN RPAC_PARTICULAR_GL_CODE_TAB
      PIPELINED
   IS
      v_ProcName_v   VARCHAR2 (30) := 'FN_RPAC_PARTICULAR_GL_CODE';
      v_Step_v       VARCHAR2 (5) := '000';
      r_row          RPAC_PARTICULAR_GL_CODE_REC;
   BEGIN
      IF p_StrReportType = 'R'
      THEN
         FOR r
            IN (SELECT ACRC_RCPT.TRAN_DATE, ACRC_GL.AC_NO, ACRC_GL.AC_AGENT_ID, ACRC_GL.AC_GLCODE, ACGL_LEDGER.DESCP, ACRC_GL.AC_DB_AMT, 
                    ACRC_GL.AC_CR_AMT, ACRC_RCPT.PROC_YR, ACRC_RCPT.PROC_MTH, ACRC_RCPT.STMT_DESCP
                FROM (ACRC_GL LEFT OUTER JOIN ACGL_LEDGER ON ACRC_GL.AC_GLCODE = ACGL_LEDGER.UKEY)  
                    LEFT OUTER JOIN ACRC_RCPT ON ACRC_GL.AC_NO = ACRC_RCPT.AC_NO
                    WHERE (ACRC_RCPT.PROC_YR *100 ) + ACRC_RCPT.PROC_MTH >= ''||p_yearMonthFrom||''
                        and (ACRC_RCPT.PROC_YR * 100 ) + ACRC_RCPT.PROC_MTH <= ''||p_yearMonthTo||''
                        and ACRC_GL.AC_NO in (SELECT C.AC_NO FROM ACRC_GL C WHERE C.COA = ''||p_glCode||'' 
                        and C.FUND || C.BRANCH||'-'||C.DEPT >=''||p_glPrefixFrom||'' and C.FUND || C.BRANCH ||'-'|| C.DEPT <=''||p_glPrefixTo||'') 
                        ORDER BY ACRC_GL.AC_NO, ACRC_GL.AC_AGENT_ID)
         LOOP
            r_row.TRAN_DATE        := NULL;
            r_row.AC_NO            := NULL;
            r_row.AC_AGENT_ID    := NULL;
            r_row.AC_GLCODE       := NULL;
            r_row.DESCP            := NULL;
            r_row.AC_DB_AMT     := NULL;
            r_row.AC_CR_AMT     := NULL;
            r_row.PROC_YR         := NULL;
            r_row.PROC_MTH         := NULL;
            r_row.STMT_DESCP     := NULL;

            r_row.TRAN_DATE     := r.TRAN_DATE;
            r_row.AC_NO            := r.AC_NO;
            r_row.AC_AGENT_ID    := r.AC_AGENT_ID;
            r_row.AC_GLCODE       := r.AC_GLCODE;
            r_row.DESCP            := r.DESCP;
            r_row.AC_DB_AMT     := r.AC_DB_AMT;
            r_row.AC_CR_AMT     := r.AC_CR_AMT;
            r_row.PROC_YR         := r.PROC_YR;
            r_row.PROC_MTH         := r.PROC_MTH;
            r_row.STMT_DESCP     := r.STMT_DESCP;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_StrReportType = 'P'
      THEN
         FOR r
            IN (SELECT ACPY_PYMT.TRAN_DATE, ACPY_GL.AC_NO, ACPY_GL.AC_AGENT_ID, ACPY_GL.AC_GLCODE, ACGL_LEDGER.DESCP, ACPY_GL.AC_DB_AMT, 
                    ACPY_GL.AC_CR_AMT, ACPY_PYMT.PROC_YR, ACPY_PYMT.PROC_MTH, ACPY_PYMT.STMT_DESCP
                FROM (ACPY_GL LEFT OUTER JOIN ACGL_LEDGER ON ACPY_GL.AC_GLCODE = ACGL_LEDGER.UKEY)  
                    LEFT OUTER JOIN ACPY_PYMT ON ACPY_GL.AC_NO = ACPY_PYMT.AC_NO
                WHERE (ACPY_PYMT.PROC_YR *100 ) + ACPY_PYMT.PROC_MTH >= ''||p_yearMonthFrom||''
                    and (ACPY_PYMT.PROC_YR * 100 ) + ACPY_PYMT.PROC_MTH <= ''||p_yearMonthTo||''
                    and ACPY_GL.AC_NO in (SELECT C.AC_NO FROM ACPY_GL C WHERE C.COA = ''||p_glCode||'' 
                    and C.FUND||C.BRANCH||'-'||C.DEPT >= ''||p_glPrefixFrom||'' and C.FUND||C.BRANCH||'-'||C.DEPT <= ''||p_glPrefixTo||'') 
                    ORDER BY ACPY_GL.AC_NO, ACPY_GL.AC_AGENT_ID)
         LOOP
            r_row.TRAN_DATE        := NULL;
            r_row.AC_NO            := NULL;
            r_row.AC_AGENT_ID    := NULL;
            r_row.AC_GLCODE       := NULL;
            r_row.DESCP            := NULL;
            r_row.AC_DB_AMT     := NULL;
            r_row.AC_CR_AMT     := NULL;
            r_row.PROC_YR         := NULL;
            r_row.PROC_MTH         := NULL;
            r_row.STMT_DESCP     := NULL;

            r_row.TRAN_DATE     := r.TRAN_DATE;
            r_row.AC_NO            := r.AC_NO;
            r_row.AC_AGENT_ID    := r.AC_AGENT_ID;
            r_row.AC_GLCODE       := r.AC_GLCODE;
            r_row.DESCP            := r.DESCP;
            r_row.AC_DB_AMT     := r.AC_DB_AMT;
            r_row.AC_CR_AMT     := r.AC_CR_AMT;
            r_row.PROC_YR         := r.PROC_YR;
            r_row.PROC_MTH         := r.PROC_MTH;
            r_row.STMT_DESCP     := r.STMT_DESCP;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_StrReportType = 'J'
      THEN
         FOR r
            IN (SELECT ACJN_JOUR.TRAN_DATE, ACJN_GL.AC_NO, ACJN_GL.AC_GLCODE, ACJN_GL.AC_AGENT_ID, ACGL_LEDGER.DESCP, ACJN_GL.AC_DB_AMT, 
                    ACJN_GL.AC_CR_AMT, ACJN_JOUR.PROC_YR, ACJN_JOUR.PROC_MTH, ACJN_JOUR.STMT_DESCP
                FROM (ACJN_GL LEFT OUTER JOIN ACGL_LEDGER ON ACJN_GL.AC_GLCODE = ACGL_LEDGER.UKEY)  
                    LEFT OUTER JOIN ACJN_JOUR ON ACJN_GL.AC_NO = ACJN_JOUR.AC_NO
                WHERE (ACJN_JOUR.PROC_YR *100 ) + ACJN_JOUR.PROC_MTH >= ''||p_yearMonthFrom||''
                    and (ACJN_JOUR.PROC_YR * 100 ) + ACJN_JOUR.PROC_MTH <= ''||p_yearMonthTo||''
                    and ACJN_GL.AC_NO in (SELECT C.AC_NO FROM ACJN_GL C WHERE C.COA = ''||p_glCode||'' 
                    and C.FUND||C.BRANCH||'-'||C.DEPT >= ''||p_glPrefixFrom||'' and C.FUND||C.BRANCH||'-'||C.DEPT <= ''||p_glPrefixTo||'') 
                    ORDER BY ACJN_GL.AC_NO, ACJN_GL.AC_AGENT_ID)
         LOOP
            r_row.TRAN_DATE        := NULL;
            r_row.AC_NO            := NULL;
            r_row.AC_AGENT_ID    := NULL;
            r_row.AC_GLCODE       := NULL;
            r_row.DESCP            := NULL;
            r_row.AC_DB_AMT     := NULL;
            r_row.AC_CR_AMT     := NULL;
            r_row.PROC_YR         := NULL;
            r_row.PROC_MTH         := NULL;
            r_row.STMT_DESCP     := NULL;

            r_row.TRAN_DATE     := r.TRAN_DATE;
            r_row.AC_NO            := r.AC_NO;
            r_row.AC_AGENT_ID    := r.AC_AGENT_ID;
            r_row.AC_GLCODE       := r.AC_GLCODE;
            r_row.DESCP            := r.DESCP;
            r_row.AC_DB_AMT     := r.AC_DB_AMT;
            r_row.AC_CR_AMT     := r.AC_CR_AMT;
            r_row.PROC_YR         := r.PROC_YR;
            r_row.PROC_MTH         := r.PROC_MTH;
            r_row.STMT_DESCP     := r.STMT_DESCP;
            
            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_StrReportType = 'D'
      THEN
         FOR r
            IN (SELECT ACDB_NDB.TRAN_DATE, ACDB_GL.AC_NO, ACDB_GL.AC_AGENT_ID, ACDB_GL.AC_GLCODE, ACGL_LEDGER.DESCP, ACDB_GL.AC_DB_AMT, 
                    ACDB_GL.AC_CR_AMT, ACDB_NDB.PROC_YR, ACDB_NDB.PROC_MTH, ACDB_NDB.STMT_DESCP
                FROM (ACDB_GL LEFT OUTER JOIN ACGL_LEDGER ON ACDB_GL.AC_GLCODE = ACGL_LEDGER.UKEY)  
                    LEFT OUTER JOIN ACDB_NDB ON ACDB_GL.AC_NO = ACDB_NDB.AC_NO
                WHERE (ACDB_NDB.PROC_YR *100 ) + ACDB_NDB.PROC_MTH >= ''||p_yearMonthFrom||''
                    and (ACDB_NDB.PROC_YR * 100 ) + ACDB_NDB.PROC_MTH <= ''||p_yearMonthTo||''
                    and ACDB_GL.AC_NO in (SELECT C.AC_NO FROM ACDB_GL C WHERE C.COA = ''||p_glCode||'' 
                    and C.FUND||C.BRANCH||'-'||C.DEPT >= ''||p_glPrefixFrom||'' and C.FUND||C.BRANCH||'-'||C.DEPT <= ''||p_glPrefixTo||'') 
                    ORDER BY ACDB_GL.AC_NO, ACDB_GL.AC_AGENT_ID)
         LOOP
            r_row.TRAN_DATE        := NULL;
            r_row.AC_NO            := NULL;
            r_row.AC_AGENT_ID    := NULL;
            r_row.AC_GLCODE       := NULL;
            r_row.DESCP            := NULL;
            r_row.AC_DB_AMT     := NULL;
            r_row.AC_CR_AMT     := NULL;
            r_row.PROC_YR         := NULL;
            r_row.PROC_MTH         := NULL;
            r_row.STMT_DESCP     := NULL;

            r_row.TRAN_DATE     := r.TRAN_DATE;
            r_row.AC_NO            := r.AC_NO;
            r_row.AC_AGENT_ID    := r.AC_AGENT_ID;
            r_row.AC_GLCODE       := r.AC_GLCODE;
            r_row.DESCP            := r.DESCP;
            r_row.AC_DB_AMT     := r.AC_DB_AMT;
            r_row.AC_CR_AMT     := r.AC_CR_AMT;
            r_row.PROC_YR         := r.PROC_YR;
            r_row.PROC_MTH         := r.PROC_MTH;
            r_row.STMT_DESCP     := r.STMT_DESCP;

            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_StrReportType = 'C'
      THEN
         FOR r
            IN (SELECT ACCR_NCR.TRAN_DATE, ACCR_GL.AC_NO, ACCR_GL.AC_AGENT_ID, ACCR_GL.AC_GLCODE, ACGL_LEDGER.DESCP, ACCR_GL.AC_DB_AMT, 
                    ACCR_GL.AC_CR_AMT, ACCR_NCR.PROC_YR, ACCR_NCR.PROC_MTH, ACCR_NCR.STMT_DESCP
                FROM (ACCR_GL LEFT OUTER JOIN ACCR_NCR ON ACCR_GL.AC_NO = ACCR_NCR.AC_NO)  
                    LEFT OUTER JOIN ACGL_LEDGER ON ACCR_GL.AC_GLCODE = ACGL_LEDGER.UKEY
                WHERE (ACCR_NCR.PROC_YR *100 ) + ACCR_NCR.PROC_MTH >= ''||p_yearMonthFrom||''
                    and (ACCR_NCR.PROC_YR * 100 ) + ACCR_NCR.PROC_MTH <= ''||p_yearMonthTo||''
                    and ACCR_GL.AC_NO in (SELECT C.AC_NO FROM ACCR_GL C WHERE C.COA = ''||p_glCode||'' 
                    and C.FUND||C.BRANCH||'-'||C.DEPT >= ''||p_glPrefixFrom||'' and C.FUND||C.BRANCH||'-'||C.DEPT <= ''||p_glPrefixTo||'') 
                    ORDER BY ACCR_GL.AC_NO, ACCR_GL.AC_AGENT_ID)
         LOOP
            r_row.TRAN_DATE        := NULL;
            r_row.AC_NO            := NULL;
            r_row.AC_AGENT_ID    := NULL;
            r_row.AC_GLCODE       := NULL;
            r_row.DESCP            := NULL;
            r_row.AC_DB_AMT     := NULL;
            r_row.AC_CR_AMT     := NULL;
            r_row.PROC_YR         := NULL;
            r_row.PROC_MTH         := NULL;
            r_row.STMT_DESCP     := NULL;

            r_row.TRAN_DATE     := r.TRAN_DATE;
            r_row.AC_NO            := r.AC_NO;
            r_row.AC_AGENT_ID    := r.AC_AGENT_ID;
            r_row.AC_GLCODE       := r.AC_GLCODE;
            r_row.DESCP            := r.DESCP;
            r_row.AC_DB_AMT     := r.AC_DB_AMT;
            r_row.AC_CR_AMT     := r.AC_CR_AMT;
            r_row.PROC_YR         := r.PROC_YR;
            r_row.PROC_MTH         := r.PROC_MTH;
            r_row.STMT_DESCP     := r.STMT_DESCP;
            
            PIPE ROW (r_row);
         END LOOP;
      ELSIF p_StrReportType = 'O'
      THEN
         FOR r
            IN (SELECT ACPY_PAYLINK.TRAN_DATE, ACPY_PAYLINK_GL.AC_NO, ACPY_PAYLINK_GL.AC_AGENT_ID, ACPY_PAYLINK_GL.AC_GLCODE, ACGL_LEDGER.DESCP, 
                    ACPY_PAYLINK_GL.AC_DB_AMT, ACPY_PAYLINK_GL.AC_CR_AMT, ACPY_PAYLINK.PROC_YR, ACPY_PAYLINK.PROC_MTH, ACPY_PAYLINK.STMT_DESCP
                FROM (ACPY_PAYLINK_GL LEFT OUTER JOIN ACGL_LEDGER ON ACPY_PAYLINK_GL.AC_GLCODE = ACGL_LEDGER.UKEY)  
                    LEFT OUTER JOIN ACPY_PAYLINK   ON ACPY_PAYLINK_GL.AC_NO = ACPY_PAYLINK.AC_NO
                WHERE (ACPY_PAYLINK.PROC_YR *100 ) + ACPY_PAYLINK.PROC_MTH >= ''||p_yearMonthFrom||''
                    and (ACPY_PAYLINK.PROC_YR * 100 ) + ACPY_PAYLINK.PROC_MTH <= ''||p_yearMonthTo||''
                    and ACPY_PAYLINK.AC_NO in (SELECT C.AC_NO FROM ACPY_PAYLINK_GL C WHERE C.COA = ''||p_glCode||'' 
                    and C.FUND||C.BRANCH||'-'||C.DEPT >= ''||p_glPrefixFrom||'' and C.FUND||C.BRANCH||'-'||C.DEPT <= ''||p_glPrefixTo||'') 
                    ORDER BY ACPY_PAYLINK_GL.AC_NO, ACPY_PAYLINK_GL.AC_AGENT_ID)
         LOOP
            r_row.TRAN_DATE        := NULL;
            r_row.AC_NO            := NULL;
            r_row.AC_AGENT_ID    := NULL;
            r_row.AC_GLCODE       := NULL;
            r_row.DESCP            := NULL;
            r_row.AC_DB_AMT     := NULL;
            r_row.AC_CR_AMT     := NULL;
            r_row.PROC_YR         := NULL;
            r_row.PROC_MTH         := NULL;
            r_row.STMT_DESCP     := NULL;

            r_row.TRAN_DATE     := r.TRAN_DATE;
            r_row.AC_NO            := r.AC_NO;
            r_row.AC_AGENT_ID    := r.AC_AGENT_ID;
            r_row.AC_GLCODE       := r.AC_GLCODE;
            r_row.DESCP            := r.DESCP;
            r_row.AC_DB_AMT     := r.AC_DB_AMT;
            r_row.AC_CR_AMT     := r.AC_CR_AMT;
            r_row.PROC_YR         := r.PROC_YR;
            r_row.PROC_MTH         := r.PROC_MTH;
            r_row.STMT_DESCP     := r.STMT_DESCP;
            
            PIPE ROW (r_row);
         END LOOP;
      END IF;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error ( g_k_V_PackageName_v || '.' || v_ProcName_v, 1, SQLERRM);
   END FN_RPAC_PARTICULAR_GL_CODE;

FUNCTION FN_AUTO_COMM_LISTING (p_procMonth IN NUMBER,
                               p_procYear IN NUMBER,
                               p_StrBranch IN VARCHAR2,                               
                               p_strAgentCode IN VARCHAR2)
   RETURN AUTO_COMM_LISTING_TAB
   PIPELINED
IS
   V_PROCNAME_V    VARCHAR2 (30) := 'FN_AUTO_COMM_LISTING';
   r_row           AUTO_COMM_LISTING_REC;
   TYPE v_cursor_type IS REF CURSOR; 
   v_cursor                        v_cursor_type; 
   v_cursor2                        v_cursor_type; 
   lStrSQL varchar2(32757);
   vBATCH_NO varchar2(100);
   vPYMT_AMT  NUMBER (22, 2);
   vAgentCode varchar2(20);
   vPVNo varchar2(30);
   vTranDate varchar2(30);
   vAgtCat  varchar2(5);
   vPOL_NO varchar2(30);
   vCOMM NUMBER (22, 2);
   vGST_COMM NUMBER (22, 2);
   vGROSS_PREM NUMBER (22, 2);
   vNETT_AMT NUMBER (22, 2);
   vAgentId  varchar2(100);
   vChildExist varchar2(2) := 'N';
BEGIN
    
    lStrSQL      := 'SELECT AC_NO, AGENT_CODE  FROM ACPY_AUTOCOMM_AGENT '
                ||' WHERE PROC_YR = '''||p_procYear
                ||''' AND PROC_MTH = '''||p_procMonth             
                ||''' AND STATUS = ''DONE''';             
               
                IF (p_StrBranch IS NOT NULL ) THEN
                lStrSQL := lStrSQL ||' AND BRANCH_CODE = '''|| p_StrBranch ||'''';
                END IF;
                IF (p_strAgentCode IS NOT NULL) THEN
                    lStrSQL := lStrSQL ||' AND AGENT_CODE = '''||p_strAgentCode ||'''';
                END IF;  
                
                lStrSQL := lStrSQL ||' ORDER BY AC_NO,REMARKS';
                
                DBMS_OUTPUT.put_line ('Q1 = '|| lStrSQL);
    OPEN v_cursor FOR lStrSQL;
        
        LOOP
            FETCH v_cursor
            INTO vPVNo,vAgentCode;

            r_row.ISSUE_DATE :=NULL;--< 6.00 Auto_comm_listing >
            r_row.EFF_DATE   :=NULL;--< 6.00 Auto_comm_listing >    

            EXIT WHEN v_cursor%NOTFOUND;
            IF( vPVNo IS NOT NULL) THEN
            
                    r_row.BATCH_NO := null; 
                    r_row.PYMT_AMT := null; 
                    r_row.TRAN_DATE := null; 
                    r_row.AGENT_CAT_TYPE := null; 
                    r_row.POL_NO :=   null; 
                    r_row.COMM := null;
                    r_row.GST_COMM := null;
                    r_row.NETT_AMT := null;
                    r_row.GROSS_PREM := null;
                    vAgentId  :=''; 
                    
                    -- to Q2: GET BATCH_NO,TRAN_DATE,CAT_TYPE                    
                    FOR R IN (SELECT BATCH_NO,ST_AMT,TRUNC(TRAN_DATE) AS TRAN_DATE, MATCH_AGENT_CAT_TYPE,                    
                    (AGENT_CODE||MATCH_AGENT_CAT_TYPE) AS AGENT_ID,MATCH_DOC                   
                    FROM ACST_MAST WHERE ST_DOC = vPVNo)
                   
                    LOOP 
                        r_row.BATCH_NO := R.BATCH_NO; 
                        r_row.PYMT_AMT := R.ST_AMT; 
                        r_row.TRAN_DATE := R.TRAN_DATE; 
                        r_row.AGENT_CAT_TYPE := R.MATCH_AGENT_CAT_TYPE;                         
                        vAgentId := R.AGENT_ID;
                        r_row.MATCH_DOC    :=R.MATCH_DOC;                        
                
                -- FOR COMM,GST_COMM,NETT FROM ACTS_MAST
                    /*    BEGIN
                        SELECT  (COMM1+COMM2) AS COMM,
                        ( GST_COMM + GST_COMM2 )   AS GST_COMM,
                        GROSS  AS GROSS_PREM,
                        NETT  AS NETT_AMT,POL_NO
                        INTO  r_row.COMM,r_row.GST_COMM,r_row.GROSS_PREM,r_row.NETT_AMT,r_row.POL_NO
                        FROM ACST_MAST WHERE ST_DOC = r_row.MATCH_DOC;                                      
                  EXCEPTION
                  WHEN OTHERS
                  THEN                  
                        NULL;
                  END;*/
                  -- FOR SUM OF ALL ST_AMT
                  BEGIN
                  SELECT SUM(ST_AMT) 
                  INTO r_row.PYMT_AMT 
                  FROM ACST_MAST WHERE ST_DOC = vPVNo;
                  EXCEPTION
                  WHEN OTHERS
                  THEN
                        NULL;
                  END;
                  
                        -- FOR COMM PAYABLE LIST
                        
                FOR RP IN 
                        /* (SELECT AT.BRANCH_CODE, 
                         (select BRANCH_NAME from CMDM_BRANCH where CMDM_BRANCH.branch_code = AT.BRANCH_CODE) as BRANCH_DESCP, 
                         AT.AC_NO , AT.AGENT_CODE, TBL1.NAME ,TBL1.SERVICED_BY ,ACPY_LNK.DOC_NO AS P_DOC,ACPY_LNK.INSURED,MTC.DOC_NO AS RCPT_NO, --12.00  
                         --CASE WHEN ACGC_KO_INST.INST_CYCLE IS NOT NULL THEN ACGC_KO_INST.DOC_AMT ELSE MTC.MATCH_DOC_AMT END as MATCH_DOC_AMT, 
                         MTC.MATCH_DOC_AMT,
                         CASE WHEN ACGC_KO_INST.INST_CYCLE IS NOT NULL THEN ACGC_KO_INST.DOC_AMT ELSE ACPY_LNK.DOC_AMT END as DOC_AMT, --12.00  
                         CASE WHEN ACGC_KO_INST.INST_CYCLE IS NOT NULL THEN ACGC_KO_INST.PREM_DUE ELSE ACPY_LNK.PREM_DUE END as PREM_DUE, --12.00  
                         CASE WHEN ACGC_KO_INST.INST_CYCLE IS NOT NULL THEN ACGC_KO_INST.COMM_DUE ELSE ACPY_LNK.COMM_DUE END as COMM_DUE,--12.00  
                         TBL.AUTH_IND,
                         CASE WHEN ACGC_KO_INST.INST_CYCLE IS NOT NULL THEN ACPY_LNK.DOC_NO||':'||ACGC_KO_INST.INST_CYCLE ELSE ACPY_LNK.DOC_NO END as PDOC    --12.00                     
                         FROM   ACPY_AUTOCOMM_AGENT AT,ACPY_PAYLINK_KO ACPY_LNK LEFT OUTER JOIN ACGC_KO_INST ON ACPY_LNK.AC_NO = ACGC_KO_INST.AC_NO AND ACGC_KO_INST.DOC_NO = ACPY_LNK.DOC_NO --12.00  
                         , ACST_MATCH MTC,    
                         (SELECT DISTINCT(DMT.REFERENCE_CODE) AGENTCODE,CP.NAME_EXT NAME,DMAG.SERVICED_BY  
                         FROM DMT_AGENTS DMT,DMAG_AGENTS DMAG, CPGE_VI_PARTNERS_ALL CP 
                         WHERE DMT.INT_ID = DMAG.INT_ID 
                         AND DMAG.PART_ID = CP.PART_ID 
                         AND DMAG.PART_VERSION = CP.VERSION  
                         UNION ALL  
                         SELECT RI.DMT_AGENT_CODE AG_CODE, CP.NAME_EXT NAME,RI.SERVICED_BY NAME  
                         FROM DMAG_RI RI JOIN CPGE_VI_PARTNERS_ALL CP ON RI.PART_ID = CP.PART_ID  
                         AND RI.PART_VERSION = CP.VERSION) TBL1 , 
                         (SELECT AC_NO, TRANSFER_IND, TRANSFER2_IND, TRANSFER_OPERATOR, TRANSFER2_OPERATOR,   
                         CASE WHEN ( TRANSFER_IND = 'Y' AND TRANSFER2_IND = 'Y' AND TRANSFER_OPERATOR = 'ONL_APPROVER1' 
                         AND TRANSFER2_OPERATOR = 'ONL_APPROVER2' ) THEN 'Y' ELSE 'N' END AS AUTH_IND
                         FROM   ACPY_PAYLINK) TBL 
                         WHERE  
                         AT.AC_NO = ACPY_LNK.AC_NO  
                         AND AT.AC_NO = TBL.AC_NO  
                         AND TBL1.AGENTCODE = AT.AGENT_CODE  
                         AND MTC.MATCH_DOC_NO = ACPY_LNK.DOC_NO  
                         AND MTC.DOC_NO <> AT.AC_NO 
                         AND MTC.ST_SEQ_NO IN (SElECT MAX(ST_SEQ_NO) FROM ACST_MATCH WHERE MATCH_DOC_NO =  ACPY_LNK.DOC_NO  and DOC_NO <> AT.AC_NO)                
                         AND AT.AC_NO IS NOT NULL 
                         AND AT.PROC_MTH = p_procMonth
                         AND AT.PROC_YR = p_procYear
                         AND AT.AC_NO =  vPVNo
                         AND ACPY_LNK.AGENT_ID = vAgentId)      */
                         
                         (SELECT AT.BRANCH_CODE, 
                         (select BRANCH_NAME from CMDM_BRANCH where CMDM_BRANCH.branch_code = AT.BRANCH_CODE) as BRANCH_DESCP, 
                        AT.AC_NO , AT.AGENT_CODE, TBL1.NAME ,TBL1.SERVICED_BY ,ACPY_LNK.DOC_NO AS P_DOC,ACPY_LNK.INSURED,MTC.DOC_NO AS RCPT_NO, --12.00  
                        MTC.MATCH_DOC_AMT,
                        CASE WHEN ACGC_KO_INST.INST_CYCLE IS NOT NULL THEN ACGC_KO_INST.DOC_AMT  ELSE ACPY_LNK.DOC_AMT END as DOC_AMT, --12.00  
                        CASE WHEN ACGC_KO_INST.INST_CYCLE IS NOT NULL THEN ACGC_KO_INST.PREM_DUE ELSE ACPY_LNK.PREM_DUE END as PREM_DUE, --12.00  
                        CASE WHEN ACGC_KO_INST.INST_CYCLE IS NOT NULL THEN ACGC_KO_INST.COMM_DUE ELSE ACPY_LNK.COMM_DUE END as COMM_DUE,--12.00  
                        TBL.AUTH_IND,
                        CASE WHEN ACGC_KO_INST.INST_CYCLE IS NOT NULL THEN ACPY_LNK.DOC_NO||':'||ACGC_KO_INST.INST_CYCLE ELSE ACPY_LNK.DOC_NO END as PDOC    --12.00                     
                        FROM ACPY_AUTOCOMM_AGENT AT,ACPY_PAYLINK_KO ACPY_LNK LEFT OUTER JOIN ACGC_KO_INST ON ACPY_LNK.AC_NO = ACGC_KO_INST.AC_NO AND ACGC_KO_INST.DOC_NO = ACPY_LNK.DOC_NO --12.00
                        , ( SELECT 0 AS INST_CYCLE,ASMT.DOC_NO,ASMT.MATCH_DOC_NO,ASMT.ST_SEQ_NO,ASMT.MATCH_DOC_AMT
                            FROM ACST_MATCH ASMT WHERE NOT EXISTS (SELECT 1 FROM ACGC_KO_INST INST WHERE INST.AC_NO = ASMT.DOC_NO AND INST.DOC_NO = ASMT.MATCH_DOC_NO)
                            UNION ALL
                            SELECT INST.INST_CYCLE,INST.AC_NO AS DOC_NO,INST.DOC_NO AS MATCH_DOC_NO,ASMT.ST_SEQ_NO,SUM(NVL(INST.DOC_AMT,0)) AS MATCH_DOC_AMT
                            FROM ACGC_KO_INST INST,ACST_MATCH ASMT WHERE INST.AC_NO = ASMT.DOC_NO AND INST.DOC_NO = ASMT.MATCH_DOC_NO
                            GROUP BY INST.INST_CYCLE,INST.AC_NO,INST.DOC_NO,ASMT.ST_SEQ_NO) MTC,
                         (SELECT DISTINCT(DMT.REFERENCE_CODE) AGENTCODE,CP.NAME_EXT NAME,DMAG.SERVICED_BY  
                         FROM DMT_AGENTS DMT,DMAG_AGENTS DMAG, CPGE_VI_PARTNERS_ALL CP 
                         WHERE DMT.INT_ID = DMAG.INT_ID 
                         AND DMAG.PART_ID = CP.PART_ID 
                         AND DMAG.PART_VERSION = CP.VERSION  
                         UNION ALL  
                         SELECT RI.DMT_AGENT_CODE AG_CODE, CP.NAME_EXT NAME,RI.SERVICED_BY NAME  
                         FROM DMAG_RI RI JOIN CPGE_VI_PARTNERS_ALL CP ON RI.PART_ID = CP.PART_ID  
                        AND RI.PART_VERSION = CP.VERSION) TBL1 , 
                         (SELECT AC_NO, TRANSFER_IND, TRANSFER2_IND, TRANSFER_OPERATOR, TRANSFER2_OPERATOR,   
                         CASE WHEN ( TRANSFER_IND = 'Y' AND TRANSFER2_IND = 'Y' AND TRANSFER_OPERATOR = 'ONL_APPROVER1' 
                         AND TRANSFER2_OPERATOR = 'ONL_APPROVER2' ) THEN 'Y' ELSE 'N' END AS AUTH_IND
                         FROM   ACPY_PAYLINK) TBL 
                         WHERE  
                         AT.AC_NO = ACPY_LNK.AC_NO  
                         AND AT.AC_NO = TBL.AC_NO  
                         AND TBL1.AGENTCODE = AT.AGENT_CODE  
                         AND MTC.MATCH_DOC_NO = ACPY_LNK.DOC_NO  
                        AND MTC.INST_CYCLE      = NVL(ACGC_KO_INST.INST_CYCLE,0)
                         AND MTC.DOC_NO <> AT.AC_NO 
                        AND MTC.ST_SEQ_NO        = NVL((SElECT MAX(ASMT2.ST_SEQ_NO) FROM ACGC_KO_INST INST2,ACST_MATCH ASMT2 
                                                       WHERE  INST2.AC_NO = ASMT2.DOC_NO AND INST2.DOC_NO = ASMT2.MATCH_DOC_NO
                                                       AND INST2.DOC_NO = ACPY_LNK.DOC_NO 
                                                       AND INST2.INST_CYCLE = ACGC_KO_INST.INST_CYCLE AND INST2.AC_NO <> AT.AC_NO),
                                                      (SElECT MAX(ST_SEQ_NO) FROM ACST_MATCH WHERE MATCH_DOC_NO = ACPY_LNK.DOC_NO  and DOC_NO <> AT.AC_NO))             
                         AND AT.AC_NO IS NOT NULL 
                         AND AT.PROC_MTH = p_procMonth
                         AND AT.PROC_YR = p_procYear
                         AND AT.AC_NO =  vPVNo
                         AND ACPY_LNK.AGENT_ID = vAgentId)             
                                  
                    LOOP                        

                        r_row.ISSUE_DATE :=NULL;--< 6.00 Auto_comm_listing >
                        r_row.EFF_DATE   :=NULL;--< 6.00 Auto_comm_listing >                    
                        vChildExist :='Y';
                        r_row.BRANCH_CODE := RP.BRANCH_CODE;
                        r_row.BRANCH_DESCP := RP.BRANCH_DESCP;
                        r_row.PAYMENT_NO := RP.AC_NO;
                        r_row.AGENT_CODE := RP.AGENT_CODE;
                        r_row.AGENT_NAME := RP.NAME; 
                        r_row.MARKETER := RP.SERVICED_BY;
                        r_row.DOC_NO := RP.PDOC;
                        r_row.NAME := RP.INSURED;
                        r_row.RCPT_NO:= RP.RCPT_NO;
                        r_row.RCPT_AMT := RP.MATCH_DOC_AMT;
                        r_row.OS_NETT_AMT := RP.DOC_AMT;
                        r_row.OS_PREM_DUE := RP.PREM_DUE;
                        r_row.OS_COMM_DUE := RP.COMM_DUE;
                        r_row.AUTH_IND := RP.AUTH_IND;
                        r_row.IS_PRN :='N';
                    
                    -- FOR COMM,GST_COMM,NETT FROM ACTS_MAST
                 BEGIN
                        --12.00    start
                        SELECT COMM,GST_COMM, GROSS_PREM, NETT_AMT, POL_NO, CNOTE_NO, VEH_NO 
                        INTO  r_row.COMM,r_row.GST_COMM,r_row.GROSS_PREM,r_row.NETT_AMT,r_row.POL_NO,r_row.CNOTE_NO,r_row.VEH_NO
                        FROM (
                        --12.00  end
                        SELECT  (COMM1+COMM2) AS COMM,
                        ( GST_COMM + GST_COMM2 )   AS GST_COMM,
                        GROSS  AS GROSS_PREM,
                        NETT  AS NETT_AMT,POL_NO,CNOTE_NO,VEH_NO
                        FROM ACST_MAST WHERE ST_DOC = RP.P_DOC
                        --12.00    start
                        AND INST_IND = 'N'
                        UNION ALL
                        SELECT  (COMM1+COMM2) AS COMM,
                        ( GST_COMM + GST_COMM2 )   AS GST_COMM,
                        GROSS  AS GROSS_PREM,
                        NETT  AS NETT_AMT,POL_NO,CNOTE_NO,VEH_NO
                        FROM ACST_INST WHERE ST_DOC_INST = RP.PDOC);   
                        --12.00 end                                   
                          EXCEPTION
                          WHEN OTHERS
                          THEN                  
                                NULL;
                  END;
                  
                      --< 6.00 Auto_comm_listing START>
                      BEGIN
                        BEGIN
                          SELECT TRUNC(UPV.ISSUE_DATE),
                            TRUNC(UPV.ENDT_EFF_DATE)
                          INTO R_ROW.ISSUE_DATE,
                            r_row.EFF_DATE
                          FROM UWGE_POLICY_VERSIONS UPV
                          WHERE ENDT_NO = RP.P_DOC; --12.00
                        EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                          BEGIN
                            SELECT TRUNC(C.ISSUE_DATE)ISSUE_DATE,
                              TRUNC(WPB.EFF_DATE)EFF_DATE
                            INTO r_row.ISSUE_DATE,
                              r_row.EFF_DATE
                            FROM OCP_POLICY_BASES OPB,
                              UWGE_POLICY_BASES WPB,
                              UWGE_POLICY_CONTRACTS C
                            WHERE OPB.CONTRACT_ID=WPB.CONTRACT_ID
                            AND WPB.TOP_INDICATOR='Y'
                            AND OPB.CONTRACT_ID  =C.CONTRACT_ID
                            AND POLICY_REF       = RP.P_DOC; --12.00
                          EXCEPTION
                          WHEN OTHERS THEN
                            NULL;
                          END;
                        END;
                      EXCEPTION
                      WHEN OTHERS THEN
                        NULL;
                      END; --< 6.00 Auto_comm_listing END>
                                

                        DBMS_OUTPUT.put_line ('Q2= 3');
                        PIPE ROW(r_row);
                    END LOOP;                    
                    IF(vChildExist ='N') THEN     
                    PIPE ROW (r_row);  
                    END IF;
                    END LOOP;
                    
            ELSE 
            
            if(vAgentCode is not null) then 
                     -- FOR CR>CP
                    FOR CPCR IN 
                            (SELECT AT.BRANCH_CODE, 
                            (select BRANCH_NAME from CMDM_BRANCH where CMDM_BRANCH.branch_code = AT.BRANCH_CODE) as BRANCH_DESCP, 
                            AT.REMARKS,AT.AGENT_CODE,TBL1.NAME,TBL1.SERVICED_BY 
                            FROM   ACPY_AUTOCOMM_AGENT AT,
                            (SELECT DISTINCT(DMT.REFERENCE_CODE) AGENTCODE,CP.NAME_EXT NAME,DMAG.SERVICED_BY  
                            FROM DMT_AGENTS DMT,DMAG_AGENTS DMAG, CPGE_VI_PARTNERS_ALL CP 
                            WHERE DMT.INT_ID = DMAG.INT_ID 
                            AND DMAG.PART_ID = CP.PART_ID 
                            AND DMAG.PART_VERSION = CP.VERSION  
                            UNION ALL  
                            SELECT RI.DMT_AGENT_CODE AG_CODE, CP.NAME_EXT NAME,RI.SERVICED_BY NAME  
                            FROM DMAG_RI RI JOIN CPGE_VI_PARTNERS_ALL CP ON RI.PART_ID = CP.PART_ID  
                            AND RI.PART_VERSION = CP.VERSION   
                            LEFT JOIN CMDM_BRANCH BR ON BR.BRANCH_CODE = RI.BRANCH) TBL1  
                            WHERE 
                            TBL1.AGENTCODE = AT.AGENT_CODE             
                            AND AT.AC_NO  IS NULL 
                            AND AT.REMARKS ='CR>CP' -- Redmine 120681
                            AND AT.PROC_MTH = p_procMonth
                            AND AT.PROC_YR = p_procYear 
                            AND AT.AGENT_CODE = vAgentCode)                 
                    
                
                    LOOP                       
                        r_row.BATCH_NO := 'NA'; 
                        r_row.PYMT_AMT := null;                        
                        r_row.BRANCH_CODE := CPCR.BRANCH_CODE;
                        r_row.BRANCH_DESCP := CPCR.BRANCH_DESCP;
                        r_row.PAYMENT_NO := CPCR.REMARKS;
                        r_row.AGENT_CODE := CPCR.AGENT_CODE;
                        r_row.AGENT_NAME := CPCR.NAME; 
                        r_row.MARKETER := CPCR.SERVICED_BY;
                        r_row.DOC_NO := '';
                        r_row.NAME := '';
                        r_row.RCPT_NO:= '';
                        r_row.RCPT_AMT := null;
                        r_row.OS_NETT_AMT := null;
                        r_row.OS_PREM_DUE := null;
                        r_row.OS_COMM_DUE := null;
                        r_row.AUTH_IND := '';                        
                        r_row.TRAN_DATE := null; 
                        r_row.AGENT_CAT_TYPE := '';
                        r_row.COMM := null;
                        r_row.GST_COMM := null;
                        r_row.GROSS_PREM := null;
                        r_row.NETT_AMT := null;
                        r_row.POL_NO :='';                
                        r_row.IS_PRN :='Y';
                        r_row.CNOTE_NO := null;--2.02
                        r_row.VEH_NO := null; --2.02
                        PIPE ROW(r_row);
                    END LOOP;
                        
                        -- FOR NO STAMT
                    FOR ST IN 
                            (SELECT AT.BRANCH_CODE, 
                            (select BRANCH_NAME from CMDM_BRANCH where CMDM_BRANCH.branch_code = AT.BRANCH_CODE) as BRANCH_DESCP, 
                            AT.REMARKS,AT.AGENT_CODE,TBL1.NAME,TBL1.SERVICED_BY 
                            FROM   ACPY_AUTOCOMM_AGENT AT,
                            (SELECT DISTINCT(DMT.REFERENCE_CODE) AGENTCODE,CP.NAME_EXT NAME,DMAG.SERVICED_BY  
                            FROM DMT_AGENTS DMT,DMAG_AGENTS DMAG, CPGE_VI_PARTNERS_ALL CP 
                            WHERE DMT.INT_ID = DMAG.INT_ID 
                            AND DMAG.PART_ID = CP.PART_ID 
                            AND DMAG.PART_VERSION = CP.VERSION  
                            UNION ALL  
                            SELECT RI.DMT_AGENT_CODE AG_CODE, CP.NAME_EXT NAME,RI.SERVICED_BY NAME  
                            FROM DMAG_RI RI JOIN CPGE_VI_PARTNERS_ALL CP ON RI.PART_ID = CP.PART_ID  
                            AND RI.PART_VERSION = CP.VERSION   
                            LEFT JOIN CMDM_BRANCH BR ON BR.BRANCH_CODE = RI.BRANCH) TBL1  
                            WHERE 
                            TBL1.AGENTCODE = AT.AGENT_CODE             
                            AND AT.AC_NO  IS NULL 
                            AND AT.REMARKS ='No Stmt Matched'
                            AND AT.PROC_MTH = p_procMonth
                            AND AT.PROC_YR = p_procYear 
                            AND AT.AGENT_CODE = vAgentCode)                 
                    
                
                    LOOP                       
                        r_row.BATCH_NO := 'NA'; 
                        r_row.PYMT_AMT := null;                        
                        r_row.BRANCH_CODE := ST.BRANCH_CODE;
                        r_row.BRANCH_DESCP := ST.BRANCH_DESCP;
                        r_row.PAYMENT_NO := ST.REMARKS;
                        r_row.AGENT_CODE := ST.AGENT_CODE;
                        r_row.AGENT_NAME := ST.NAME; 
                        r_row.MARKETER := ST.SERVICED_BY;
                        r_row.DOC_NO := '';
                        r_row.NAME := '';
                        r_row.RCPT_NO:= '';
                        r_row.RCPT_AMT := null;
                        r_row.OS_NETT_AMT := null;
                        r_row.OS_PREM_DUE := null;
                        r_row.OS_COMM_DUE := null;
                        r_row.AUTH_IND := '';                        
                        r_row.TRAN_DATE := null; 
                        r_row.AGENT_CAT_TYPE := '';
                        r_row.COMM := null;
                        r_row.GST_COMM := null;
                        r_row.GROSS_PREM := null;
                        r_row.NETT_AMT := null;
                        r_row.POL_NO :='';                
                        r_row.IS_PRN :='Y';
                        r_row.CNOTE_NO := null; --2.02
                        r_row.VEH_NO := null; --2.02
                        PIPE ROW(r_row);
                    END LOOP;
            END IF;
            END IF;
    END LOOP;
    CLOSE v_cursor;
    
   RETURN;
EXCEPTION
   WHEN OTHERS
   THEN
      PG_UTIL_LOG_ERROR.PC_INS_log_error (
         v_ProcName_v || '.' || v_ProcName_v,
         1,
         SQLERRM);
END FN_AUTO_COMM_LISTING;
FUNCTION FN_AUTO_COMM_LISTING_ERR (p_procMonth IN NUMBER,
                               p_procYear IN NUMBER,
                               p_StrBranch IN VARCHAR2,                               
                               p_strAgentCode IN VARCHAR2)
   RETURN AUTO_COMM_LISTING_ERR_TAB
   PIPELINED
IS
   V_PROCNAME_V    VARCHAR2 (30) := 'FN_AUTO_COMM_LISTING_ERR';
   R_ROW           AUTO_COMM_LISTING_ERR_REC;


   
BEGIN
   FOR r
      IN ( SELECT AT.BRANCH_CODE,
            (select BRANCH_NAME from CMDM_BRANCH where CMDM_BRANCH.branch_code = AT.BRANCH_CODE) as BRANCH_DESCP,
               AT.AGENT_CODE,
                TBL1.NAME AS AGENT_NAME, 
                 'Issue Manually'                   AS ACTION
         FROM   ACPY_AUTOCOMM_AGENT AT, 
               (SELECT DISTINCT(DMT.REFERENCE_CODE) AGENTCODE,CP.NAME_EXT NAME
                  FROM DMT_AGENTS DMT,DMAG_AGENTS DMAG, CPGE_VI_PARTNERS_ALL CP
                  WHERE DMT.INT_ID = DMAG.INT_ID
                  AND DMAG.PART_ID = CP.PART_ID
                  AND DMAG.PART_VERSION = CP.VERSION 
                   UNION ALL
                  SELECT RI.DMT_AGENT_CODE AG_CODE, CP.NAME_EXT NAME
                  FROM DMAG_RI RI JOIN CPGE_VI_PARTNERS_ALL CP ON RI.PART_ID = CP.PART_ID
                  AND RI.PART_VERSION = CP.VERSION 
                  LEFT JOIN CMDM_BRANCH BR ON BR.BRANCH_CODE = RI.BRANCH) TBL1 
                /*(SELECT AC_NO, 
                        TRANSFER_IND, 
                        TRANSFER2_IND, 
                        TRANSFER_OPERATOR, 
                        TRANSFER2_OPERATOR, 
                        CASE 
                          WHEN ( TRANSFER_IND = 'Y' 
                                 AND TRANSFER2_IND = 'Y' 
                                 AND TRANSFER_OPERATOR = 'ONL_APPROVER1' 
                                 AND TRANSFER2_OPERATOR = 'ONL_APPROVER2' ) THEN 
                          'Y' 
                          ELSE 'N' 
                        END AS AUTH_IND 
                 FROM   ACPY_PAYLINK) TBL */
         WHERE  --AT.AC_NO = TBL.AC_NO 
                --AND 
                TBL1.AGENTCODE = AT.AGENT_CODE
                AND AT.STATUS = 'ERR' 
                --AND AT.AC_NO IS NOT NULL
              AND  ( ( p_StrBranch IS NOT NULL AND AT.BRANCH_CODE = p_StrBranch ) OR p_StrBranch IS NULL ) 
              AND  ( ( p_strAgentCode IS NOT NULL AND AT.AGENT_CODE = p_strAgentCode ) OR p_strAgentCode IS NULL )
              AND AT.PROC_MTH = p_procMonth
              AND AT.PROC_YR = p_procYear )
   LOOP

        r_row.BRANCH_CODE := r.BRANCH_CODE;
      r_row.BRANCH_DESCP := r.BRANCH_DESCP;
      r_row.AGENT_CODE := r.AGENT_CODE;
      r_row.AGENT_NAME := r.AGENT_NAME;
      r_row.ACTION := r.ACTION;

      PIPE ROW (r_row);
   END LOOP;
   


   RETURN;
EXCEPTION
   WHEN OTHERS
   THEN
      PG_UTIL_LOG_ERROR.PC_INS_log_error (
         v_ProcName_v || '.' || v_ProcName_v,
         1,
         SQLERRM);
END FN_AUTO_COMM_LISTING_ERR;
--start 7.0
FUNCTION FN_SURVEYS_LISTING(
    p_ReportName    IN VARCHAR2,
    p_LOB           IN VARCHAR2,
    p_Product       IN VARCHAR2,
    p_Region        IN VARCHAR2,
    p_Branch        IN VARCHAR2,
    p_AgentChannel  IN VARCHAR2,
    p_PolicyStatus  IN VARCHAR2,
    p_IssueDateFrom IN VARCHAR2,
    p_IssueDateTo   IN VARCHAR2)
  RETURN SURVEYS_LISTING_TAB PIPELINED
IS
  V_PROCNAME_V VARCHAR2 (30) := 'FN_SURVEYS_LISTING';
  V_STEPS      VARCHAR2 (10) :='000';
  r_row SURVEYS_LISTING_REC;
  SURVEYS_DET PG_RPGE_LISTING.SURVEYS_LISTING_TAB;
  ISVALID_REC  VARCHAR2 (1) :='N';
  lStrSQL      VARCHAR2(32757);
  lStrWhereSQL VARCHAR2(32757);
  lStrWIPSQL   VARCHAR2(32757);
  lStrOCPSQL   VARCHAR2(32757);
  v_OCCP_REF_RISK SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'OCCP_REF_RISK');
  v_OCCP_DEC_RISK SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'OCCP_DEC_RISK');
  v_SUB_OCCP SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE      := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'SUB_OCCP');
  v_OCCP_TW SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE       := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'OCCP_TW');
  v_OCCP_CON_1 SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE    := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'OCCP_CON_1');
  v_OCCP_CON_11 SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE   := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'OCCP_CON_11');
  v_OCCP_CON_2 SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE    := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'OCCP_CON_2');
  v_OCCP_CON_22 SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE   := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'OCCP_CON_22');
  -- 8.01 start
  v_OCCP_REF_RISK_PP SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'OCCP_REF_RISK_PP');
  v_OCCP_DEC_RISK_PP SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'OCCP_DEC_RISK_PP');
  v_OCCP_CON_PP_1 SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE    := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'OCCP_CON_PP_1');
  v_OCCP_CON_PP_11 SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE   := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'OCCP_CON_PP_11');
  v_OCCP_CON_PP_2 SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE    := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'OCCP_CON_PP_2');
  v_OCCP_CON_PP_22 SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE   := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'OCCP_CON_PP_22');
  --8.01 end 
  V_SURVEY_CONDUCT_IND UWGE_POLICY_SURVEY_HDR.SURVEY_CONDUCT_IND%TYPE;
  V_SURVEY_CONDUCT_IND_DESC CMGE_CODE.CODE_DESC%TYPE;
  V_SURVEY_DATE UWGE_POLICY_SURVEY_HDR.SURVEY_DATE%TYPE;
  V_SURVEY_REVIEW_DATE UWGE_POLICY_SURVEY_HDR.SURVEY_REVIEW_DATE%TYPE;
  V_SURVEY_CONDUCT_BY UWGE_POLICY_SURVEY_HDR.SURVEY_CONDUCT_BY%TYPE;
  V_SURVEY_CONDUCT_BY_DESC CMGE_CODE.CODE_DESC%TYPE;
  V_APPLIED_ON_RISK VARCHAR2 (400);
  V_SURVEY_YEAR     NUMBER;
  V_REFER_ADMIN_REMARKS CNUW_BASES.REFER_ADMIN_REMARKS%TYPE;
  V_RISK_SI    UWGE_RISK.RISK_SI%TYPE;
  -- 29.00 start
  v_CONSTRUCTION_CODE SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE    := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'CONSTRUCTION_CLASS');
  -- 29.00 end
  v_RES_OCCP_CODE SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE    := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'RES_OCCP_CODE'); --35.00
  v_NRD_OCCP_CODE_1  SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE    := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'NRD_OCCP_CODE_1'); --35.00
  v_NRD_OCCP_CODE_11 SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE    := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_RPGE', 'NRD_OCCP_CODE_11'); --35.00
BEGIN
  V_STEPS    := '001';
  lStrWIPSQL :=
  'SELECT '''' POLICY_REF,
UPV.VERSION_NO,
UPV.CONTRACT_ID,
UPV.REF_NO,
UPB.ISSUE_OFFICE AS BRANCH_CODE,
(SELECT CC.BRANCH_NAME
FROM CMDM_BRANCH CC
WHERE  CC.BRANCH_CODE=UPB.ISSUE_OFFICE
) AS BRANCH_NAME,
UPC.PRODUCT_CONFIG_CODE AS PRODUCT_CODE,
(SELECT CC.CODE_DESC
FROM CMGE_CODE CC
WHERE CC.CAT_CODE=UPC.LOB
||''_PRODUCT''
AND CC.CODE_CD=UPC.PRODUCT_CONFIG_CODE
) AS PRODUCT_DESC,
UPB.AGENT_CODE,
(select CP.NAME_EXT  FROM CPGE_VI_PARTNERS_ALL CP
   where AGENT_TBL.PART_ID      = CP.PART_ID
  AND AGENT_TBL.PART_VERSION = CP.VERSION) AS AGENT_NAME,
UPB.AGENT_SOURCE_ID,
(SELECT CP.NAME_EXT FROM CUSTOMER.CPGE_VI_PARTNERS_ALL CP WHERE CP.PART_ID = UPB.CP_PART_ID AND CP.VERSION = UPB.CP_VERSION) AS CP_NAME,
UPB.LONG_NAME,
UPB.EFF_DATE,
UPB.EXP_DATE,
''W'' POLICY_STATUS,
''WIP'' AS POLICY_STATUS_DESC,
UL.OCCP_CODE,
UL.OCCP_DESCP,
UL.SUB_OCCP_CODE,
(SELECT CC.CODE_DESC
FROM CMGE_CODE CC
WHERE CC.CAT_CODE=''SUB_OCCUPATION_CODE''
AND CC.CODE_CD=UL.SUB_OCCP_CODE
) AS SUB_OCCP_CODE_DESC,
UL.CONSTRUCTION_CODE,
UL.RISK_CODE,
REPLACE ( REPLACE (UL.ADDRESS_NO, CHR (10), CHR (32)), CHR (13), ''!!'')  ||'' ''||(select REPLACE ( REPLACE (CRA.ADDRESS_LINE1, CHR (10), CHR (32)), CHR (13), ''!!'')||'' ''||
REPLACE ( REPLACE (CRA.ADDRESS_LINE2, CHR (10), CHR (32)), CHR (13), ''!!'')||'' ''||
REPLACE ( REPLACE (CRA.ADDRESS_LINE3, CHR (10), CHR (32)), CHR (13), ''!!'')||'' ''||
( SELECT CC.CODE_DESC FROM CMGE_CODE CC WHERE CRA.CITY = CC.CODE_CD AND CC.CAT_CODE = ''CITY''  and rownum = 1)||'' ''||
( SELECT CC.CODE_DESC FROM CMGE_CODE CC WHERE CRA.STATE = CC.CODE_CD AND CC.CAT_CODE = ''STATE''  and rownum = 1)||'' ''||
( SELECT CC.CODE_DESC FROM CMGE_CODE CC WHERE CRA.COUNTRY = CC.CODE_CD AND CC.CAT_CODE = ''COUNTRY''  and rownum = 1)
AS ADDS from CMUW_RISK_ACCUM CRA WHERE CRA.RISK_ACCM_CODE=UL.RISK_CODE) AS RISK_LOCATION,
(select CRA.POSTCODE from CMUW_RISK_ACCUM CRA WHERE CRA.RISK_ACCM_CODE=UL.RISK_CODE) AS POSTCODE,
UR.SEQ_NO AS RISK_NO,
CASE WHEN LOB=''PP'' THEN (SELECT SUM(COV_SI) FROM WIP_UWGE_COVER UC WHERE  UR.CONTRACT_ID = UC.CONTRACT_ID
AND UR.RISK_ID = UC.RISK_ID  AND COV_CODE IN (''01'',''02'')) ELSE UR.RISK_SI END AS RISK_SI,
CASE WHEN AGENT_TBL.AGENT_TYPE =''RI'' THEN (SELECT INW_PCT FROM WIP_UWGE_POLICY_INW UPI WHERE UPI.CONTRACT_ID=UR.CONTRACT_ID  AND ROWNUM=1) ELSE 0 END  AS INW_PCT,UR.RISK_ID,
(select distinct UPSD.SURVEY_ID from WIP_UWGE_POLICY_SURVEY_DET UPSD where UPSD.CONTRACT_ID = UR.CONTRACT_ID
AND UPSD.VERSION_NO = UR.VERSION_NO AND UPSD.RISK_ID=UR.RISK_ID and rownum=1) as SURVEY_ID,
null AS SURVEY_CONDUCT_IND,
null AS SURVEY_CONDUCT_IND_DESC,
null AS SURVEY_DATE,
null AS SURVEY_CONDUCT_BY,
null AS SURVEY_CONDUCT_BY_DESC,
null AS SURVEY_REVIEW_DATE,
null AS APPLIED_ON_RISK,
null AS REMARKS
FROM WIP_UWGE_POLICY_CONTRACTS UPC,
WIP_UWGE_POLICY_VERSIONS UPV,
WIP_UWGE_POLICY_BASES UPB ,
WIP_UWGE_RISK UR,
WIP_UWFI_RISK_LOCATION UL,
(SELECT DISTINCT(DMT.REFERENCE_CODE) AGENT_CODE,
      DMAG.PART_ID,
      DMAG.PART_VERSION,
      ''DMAG'' AGENT_TYPE,
          DMAG.CHANNEL
    FROM DMT_AGENTS DMT,
      DMAG_AGENTS DMAG
    WHERE DMT.INT_ID      = DMAG.INT_ID
    UNION ALL
    SELECT RI.DMT_AGENT_CODE AGENT_CODE,
      RI.PART_ID,
      RI.PART_VERSION,
      ''RI'' AGENT_TYPE,
          ''0'' CHANNEL
    FROM DMAG_RI RI
    ) AGENT_TBL
WHERE  UPV.CONTRACT_ID = UPC.CONTRACT_ID
AND UPV.ENDT_CODE IS NULL
AND UPB.CONTRACT_ID = UPC.CONTRACT_ID
AND UR.CONTRACT_ID = UPC.CONTRACT_ID
AND UL.CONTRACT_ID   = UPC.CONTRACT_ID
AND UL.RISK_ID = UR.RISK_ID
AND AGENT_TBL.AGENT_CODE = UPB.AGENT_CODE  '
  ;
  V_STEPS    := '002';
  lStrOCPSQL :=
  'SELECT (SELECT OPB.POLICY_REF FROM OCP_POLICY_BASES OPB WHERE OPB.CONTRACT_ID = UPC.CONTRACT_ID) AS POLICY_REF ,
UPV.VERSION_NO,
UPV.CONTRACT_ID,
UPV.REF_NO,
UPB.ISSUE_OFFICE,
(SELECT CC.BRANCH_NAME
FROM CMDM_BRANCH CC
WHERE  CC.BRANCH_CODE=UPB.ISSUE_OFFICE
) AS BRANCH_NAME,
UPC.PRODUCT_CONFIG_CODE,
(SELECT CC.CODE_DESC
FROM CMGE_CODE CC
WHERE CC.CAT_CODE=UPC.LOB
||''_PRODUCT''
AND CC.CODE_CD=UPC.PRODUCT_CONFIG_CODE
) AS PRODUCT_DESC,
UPB.AGENT_CODE,
(select CP.NAME_EXT  FROM CPGE_VI_PARTNERS_ALL CP
   where AGENT_TBL.PART_ID      = CP.PART_ID
  AND AGENT_TBL.PART_VERSION = CP.VERSION)  AS AGENT_NAME,
UPB.AGENT_SOURCE_ID,
(SELECT CP.NAME_EXT FROM CUSTOMER.CPGE_VI_PARTNERS_ALL CP WHERE CP.PART_ID = UPB.CP_PART_ID AND CP.VERSION = UPB.CP_VERSION) AS CP_NAME,
UPB.LONG_NAME,
UPB.EFF_DATE,
UPB.EXP_DATE,
UPC.POLICY_STATUS,
(SELECT CC.CODE_DESC
FROM CMGE_CODE CC
WHERE CC.CAT_CODE=''POL_STATUS''
AND CC.CODE_CD=UPC.POLICY_STATUS
) AS POLICY_STATUS_DESC,
UL.OCCP_CODE,
UL.OCCP_DESCP,
UL.SUB_OCCP_CODE,
(SELECT CC.CODE_DESC
FROM CMGE_CODE CC
WHERE CC.CAT_CODE=''SUB_OCCUPATION_CODE''
AND CC.CODE_CD=UL.SUB_OCCP_CODE
) AS SUB_OCCP_CODE_DESC,
UL.CONSTRUCTION_CODE,
UL.RISK_CODE,
REPLACE ( REPLACE (UL.ADDRESS_NO, CHR (10), CHR (32)), CHR (13), ''!!'')  ||'' ''||(select REPLACE ( REPLACE (CRA.ADDRESS_LINE1, CHR (10), CHR (32)), CHR (13), ''!!'')||'' ''||
REPLACE ( REPLACE (CRA.ADDRESS_LINE2, CHR (10), CHR (32)), CHR (13), ''!!'')||'' ''||
REPLACE ( REPLACE (CRA.ADDRESS_LINE3, CHR (10), CHR (32)), CHR (13), ''!!'')||'' ''||
( SELECT CC.CODE_DESC FROM CMGE_CODE CC WHERE CRA.CITY = CC.CODE_CD AND CC.CAT_CODE = ''CITY''  and rownum = 1)||'' ''||
( SELECT CC.CODE_DESC FROM CMGE_CODE CC WHERE CRA.STATE = CC.CODE_CD AND CC.CAT_CODE = ''STATE''  and rownum = 1)||'' ''||
( SELECT CC.CODE_DESC FROM CMGE_CODE CC WHERE CRA.COUNTRY = CC.CODE_CD AND CC.CAT_CODE = ''COUNTRY''  and rownum = 1)
AS ADDS from CMUW_RISK_ACCUM CRA WHERE CRA.RISK_ACCM_CODE=UL.RISK_CODE) AS RISK_LOCATION,
(select CRA.POSTCODE from CMUW_RISK_ACCUM CRA WHERE CRA.RISK_ACCM_CODE=UL.RISK_CODE) AS POSTCODE,
UR.SEQ_NO AS RISK_NO,
CASE WHEN LOB=''PP'' THEN (SELECT SUM(COV_SI) FROM UWGE_COVER UC WHERE  UR.CONTRACT_ID = UC.CONTRACT_ID
AND UR.RISK_ID = UC.RISK_ID AND UC.TOP_INDICATOR=''Y'' AND COV_CODE IN (''01'',''02'')) ELSE UR.RISK_SI END AS RISK_SI,
CASE WHEN AGENT_TBL.AGENT_TYPE =''RI'' THEN (SELECT INW_PCT FROM UWGE_POLICY_INW UPI WHERE UPI.CONTRACT_ID=UR.CONTRACT_ID and UPI.TOP_INDICATOR=''Y'' AND ROWNUM=1) ELSE 0 END  AS INW_PCT,UR.RISK_ID,
(select distinct UPSD.SURVEY_ID from UWGE_POLICY_SURVEY_DET UPSD where UPSD.CONTRACT_ID = UR.CONTRACT_ID
AND UPSD.RISK_ID=UR.RISK_ID AND UPSD.TOP_INDICATOR=''Y'' and rownum=1) as SURVEY_ID,--16.00 
null AS SURVEY_CONDUCT_IND,
null AS SURVEY_CONDUCT_IND_DESC,
null AS SURVEY_DATE,
null AS SURVEY_CONDUCT_BY,
null AS SURVEY_CONDUCT_BY_DESC,
null AS SURVEY_REVIEW_DATE,
null AS APPLIED_ON_RISK,
null AS REMARKS
FROM UWGE_POLICY_CONTRACTS UPC,
UWGE_POLICY_VERSIONS UPV,
UWGE_POLICY_BASES UPB ,
UWGE_RISK UR,
UWFI_RISK_LOCATION UL,
(SELECT DISTINCT(DMT.REFERENCE_CODE) AGENT_CODE,
      DMAG.PART_ID,
      DMAG.PART_VERSION,
      ''DMAG'' AGENT_TYPE,
          DMAG.CHANNEL
    FROM DMT_AGENTS DMT,
      DMAG_AGENTS DMAG
    WHERE DMT.INT_ID      = DMAG.INT_ID
    UNION ALL
    SELECT RI.DMT_AGENT_CODE AGENT_CODE,
      RI.PART_ID,
      RI.PART_VERSION,
      ''RI'' AGENT_TYPE,
          ''0'' CHANNEL
    FROM DMAG_RI RI
    ) AGENT_TBL
WHERE  UPV.CONTRACT_ID = UPC.CONTRACT_ID
AND UPV.TOP_INDICATOR=''Y''
AND UPB.CONTRACT_ID = UPV.CONTRACT_ID
AND UPB.TOP_INDICATOR=''Y''
AND UR.CONTRACT_ID = UPV.CONTRACT_ID
AND UR.TOP_INDICATOR=''Y''
AND AGENT_TBL.AGENT_CODE = UPB.AGENT_CODE
AND UL.CONTRACT_ID   = UPV.CONTRACT_ID
AND UL.TOP_INDICATOR=''Y''
AND UL.RISK_ID = UR.RISK_ID  '
  ;
  
  V_STEPS        := '003';
  IF p_LOB       IS NOT NULL THEN
    lStrWhereSQL := lStrWhereSQL ||' AND UPC.LOB ='''||p_LOB||'''';
  END IF;
  IF p_Product   IS NULL OR p_Product='ALL' THEN
    --lStrWhereSQL := lStrWhereSQL ||' AND UPC.PRODUCT_CONFIG_CODE IN (select CC.CODE_CD from CMGE_CODE CC where CC.CAT_CODE = ''SURVEY_PRODUCT'' and CC.CD_VALUE='''||p_LOB||''')';
    lStrWhereSQL := lStrWhereSQL ||' AND UPC.PRODUCT_CONFIG_CODE IN (select CC.CODE_CD from CMGE_CODE CC where CC.CAT_CODE = ''SURVEY_PRODUCT2'' and CC.CD_VALUE='''||p_LOB||''')'; --8.0
  ELSE
    lStrWhereSQL := lStrWhereSQL ||' AND UPC.PRODUCT_CONFIG_CODE ='''||p_Product||'''';
  END IF;
  IF p_PolicyStatus IS NOT NULL AND p_PolicyStatus <> 'ALL' THEN
    lStrWhereSQL    := lStrWhereSQL ||' AND UPC.POLICY_STATUS ='''||p_PolicyStatus||'''';
  END IF;
  IF(p_Region    IS NULL OR p_Region='ALL' )THEN
    lStrWhereSQL := lStrWhereSQL ||' AND UPB.ISSUE_OFFICE IN (SELECT DISTINCT CMB.BRANCH_CODE FROM CMDM_BRANCH CMB)';
  ELSE
    lStrWhereSQL := lStrWhereSQL ||' AND UPB.ISSUE_OFFICE IN (SELECT DISTINCT CMB.BRANCH_CODE FROM CMDM_BRANCH CMB WHERE CMB.REGION_CODE ='''||p_Region||''')';
  END IF;
  IF(p_Branch    IS NULL OR p_Branch='ALL' )THEN
    lStrWhereSQL := lStrWhereSQL ||' AND UPB.ISSUE_OFFICE IN (SELECT DISTINCT CMB.BRANCH_CODE FROM CMDM_BRANCH CMB)';
  ELSE
    lStrWhereSQL := lStrWhereSQL ||' AND UPB.ISSUE_OFFICE IN (select regexp_substr('''||p_Branch||''',''[^|]+'', 1, level) from dual connect by regexp_substr('''||p_Branch||''', ''[^|]+'', 1, level) is not null )';
  END IF;
  IF(p_IssueDateFrom IS NOT NULL AND p_IssueDateTo IS NOT NULL) THEN
    lStrWhereSQL     := lStrWhereSQL ||' AND TRUNC(UPC.ISSUE_DATE) BETWEEN  TO_DATE ('''||p_IssueDateFrom||''',''DD/MM/YYYY'')  AND TO_DATE ('''||p_IssueDateTo||''',''DD/MM/YYYY'') ';
  END IF;
  IF p_AgentChannel IS NOT NULL AND p_AgentChannel <> 'ALL' THEN
    lStrWhereSQL    := lStrWhereSQL||' AND  AGENT_TBL.CHANNEL='''||p_AgentChannel||'''';
  END IF;
   -- 8.01 start
     IF p_LOB IS NOT NULL AND p_LOB ='PP' THEN 
      v_OCCP_REF_RISK := v_OCCP_REF_RISK_PP;
      v_OCCP_DEC_RISK := v_OCCP_DEC_RISK_PP;
      v_OCCP_CON_1 := v_OCCP_CON_PP_1;
      v_OCCP_CON_11 := v_OCCP_CON_PP_11;
      v_OCCP_CON_2 := v_OCCP_CON_PP_2;
      v_OCCP_CON_22 := v_OCCP_CON_PP_22;
     END IF;
   -- 8.01 end
  IF (p_ReportName    IS NOT NULL AND p_ReportName='RefSIGrt10') THEN
    lStrWhereSQL      := lStrWhereSQL ||'  AND ((UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_REF_RISK||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_REF_RISK||''', ''[^,]+'', 1, level) is not null ) OR (UL.OCCP_CODE  IN(select regexp_substr('''||v_SUB_OCCP||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_SUB_OCCP||''', ''[^,]+'', 1, level) is not null ) AND  UL.SUB_OCCP_CODE=''H'' )) OR (UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_REF_RISK||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_REF_RISK||''', ''[^,]+'', 1, level) is not null ) OR (UL.OCCP_CODE  IN(select regexp_substr('''||v_CONSTRUCTION_CODE||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_CONSTRUCTION_CODE||''', ''[^,]+'', 1, level) is not null ) AND  UL.CONSTRUCTION_CODE in (''C1B'',''C2'',''C3''))))'; -- 29.00
  ELSIF (p_ReportName IS NOT NULL AND p_ReportName='RefSILess10') THEN
    lStrWhereSQL      := lStrWhereSQL ||'  AND ((UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_REF_RISK||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_REF_RISK||''', ''[^,]+'', 1, level) is not null ) OR (UL.OCCP_CODE  IN(select regexp_substr('''||v_SUB_OCCP||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_SUB_OCCP||''', ''[^,]+'', 1, level) is not null ) AND  UL.SUB_OCCP_CODE=''H'' )) OR (UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_REF_RISK||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_REF_RISK||''', ''[^,]+'', 1, level) is not null ) OR (UL.OCCP_CODE  IN(select regexp_substr('''||v_CONSTRUCTION_CODE||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_CONSTRUCTION_CODE||''', ''[^,]+'', 1, level) is not null ) AND  UL.CONSTRUCTION_CODE in (''C1B'',''C2'',''C3''))))'; -- 29.00
  ELSIF (p_ReportName IS NOT NULL AND p_ReportName='DecSIGrt10') THEN
    lStrWhereSQL      := lStrWhereSQL ||'  AND (UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_DEC_RISK||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_DEC_RISK||''', ''[^,]+'', 1, level) is not null ) OR (UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_TW||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_TW||''', ''[^,]+'', 1, level) is not null ) ))';
  ELSIF (p_ReportName IS NOT NULL AND p_ReportName='DecSILess10') THEN
    lStrWhereSQL      := lStrWhereSQL ||'  AND (UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_DEC_RISK||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_DEC_RISK||''', ''[^,]+'', 1, level) is not null ) )';
  ELSIF (p_ReportName IS NOT NULL AND p_ReportName='TimSILess10') THEN
    lStrWhereSQL      := lStrWhereSQL ||'  AND (UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_TW||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_TW||''', ''[^,]+'', 1, level) is not null ) )';
  ELSIF (p_ReportName IS NOT NULL AND p_ReportName='RskSILess300') THEN
    lStrWhereSQL      := lStrWhereSQL ||'  AND (UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_CON_1||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_CON_1||''', ''[^,]+'', 1, level) is not null ) OR UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_CON_11||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_CON_11||''', ''[^,]+'', 1, level) is not null ) OR (UL.OCCP_CODE  IN(select regexp_substr('''||v_SUB_OCCP||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_SUB_OCCP||''', ''[^,]+'', 1, level) is not null ) AND  UL.SUB_OCCP_CODE in (''L'',''M'') ))';
  ELSIF (p_ReportName IS NOT NULL AND p_ReportName='RskSIGrt300') THEN
    lStrWhereSQL      := lStrWhereSQL ||'  AND (UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_CON_2||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_CON_2||''', ''[^,]+'', 1, level) is not null ) OR (UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_CON_22||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_CON_22||''', ''[^,]+'', 1, level) is not null ) ))';
  -- 35.00 Start
  ELSIF (p_ReportName IS NOT NULL AND p_ReportName='ResSI100T400') THEN
    lStrWhereSQL      := lStrWhereSQL ||'  AND (UL.OCCP_CODE  IN(select regexp_substr('''||v_RES_OCCP_CODE||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_RES_OCCP_CODE||''', ''[^,]+'', 1, level) is not null ) )';
  ELSIF (p_ReportName IS NOT NULL AND p_ReportName='ResSIGrt400') THEN
    lStrWhereSQL      := lStrWhereSQL ||'  AND (UL.OCCP_CODE  IN(select regexp_substr('''||v_RES_OCCP_CODE||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_RES_OCCP_CODE||''', ''[^,]+'', 1, level) is not null ) )';
  ELSIF (p_ReportName IS NOT NULL AND p_ReportName='NRDecSI10T100') THEN
    lStrWhereSQL      := lStrWhereSQL ||'  AND (UL.OCCP_CODE  IN(select regexp_substr('''||v_NRD_OCCP_CODE_1||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_NRD_OCCP_CODE_1||''', ''[^,]+'', 1, level) is not null ) OR UL.OCCP_CODE  IN(select regexp_substr('''||v_NRD_OCCP_CODE_11||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_NRD_OCCP_CODE_11||''', ''[^,]+'', 1, level) is not null ) OR (UL.OCCP_CODE  IN(select regexp_substr('''||v_SUB_OCCP||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_SUB_OCCP||''', ''[^,]+'', 1, level) is not null ) AND  UL.SUB_OCCP_CODE in (''L'',''M'') ))';
  ELSIF (p_ReportName IS NOT NULL AND p_ReportName='NRDecSIGrt100') THEN
    lStrWhereSQL      := lStrWhereSQL ||'  AND (UL.OCCP_CODE  IN(select regexp_substr('''||v_NRD_OCCP_CODE_1||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_NRD_OCCP_CODE_1||''', ''[^,]+'', 1, level) is not null ) OR UL.OCCP_CODE  IN(select regexp_substr('''||v_NRD_OCCP_CODE_11||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_NRD_OCCP_CODE_11||''', ''[^,]+'', 1, level) is not null ) OR (UL.OCCP_CODE  IN(select regexp_substr('''||v_SUB_OCCP||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_SUB_OCCP||''', ''[^,]+'', 1, level) is not null ) AND  UL.SUB_OCCP_CODE in (''L'',''M'') ))';
  ELSIF (p_ReportName IS NOT NULL AND p_ReportName='RefSI10T30') THEN
    lStrWhereSQL      := lStrWhereSQL ||'  AND ((UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_REF_RISK||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_REF_RISK||''', ''[^,]+'', 1, level) is not null ) OR (UL.OCCP_CODE  IN(select regexp_substr('''||v_SUB_OCCP||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_SUB_OCCP||''', ''[^,]+'', 1, level) is not null ) AND  UL.SUB_OCCP_CODE=''H'' )) OR (UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_REF_RISK||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_REF_RISK||''', ''[^,]+'', 1, level) is not null ) OR (UL.OCCP_CODE  IN(select regexp_substr('''||v_CONSTRUCTION_CODE||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_CONSTRUCTION_CODE||''', ''[^,]+'', 1, level) is not null ) AND  UL.CONSTRUCTION_CODE in (''C1B'',''C2'',''C3''))))';
  ELSIF (p_ReportName IS NOT NULL AND p_ReportName='RefSIGrt30') THEN
    lStrWhereSQL      := lStrWhereSQL ||'  AND ((UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_REF_RISK||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_REF_RISK||''', ''[^,]+'', 1, level) is not null ) OR (UL.OCCP_CODE  IN(select regexp_substr('''||v_SUB_OCCP||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_SUB_OCCP||''', ''[^,]+'', 1, level) is not null ) AND  UL.SUB_OCCP_CODE=''H'' )) OR (UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_REF_RISK||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_REF_RISK||''', ''[^,]+'', 1, level) is not null ) OR (UL.OCCP_CODE  IN(select regexp_substr('''||v_CONSTRUCTION_CODE||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_CONSTRUCTION_CODE||''', ''[^,]+'', 1, level) is not null ) AND  UL.CONSTRUCTION_CODE in (''C1B'',''C2'',''C3''))))';
  ELSIF (p_ReportName IS NOT NULL AND p_ReportName='DecSI10T30') THEN
    lStrWhereSQL      := lStrWhereSQL ||'  AND (UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_DEC_RISK||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_DEC_RISK||''', ''[^,]+'', 1, level) is not null ) )';
  ELSIF (p_ReportName IS NOT NULL AND p_ReportName='DecSIGrt30') THEN
    lStrWhereSQL      := lStrWhereSQL ||'  AND (UL.OCCP_CODE  IN(select regexp_substr('''||v_OCCP_DEC_RISK||''',''[^,]+'', 1, level) from dual connect by regexp_substr('''||v_OCCP_DEC_RISK||''', ''[^,]+'', 1, level) is not null ) )';
  -- end 35.00
  END IF;
  IF p_PolicyStatus   IS NULL OR p_PolicyStatus='ALL' THEN
    lStrSQL           := ' ('|| lStrOCPSQL || lStrWhereSQL ||' ) union all ('||lStrWIPSQL||lStrWhereSQL||')';
  ELSIF p_PolicyStatus ='W' THEN
    lStrSQL           := lStrWIPSQL||lStrWhereSQL;
  ELSE
    lStrSQL := lStrOCPSQL||lStrWhereSQL;
  END IF;
  V_STEPS := '004';
  --PG_UTIL_LOG_ERROR.PC_INS_log_error ( v_ProcName_v || '.' || v_ProcName_v ||' V_STEPS:'||V_STEPS, 1, lStrWhereSQL);
  V_STEPS := '005';
  BEGIN
    EXECUTE IMMEDIATE lStrSQL BULK COLLECT INTO SURVEYS_DET;
  END;
  V_STEPS := '006';
  FOR V_ROW_NUM IN 1 .. SURVEYS_DET.COUNT
  LOOP
      ISVALID_REC  := 'N';
      V_RISK_SI := SURVEYS_DET (V_ROW_NUM).RISK_SI;
      IF SURVEYS_DET (V_ROW_NUM).INW_PCT >0 THEN
        V_RISK_SI :=(SURVEYS_DET (V_ROW_NUM).RISK_SI/SURVEYS_DET (V_ROW_NUM).INW_PCT)*100;
      END IF;

      --IF (p_ReportName ='RefSIGrt10' OR p_ReportName='RskSILess300') AND V_RISK_SI >=10000001 AND V_RISK_SI <=300000000  THEN  --35.00
      IF (p_ReportName ='RefSIGrt10' OR p_ReportName='RskSILess300' OR p_ReportName='RefSI10T30' OR p_ReportName='DecSI10T30') AND V_RISK_SI >=10000001 AND V_RISK_SI <=300000000  THEN --35.00
        ISVALID_REC     :='Y';
      ELSIF ( p_ReportName='RefSILess10' OR p_ReportName='DecSILess10'  OR p_ReportName='TimSILess10') AND V_RISK_SI <=10000000 THEN
        ISVALID_REC     :='Y';
      ELSIF  p_ReportName='DecSIGrt10' AND V_RISK_SI >=10000001 THEN
        ISVALID_REC     :='Y';
      -- ELSIF  p_ReportName='RskSIGrt300' AND V_RISK_SI >=300000001 THEN --35.00
      ELSIF  (p_ReportName='RskSIGrt300' OR p_ReportName='DecSIGrt30' OR p_ReportName='RefSIGrt30') AND V_RISK_SI >=30000001 THEN --35.00
        ISVALID_REC     :='Y';
      -- 35.00 Start
      ELSIF  p_ReportName='ResSI100T400' AND V_RISK_SI >=100000001 AND V_RISK_SI <=400000000 THEN
        ISVALID_REC     :='Y';
      ELSIF  p_ReportName='ResSIGrt400' AND V_RISK_SI >=400000001 THEN
        ISVALID_REC     :='Y';
      ELSIF  p_ReportName='NRDecSI10T100' AND V_RISK_SI >=10000001 AND V_RISK_SI <=100000000 THEN
        ISVALID_REC     :='Y';
      ELSIF  p_ReportName='NRDecSIGrt100' AND V_RISK_SI >=100000000 THEN
        ISVALID_REC     :='Y';        
      -- 35.00 End
      END IF;
           
    IF ISVALID_REC ='Y' AND SURVEYS_DET (V_ROW_NUM).SURVEY_ID IS NULL THEN
      ISVALID_REC                        :='Y';
      V_SURVEY_CONDUCT_IND               :='';
      V_SURVEY_CONDUCT_IND_DESC          :='No';
      V_SURVEY_DATE                      :=NULL;
      V_SURVEY_REVIEW_DATE               :=NULL;
      V_SURVEY_CONDUCT_BY                :='';
      V_SURVEY_CONDUCT_BY_DESC           :='';
      V_APPLIED_ON_RISK                  :='';
    ELSIF ISVALID_REC ='Y'  THEN 
      IF SURVEYS_DET (V_ROW_NUM).POLICY_STATUS ='W' THEN
        SELECT SURVEY_CONDUCT_IND,
          (SELECT CC.CODE_DESC
          FROM CMGE_CODE CC
          WHERE CAT_CODE='ANSWER'
          AND CC.CODE_CD=SURVEY_CONDUCT_IND
          ),
          SURVEY_DATE,
          SURVEY_REVIEW_DATE,
          SURVEY_CONDUCT_BY,
          (SELECT CC.CODE_DESC
          FROM CMGE_CODE CC
          WHERE CAT_CODE='SURVEY_BY'
          AND CC.CODE_CD=SURVEY_CONDUCT_BY
          ),
          (SELECT LISTAGG ( COVER.COV_SEQ_REF,', ') WITHIN GROUP (
          ORDER BY COVER.COV_ID, COVER.COV_SEQ_REF)
          FROM WIP_UWGE_COVER COVER,
            WIP_UWGE_POLICY_SURVEY_DET DET
          WHERE COVER.CONTRACT_ID = DET.CONTRACT_ID
          AND COVER.RISK_ID       = DET.RISK_ID
          AND COVER.COV_ID        = DET.COV_ID
          AND DET.CONTRACT_ID     = UPSH.CONTRACT_ID
          AND DET.SURVEY_ID       = UPSH.SURVEY_ID
          ),
          ROUND( (TRUNC(SURVEY_DATE)-SURVEYS_DET (V_ROW_NUM).EFF_DATE) /365) AS SURVEY_YEAR
        INTO V_SURVEY_CONDUCT_IND,
          V_SURVEY_CONDUCT_IND_DESC,
          V_SURVEY_DATE,
          V_SURVEY_REVIEW_DATE,
          V_SURVEY_CONDUCT_BY,
          V_SURVEY_CONDUCT_BY_DESC,
          V_APPLIED_ON_RISK,
          V_SURVEY_YEAR
        FROM WIP_UWGE_POLICY_SURVEY_HDR UPSH
        WHERE UPSH.CONTRACT_ID=SURVEYS_DET (V_ROW_NUM).CONTRACT_ID
        AND UPSH.SURVEY_ID    =SURVEYS_DET (V_ROW_NUM).SURVEY_ID ;
      ELSE
        SELECT SURVEY_CONDUCT_IND,
          (SELECT CC.CODE_DESC
          FROM CMGE_CODE CC
          WHERE CAT_CODE='ANSWER'
          AND CC.CODE_CD=SURVEY_CONDUCT_IND
          ),
          SURVEY_DATE,
          SURVEY_REVIEW_DATE,
          SURVEY_CONDUCT_BY,
          (SELECT CC.CODE_DESC
          FROM CMGE_CODE CC
          WHERE CAT_CODE='SURVEY_BY'
          AND CC.CODE_CD=SURVEY_CONDUCT_BY
          ),
          (SELECT LISTAGG ( COVER.COV_SEQ_REF,', ') WITHIN GROUP (
          ORDER BY COVER.COV_ID, COVER.COV_SEQ_REF)
          FROM UWGE_COVER COVER,
            UWGE_POLICY_SURVEY_DET DET
          WHERE COVER.CONTRACT_ID = DET.CONTRACT_ID
          AND COVER.RISK_ID       = DET.RISK_ID
          AND COVER.COV_ID        = DET.COV_ID
          AND COVER.TOP_INDICATOR ='Y'
          AND DET.CONTRACT_ID     = UPSH.CONTRACT_ID
          AND DET.SURVEY_ID       = UPSH.SURVEY_ID
          AND DET.TOP_INDICATOR   ='Y'
          ),
          ROUND( (TRUNC(SURVEY_DATE)-SURVEYS_DET (V_ROW_NUM).EFF_DATE) /365) AS SURVEY_YEAR
        INTO V_SURVEY_CONDUCT_IND,
          V_SURVEY_CONDUCT_IND_DESC,
          V_SURVEY_DATE,
          V_SURVEY_REVIEW_DATE,
          V_SURVEY_CONDUCT_BY,
          V_SURVEY_CONDUCT_BY_DESC,
          V_APPLIED_ON_RISK,
          V_SURVEY_YEAR
        FROM UWGE_POLICY_SURVEY_HDR UPSH
        WHERE UPSH.CONTRACT_ID=SURVEYS_DET (V_ROW_NUM).CONTRACT_ID
        AND UPSH.SURVEY_ID    =SURVEYS_DET (V_ROW_NUM).SURVEY_ID
        AND TOP_INDICATOR     ='Y';
      END IF;
    --IF (p_ReportName   ='RefSIGrt10' OR p_ReportName='RefSILess10' OR p_ReportName='RskSILess300') AND V_SURVEY_YEAR <= -3 THEN  -- 35.00 Start
      IF (p_ReportName   ='RefSIGrt10' OR p_ReportName='RefSILess10' OR p_ReportName='RskSILess300' OR p_ReportName ='DecSIGrt30' OR p_ReportName ='DecSI10T30') AND V_SURVEY_YEAR <= -2 THEN -- 35.00 Start
        ISVALID_REC     :='Y';
      ELSIF (p_ReportName='DecSIGrt10' OR p_ReportName='RskSIGrt300') AND V_SURVEY_YEAR <= -1 THEN
        ISVALID_REC     :='Y';
      ELSIF (p_ReportName='DecSILess10' OR p_ReportName='TimSILess10') AND V_SURVEY_YEAR <= -2 THEN
        ISVALID_REC     :='Y';
        -- 35.00 Start
      ELSIF (p_ReportName   ='ResSI100T400' OR p_ReportName='ResSIGrt400' OR p_ReportName='NRDecSI10T100'
            OR p_ReportName = 'NRDecSIGrt100' OR p_ReportName ='RefSI10T30' OR p_ReportName ='RefSIGrt30' 
             ) AND V_SURVEY_YEAR <= -3 THEN
            ISVALID_REC     :='Y';
        -- 35.00 End
      ELSE 
         ISVALID_REC  := 'N';      
      END IF;
    END IF;
     
    IF ISVALID_REC                   ='Y' THEN
      r_row.POLICY_REF              := SURVEYS_DET (V_ROW_NUM).POLICY_REF;
      r_row.VERSION_NO              := SURVEYS_DET (V_ROW_NUM).VERSION_NO;
      r_row.CONTRACT_ID             := SURVEYS_DET (V_ROW_NUM).CONTRACT_ID;
      r_row.REF_NO                  := SURVEYS_DET (V_ROW_NUM).REF_NO;
      r_row.BRANCH_CODE             := SURVEYS_DET (V_ROW_NUM).BRANCH_CODE;
      r_row.BRANCH_NAME             := SURVEYS_DET (V_ROW_NUM).BRANCH_NAME;
      r_row.PRODUCT_CODE            := SURVEYS_DET (V_ROW_NUM).PRODUCT_CODE;
      r_row.PRODUCT_DESC            := SURVEYS_DET (V_ROW_NUM).PRODUCT_DESC;
      r_row.AGENT_CODE              := SURVEYS_DET (V_ROW_NUM).AGENT_CODE;
      r_row.AGENT_NAME              := SURVEYS_DET (V_ROW_NUM).AGENT_NAME;
      r_row.AGENT_SOURCE_ID         := SURVEYS_DET (V_ROW_NUM).AGENT_SOURCE_ID;
      r_row.CP_NAME                 := SURVEYS_DET (V_ROW_NUM).CP_NAME;
      r_row.LONG_NAME               := SURVEYS_DET (V_ROW_NUM).LONG_NAME;
      r_row.EFF_DATE                := SURVEYS_DET (V_ROW_NUM).EFF_DATE;
      r_row.EXP_DATE                := SURVEYS_DET (V_ROW_NUM).EXP_DATE;
      r_row.POLICY_STATUS           := SURVEYS_DET (V_ROW_NUM).POLICY_STATUS;
      r_row.POLICY_STATUS_DESC      := SURVEYS_DET (V_ROW_NUM).POLICY_STATUS_DESC;
      r_row.OCCP_CODE               := SURVEYS_DET (V_ROW_NUM).OCCP_CODE;
      r_row.OCCP_DESCP              := SURVEYS_DET (V_ROW_NUM).OCCP_DESCP;
      r_row.SUB_OCCP_CODE           := SURVEYS_DET (V_ROW_NUM).SUB_OCCP_CODE;
      r_row.SUB_OCCP_CODE_DESC      := SURVEYS_DET (V_ROW_NUM).SUB_OCCP_CODE_DESC;
      r_row.CONSTRUCTION_CODE       := SURVEYS_DET (V_ROW_NUM).CONSTRUCTION_CODE;
      r_row.RISK_CODE               := SURVEYS_DET (V_ROW_NUM).RISK_CODE;
      r_row.RISK_LOCATION           := SURVEYS_DET (V_ROW_NUM).RISK_LOCATION;
      r_row.POSTCODE                := SURVEYS_DET (V_ROW_NUM).POSTCODE;
      r_row.RISK_NO                 := SURVEYS_DET (V_ROW_NUM).RISK_NO;
      r_row.RISK_SI                 := SURVEYS_DET (V_ROW_NUM).RISK_SI;
      r_row.INW_PCT                 := SURVEYS_DET (V_ROW_NUM).INW_PCT;
      r_row.RISK_ID                 := SURVEYS_DET (V_ROW_NUM).RISK_ID;
      r_row.SURVEY_ID               := SURVEYS_DET (V_ROW_NUM).SURVEY_ID;
      r_row.SURVEY_CONDUCT_IND      := V_SURVEY_CONDUCT_IND;
      r_row.SURVEY_CONDUCT_IND_DESC := V_SURVEY_CONDUCT_IND_DESC;
      r_row.SURVEY_DATE             := V_SURVEY_DATE;
      r_row.SURVEY_REVIEW_DATE      := V_SURVEY_REVIEW_DATE;
      r_row.SURVEY_CONDUCT_BY       := V_SURVEY_CONDUCT_BY;
      r_row.SURVEY_CONDUCT_BY_DESC  := V_SURVEY_CONDUCT_BY_DESC;
      r_row.APPLIED_ON_RISK         := V_APPLIED_ON_RISK;
      IF SURVEYS_DET (V_ROW_NUM).POLICY_REF IS NOT NULL THEN
      BEGIN
        SELECT  REFER_ADMIN_REMARKS INTO V_REFER_ADMIN_REMARKS FROM CNUW_BASES WHERE POL_NO=SURVEYS_DET (V_ROW_NUM).POLICY_REF AND ROWNUM=1;
       EXCEPTION
        WHEN OTHERS THEN
          V_REFER_ADMIN_REMARKS := '';
        END;
        r_row.REMARKS                 := V_REFER_ADMIN_REMARKS;
      ELSE
         r_row.REMARKS                 := '';
      END IF;
      PIPE ROW(r_row);
    END IF;
  END LOOP;
  V_STEPS := '007';
  RETURN;
EXCEPTION
WHEN OTHERS THEN
  PG_UTIL_LOG_ERROR.PC_INS_log_error ( v_ProcName_v || '.' || v_ProcName_v ||' V_STEPS:'||V_STEPS, 1, SQLERRM);
END FN_SURVEYS_LISTING;
--End 7.0

--Start 10.00
FUNCTION FN_NMCN_PATCHING_RPT(p_ReportName IN VARCHAR2,
                               p_CN_Status IN VARCHAR2,
                               p_Branch IN VARCHAR2,
                               p_Agent_Code IN VARCHAR2,
                               p_Scrtny_User IN VARCHAR2,
                               p_Lob IN VARCHAR2,
                               p_Product  IN VARCHAR2,
                               p_CNIssueDateFrom IN VARCHAR2,
                               p_CNIssueDateTo IN VARCHAR2)
  RETURN NMCN_PATCHING_TAB PIPELINED
IS
  V_PROCNAME_V VARCHAR2 (30) := 'FN_NMCN_PATCHING_RPT';
  V_STEPS      VARCHAR2 (4000) :='000';
  r_row NMCN_PATCHING_REC;
  PATCHING_NMCN PG_RPGE_LISTING.NMCN_PATCHING_TAB;
  lStrSQL      VARCHAR2(32757);
  lStrWhereSQL VARCHAR2(32757);

  v_CatCode CMGE_CODE.CAT_CODE%TYPE;

  BEGIN
  V_STEPS    := '001';
  lStrSQL :='SELECT ROW_NUMBER () OVER (ORDER BY NMCN.CONTRACT_ID) S_NO, NMCN.*
  FROM (                                      /*Cnote Mapped to Policy Query*/
        SELECT UPC.CONTRACT_ID,
               UPB.ISSUE_OFFICE BRANCH_CODE,
               (SELECT BRANCH_NAME
                  FROM CMDM_BRANCH
                 WHERE BRANCH_CODE = UPB.ISSUE_OFFICE)
                  BRANCH_NAME,
               CN.AGENT_CODE,
               (SELECT DVA.NAME
                  FROM DMAG_VI_AGENT DVA
                 WHERE DVA.AGENTCODE = CN.AGENT_CODE)
                  AGENT_NAME,
               UPC.LOB,
               (SELECT CODE_DESC
                  FROM cmge_code
                 WHERE CAT_CODE = ''LOB'' AND code_cd = UPC.LOB)
                  LOB_DESC,
               CN.PRODUCT_CONFIG_CODE PRODUCT_CODE,
               (SELECT CODE_DESC
                  FROM cmge_code
                 WHERE     CAT_CODE = UPC.LOB || ''_PRODUCT''
                       AND code_cd = CN.PRODUCT_CONFIG_CODE)
                  PRODUCT_DESC,
               (SELECT NAME_EXT
                  FROM CPGE_VI_PARTNERS_ALL CP
                 WHERE     CP.PART_ID = CN.CP_PART_ID
                       AND CP.VERSION = CN.CP_VERSION)
                  CP_NAME,
               CN.CNOTE_DATE,
               CN.CN_TYPE,
               CN.CNOTE_NO,
               cn.STATUS,
               (SELECT CODE_DESC
                  FROM cmge_code
                 WHERE CAT_CODE = ''CN_STATUS'' AND code_cd = cn.STATUS)
                  STATUS_DESC,
               BSM.RECEIVED_DATE SCRTNY_ISSUE_DATE,
               BSM.REFERENCE_NO SCRTNY_REF_NO,
               --BS.CNOTE_NO SCRTNY_CNOTE_NO,
               (select BS.CNOTE_NO from BPUW_SCRUTINY BS where UPV.REF_NO = BS.REFERENCE_NO) SCRTNY_CNOTE_NO,
               BSM.CREATED_BY SCRTNY_USER_NAME,
               UPC.ISSUE_DATE NMCN_ISSUE_DATE,
               --OPB.POLICY_REF NMCN_ENDT_NO,
               (select OPB.POLICY_REF from OCP_POLICY_BASES OPB where OPB.CONTRACT_ID = UPB.CONTRACT_ID and OPB.VERSION_NO = UPB.VERSION_NO) POLICY_REF,
               UPB.CNOTE_NO NMCN_ENDT_CN_NO,
               UPC.ISSUE_BY NMCN_ENDT_ISSUE_BY,
               NULL ENDT_ISSUE_DATE,
               NULL ENDT_NO,
               NULL ENDT_CN_NO,
               NULL AS "ENDT_ISSUE_BY"
          FROM CNGE_NOTE CN,
               --OCP_POLICY_BASES OPB, --use subselect --nk
               UWGE_POLICY_CONTRACTS UPC,
               UWGE_POLICY_BASES UPB,
               UWGE_POLICY_VERSIONS UPV, 
               BPGE_SCRUTINY_MAIN BSM --,
               --BPUW_SCRUTINY BS --use subselect --nk
         WHERE     --OPB.CONTRACT_ID = UPB.CONTRACT_ID
               --AND 
               UPC.CONTRACT_ID = UPB.CONTRACT_ID
               AND UPC.CONTRACT_ID = UPV.CONTRACT_ID
               AND UPB.VERSION_NO = UPV.VERSION_NO
               AND UPB.VERSION_NO = 1
               AND cn.CNOTE_NO = UPB.CNOTE_NO
               AND CN.CNOTE_SERIES = ''73''
               --AND UPC.LOB != ''MT''
               AND UPV.REF_NO = BSM.REFERENCE_NO(+)
               --AND UPV.REF_NO = BS.REFERENCE_NO(+)
               --AND UPV.CNOTE_UW_IND <> ''Y''
               AND UPV.CNOTE_UW_IND = ''N'' --nk to add index for this column
               and TRUNC(CN.CNOTE_DATE) BETWEEN  TO_DATE ('''||p_CNIssueDateFrom||''',''YYYY-MM-DD'')  AND TO_DATE ('''||p_CNIssueDateTo||''',''YYYY-MM-DD'') --nk
        UNION
        /*Cnote Mapped to Endorsement Query, Not EC165*/
        SELECT UPC.CONTRACT_ID,
               UPB.ISSUE_OFFICE BRANCH_CODE,
               (SELECT BRANCH_NAME
                  FROM CMDM_BRANCH
                 WHERE BRANCH_CODE = UPB.ISSUE_OFFICE)
                  BRANCH_NAME,
               CN.AGENT_CODE,
               (SELECT DVA.NAME
                  FROM DMAG_VI_AGENT DVA
                 WHERE DVA.AGENTCODE = CN.AGENT_CODE)
                  AGENT_NAME,
               UPC.LOB,
               (SELECT CODE_DESC
                  FROM cmge_code
                 WHERE CAT_CODE = ''LOB'' AND code_cd = UPC.LOB)
                  LOB_DESC,
               CN.PRODUCT_CONFIG_CODE PRODUCT_CODE,
               (SELECT CODE_DESC
                  FROM cmge_code
                 WHERE     CAT_CODE = UPC.LOB || ''_PRODUCT''
                       AND code_cd = CN.PRODUCT_CONFIG_CODE)
                  PRODUCT_DESC,
               (SELECT NAME_EXT
                  FROM CPGE_VI_PARTNERS_ALL CP
                 WHERE     CP.PART_ID = CN.CP_PART_ID
                       AND CP.VERSION = CN.CP_VERSION)
                  CP_NAME,
               CN.CNOTE_DATE,
               CN.CN_TYPE,
               CN.CNOTE_NO,
               cn.STATUS,
               (SELECT CODE_DESC
                  FROM cmge_code
                 WHERE CAT_CODE = ''CN_STATUS'' AND code_cd = cn.STATUS)
                  STATUS_DESC,
               BSM.RECEIVED_DATE SCRTNY_ISSUE_DATE,
               BSM.REFERENCE_NO SCRTNY_REF_NO,
               --BS.CNOTE_NO SCRTNY_CNOTE_NO,
               (select BS.CNOTE_NO from BPUW_SCRUTINY BS where UPV.REF_NO = BS.REFERENCE_NO) SCRTNY_CNOTE_NO,
               BSM.CREATED_BY SCRTNY_USER_NAME,
               UPC.ISSUE_DATE NMCN_ISSUE_DATE,
               UPV.ENDT_NO NMCN_ENDT_NO,
               UPV.ENDT_CNOTE_NO NMCN_ENDT_CN_NO,
               UPC.ISSUE_BY NMCN_ENDT_ISSUE_BY,
               NULL ENDT_ISSUE_DATE,
               NULL ENDT_NO,
               NULL ENDT_CN_NO,
               NULL AS "ENDT_ISSUE_BY"
          FROM CNGE_NOTE CN,
               UWGE_POLICY_CONTRACTS UPC,
               UWGE_POLICY_BASES UPB,
               UWGE_POLICY_VERSIONS UPV,
               BPGE_SCRUTINY_MAIN BSM --,
               --BPUW_SCRUTINY BS
         WHERE     UPC.CONTRACT_ID = UPB.CONTRACT_ID
               AND UPC.CONTRACT_ID = UPV.CONTRACT_ID
               AND UPB.VERSION_NO = UPV.VERSION_NO
               AND UPB.VERSION_NO <> 1
               AND cn.CNOTE_NO = UPV.ENDT_CNOTE_NO
               AND CN.CNOTE_SERIES = ''73''
               --AND UPC.LOB != ''MT''
               AND UPV.REF_NO = BSM.REFERENCE_NO(+)
               --AND UPV.REF_NO = BS.REFERENCE_NO(+)
               --AND UPV.CNOTE_UW_IND <> ''Y''
               AND UPV.CNOTE_UW_IND = ''N'' --nk
               and TRUNC(CN.CNOTE_DATE) BETWEEN  TO_DATE ('''||p_CNIssueDateFrom||''',''YYYY-MM-DD'')  AND TO_DATE ('''||p_CNIssueDateTo||''',''YYYY-MM-DD'')
        --AND UPV.CNOTE_VER_NO IS NULL
        /* Cnote Patched Through 165 query*/
        UNION
        SELECT UPC.CONTRACT_ID,
               UPB.ISSUE_OFFICE BRANCH_CODE,
               (SELECT BRANCH_NAME
                  FROM CMDM_BRANCH
                 WHERE BRANCH_CODE = UPB.ISSUE_OFFICE)
                  BRANCH_NAME,
               CN.AGENT_CODE,
               (SELECT DVA.NAME
                  FROM DMAG_VI_AGENT DVA
                 WHERE DVA.AGENTCODE = CN.AGENT_CODE)
                  AGENT_NAME,
               UPC.LOB,
               (SELECT CODE_DESC
                  FROM cmge_code
                 WHERE CAT_CODE = ''LOB'' AND code_cd = UPC.LOB)
                  LOB_DESC,
               CN.PRODUCT_CONFIG_CODE PRODUCT_CODE,
               (SELECT CODE_DESC
                  FROM cmge_code
                 WHERE     CAT_CODE = UPC.LOB || ''_PRODUCT''
                       AND code_cd = CN.PRODUCT_CONFIG_CODE)
                  PRODUCT_DESC,
               (SELECT NAME_EXT
                  FROM CPGE_VI_PARTNERS_ALL CP
                 WHERE     CP.PART_ID = CN.CP_PART_ID
                       AND CP.VERSION = CN.CP_VERSION)
                  NAME,
               CN.CNOTE_DATE,
               CN.CN_TYPE,
               CN.CNOTE_NO,
               cn.STATUS,
               (SELECT CODE_DESC
                  FROM cmge_code
                 WHERE CAT_CODE = ''CN_STATUS'' AND code_cd = cn.STATUS)
                  STATUS_DESC,
               BSM.RECEIVED_DATE SCRTNY_ISSUE_DATE,
               BSM.REFERENCE_NO SCRTNY_REF_NO,
               --BS.CNOTE_NO SCRTNY_CNOTE_NO,
               (select BS.CNOTE_NO from BPUW_SCRUTINY BS where UPV.REF_NO = BS.REFERENCE_NO) SCRTNY_CNOTE_NO,
               BSM.CREATED_BY SCRTNY_USER_NAME,
               NMCN_ENDT.NMCN_ISSUE_DATE,
               NMCN_ENDT.NMCN_ENDT_NO,
               NMCN_ENDT.NMCN_ENDT_CN_NO,
               NMCN_ENDT.NMCN_ENDT_ISSUE_BY,
               UPV.ISSUE_DATE ENDT_ISSUE_DATE,
               UPV.ENDT_NO,
               NMCN_ENDT.NMCN_ENDT_CN_NO ENDT_CN_NO,
               UPV.ISSUE_BY AS "ENDT_ISSUE_BY"
          FROM CNGE_NOTE CN,
               UWGE_POLICY_CONTRACTS UPC,
               UWGE_POLICY_BASES UPB,
               UWGE_POLICY_VERSIONS UPV,
               BPGE_SCRUTINY_MAIN BSM,
               --BPUW_SCRUTINY BS,
               /*Through EC165 Patched version Query*/
               (SELECT --OPB.CONTRACT_ID,
                       PB.CONTRACT_ID,
                       (CASE
                           WHEN PV.ENDT_NO IS NOT NULL THEN PV.ENDT_NO
                           ELSE (select OPB.POLICY_REF from OCP_POLICY_BASES OPB where OPB.CONTRACT_ID = PB.CONTRACT_ID and OPB.VERSION_NO = 1) 
                           --OPB.POLICY_REF
                        END)
                          NMCN_ENDT_NO,
                       PV.ISSUE_BY NMCN_ENDT_ISSUE_BY,
                       PV.ISSUE_DATE NMCN_ISSUE_DATE,
                       (CASE
                           WHEN PV.VERSION_NO = 1 THEN PB.CNOTE_NO
                           ELSE PV.ENDT_CNOTE_NO
                        END)
                          NMCN_ENDT_CN_NO,
                       PV.VERSION_NO
                  FROM --OCP_POLICY_BASES OPB,
                       UWGE_POLICY_BASES PB,
                       uwge_policy_versions PV
                 WHERE     --OPB.CONTRACT_ID = pb.CONTRACT_ID --nk
                       --AND 
                       PB.CONTRACT_ID = pv.CONTRACT_ID
                       AND PV.CNOTE_UW_IND = ''Y''
                       AND PV.VERSION_NO = PB.VERSION_NO
                       ) NMCN_ENDT
         WHERE     UPC.CONTRACT_ID = UPB.CONTRACT_ID
               AND UPC.CONTRACT_ID = UPV.CONTRACT_ID
               AND UPB.VERSION_NO = UPV.VERSION_NO
               AND cn.CNOTE_NO = NMCN_ENDT.NMCN_ENDT_CN_NO
               AND CN.CNOTE_SERIES = ''73''
               --AND UPC.LOB != ''MT''
               AND UPC.CONTRACT_ID = NMCN_ENDT.CONTRACT_ID
               AND NMCN_ENDT.VERSION_NO = UPV.CNOTE_VER_NO
               AND UPV.REF_NO = BSM.REFERENCE_NO(+)
               --AND UPV.REF_NO = BS.REFERENCE_NO(+)
               AND UPV.endt_code = ''165''
               and TRUNC(CN.CNOTE_DATE) BETWEEN TO_DATE ('''||p_CNIssueDateFrom||''',''YYYY-MM-DD'')  AND TO_DATE ('''||p_CNIssueDateTo||''',''YYYY-MM-DD'')
        UNION
        /*Cnote not mapped to Pol/Endt Query*/
        SELECT NULL CONTRACT_ID,
               CN.CNOTE_BRANCH_CODE,
               (SELECT BRANCH_NAME
                  FROM CMDM_BRANCH
                 WHERE BRANCH_CODE = CN.CNOTE_BRANCH_CODE)
                  BRANCH_NAME,
               CN.AGENT_CODE,
               (SELECT DVA.NAME
                  FROM DMAG_VI_AGENT DVA
                 WHERE DVA.AGENTCODE = CN.AGENT_CODE)
                  AGENT_NAME,
               (CASE
                   WHEN CN.LOB = ''07'' THEN ''PL''
                   WHEN CN.LOB = ''10'' THEN ''PL''
                   WHEN CN.LOB = ''09'' THEN ''PL''
                   ELSE CN.LOB
                END)
                  LOB,
               (CASE
                   WHEN CN.LOB = ''07''
                   THEN
                      ''PLINES''
                   WHEN CN.LOB = ''10''
                   THEN
                      ''PLINES''
                   WHEN CN.LOB = ''09''
                   THEN
                      ''PLINES''
                   ELSE
                      (SELECT CODE_DESC
                         FROM cmge_code
                        WHERE CAT_CODE = ''LOB'' AND code_cd = CN.LOB)
                END)
                  LOB_DESC,
               CN.PRODUCT_CONFIG_CODE PRODUCT_CODE,
               (SELECT CODE_DESC
                  FROM cmge_code
                 WHERE     CAT_CODE LIKE ''%_PRODUCT''
                       AND code_cd = CN.PRODUCT_CONFIG_CODE
                       AND ROWNUM = 1)
                  PRODUCT_DESC,
               (SELECT NAME_EXT
                  FROM CPGE_VI_PARTNERS_ALL CP
                 WHERE     CP.PART_ID = CN.CP_PART_ID
                       AND CP.VERSION = CN.CP_VERSION)
                  NAME,
               CN.CNOTE_DATE,
               CN.CN_TYPE,
               CN.CNOTE_NO,
               cn.STATUS,
               (SELECT CODE_DESC
                  FROM cmge_code
                 WHERE CAT_CODE = ''CN_STATUS'' AND code_cd = cn.STATUS)
                  STATUS_DESC,
               NULL SCRTNY_ISSUE_DATE,
               NULL SCRTNY_REF_NO,
               NULL SCRTNY_CNOTE_NO,
               NULL SCRTNY_USER_NAME,
               NULL NMCN_ISSUE_DATE,
               NULL NMCN_ENDT_NO,
               NULL NMCN_ENDT_CN_NO,
               NULL NMCN_ENDT_ISSUE_BY,
               NULL ENDT_ISSUE_DATE,
               NULL ENDT_NO,
               NULL ENDT_CN_NO,
               NULL AS "ENDT_ISSUE_BY"
          FROM CNGE_NOTE CN
         WHERE     CN.CNOTE_SERIES = ''73''
         and TRUNC(CN.CNOTE_DATE) BETWEEN  TO_DATE ('''||p_CNIssueDateFrom||''',''YYYY-MM-DD'')  AND TO_DATE ('''||p_CNIssueDateTo||''',''YYYY-MM-DD'')
               AND NOT EXISTS
                      (SELECT 1
                         FROM UWGE_POLICY_BASES UPB
                        WHERE UPB.CNOTE_NO = CN.CNOTE_NO)
               AND NOT EXISTS
                      (SELECT 1
                         FROM WIP_UWGE_POLICY_BASES UPB
                        WHERE UPB.CNOTE_NO = CN.CNOTE_NO)
               AND NOT EXISTS
                      (SELECT 1
                         FROM UWGE_POLICY_VERSIONS UPV
                        WHERE UPV.ENDT_CNOTE_NO = CN.CNOTE_NO)
               AND NOT EXISTS
                      (SELECT 1
                         FROM WIP_UWGE_POLICY_VERSIONS UPV
                        WHERE UPV.ENDT_CNOTE_NO = CN.CNOTE_NO)
               AND NOT EXISTS
                      (SELECT 1
                         FROM BPUW_SCRUTINY BS
                        WHERE BS.CNOTE_NO = CN.CNOTE_NO)) NMCN
WHERE 1 = 1';
V_STEPS        := '003' ;
  IF p_CN_Status       IS NOT NULL THEN
    lStrSQL := lStrSQL ||' AND NMCN.STATUS ='''||p_CN_Status||'''';
  END IF;

    IF(p_Branch='ALL' )THEN
    lStrSQL := lStrSQL ||' AND (NMCN.BRANCH_CODE IN (SELECT DISTINCT CMB.BRANCH_CODE FROM CMDM_BRANCH CMB) OR NMCN.BRANCH_CODE IS NULL)';

    ELSIF p_Branch IS NOT NULL  THEN
    lStrSQL := lStrSQL ||' AND NMCN.BRANCH_CODE IN (select regexp_substr('''||p_Branch||''',''[^|]+'', 1, level) from dual connect by regexp_substr('''||p_Branch||''', ''[^|]+'', 1, level) is not null )';

    END IF;
--    IF p_Branch != 'ALL' AND p_Branch IS NOT NULL  THEN
--    lStrSQL := lStrSQL ||' AND NMCN.BRANCH_CODE IN (select regexp_substr('''||p_Branch||''',''[^|]+'', 1, level) from dual connect by regexp_substr('''||p_Branch||''', ''[^|]+'', 1, level) is not null )';
--     
--  END IF;
    IF p_Agent_Code       IS NOT NULL THEN
    lStrSQL := lStrSQL ||' AND NMCN.AGENT_CODE ='''||p_Agent_Code||'''';
  END IF;
    IF p_Scrtny_User       IS NOT NULL THEN
    lStrSQL := lStrSQL ||' AND NMCN.SCRTNY_USER_NAME ='''||p_Scrtny_User||'''';
  END IF;
  IF p_Lob       IS NOT NULL THEN
    lStrSQL := lStrSQL ||' AND NMCN.LOB ='''||p_Lob||'''';
  END IF;
  IF p_Product='ALL' AND p_Lob       IS NOT NULL THEN
   v_CatCode := p_Lob||'_PRODUCT';
    lStrSQL := lStrSQL ||' AND NMCN.PRODUCT_CODE IN (select CC.CODE_CD from CMGE_CODE CC where CC.CAT_CODE = '''||v_CatCode||''')'; 
  ELSIF p_Product IS NOT NULL  THEN 
    lStrSQL := lStrSQL ||' AND NMCN.PRODUCT_CODE IN (select regexp_substr('''||p_Product||''',''[^|]+'', 1, level) from dual connect by regexp_substr('''||p_Product||''', ''[^|]+'', 1, level) is not null )';
  END IF;
  /*IF(p_CNIssueDateFrom IS NOT NULL AND p_CNIssueDateTo IS NOT NULL) THEN
    lStrSQL     := lStrSQL ||' AND TRUNC(NMCN.CNOTE_DATE) BETWEEN  TO_DATE ('''||p_CNIssueDateFrom||''',''YYYY-MM-DD'')  AND TO_DATE ('''||p_CNIssueDateTo||''',''YYYY-MM-DD'') ';

  END IF;*/
  
    V_STEPS := '004';
  BEGIN
    EXECUTE IMMEDIATE lStrSQL BULK COLLECT INTO PATCHING_NMCN;
  END;
  V_STEPS := '005';

  FOR V_ROW_NUM IN 1 .. PATCHING_NMCN.COUNT
  LOOP
          r_row.S_NO                 :=     PATCHING_NMCN (V_ROW_NUM).S_NO;    
        r_row.CONTRACT_ID         :=    PATCHING_NMCN (V_ROW_NUM).CONTRACT_ID;        
        r_row.BRANCH_CODE         :=    PATCHING_NMCN (V_ROW_NUM).BRANCH_CODE;
        r_row.BRANCH_NAME         :=    PATCHING_NMCN (V_ROW_NUM).BRANCH_NAME;
        r_row.AGENT_CODE          :=    PATCHING_NMCN (V_ROW_NUM).AGENT_CODE;
        r_row.AGENT_NAME          :=    PATCHING_NMCN (V_ROW_NUM).AGENT_NAME;
        r_row.LOB_DESC                   :=    PATCHING_NMCN (V_ROW_NUM).LOB_DESC;

        r_row.PRODUCT_CODE        :=     PATCHING_NMCN (V_ROW_NUM).PRODUCT_CODE;
        r_row.PRODUCT_DESC         :=    PATCHING_NMCN (V_ROW_NUM).PRODUCT_DESC;
        r_row.CP_NAME         :=    PATCHING_NMCN (V_ROW_NUM).CP_NAME;
        r_row.CNOTE_DATE         :=    PATCHING_NMCN (V_ROW_NUM).CNOTE_DATE;
        r_row.CN_TYPE             :=    PATCHING_NMCN (V_ROW_NUM).CN_TYPE;
        r_row.CNOTE_NO             :=    PATCHING_NMCN (V_ROW_NUM).CNOTE_NO;
        r_row.STATUS             :=    PATCHING_NMCN (V_ROW_NUM).STATUS;
        r_row.STATUS_DESC         :=    PATCHING_NMCN (V_ROW_NUM).STATUS_DESC;
        r_row.SCRTNY_ISSUE_DATE :=    PATCHING_NMCN (V_ROW_NUM).SCRTNY_ISSUE_DATE;
        r_row.SCRTNY_REF_NO     :=    PATCHING_NMCN (V_ROW_NUM).SCRTNY_REF_NO;
        r_row.SCRTNY_CNOTE_NO     :=    PATCHING_NMCN (V_ROW_NUM).SCRTNY_CNOTE_NO;
        r_row.SCRTNY_USER_NAME     :=    PATCHING_NMCN (V_ROW_NUM).SCRTNY_USER_NAME;
        r_row.NMCN_ISSUE_DATE     :=    PATCHING_NMCN (V_ROW_NUM).NMCN_ISSUE_DATE;
        r_row.NMCN_ENDT_NO         :=    PATCHING_NMCN (V_ROW_NUM).NMCN_ENDT_NO;
        r_row.NMCN_ENDT_CN_NO     :=    PATCHING_NMCN (V_ROW_NUM).NMCN_ENDT_CN_NO;
        r_row.NMCN_ENDT_ISSUE_BY :=    PATCHING_NMCN (V_ROW_NUM).NMCN_ENDT_ISSUE_BY;

        r_row.ENDT_ISSUE_DATE     :=    PATCHING_NMCN (V_ROW_NUM).ENDT_ISSUE_DATE;
        r_row.ENDT_NO             :=    PATCHING_NMCN (V_ROW_NUM).ENDT_NO;
        r_row.ENDT_CN_NO         :=    PATCHING_NMCN (V_ROW_NUM).ENDT_CN_NO;
        r_row.ENDT_ISSUE_BY     :=    PATCHING_NMCN (V_ROW_NUM).ENDT_ISSUE_BY;
        PIPE ROW(r_row);

  END LOOP;

 V_STEPS := '007';
  V_STEPS := '006';
RETURN;
  EXCEPTION
WHEN OTHERS THEN
  PG_UTIL_LOG_ERROR.PC_INS_log_error ( v_ProcName_v || '.' || v_ProcName_v ||' V_STEPS:'||V_STEPS||'lStrSQL'||lStrSQL, 1, SQLERRM);
 END FN_NMCN_PATCHING_RPT;
 --10.00 end
-- 13.00 start
FUNCTION FN_REFUND_PRE_PYMT_LISTING (pFromDate      IN DATE,
                                       pToDate       IN DATE,
                                       p_StrBranch      IN VARCHAR2,
                                       p_strAgentCode   IN VARCHAR2)
      RETURN REFUND_PRE_PYMT_LISTING_TAB
      PIPELINED
   IS
      V_PROCNAME_V   VARCHAR2 (30) := 'FN_REFUND_PRE_PYMT_LISTING';
       r_row PG_RPGE_LISTING.REFUND_PRE_PYMT_LISTING_REC;

      TYPE v_cursor_type IS REF CURSOR;

      v_cursor       v_cursor_type;
      lStrSQL        VARCHAR2 (32757);

   BEGIN
      lStrSQL :=
            'SELECT TRUNC(MST.TRAN_DATE) AS TRAN_DATE, '
         || ' AT.ISSUE_OFFICE AS BRANCH_CODE, '
         || ' (select BRANCH_NAME from CMDM_BRANCH where CMDM_BRANCH.branch_code = AT.ISSUE_OFFICE) as BRANCH_DESCP, '
         || ' AT.PV_NO AS PAYMENT_NO , AT.AGENT_CODE, TBL1.NAME AS AGENT_NAME,TBL1.SERVICED_BY AS MARKETER, '
         || ' ACPY_LNK.DOC_NO AS P_DOC,  (SELECT CP1.NAME_EXT FROM CPGE_VI_PARTNERS_ALL CP1 WHERE CP1.PART_ID = AT.PART_ID AND CP1.VERSION = AT.PART_VERSION ) AS INSURED,'
         || ' ACPY_LNK.DOC_AMT as DOC_AMT, ACPY_LNK.PREM_DUE, TBL.AUTH_IND, '
         || ' ACPY_LNK.DOC_NO PDOC, '
         || ' (SELECT MST1.STAX FROM ACST_MAST MST1 WHERE MST1.ST_DOC = ACPY_LNK.DOC_NO ) as STAX_AMT, '
         || ' (SELECT MST1.REBT FROM ACST_MAST MST1 WHERE MST1.ST_DOC = ACPY_LNK.DOC_NO ) as REBATE_AMT, '
         || ' (SELECT MST1.STAMP FROM ACST_MAST MST1 WHERE MST1.ST_DOC = ACPY_LNK.DOC_NO ) as STAMP_AMT, ' 
         || ' MST.BATCH_NO, AT.AGENT_CAT_TYPE, (SELECT MST1.POL_NO FROM ACST_MAST MST1 WHERE MST1.ST_DOC = ACPY_LNK.DOC_NO ) as POL_NO, '
         || ' AT.ENDT_ISSUE_DATE AS ISSUE_DATE,(SELECT UW.ENDT_EFF_DATE FROM UWGE_POLICY_VERSIONS UW WHERE UW.ENDT_NO = AT.ENDT_NO) AS EFF_DATE, AT.VEH_NO, '
         || ' (SELECT MST1.GROSS FROM ACST_MAST MST1 WHERE MST1.ST_DOC = ACPY_LNK.DOC_NO ) as GROSS_PREM '
         || ' FROM   ACPY_AUTO_PREMREFUND AT,ACPY_PAYLINK_KO ACPY_LNK, ACST_MATCH MTC, ACST_MAST MST , '
         || ' (SELECT DISTINCT(DMT.REFERENCE_CODE) AGENTCODE,CP.NAME_EXT NAME,DMAG.SERVICED_BY  '
         || ' FROM DMT_AGENTS DMT,DMAG_AGENTS DMAG, CPGE_VI_PARTNERS_ALL CP '
         || ' WHERE DMT.INT_ID = DMAG.INT_ID  '
         || ' AND DMAG.PART_ID = CP.PART_ID  '
         || ' AND DMAG.PART_VERSION = CP.VERSION '
         || ' UNION ALL '
         || ' SELECT RI.DMT_AGENT_CODE AG_CODE, CP.NAME_EXT NAME,RI.SERVICED_BY NAME  '
         || ' FROM DMAG_RI RI JOIN CPGE_VI_PARTNERS_ALL CP ON RI.PART_ID = CP.PART_ID  '
         || ' AND RI.PART_VERSION = CP.VERSION) TBL1 ,  '
         || ' (SELECT AC_NO, TRANSFER_IND, TRANSFER2_IND, TRANSFER_OPERATOR, TRANSFER2_OPERATOR, '
         || ' CASE WHEN ( TRANSFER_IND = ''Y'' AND TRANSFER2_IND = ''Y'' AND TRANSFER_OPERATOR = ''ONL_APPROVER1''  '
         || ' AND TRANSFER2_OPERATOR = ''ONL_APPROVER2'' ) THEN ''Y'' ELSE ''N'' END AS AUTH_IND '
         || ' FROM   ACPY_PAYLINK) TBL  '
         || ' WHERE   '
         || ' AT.PV_NO = ACPY_LNK.AC_NO AND  '
         || ' AT.PV_NO = TBL.AC_NO   '
         || ' AND AT.PV_NO = MST.ST_DOC '
         || ' AND TBL1.AGENTCODE = AT.AGENT_CODE   '
         || ' AND MTC.MATCH_DOC_NO = ACPY_LNK.DOC_NO '
         --|| ' AND MTC.DOC_NO <> AT.PV_NO  '
         || ' AND MTC.ST_SEQ_NO IN (SElECT MAX(ST_SEQ_NO) FROM ACST_MATCH WHERE MATCH_DOC_NO =  ACPY_LNK.DOC_NO ) '
         || ' AND AT.PV_NO IS NOT NULL  '
         || ' AND TRUNC(MST.TRAN_DATE) BETWEEN '''
         || pFromDate
         || ''' AND '''
         || pToDate
         || '''';

      IF p_strAgentCode IS NOT NULL
      THEN
         lStrSQL :=
            lStrSQL || ' AND AT.AGENT_CODE = ''' || p_strAgentCode || '''';
      END IF;

      IF p_StrBranch IS NOT NULL
      THEN
         lStrSQL :=
            lStrSQL || ' AND AT.ISSUE_OFFICE = ''' || p_StrBranch || '''';
      END IF;

       lStrSQL :=
            lStrSQL || ' ORDER BY TRUNC(MST.TRAN_DATE), AT.ISSUE_OFFICE ASC ';

    
      DBMS_OUTPUT.put_line ('lStrSQL Query - ' || lStrSQL);

      OPEN v_cursor FOR lStrSQL;

      LOOP
         FETCH v_cursor
            INTO r_row.TRAN_DATE,
                r_row.BRANCH_CODE,
                r_row.BRANCH_DESCP,
                r_row.PAYMENT_NO,
                r_row.AGENT_CODE,
                r_row.AGENT_NAME,
                r_row.MARKETER,
                r_row.P_DOC,
                r_row.INSURED,
                r_row.DOC_AMT,
                r_row.PREM_DUE,
                r_row.AUTH_IND,
                r_row.PDOC,
                r_row.STAX_AMT,
                r_row.REBATE_AMT,
                r_row.STAMP_AMT,
                r_row.BATCH_NO,
                r_row.AGENT_CAT_TYPE,
                r_row.POL_NO,
                r_row.ISSUE_DATE,
                r_row.EFF_DATE,
                r_row.VEH_NO, 
                r_row.GROSS_PREM;

         EXIT WHEN v_cursor%NOTFOUND;
         PIPE ROW (r_row);
      END LOOP;

      CLOSE v_cursor;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error (
            v_ProcName_v || '.' || v_ProcName_v,
            1,
            SQLERRM);
   END FN_REFUND_PRE_PYMT_LISTING;
--13.00 end
--13.00 start <Rejected Payment Daily Report>
FUNCTION FN_REFUND_REJ_PYMT_LISTING (pFromDate        IN DATE,
                                     pToDate           IN DATE,
                                     p_StrBranch    IN VARCHAR2,
                                     p_bank_code    IN VARCHAR2)
      RETURN REFUND_REJ_PYMT_LISTING_TAB
      PIPELINED
   IS
      V_PROCNAME_V   VARCHAR2 (30) := 'FN_REFUND_REJ_PYMT_LISTING';
       r_row PG_RPGE_LISTING.REFUND_REJ_PYMT_LISTING_REC;

      TYPE v_cursor_type IS REF CURSOR;

      v_cursor       v_cursor_type;
      lStrSQL        VARCHAR2 (32757);

   BEGIN
   
       lStrSQL :=
            'SELECT TRUNC(AT.CHQ_DATE) AS CHQ_DATE, '
            || ' CASE WHEN RF.REJ_STATUS = ''APR'' THEN ''APPROVED'' WHEN RF.REJ_STATUS = ''REJ'' THEN ''REJECTED'' ELSE '''' END AS STATUS,'    
            || ' AT.CHQ_NO, TRUNC(AT.TRAN_DATE) AS ISSUE_DATE, '
            || ' AT.OPERATOR AS ISSUED_BY,AT.AGENT_CODE,'
            || ' (select DVDI.NAME from DMAG_VI_DIR_INW DVDI where DVDI.AGENTCODE = AT.AGENT_CODE and rownum =1) AS AGENT_NAME, '
            || ' (SELECT DMAG.SERVICED_BY  '
            || ' FROM DMT_AGENTS DMT,DMAG_AGENTS DMAG '
            || ' WHERE DMT.INT_ID = DMAG.INT_ID  '
            || ' AND DMT.REFERENCE_CODE = AT.AGENT_CODE '
            || ' UNION ALL '
            || ' SELECT RI.SERVICED_BY '  
            || ' FROM DMAG_RI RI JOIN CPGE_VI_PARTNERS_ALL CP ON RI.PART_ID = CP.PART_ID '  
            || ' AND RI.PART_VERSION = CP.VERSION '
            || ' AND RI.DMT_AGENT_CODE = AT.AGENT_CODE) AS MARKETER, '
            || ' AT.BATCH_NO, '
            || ' AT.AC_NO AS PV_NO, '
            || ' RF.ENDT_NO AS DOC_NO, '
            || ' RF.VEH_NO AS VEH_NO, ' 
            || ' AT.STMT_DESCP AS PYMT_DESC, '
            || ' AT.NAME AS PAYEE_NAME, ' 
            || ' AT.AMOUNT, '
            || ' (select APN.NARR from ACPY_PAYLINK_NARR APN where APN.AC_NO = AT.AC_NO ) AS NARRATION, '       
            || ' CASE WHEN (select APD.DEL_DATE from ACPY_PYMT_DEL APD where APD.AC_NO = AT.AC_NO ) IS NULL THEN ''-''' 
            || ' ELSE TO_CHAR((select APD.DEL_DATE from ACPY_PYMT_DEL APD where APD.AC_NO = AT.AC_NO ),''DD/MM/YYYY'') END || ''/ ''|| CASE WHEN (SELECT TO_CHAR(AT1.TRAN_DATE,''DD/MM/YYYY'') FROM ACPY_PYMT AT1 WHERE AT1.AC_NO = AT.AC_NO|| ''*C'') IS NULL THEN ''-'' ELSE (SELECT TO_CHAR(AT1.TRAN_DATE,''DD/MM/YYYY'') FROM ACPY_PYMT AT1 WHERE AT1.AC_NO = AT.AC_NO|| ''*C'') END AS DEL_DATE '       
            || ' FROM ACPY_PYMT AT, ACPY_AUTO_PREMREFUND RF '
            || ' WHERE ' 
            || ' AT.AC_NO = RF.PV_NO '
            || ' AND RF.REJ_STATUS = ''REJ'''
            || ' AND TRUNC(AT.TRAN_DATE) BETWEEN '''
            || pFromDate
            || ''' AND '''
            || pToDate
            || ''''
            || 'UNION ALL SELECT TRUNC(AT.CHQ_DATE) AS CHQ_DATE, '
            || ' CASE WHEN RF.REJ_STATUS = ''APR'' THEN ''APPROVED'' WHEN RF.REJ_STATUS = ''REJ'' THEN ''REJECTED'' ELSE '''' END AS STATUS,'    
            || ' AT.CHQ_NO, TRUNC(AT.TRAN_DATE) AS ISSUE_DATE, '
            || ' AT.OPERATOR AS ISSUED_BY,AT.AGENT_CODE,'
            || ' (select DVDI.NAME from DMAG_VI_DIR_INW DVDI where DVDI.AGENTCODE = AT.AGENT_CODE and rownum =1) AS AGENT_NAME, '
            || ' (SELECT DMAG.SERVICED_BY  '
            || ' FROM DMT_AGENTS DMT,DMAG_AGENTS DMAG '
            || ' WHERE DMT.INT_ID = DMAG.INT_ID  '
            || ' AND DMT.REFERENCE_CODE = AT.AGENT_CODE '
            || ' UNION ALL '
            || ' SELECT RI.SERVICED_BY '  
            || ' FROM DMAG_RI RI JOIN CPGE_VI_PARTNERS_ALL CP ON RI.PART_ID = CP.PART_ID '  
            || ' AND RI.PART_VERSION = CP.VERSION '
            || ' AND RI.DMT_AGENT_CODE = AT.AGENT_CODE) AS MARKETER, '
            || ' AT.BATCH_NO, '
            || ' AT.AC_NO AS PV_NO, '
            || ' RF.ENDT_NO AS DOC_NO, '
            || ' RF.VEH_NO AS VEH_NO, ' 
            || ' AT.STMT_DESCP AS PYMT_DESC, '
            || ' AT.NAME AS PAYEE_NAME, ' 
            || ' AT.AMOUNT, '
            || ' (select APN.NARR from ACPY_PAYLINK_NARR APN where APN.AC_NO = AT.AC_NO ) AS NARRATION, '       
            || ' CASE WHEN (select APD.DEL_DATE from ACPY_PYMT_DEL APD where APD.AC_NO = AT.AC_NO ) IS NULL THEN ''-''' 
            || ' ELSE TO_CHAR((select APD.DEL_DATE from ACPY_PYMT_DEL APD where APD.AC_NO = AT.AC_NO ),''DD/MM/YYYY'') END || ''/ ''|| CASE WHEN (SELECT TO_CHAR(AT1.TRAN_DATE,''DD/MM/YYYY'') FROM ACPY_PYMT AT1 WHERE AT1.AC_NO = AT.AC_NO|| ''*C'') IS NULL THEN ''-'' ELSE (SELECT TO_CHAR(AT1.TRAN_DATE,''DD/MM/YYYY'') FROM ACPY_PYMT AT1 WHERE AT1.AC_NO = AT.AC_NO|| ''*C'') END AS DEL_DATE '       
            || ' FROM ACPY_PYMT_DEL AT, ACPY_AUTO_PREMREFUND RF '
            || ' WHERE ' 
            || ' AT.AC_NO = RF.PV_NO '
            || ' AND RF.REJ_STATUS = ''REJ'''
            || ' AND TRUNC(AT.TRAN_DATE) BETWEEN '''
            || pFromDate
            || ''' AND '''
            || pToDate
            || '''';


   /*
      lStrSQL :=
            'SELECT TRUNC(AT.CHQ_DATE) AS CHQ_DATE, '
         || ' ''IBG/TT - Rejected'' AS STATUS, '
         || ' AT.CHQ_NO, '
         || ' TRUNC(AT.TRAN_DATE) AS ISSUE_DATE, '
         || ' AT.OPERATOR AS ISSUED_BY, '
         || ' AT.AGENT_CODE, '
         || ' (select DVDI.NAME from DMAG_VI_DIR_INW DVDI where DVDI.AGENTCODE = AT.AGENT_CODE and rownum =1) AS AGENT_NAME, '         
         || ' ''Marker'' AS MARKETER, '        
         || ' AT.BATCH_NO, '
         || ' AT.AC_NO AS PV_NO, '         
         || ' (select ACP.ENDT_NO from ACPY_AUTO_PREMREFUND ACP where ACP.PV_NO = AT.AC_NO and rownum =1) AS DOC_NO, '
         || ' (select ACP.ENDT_NO from ACPY_AUTO_PREMREFUND ACP where ACP.PV_NO = AT.AC_NO and rownum =1) AS VEH_NO, '        
         || ' AT.STMT_DESCP AS PYMT_DESC, '
         || ' AT.NAME AS PAYEE_NAME, '
         || ' AT.AMOUNT, '
         || ' (select APN.NARR from ACPY_PAYLINK_NARR APN where APN.AC_NO = AT.AC_NO ) AS NARRATION, '    
         || ' (select APD.DEL_DATE from ACPY_PYMT_DEL APD where APD.AC_NO = AT.AC_NO ) AS DEL_DATE '
         || ' FROM ACPY_PYMT AT  '
         || ' WHERE '                 
         || ' TRUNC(AT.TRAN_DATE) BETWEEN '''
         || pFromDate
         || ''' AND '''
         || pToDate
         || ''''; */
      IF p_StrBranch IS NOT NULL
      THEN
         lStrSQL :=
            lStrSQL || ' AND AT.ISSUE_OFFICE = ''' || p_StrBranch || '''';
      END IF;
      IF p_bank_code IS NOT NULL
      THEN
         lStrSQL :=
             /* lStrSQL || ' AND AT.BANK_CODE = ''' || p_bank_code || ''''; */ -- 26.00 START
            lStrSQL || ' AND AT.BANK = ''' || p_bank_code || ''''; -- 26.00 END
      END IF;
    
      DBMS_OUTPUT.put_line ('lStrSQL Query - ' || lStrSQL);

      OPEN v_cursor FOR lStrSQL;

      LOOP
         FETCH v_cursor
            INTO r_row.CHQ_DATE,
                r_row.STATUS,        
                r_row.CHQ_NO,    
                r_row.ISSUE_DATE,            -- PV generated date    
                r_row.ISSUED_BY,        
                r_row.AGENT_CODE,    
                r_row.AGENT_NAME,        
                r_row.MARKETER,        
                r_row.BATCH_NO,        
                r_row.PV_NO,        
                r_row.DOC_NO,                -- Endorsement code    
                r_row.VEH_NO,        
                r_row.PYMT_DESC,            
                r_row.PAYEE_NAME,            -- Payee name for the refund
                r_row.AMOUNT,                -- Amount to be refunded    
                r_row.NARRATION,    
                r_row.DEL_DATE;                 -- Date PV deleted / Reversed
         EXIT WHEN v_cursor%NOTFOUND;
         PIPE ROW (r_row);
      END LOOP;

      CLOSE v_cursor;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error (
            v_ProcName_v || '.' || v_ProcName_v,
            1,
            SQLERRM);
   END FN_REFUND_REJ_PYMT_LISTING;
--14.00 end
   
   FUNCTION FN_REFUND_UPD_BENE_LISTING (pFromDate        IN DATE,
                                     pToDate           IN DATE,
                                     p_StrBranch    IN VARCHAR2,
                                     p_bank_code    IN VARCHAR2)
   RETURN REFUND_UPD_BENE_LISTING_TAB
   PIPELINED
IS
   V_PROCNAME_V    VARCHAR2 (30) := 'FN_REFUND_UPD_BENE_LISTING';
   R_ROW           REFUND_UPD_BENE_LISTING_REC;

   V_EXT_BENE_NAME      VARCHAR (200);
   V_EXT_IBG_VERIFY_ID  VARCHAR (40);
   V_EXT_BANK_CODE      VARCHAR (15);
   V_EXT_BANK_ACC_NO    VARCHAR (30);
   V_NEW_BENE_NAME      VARCHAR (200);
   V_NEW_IBG_VERIFY_ID  VARCHAR (40);
   V_NEW_BANK_CODE      VARCHAR (15);
   V_NEW_BANK_ACC_NO    VARCHAR (30);
    -- 157219: extra columns
   V_EXT_BANK_NAME      VARCHAR2 (100); 
   V_NEW_BANK_NAME      VARCHAR2 (100);
   
BEGIN
DBMS_OUTPUT.put_line ('STEP-01A');
   FOR r
      IN ( 
      SELECT 
            ACPY_RF.UPDATED_DATE AS UPDATED_ON, 
            ACPY_RF.UPDATED_BY, 
            ACPY_RF.ENDT_NO,
            ACPY_RF.PV_NO AS REJ_PV_NO, 
            ACPY_PAY.TRAN_DATE AS REJ_ISSUE_DATE,
            CASE WHEN (SELECT ACPY_PYMT_DEL.DEL_REASON FROM ACPY_PYMT_DEL ACPY_PYMT_DEL WHERE ACPY_PYMT_DEL.AC_NO = ACPY_PAY.AC_NO) IS NOT NULL THEN 
                (SELECT ACPY_PYMT_DEL.DEL_REASON FROM ACPY_PYMT_DEL ACPY_PYMT_DEL WHERE ACPY_PYMT_DEL.AC_NO = ACPY_PAY.AC_NO)
            ||' ' ||(SELECT SAPM.DESCP FROM ACPY_PYMT_DEL ACPY_PYMT_DEL, SAPM_SC_DET SAPM WHERE ACPY_PYMT_DEL.AC_NO = ACPY_PAY.AC_NO 
            AND ACPY_PYMT_DEL.DEL_REASON = SAPM.SCCODE AND SCTYPE = 'ACPY_DEL_REASON')
            ELSE (SELECT ACPY_PYMT.REV_REASON FROM ACPY_PYMT ACPY_PYMT WHERE ACPY_PYMT.AC_NO = ACPY_PAY.AC_NO||'*C')||' ' ||(SELECT SAPM.DESCP 
            FROM ACPY_PYMT ACPY_PYMT, SAPM_SC_DET SAPM WHERE ACPY_PYMT.AC_NO = ACPY_PAY.AC_NO||'*C' AND ACPY_PYMT.REV_REASON = SAPM.SCCODE AND SCTYPE = 'ACPY_DEL_REASON') END REJ_REASON, 
            (SELECT ACPY_PYMT_DEL.DEL_REMARK FROM ACPY_PYMT_DEL ACPY_PYMT_DEL WHERE ACPY_PYMT_DEL.AC_NO = ACPY_PAY.AC_NO) AS REJ_REMARKS,
            ACPY_RF.BENE_ID, ACPY_RF.BENE_VERSION,
            TBL.PV_NO AS NEW_PV_NO,
            (SELECT ACPY_PAY_NEW.TRAN_DATE FROM ACPY_PAYLINK ACPY_PAY_NEW WHERE ACPY_PAY_NEW.AC_NO = TBL.PV_NO) AS NEW_PV_ISSUED_DATE, 
            TBL.NEW_BENE_ID, TBL.NEW_BENE_VERSION,
            TBL.AUTHORIZED_BY, TBL.AUTH_DATE
            FROM ACPY_AUTO_PREMREFUND ACPY_RF
            LEFT OUTER JOIN ACPY_PAYLINK ACPY_PAY ON ACPY_PAY.AC_NO = ACPY_RF.PV_NO
            LEFT OUTER JOIN (SELECT ACPY_RF_NEW.PV_NO, ACPY_RF_NEW.PREV_PV_NO, ACPY_RF_NEW.BENE_ID AS NEW_BENE_ID, ACPY_RF_NEW.BENE_VERSION AS NEW_BENE_VERSION, ACPY_RF_NEW.AUTHORIZED_BY, 
            TO_CHAR(ACPY_RF_NEW.AUTH_DATE,'DD-MM-YYYY') AS AUTH_DATE
            FROM ACPY_AUTO_PREMREFUND ACPY_RF_NEW ) TBL ON TBL.PREV_PV_NO = ACPY_RF.PV_NO
            WHERE ACPY_RF.REJ_STATUS = 'REJ'
            AND TRUNC(ACPY_RF.UPDATED_DATE) BETWEEN pFromDate AND pToDate
            AND (p_StrBranch is null or ACPY_RF.ISSUE_OFFICE= p_StrBranch)
         --   SELECT 1 from DUAL
            )
      
   LOOP
      DBMS_OUTPUT.put_line ('STEP-01');
      DBMS_OUTPUT.put_line ('r.BENE_ID '|| r.BENE_ID );
      DBMS_OUTPUT.put_line ('r.BENE_VERSION '|| r.BENE_VERSION );
        -- Get existing Beneficiary Info
        SELECT EXT_BENE_NAME, EXT_IBG_VERIFY_ID, EXT_BANK_CODE, EXT_BANK_ACC_NO,BANK_NAME
        INTO V_EXT_BENE_NAME, V_EXT_IBG_VERIFY_ID, V_EXT_BANK_CODE, V_EXT_BANK_ACC_NO, V_EXT_BANK_NAME
        FROM(
        SELECT CPGE.BENE_NAME AS EXT_BENE_NAME, CPGE.IBG_VERIFY_ID AS EXT_IBG_VERIFY_ID, CPGE.BANK_CODE AS EXT_BANK_CODE, CPGE.BANK_ACC_NO AS EXT_BANK_ACC_NO,
        (SELECT CM.BANK_DESCP FROM CMGE_BANK CM WHERE CM.BANK_CODE = CPGE.BANK_CODE) AS BANK_NAME
        FROM CPGE_PARTNERS_BENE CPGE
        WHERE CPGE.BENE_ID = r.BENE_ID 
        AND CPGE.BENE_VERSION = r.BENE_VERSION
        UNION ALL 
        SELECT CPGE_H.BENE_NAME AS EXT_BENE_NAME, CPGE_H.IBG_VERIFY_ID AS EXT_IBG_VERIFY_ID, CPGE_H.BANK_CODE AS EXT_BANK_CODE, CPGE_H.BANK_ACC_NO AS EXT_BANK_ACC_NO,
        (SELECT CM.BANK_DESCP FROM CMGE_BANK CM WHERE CM.BANK_CODE = CPGE_H.BANK_CODE) AS BANK_NAME
        FROM CPGE_PARTNERS_BENE_HIST CPGE_H
        WHERE CPGE_H.BENE_ID = r.BENE_ID 
        AND CPGE_H.BENE_VERSION = r.BENE_VERSION) WHERE ROWNUM =1; 
        
        DBMS_OUTPUT.put_line ('STEP-02');
        
        IF r.NEW_BENE_ID IS NOT NULL THEN 
            -- Get NEW Beneficiary Info
            SELECT NEW_BENE_NAME, NEW_IBG_VERIFY_ID, NEW_BANK_CODE, NEW_BANK_ACC_NO,BANK_NAME
            INTO V_NEW_BENE_NAME, V_NEW_IBG_VERIFY_ID, V_NEW_BANK_CODE, V_NEW_BANK_ACC_NO,V_NEW_BANK_NAME
            FROM(
            SELECT CPGE.BENE_NAME AS NEW_BENE_NAME, CPGE.IBG_VERIFY_ID AS NEW_IBG_VERIFY_ID, CPGE.BANK_CODE AS NEW_BANK_CODE, CPGE.BANK_ACC_NO AS NEW_BANK_ACC_NO,
            (SELECT CM.BANK_DESCP FROM CMGE_BANK CM WHERE CM.BANK_CODE = CPGE.BANK_CODE) AS BANK_NAME
            FROM CPGE_PARTNERS_BENE CPGE
            WHERE CPGE.BENE_ID = r.NEW_BENE_ID 
            AND CPGE.BENE_VERSION = r.NEW_BENE_VERSION
            UNION ALL 
            SELECT CPGE_H.BENE_NAME AS EXT_BENE_NAME, CPGE_H.IBG_VERIFY_ID AS EXT_IBG_VERIFY_ID, CPGE_H.BANK_CODE AS EXT_BANK_CODE, CPGE_H.BANK_ACC_NO AS EXT_BANK_ACC_NO,
            (SELECT CM.BANK_DESCP FROM CMGE_BANK CM WHERE CM.BANK_CODE = CPGE_H.BANK_CODE) AS BANK_NAME
            FROM CPGE_PARTNERS_BENE_HIST CPGE_H
            WHERE CPGE_H.BENE_ID = r.NEW_BENE_ID 
            AND CPGE_H.BENE_VERSION = r.NEW_BENE_VERSION) WHERE ROWNUM =1; 
        -- 17.00 start
        ELSE 
            V_NEW_BENE_NAME     := NULL; 
            V_NEW_IBG_VERIFY_ID := NULL; 
            V_NEW_BANK_CODE     := NULL; 
            V_NEW_BANK_ACC_NO   := NULL;
            V_NEW_BANK_NAME     := NULL;
        --- 17.00 end
        END IF;
        
        DBMS_OUTPUT.put_line ('STEP-03');
        r_row.UPDATED_ON := r.UPDATED_ON;
        r_row.UPDATED_BY := r.UPDATED_BY;
        r_row.ENDT_NO := r.ENDT_NO;
        -- Rejected PV details
        r_row.REJ_PV_NO := r.REJ_PV_NO;
        r_row.REJ_ISSUE_DATE := r.REJ_ISSUE_DATE;
        r_row.REJ_REASON := r.REJ_REASON;
        r_row.REJ_REMARKS := r.REJ_REMARKS;
        -- Existing Beneficiary Details    
        r_row.EXT_BENE_NAME := V_EXT_BENE_NAME;
        r_row.EXT_IBG_VERIFY_ID := V_EXT_IBG_VERIFY_ID;
        r_row.EXT_BANK_CODE := V_EXT_BANK_CODE;
        r_row.EXT_BANK_ACC_NO := V_EXT_BANK_ACC_NO;
        -- Updated Beneficiary Details
        r_row.NEW_BENE_NAME := V_NEW_BENE_NAME;
        r_row.NEW_IBG_VERIFY_ID := V_NEW_IBG_VERIFY_ID;
        r_row.NEW_BANK_CODE := V_NEW_BANK_CODE;
        r_row.NEW_BANK_ACC_NO := V_NEW_BANK_ACC_NO;
        -- Updated PV Details
        r_row.NEW_PV_NO := r.NEW_PV_NO;
        r_row.NEW_PV_ISSUED_DATE := r.NEW_PV_ISSUED_DATE;
        -- 157219: extra columns 
        r_row.EXT_BANK_NAME := V_EXT_BANK_NAME;
        r_row.NEW_BANK_NAME := V_NEW_BANK_NAME;
        r_row.APPROVE_BY := r.AUTHORIZED_BY;
        r_row.APPROVE_DATE_TIME := r.AUTH_DATE;
      PIPE ROW (r_row);
   END LOOP;
   
   RETURN;
EXCEPTION
   WHEN OTHERS
   THEN
      PG_UTIL_LOG_ERROR.PC_INS_log_error (
         v_ProcName_v || '.' || v_ProcName_v,
         1,
         SQLERRM);
END FN_REFUND_UPD_BENE_LISTING;
--13.00 end

-- 18.00 start  
-- 19.00 start
FUNCTION FN_WHO_CON_ACC_DOC_LISTING (pFromDate      IN DATE,
                                       pToDate       IN DATE,
                                       pBranch IN VARCHAR2,
                                       pAgentCode IN VARCHAR2, 
                                       pDocType IN VARCHAR2)
      RETURN WHO_CON_ACC_DOC_LISTING_TAB
      PIPELINED
   IS
      V_PROCNAME_V   VARCHAR2 (30) := 'FN_WHO_CON_ACC_DOC_LISTING';
       r_row PG_RPGE_LISTING.WHO_CON_ACC_DOC_LISTING_REC;

      TYPE v_cursor_type IS REF CURSOR;

      v_cursor       v_cursor_type;
      lStrSQL        VARCHAR2 (32757);

   BEGIN
  

        lStrSQL :=
        'SELECT * FROM ( ';
        
        IF pDocType = 'JN' OR pDocType IS NULL  THEN 
            lStrSQL := lStrSQL || 'SELECT TRAN_DATE, DOWNLOAD_DATE,DOCUMENT_TYPE,DOCUMENT_NO, ACCOUNT_CODE, AGENT_NAME, EVENT_CODE, WHT_AMT, MATCH_DOC, MATCH_DUE_AMOUNT, DOC_AMT,  BRANCH, AGENT_CAT_TYPE,AGENT_MAP FROM ('
            --|| 'SELECT DISTINCT JN.TRAN_DATE, NULL AS DOWNLOAD_DATE, ''Journal'' AS DOCUMENT_TYPE, JN.AC_NO AS DOCUMENT_NO, JN.AGENT_CODE AS ACCOUNT_CODE,  ' --27.00
            || 'SELECT  JN.TRAN_DATE, NULL AS DOWNLOAD_DATE, ''Journal'' AS DOCUMENT_TYPE, JN.AC_NO AS DOCUMENT_NO, KO.AGENT_CODE AS ACCOUNT_CODE,  ' --27.00 ,added 31.00
            || 'BR.NAME AS AGENT_NAME, '
            || '''WHTJNL''  AS EVENT_CODE, NVL(KO.WHT_AMT,0) AS WHT_AMT, CASE WHEN KO.DOC_NO IS NULL THEN ''n/a'' ELSE KO.DOC_NO END AS MATCH_DOC, NVL((KO.COMM_DUE ),0)   AS MATCH_DUE_AMOUNT, BR.BRANCH_CODE AS BRANCH,  '
            --|| ' (NVL((KO.COMM_DUE ),0) - NVL(KO.WHT_AMT,0) ) AS DOC_AMT, ''-'' AS AGENT_CAT_TYPE ' -- 25.00 
            || ' (NVL((KO.COMM_DUE ),0) - NVL(KO.WHT_AMT,0) ) AS DOC_AMT, ''-'' AS AGENT_CAT_TYPE, KO.AGENT_CODE AS AGENT_MAP ' -- 25.00 ,added 31.00
            || 'FROM ACJN_JOUR JN INNER JOIN ACJN_KO KO ON KO.AC_NO = JN.AC_NO ' -- based on DBA commnet changed to Inner join with ACJN_KO @07/03/2022
            || ' , (SELECT CP.NAME_EXT AS NAME, DMT.REFERENCE_CODE AS AGENTCODE, DMAG.BRANCH_CODE, DMAG.SERVICED_BY,DMAG.CHANNEL, DMAG.SUBCHANNEL  '
            || ' FROM DMT_AGENTS DMT, '
            || ' DMAG_AGENTS DMAG, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE DMT.INT_ID = DMAG.INT_ID '
            || ' AND DMAG.PART_ID = CP.PART_ID '
            || ' AND DMAG.PART_VERSION = CP.VERSION '
            || ' UNION ALL '
            || ' SELECT CP.NAME_EXT  AS NAME, RI.DMT_AGENT_CODE AS AGENTCODE, RI.BRANCH AS BRANCH_CODE, RI.SERVICED_BY, NULL AS CHANNEL, NULL AS SUBCHANNEL '
            || ' FROM DMAG_RI RI, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE RI.PART_ID = CP.PART_ID '
            || ' AND RI.PART_VERSION = CP.VERSION) BR '
            || ' WHERE  '
            || ' KO.AGENT_CODE = BR.AGENTCODE ' --31.0
            --|| ' AND JN.AC_NO IN (SELECT AC_NO FROM ACJN_KO) '-- based on DBA commnet changed to Inner join with ACJN_KO @07/03/2022
            || 'AND TRUNC(JN.TRAN_DATE) BETWEEN '''
            || pFromDate
            || ''' AND '''
            || pToDate
            || '''' 
            || ' AND JN.WHT_IND = ''Y'''
            || 'AND JN.AC_NO IN (SELECT GL2.AC_NO FROM ACJN_GL GL2, ACJN_GL GL3 WHERE GL2.AC_NO = GL3.AC_NO  AND GL2.GL_SEQ_NO = KO.GL_SEQ_NO AND GL2.AC_GLSEQ = GL3.AC_GLSEQ AND GL3.EVENT_CODE IN (''WHTJNL'',''WHTADJ'') AND GL2.AC_NO = JN.AC_NO)' ;
            
            IF pBranch IS NOT NULL THEN
               lStrSQL := lStrSQL || ' AND BR.BRANCH_CODE = '''|| pBranch ||'''';
            END IF; 
            
            IF pAgentCode IS NOT NULL THEN 
                 lStrSQL := lStrSQL || ' AND JN.AGENT_CODE = '''|| pAgentCode ||'''';
            END IF; 
            
            
            lStrSQL := lStrSQL ||' ) WHERE  WHT_AMT <> 0 ';
            
            lStrSQL := lStrSQL || 'UNION ALL  '; 
            
            lStrSQL := lStrSQL || 'SELECT TRAN_DATE, DOWNLOAD_DATE,DOCUMENT_TYPE,DOCUMENT_NO, ACCOUNT_CODE, AGENT_NAME, EVENT_CODE, WHT_AMT, MATCH_DOC, MATCH_DUE_AMOUNT, DOC_AMT, BRANCH, AGENT_CAT_TYPE, AGENT_MAP FROM ('
            --|| 'SELECT DISTINCT JN.TRAN_DATE, NULL AS DOWNLOAD_DATE, ''Journal'' AS DOCUMENT_TYPE, JN.AC_NO AS DOCUMENT_NO, JN.AGENT_CODE AS ACCOUNT_CODE,  ' --27.00
            || 'SELECT  JN.TRAN_DATE, NULL AS DOWNLOAD_DATE, ''Journal'' AS DOCUMENT_TYPE, JN.AC_NO AS DOCUMENT_NO, JN.AGENT_CODE AS ACCOUNT_CODE,  ' --27.00
            || 'BR.NAME AS AGENT_NAME, '
            || 'GL.EVENT_CODE, NVL(GL.AC_GLAMT,0) AS WHT_AMT, ''n/a'' AS MATCH_DOC, 0   AS MATCH_DUE_AMOUNT,BR.BRANCH_CODE  AS BRANCH,  '
            --|| ' (0 - NVL(GL.AC_GLAMT,0) ) AS DOC_AMT, ''-'' AS AGENT_CAT_TYPE  '  -- 25.00
            || ' (0 - NVL(GL.AC_GLAMT,0) ) AS DOC_AMT, ''-'' AS AGENT_CAT_TYPE, JN.AGENT_CODE AS AGENT_MAP ' -- 25.00
            || 'FROM ACJN_JOUR JN , ACJN_GL GL '
            || ' , (SELECT CP.NAME_EXT AS NAME, DMT.REFERENCE_CODE AS AGENTCODE, DMAG.BRANCH_CODE, DMAG.SERVICED_BY,DMAG.CHANNEL, DMAG.SUBCHANNEL  '
            || ' FROM DMT_AGENTS DMT, '
            || ' DMAG_AGENTS DMAG, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE DMT.INT_ID = DMAG.INT_ID '
            || ' AND DMAG.PART_ID = CP.PART_ID '
            || ' AND DMAG.PART_VERSION = CP.VERSION '
            || ' UNION ALL '
            || ' SELECT CP.NAME_EXT  AS NAME, RI.DMT_AGENT_CODE AS AGENTCODE, RI.BRANCH AS BRANCH_CODE, RI.SERVICED_BY, NULL AS CHANNEL, NULL AS SUBCHANNEL '
            || ' FROM DMAG_RI RI, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE RI.PART_ID = CP.PART_ID '
            || ' AND RI.PART_VERSION = CP.VERSION) BR '
            || 'WHERE '
            || ' JN.AGENT_CODE = BR.AGENTCODE '
            || ' AND JN.AC_NO = GL.AC_NO '
            || 'AND GL.AC_CR_AMT >0 '
            || 'AND JN.AC_NO NOT IN (SELECT AC_NO FROM ACJN_KO) ' 
            || 'AND TRUNC(JN.TRAN_DATE) BETWEEN '''
            || pFromDate
            || ''' AND '''
            || pToDate
            || '''' 
            || ' AND GL.EVENT_CODE IN  (''WHTJNL'') ' ;
            
            IF pBranch IS NOT NULL THEN
                lStrSQL := lStrSQL || ' AND BR.BRANCH_CODE = '''|| pBranch ||'''';
            END IF; 
            
             IF pAgentCode IS NOT NULL THEN 
                 lStrSQL := lStrSQL || ' AND JN.AGENT_CODE = '''|| pAgentCode ||'''';
            END IF;
            
            lStrSQL := lStrSQL || ' ) WHERE  WHT_AMT <> 0 ';
            
            --25.00 start add WHTADJ in JN
             lStrSQL := lStrSQL || 'UNION ALL  '; 

            lStrSQL := lStrSQL || 'SELECT TRAN_DATE, DOWNLOAD_DATE,DOCUMENT_TYPE,DOCUMENT_NO, ACCOUNT_CODE, AGENT_NAME, EVENT_CODE, WHT_AMT, MATCH_DOC, MATCH_DUE_AMOUNT, DOC_AMT, BRANCH, AGENT_CAT_TYPE, AGENT_MAP FROM ('
            --|| 'SELECT DISTINCT JN.TRAN_DATE, NULL AS DOWNLOAD_DATE, ''Journal'' AS DOCUMENT_TYPE, JN.AC_NO AS DOCUMENT_NO, JN.AGENT_CODE AS ACCOUNT_CODE,  ' --27.00
            || 'SELECT  JN.TRAN_DATE, NULL AS DOWNLOAD_DATE, ''Journal'' AS DOCUMENT_TYPE, JN.AC_NO AS DOCUMENT_NO, JN.AGENT_CODE AS ACCOUNT_CODE,  ' --27.00
            || 'BR.NAME AS AGENT_NAME, '
            || 'GL.EVENT_CODE, (AC_CR_AMT-AC_DB_AMT) AS WHT_AMT, ''n/a'' AS MATCH_DOC, 0   AS MATCH_DUE_AMOUNT,BR.BRANCH_CODE  AS BRANCH,  '
            || ' (AC_CR_AMT-AC_DB_AMT) AS DOC_AMT, ''-'' AS AGENT_CAT_TYPE, JN.AGENT_CODE AS AGENT_MAP ' -- 25.00
            || 'FROM ACJN_JOUR JN , ACJN_GL GL '
            || ' , (SELECT CP.NAME_EXT AS NAME, DMT.REFERENCE_CODE AS AGENTCODE, DMAG.BRANCH_CODE, DMAG.SERVICED_BY,DMAG.CHANNEL, DMAG.SUBCHANNEL  '
            || ' FROM DMT_AGENTS DMT, '
            || ' DMAG_AGENTS DMAG, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE DMT.INT_ID = DMAG.INT_ID '
            || ' AND DMAG.PART_ID = CP.PART_ID '
            || ' AND DMAG.PART_VERSION = CP.VERSION '
            || ' UNION ALL '
            || ' SELECT CP.NAME_EXT  AS NAME, RI.DMT_AGENT_CODE AS AGENTCODE, RI.BRANCH AS BRANCH_CODE, RI.SERVICED_BY, NULL AS CHANNEL, NULL AS SUBCHANNEL '
            || ' FROM DMAG_RI RI, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE RI.PART_ID = CP.PART_ID '
            || ' AND RI.PART_VERSION = CP.VERSION) BR '
            || 'WHERE '
            || ' JN.AGENT_CODE = BR.AGENTCODE '
            || ' AND JN.AC_NO = GL.AC_NO '
            || ' AND GL.COA = ''95379'' '
            || 'AND JN.AC_NO NOT IN (SELECT AC_NO FROM ACJN_KO) ' 
            || 'AND TRUNC(JN.TRAN_DATE) BETWEEN '''
            || pFromDate
            || ''' AND '''
            || pToDate
            || '''' 
            || ' AND GL.EVENT_CODE IN  (''WHTADJ'') ' ;
            
            IF pBranch IS NOT NULL THEN
                lStrSQL := lStrSQL || ' AND BR.BRANCH_CODE = '''|| pBranch ||'''';
            END IF; 
            
             IF pAgentCode IS NOT NULL THEN 
                 lStrSQL := lStrSQL || ' AND JN.AGENT_CODE = '''|| pAgentCode ||'''';
            END IF;
            
            lStrSQL := lStrSQL || ' ) WHERE  WHT_AMT <> 0 ';
            
            -- 25.00 END add WHTADJ in JN
            
            IF pDocType IS NULL THEN 
                lStrSQL := lStrSQL || 'UNION ALL  ';
            END IF; 
            
        END if; -- END pDocType = 'JN' OR pDocType IS NULL 
        
        IF pDocType = 'CN' OR pDocType IS NULL  THEN 
           -- lStrSQL := lStrSQL || 'SELECT DISTINCT TRAN_DATE, DOWNLOAD_DATE,DOCUMENT_TYPE,DOCUMENT_NO, ACCOUNT_CODE, AGENT_NAME, EVENT_CODE, WHT_AMT, MATCH_DOC, MATCH_DUE_AMOUNT, DOC_AMT,  BRANCH, AGENT_CAT_TYPE, AGENT_MAP FROM (' --27.00
            lStrSQL := lStrSQL || 'SELECT  TRAN_DATE, DOWNLOAD_DATE,DOCUMENT_TYPE,DOCUMENT_NO, ACCOUNT_CODE, AGENT_NAME, EVENT_CODE, WHT_AMT, MATCH_DOC, MATCH_DUE_AMOUNT, DOC_AMT,  BRANCH, AGENT_CAT_TYPE, AGENT_MAP FROM (' --27.00
            || 'SELECT MCN.TRAN_DATE, NULL AS DOWNLOAD_DATE, ''MCN'' AS DOCUMENT_TYPE, MCN.AC_NO AS DOCUMENT_NO, MCN.AGENT_CODE AS ACCOUNT_CODE, '
            || 'BR.NAME AS AGENT_NAME, '
            || 'GL.EVENT_CODE, NVL(GL.AC_GLAMT,0) AS WHT_AMT, ''n/a''  MATCH_DOC, 0   AS MATCH_DUE_AMOUNT, BR.BRANCH_CODE  AS BRANCH,  '
            --|| ' (SELECT (NVL(SUM(CNGL.AC_CR_AMT),0) - NVL(SUM(CNGL.AC_DB_AMT),0)) FROM ACCR_GL CNGL WHERE CNGL.AC_NO = GL.AC_NO AND CNGL.COA = ''82016'') AS DOC_AMT, ''-'' AS AGENT_CAT_TYPE '  --23.00
            --|| ' (SELECT NVL(SUM(AC_CR_AMT - AC_DB_AMT),0) FROM ACCR_GL B WHERE B.AC_NO = GL.AC_NO AND B.COA IN (''82016'',''82015'')) AS DOC_AMT, ''-'' AS AGENT_CAT_TYPE ' --23.00 amendment on Doc_amt  -- 25.00
            || ' (SELECT NVL(SUM(AC_CR_AMT - AC_DB_AMT),0) FROM ACCR_GL B WHERE B.AC_NO = GL.AC_NO AND B.COA IN (''82016'',''82015'')) AS DOC_AMT, ''-'' AS AGENT_CAT_TYPE, MCN.AGENT_CODE AS AGENT_MAP ' --23.00 amendment on Doc_amt  -- 25.00
            || 'FROM ACCR_NCR MCN, ACCR_GL GL   '
            || ' , (SELECT CP.NAME_EXT AS NAME, DMT.REFERENCE_CODE AS AGENTCODE, DMAG.BRANCH_CODE, DMAG.SERVICED_BY,DMAG.CHANNEL, DMAG.SUBCHANNEL  '
            || ' FROM DMT_AGENTS DMT, '
            || ' DMAG_AGENTS DMAG, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE DMT.INT_ID = DMAG.INT_ID '
            || ' AND DMAG.PART_ID = CP.PART_ID '
            || ' AND DMAG.PART_VERSION = CP.VERSION '
            || ' UNION ALL '
            || ' SELECT CP.NAME_EXT  AS NAME, RI.DMT_AGENT_CODE AS AGENTCODE, RI.BRANCH AS BRANCH_CODE, RI.SERVICED_BY, NULL AS CHANNEL, NULL AS SUBCHANNEL '
            || ' FROM DMAG_RI RI, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE RI.PART_ID = CP.PART_ID '
            || ' AND RI.PART_VERSION = CP.VERSION) BR '
            || 'WHERE '
            || ' MCN.AGENT_CODE = BR.AGENTCODE '
            || ' AND MCN.AC_NO = GL.AC_NO '
            || 'AND GL.AC_CR_AMT >0  '
            || 'AND TRUNC(MCN.TRAN_DATE) BETWEEN '''
            || pFromDate
            || ''' AND '''
            || pToDate
            || ''''
            || ' AND GL.EVENT_CODE IN  (''WHTMCN'') '; 
            
            IF pBranch IS NOT NULL THEN
                lStrSQL := lStrSQL || ' AND BR.BRANCH_CODE = ''' ||pBranch|| '''';
            END IF; 
            
            IF pAgentCode IS NOT NULL THEN 
                 lStrSQL := lStrSQL || ' AND MCN.AGENT_CODE = '''|| pAgentCode ||'''';
            END IF;
            
            lStrSQL := lStrSQL || ') WHERE  WHT_AMT <> 0 ';
            
           
            IF pDocType IS NULL THEN 
               lStrSQL := lStrSQL || 'UNION ALL  ';
            END IF;         
        
        END IF;  -- END pDocType = 'CN' OR pDocType IS NULL
        
        
        IF pDocType = 'CQ' OR pDocType IS NULL  THEN 
            lStrSQL := lStrSQL || 'SELECT  TRAN_DATE, DOWNLOAD_DATE,DOCUMENT_TYPE,DOCUMENT_NO, ACCOUNT_CODE, AGENT_NAME, EVENT_CODE, WHT_AMT, MATCH_DOC, MATCH_DUE_AMOUNT, DOC_AMT, BRANCH, AGENT_CAT_TYPE, AGENT_MAP FROM ('
           -- || 'SELECT PAYMENT.TRAN_DATE, PAYMENT.DOWNLOAD_DATE AS DOWNLOAD_DATE, ''Outsource Payment'' AS DOCUMENT_TYPE, PAYMENT.AC_NO AS DOCUMENT_NO, CASE WHEN PAYMENT.AGENT_CODE IS NOT NULL THEN PAYMENT.AGENT_CODE ELSE PAYMENT.CREDITOR END  AS ACCOUNT_CODE ,  ' --31.00
            || 'SELECT PAYMENT.TRAN_DATE, PAYMENT.DOWNLOAD_DATE AS DOWNLOAD_DATE, ''Outsource Payment'' AS DOCUMENT_TYPE, PAYMENT.AC_NO AS DOCUMENT_NO, CASE WHEN KO.AGENT_CODE IS NOT NULL THEN KO.AGENT_CODE ELSE PAYMENT.CREDITOR END  AS ACCOUNT_CODE ,  ' --added 31.00
           -- || 'CASE WHEN PAYMENT.AGENT_CODE IS NOT  NULL THEN BR.NAME ELSE PAYMENT.NAME END  AS AGENT_NAME,  ' 31.00
            || 'CASE WHEN KO.AGENT_CODE IS NOT  NULL THEN BR.NAME ELSE PAYMENT.NAME END  AS AGENT_NAME,  ' --aded 31.00
            || ' ''WHTCOM'' AS EVENT_CODE, NVL(KO.WHT_AMT,0) AS WHT_AMT, CASE WHEN KO.DOC_NO IS NULL THEN ''n/a'' ELSE KO.DOC_NO END AS MATCH_DOC, NVL(( KO.COMM_DUE ),0)   AS MATCH_DUE_AMOUNT, BR.BRANCH_CODE  AS BRANCH, '
            || ' PAYMENT.AMOUNT AS DOC_AMT, ''-'' AS AGENT_CAT_TYPE, ' 
            || '  CASE WHEN KO.AGENT_CODE IS NOT NULL THEN KO.AGENT_CODE ELSE RTRIM(SUBSTR( PAYMENT.CREDITOR,2,10)) END AS AGENT_MAP ' -- 25.00 , added 31.00
            || 'FROM ACPY_PAYLINK PAYMENT INNER JOIN ACPY_PAYLINK_KO KO ON KO.AC_NO = PAYMENT.AC_NO   '
            || ' LEFT JOIN (SELECT CP.NAME_EXT AS NAME, DMT.REFERENCE_CODE AS AGENTCODE, DMAG.BRANCH_CODE, DMAG.SERVICED_BY,DMAG.CHANNEL, DMAG.SUBCHANNEL  '
            || ' FROM DMT_AGENTS DMT, '
            || ' DMAG_AGENTS DMAG, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE DMT.INT_ID = DMAG.INT_ID '
            || ' AND DMAG.PART_ID = CP.PART_ID '
            || ' AND DMAG.PART_VERSION = CP.VERSION '
            || ' UNION ALL '
            || ' SELECT CP.NAME_EXT  AS NAME, RI.DMT_AGENT_CODE AS AGENTCODE, RI.BRANCH AS BRANCH_CODE, RI.SERVICED_BY, NULL AS CHANNEL, NULL AS SUBCHANNEL '
            || ' FROM DMAG_RI RI, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE RI.PART_ID = CP.PART_ID '
            || ' AND RI.PART_VERSION = CP.VERSION) BR  ON KO.AGENT_CODE = BR.AGENTCODE '  --31.0
            || 'WHERE '
          --  || ' PAYMENT.AC_NO IN (SELECT AC_NO FROM ACPY_PAYLINK_KO) ' -- Commented based on DBA siggestion 24.00
            || ' TRUNC(PAYMENT.DOWNLOAD_DATE) BETWEEN '''
            || pFromDate
            || ''' AND '''
            || pToDate
            || ''''
            || ' AND PAYMENT.WHT_IND = ''Y'''
            || ' AND NOT EXISTS (SELECT 1 FROM ACPY_PYMT_DEL DEL WHERE DEL.AC_NO = PAYMENT.AC_NO) '; --23.00            
            --|| ' AND PAYMENT.AC_NO IN (SELECT GL2.AC_NO FROM ACPY_GL GL2 WHERE GL2.GL_SEQ_NO = KO.GL_SEQ_NO  AND EVENT_CODE = ''WHTCOM'')' ;
            
            IF pBranch IS NOT NULL THEN
                lStrSQL := lStrSQL || ' AND BR.BRANCH_CODE = ''' ||pBranch|| '''';
            END IF;
            
          
            lStrSQL := lStrSQL || ' ) WHERE  WHT_AMT <> 0 ';
                            
            IF pAgentCode IS NOT NULL THEN 
                 lStrSQL := lStrSQL || ' AND ACCOUNT_CODE = '''|| pAgentCode ||'''';
            END IF;
                            
            lStrSQL := lStrSQL || 'UNION ALL ';
                            
            lStrSQL := lStrSQL || 'SELECT TRAN_DATE, DOWNLOAD_DATE,DOCUMENT_TYPE,DOCUMENT_NO, ACCOUNT_CODE, AGENT_NAME, EVENT_CODE, WHT_AMT, MATCH_DOC, MATCH_DUE_AMOUNT, DOC_AMT,  BRANCH, AGENT_CAT_TYPE, AGENT_MAP FROM ('
            || 'SELECT PAYMENT.TRAN_DATE, PAYMENT.DOWNLOAD_DATE AS DOWNLOAD_DATE, ''Outsource Payment'' AS DOCUMENT_TYPE, PAYMENT.AC_NO AS DOCUMENT_NO, CASE WHEN PAYMENT.AGENT_CODE IS NOT NULL THEN PAYMENT.AGENT_CODE ELSE PAYMENT.CREDITOR END  AS ACCOUNT_CODE , '
            || 'CASE WHEN PAYMENT.AGENT_CODE IS NOT  NULL THEN BR.NAME ELSE PAYMENT.NAME END  AS AGENT_NAME,  '
            || 'GL.EVENT_CODE, NVL(GL.AC_GLAMT,0) AS WHT_AMT, ''n/a'' AS MATCH_DOC, 0   AS MATCH_DUE_AMOUNT, BR.BRANCH_CODE  AS BRANCH,  '
            || ' PAYMENT.AMOUNT AS DOC_AMT, ''-'' AS AGENT_CAT_TYPE, '
            || '  CASE WHEN PAYMENT.AGENT_CODE IS NOT NULL THEN PAYMENT.AGENT_CODE ELSE RTRIM(SUBSTR( PAYMENT.CREDITOR,2,10)) END AS AGENT_MAP ' -- 25.00 
            || 'FROM ACPY_PAYLINK PAYMENT  '
            || ' LEFT JOIN (SELECT CP.NAME_EXT AS NAME, DMT.REFERENCE_CODE AS AGENTCODE, DMAG.BRANCH_CODE, DMAG.SERVICED_BY,DMAG.CHANNEL, DMAG.SUBCHANNEL  '
            || ' FROM DMT_AGENTS DMT, '
            || ' DMAG_AGENTS DMAG, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE DMT.INT_ID = DMAG.INT_ID '
            || ' AND DMAG.PART_ID = CP.PART_ID '
            || ' AND DMAG.PART_VERSION = CP.VERSION '
            || ' UNION ALL '
            || ' SELECT CP.NAME_EXT  AS NAME, RI.DMT_AGENT_CODE AS AGENTCODE, RI.BRANCH AS BRANCH_CODE, RI.SERVICED_BY, NULL AS CHANNEL, NULL AS SUBCHANNEL '
            || ' FROM DMAG_RI RI, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE RI.PART_ID = CP.PART_ID '
            || ' AND RI.PART_VERSION = CP.VERSION) BR ON PAYMENT.AGENT_CODE = BR.AGENTCODE, ACPY_PAYLINK_GL GL '
            || 'WHERE PAYMENT.AC_NO = GL.AC_NO '
            || 'AND GL.AC_CR_AMT >0  '
            || 'AND PAYMENT.AC_NO NOT IN (SELECT AC_NO FROM ACPY_PAYLINK_KO) ' 
            || 'AND TRUNC(PAYMENT.DOWNLOAD_DATE) BETWEEN '''
            || pFromDate
            || ''' AND '''
            || pToDate
            || ''''
            || ' AND GL.EVENT_CODE IN (''WHTCOM'') '
            || ' AND NOT EXISTS (SELECT 1 FROM ACPY_PYMT_DEL DEL WHERE DEL.AC_NO = PAYMENT.AC_NO) '; --24.00
            
            IF pBranch IS NOT NULL THEN
                lStrSQL := lStrSQL || ' AND BR.BRANCH_CODE = ''' ||pBranch ||'''';
            END IF; 
           
            
            lStrSQL := lStrSQL || ') WHERE  WHT_AMT <> 0 ';
            
            IF pAgentCode IS NOT NULL THEN 
                 lStrSQL := lStrSQL || ' AND ACCOUNT_CODE = '''|| pAgentCode ||'''';
            END IF;
                    
             --25.00 added WHTADJ type for Out Source pymt start
             lStrSQL := lStrSQL || 'UNION ALL ';

            lStrSQL := lStrSQL || 'SELECT TRAN_DATE, DOWNLOAD_DATE,DOCUMENT_TYPE,DOCUMENT_NO, ACCOUNT_CODE, AGENT_NAME, EVENT_CODE, WHT_AMT, MATCH_DOC, MATCH_DUE_AMOUNT, DOC_AMT,  BRANCH, AGENT_CAT_TYPE, AGENT_MAP FROM ('
            || 'SELECT PAYMENT.TRAN_DATE, PAYMENT.DOWNLOAD_DATE AS DOWNLOAD_DATE, ''Outsource Payment'' AS DOCUMENT_TYPE, PAYMENT.AC_NO AS DOCUMENT_NO, CASE WHEN PAYMENT.AGENT_CODE IS NOT NULL THEN PAYMENT.AGENT_CODE ELSE PAYMENT.CREDITOR END  AS ACCOUNT_CODE , '
            || 'CASE WHEN PAYMENT.AGENT_CODE IS NOT  NULL THEN BR.NAME ELSE PAYMENT.NAME END  AS AGENT_NAME,  '
            || 'GL.EVENT_CODE,  (GL.AC_CR_AMT-GL.AC_DB_AMT) AS WHT_AMT, ''n/a'' AS MATCH_DOC, 0   AS MATCH_DUE_AMOUNT, BR.BRANCH_CODE  AS BRANCH,  '
            || ' (GL.AC_CR_AMT-GL.AC_DB_AMT) AS DOC_AMT, ''-'' AS AGENT_CAT_TYPE, '
            || '  CASE WHEN PAYMENT.AGENT_CODE IS NOT NULL THEN PAYMENT.AGENT_CODE ELSE RTRIM(SUBSTR( PAYMENT.CREDITOR,2,10)) END AS AGENT_MAP ' -- 25.00 
            || 'FROM ACPY_PAYLINK PAYMENT  '
            || ' LEFT JOIN (SELECT CP.NAME_EXT AS NAME, DMT.REFERENCE_CODE AS AGENTCODE, DMAG.BRANCH_CODE, DMAG.SERVICED_BY,DMAG.CHANNEL, DMAG.SUBCHANNEL  '
            || ' FROM DMT_AGENTS DMT, '
            || ' DMAG_AGENTS DMAG, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE DMT.INT_ID = DMAG.INT_ID '
            || ' AND DMAG.PART_ID = CP.PART_ID '
            || ' AND DMAG.PART_VERSION = CP.VERSION '
            || ' UNION ALL '
            || ' SELECT CP.NAME_EXT  AS NAME, RI.DMT_AGENT_CODE AS AGENTCODE, RI.BRANCH AS BRANCH_CODE, RI.SERVICED_BY, NULL AS CHANNEL, NULL AS SUBCHANNEL '
            || ' FROM DMAG_RI RI, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE RI.PART_ID = CP.PART_ID '
            || ' AND RI.PART_VERSION = CP.VERSION) BR ON PAYMENT.AGENT_CODE = BR.AGENTCODE, ACPY_PAYLINK_GL GL '
            || 'WHERE PAYMENT.AC_NO = GL.AC_NO '
            --|| 'AND GL.AC_CR_AMT >0  '
            || ' AND GL.COA = ''95379'' '
            || 'AND PAYMENT.AC_NO NOT IN (SELECT AC_NO FROM ACPY_PAYLINK_KO) ' 
            || 'AND TRUNC(PAYMENT.DOWNLOAD_DATE) BETWEEN '''
            || pFromDate
            || ''' AND '''
            || pToDate
            || ''''
            || ' AND GL.EVENT_CODE IN (''WHTADJ'') '
            || ' AND NOT EXISTS (SELECT 1 FROM ACPY_PYMT_DEL DEL WHERE DEL.AC_NO = PAYMENT.AC_NO) '; --24.00
            
            IF pBranch IS NOT NULL THEN
                lStrSQL := lStrSQL || ' AND BR.BRANCH_CODE = ''' ||pBranch ||'''';
            END IF; 
           
            
            lStrSQL := lStrSQL || ') WHERE  WHT_AMT <> 0 ';
            
            IF pAgentCode IS NOT NULL THEN 
                 lStrSQL := lStrSQL || ' AND ACCOUNT_CODE = '''|| pAgentCode ||'''';
            END IF;
              --25.00 added WHTADJ type for Out Source pymt end 
                    
            lStrSQL := lStrSQL || 'UNION ALL  ';
                            
            lStrSQL := lStrSQL || 'SELECT TRAN_DATE, DOWNLOAD_DATE,DOCUMENT_TYPE,DOCUMENT_NO, ACCOUNT_CODE, AGENT_NAME, EVENT_CODE, WHT_AMT, MATCH_DOC, MATCH_DUE_AMOUNT, DOC_AMT,  BRANCH, AGENT_CAT_TYPE, AGENT_MAP FROM ( '
           -- || 'SELECT  PAYMENT.TRAN_DATE,NULL AS DOWNLOAD_DATE, ''Payment'' AS DOCUMENT_TYPE, PAYMENT.AC_NO AS DOCUMENT_NO, CASE WHEN PAYMENT.AGENT_CODE IS NOT NULL THEN PAYMENT.AGENT_CODE ELSE PAYMENT.CREDITOR END  AS ACCOUNT_CODE ,   '
           -- || 'CASE WHEN PAYMENT.AGENT_CODE IS NOT  NULL THEN  BR.NAME ELSE PAYMENT.NAME END  AS AGENT_NAME,   ' -- 31.00
            || 'SELECT  PAYMENT.TRAN_DATE,NULL AS DOWNLOAD_DATE, ''Payment'' AS DOCUMENT_TYPE, PAYMENT.AC_NO AS DOCUMENT_NO, CASE WHEN KO.AGENT_CODE IS NOT NULL THEN KO.AGENT_CODE ELSE PAYMENT.CREDITOR END  AS ACCOUNT_CODE ,   ' -- added 31.00
            || 'CASE WHEN KO.AGENT_CODE IS NOT  NULL THEN  BR.NAME ELSE PAYMENT.NAME END  AS AGENT_NAME,   ' -- added 31.00
            || '''WHTCOM''  AS EVENT_CODE, NVL(KO.WHT_AMT,0) AS WHT_AMT, CASE WHEN KO.DOC_NO IS NULL THEN ''n/a'' ELSE KO.DOC_NO END AS MATCH_DOC, NVL(( KO.COMM_DUE ),0)   AS MATCH_DUE_AMOUNT, BR.BRANCH_CODE  AS BRANCH, '
            || ' PAYMENT.AMOUNT AS DOC_AMT, ''-'' AS AGENT_CAT_TYPE, '
            || '   CASE WHEN KO.AGENT_CODE IS NOT NULL THEN KO.AGENT_CODE ELSE RTRIM(SUBSTR( PAYMENT.CREDITOR,2,10)) END AS AGENT_MAP ' -- 25.00
            || 'FROM ACPY_PYMT PAYMENT LEFT JOIN ACPY_KO KO ON KO.AC_NO = PAYMENT.AC_NO  '
            || ' LEFT JOIN (SELECT CP.NAME_EXT AS NAME, DMT.REFERENCE_CODE AS AGENTCODE, DMAG.BRANCH_CODE, DMAG.SERVICED_BY,DMAG.CHANNEL, DMAG.SUBCHANNEL  '
            || ' FROM DMT_AGENTS DMT, '
            || ' DMAG_AGENTS DMAG, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE DMT.INT_ID = DMAG.INT_ID '
            || ' AND DMAG.PART_ID = CP.PART_ID '
            || ' AND DMAG.PART_VERSION = CP.VERSION '
            || ' UNION ALL '
            || ' SELECT CP.NAME_EXT  AS NAME, RI.DMT_AGENT_CODE AS AGENTCODE, RI.BRANCH AS BRANCH_CODE, RI.SERVICED_BY, NULL AS CHANNEL, NULL AS SUBCHANNEL '
            || ' FROM DMAG_RI RI, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE RI.PART_ID = CP.PART_ID '
            || ' AND RI.PART_VERSION = CP.VERSION) BR ON KO.AGENT_CODE = BR.AGENTCODE ' --31.0
            || 'WHERE  '
            || '  TRUNC(PAYMENT.TRAN_DATE) BETWEEN '''
            || pFromDate
            || ''' AND '''
            || pToDate
            || ''''
            || ' AND PAYMENT.AC_NO IN (SELECT AC_NO FROM ACPY_KO) '
            || 'AND PAYMENT.AC_NO LIKE ''%*C'' '
            || 'AND PAYMENT.WHT_IND = ''Y'' ';
            
            IF pBranch IS NOT NULL THEN
                lStrSQL := lStrSQL || ' AND BR.BRANCH_CODE = '''|| pBranch|| '''';
            END IF; 
            
            lStrSQL := lStrSQL || ')WHERE  WHT_AMT <> 0 ';
            
            IF pAgentCode IS NOT NULL THEN 
                 lStrSQL := lStrSQL || ' AND ACCOUNT_CODE = '''|| pAgentCode ||'''';
            END IF;
            
            lStrSQL := lStrSQL || 'UNION ALL  ';
                            
            lStrSQL := lStrSQL || 'SELECT TRAN_DATE, DOWNLOAD_DATE,DOCUMENT_TYPE,DOCUMENT_NO, ACCOUNT_CODE, AGENT_NAME, EVENT_CODE, WHT_AMT, MATCH_DOC, MATCH_DUE_AMOUNT, DOC_AMT,  BRANCH, AGENT_CAT_TYPE, AGENT_MAP FROM ( '
            || 'SELECT  PAYMENT.TRAN_DATE,NULL AS DOWNLOAD_DATE, ''Payment'' AS DOCUMENT_TYPE, PAYMENT.AC_NO AS DOCUMENT_NO, CASE WHEN PAYMENT.AGENT_CODE IS NOT NULL THEN PAYMENT.AGENT_CODE ELSE PAYMENT.CREDITOR END  AS ACCOUNT_CODE ,  '
            || 'CASE WHEN PAYMENT.AGENT_CODE IS NOT NULL THEN BR.NAME ELSE PAYMENT.NAME END  AS AGENT_NAME,  '
            || 'GL.EVENT_CODE, NVL(GL.AC_GLAMT,0)*-1 AS WHT_AMT, ''n/a'' AS MATCH_DOC, 0   AS MATCH_DUE_AMOUNT, BR.BRANCH_CODE  AS BRANCH,  '
            || ' PAYMENT.AMOUNT AS DOC_AMT, ''-'' AS AGENT_CAT_TYPE, '
            || '   CASE WHEN PAYMENT.AGENT_CODE IS NOT NULL THEN PAYMENT.AGENT_CODE ELSE RTRIM(SUBSTR( PAYMENT.CREDITOR,2,10)) END AS AGENT_MAP ' -- 25.00
            || 'FROM ACPY_PYMT PAYMENT '
            || ' LEFT JOIN (SELECT CP.NAME_EXT AS NAME, DMT.REFERENCE_CODE AS AGENTCODE, DMAG.BRANCH_CODE, DMAG.SERVICED_BY,DMAG.CHANNEL, DMAG.SUBCHANNEL  '
            || ' FROM DMT_AGENTS DMT, '
            || ' DMAG_AGENTS DMAG, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE DMT.INT_ID = DMAG.INT_ID '
            || ' AND DMAG.PART_ID = CP.PART_ID '
            || ' AND DMAG.PART_VERSION = CP.VERSION '
            || ' UNION ALL '
            || ' SELECT CP.NAME_EXT  AS NAME, RI.DMT_AGENT_CODE AS AGENTCODE, RI.BRANCH AS BRANCH_CODE, RI.SERVICED_BY, NULL AS CHANNEL, NULL AS SUBCHANNEL '
            || ' FROM DMAG_RI RI, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE RI.PART_ID = CP.PART_ID '
            || ' AND RI.PART_VERSION = CP.VERSION) BR ON PAYMENT.AGENT_CODE = BR.AGENTCODE , ACPY_GL GL  ' 
            || ' WHERE PAYMENT.AC_NO = GL.AC_NO  '
            || 'AND GL.AC_CR_AMT >0  ' 
            || 'AND TRUNC(PAYMENT.TRAN_DATE) BETWEEN '''
            || pFromDate
            || ''' AND '''
            || pToDate
            || ''''
            || ' AND PAYMENT.AC_NO NOT IN (SELECT AC_NO FROM ACPY_KO) '
            || 'AND GL.EVENT_CODE IN (''WHTCOM'') ' 
            || 'AND PAYMENT.AC_NO LIKE ''%*C'' '; 
            
            IF pBranch IS NOT NULL THEN
                lStrSQL := lStrSQL || ' AND BR.BRANCH_CODE = '''|| pBranch ||'''';
            END IF; 
           
            lStrSQL := lStrSQL || ') WHERE  WHT_AMT <> 0 ';
            
            IF pAgentCode IS NOT NULL THEN 
                 lStrSQL := lStrSQL || ' AND ACCOUNT_CODE = '''|| pAgentCode ||'''';
            END IF;
            
            IF pDocType IS NULL THEN 
               lStrSQL := lStrSQL || 'UNION ALL  ';
            END IF; 
        
        END IF; --pDocType = 'CQ' OR pDocType IS NULL
        
        
         IF pDocType = 'DN' OR pDocType IS NULL THEN 

            --lStrSQL := lStrSQL || 'SELECT DISTINCT TRAN_DATE, DOWNLOAD_DATE,DOCUMENT_TYPE,DOCUMENT_NO, ACCOUNT_CODE, AGENT_NAME, EVENT_CODE, WHT_AMT, MATCH_DOC, MATCH_DUE_AMOUNT, DOC_AMT,  BRANCH, AGENT_CAT_TYPE, AGENT_MAP FROM ('  --27.00
            lStrSQL := lStrSQL || 'SELECT TRAN_DATE, DOWNLOAD_DATE,DOCUMENT_TYPE,DOCUMENT_NO, ACCOUNT_CODE, AGENT_NAME, EVENT_CODE, WHT_AMT, MATCH_DOC, MATCH_DUE_AMOUNT, DOC_AMT,  BRANCH, AGENT_CAT_TYPE, AGENT_MAP FROM (' --27.00
            || 'SELECT MDN.TRAN_DATE, NULL AS DOWNLOAD_DATE, ''MDN'' AS DOCUMENT_TYPE, MDN.AC_NO AS DOCUMENT_NO, MDN.AGENT_CODE AS ACCOUNT_CODE, '
            || 'BR.NAME AS AGENT_NAME, '
            || 'GL.EVENT_CODE, NVL(GL.AC_GLAMT,0) AS WHT_AMT, ''n/a''  MATCH_DOC, 0   AS MATCH_DUE_AMOUNT, BR.BRANCH_CODE  AS BRANCH,  '
            --|| ' NVL(GL.AC_CR_AMT, GL.AC_DB_AMT) AS  DOC_AMT, ''-'' AS AGENT_CAT_TYPE ' --25.00
            || ' NVL(GL.AC_CR_AMT, GL.AC_DB_AMT) AS  DOC_AMT, ''-'' AS AGENT_CAT_TYPE, MDN.AGENT_CODE AS AGENT_MAP ' --25.00
            || ' FROM ACDB_GL GL JOIN ACDB_NDB MDN ON MDN.AC_NO = GL.AC_NO '
            || ' , (SELECT CP.NAME_EXT AS NAME, DMT.REFERENCE_CODE AS AGENTCODE, DMAG.BRANCH_CODE, DMAG.SERVICED_BY,DMAG.CHANNEL, DMAG.SUBCHANNEL  '
            || ' FROM DMT_AGENTS DMT, '
            || ' DMAG_AGENTS DMAG, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE DMT.INT_ID = DMAG.INT_ID '
            || ' AND DMAG.PART_ID = CP.PART_ID '
            || ' AND DMAG.PART_VERSION = CP.VERSION '
            || ' UNION ALL '
            || ' SELECT CP.NAME_EXT  AS NAME, RI.DMT_AGENT_CODE AS AGENTCODE, RI.BRANCH AS BRANCH_CODE, RI.SERVICED_BY, NULL AS CHANNEL, NULL AS SUBCHANNEL '
            || ' FROM DMAG_RI RI, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE RI.PART_ID = CP.PART_ID '
            || ' AND RI.PART_VERSION = CP.VERSION) BR ' 
            || 'WHERE '
            --|| 'MDN.AC_NO = GL.AC_NO '
            || ' MDN.AGENT_CODE = BR.AGENTCODE '
            || 'AND GL.AC_CR_AMT >0  '
            || 'AND TRUNC(MDN.TRAN_DATE) BETWEEN '''
            || pFromDate
            || ''' AND '''
            || pToDate
            || ''''
            || ' AND GL.EVENT_CODE IN (''WHTMDN'') '; 
            
            
            IF pBranch IS NOT NULL THEN
                lStrSQL := lStrSQL || ' AND BR.BRANCH_CODE = ''' ||pBranch|| '''';
            END IF; 
            
            IF pAgentCode IS NOT NULL THEN 
                 lStrSQL := lStrSQL || ' AND MDN.AGENT_CODE = '''|| pAgentCode ||'''';
            END IF;
            
            lStrSQL := lStrSQL || ') WHERE  WHT_AMT <> 0 ';
            
            --23.00 start
            lStrSQL := lStrSQL || 'UNION ALL  ';
            
            lStrSQL := lStrSQL || 'SELECT  TRAN_DATE, DOWNLOAD_DATE,DOCUMENT_TYPE,DOCUMENT_NO, ACCOUNT_CODE, AGENT_NAME, EVENT_CODE, WHT_AMT, MATCH_DOC, MATCH_DUE_AMOUNT, DOC_AMT,  BRANCH, AGENT_CAT_TYPE, AGENT_MAP FROM ('
            || 'SELECT MDN.TRAN_DATE, NULL AS DOWNLOAD_DATE, ''MDN'' AS DOCUMENT_TYPE, MDN.AC_NO AS DOCUMENT_NO, MDN.AGENT_CODE AS ACCOUNT_CODE, '
            || 'BR.NAME AS AGENT_NAME, '
            || 'GL.EVENT_CODE,  (AC_DB_AMT-AC_CR_AMT) AS WHT_AMT, ''n/a''  MATCH_DOC, 0   AS MATCH_DUE_AMOUNT, BR.BRANCH_CODE  AS BRANCH,  '
            --|| ' (SELECT NVL(SUM(AC_CR_AMT - AC_DB_AMT),0) FROM ACDB_GL B WHERE B.AC_NO = GL.AC_NO AND B.COA IN (''82016'',''82015'')) AS  DOC_AMT, ''-'' AS AGENT_CAT_TYPE '  -- 25.00
            || ' (SELECT NVL(SUM(AC_CR_AMT - AC_DB_AMT),0) FROM ACDB_GL B WHERE B.AC_NO = GL.AC_NO AND B.COA IN (''82016'',''82015'')) AS  DOC_AMT, ''-'' AS AGENT_CAT_TYPE, MDN.AGENT_CODE AS AGENT_MAP  ' -- 25.00
            || ' FROM ACDB_GL GL JOIN ACDB_NDB MDN ON MDN.AC_NO = GL.AC_NO '
            || ' , (SELECT CP.NAME_EXT AS NAME, DMT.REFERENCE_CODE AS AGENTCODE, DMAG.BRANCH_CODE, DMAG.SERVICED_BY,DMAG.CHANNEL, DMAG.SUBCHANNEL  '
            || ' FROM DMT_AGENTS DMT, '
            || ' DMAG_AGENTS DMAG, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE DMT.INT_ID = DMAG.INT_ID '
            || ' AND DMAG.PART_ID = CP.PART_ID '
            || ' AND DMAG.PART_VERSION = CP.VERSION '
            || ' UNION ALL '
            || ' SELECT CP.NAME_EXT  AS NAME, RI.DMT_AGENT_CODE AS AGENTCODE, RI.BRANCH AS BRANCH_CODE, RI.SERVICED_BY, NULL AS CHANNEL, NULL AS SUBCHANNEL '
            || ' FROM DMAG_RI RI, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE RI.PART_ID = CP.PART_ID '
            || ' AND RI.PART_VERSION = CP.VERSION) BR ' 
            || 'WHERE '
            --|| 'MDN.AC_NO = GL.AC_NO '
            || ' MDN.AGENT_CODE = BR.AGENTCODE '
            || 'AND GL.AC_CR_AMT >0  '
            || 'AND TRUNC(MDN.TRAN_DATE) BETWEEN '''
            || pFromDate
            || ''' AND '''
            || pToDate
            || ''''
            || ' AND GL.EVENT_CODE IN (''WHTRVS'') '; 


            IF pBranch IS NOT NULL THEN
                lStrSQL := lStrSQL || ' AND BR.BRANCH_CODE = ''' ||pBranch|| '''';
            END IF; 

            IF pAgentCode IS NOT NULL THEN 
                 lStrSQL := lStrSQL || ' AND MDN.AGENT_CODE = '''|| pAgentCode ||'''';
            END IF;

            lStrSQL := lStrSQL || ') WHERE  WHT_AMT <> 0 ';
            --23.00 end
           
            IF pDocType IS NULL THEN 
               lStrSQL := lStrSQL || 'UNION ALL  ';
            END IF;         

        END IF; -- pDocType = 'DN' OR pDocType IS NULL
        
        IF pDocType = 'AL' OR pDocType IS NULL THEN 

            lStrSQL := lStrSQL || 'SELECT TRAN_DATE, DOWNLOAD_DATE,DOCUMENT_TYPE,DOCUMENT_NO, ACCOUNT_CODE, AGENT_NAME, EVENT_CODE, WHT_AMT, MATCH_DOC, MATCH_DUE_AMOUNT, DOC_AMT, BRANCH, AGENT_CAT_TYPE, AGENT_MAP FROM ('
           -- || 'SELECT DISTINCT RC.TRAN_DATE, NULL AS DOWNLOAD_DATE, ''Receipt'' AS DOCUMENT_TYPE, RC.AC_NO AS DOCUMENT_NO, RC.AGENT_CODE AS ACCOUNT_CODE,  '  --27.00
            || 'SELECT  RC.TRAN_DATE, NULL AS DOWNLOAD_DATE, ''Receipt'' AS DOCUMENT_TYPE, RC.AC_NO AS DOCUMENT_NO, KO.AGENT_CODE AS ACCOUNT_CODE,  ' --27.00 , added 31.00
            || 'BR.NAME AS AGENT_NAME, '
            || '''RARDIR'' AS EVENT_CODE, NVL(KO.WHT_AMT,0) AS WHT_AMT, CASE WHEN KO.DOC_NO IS NULL THEN ''n/a'' ELSE KO.DOC_NO END AS MATCH_DOC, NVL((KO.COMM_DUE ),0)   AS MATCH_DUE_AMOUNT, BR.BRANCH_CODE AS BRANCH,  '
            --|| ' RC.AMOUNT AS DOC_AMT, KO.AGENT_CAT_TYPE '  --25.00
            || ' RC.AMOUNT AS DOC_AMT, KO.AGENT_CAT_TYPE, KO.AGENT_CODE AS AGENT_MAP '  --25.00, added 31.00
            || 'FROM ACRC_RCPT RC LEFT JOIN ACRC_KO KO ON KO.AC_NO = RC.AC_NO  '
            || ' , (SELECT CP.NAME_EXT AS NAME, DMT.REFERENCE_CODE AS AGENTCODE, DMAG.BRANCH_CODE, DMAG.SERVICED_BY,DMAG.CHANNEL, DMAG.SUBCHANNEL  '
            || ' FROM DMT_AGENTS DMT, '
            || ' DMAG_AGENTS DMAG, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE DMT.INT_ID = DMAG.INT_ID '
            || ' AND DMAG.PART_ID = CP.PART_ID '
            || ' AND DMAG.PART_VERSION = CP.VERSION '
            || ' UNION ALL '
            || ' SELECT CP.NAME_EXT  AS NAME, RI.DMT_AGENT_CODE AS AGENTCODE, RI.BRANCH AS BRANCH_CODE, RI.SERVICED_BY, NULL AS CHANNEL, NULL AS SUBCHANNEL '
            || ' FROM DMAG_RI RI, '
            || ' CPGE_VI_PARTNERS_ALL CP '
            || ' WHERE RI.PART_ID = CP.PART_ID '
            || ' AND RI.PART_VERSION = CP.VERSION) BR ' 
            || 'WHERE  '
            || ' KO.AGENT_CODE = BR.AGENTCODE ' --31.0
            || ' AND RC.AC_NO IN (SELECT AC_NO FROM ACRC_KO)  '
            || ' AND KO.COMM_DUE <> 0 '
            || 'AND TRUNC(RC.TRAN_DATE) BETWEEN '''
            || pFromDate
            || ''' AND '''
            || pToDate
            || '''' 
            || ' AND RC.WHT_IND = ''Y''';
        
            
            IF pBranch IS NOT NULL THEN
               lStrSQL := lStrSQL || ' AND BR.BRANCH_CODE = '''|| pBranch ||'''';
            END IF; 
            
            IF pAgentCode IS NOT NULL THEN 
                 lStrSQL := lStrSQL || ' AND RC.AGENT_CODE = '''|| pAgentCode ||'''';
            END IF;
            
            lStrSQL := lStrSQL ||' ) WHERE  WHT_AMT <> 0 ';
            
      
        END IF; -- pDocType = 'RC' OR pDocType IS NULL
        
        lStrSQL := lStrSQL || ') ';

        
       lStrSQL :=
            lStrSQL || ' ORDER BY DOCUMENT_TYPE, DOCUMENT_NO ASC ';


      DBMS_OUTPUT.put_line ('lStrSQL Query - ' || lStrSQL);

      OPEN v_cursor FOR lStrSQL;

      LOOP
         FETCH v_cursor
            INTO r_row.TRAN_DATE,
                r_row.DOWNLOAD_DATE,
                r_row.DOCUMENT_TYPE,
                r_row.DOCUMENT_NO,
                r_row.ACCOUNT_CODE,
                r_row.AGENT_NAME,
                r_row.EVENT_CODE,
                r_row.WHT_AMT,
                r_row.MATCH_DOC,
                r_row.MATCH_DUE_AMOUNT,
                r_row.DOC_AMT,
                r_row.BRANCH,
                r_row.AGENT_CAT_TYPE,
                r_row.AGENT_MAP;  --25.00
                
         EXIT WHEN v_cursor%NOTFOUND;
         PIPE ROW (r_row);
      END LOOP;

      CLOSE v_cursor;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error (
            v_ProcName_v || '.' || v_ProcName_v,
            1,
            SQLERRM);
   END FN_WHO_CON_ACC_DOC_LISTING;
--18.00 end
-- 19.00 end 
-- 26 start
FUNCTION FN_DAILY_CONS_LST_DTL (pFromDate      IN DATE,
                                       pToDate       IN DATE)
      RETURN DAILY_CONS_LST_DTL_REC_TAB
      PIPELINED
   IS
      V_PROCNAME_V   VARCHAR2 (30) := 'FN_DAILY_CONS_LST_DTL';
       r_row PG_RPGE_LISTING.DAILY_CONS_LST_DTL_REC;

      TYPE v_cursor_type IS REF CURSOR;

      v_cursor       v_cursor_type;
      lStrSQL        VARCHAR2 (32757);

      vFromDt VARCHAR2(20); 
      vToDt VARCHAR2(20); 






   BEGIN
  

DBMS_OUTPUT.put_line  ('pFromDate BEFORE: - ' || pFromDate);

DBMS_OUTPUT.put_line ('pToDate BEFORE: - ' || pToDate);

DBMS_OUTPUT.put_line ('pFromDate AFTER: - ' || TO_CHAR(pFromDate,'DD-MM-YYYY'));

DBMS_OUTPUT.put_line ('pToDate AFTER: - ' || TO_CHAR(pToDate,'DD-MM-YYYY'));

vFromDt:= TO_CHAR(pFromDate,'DD-MM-YYYY');
vToDt:=TO_CHAR(pToDate,'DD-MM-YYYY');

DBMS_OUTPUT.put_line  ('vFromDt : - ' || vFromDt);

DBMS_OUTPUT.put_line ('vToDt : - ' || vToDt);

                
            lStrSQL := 'SELECT * FROM ( '
            || ' select  RC.TRAN_DATE AS transactionDate, ''Receipt'' as documentType, RC.BATCH_NO AS batchNo, RC.AC_NO As documentNo, RC.AMOUNT as docAmount , ACM.MATCH_DOC_NO as matchedDoc, ACM.MATCH_DOC_AMT  as matchedDocAmount'
            || ' from ACRC_RCPT RC, ACST_MATCH ACM, ACRC_KO KO  '
            || ' where  '
            || ' RC.AC_NO = ACM.Doc_no AND RC.AC_NO = KO.AC_NO  '
            || ' AND ACM.MATCH_DOC_NO = KO.DOC_NO '
            || ' AND KO.DOC_GL_SEQ_NO  = ACM.MATCH_GL_SEQ_NO '
            || ' AND TRUNC(RC.TRAN_DATE) BETWEEN  TO_DATE(''' || vFromDt ||''',''DD-MM-YYYY'') AND TO_DATE(''' || vToDt || ''',''DD-MM-YYYY'')' 
            || ' AND KO.UNIVERSAL_TYPE = ''UNIF5'' '
            || ' UNION ALL  '
            || ' select JR.TRAN_DATE AS transactionDate, ''Journal'' as documentType, JR.BATCH_NO  AS batchNo, JR.AC_NO As documentNo, JR.AMOUNT as docAmount  , KO.DOC_NO as matchedDoc,' 
            || ' (SELECT ACM.MATCH_DOC_AMT FROM ACST_MATCH ACM WHERE ACM.MATCH_DOC_NO = KO.DOC_NO AND ACM.DOC_NO = JR.AC_NO AND ACM.GL_SEQ_NO = KO.GL_SEQ_NO) as matchedDocAmount '
            || ' from ACJN_JOUR JR, ACJN_KO KO '
            || ' where '
            || ' JR.AC_NO = KO.AC_NO' 
            || ' AND TRUNC(JR.TRAN_DATE) BETWEEN  TO_DATE(''' || vFromDt ||''',''DD-MM-YYYY'') AND TO_DATE(''' || vToDt || ''',''DD-MM-YYYY'')' 
            || ' AND KO.UNIVERSAL_TYPE = ''UNIF5''' 
           -- || ' select JR.TRAN_DATE AS transactionDate, ''Journal'' as documentType, JR.BATCH_NO  AS batchNo, JR.AC_NO As documentNo, JR.AMOUNT as docAmount  , ACM.MATCH_DOC_NO as matchedDoc, ACM.MATCH_DOC_AMT  as matchedDocAmount '
           -- || ' from ACJN_JOUR JR, ACST_MATCH ACM, ACJN_KO KO  '
            --|| ' where  '
            --|| ' JR.AC_NO = ACM.Doc_no AND JR.AC_NO = KO.AC_NO  '
            --|| ' AND ACM.MATCH_DOC_NO = KO.DOC_NO '
            --|| ' AND TRUNC(JR.TRAN_DATE) BETWEEN ''' || pFromDate ||''' AND ''' || pToDate || '''' 
           -- || ' AND KO.UNIVERSAL_TYPE = ''UNIF5''   '
            || ' UNION ALL  '
            || ' select OP.TRAN_DATE AS transactionDate, ''Outsource Payment'' as documentType, OP.BATCH_NO  AS batchNo, OP.AC_NO As documentNo, OP.AMOUNT  as docAmount , ACM.MATCH_DOC_NO as matchedDoc, ACM.MATCH_DOC_AMT  as matchedDocAmount '
            || ' from ACPY_PAYLINK OP, ACST_MATCH ACM, ACPY_KO KO  '
            || ' where  '
            || ' OP.AC_NO = ACM.Doc_no AND OP.AC_NO = KO.AC_NO  '
            || ' AND ACM.MATCH_DOC_NO = KO.DOC_NO '
            || ' AND KO.DOC_GL_SEQ_NO  = ACM.MATCH_GL_SEQ_NO '
            || ' AND TRUNC(OP.TRAN_DATE) BETWEEN TO_DATE(''' || vFromDt ||''',''DD-MM-YYYY'') AND TO_DATE(''' || vToDt || ''',''DD-MM-YYYY'')' 
            || ' AND KO.UNIVERSAL_TYPE = ''UNIF5''  '
            || ' UNION ALL  '
            || ' select CN.TRAN_DATE AS transactionDate, ''Credit Note'' as documentType, CN.BATCH_NO  AS batchNo, CN.AC_NO As documentNo, CN.AMOUNT as docAmount  , ACM.MATCH_DOC_NO as matchedDoc, ACM.MATCH_DOC_AMT  as matchedDocAmount '
            || ' from ACCR_NCR CN, ACST_MATCH ACM, ACCR_KO KO  '
            || ' where  '
            || ' CN.AC_NO = ACM.Doc_no AND CN.AC_NO = KO.AC_NO  '
            || ' AND ACM.MATCH_DOC_NO = KO.DOC_NO '
            || ' AND KO.DOC_GL_SEQ_NO  = ACM.MATCH_GL_SEQ_NO '
            || ' AND TRUNC(CN.TRAN_DATE) BETWEEN TO_DATE(''' || vFromDt ||''',''DD-MM-YYYY'') AND TO_DATE(''' || vToDt || ''',''DD-MM-YYYY'')' 
            || ' AND KO.UNIVERSAL_TYPE = ''UNIF5''  '
            || ' UNION ALL  '
            || ' select DN.TRAN_DATE AS transactionDate, ''Debit Note'' as documentType, DN.BATCH_NO  AS batchNo, DN.AC_NO As documentNo, DN.AMOUNT as docAmount , ACM.MATCH_DOC_NO as matchedDoc, ACM.MATCH_DOC_AMT  as matchedDocAmount '
            || ' from ACDB_NDB DN, ACST_MATCH ACM, ACDB_KO KO  '
            || ' where  '
            || ' DN.AC_NO = ACM.Doc_no AND DN.AC_NO = KO.AC_NO  '
            || ' AND ACM.MATCH_DOC_NO = KO.DOC_NO '
            || ' AND KO.DOC_GL_SEQ_NO  = ACM.MATCH_GL_SEQ_NO '
            || ' AND TRUNC(DN.TRAN_DATE) BETWEEN TO_DATE(''' || vFromDt ||''',''DD-MM-YYYY'') AND TO_DATE(''' || vToDt || ''',''DD-MM-YYYY'')' 
            || ' AND KO.UNIVERSAL_TYPE = ''UNIF5'') '; 


      DBMS_OUTPUT.put_line ('lStrSQL Query - ' || lStrSQL);

      OPEN v_cursor FOR lStrSQL;

      LOOP
         FETCH v_cursor
            INTO r_row.transactionDate,
                r_row.documentType,
                r_row.batchNo,
                r_row.documentNo,
                r_row.docAmount,
                r_row.matchedDoc,
                r_row.matchedDocAmount;
             
        
         EXIT WHEN v_cursor%NOTFOUND;
         PIPE ROW (r_row);
      END LOOP;

      CLOSE v_cursor;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error (
            v_ProcName_v || '.' || v_ProcName_v,
            1,
            SQLERRM);
   END FN_DAILY_CONS_LST_DTL;
   
   
   
   FUNCTION FN_DAILY_CONS_LST_SUM (pFromDate      IN DATE,
                                       pToDate       IN DATE)
      RETURN DAILY_CONS_LST_SUM_REC_TAB
      PIPELINED
   IS
      V_PROCNAME_V   VARCHAR2 (30) := 'FN_DAILY_CONS_LST_SUM';
       r_row PG_RPGE_LISTING.DAILY_CONS_LST_SUM_REC;

      TYPE v_cursor_type IS REF CURSOR;

      v_cursor       v_cursor_type;
      lStrSQL        VARCHAR2 (32757);
       vFromDt VARCHAR2(20); 
      vToDt VARCHAR2(20); 


   BEGIN
  

    vFromDt:= TO_CHAR(pFromDate,'DD-MM-YYYY');
    vToDt:=TO_CHAR(pToDate,'DD-MM-YYYY');

            lStrSQL := 'SELECT * FROM (select ''Receipt'' as doc_type, SUM(ACM.MATCH_DOC_AMT)  as docAmountRM '
            || ' from ACRC_RCPT RC, ACST_MATCH ACM, ACRC_KO KO  '
            || ' where  '
            || ' RC.AC_NO = ACM.Doc_no AND RC.AC_NO = KO.AC_NO AND ACM.MATCH_DOC_NO = KO.DOC_NO ' 
            || ' AND KO.DOC_GL_SEQ_NO  = ACM.MATCH_GL_SEQ_NO '
            || ' AND TRUNC(RC.TRAN_DATE) BETWEEN  TO_DATE(''' || vFromDt ||''',''DD-MM-YYYY'') AND TO_DATE(''' || vToDt || ''',''DD-MM-YYYY'')' 
            || ' AND KO.UNIVERSAL_TYPE = ''UNIF5'''
            || ' UNION ALL  '
            || ' select ''Journal'' as doc_type, SUM((SELECT ACM.MATCH_DOC_AMT FROM ACST_MATCH ACM WHERE ACM.MATCH_DOC_NO = KO.DOC_NO AND ACM.DOC_NO = JR.AC_NO AND ACM.GL_SEQ_NO = KO.GL_SEQ_NO) ) as docAmountRM '
            || ' from ACJN_JOUR JR, ACJN_KO KO '
            || ' where  '
            || ' JR.AC_NO = KO.AC_NO ' 
            || ' AND TRUNC(JR.TRAN_DATE) BETWEEN  TO_DATE(''' || vFromDt ||''',''DD-MM-YYYY'') AND TO_DATE(''' || vToDt || ''',''DD-MM-YYYY'')' 
            || ' AND KO.UNIVERSAL_TYPE = ''UNIF5'' '
            || ' UNION ALL  '
            || ' select ''Outsource Payment'' as doc_type, SUM(ACM.MATCH_DOC_AMT)  as docAmountRM '
            || ' from ACPY_PAYLINK OP, ACST_MATCH ACM, ACPY_KO KO '
            || ' where  '
            || ' OP.AC_NO = ACM.Doc_no AND OP.AC_NO = KO.AC_NO AND ACM.MATCH_DOC_NO = KO.DOC_NO '
            || ' AND KO.DOC_GL_SEQ_NO  = ACM.MATCH_GL_SEQ_NO '
            || ' AND TRUNC(OP.TRAN_DATE) BETWEEN  TO_DATE(''' || vFromDt ||''',''DD-MM-YYYY'') AND TO_DATE(''' || vToDt || ''',''DD-MM-YYYY'')' 
            || ' AND KO.UNIVERSAL_TYPE = ''UNIF5''          '
            || ' UNION ALL  '
            || ' select ''Credit Note'' as doc_type, SUM(ACM.MATCH_DOC_AMT) as docAmountRM '
            || ' from ACCR_NCR CN, ACST_MATCH ACM, ACCR_KO KO  '
            || ' where  '
            || ' CN.AC_NO = ACM.Doc_no AND CN.AC_NO = KO.AC_NO AND ACM.MATCH_DOC_NO = KO.DOC_NO ' 
            || ' AND KO.DOC_GL_SEQ_NO  = ACM.MATCH_GL_SEQ_NO '
            || ' AND TRUNC(CN.TRAN_DATE) BETWEEN  TO_DATE(''' || vFromDt ||''',''DD-MM-YYYY'') AND TO_DATE(''' || vToDt || ''',''DD-MM-YYYY'')' 
            || ' AND KO.UNIVERSAL_TYPE = ''UNIF5''         '
            || ' UNION ALL  '
            || ' select ''Debit Note'' as doc_type, SUM(ACM.MATCH_DOC_AMT) as docAmountRM '
            || ' from ACDB_NDB DN, ACST_MATCH ACM, ACDB_KO KO  '
            || ' where  '
            || ' DN.AC_NO = ACM.Doc_no AND DN.AC_NO = KO.AC_NO AND ACM.MATCH_DOC_NO = KO.DOC_NO '
            || ' AND KO.DOC_GL_SEQ_NO  = ACM.MATCH_GL_SEQ_NO '
            || ' AND TRUNC(DN.TRAN_DATE) BETWEEN  TO_DATE(''' || vFromDt ||''',''DD-MM-YYYY'') AND TO_DATE(''' || vToDt || ''',''DD-MM-YYYY'')' 
            || ' AND KO.UNIVERSAL_TYPE = ''UNIF5'') where docAmountRM IS NOT NULL AND docAmountRM <> 0 ';
                    

      DBMS_OUTPUT.put_line ('lStrSQL Query - ' || lStrSQL);

      OPEN v_cursor FOR lStrSQL;

      LOOP
         FETCH v_cursor
            INTO 
                r_row.documentType,
                r_row.docAmountRM;
             
        
         EXIT WHEN v_cursor%NOTFOUND;
         PIPE ROW (r_row);
      END LOOP;

      CLOSE v_cursor;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error (
            v_ProcName_v || '.' || v_ProcName_v,
            1,
            SQLERRM);
   END FN_DAILY_CONS_LST_SUM;
   

-- 26.00 end
-- 34.00 Start
FUNCTION FN_EINV_STATUS_REPORT_DTL (pFromDate    IN DATE,
                                       pToDate   IN DATE,
                                       pBranch IN VARCHAR2,
                                       eInvStatus IN VARCHAR2, 
                                       pDocType IN VARCHAR2)
      RETURN EINV_STATUS_REPORT_DTL_TAB
      PIPELINED
   IS
      V_PROCNAME_V   VARCHAR2 (30) := 'FN_EINV_STATUS_REPORT_DTL';
       r_row PG_RPGE_LISTING.EINV_STATUS_REPORT_DTL_REC;

      TYPE v_cursor_type IS REF CURSOR;

      v_cursor       v_cursor_type;
      lStrSQL        VARCHAR2 (32757);
      eInvStausVal   VARCHAR2(40);
      vFromDt VARCHAR2(20); 
      vToDt VARCHAR2(20); 

   BEGIN
   DBMS_OUTPUT.put_line ('pDocType - '|| pDocType);

   PG_UTIL_LOG_ERROR.PC_INS_Log_Error (v_ProcName_v , 1,'::' || 'pFromDate=' || pFromDate); --3.29
   PG_UTIL_LOG_ERROR.PC_INS_Log_Error (v_ProcName_v , 1,'::' || 'pToDate=' || pToDate); --3.29
   PG_UTIL_LOG_ERROR.PC_INS_Log_Error (v_ProcName_v , 1,'::' || 'pBranch=' || pBranch); --3.29
   PG_UTIL_LOG_ERROR.PC_INS_Log_Error (v_ProcName_v , 1,'::' || 'eInvStatus=' || eInvStatus); --3.29
   PG_UTIL_LOG_ERROR.PC_INS_Log_Error (v_ProcName_v , 1,'::' || 'pDocType=' || pDocType); --3.29

    vFromDt:= TO_CHAR(pFromDate,'DD-MM-YYYY');
    vToDt:=TO_CHAR(pToDate,'DD-MM-YYYY');

DBMS_OUTPUT.put_line ('vFromDt - '|| vFromDt);

DBMS_OUTPUT.put_line ('vFromDt - '|| vFromDt);

   PG_UTIL_LOG_ERROR.PC_INS_Log_Error (v_ProcName_v , 1,'::' || 'AFTER CONVERT vFromDt=' || vFromDt); --3.29
   PG_UTIL_LOG_ERROR.PC_INS_Log_Error (v_ProcName_v , 1,'::' || 'AFTER CONVERT  vToDt=' || vToDt); --3.29

    lStrSQL :=' SELECT  TRAN_DATE,  DOWNLOAD_DATE,BRANCH_NAME,BRANCH_CODE,DOCUMENT_TYPE,DOCUMENT_NO,STAX_NO,AGENT_NAME,EVENT_CODE,AMOUNT,OPERATOR,EINV_STATUS, VALIDTN_REASON FROM ( ' ;

IF pDocType = 'DN' OR pDocType IS NULL  THEN
    --DBMS_OUTPUT.put_line ('DEBIT NOTE IN : ');
    lStrSQL := lStrSQL ||' SELECT TRAN_DATE,DOWNLOAD_DATE,BRANCH_NAME,BRANCH_CODE,DOCUMENT_TYPE,DOCUMENT_NO,STAX_NO,AGENT_NAME,EVENT_CODE, ' 
                ||' (CASE WHEN COUNT =0 THEN AMOUNT ELSE AMOUNT*-1 END) AS AMOUNT,OPERATOR,EINV_STATUS, '
                ||' VALIDTN_REASON,CUTOFF_DATE, EINV_IND FROM ( '
                ||' SELECT DISTINCT TO_CHAR(A.TRAN_DATE,''dd/MM/yyyy'')  AS TRAN_DATE, '''' AS DOWNLOAD_DATE,B.BRANCH_NAME AS BRANCH_NAME,A.ISSUE_OFFICE AS BRANCH_CODE, ''Debit Note'' as DOCUMENT_TYPE, '
                ||' A.AC_NO AS DOCUMENT_NO,A.EINV_NO AS STAX_NO,CP.NAME_EXT AS AGENT_NAME,GL.EVENT_CODE AS EVENT_CODE,NVL(GL.AC_GLAMT,0) AS AMOUNT, '                 
                ||' A.OPERATOR AS OPERATOR,A.EINV_STATUS AS EINV_STATUS,A.EINV_REASON AS VALIDTN_REASON, '
                ||'  (SELECT CODE FROM SAPM_NEWUTIL WHERE UKEY=''OPUSI_EINVOICE_CUTOFF'') AS CUTOFF_DATE, '
                --||'  (SELECT  EV.EINV_IND FROM CMGE_SAP_EVENTS_ACC EV WHERE EV.event_code = GL.EVENT_CODE and rownum=1)  as EINV_IND '
                ||' A.EINV_IND ,'
                ||' (SELECT count(*)    '        
                ||'  FROM(SELECT EINV_REFNO,EINV_NO,TRAN_DATE FROM ACCR_NCR '
                ||'  WHERE AC_NO = A.LINK_AC_NO'
                ||'  UNION ALL'
                ||'  SELECT EINV_REFNO,EINV_NO,TRAN_DATE  FROM ACDB_NDB' 
                ||'  WHERE AC_NO = A.LINK_AC_NO'
                ||'  UNION ALL'
                ||' SELECT EINV_REFNO,EINV_NO,TRAN_DATE FROM ACPY_PYMT '
                ||' WHERE AC_NO = A.LINK_AC_NO'
                ||' UNION ALL'
                ||'  SELECT EINV_REFNO,EINV_NO,TRAN_DATE  FROM ACJN_JOUR '
                ||'  WHERE AC_NO = A.LINK_AC_NO'
                ||' UNION ALL'
                ||' SELECT EINV_REFNO,EINV_NO,TRAN_DATE   FROM ACRC_RCPT '
                ||' WHERE AC_NO = A.LINK_AC_NO) ) AS COUNT'
                ||' FROM ACDB_NDB A  '
                ||' LEFT JOIN TABLE ( PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (A.PART_ID, A.PART_VERSION)) CP ON A.PART_ID = CP.PART_ID AND A.PART_VERSION = CP.VERSION '
                --||' LEFT JOIN ACDB_GL GL ON GL.AC_NO =A.AC_NO '
                --||' LEFT JOIN CMGE_SAP_EVENTS_ACC EV ON GL.EVENT_CODE = EV.EVENT_CODE AND EV.EINV_IND = ''Y'' AND EV.MODULE=''DN'' '
                ||' LEFT JOIN (SELECT A.AC_NO, A.EVENT_CODE, A.AC_GLAMT FROM ACDB_GL A '
                ||' WHERE A.EVENT_CODE IN (SELECT EVENT_CODE FROM CMGE_SAP_EVENTS_ACC C '
               --||' WHERE NOT EXISTS (SELECT 1 FROM CMGE_CODE D ' --38.00
               --||' WHERE D.CAT_CODE =''EINV_TAX_EVENT'' AND D.CODE_CD = A.EVENT_CODE) and C.EINV_IND = ''Y''))GL ON GL.AC_NO =A.AC_NO ' --38.00
                ||' WHERE C.EINV_IND = ''Y'' AND C.MODULE = ''DN'' ))GL ON GL.AC_NO =A.AC_NO ' --38.00
                ||' LEFT JOIN CMDM_BRANCH B ON A.ISSUE_OFFICE = B.BRANCH_CODE '
                || ' WHERE TRUNC(A.TRAN_DATE) BETWEEN  TO_DATE(''' || vFromDt ||''',''DD-MM-YYYY'') AND TO_DATE(''' || vToDt || ''',''DD-MM-YYYY'') ' ;
                --||' AND GL.EVENT_CODE IN (SELECT EVENT_CODE FROM CMGE_SAP_EVENTS_ACC WHERE EINV_IND = ''Y'') '
             IF pBranch IS NOT NULL THEN /* 36.00 Start*/
      lStrSQL := lStrSQL ||' AND A.ISSUE_OFFICE = '''|| pBranch ||''' ';
             END IF; /* 36.00 End*/
             IF eInvStatus = 'ALL'  THEN  
     lStrSQL := lStrSQL ||'AND UPPER(A.EINV_STATUS) IN (''PENDING'',''SUBMITTED'',''INVALID'',''REJECTED'',''FAIL'')'; /* 36.00*/
             ELSE
     lStrSQL := lStrSQL ||' AND UPPER(A.EINV_STATUS) = '''|| eInvStatus ||''' ';
            END IF;
       --lStrSQL := lStrSQL ||') WHERE  EINV_IND = ''Y'' '; 
       lStrSQL := lStrSQL ||') '; 
--       IF pDocType IS NOT NULL  THEN
--       lStrSQL := lStrSQL ||' ORDER BY TRAN_DATE,DOCUMENT_NO,DOCUMENT_TYPE DESC ';
--       END IF;
        --DBMS_OUTPUT.put_line ('DN-DEBIT NOTE IN - '|| lStrSQL);
        IF pDocType IS NULL THEN 
            lStrSQL := lStrSQL || 'UNION ALL  ';
        END IF;
END IF; -- DEBIT NOTE IF END 

IF pDocType = 'CN' OR pDocType IS NULL  THEN   -- CREDIT NOTE IF START
      --DBMS_OUTPUT.put_line ('CREDIT NOTE IN : ');     
      lStrSQL := lStrSQL ||' SELECT  TRAN_DATE,DOWNLOAD_DATE,BRANCH_NAME,BRANCH_CODE,DOCUMENT_TYPE,DOCUMENT_NO,STAX_NO,AGENT_NAME,EVENT_CODE,'
                ||' (CASE WHEN COUNT =0 THEN AMOUNT ELSE AMOUNT*-1 END) AS AMOUNT,OPERATOR,EINV_STATUS, '
                ||' VALIDTN_REASON,CUTOFF_DATE, EINV_IND FROM ( '
                ||' SELECT DISTINCT TO_CHAR(A.TRAN_DATE,''dd/MM/yyyy'')  AS TRAN_DATE, '''' AS DOWNLOAD_DATE,B.BRANCH_NAME AS BRANCH_NAME,A.ISSUE_OFFICE AS BRANCH_CODE, ''Credit Note'' as DOCUMENT_TYPE, '
                ||' A.AC_NO AS DOCUMENT_NO,A.EINV_NO AS STAX_NO,CP.NAME_EXT AS AGENT_NAME,GL.EVENT_CODE AS EVENT_CODE,NVL(GL.AC_GLAMT,0) AS AMOUNT, '
                ||' A.OPERATOR AS OPERATOR,A.EINV_STATUS AS EINV_STATUS,A.EINV_REASON AS VALIDTN_REASON , '
                ||' (SELECT CODE FROM SAPM_NEWUTIL WHERE UKEY=''OPUSI_EINVOICE_CUTOFF'') AS CUTOFF_DATE, '
                ||' A.EINV_IND ,'
                ||' (SELECT count(*)    '        
                ||'  FROM(SELECT EINV_REFNO,EINV_NO,TRAN_DATE FROM ACCR_NCR '
                ||'  WHERE AC_NO = A.LINK_AC_NO'
                ||'  UNION ALL'
                ||'  SELECT EINV_REFNO,EINV_NO,TRAN_DATE  FROM ACDB_NDB' 
                ||'  WHERE AC_NO = A.LINK_AC_NO'
                ||'  UNION ALL'
                ||' SELECT EINV_REFNO,EINV_NO,TRAN_DATE FROM ACPY_PYMT '
                ||' WHERE AC_NO = A.LINK_AC_NO'
                ||' UNION ALL'
                ||'  SELECT EINV_REFNO,EINV_NO,TRAN_DATE  FROM ACJN_JOUR '
                ||'  WHERE AC_NO = A.LINK_AC_NO'
                ||' UNION ALL'
                ||' SELECT EINV_REFNO,EINV_NO,TRAN_DATE   FROM ACRC_RCPT '
                ||' WHERE AC_NO = A.LINK_AC_NO) ) AS COUNT'
                --||'  (SELECT  EV.EINV_IND FROM CMGE_SAP_EVENTS_ACC EV WHERE EV.event_code = GL.EVENT_CODE and rownum=1)  as EINV_IND '
                ||' FROM ACCR_NCR A  '
                ||' LEFT JOIN TABLE ( PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (A.PART_ID, A.PART_VERSION)) CP ON A.PART_ID = CP.PART_ID AND A.PART_VERSION = CP.VERSION '
                --||' LEFT JOIN ACCR_GL GL ON GL.AC_NO =A.AC_NO '
                --||' LEFT JOIN CMGE_SAP_EVENTS_ACC EV ON GL.EVENT_CODE = EV.EVENT_CODE AND EV.EINV_IND = ''Y'' AND EV.MODULE=''CN'' '
                ||' LEFT JOIN (SELECT A.AC_NO, A.EVENT_CODE, A.AC_GLAMT FROM ACCR_GL A '
                ||' WHERE A.EVENT_CODE IN (SELECT EVENT_CODE  FROM CMGE_SAP_EVENTS_ACC C '
                --||' WHERE NOT EXISTS (SELECT 1 FROM CMGE_CODE D '--38.00
                --||' WHERE D.CAT_CODE =''EINV_TAX_EVENT'' AND D.CODE_CD = A.EVENT_CODE) and C.EINV_IND = ''Y''))GL ON GL.AC_NO =A.AC_NO '--38.00
                ||' WHERE C.EINV_IND = ''Y'' AND C.MODULE = ''CN'' ))GL ON GL.AC_NO =A.AC_NO '--38.00
                ||' LEFT JOIN ACPY_PAYLINK PAY ON A.AC_NO = PAY.AC_NO '
                ||' LEFT JOIN CMDM_BRANCH B ON A.ISSUE_OFFICE = B.BRANCH_CODE '
                  || ' WHERE TRUNC(A.TRAN_DATE) BETWEEN  TO_DATE(''' || vFromDt ||''',''DD-MM-YYYY'') AND TO_DATE(''' || vToDt || ''',''DD-MM-YYYY'') ' ;
             IF pBranch IS NOT NULL THEN /* 36.00 Start*/
      lStrSQL := lStrSQL ||' AND A.ISSUE_OFFICE = '''|| pBranch ||''' ';
             END IF; /* 36.00 End*/
             IF eInvStatus = 'ALL'  THEN  
     lStrSQL := lStrSQL ||'AND UPPER(A.EINV_STATUS) IN (''PENDING'',''SUBMITTED'',''INVALID'',''REJECTED'',''FAIL'')'; /* 36.00*/
             ELSE
     lStrSQL := lStrSQL ||' AND UPPER(A.EINV_STATUS) = '''|| eInvStatus ||''' ';
            END IF;
      -- lStrSQL := lStrSQL ||') WHERE  EINV_IND = ''Y'' ';  
       lStrSQL := lStrSQL ||')  ';
--       IF pDocType IS NOT NULL  THEN
--        lStrSQL := lStrSQL ||' ORDER BY TRAN_DATE,DOCUMENT_NO,DOCUMENT_TYPE DESC ';
--       END IF;
       --DBMS_OUTPUT.put_line ('CN-CREDIT NOTE IN - '|| lStrSQL);
       IF pDocType IS NULL THEN 
           lStrSQL := lStrSQL || 'UNION ALL  ';
        END IF;
END IF; -- CEDIT NOTE IF END 

IF pDocType = 'CQ' OR pDocType IS NULL  THEN   -- PAYMENT NOTE IF START
      --DBMS_OUTPUT.put_line ('PAYEMNT IN : ');     
      lStrSQL := lStrSQL ||' SELECT TRAN_DATE,DOWNLOAD_DATE,BRANCH_NAME,BRANCH_CODE,DOCUMENT_TYPE,DOCUMENT_NO,STAX_NO,AGENT_NAME,EVENT_CODE,AMOUNT,OPERATOR,EINV_STATUS, '
                ||' VALIDTN_REASON,CUTOFF_DATE, EINV_IND FROM ( '
                ||' SELECT DISTINCT TO_CHAR(A.TRAN_DATE,''dd/MM/yyyy'')  AS TRAN_DATE, TO_CHAR(PAY.DOWNLOAD_DATE,''dd/MM/yyyy'') AS DOWNLOAD_DATE,B.BRANCH_NAME AS BRANCH_NAME,A.ISSUE_OFFICE AS BRANCH_CODE, ''Payment'' as DOCUMENT_TYPE, '
                ||' A.AC_NO AS DOCUMENT_NO,A.EINV_NO AS STAX_NO,CP.NAME_EXT AS AGENT_NAME,GL.EVENT_CODE AS EVENT_CODE,'
                ||' CASE WHEN A.REV_REASON IS NULL THEN NVL(GL.AC_GLAMT,0) ELSE  NVL(GL.AC_GLAMT,0) *-1 END AS AMOUNT, '
                ||' A.OPERATOR AS OPERATOR,A.EINV_STATUS AS EINV_STATUS,A.EINV_REASON AS VALIDTN_REASON , '
                ||'  (SELECT CODE FROM SAPM_NEWUTIL WHERE UKEY=''OPUSI_EINVOICE_CUTOFF'') AS CUTOFF_DATE,A.EINV_IND ,1 '
                --||'  (SELECT  EV.EINV_IND FROM CMGE_SAP_EVENTS_ACC EV WHERE EV.event_code = GL.EVENT_CODE and rownum=1)  as EINV_IND '
                ||' FROM ACPY_PYMT A  '
                ||' LEFT JOIN TABLE ( PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (A.PART_ID, A.PART_VERSION)) CP ON A.PART_ID = CP.PART_ID AND A.PART_VERSION = CP.VERSION '
                --||' LEFT JOIN ACPY_GL GL ON GL.AC_NO =A.AC_NO '
                --||' LEFT JOIN CMGE_SAP_EVENTS_ACC EV ON GL.EVENT_CODE = EV.EVENT_CODE AND EV.EINV_IND = ''Y'' AND EV.MODULE=''CQ'' '
                ||' LEFT JOIN (SELECT A.AC_NO, A.EVENT_CODE, A.AC_GLAMT FROM ACPY_GL A '
                ||' WHERE A.EVENT_CODE IN (SELECT EVENT_CODE  FROM CMGE_SAP_EVENTS_ACC C '
                --||' WHERE NOT EXISTS (SELECT 1 FROM CMGE_CODE D '  -- 38.00
                --||' WHERE D.CAT_CODE =''EINV_TAX_EVENT'' AND D.CODE_CD = A.EVENT_CODE) and C.EINV_IND = ''Y''))GL ON GL.AC_NO =A.AC_NO ' -- 38.00
                ||' WHERE C.EINV_IND = ''Y'' AND C.MODULE = ''CQ'' ))GL ON GL.AC_NO =A.AC_NO ' -- 38.00
                ||' LEFT JOIN ACPY_PAYLINK PAY ON A.AC_NO = PAY.AC_NO '
                ||' LEFT JOIN CMDM_BRANCH B ON A.ISSUE_OFFICE = B.BRANCH_CODE '
                  || ' WHERE TRUNC(A.TRAN_DATE) BETWEEN  TO_DATE(''' || vFromDt ||''',''DD-MM-YYYY'') AND TO_DATE(''' || vToDt || ''',''DD-MM-YYYY'') ' 
                --||' AND NOT EXISTS (SELECT 1 FROM ACPY_CP58 CP58 WHERE CP58.AC_NO=A.AC_NO) '; --37.00       ---38.00
                ||' AND A.AC_NO IN  (SELECT  DISTINCT B.AC_NO  FROM CMGE_SAP_EVENTS_ACC A, ACPY_GL B '  --38.00
                ||' WHERE A.EVENT_CODE = B.EVENT_CODE AND A.MODULE = ''CQ'' AND A.EINV_IND =''Y'' and A.IRBM_TYPE <> ''SBNM'' AND B.AC_NO = A.AC_NO ) '; --37.00         ---38.00       
           IF pBranch IS NOT NULL THEN /* 36.00 Start*/
      lStrSQL := lStrSQL ||' AND A.ISSUE_OFFICE = '''|| pBranch ||''' ';
             END IF; /* 36.00 End*/
             IF eInvStatus = 'ALL'  THEN  
     lStrSQL := lStrSQL ||'AND UPPER(A.EINV_STATUS) IN (''PENDING'',''SUBMITTED'',''INVALID'',''REJECTED'',''FAIL'')'; /* 36.00*/
             ELSE
     lStrSQL := lStrSQL ||' AND UPPER(A.EINV_STATUS) = '''|| eInvStatus ||''' ';
            END IF;
       --lStrSQL := lStrSQL ||') WHERE  EINV_IND = ''Y'' ';
       lStrSQL := lStrSQL ||')  ';
       -- 37.00         
         lStrSQL := lStrSQL ||' UNION ALL    (SELECT TRAN_DATE,DOWNLOAD_DATE,BRANCH_NAME,BRANCH_CODE,DOCUMENT_TYPE,DOCUMENT_NO,STAX_NO,AGENT_NAME,EVENT_CODE,AMOUNT,OPERATOR,EINV_STATUS, '
                ||' VALIDTN_REASON,CUTOFF_DATE, EINV_IND FROM ( '
                ||' SELECT DISTINCT TO_CHAR(A.TRAN_DATE,''dd/MM/yyyy'')  AS TRAN_DATE, TO_CHAR(PAY.DOWNLOAD_DATE,''dd/MM/yyyy'') AS DOWNLOAD_DATE,B.BRANCH_NAME AS BRANCH_NAME,A.ISSUE_OFFICE AS BRANCH_CODE, ''Payment'' as DOCUMENT_TYPE, '
                ||' A.AC_NO AS DOCUMENT_NO,E.EINV_NO AS STAX_NO,CP.NAME_EXT AS AGENT_NAME,GL.EVENT_CODE AS EVENT_CODE,'
                --||' CASE WHEN A.REV_REASON IS NULL THEN NVL(E.CP58_AMT,0) ELSE  NVL(E.CP58_AMT,0) *-1 END AS AMOUNT, '  --38.00
                || ' E.CP58_AMT AS AMOUNT, ' --38.00
                ||' A.OPERATOR AS OPERATOR,E.EINV_STATUS AS EINV_STATUS,E.EINV_REASON AS VALIDTN_REASON , '
                ||'  (SELECT CODE FROM SAPM_NEWUTIL WHERE UKEY=''OPUSI_EINVOICE_CUTOFF'') AS CUTOFF_DATE,A.EINV_IND ,1 '                
                ||' FROM ACPY_PYMT A  '
                ||' LEFT JOIN ACPY_CP58 E ON A.AC_NO=E.AC_NO '
                ||' LEFT JOIN TABLE ( PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (E.PART_ID, E.PART_VERSION)) CP ON E.PART_ID = CP.PART_ID AND E.PART_VERSION = CP.VERSION '                
                --||' LEFT JOIN (SELECT A.AC_NO, A.EVENT_CODE, A.AC_GLAMT FROM ACPY_GL A '  --38.00
                ||' LEFT JOIN (SELECT A.AC_NO, A.EVENT_CODE, A.AC_GLAMT,A.AC_GLSEQ,A.CP58_IND  FROM ACPY_GL A ' --38.00
                ||' WHERE A.EVENT_CODE IN (SELECT EVENT_CODE  FROM CMGE_SAP_EVENTS_ACC C '
                --||' WHERE NOT EXISTS (SELECT 1 FROM CMGE_CODE D '  --38.00
                --||' WHERE D.CAT_CODE =''EINV_TAX_EVENT'' AND D.CODE_CD = A.EVENT_CODE) and C.EINV_IND = ''Y''))GL ON GL.AC_NO =A.AC_NO '  --38.00
                ||' WHERE  C.EINV_IND = ''Y'' AND C.MODULE = ''CQ'' AND A.CP58_IND = ''Y'' ))GL ON GL.AC_NO =A.AC_NO AND GL.AC_GLSEQ = E.GL_SEQ_NO '  --38.00
                ||' LEFT JOIN ACPY_PAYLINK PAY ON A.AC_NO = PAY.AC_NO '
                ||' LEFT JOIN CMDM_BRANCH B ON A.ISSUE_OFFICE = B.BRANCH_CODE '
                ||' WHERE TRUNC(A.TRAN_DATE) BETWEEN  TO_DATE(''' || vFromDt ||''',''DD-MM-YYYY'') AND TO_DATE(''' || vToDt || ''',''DD-MM-YYYY'') ' 
                ||' AND E.PROFILE_TYPE=''AG'' ' 
                ||' AND E.GL_SEQ_NO = GL.AC_GLSEQ AND A.AC_NO IN  (SELECT  DISTINCT B.AC_NO  FROM CMGE_SAP_EVENTS_ACC A, ACPY_GL B ' --38.00
                ||' WHERE A.EVENT_CODE = B.EVENT_CODE AND A.MODULE = ''CQ'' AND A.EINV_IND =''Y'' AND A.IRBM_TYPE = ''SBNM'' AND B.AC_NO = A.AC_NO)   ' ;--38.00
            IF pBranch IS NOT NULL THEN /* 36.00 Start*/
              lStrSQL := lStrSQL ||' AND A.ISSUE_OFFICE = '''|| pBranch ||''' ';
             END IF; /* 36.00 End*/
             IF eInvStatus = 'ALL'  THEN  
                lStrSQL := lStrSQL ||'AND UPPER(E.EINV_STATUS) IN (''PENDING'',''SUBMITTED'',''INVALID'',''REJECTED'',''FAIL'')'; /* 36.00*/
             ELSE
                lStrSQL := lStrSQL ||' AND UPPER(A.EINV_STATUS) = '''|| eInvStatus ||''' ';
            END IF;       
            lStrSQL := lStrSQL ||'))  ';
            -- 37.00
--       IF pDocType IS NOT NULL  THEN
--        lStrSQL := lStrSQL ||' ORDER BY TRAN_DATE,DOCUMENT_NO,DOCUMENT_TYPE DESC ';
--       END IF;
       --DBMS_OUTPUT.put_line ('CQ-PAYMENT  IN - '|| lStrSQL);
       IF pDocType IS NULL THEN 
           lStrSQL := lStrSQL || 'UNION ALL  ';
        END IF;
END IF; -- PAYMENT IF END          
IF pDocType = 'JN' OR pDocType IS NULL  THEN   -- JOURNAL IF START
     -- DBMS_OUTPUT.put_line ('JOURNAL IN : ');     
      lStrSQL := lStrSQL ||' SELECT  TRAN_DATE,DOWNLOAD_DATE,BRANCH_NAME,BRANCH_CODE,DOCUMENT_TYPE,DOCUMENT_NO,STAX_NO,AGENT_NAME,EVENT_CODE,AMOUNT,OPERATOR,EINV_STATUS, '
                ||' VALIDTN_REASON,CUTOFF_DATE,EINV_IND FROM ( '
                ||' SELECT DISTINCT TO_CHAR(A.TRAN_DATE,''dd/MM/yyyy'')  AS TRAN_DATE, '''' AS DOWNLOAD_DATE,B.BRANCH_NAME AS BRANCH_NAME,A.ISSUE_OFFICE AS BRANCH_CODE, ''Journal'' as DOCUMENT_TYPE, '
                ||' A.AC_NO AS DOCUMENT_NO,A.EINV_NO AS STAX_NO,CP.NAME_EXT AS AGENT_NAME,GL.EVENT_CODE AS EVENT_CODE,'
                ||' CASE WHEN A.REV_REASON IS NULL THEN NVL(GL.AC_GLAMT,0) ELSE  NVL(GL.AC_GLAMT,0) *-1 END AS AMOUNT, '
                ||' A.OPERATOR AS OPERATOR,A.EINV_STATUS AS EINV_STATUS,A.EINV_REASON AS VALIDTN_REASON , '
                ||'  (SELECT CODE FROM SAPM_NEWUTIL WHERE UKEY=''OPUSI_EINVOICE_CUTOFF'') AS CUTOFF_DATE, A.EINV_IND ,1 '
                --||'  (SELECT  EV.EINV_IND FROM CMGE_SAP_EVENTS_ACC EV WHERE EV.event_code = GL.EVENT_CODE and rownum=1)  as EINV_IND '
                ||' FROM ACJN_JOUR A  '
                ||' LEFT JOIN TABLE ( PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (A.PART_ID, A.PART_VERSION)) CP ON A.PART_ID = CP.PART_ID AND A.PART_VERSION = CP.VERSION '
                --||' LEFT JOIN ACJN_GL GL ON GL.AC_NO =A.AC_NO '
                --||' LEFT JOIN CMGE_SAP_EVENTS_ACC EV ON GL.EVENT_CODE = EV.EVENT_CODE AND EV.EINV_IND = ''Y'' AND EV.MODULE=''JN'' '
                ||' LEFT JOIN (SELECT A.AC_NO, A.EVENT_CODE, A.AC_GLAMT FROM ACJN_GL A '
                ||' WHERE A.EVENT_CODE IN (SELECT EVENT_CODE  FROM CMGE_SAP_EVENTS_ACC C '
                --||' WHERE NOT EXISTS (SELECT 1 FROM CMGE_CODE D ' --38.00
               -- ||' WHERE D.CAT_CODE =''EINV_TAX_EVENT'' AND D.CODE_CD = A.EVENT_CODE) and C.EINV_IND = ''Y''))GL ON GL.AC_NO =A.AC_NO '--38.00
               ||' WHERE C.EINV_IND = ''Y'' AND C.MODULE = ''JN'' ))GL ON GL.AC_NO =A.AC_NO '--38.00
                ||' LEFT JOIN CMDM_BRANCH B ON A.ISSUE_OFFICE = B.BRANCH_CODE '
                || ' WHERE TRUNC(A.TRAN_DATE) BETWEEN  TO_DATE(''' || vFromDt ||''',''DD-MM-YYYY'') AND TO_DATE(''' || vToDt || ''',''DD-MM-YYYY'') ' ;
              IF pBranch IS NOT NULL THEN /* 36.00 Start*/
      lStrSQL := lStrSQL ||' AND A.ISSUE_OFFICE = '''|| pBranch ||''' ';
             END IF; /* 36.00 End*/
             IF eInvStatus = 'ALL'  THEN  
     lStrSQL := lStrSQL ||'AND UPPER(A.EINV_STATUS) IN (''PENDING'',''SUBMITTED'',''INVALID'',''REJECTED'',''FAIL'')'; /* 36.00*/
             ELSE
     lStrSQL := lStrSQL ||' AND UPPER(A.EINV_STATUS) = '''|| eInvStatus ||''' ';
            END IF;
      -- lStrSQL := lStrSQL ||') WHERE  EINV_IND = ''Y'' ';
       lStrSQL := lStrSQL ||')  '; 
--       IF pDocType IS NOT NULL  THEN
--        lStrSQL := lStrSQL ||' ORDER BY TRAN_DATE,DOCUMENT_NO,DOCUMENT_TYPE DESC ';
--       END IF;
      -- DBMS_OUTPUT.put_line ('JN-JOURNAL  IN - '|| lStrSQL);
       IF pDocType IS NULL THEN 
           lStrSQL := lStrSQL || 'UNION ALL  ';
        END IF;
END IF; -- JOURNAL IF END           
IF pDocType = 'AL' OR pDocType IS NULL  THEN   -- JOURNAL IF START
     -- DBMS_OUTPUT.put_line ('RECEIPT IN : ');     
      lStrSQL := lStrSQL ||' SELECT  TRAN_DATE ,DOWNLOAD_DATE,BRANCH_NAME,BRANCH_CODE,DOCUMENT_TYPE,DOCUMENT_NO,STAX_NO,AGENT_NAME,EVENT_CODE,AMOUNT,OPERATOR,EINV_STATUS, '
                ||' VALIDTN_REASON,CUTOFF_DATE, EINV_IND FROM ( '
                ||' SELECT DISTINCT TO_CHAR(A.TRAN_DATE,''dd/MM/yyyy'')  AS TRAN_DATE, '''' AS DOWNLOAD_DATE,B.BRANCH_NAME AS BRANCH_NAME,A.ISSUE_OFFICE AS BRANCH_CODE, ''Receipt'' as DOCUMENT_TYPE, '
                ||' A.AC_NO AS DOCUMENT_NO,A.EINV_NO AS STAX_NO,CP.NAME_EXT AS AGENT_NAME,GL.EVENT_CODE AS EVENT_CODE,'
                ||' CASE WHEN A.REV_REASON IS NULL THEN NVL(GL.AC_GLAMT,0) ELSE  NVL(GL.AC_GLAMT,0) *-1 END AS AMOUNT, '
                ||' A.OPERATOR AS OPERATOR,A.EINV_STATUS AS EINV_STATUS,A.EINV_REASON AS VALIDTN_REASON , '
                ||'  (SELECT CODE FROM SAPM_NEWUTIL WHERE UKEY=''OPUSI_EINVOICE_CUTOFF'') AS CUTOFF_DATE, A.EINV_IND ,1 '
               -- ||'  (SELECT  EV.EINV_IND FROM CMGE_SAP_EVENTS_ACC EV WHERE EV.event_code = GL.EVENT_CODE and rownum=1)  as EINV_IND '
                ||' FROM ACRC_RCPT A  '
                ||' LEFT JOIN TABLE ( PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (A.PART_ID, A.PART_VERSION)) CP ON A.PART_ID = CP.PART_ID AND A.PART_VERSION = CP.VERSION '
                --||' LEFT JOIN ACRC_GL GL ON GL.AC_NO =A.AC_NO '
                --||' LEFT JOIN CMGE_SAP_EVENTS_ACC EV ON GL.EVENT_CODE = EV.EVENT_CODE AND EV.EINV_IND = ''Y'' AND EV.MODULE=''AL'' '
                ||' LEFT JOIN (SELECT A.AC_NO, A.EVENT_CODE, A.AC_GLAMT FROM ACRC_GL A '
                ||' WHERE A.EVENT_CODE IN (SELECT EVENT_CODE  FROM CMGE_SAP_EVENTS_ACC C '
                --||' WHERE NOT EXISTS (SELECT 1 FROM CMGE_CODE D '--38.00
                --||' WHERE D.CAT_CODE =''EINV_TAX_EVENT'' AND D.CODE_CD = A.EVENT_CODE) and C.EINV_IND = ''Y''))GL ON GL.AC_NO =A.AC_NO ' --38.00
                ||' WHERE C.EINV_IND = ''Y'' AND C.MODULE = ''AL''))GL ON GL.AC_NO =A.AC_NO ' --38.00
                ||' LEFT JOIN CMDM_BRANCH B ON A.ISSUE_OFFICE = B.BRANCH_CODE '
                || ' WHERE TRUNC(A.TRAN_DATE) BETWEEN  TO_DATE(''' || vFromDt ||''',''DD-MM-YYYY'') AND TO_DATE(''' || vToDt || ''',''DD-MM-YYYY'') ' ;
            IF pBranch IS NOT NULL THEN  /* 36.00 Start*/
      lStrSQL := lStrSQL ||' AND A.ISSUE_OFFICE = '''|| pBranch ||''' ';
             END IF;/* 36.00 End*/
             IF eInvStatus = 'ALL'  THEN  
     lStrSQL := lStrSQL ||'AND UPPER(A.EINV_STATUS) IN (''PENDING'',''SUBMITTED'',''INVALID'',''REJECTED'',''FAIL'') '; /* 36.00*/
             ELSE
     lStrSQL := lStrSQL ||' AND UPPER(A.EINV_STATUS) = '''|| eInvStatus ||''' ';
            END IF;
       --lStrSQL := lStrSQL ||') WHERE  EINV_IND = ''Y'' '; 
       lStrSQL := lStrSQL ||') '; 
--       IF pDocType IS NOT NULL  THEN
--        lStrSQL := lStrSQL ||' ORDER BY TRAN_DATE,DOCUMENT_NO,DOCUMENT_TYPE DESC ';
--       END IF;
END IF; -- RECEIPT IF END              

        lStrSQL := lStrSQL || '  ) ORDER BY TRAN_DATE ,DOCUMENT_TYPE, DOCUMENT_NO ASC ';

      DBMS_OUTPUT.put_line ('EINV STATUS Query - ' || lStrSQL);
      OPEN v_cursor FOR lStrSQL;
      LOOP
         FETCH v_cursor
           INTO r_row.TRAN_DATE,
                r_row.DOWNLOAD_DATE,
                r_row.BRANCH_NAME,
                r_row.BRANCH_CODE,
                r_row.DOCUMENT_TYPE,
                r_row.DOCUMENT_NO,
                r_row.STAX_NO,
                r_row.AGENT_NAME,
                r_row.EVENT_CODE,
                r_row.AMOUNT,
                r_row.OPERATOR,
                r_row.EINV_STATUS, 
                r_row.VALIDTN_REASON--,
              --  r_row.CUTOFF_DATE
                ;

         EXIT WHEN v_cursor%NOTFOUND;
         PIPE ROW (r_row);
      END LOOP;

      CLOSE v_cursor;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         PG_UTIL_LOG_ERROR.PC_INS_log_error (
            v_ProcName_v || '.' || v_ProcName_v,
            1,
            SQLERRM);
END FN_EINV_STATUS_REPORT_DTL;
--34.00 end 

END PG_RPGE_LISTING;