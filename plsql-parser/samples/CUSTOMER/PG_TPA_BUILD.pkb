CREATE OR REPLACE PACKAGE BODY                            PG_TPA_BUILD AS
V_PKG_NAME   VARCHAR2 (50) := 'PG_TPA_BUILD';
--1.4
v_AAN_ENDT_CODE_R                SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE
      := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_ENDT_CODE_R');
V_AAN_ENDT_CODE_A            SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE
      := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_ENDT_CODE_A');
v_AAN_ENDT_CODE                 SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE
      := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_ENDT_CODE');
/******************************************************************************
   NAME:       PG_TPA_BUILD
   PURPOSE:    TPA BUILD DOWNLOAD FILE

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        06/01/2018  RAMAKRISHNA.METLA  1. Created this package body.
   1.1        23/07/2018  RAMAKRISHNA.METLA  AAN bug Principal Name,Principal NRIC
   1.2        30/07/2018  SAI.PAVAN          SPAA ADDED
   1.3        20/08/2018  RAMAKRISHNA.METLA  IMPLEMENTING UWGE_POLICY_CTRL_DLOAD LOGIC
   1.4        12/11/2018  RAMAKRISHNA.METLA  Enhancement #99405 for AAN Data Downloading for Plines and CEB (Data selection based on Endorsement code  add new column in the bordereaux to cater OP Sub-cover)

   1.6        28/01/2019  SAI.PAVAN          Enhancement #105111 To automate the email notification to Users after Data Downloading for AAN
   1.7        19/03/2019  SAI.PAVAN          Bug Fix for incorrect data captured in Download Fail List for AAN Checksum
   1.8        29/03/2019  RAMAKRISHNA.METLA  Bug #107927 Data Downloading SPIKPA - Worker ID and Risk ID are different when downloaded from OPUS and IIMS.
   1.9        23/09/2019  VIPIN VINCE        Enhancement 116958_ALLIANZ SHIELD PLUS -Added IMA FEE ,MCO FEES  LIMIT
   1.10       27/09/2019  VIPIN VINCE        Enhancement 121349 Mondial Data Downloading removing the conditions for Replacement Fee
   2.0        23/04/2020  KAHFINA            Enhancement 128431 AAN data downloading for HC and PA import type PC_TPA_AAN_HC_PA_POL_ENDT_2
   2.1        10/12/2019  VIPIN VINCE        Enhacement 127746 MERCEDEZ BENZ AAN_Products 080100_080200_080300
   3.0        16/09/2022  PRABHU RAMACHANDRAN AGIC-2808 - Motor MRW and MREW Data Download for MONDIAL.
   3.1           13/01/2022  JACELINE             Fixes for AGIC-2808 - Breaking line when reading endorsement narration 
   3.2        13/01/2023  PRABHU RAMACHANDRAN AGIC-11691- Error in MRW and MERW data to Mondial.
     3.5        13/12/2024  RAULNICO GONZALES  Enhancement AGIC-41641 - EA (Euro Assistance) Exit Plan (Adhoc Submission to Medix)
   3.4        03/12/2024  RAULNICO GONZALES  Enhancement AGIC-41641 - EA (Euro Assistance) Exit Plan
******************************************************************************/
PROCEDURE PC_TPA_MONDIAL_POL(P_DOWNLOAD_TYPE IN VARCHAR2,P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE) IS

  CURSOR C_TPA_MONDIAL
      IS
       SELECT OPB.POLICY_REF,UPV.CONTRACT_ID,(CASE  WHEN CP.ID_VALUE1 IS  NULL THEN CP.ID_VALUE2 WHEN LENGTH(CP.ID_VALUE1)=12
 THEN
 SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
 ELSE CP.ID_VALUE1
        END) AS NRIC_NUMBER,
        CP.NAME_EXT,REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
        REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,
        REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3,
        CPA.POSTCODE,
        (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'CITY'
        AND CODE_CD = CPA.CITY) AS CITY,
        (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'STATE'
        AND CODE_CD = CPA.STATE) AS STATE,
        UPB.CNOTE_NO,UPB.LONG_NAME,TO_CHAR(UPB.EFF_DATE, 'YYYYMMDD') AS EFF_DATE,TO_CHAR(UPB.EXP_DATE, 'YYYYMMDD') AS EXP_DATE,UPB.AGENT_CODE,
        TO_CHAR(UPV.ISSUE_DATE, 'YYYYMMDD') AS ISSUE_DATE,CP.EMAIL,regexp_replace((CASE  WHEN CP.MOBILE_NO1 is not null and CP.MOBILE_CODE1 is not null THEN CP.MOBILE_CODE1||CP.MOBILE_NO1   else CP.MOBILE_CODE2||CP.MOBILE_NO2 END),'[^0-9]') AS PhoneNumber,
        CPA.PHONE_CODE,CPA.PHONE_NO,
         URV.VEH_NO,(select  CMV.VEH_MODEL_DESC from  CMUW_MODEL_VEH CMV where CMV.VEH_MODEL_CODE=URV.VEH_MODEL) AS VEH_MODEL_DESC, (CASE WHEN  URV.VEH_MAKE_YEAR='0' THEN '0000' ELSE  URV.VEH_MAKE_YEAR||'' END)AS VEH_MAKE_YEAR,
         URV.VEH_CHASSIS,(SELECT UPLC.PLAN_CODE FROM UWPL_COVER UPLC WHERE UPLC.CONTRACT_ID =UCOV.CONTRACT_ID
         AND UPLC.COV_ID =UCOV.COV_ID
         AND UPLC.VERSION_NO =UCOV.VERSION_NO) AS PLAN_CODE,(SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE ='POL_STATUS' AND CODE_CD =UPC.POLICY_STATUS) AS POLICY_STATUS
        ,UPCD.VERSION_NO --1.3
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD  --1.3
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =1
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =1
        INNER JOIN UWGE_POLICY_FEES UPF
        ON UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =1
        AND UPF.FEE_CODE ='ASST'
        INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
        ON CP.PART_ID=UPB.CP_PART_ID
        AND CP.VERSION=UPB.CP_VERSION
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (UPB.CP_ADDR_ID,UPB.CP_ADDR_VERSION)) CPA
        ON CPA.ADD_ID = UPB.CP_ADDR_ID
        AND CPA.VERSION = UPB.CP_ADDR_VERSION
        INNER JOIN UWGE_RISK_VEH URV
        ON URV.CONTRACT_ID =UPV.CONTRACT_ID
        AND URV.VERSION_NO =1
        INNER JOIN UWGE_COVER UCOV
        ON UCOV.CONTRACT_ID =UPV.CONTRACT_ID
        AND URV.RISK_ID =UCOV.RISK_ID
        AND UCOV.COV_PARENT_ID IS NULL
        AND UCOV.VERSION_NO =1
        WHERE  UPC.PRODUCT_CONFIG_CODE IN(
        select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', P_DOWNLOAD_TYPE),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', P_DOWNLOAD_TYPE), '[^,]+', 1, level) is not null )
        AND UPC.POLICY_STATUS ='A'
        AND UPV.VERSION_NO =1
        --AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy')
        AND UPCD.DLOAD_STATUS ='P' --1.3
        AND UPCD.TPA_NAME='MONDIAL'
        AND PLPB.TPA_NAME = 'MONDIAL'
        --AND UPF.FEE_AMT >0 --1.10 Enhancement 121349
        AND UPV.ENDT_NO IS NULL;

        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_MONDIAL_POL';
        FILENAME  UTL_FILE.FILE_TYPE;
      FILENAME1 VARCHAR2(1000);
      v_file_dir VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_MONDIAL_DIR');
          REC C_TPA_MONDIAL%rowtype;
          seq number := 1;
         V_RET                 NUMBER := 0; --1.3

    BEGIN
            V_STEPS := '001';
             FILENAME1   := TO_CHAR(P_START_DT, 'YYYYMMDD')||'_' || P_DOWNLOAD_TYPE || 'POL_MONDIAL.CSV';
          --dbms_output.put_line ('FILENAME1=' || FILENAME1);
      FILENAME    := UTL_FILE.FOPEN(v_file_dir, FILENAME1, 'W',32767);

        UTL_FILE.PUT_LINE(FILENAME,
                       'Sequence Number' || ',' || 'Cover Note Number' || ',' || 'Policy Number' || ',' ||
                        'Chassis Number' || ',' || 'Vehicle Number'|| ',' || 'Vehicle Make/Model'
                                                || ',' || 'Year Manufactured'|| ',' || 'Program'|| ',' || 'Plan Code'
                                                || ',' || 'Attaching Motor/Personal Accident Insurance'|| ',' || 'Insured Name'
                                                || ',' || 'Insured''s IC/ID Number'|| ',' || 'Insured''s Phone Number'
                                                || ',' || 'Insured''s Email Address'|| ',' || 'Insured''s Home Address'
                                                || ',' || 'Postcode'|| ',' || 'City'|| ',' || 'State'|| ',' || 'Effective Date'
                                                || ',' || 'Expiry Date'|| ',' || 'Transaction Date'|| ',' || 'Policy Status'
                                                || ',' || ' Endorsement Effective Date'|| ',' || ' Endorsement Remark');
        FOR REC IN C_TPA_MONDIAL
          LOOP
        UTL_FILE.PUT_LINE(FILENAME,
                          seq || ' ,' || '"' ||
                          REC.CNOTE_NO || '"' || ' ,' || '"' ||
                          REC.POLICY_REF || '"' || ' ,' ||'"=""' ||
                          REC.VEH_CHASSIS || '"""' || ' ,' || '"=""' ||
                          REC.VEH_NO|| '"""'|| ' ,' || '"' ||
                          REC.VEH_MODEL_DESC|| '"'|| ' ,' || '"' ||
                          REC.VEH_MAKE_YEAR|| '"'|| ' ,' || '"' ||
                          P_DOWNLOAD_TYPE|| '"'|| ' ,' || '"' ||
                          REC.PLAN_CODE|| '"'|| ' ,' || '""'|| ' ,' || '"' ||
                          REC.NAME_EXT|| '"'|| ' ,' || '"=""' ||
                          REC.NRIC_NUMBER|| '"""'|| ' ,' ||'"=""'||
                                                      REC.PhoneNumber|| '"""'|| ' ,' || '"' ||
                          REC.EMAIL|| '"'|| ' ,' || '"' ||
                          REC.ADDRESS_LINE1||' '||REC.ADDRESS_LINE2||' '||REC.ADDRESS_LINE3|| '"'|| ' ,' || '"' ||
                          REC.POSTCODE|| '"'|| ' ,' || '"' ||
                          REC.CITY|| '"'|| ' ,' || '"' ||
                          REC.STATE|| '"'|| ' ,' || '"' ||
                          REC.EFF_DATE|| '"'|| ' ,' || '"' ||
                          REC.EXP_DATE|| '"'|| ' ,' || '"' ||
                                                      REC.ISSUE_DATE|| '"'|| ' ,' || REC.POLICY_STATUS);

          V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.VERSION_NO,'MONDIAL'); --1.3
          seq :=seq+1;
      END LOOP;
      UTL_FILE.FCLOSE(FILENAME);
    EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                    --dbms_output.put_line ('SQLERRM=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
    END PC_TPA_MONDIAL_POL;

  PROCEDURE PC_TPA_MONDIAL_ENDT(P_DOWNLOAD_TYPE IN VARCHAR2,P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE) IS

  CURSOR C_TPA_MONDIAL
      IS
        SELECT OPB.POLICY_REF,UPV.CONTRACT_ID,UPV.ENDT_NO,(CASE  WHEN CP.ID_VALUE1 IS  NULL THEN CP.ID_VALUE2 WHEN LENGTH(CP.ID_VALUE1)=12
 THEN
 SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
 ELSE CP.ID_VALUE1
        END) AS NRIC_NUMBER,
        (CASE WHEN ID_TYPE1 = 'OLD_IC' THEN ID_VALUE1 WHEN ID_TYPE2 = 'OLD_IC' THEN ID_VALUE2 END) AS IC_PASSPORT,
        CP.NAME_EXT,REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
        REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,
        REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3,
        CPA.POSTCODE,
        (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'CITY'
        AND CODE_CD = CPA.CITY) AS CITY,
        (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'STATE'
        AND CODE_CD = CPA.STATE) AS STATE,
        UPB.CNOTE_NO,UPB.LONG_NAME,
        TO_CHAR(UPB.EFF_DATE, 'YYYYMMDD') AS EFF_DATE,TO_CHAR(UPB.EXP_DATE, 'YYYYMMDD') AS EXP_DATE,UPB.AGENT_CODE,
        TO_CHAR(UPV.ISSUE_DATE, 'YYYYMMDD') AS ISSUE_DATE,TO_CHAR(UPV.ENDT_EFF_DATE, 'YYYYMMDD') AS ENDT_EFF_DATE,
        REPLACE (UPV.ENDT_NARR, CHR (10), '') AS ENDT_NARR,CP.EMAIL,regexp_replace((CASE  WHEN CP.MOBILE_NO1 is not null and CP.MOBILE_CODE1 is not null THEN CP.MOBILE_CODE1||CP.MOBILE_NO1   else CP.MOBILE_CODE2||CP.MOBILE_NO2 END),'[^0-9]') AS PhoneNumber,
        CPA.PHONE_CODE,CPA.PHONE_NO,
         URV.VEH_NO,(select  CMV.VEH_MODEL_DESC from  CMUW_MODEL_VEH CMV where CMV.VEH_MODEL_CODE=URV.VEH_MODEL) AS VEH_MODEL_DESC, (CASE WHEN  URV.VEH_MAKE_YEAR='0' THEN '0000' ELSE  URV.VEH_MAKE_YEAR||'' END)AS VEH_MAKE_YEAR,
         URV.VEH_CHASSIS,(SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE ='POL_STATUS' AND CODE_CD =UPC.POLICY_STATUS) AS POLICY_STATUS,
         UPV.ENDT_CNT,UPV.VERSION_NO AS POLICY_VERSION --1.3
         ,UCOV.COV_ID
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD --1.3
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWGE_POLICY_FEES UPF
        ON UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='ASST'
        INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
        ON CP.PART_ID=UPB.CP_PART_ID
        AND CP.VERSION=UPB.CP_VERSION
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (UPB.CP_ADDR_ID,UPB.CP_ADDR_VERSION)) CPA
        ON CPA.ADD_ID = UPB.CP_ADDR_ID
        AND CPA.VERSION = UPB.CP_ADDR_VERSION
        INNER JOIN SB_UWGE_RISK_VEH URV
        ON URV.CONTRACT_ID =UPV.CONTRACT_ID
        AND URV.POLICY_VERSION =UPV.VERSION_NO
        INNER JOIN UWGE_COVER UCOV
        ON UCOV.CONTRACT_ID =UPV.CONTRACT_ID
        AND URV.RISK_ID =UCOV.RISK_ID
        AND UCOV.COV_PARENT_ID IS NULL
        AND UCOV.VERSION_NO =UPV.VERSION_NO
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', P_DOWNLOAD_TYPE),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', P_DOWNLOAD_TYPE), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS IN('A','C','E')
        AND UPV.VERSION_NO >1
        AND UPV.ACTION_CODE IN('A','C')
        AND (UPV.ENDT_CODE IS NOT NULL AND UPV.ENDT_CODE NOT IN('75','108'))-- 1.10 Enhancement 121349
        --AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy')
        AND UPCD.DLOAD_STATUS ='P' --1.3
        AND UPCD.TPA_NAME='MONDIAL'
        AND PLPB.TPA_NAME = 'MONDIAL'; --1.10 Enhancement 121349 start
        --AND UPF.FEE_AMT >=0;  --1.10 Enhancement 121349 end

        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_MONDIAL_POL';
        FILENAME  UTL_FILE.FILE_TYPE;
        FILENAME1 VARCHAR2(1000);
        v_file_dir VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_MONDIAL_DIR');
        REC C_TPA_MONDIAL%rowtype;
        V_RET                 NUMBER := 0; --1.3
        V_UWPL_COVER_DET   PG_TPA_UTILS.UWPL_COVER_DET;

    BEGIN
          V_STEPS := '001';
          FILENAME1   := TO_CHAR(P_START_DT, 'YYYYMMDD')||'_' || P_DOWNLOAD_TYPE || 'END_MONDIAL.CSV';
      FILENAME    := UTL_FILE.FOPEN(v_file_dir, FILENAME1, 'W',32767);

        UTL_FILE.PUT_LINE(FILENAME,
                       'Sequence Number' || ',' || 'Cover Note Number' || ',' || 'Policy Number' || ',' ||
                        'Chassis Number' || ',' || 'Vehicle Number'|| ',' || 'Vehicle Make/Model'
                                                || ',' || 'Year Manufactured'|| ',' || 'Program'|| ',' || 'Plan Code'
                                                || ',' || 'Attaching Motor/Personal Accident Insurance'|| ',' || 'Insured Name'
                                                || ',' || 'Insured''s IC/ID Number'|| ',' || 'Insured''s Phone Number'
                                                || ',' || 'Insured''s Email Address'|| ',' || 'Insured''s Home Address'
                                                || ',' || 'Postcode'|| ',' || 'City'|| ',' || 'State'|| ',' || 'Effective Date'
                                                || ',' || 'Expiry Date'|| ',' || 'Transaction Date'|| ',' || 'Policy Status'
                                                || ',' || ' Endorsement Effective Date'|| ',' || ' Endorsement Remark');
        FOR REC IN C_TPA_MONDIAL
          LOOP
          V_UWPL_COVER_DET := PG_TPA_UTILS.FN_GET_UWPL_COVER_DET(REC.CONTRACT_ID,REC.POLICY_VERSION,REC.COV_ID);
        UTL_FILE.PUT_LINE(FILENAME,
                          '"'||' '||'"' || ' ,' || '"' ||
                          REC.CNOTE_NO || '"' || ' ,' || '"' ||
                          REC.ENDT_NO || '"' || ' ,' || '"=""' ||
                          REC.VEH_CHASSIS || '"""' || ' ,' || '"=""' ||
                          REC.VEH_NO|| '"""'|| ' ,' || '"' ||
                          REC.VEH_MODEL_DESC|| '"'|| ' ,' || '"' ||
                          REC.VEH_MAKE_YEAR|| '"'|| ' ,' || '"' ||
                          P_DOWNLOAD_TYPE|| '"'|| ' ,' || '"' ||
                          NVL(V_UWPL_COVER_DET.PLAN_CODE,' ')|| '"'|| ' ,' || '""'|| ' ,' || '"' ||
                          REC.NAME_EXT|| '"'|| ' ,' || '"=""' ||
                          REC.NRIC_NUMBER|| '"""'|| ' ,' || '"=""' ||
                          REC.PhoneNumber|| '"""'|| ' ,' || '"' ||
                          REC.EMAIL|| '"'|| ' ,' || '"' ||
                          REC.ADDRESS_LINE1||' '||REC.ADDRESS_LINE2||' '||REC.ADDRESS_LINE3|| '"'|| ' ,' || '"' ||
                          REC.POSTCODE|| '"'|| ' ,' || '"' ||
                          REC.CITY|| '"'|| ' ,' || '"' ||
                          REC.STATE|| '"'|| ' ,' || '"' ||
                          REC.EFF_DATE|| '"'|| ' ,' || '"' ||
                          REC.EXP_DATE|| '"'|| ' ,' || '"' ||
                          REC.ISSUE_DATE|| '"'|| ' ,' || '"'||REC.POLICY_STATUS||'"'|| ' ,' || '"' ||
                          REC.ENDT_EFF_DATE|| '"'|| ' ,' || '"' ||
                          REC.ENDT_NARR|| '"');
--UTL_FILE.FFLUSH(FILENAME);

      V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.POLICY_VERSION,'MONDIAL'); --1.3
      END LOOP;
      UTL_FILE.FCLOSE(FILENAME);
    EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                    --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
    END PC_TPA_MONDIAL_ENDT;
 PROCEDURE PC_TPA_MOTOR_MONDIAL_POL(P_DOWNLOAD_TYPE IN VARCHAR2,P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE) IS

  CURSOR C_TPA_MONDIAL
      IS
       SELECT UPB.CNOTE_NO,OPB.POLICY_REF, URV.VEH_CHASSIS,URV.VEH_NO,(select  CMV.VEH_MODEL_DESC from  CMUW_MODEL_VEH CMV where CMV.VEH_MODEL_CODE=URV.VEH_MODEL) AS VEH_MODEL_DESC,
        (CASE WHEN  URV.VEH_MAKE_YEAR='0' THEN '0000' ELSE  URV.VEH_MAKE_YEAR||'' END)AS VEH_MAKE_YEAR,'AAA' as Program,(SELECT UPLC.PLAN_CODE FROM UWPL_COVER UPLC WHERE UPLC.CONTRACT_ID =UCOV.CONTRACT_ID
         AND UPLC.COV_ID =UCOV.COV_ID
         AND UPLC.VERSION_NO =UCOV.VERSION_NO) AS PLAN_CODE,'YES' AS Attaching,CP.NAME_EXT ,
         (CASE  WHEN CP.ID_VALUE1 IS  NULL THEN CP.ID_VALUE2 WHEN LENGTH(CP.ID_VALUE1)=12
         THEN SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
         ELSE CP.ID_VALUE1 END) AS NRIC_NUMBER,
        regexp_replace((CASE  WHEN CP.MOBILE_NO1 is not null and CP.MOBILE_CODE1 is not null THEN CP.MOBILE_CODE1||CP.MOBILE_NO1   else CP.MOBILE_CODE2||CP.MOBILE_NO2 END),'[^0-9]') AS PhoneNumber,
         CP.EMAIL,
        REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
        REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,
        REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3,
        CPA.POSTCODE,
        (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'CITY'
        AND CODE_CD = CPA.CITY) AS CITY,
        (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'STATE'
        AND CODE_CD = CPA.STATE) AS STATE,
        TO_CHAR(UPB.EFF_DATE, 'YYYYMMDD') AS EFF_DATE,
        TO_CHAR(UPB.EXP_DATE, 'YYYYMMDD') AS EXP_DATE,
        TO_CHAR(UPV.ISSUE_DATE, 'YYYYMMDD') AS ISSUE_DATE,
        (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE ='POL_STATUS' AND CODE_CD =UPC.POLICY_STATUS) AS POLICY_STATUS
        ,UPV.CONTRACT_ID,UPV.VERSION_NO --1.3
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD --1.3
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =1
        INNER JOIN UWGE_POLICY_MT UPM
        ON UPM.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPM.VERSION_NO =1
        INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
        ON CP.PART_ID=UPB.CP_PART_ID
        AND CP.VERSION=UPB.CP_VERSION
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (UPB.CP_ADDR_ID,UPB.CP_ADDR_VERSION)) CPA
        ON CPA.ADD_ID = UPB.CP_ADDR_ID
        AND CPA.VERSION = UPB.CP_ADDR_VERSION
        INNER JOIN UWGE_RISK_VEH URV
        ON URV.CONTRACT_ID =UPV.CONTRACT_ID
        AND URV.VERSION_NO =1
        INNER JOIN UWGE_COVER UCOV
        ON UCOV.CONTRACT_ID =UPV.CONTRACT_ID
        AND URV.RISK_ID =UCOV.RISK_ID
        AND UCOV.COV_PARENT_ID IS NULL
        AND UCOV.VERSION_NO =1
        WHERE  UPC.PRODUCT_CONFIG_CODE IN(
        select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', P_DOWNLOAD_TYPE),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', P_DOWNLOAD_TYPE), '[^,]+', 1, level) is not null )
        AND UPC.POLICY_STATUS ='A'
        AND UPV.VERSION_NO =1
        --AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy')
        AND UPCD.DLOAD_STATUS ='P' --1.3
        AND UPCD.TPA_NAME='MONDIAL'
        AND UPM.DEALER_PROVIDER = 'MONDIAL'
        --AND UPM.DEALER3A_FEE >0 --1.10 Enhancement 121349 
        AND UPV.ENDT_NO IS NULL;

        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_MOTOR_MONDIAL_POL';
        FILENAME  UTL_FILE.FILE_TYPE;
      FILENAME1 VARCHAR2(1000);
      v_file_dir VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_MONDIAL_DIR');
          REC C_TPA_MONDIAL%rowtype;
          seq number := 1;
        V_RET                 NUMBER := 0; --1.3

    BEGIN
            V_STEPS := '001';
             FILENAME1   := TO_CHAR(P_START_DT, 'YYYYMMDD')||'_' || P_DOWNLOAD_TYPE || 'POL_MONDIAL.CSV';
          --dbms_output.put_line ('FILENAME1=' || FILENAME1);
      FILENAME    := UTL_FILE.FOPEN(v_file_dir, FILENAME1, 'W',32767);

        UTL_FILE.PUT_LINE(FILENAME,
                       'Sequence Number' || ',' || 'Cover Note Number' || ',' || 'Policy Number' || ',' ||
                        'Chassis Number' || ',' || 'Vehicle Number'|| ',' || 'Vehicle Make/Model'
                                                || ',' || 'Year Manufactured'|| ',' || 'Program'|| ',' || 'Plan Code'
                                                || ',' || 'Attaching Motor/Personal Accident Insurance'|| ',' || 'Insured Name'
                                                || ',' || 'Insured''s IC/ID Number'|| ',' || 'Insured''s Phone Number'
                                                || ',' || 'Insured''s Email Address'|| ',' || 'Insured''s Home Address'
                                                || ',' || 'Postcode'|| ',' || 'City'|| ',' || 'State'|| ',' || 'Effective Date'
                                                || ',' || 'Expiry Date'|| ',' || 'Transaction Date'|| ',' || 'Policy Status'
                                                || ',' || ' Endorsement Effective Date'|| ',' || ' Endorsement Remark');
        FOR REC IN C_TPA_MONDIAL
          LOOP
        UTL_FILE.PUT_LINE(FILENAME,
                          seq || ' ,' || '"' ||
                          REC.CNOTE_NO || '"' || ' ,' || '"' ||
                          REC.POLICY_REF || '"' || ' ,' ||'"=""' ||
                          REC.VEH_CHASSIS || '"""' || ' ,' || '"=""' ||
                          REC.VEH_NO|| '"""'|| ' ,' || '"' ||
                          REC.VEH_MODEL_DESC|| '"'|| ' ,' || '"' ||
                          REC.VEH_MAKE_YEAR|| '"'|| ' ,' || '"' ||
                          REC.Program|| '"'|| ' ,' || '"' ||
                          REC.PLAN_CODE|| '"'|| ' ,' || '"' ||
                          REC.Attaching|| '"'|| ' ,' || '"' ||
                          REC.NAME_EXT|| '"'|| ' ,' || '"=""' ||
                          REC.NRIC_NUMBER|| '"""'|| ' ,' ||'"=""'||
                          REC.PhoneNumber|| '"""'|| ' ,' || '"' ||
                          REC.EMAIL|| '"'|| ' ,' || '"' ||
                          REC.ADDRESS_LINE1||' '||REC.ADDRESS_LINE2||' '||REC.ADDRESS_LINE3|| '"'|| ' ,' || '"' ||
                          REC.POSTCODE|| '"'|| ' ,' || '"' ||
                          REC.CITY|| '"'|| ' ,' || '"' ||
                          REC.STATE|| '"'|| ' ,' || '"' ||
                          REC.EFF_DATE|| '"'|| ' ,' || '"' ||
                          REC.EXP_DATE|| '"'|| ' ,' || '"' ||
                          REC.ISSUE_DATE|| '"'|| ' ,' || REC.POLICY_STATUS);

          V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.VERSION_NO,'MONDIAL'); --1.3
          seq :=seq+1;
      END LOOP;
      UTL_FILE.FCLOSE(FILENAME);
    EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                    --dbms_output.put_line ('SQLERRM=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
    END PC_TPA_MOTOR_MONDIAL_POL;

 PROCEDURE PC_TPA_MOTOR_MONDIAL_ENDT(P_DOWNLOAD_TYPE IN VARCHAR2,P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE) IS

  CURSOR C_TPA_MONDIAL
      IS
         SELECT UPV.ENDT_CNT,UPB.CNOTE_NO,UPV.ENDT_NO,URV.VEH_CHASSIS,URV.VEH_NO,(select  CMV.VEH_MODEL_DESC from  CMUW_MODEL_VEH CMV where CMV.VEH_MODEL_CODE=URV.VEH_MODEL) AS VEH_MODEL_DESC,
         (CASE WHEN  URV.VEH_MAKE_YEAR='0' THEN '0000' ELSE  URV.VEH_MAKE_YEAR||'' END)AS VEH_MAKE_YEAR,'AAA' AS PROGRAM,
                 'NO' AS ASD, CP.NAME_EXT,(CASE  WHEN CP.ID_VALUE1 IS  NULL THEN CP.ID_VALUE2 WHEN LENGTH(CP.ID_VALUE1)=12 THEN
         SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
         ELSE CP.ID_VALUE1 END) AS NRIC_NUMBER,
         regexp_replace((CASE  WHEN CP.MOBILE_NO1 is not null and CP.MOBILE_CODE1 is not null THEN CP.MOBILE_CODE1||CP.MOBILE_NO1   else CP.MOBILE_CODE2||CP.MOBILE_NO2 END),'[^0-9]') AS PhoneNumber,CP.EMAIL,
         REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
        REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,
        REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3, CPA.POSTCODE,(SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'CITY'
        AND CODE_CD = CPA.CITY) AS CITY,
        (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'STATE'
        AND CODE_CD = CPA.STATE) AS STATE,
        TO_CHAR(UPB.EFF_DATE, 'YYYYMMDD') AS EFF_DATE,
        TO_CHAR(UPB.EXP_DATE, 'YYYYMMDD') AS EXP_DATE,
        TO_CHAR(UPV.ISSUE_DATE, 'YYYYMMDD') AS ISSUE_DATE,
         (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE ='POL_STATUS' AND CODE_CD =UPC.POLICY_STATUS) AS POLICY_STATUS,
        TO_CHAR(UPV.ENDT_EFF_DATE, 'YYYYMMDD') AS ENDT_EFF_DATE,
          REPLACE (UPV.ENDT_NARR, CHR (10), '') AS ENDT_NARR
          ,UPV.CONTRACT_ID,UPV.VERSION_NO AS POLICY_VERSION --1.3
          ,UCOV.COV_ID
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD --1.3
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWGE_POLICY_MT UPM
        ON UPM.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPM.VERSION_NO =UPV.VERSION_NO
        INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
        ON CP.PART_ID=UPB.CP_PART_ID
        AND CP.VERSION=UPB.CP_VERSION
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (UPB.CP_ADDR_ID,UPB.CP_ADDR_VERSION)) CPA
        ON CPA.ADD_ID = UPB.CP_ADDR_ID
        AND CPA.VERSION = UPB.CP_ADDR_VERSION
        INNER JOIN SB_UWGE_RISK_VEH URV
        ON URV.CONTRACT_ID =UPV.CONTRACT_ID
        AND URV.POLICY_VERSION =UPV.VERSION_NO
        INNER JOIN UWGE_COVER UCOV
        ON UCOV.CONTRACT_ID =UPV.CONTRACT_ID
        AND URV.RISK_ID =UCOV.RISK_ID
        AND UCOV.COV_PARENT_ID IS NULL
        AND UCOV.VERSION_NO =UPV.VERSION_NO
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', P_DOWNLOAD_TYPE),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', P_DOWNLOAD_TYPE), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS IN('A','C','E')
        AND UPV.VERSION_NO >1
        AND UPV.ACTION_CODE IN('A','C')
        AND (UPV.ENDT_CODE IS NOT NULL AND UPV.ENDT_CODE NOT IN('75','108'))--1.10 Enhancement 121349
        --AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy')
        AND UPCD.DLOAD_STATUS ='P' --1.3
        AND UPCD.TPA_NAME='MONDIAL'
         AND UPM.DEALER_PROVIDER = 'MONDIAL'; --1.10 Enhancement 121349 start
        --AND UPM.DEALER3A_FEE >0; --1.10 Enhancement 121349 end
        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_MOTOR_MONDIAL_ENDT';
        FILENAME  UTL_FILE.FILE_TYPE;
        FILENAME1 VARCHAR2(1000);
        v_file_dir VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_MONDIAL_DIR');
        REC C_TPA_MONDIAL%rowtype;
        V_RET                 NUMBER := 0; --1.3
        V_UWPL_COVER_DET   PG_TPA_UTILS.UWPL_COVER_DET;

    BEGIN
          V_STEPS := '001';
          FILENAME1   := TO_CHAR(P_START_DT, 'YYYYMMDD')||'_' || P_DOWNLOAD_TYPE || 'END_MONDIAL.CSV';
      FILENAME    := UTL_FILE.FOPEN(v_file_dir, FILENAME1, 'W',32767);
        V_STEPS := '002';
        UTL_FILE.PUT_LINE(FILENAME,
                       'Sequence Number' || ',' || 'Cover Note Number' || ',' || 'Policy Number' || ',' ||
                        'Chassis Number' || ',' || 'Vehicle Number'|| ',' || 'Vehicle Make/Model'
                                                || ',' || 'Year Manufactured'|| ',' || 'Program'|| ',' || 'Plan Code'
                                                || ',' || 'Attaching Motor/Personal Accident Insurance'|| ',' || 'Insured Name'
                                                || ',' || 'Insured''s IC/ID Number'|| ',' || 'Insured''s Phone Number'
                                                || ',' || 'Insured''s Email Address'|| ',' || 'Insured''s Home Address'
                                                || ',' || 'Postcode'|| ',' || 'City'|| ',' || 'State'|| ',' || 'Effective Date'
                                                || ',' || 'Expiry Date'|| ',' || 'Transaction Date'|| ',' || 'Policy Status'
                                                || ',' || ' Endorsement Effective Date'|| ',' || ' Endorsement Remark');
        FOR REC IN C_TPA_MONDIAL
          LOOP
          V_STEPS := '003';
          V_UWPL_COVER_DET := PG_TPA_UTILS.FN_GET_UWPL_COVER_DET(REC.CONTRACT_ID,REC.POLICY_VERSION,REC.COV_ID);
          V_STEPS := '004';
        UTL_FILE.PUT_LINE(FILENAME,
                          '"'||' '||'"' || ' ,' || '"' ||
                          REC.CNOTE_NO || '"' || ' ,' || '"' ||
                          REC.ENDT_NO || '"' || ' ,' || '"=""' ||
                          REC.VEH_CHASSIS || '"""' || ' ,' || '"=""' ||
                          REC.VEH_NO|| '"""'|| ' ,' || '"' ||
                          REC.VEH_MODEL_DESC|| '"'|| ' ,' || '"' ||
                          REC.VEH_MAKE_YEAR|| '"'|| ' ,' || '"' ||
                          REC.PROGRAM|| '"'|| ' ,' || '"' ||
                          NVL(V_UWPL_COVER_DET.PLAN_CODE,' ')|| '"'|| ' ,' || '"' ||
                          REC.ASD|| '"'|| ' ,' || '"' ||
                          REC.NAME_EXT|| '"'|| ' ,' || '"=""' ||
                          REC.NRIC_NUMBER|| '"""'|| ' ,' || '"=""' ||
                          REC.PhoneNumber|| '"""'|| ' ,' || '"' ||
                          REC.EMAIL|| '"'|| ' ,' || '"' ||
                          REC.ADDRESS_LINE1||' '||REC.ADDRESS_LINE2||' '||REC.ADDRESS_LINE3|| '"'|| ' ,' || '"' ||
                          REC.POSTCODE|| '"'|| ' ,' || '"' ||
                          REC.CITY|| '"'|| ' ,' || '"' ||
                          REC.STATE|| '"'|| ' ,' || '"' ||
                          REC.EFF_DATE|| '"'|| ' ,' || '"' ||
                          REC.EXP_DATE|| '"'|| ' ,' || '"' ||
                          REC.ISSUE_DATE|| '"'|| ' ,' || '"'||REC.POLICY_STATUS||'"'|| ' ,' || '"' ||
                          REC.ENDT_EFF_DATE|| '"'|| ' ,' || '"' ||
                          REC.ENDT_NARR|| '"');
--UTL_FILE.FFLUSH(FILENAME);
      V_STEPS := '005';
      V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.POLICY_VERSION,'MONDIAL'); --1.3
      V_STEPS := '006';
      END LOOP;
      UTL_FILE.FCLOSE(FILENAME);
    EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    'REC.CNOTE_NO::'||REC.CNOTE_NO || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                    --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
    END PC_TPA_MOTOR_MONDIAL_ENDT;

  PROCEDURE PC_TPA_MOOSHICAR_POL(P_DOWNLOAD_TYPE IN VARCHAR2,P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE) IS

  CURSOR C_TPA_MOOSHICAR
      IS
       SELECT OPB.POLICY_REF,(CASE  WHEN CP.ID_VALUE1 IS  NULL THEN CP.ID_VALUE2 WHEN LENGTH(CP.ID_VALUE1)=12
 THEN
 SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
 ELSE CP.ID_VALUE1
        END) AS NRIC,
        NVL(CP.NAME_EXT,' ') AS NAME_EXT,NVL(UPB.CNOTE_NO,' ') AS CNOTE_NO,UPB.LONG_NAME,TO_CHAR(UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,TO_CHAR(UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,UPB.AGENT_CODE,
        TO_CHAR(UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,NVL(CP.EMAIL,' ') AS EMAIL,NVL(CP.MOBILE_CODE1||'-'||CP.MOBILE_NO1,' ') AS MOBILE_NO,
         NVL(URV.VEH_NO,' ') AS VEH_NO,NVL((select  CMV.VEH_MODEL_DESC from  CMUW_MODEL_VEH CMV where CMV.VEH_MODEL_CODE=URV.VEH_MODEL),' ') AS VEH_MODEL_DESC,
         NVL(URV.VEH_CHASSIS,' ') AS VEH_CHASSIS,( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =1
        AND UPF.FEE_CODE ='OCR') AS OCR_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =1
        AND UPF.FEE_CODE ='CRF') AS CRF_FEE_AMT,
       NVL( (SELECT (SELECT CPB.DESCP ||CPB.NARRATION FROM CMUW_PL_BENEFIT CPB WHERE CPB.BEN_CODE =UCB.BENEFIT_CODE
       AND CPB.MAINCLS = (SELECT VALUE FROM PDC_V_PROD_ATTRIBUTE_VALUES WHERE ATTRIBUTE = 'MAINCLS'
       AND PRODUCT_CODE =UPC.PRODUCT_CONFIG_CODE AND ROWNUM=1))
  FROM UWPL_COVER_BENEFIT UCB WHERE UCB.CONTRACT_ID=UPV.CONTRACT_ID
  AND UCB.BENEFIT_CODE IN('CR','OPCR1','OPCR2') AND UCB.VERSION_NO=1 AND ROWNUM=1),' ') AS BEN_DESCP,
         UPLC.PLAN_CODE AS PLAN_CODE,
         URV.CONTACT_NAME,URV.PHONE_NO
         ,UPV.CONTRACT_ID,UPV.VERSION_NO --1.3
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD --1.3
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =1
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =1
        INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
        ON CP.PART_ID=UPB.CP_PART_ID
        AND CP.VERSION=UPB.CP_VERSION
        INNER JOIN UWGE_RISK_VEH URV
        ON URV.CONTRACT_ID =UPV.CONTRACT_ID
        AND URV.VERSION_NO =1
        INNER JOIN UWGE_COVER UCOV
        ON UCOV.CONTRACT_ID =UPV.CONTRACT_ID
        AND URV.RISK_ID =UCOV.RISK_ID
        AND UCOV.COV_PARENT_ID IS NULL
        AND UCOV.VERSION_NO =1
        INNER JOIN UWPL_COVER UPLC
        ON UPLC.CONTRACT_ID =UCOV.CONTRACT_ID
         AND UPLC.COV_ID =UCOV.COV_ID
         AND UPLC.VERSION_NO =UCOV.VERSION_NO
        WHERE  UPC.PRODUCT_CONFIG_CODE IN(
        select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', P_DOWNLOAD_TYPE),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', P_DOWNLOAD_TYPE), '[^,]+', 1, level) is not null )
        AND UPC.POLICY_STATUS ='A'
        AND UPV.VERSION_NO =1
        AND UPC.LOB='PL'
        AND UPV.ENDT_NO IS NULL
        --AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy');
        AND UPCD.DLOAD_STATUS ='P'
        AND UPCD.TPA_NAME='MOOSHICAR'; --1.3


        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_MOOSHICAR_POL';
        FILENAME  UTL_FILE.FILE_TYPE;
      FILENAME1 VARCHAR2(1000);
      v_file_dir VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_MOOSHICAR_D');
          rowIDx number := 5;
          seq number := 1;
          V_RET                 NUMBER := 0; --1.3

    BEGIN
            V_STEPS := '001';
             FILENAME1   := TO_CHAR(P_START_DT, 'YYYYMMDD')||'_' || P_DOWNLOAD_TYPE || 'POL_MOOSHICAR.xlsx';


         PG_EXCEL_UTILS.clear_workbook;
        PG_EXCEL_UTILS.new_sheet;
        IF P_DOWNLOAD_TYPE ='CAR'
        THEN
            PG_EXCEL_UTILS.CELL(1,1,'CIMB AUTO RELIEF BORDEREAUX (POLICY)');
         ELSIF P_DOWNLOAD_TYPE ='ERW'
         THEN
          PG_EXCEL_UTILS.CELL(1,1,'ENHANCED ROAD WARRIOR BORDEREAUX (POLICY)');
          ELSIF P_DOWNLOAD_TYPE ='STERW'
         THEN
          PG_EXCEL_UTILS.CELL(1,1,'Short-term Enhanced Road Warrior BORDEREAUX (POLICY)');
        END IF;
        PG_EXCEL_UTILS.MERGECELLS(1,1,3,1);
        PG_EXCEL_UTILS.CELL(1,2,'FROM : ALLIANZ GENERAL INSURANCE COMPANY (MALAYSIA) BERHAD');
        PG_EXCEL_UTILS.MERGECELLS(1,2,3,2);
        PG_EXCEL_UTILS.CELL(1,3,'DATE :');
        PG_EXCEL_UTILS.CELL(2,3,TO_CHAR(P_START_DT, 'DD/MM/YYYY'));

        PG_EXCEL_UTILS.SET_ROW(4
        ,p_fontId => PG_EXCEL_UTILS.get_font( 'Arial',p_bold => true));
        PG_EXCEL_UTILS.CELL(1,4,'No.');
        PG_EXCEL_UTILS.CELL(2,4,'Name');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(2,30);
        PG_EXCEL_UTILS.CELL(3,4,'Emergency Contact Name and Number');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(3,40);
        PG_EXCEL_UTILS.CELL(4,4,'Make');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(4,20);
        PG_EXCEL_UTILS.CELL(5,4,'Registration No.');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(5,20);
        PG_EXCEL_UTILS.CELL(6,4,'Effective Date');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(6,20);
        PG_EXCEL_UTILS.CELL(7,4,'Expiry Date');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(7,20);
        PG_EXCEL_UTILS.CELL(8,4,'NRIC No.');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(8,20);
        PG_EXCEL_UTILS.CELL(9,4,'EASC Cover Note No.');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(9,20);
        PG_EXCEL_UTILS.CELL(10,4,'Policy No.');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(10,20);
        PG_EXCEL_UTILS.CELL(11,4,'Plan');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(11,20);
        PG_EXCEL_UTILS.CELL(12,4,'Region');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(12,20);
        PG_EXCEL_UTILS.CELL(13,4,'Car Replacement');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(13,25);
        PG_EXCEL_UTILS.CELL(14,4,'Optional Car Replacement (Y/N)',p_alignment => PG_EXCEL_UTILS.get_alignment(p_vertical =>'left',p_horizontal =>'top',p_wrapText=>true));
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(14,20);

        FOR REC IN C_TPA_MOOSHICAR
          LOOP
          IF REC.OCR_FEE_AMT >0 OR REC.CRF_FEE_AMT >0
          THEN
            PG_EXCEL_UTILS.CELL(1,rowIDx,seq);
            PG_EXCEL_UTILS.CELL(2,rowIDx,NVL(REC.NAME_EXT,' '));
            PG_EXCEL_UTILS.CELL(3,rowIDx,NVL(REC.CONTACT_NAME,' ')||' '||NVL(REC.PHONE_NO,' '));
            PG_EXCEL_UTILS.CELL(4,rowIDx,REC.VEH_MODEL_DESC);
            PG_EXCEL_UTILS.CELL(5,rowIDx,REC.VEH_NO);
            PG_EXCEL_UTILS.CELL(6,rowIDx,REC.EFF_DATE);
            PG_EXCEL_UTILS.CELL(7,rowIDx,REC.EXP_DATE);
            PG_EXCEL_UTILS.CELL(8,rowIDx,REC.NRIC);
            PG_EXCEL_UTILS.CELL(9,rowIDx,REC.CNOTE_NO);
            PG_EXCEL_UTILS.CELL(10,rowIDx,REC.POLICY_REF);
            PG_EXCEL_UTILS.CELL(11,rowIDx,REC.PLAN_CODE);
            PG_EXCEL_UTILS.CELL(12,rowIDx,' ');
            PG_EXCEL_UTILS.CELL(13,rowIDx,REC.BEN_DESCP,p_alignment => PG_EXCEL_UTILS.get_alignment(p_vertical =>'left',p_horizontal =>'top',p_wrapText=>true));
            IF REC.OCR_FEE_AMT >0 THEN
              PG_EXCEL_UTILS.CELL(14,rowIDx,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment(p_vertical =>'center',p_horizontal =>'center',p_wrapText=>true));
            ELSE
              PG_EXCEL_UTILS.CELL(14,rowIDx,'N',p_alignment => PG_EXCEL_UTILS.get_alignment(p_vertical =>'center',p_horizontal =>'center',p_wrapText=>true));
            END IF;
            rowIDx :=rowIDx+1;
            seq :=seq+1;
            V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.VERSION_NO,'MOOSHICAR'); --1.3
        END IF;
      END LOOP;
            PG_EXCEL_UTILS.save( v_file_dir, FILENAME1 );
    EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                    --dbms_output.put_line ('SQLERRM=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
    END PC_TPA_MOOSHICAR_POL;

   PROCEDURE PC_TPA_MOOSHICAR_ENDT(P_DOWNLOAD_TYPE IN VARCHAR2,P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE) IS

     CURSOR C_TPA_MOOSHICAR
      IS
        SELECT (CASE  WHEN CP.ID_VALUE1 IS  NULL THEN CP.ID_VALUE2 WHEN LENGTH(CP.ID_VALUE1)=12 THEN
         SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
         ELSE CP.ID_VALUE1 END) AS NRIC,
        CP.NAME_EXT,UPB.CNOTE_NO,UPB.LONG_NAME,TO_CHAR(UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,TO_CHAR(UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
        TO_CHAR(UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,   NVL(URV.VEH_NO,' ') AS VEH_NO,
         NVL((select  CMV.VEH_MODEL_DESC from  CMUW_MODEL_VEH CMV where CMV.VEH_MODEL_CODE=URV.VEH_MODEL),' ') AS VEH_MODEL_DESC,
         ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='OCR') AS OCR_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='CRF') AS CRF_FEE_AMT,
        NVL( (SELECT (SELECT CPB.DESCP ||CPB.NARRATION FROM CMUW_PL_BENEFIT CPB WHERE CPB.BEN_CODE =UCB.BENEFIT_CODE
       AND CPB.MAINCLS = (SELECT VALUE FROM PDC_V_PROD_ATTRIBUTE_VALUES WHERE ATTRIBUTE = 'MAINCLS'
       AND PRODUCT_CODE =UPC.PRODUCT_CONFIG_CODE AND ROWNUM=1))
      FROM UWPL_COVER_BENEFIT UCB WHERE UCB.CONTRACT_ID=UPV.CONTRACT_ID
      AND UCB.BENEFIT_CODE IN('CR','OPCR1','OPCR2') AND UCB.VERSION_NO=1 AND ROWNUM=1),' ') AS BEN_DESCP,
      NVL(UPV.ENDT_NARR,' ') AS ENDT_NARR,UPV.ENDT_NO,
         URV.CONTACT_NAME,URV.PHONE_NO
         ,UPV.CONTRACT_ID,UPV.VERSION_NO AS POLICY_VERSION --1.3
         ,UCOV.COV_ID
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD --1.3
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
        ON CP.PART_ID=UPB.CP_PART_ID
        AND CP.VERSION=UPB.CP_VERSION
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (UPB.CP_ADDR_ID,UPB.CP_ADDR_VERSION)) CPA
        ON CPA.ADD_ID = UPB.CP_ADDR_ID
        AND CPA.VERSION = UPB.CP_ADDR_VERSION
        INNER JOIN SB_UWGE_RISK_VEH URV
        ON URV.CONTRACT_ID =UPV.CONTRACT_ID
        AND URV.POLICY_VERSION =UPV.VERSION_NO
        INNER JOIN UWGE_COVER UCOV
        ON UCOV.CONTRACT_ID =UPV.CONTRACT_ID
        AND UCOV.VERSION_NO =UPV.VERSION_NO
        AND URV.RISK_ID =UCOV.RISK_ID
        AND UCOV.COV_PARENT_ID IS NULL
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', P_DOWNLOAD_TYPE),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', P_DOWNLOAD_TYPE), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS ='A'
        AND UPC.LOB='PL'
        AND UPV.VERSION_NO >1
        AND UPV.ENDT_NO IS NOT NULL
        --AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy');
        AND UPCD.DLOAD_STATUS ='P'
        AND UPCD.TPA_NAME='MOOSHICAR'; --1.3

        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_MOOSHICAR_ENDT';
        FILENAME  UTL_FILE.FILE_TYPE;
        FILENAME1 VARCHAR2(1000);
        v_file_dir VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_MOOSHICAR_D');
         rowIDx number := 5;
         V_RET                 NUMBER := 0; --1.3
         V_UWPL_COVER_DET   PG_TPA_UTILS.UWPL_COVER_DET;

    BEGIN
            V_STEPS := '001';
             FILENAME1   := TO_CHAR(P_START_DT, 'YYYYMMDD')||'_' || P_DOWNLOAD_TYPE || 'END_MOOSHICAR.xlsx';

         PG_EXCEL_UTILS.clear_workbook;
        PG_EXCEL_UTILS.new_sheet;
        IF P_DOWNLOAD_TYPE ='CAR'
        THEN
            PG_EXCEL_UTILS.CELL(1,1,'CIMB AUTO RELIEF BORDEREAUX (ENDORSEMENT)');
         ELSIF P_DOWNLOAD_TYPE ='ERW'
         THEN
          PG_EXCEL_UTILS.CELL(1,1,'ENHANCED ROAD WARRIOR BORDEREAUX (ENDORSEMENT)');
          ELSIF P_DOWNLOAD_TYPE ='STERW'
         THEN
          PG_EXCEL_UTILS.CELL(1,1,'Short-term Enhanced Road Warrior BORDEREAUX (ENDORSEMENT)');
        END IF;
        PG_EXCEL_UTILS.MERGECELLS(1,1,3,1);
        PG_EXCEL_UTILS.CELL(1,2,'FROM : ALLIANZ GENERAL INSURANCE COMPANY (MALAYSIA) BERHAD');
        PG_EXCEL_UTILS.MERGECELLS(1,2,3,2);
        PG_EXCEL_UTILS.CELL(1,3,'DATE :');
        PG_EXCEL_UTILS.CELL(2,3,TO_CHAR(P_START_DT, 'DD/MM/YYYY'));

        PG_EXCEL_UTILS.SET_ROW(4
        ,p_fontId => PG_EXCEL_UTILS.get_font( 'Arial',p_bold => true));
        PG_EXCEL_UTILS.CELL(1,4,'No.');
        PG_EXCEL_UTILS.CELL(2,4,'Name');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(2,30);
        PG_EXCEL_UTILS.CELL(3,4,'Emergency Contact Name and Number');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(3,40);
        PG_EXCEL_UTILS.CELL(4,4,'Make');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(4,20);
        PG_EXCEL_UTILS.CELL(5,4,'Registration No.');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(5,20);
        PG_EXCEL_UTILS.CELL(6,4,'Effective Date');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(6,20);
        PG_EXCEL_UTILS.CELL(7,4,'Expiry Date');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(7,20);
        PG_EXCEL_UTILS.CELL(8,4,'NRIC No.');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(8,20);
        PG_EXCEL_UTILS.CELL(9,4,'Endorsement No.');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(9,20);
        PG_EXCEL_UTILS.CELL(10,4,'Plan');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(10,20);
        PG_EXCEL_UTILS.CELL(11,4,'Region');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(11,20);
        PG_EXCEL_UTILS.CELL(12,4,'Car Replacement');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(12,25);
        PG_EXCEL_UTILS.CELL(13,4,'Optional Car Replacement (Y/N)',p_alignment => PG_EXCEL_UTILS.get_alignment(p_vertical =>'left',p_horizontal =>'top',p_wrapText=>true));
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(13,20);
        PG_EXCEL_UTILS.CELL(14,4,'Text Decription');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(14,60);

        FOR REC IN C_TPA_MOOSHICAR
          LOOP
          IF REC.OCR_FEE_AMT >=0 OR REC.CRF_FEE_AMT >=0
          THEN
            PG_EXCEL_UTILS.CELL(1,rowIDx,' ');
            PG_EXCEL_UTILS.CELL(2,rowIDx,NVL(REC.NAME_EXT,' '));
            PG_EXCEL_UTILS.CELL(3,rowIDx,NVL(REC.CONTACT_NAME,' ')||' '||NVL(REC.PHONE_NO,' '));
            PG_EXCEL_UTILS.CELL(4,rowIDx,REC.VEH_MODEL_DESC);
            PG_EXCEL_UTILS.CELL(5,rowIDx,REC.VEH_NO);
            PG_EXCEL_UTILS.CELL(6,rowIDx,REC.EFF_DATE);
            PG_EXCEL_UTILS.CELL(7,rowIDx,REC.EXP_DATE);
            PG_EXCEL_UTILS.CELL(8,rowIDx,REC.NRIC);
            PG_EXCEL_UTILS.CELL(9,rowIDx,REC.ENDT_NO);
            V_UWPL_COVER_DET := PG_TPA_UTILS.FN_GET_UWPL_COVER_DET(REC.CONTRACT_ID,REC.POLICY_VERSION,REC.COV_ID);
            PG_EXCEL_UTILS.CELL(10,rowIDx,NVL(V_UWPL_COVER_DET.PLAN_CODE,' '));
            PG_EXCEL_UTILS.CELL(11,rowIDx,' ');
            PG_EXCEL_UTILS.CELL(12,rowIDx,REC.BEN_DESCP,p_alignment => PG_EXCEL_UTILS.get_alignment(p_vertical =>'left',p_horizontal =>'top',p_wrapText=>true));
            IF REC.OCR_FEE_AMT >0 THEN
              PG_EXCEL_UTILS.CELL(13,rowIDx,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment(p_vertical =>'center',p_horizontal =>'center',p_wrapText=>true));
            ELSE
              PG_EXCEL_UTILS.CELL(13,rowIDx,'N',p_alignment => PG_EXCEL_UTILS.get_alignment(p_vertical =>'center',p_horizontal =>'center',p_wrapText=>true));
            END IF;
            PG_EXCEL_UTILS.CELL(14,rowIDx,REC.ENDT_NARR);
            V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.POLICY_VERSION,'MOOSHICAR'); --1.3
            rowIDx :=rowIDx+1;
        END IF;
      END LOOP;
            PG_EXCEL_UTILS.save( v_file_dir, FILENAME1 );
    EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                    --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
    END PC_TPA_MOOSHICAR_ENDT;

    PROCEDURE PC_TPA_AAN_MS_POL_ENDT(P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE) IS

     CURSOR C_TPA_AAN_MS
      IS
        SELECT OPB.POLICY_REF,UPV.VERSION_NO AS POLICY_VERSION,
        (CASE  WHEN CP.ID_VALUE1 is not null AND length(ID_VALUE1) =12 THEN  SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
        WHEN CP.ID_VALUE1 is not null THEN CP.ID_VALUE1 else CP.ID_VALUE2  END) AS ID_VALUE,
        (CASE WHEN ID_TYPE1 IS NULL THEN ID_TYPE2 ELSE ID_TYPE1 END) ID_TYPE,
        UPC.PRODUCT_CONFIG_CODE,(SELECT CODE_DESC FROM CMGE_CODE CC WHERE CC.CODE_CD=UPC.PRODUCT_CONFIG_CODE AND CC.CAT_CODE = UPC.LOB||'_PRODUCT') AS PRODUCT_DESC,
        CP.NAME_EXT,UPB.LONG_NAME,TO_CHAR(UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,TO_CHAR(UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
        TO_CHAR(UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,
        NVL((SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='ASST'),0) AS ASST_FEE_AMT,
        NVL((SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='DMA'),0) AS DMA_FEE_AMT,
        NVL(UPV.ENDT_NARR,' ') AS ENDT_NARR,UPV.ENDT_NO
        ,UPV.CONTRACT_ID --1.3
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD--1.3
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWPL_RISK_PERSON URP
        ON URP.CONTRACT_ID =UPV.CONTRACT_ID
        AND URP.VERSION_NO =UPV.VERSION_NO
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(URP.RISK_PART_ID, URP.RISK_PART_VER)) CP
        ON CP.PART_ID=URP.RISK_PART_ID
        AND CP.VERSION=URP.RISK_PART_VER
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_MS'),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_MS'), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS IN('A','C','E')
        AND PLPB.TPA_NAME = 'A'
        AND (UPV.ENDT_CODE IS NULL OR UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE_R,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE_R, '[^,]+', 1, level) is not null )) --1.4
        AND UPV.ACTION_CODE IN('A','C')
        AND UPC.LOB='MS'
        --AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy');
        AND UPCD.DLOAD_STATUS ='P'
        AND UPCD.TPA_NAME='AAN'
        UNION ALL --1.4
        SELECT OPB.POLICY_REF,UPV.VERSION_NO AS POLICY_VERSION,
        (CASE  WHEN CP.ID_VALUE1 is not null AND length(ID_VALUE1) =12 THEN  SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
        WHEN CP.ID_VALUE1 is not null THEN CP.ID_VALUE1 else CP.ID_VALUE2  END) AS ID_VALUE,
        (CASE WHEN ID_TYPE1 IS NULL THEN ID_TYPE2 ELSE ID_TYPE1 END) ID_TYPE,
        UPC.PRODUCT_CONFIG_CODE,(SELECT CODE_DESC FROM CMGE_CODE CC WHERE CC.CODE_CD=UPC.PRODUCT_CONFIG_CODE AND CC.CAT_CODE = UPC.LOB||'_PRODUCT') AS PRODUCT_DESC,
        CP.NAME_EXT,UPB.LONG_NAME,TO_CHAR(UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,TO_CHAR(UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
        TO_CHAR(UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,
        NVL((SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='ASST'),0) AS ASST_FEE_AMT,
        NVL((SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='DMA'),0) AS DMA_FEE_AMT,
        NVL(UPV.ENDT_NARR,' ') AS ENDT_NARR,UPV.ENDT_NO
        ,UPV.CONTRACT_ID --1.3
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD--1.3
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWPL_RISK_PERSON URP
        ON URP.CONTRACT_ID =UPV.CONTRACT_ID
        AND URP.VERSION_NO =(SELECT MAX (b.version_no)
        FROM UWPL_RISK_PERSON b
        WHERE b.contract_id = UPV.CONTRACT_ID
        AND URP.object_id = b.object_id
        AND b.version_no <= UPV.VERSION_NO
        AND b.reversing_version IS NULL)
        AND URP.action_code <> 'D'
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(URP.RISK_PART_ID, URP.RISK_PART_VER)) CP
        ON CP.PART_ID=URP.RISK_PART_ID
        AND CP.VERSION=URP.RISK_PART_VER
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_MS'),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_MS'), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS IN('A','C','E')
        AND PLPB.TPA_NAME = 'A'
        AND UPV.VERSION_NO >1
        AND (UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE_A,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE_A, '[^,]+', 1, level) is not null ))
        AND UPV.ACTION_CODE IN('A','C')
        AND UPC.LOB='MS'
        --AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy');
        AND UPCD.DLOAD_STATUS ='P'
        AND UPCD.TPA_NAME='AAN'
        ; --1.3

        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_AAN_MS_POL_ENDT';
        FILENAME  UTL_FILE.FILE_TYPE;
        FILENAME1 VARCHAR2(1000);
        v_file_dir VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_AAN_DIR');
         rowIDx number := 5;
          seq number := 1;
          V_RET                 NUMBER := 0; --1.3

    BEGIN
            V_STEPS := '001';
             FILENAME1   := TO_CHAR(P_START_DT, 'YYYYMMDD')||'_MISCELLANEOUS_POLEND.xlsx';

         PG_EXCEL_UTILS.clear_workbook;
        PG_EXCEL_UTILS.new_sheet;
            PG_EXCEL_UTILS.CELL(1,1,'BORDEREAUX (POLICY &'||' ENDORSEMENT)');

        PG_EXCEL_UTILS.MERGECELLS(1,1,3,1);
        PG_EXCEL_UTILS.CELL(1,2,'FROM : ALLIANZ GENERAL INSURANCE COMPANY (MALAYSIA) BERHAD');
        PG_EXCEL_UTILS.MERGECELLS(1,2,3,2);
        PG_EXCEL_UTILS.CELL(1,3,'DATE :');
        PG_EXCEL_UTILS.CELL(2,3,TO_CHAR(P_START_DT, 'DD/MM/YYYY'));

        PG_EXCEL_UTILS.SET_ROW(4
        ,p_fontId => PG_EXCEL_UTILS.get_font( 'Arial',p_bold => true));
        PG_EXCEL_UTILS.CELL(1,4,'No.');
        PG_EXCEL_UTILS.CELL(2,4,'Transaction Type');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(2,30);
        PG_EXCEL_UTILS.CELL(3,4,'Product Code');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(3,40);
        PG_EXCEL_UTILS.CELL(4,4,'Product Name');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(4,20);
        PG_EXCEL_UTILS.CELL(5,4,'Name');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(5,20);
        PG_EXCEL_UTILS.CELL(6,4,'ID Type');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(6,20);
        PG_EXCEL_UTILS.CELL(7,4,'ID No.');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(7,20);
        PG_EXCEL_UTILS.CELL(8,4,'Policy No.');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(8,20);
        PG_EXCEL_UTILS.CELL(9,4,'Effective Date');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(9,20);
        PG_EXCEL_UTILS.CELL(10,4,'Expiry Date');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(10,20);
        PG_EXCEL_UTILS.CELL(11,4,'Text Decription');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(11,20);

        FOR REC IN C_TPA_AAN_MS
          LOOP
          IF (REC.POLICY_VERSION =1 AND REC.ASST_FEE_AMT >0 AND REC.PRODUCT_CONFIG_CODE ='012104')
            OR (REC.POLICY_VERSION >1 AND REC.ASST_FEE_AMT >=0 AND REC.PRODUCT_CONFIG_CODE ='012104')
            OR (REC.POLICY_VERSION >1 AND REC.DMA_FEE_AMT >=0 AND REC.PRODUCT_CONFIG_CODE ='012102')
            OR (REC.POLICY_VERSION =1 AND REC.DMA_FEE_AMT >0 AND REC.PRODUCT_CONFIG_CODE ='012102')
          THEN
            PG_EXCEL_UTILS.CELL(1,rowIDx,seq);
            IF REC.POLICY_VERSION =1 THEN
             PG_EXCEL_UTILS.CELL(2,rowIDx,'PL');
             ELSE
             PG_EXCEL_UTILS.CELL(2,rowIDx,'EN');
            END IF;
            PG_EXCEL_UTILS.CELL(3,rowIDx,REC.PRODUCT_CONFIG_CODE);
            PG_EXCEL_UTILS.CELL(4,rowIDx,REC.PRODUCT_DESC);
            PG_EXCEL_UTILS.CELL(5,rowIDx,REC.NAME_EXT);
            PG_EXCEL_UTILS.CELL(6,rowIDx,REC.ID_TYPE);
            PG_EXCEL_UTILS.CELL(7,rowIDx,REC.ID_VALUE);
            IF REC.POLICY_VERSION =1 THEN
             PG_EXCEL_UTILS.CELL(8,rowIDx,REC.POLICY_REF);
             ELSE
             PG_EXCEL_UTILS.CELL(8,rowIDx,REC.ENDT_NO);
            END IF;
            PG_EXCEL_UTILS.CELL(9,rowIDx,REC.EFF_DATE);
            PG_EXCEL_UTILS.CELL(10,rowIDx,REC.EXP_DATE);
            PG_EXCEL_UTILS.CELL(11,rowIDx,REC.ENDT_NARR);

            rowIDx :=rowIDx+1;
            seq :=seq+1;
            V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.POLICY_VERSION,'AAN'); --1.3
        END IF;
      END LOOP;
            PG_EXCEL_UTILS.save( v_file_dir, FILENAME1 );
    EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                    --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
    END PC_TPA_AAN_MS_POL_ENDT;

    PROCEDURE PC_TPA_AAN_TOWING_POL_ENDT(P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE) IS

     CURSOR C_TPA_AAN_TOWING
      IS
        SELECT OPB.POLICY_REF,UPV.VERSION_NO AS POLICY_VERSION, (CASE  WHEN CP.ID_VALUE1 is not null AND length(ID_VALUE1) =12 THEN  SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
        WHEN CP.ID_VALUE1 is not null THEN CP.ID_VALUE1 else CP.ID_VALUE2  END)  AS ID_VALUE,
        (CASE WHEN ID_TYPE1 IS NULL THEN ID_TYPE2 ELSE ID_TYPE1 END) ID_TYPE,
        UPC.PRODUCT_CONFIG_CODE,(SELECT CODE_DESC FROM CMGE_CODE CC WHERE CC.CODE_CD=UPC.PRODUCT_CONFIG_CODE AND CC.CAT_CODE = UPC.LOB||'_PRODUCT') AS PRODUCT_DESC,
        CP.NAME_EXT,UPB.LONG_NAME,TO_CHAR(UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,TO_CHAR(UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
        TO_CHAR(UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='ASST') AS ASST_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='CRF') AS CRF_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='OCR') AS OCR_FEE_AMT,
        NVL(UPV.ENDT_NARR,' ') AS ENDT_NARR,UPV.ENDT_NO,
        NVL(URV.VEH_NO,' ') AS VEH_NO,
        NVL((select  CMV.VEH_MODEL_DESC from  CMUW_MODEL_VEH CMV where CMV.VEH_MODEL_CODE=URV.VEH_MODEL),' ') AS VEH_MODEL_DESC,
        URV.CONTACT_NAME,URV.PHONE_NO,UPV.CONTRACT_ID --1.3
        ,UCOV.COV_ID
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD --1.3
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
        ON CP.PART_ID=UPB.CP_PART_ID
        AND CP.VERSION=UPB.CP_VERSION
        INNER JOIN UWGE_RISK_VEH URV
        ON URV.CONTRACT_ID =UPV.CONTRACT_ID
        AND URV.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWGE_COVER UCOV
        ON UCOV.CONTRACT_ID =UPV.CONTRACT_ID
        AND UCOV.VERSION_NO =UPV.VERSION_NO
        AND URV.RISK_ID =UCOV.RISK_ID
        AND UCOV.COV_PARENT_ID IS NULL
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_TOWING'),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_TOWING'), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS ='A'
        AND (UPV.ENDT_CODE IS NULL OR UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE_R,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE_R, '[^,]+', 1, level) is not null ))
        AND UPV.ACTION_CODE IN('A','C')
        AND PLPB.TPA_NAME = 'AAN'
        --AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy');
        AND UPCD.DLOAD_STATUS ='P'
        AND UPCD.TPA_NAME='AAN'
        UNION ALL --1.4
        SELECT OPB.POLICY_REF,UPV.VERSION_NO AS POLICY_VERSION, (CASE  WHEN CP.ID_VALUE1 is not null AND length(ID_VALUE1) =12 THEN  SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
        WHEN CP.ID_VALUE1 is not null THEN CP.ID_VALUE1 else CP.ID_VALUE2  END)  AS ID_VALUE,
        (CASE WHEN ID_TYPE1 IS NULL THEN ID_TYPE2 ELSE ID_TYPE1 END) ID_TYPE,
        UPC.PRODUCT_CONFIG_CODE,(SELECT CODE_DESC FROM CMGE_CODE CC WHERE CC.CODE_CD=UPC.PRODUCT_CONFIG_CODE AND CC.CAT_CODE = UPC.LOB||'_PRODUCT') AS PRODUCT_DESC,
        CP.NAME_EXT,UPB.LONG_NAME,TO_CHAR(UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,TO_CHAR(UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
        TO_CHAR(UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='ASST') AS ASST_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='CRF') AS CRF_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='OCR') AS OCR_FEE_AMT,
        NVL(UPV.ENDT_NARR,' ') AS ENDT_NARR,UPV.ENDT_NO,
        NVL(URV.VEH_NO,' ') AS VEH_NO,
        NVL((select  CMV.VEH_MODEL_DESC from  CMUW_MODEL_VEH CMV where CMV.VEH_MODEL_CODE=URV.VEH_MODEL),' ') AS VEH_MODEL_DESC,
        URV.CONTACT_NAME,URV.PHONE_NO,UPV.CONTRACT_ID --1.3
        ,UCOV.COV_ID
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD --1.3
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
        ON CP.PART_ID=UPB.CP_PART_ID
        AND CP.VERSION=UPB.CP_VERSION
        INNER JOIN UWGE_RISK_VEH URV
        ON URV.CONTRACT_ID =UPV.CONTRACT_ID
        AND URV.VERSION_NO  =(SELECT MAX (b.version_no)
        FROM UWGE_RISK_VEH b
        WHERE b.contract_id = UPV.CONTRACT_ID
        AND URV.object_id = b.object_id
        AND b.version_no <= UPV.VERSION_NO
        AND b.reversing_version IS NULL)
        AND URV.action_code <> 'D'
        INNER JOIN UWGE_COVER UCOV
        ON UCOV.CONTRACT_ID =UPV.CONTRACT_ID
        AND UCOV.VERSION_NO =UPV.VERSION_NO
        AND URV.RISK_ID =UCOV.RISK_ID
        AND UCOV.COV_PARENT_ID IS NULL
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_TOWING'),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_TOWING'), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS ='A'
        AND UPV.VERSION_NO >1
        AND (UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE_A,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE_A, '[^,]+', 1, level) is not null ))
        AND UPV.ACTION_CODE IN('A','C')
        AND PLPB.TPA_NAME = 'AAN'
        --AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy');
        AND UPCD.DLOAD_STATUS ='P'
        AND UPCD.TPA_NAME='AAN';

    CURSOR C_TPA_AAN_MOTOR_TOWING
      IS
        SELECT (CASE WHEN UPV.VERSION_NO=1 THEN 'PL' else 'EN' END) AS TRANSACTION_TYPE,UPC.PRODUCT_CONFIG_CODE,
        (SELECT CODE_DESC FROM CMGE_CODE CC WHERE CC.CODE_CD=UPC.PRODUCT_CONFIG_CODE AND CC.CAT_CODE = UPC.LOB||'_PRODUCT') AS PRODUCT_DESC,
        (CASE WHEN UPB.LONG_NAME IS NOT NULL THEN UPB.LONG_NAME ELSE CP.NAME_EXT END) AS LONG_NAME ,URV.CONTACT_NAME,URV.PHONE_NO,TO_CHAR(UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,TO_CHAR(UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
        NVL((select  CMV.VEH_MODEL_DESC from  CMUW_MODEL_VEH CMV where CMV.VEH_MODEL_CODE=URV.VEH_MODEL),' ') AS VEH_MODEL_DESC,
        NVL(URV.VEH_NO,' ') AS VEH_NO,
        (CASE WHEN ID_TYPE1 IS NULL THEN ID_TYPE2 ELSE ID_TYPE1 END) ID_TYPE,
        OPB.POLICY_REF,UPV.VERSION_NO AS POLICY_VERSION, (CASE  WHEN CP.ID_VALUE1 is not null AND length(ID_VALUE1) =12 THEN  SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
        WHEN CP.ID_VALUE1 is not null THEN CP.ID_VALUE1 else CP.ID_VALUE2  END) AS ID_VALUE,
        (CASE WHEN UPV.VERSION_NO=1 THEN OPB.POLICY_REF else UPV.ENDT_NO END) AS Policy_No,
        NVL(UPV.ENDT_NARR,' ') AS ENDT_NARR,UPM.DEALER3A_FEE
        ,UPV.CONTRACT_ID --1.3
        ,UCOV.COV_ID
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD --1.3
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWGE_POLICY_MT UPM
        ON UPM.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPM.VERSION_NO =UPV.VERSION_NO
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
        ON CP.PART_ID=UPB.CP_PART_ID
        AND CP.VERSION=UPB.CP_VERSION
        INNER JOIN UWGE_RISK_VEH URV
        ON URV.CONTRACT_ID =UPV.CONTRACT_ID
        AND URV.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWGE_COVER UCOV
        ON UCOV.CONTRACT_ID =UPV.CONTRACT_ID
        AND UCOV.VERSION_NO =UPV.VERSION_NO
        AND URV.RISK_ID =UCOV.RISK_ID
        AND UCOV.COV_PARENT_ID IS NULL
        --- 2.1 start
                INNER JOIN
               ( Select * from
                  (select UPM.Contract_ID, UPM.Version_no, UPC.Product_Config_Code, UPC.Lob, UPC.Policy_Status, UPM.DEALER3A_FEE, '0' as Benz_ind 
                         from UWGE_POLICY_CONTRACTS UPC,       UWGE_POLICY_MT UPM
                    Where UPC.CONTRACT_ID = UPM.CONTRACT_ID
                      AND (UPC.PRODUCT_CONFIG_CODE in( 
                            select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_TOWING'),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_TOWING'), '[^,]+', 1, level) is not null)
                      AND UPM.DEALER_PROVIDER = 'AAN')
                    Union
                   select UPM.Contract_ID, UPM.Version_no, UPC.Product_Config_Code, UPC.Lob, UPC.Policy_Status, UPM.DEALER3A_FEE, '1' as Benz_ind 
                         from UWGE_POLICY_CONTRACTS UPC, UWGE_POLICY_MT UPM
                    Where UPC.CONTRACT_ID = UPM.CONTRACT_ID
                      AND UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'MERCEDES_BENZ_PRODUCTS'),'[^,]+', 1, level)
                                                       from dual
                                                       connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'MERCEDES_BENZ_PRODUCTS'), '[^,]+', 1, level) is not null)
                      --AND CMV1.VEH_MODEL_CODE=URV.VEH_MODEL)
                      AND (UPM.DEALER_PROVIDER = 'AAN' OR UPM.DEALER_PROVIDER IS NULL)
                  ))  UPC_UPM
         ON UPC_UPM.Contract_ID = UPV.Contract_ID AND UPC_UPM.Version_no = UPV.Version_No
        WHERE 1=1

        AND UPC.POLICY_STATUS ='A'
        --AND UPM.DEALER_PROVIDER = 'AAN' -- 2.1 -- moved this condition to up (other than MERCEDES)
        --2.1 end
        AND (UPV.ENDT_CODE IS NULL OR UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE_R,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE_R, '[^,]+', 1, level) is not null ))
        AND UPV.ACTION_CODE IN('A','C')
        --AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy');
        AND UPCD.DLOAD_STATUS ='P'
        AND UPCD.TPA_NAME='AAN'
        --- 2.1 start
        AND ((UPC_UPM.Benz_ind = '1'
         AND URV.VEH_MODEL IN (SELECT CMV1.VEH_MODEL_CODE FROM CMUW_MODEL_VEH CMV1 WHERE UPPER(CMV1.VEH_MODEL_DESC) LIKE '%MERCEDES%' AND CMV1.VEH_MODEL_CODE=URV.VEH_MODEL)
         AND UPB.AGENT_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'MERCEDES_BENZ_AGENTS'),'[^,]+', 1, level) from dual
                               connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'MERCEDES_BENZ_AGENTS'), '[^,]+', 1, level) is not null)
         )
         OR
         UPC_UPM.Benz_ind = '0'
        )--- 2.1 end

        UNION ALL --1.4
        SELECT (CASE WHEN UPV.VERSION_NO=1 THEN 'PL' else 'EN' END) AS TRANSACTION_TYPE,UPC.PRODUCT_CONFIG_CODE,
        (SELECT CODE_DESC FROM CMGE_CODE CC WHERE CC.CODE_CD=UPC.PRODUCT_CONFIG_CODE AND CC.CAT_CODE = UPC.LOB||'_PRODUCT') AS PRODUCT_DESC,
        (CASE WHEN UPB.LONG_NAME IS NOT NULL THEN UPB.LONG_NAME ELSE CP.NAME_EXT END) AS LONG_NAME ,URV.CONTACT_NAME,URV.PHONE_NO,TO_CHAR(UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,TO_CHAR(UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
        NVL((select  CMV.VEH_MODEL_DESC from  CMUW_MODEL_VEH CMV where CMV.VEH_MODEL_CODE=URV.VEH_MODEL),' ') AS VEH_MODEL_DESC,
        NVL(URV.VEH_NO,' ') AS VEH_NO,
        (CASE WHEN ID_TYPE1 IS NULL THEN ID_TYPE2 ELSE ID_TYPE1 END) ID_TYPE,
        OPB.POLICY_REF,UPV.VERSION_NO AS POLICY_VERSION, (CASE  WHEN CP.ID_VALUE1 is not null AND length(ID_VALUE1) =12 THEN  SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
        WHEN CP.ID_VALUE1 is not null THEN CP.ID_VALUE1 else CP.ID_VALUE2  END) AS ID_VALUE,
        (CASE WHEN UPV.VERSION_NO=1 THEN OPB.POLICY_REF else UPV.ENDT_NO END) AS Policy_No,
        NVL(UPV.ENDT_NARR,' ') AS ENDT_NARR,UPM.DEALER3A_FEE
        ,UPV.CONTRACT_ID --1.3
        ,UCOV.COV_ID
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD --1.3
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWGE_POLICY_MT UPM
        ON UPM.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPM.VERSION_NO =UPV.VERSION_NO
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
        ON CP.PART_ID=UPB.CP_PART_ID
        AND CP.VERSION=UPB.CP_VERSION
        INNER JOIN UWGE_RISK_VEH URV
        ON URV.CONTRACT_ID =UPV.CONTRACT_ID
        AND URV.VERSION_NO =(SELECT MAX (b.version_no)
        FROM UWGE_RISK_VEH b
        WHERE b.contract_id = UPV.CONTRACT_ID
        AND URV.object_id = b.object_id
        AND b.version_no <= UPV.VERSION_NO
        AND b.reversing_version IS NULL)
        AND URV.action_code <> 'D'
        INNER JOIN UWGE_COVER UCOV
        ON UCOV.CONTRACT_ID =UPV.CONTRACT_ID
        AND UCOV.VERSION_NO =UPV.VERSION_NO
        AND URV.RISK_ID =UCOV.RISK_ID
        AND UCOV.COV_PARENT_ID IS NULL
        --2.1 start
                INNER JOIN
               ( Select * from
                  (select UPM.Contract_ID, UPM.Version_no, UPC.Product_Config_Code, UPC.Lob, UPC.Policy_Status, UPM.DEALER3A_FEE, '0' as Benz_ind 
                         from UWGE_POLICY_CONTRACTS UPC, UWGE_POLICY_MT UPM
                    Where UPC.CONTRACT_ID = UPM.CONTRACT_ID
                      AND UPC.PRODUCT_CONFIG_CODE in(  ---1.11 start
                            select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_TOWING'),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_TOWING'), '[^,]+', 1, level) is not null)
                      AND UPM.DEALER_PROVIDER = 'AAN'
        AND UPC.POLICY_STATUS ='A'
                    Union
                   select UPM.Contract_ID, UPM.Version_no, UPC.Product_Config_Code, UPC.Lob,UPC.Policy_Status, UPM.DEALER3A_FEE, '1' as Benz_ind 
                        from UWGE_POLICY_CONTRACTS UPC, UWGE_POLICY_MT UPM
                    Where UPC.CONTRACT_ID = UPM.CONTRACT_ID
                      AND UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'MERCEDES_BENZ_PRODUCTS'),'[^,]+', 1, level)
                                                       from dual
                                                       connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'MERCEDES_BENZ_PRODUCTS'), '[^,]+', 1, level) is not null)
                      AND (UPM.DEALER_PROVIDER = 'AAN' OR UPM.DEALER_PROVIDER IS NULL)
                      AND UPC.POLICY_STATUS IN('A','C','E')
                  ))  UPC_UPM
         ON UPC_UPM.Contract_ID = UPV.Contract_ID AND UPC_UPM.Version_no = UPV.Version_No
        WHERE 1=1         

        --AND UPC.POLICY_STATUS ='A' -- moved this condition to up (other than MERCEDES)
        --AND UPM.DEALER_PROVIDER = 'AAN' -- moved this condition to up (other than MERCEDES)
         ---2.1 end

    AND UPV.VERSION_NO >1
        AND UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE_A,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE_A, '[^,]+', 1, level) is not null )
        AND UPV.ACTION_CODE IN('A','C')
        --AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy');
        AND UPCD.DLOAD_STATUS ='P'
        AND UPCD.TPA_NAME='AAN' -- 2.1 start ; --1.3
            AND ((UPC_UPM.Benz_ind = '1'
          AND URV.VEH_MODEL IN (SELECT CMV1.VEH_MODEL_CODE FROM CMUW_MODEL_VEH CMV1 WHERE UPPER(CMV1.VEH_MODEL_DESC) LIKE '%MERCEDES%' AND CMV1.VEH_MODEL_CODE=URV.VEH_MODEL)
          AND UPB.AGENT_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'MERCEDES_BENZ_AGENTS'),'[^,]+', 1, level) from dual
                                connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'MERCEDES_BENZ_AGENTS'), '[^,]+', 1, level) is not null)
          )
          OR
          UPC_UPM.Benz_ind = '0'
        );-- 2.1 end


        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_AAN_TOWING_POL_ENDT';
        FILENAME  UTL_FILE.FILE_TYPE;
        FILENAME1 VARCHAR2(1000);
        v_file_dir VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_AAN_DIR');
        V_OCR VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'OCR_TOWING');
        V_CRF VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'CRF_TOWING');
        V_NPOL VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'POL_TOWING');
         rowIDx number := 5;
          seq number := 1;
        V_RET                 NUMBER := 0; --1.3
        V_UWPL_COVER_DET   PG_TPA_UTILS.UWPL_COVER_DET;

    BEGIN
            V_STEPS := '001';
             FILENAME1   := TO_CHAR(P_START_DT, 'YYYYMMDD')||'_TOWINGASST_POLEND.xlsx';

         PG_EXCEL_UTILS.clear_workbook;
        PG_EXCEL_UTILS.new_sheet;
            PG_EXCEL_UTILS.CELL(1,1,'BORDEREAUX (POLICY &'||' ENDORSEMENT)');

        PG_EXCEL_UTILS.MERGECELLS(1,1,3,1);
        PG_EXCEL_UTILS.CELL(1,2,'FROM : ALLIANZ GENERAL INSURANCE COMPANY (MALAYSIA) BERHAD');
        PG_EXCEL_UTILS.MERGECELLS(1,2,3,2);
        PG_EXCEL_UTILS.CELL(1,3,'DATE :');
        PG_EXCEL_UTILS.CELL(2,3,TO_CHAR(P_START_DT,'DD/MM/YYYY'));

        PG_EXCEL_UTILS.SET_ROW(4
        ,p_fontId => PG_EXCEL_UTILS.get_font( 'Arial',p_bold => true));
        PG_EXCEL_UTILS.CELL(1,4,'No.');
        PG_EXCEL_UTILS.CELL(2,4,'Transaction Type');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(2,20);
        PG_EXCEL_UTILS.CELL(3,4,'Product Code');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(3,40);
        PG_EXCEL_UTILS.CELL(4,4,'Product Name');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(4,20);
        PG_EXCEL_UTILS.CELL(5,4,'Name');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(5,20);
        PG_EXCEL_UTILS.CELL(6,4,'Emergency Contact Name and Number');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(6,40);
        PG_EXCEL_UTILS.CELL(7,4,'Model');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(7,20);
        PG_EXCEL_UTILS.CELL(8,4,'Registration No.');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(8,20);
        PG_EXCEL_UTILS.CELL(9,4,'Effective Date');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(9,20);
        PG_EXCEL_UTILS.CELL(10,4,'Expiry Date');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(10,20);
        PG_EXCEL_UTILS.CELL(11,4,'ID Type');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(11,20);
        PG_EXCEL_UTILS.CELL(12,4,'ID No.');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(12,20);
        PG_EXCEL_UTILS.CELL(13,4,'Policy No.');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(13,20);
        PG_EXCEL_UTILS.CELL(14,4,'Plan');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(14,20);
        PG_EXCEL_UTILS.CELL(15,4,'Memo Narration');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(15,20);
        PG_EXCEL_UTILS.CELL(16,4,'Text Decription');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(16,20);

        FOR REC IN C_TPA_AAN_TOWING
          LOOP
          IF(REC.POLICY_VERSION =1 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_NPOL,REC.PRODUCT_CONFIG_CODE) = 'N')
          THEN
          IF (REC.POLICY_VERSION =1 AND REC.ASST_FEE_AMT >0 )
            OR (REC.POLICY_VERSION >1 AND REC.ASST_FEE_AMT >=0 )
            OR (REC.POLICY_VERSION =1 AND REC.OCR_FEE_AMT >0  AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_OCR,REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (REC.POLICY_VERSION >1 AND REC.OCR_FEE_AMT >=0 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_OCR,REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (REC.POLICY_VERSION =1 AND REC.CRF_FEE_AMT >0  AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_CRF,REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (REC.POLICY_VERSION >1 AND REC.CRF_FEE_AMT >=0 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_CRF,REC.PRODUCT_CONFIG_CODE) = 'Y')
          THEN
            PG_EXCEL_UTILS.CELL(1,rowIDx,seq);
            IF REC.POLICY_VERSION =1 THEN
             PG_EXCEL_UTILS.CELL(2,rowIDx,'PL');
             ELSE
             PG_EXCEL_UTILS.CELL(2,rowIDx,'EN');
            END IF;
            PG_EXCEL_UTILS.CELL(3,rowIDx,REC.PRODUCT_CONFIG_CODE);
            PG_EXCEL_UTILS.CELL(4,rowIDx,REC.PRODUCT_DESC);
            PG_EXCEL_UTILS.CELL(5,rowIDx,REC.LONG_NAME);
            PG_EXCEL_UTILS.CELL(6,rowIDx,NVL(REC.CONTACT_NAME,' ')||' '||NVL(REC.PHONE_NO,' '));
            PG_EXCEL_UTILS.CELL(7,rowIDx,REC.VEH_MODEL_DESC);
            PG_EXCEL_UTILS.CELL(8,rowIDx,REC.VEH_NO);
            PG_EXCEL_UTILS.CELL(9,rowIDx,REC.EFF_DATE);
            PG_EXCEL_UTILS.CELL(10,rowIDx,REC.EXP_DATE);
            PG_EXCEL_UTILS.CELL(11,rowIDx,REC.ID_TYPE);
            PG_EXCEL_UTILS.CELL(12,rowIDx,REC.ID_VALUE);
            IF REC.POLICY_VERSION =1 THEN
             PG_EXCEL_UTILS.CELL(13,rowIDx,REC.POLICY_REF);
             ELSE
             PG_EXCEL_UTILS.CELL(13,rowIDx,REC.ENDT_NO);
            END IF;
            V_UWPL_COVER_DET := PG_TPA_UTILS.FN_GET_UWPL_COVER_DET(REC.CONTRACT_ID,REC.POLICY_VERSION,REC.COV_ID);
            PG_EXCEL_UTILS.CELL(14,rowIDx,NVL(V_UWPL_COVER_DET.PLAN_CODE,' '));
            PG_EXCEL_UTILS.CELL(15,rowIDx,' ');
            PG_EXCEL_UTILS.CELL(16,rowIDx,REC.ENDT_NARR);

            rowIDx :=rowIDx+1;
            seq :=seq+1;
            V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.POLICY_VERSION,'AAN'); --1.3
          END IF;
        END IF;
      END LOOP;
      FOR RECC IN C_TPA_AAN_MOTOR_TOWING
          LOOP
          IF(RECC.POLICY_VERSION =1 AND RECC.DEALER3A_FEE >0) OR  (RECC.POLICY_VERSION >1 AND RECC.DEALER3A_FEE >=0)
          THEN
            PG_EXCEL_UTILS.CELL(1,rowIDx,seq);
            IF RECC.POLICY_VERSION =1 THEN
             PG_EXCEL_UTILS.CELL(2,rowIDx,'PL');
             ELSE
             PG_EXCEL_UTILS.CELL(2,rowIDx,'EN');
            END IF;
            PG_EXCEL_UTILS.CELL(3,rowIDx,RECC.PRODUCT_CONFIG_CODE);
            PG_EXCEL_UTILS.CELL(4,rowIDx,RECC.PRODUCT_DESC);
            PG_EXCEL_UTILS.CELL(5,rowIDx,RECC.LONG_NAME);
            PG_EXCEL_UTILS.CELL(6,rowIDx,NVL(RECC.CONTACT_NAME,' ')||' '||NVL(RECC.PHONE_NO,' '));
            PG_EXCEL_UTILS.CELL(7,rowIDx,RECC.VEH_MODEL_DESC);
            PG_EXCEL_UTILS.CELL(8,rowIDx,RECC.VEH_NO);
            PG_EXCEL_UTILS.CELL(9,rowIDx,RECC.EFF_DATE);
            PG_EXCEL_UTILS.CELL(10,rowIDx,RECC.EXP_DATE);
            PG_EXCEL_UTILS.CELL(11,rowIDx,RECC.ID_TYPE);
            PG_EXCEL_UTILS.CELL(12,rowIDx,RECC.ID_VALUE);
             PG_EXCEL_UTILS.CELL(13,rowIDx,RECC.POLICY_REF);
             V_UWPL_COVER_DET := PG_TPA_UTILS.FN_GET_UWPL_COVER_DET(RECC.CONTRACT_ID,RECC.POLICY_VERSION,RECC.COV_ID);
            PG_EXCEL_UTILS.CELL(14,rowIDx,NVL(V_UWPL_COVER_DET.PLAN_CODE,' '));
            PG_EXCEL_UTILS.CELL(15,rowIDx,' ');
            PG_EXCEL_UTILS.CELL(16,rowIDx,RECC.ENDT_NARR);

            rowIDx :=rowIDx+1;
            seq :=seq+1;
            V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(RECC.CONTRACT_ID,RECC.POLICY_VERSION,'AAN'); --1.3
        END IF;
      END LOOP;
            PG_EXCEL_UTILS.save( v_file_dir, FILENAME1 );
    EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                    --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
    END PC_TPA_AAN_TOWING_POL_ENDT;


    PROCEDURE PC_TPA_AAN_HC_PA_POL_ENDT(P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE) IS

     CURSOR C_TPA_AAN_PA
      IS
        SELECT TO_CHAR(UPV.ENDT_EFF_DATE, 'DD/MM/YYYY') AS ENDT_EFF_DATE,OPB.POLICY_REF,UPB.PREV_POL_NO,UPV.ENDT_CODE,UPV.VERSION_NO AS POLICY_VERSION,
        UPB.AGENT_CODE,DVA.NAME AS AGENT_NAME,(CASE WHEN upb.PREV_POL_NO IS NOT NULL THEN upb.PREV_POL_NO else upb.PREV_POL_NO_IIMS END) as prev_pol,
        (CASE  WHEN CP.ID_VALUE1 is null THEN  CP.ID_VALUE2 else CP.ID_VALUE1 END) AS P_NRIC_OTH,
        (CASE WHEN CP.ID_TYPE1 = 'NRIC' THEN CP.ID_VALUE1 WHEN CP.ID_TYPE2 = 'NRIC' THEN CP.ID_VALUE2 END) AS P_NRIC,
        regexp_replace((CASE  WHEN CP.MOBILE_NO1 is not null and CP.MOBILE_CODE1 is not null THEN CP.MOBILE_CODE1||CP.MOBILE_NO1   else CP.MOBILE_CODE2||CP.MOBILE_NO2 END),'[^0-9]') AS PhoneNumber,REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
        REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,
        REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3,
        CPA.POSTCODE,
        (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'CITY'
        AND CODE_CD = CPA.CITY) AS CITY,
        (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'STATE'
        AND CODE_CD = CPA.STATE) AS STATE,
        UPC.PRODUCT_CONFIG_CODE,
        CP.NAME_EXT,UPB.LONG_NAME,TO_CHAR(UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,TO_CHAR(UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
        TO_CHAR(UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='ASST') AS ASST_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='MCO') AS MCO_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='MCOO') AS MCOO_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='MCOI') AS MCOI_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='IMA') AS IMA_FEE_AMT,
        NVL(UPV.ENDT_NARR,' ') AS ENDT_NARR,UPV.ENDT_NO,
        to_char(sysdate,'DD/MM/YYYY')as DateReceivedbyAAN,UPB.ISSUE_OFFICE,
        (SELECT BRANCH_NAME FROM CMDM_BRANCH WHERE BRANCH_CODE = UPB.ISSUE_OFFICE)AS BRANCH_DESC,
        (select EXP_DATE from uwge_policy_bases where CONTRACT_ID =(select CONTRACT_ID from OCP_POLICY_BASES where policy_ref=(CASE WHEN upb.PREV_POL_NO IS NOT NULL THEN upb.PREV_POL_NO else upb.PREV_POL_NO_IIMS END) AND ROWNUM = 1) and uwge_policy_bases.TOP_INDICATOR='Y' AND ROWNUM = 1) as prev_exp_date,
        OPB.CONTRACT_ID
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD --1.3
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
        ON CP.PART_ID=UPB.CP_PART_ID
        AND CP.VERSION=UPB.CP_VERSION
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (UPB.CP_ADDR_ID,UPB.CP_ADDR_VERSION)) CPA
        ON CPA.ADD_ID = UPB.CP_ADDR_ID
        AND CPA.VERSION = UPB.CP_ADDR_VERSION
        INNER JOIN DMAG_VI_AGENT DVA
        ON DVA.AGENTCODE =UPB.AGENT_CODE
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_PA'),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_PA'), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS IN('A','C','E')
        AND PLPB.TPA_NAME = 'AAN'
        AND UPV.ACTION_CODE IN('A','C')
        AND (UPV.ENDT_CODE IS NULL OR UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE, '[^,]+', 1, level) is not null )) --1.4
        --AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy')
        AND UPCD.DLOAD_STATUS ='P' --1.3
        AND UPCD.TPA_NAME='AAN'
        ORDER BY  OPB.policy_ref ASC, UPV.VERSION_NO ASC;


      CURSOR C_TPA_AAN_HC
      IS
          SELECT TO_CHAR(UPV.ENDT_EFF_DATE, 'DD/MM/YYYY') AS ENDT_EFF_DATE,OPB.POLICY_REF,UPB.PREV_POL_NO,UPV.ENDT_CODE,UPV.VERSION_NO AS POLICY_VERSION,UPB.AGENT_CODE,DVA.NAME AS AGENT_NAME,(CASE WHEN upb.PREV_POL_NO IS NOT NULL THEN upb.PREV_POL_NO else upb.PREV_POL_NO_IIMS END) as prev_pol,
        (CASE  WHEN CP.ID_VALUE1 is null THEN  CP.ID_VALUE2 else CP.ID_VALUE1 END) AS P_NRIC_OTH,
        (CASE WHEN CP.ID_TYPE1 = 'NRIC' THEN CP.ID_VALUE1 WHEN CP.ID_TYPE2 = 'NRIC' THEN CP.ID_VALUE2 END) AS P_NRIC,
        regexp_replace((CASE  WHEN CP.MOBILE_NO1 is not null and CP.MOBILE_CODE1 is not null THEN CP.MOBILE_CODE1||CP.MOBILE_NO1   else CP.MOBILE_CODE2||CP.MOBILE_NO2 END),'[^0-9]') AS PhoneNumber,REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
        REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,
        REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3,
        CPA.POSTCODE,
        (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'CITY'
        AND CODE_CD = CPA.CITY) AS CITY,
        (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'STATE'
        AND CODE_CD = CPA.STATE) AS STATE,
        UPC.PRODUCT_CONFIG_CODE,
        CP.NAME_EXT,UPB.LONG_NAME,TO_CHAR(UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,TO_CHAR(UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
        TO_CHAR(UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='MCOO') AS MCOO_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='MCOI') AS MCOI_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='MCO') AS MCO_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='IMA') AS IMA_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='MCODMA') AS DMA_FEE_AMT,
        NVL(UPV.ENDT_NARR,' ') AS ENDT_NARR,UPV.ENDT_NO,
        to_char(sysdate,'DD/MM/YYYY')as DateReceivedbyAAN,
        (SELECT BRANCH_NAME FROM CMDM_BRANCH WHERE BRANCH_CODE = UPB.ISSUE_OFFICE)AS BRANCH_DESC,
        (select EXP_DATE from uwge_policy_bases where CONTRACT_ID =(select CONTRACT_ID from OCP_POLICY_BASES where policy_ref=(CASE WHEN upb.PREV_POL_NO IS NOT NULL THEN upb.PREV_POL_NO else upb.PREV_POL_NO_IIMS END) AND ROWNUM = 1) and uwge_policy_bases.TOP_INDICATOR='Y' AND ROWNUM = 1) as prev_exp_date,
        OPB.CONTRACT_ID
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD --1.3
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
        ON CP.PART_ID=UPB.CP_PART_ID
        AND CP.VERSION=UPB.CP_VERSION
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (UPB.CP_ADDR_ID,UPB.CP_ADDR_VERSION)) CPA
        ON CPA.ADD_ID = UPB.CP_ADDR_ID
        AND CPA.VERSION = UPB.CP_ADDR_VERSION
        INNER JOIN DMAG_VI_AGENT DVA
        ON DVA.AGENTCODE =UPB.AGENT_CODE
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_HC'),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_HC'), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS IN('A','C','E')
        AND UPV.ACTION_CODE IN('A','C')
        AND (UPV.ENDT_CODE IS NULL OR UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE, '[^,]+', 1, level) is not null )) --1.4
        AND (PLPB.TPA_NAME = 'AAN' OR PLPB.TPA_NAME IS NULL)
        --AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy')
        AND UPCD.DLOAD_STATUS ='P' --1.3
        AND UPCD.TPA_NAME='AAN'
        ORDER BY  OPB.policy_ref ASC, UPV.VERSION_NO ASC ;

        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_AAN_HC_PA_POL_ENDT';
        FILENAME  UTL_FILE.FILE_TYPE;
        FILENAME1 VARCHAR2(1000);
        v_file_dir VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_AAN_DIR');
        V_ASST VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'PA_ASST');
        V_IMA VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'PA_IMA');
        V_MCO VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'PA_MCO');
        V_NPOL VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'POL_PA');
        V_HC_MCOI VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'HC_MCOI');
        V_HC_MCOO VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'HC_MCOO');
        V_HC_DMA VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'HC_DMA');

        V_RISK_LEVEL_DTLS VARCHAR2(100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'RISK_LEVEL_DTLS'); --116958_ALLIANZ SHIELD PLUS
        V_IMA_LMT_2M VARCHAR2(100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'IMA_LMT_2M');--116958_ALLIANZ SHIELD PLUS

        V_PRINCIPAL_DET   PG_TPA_UTILS.RISK_PERSON_PARTNERS_ALL_DET;--1.1
        V_UWPL_COVER_DET   PG_TPA_UTILS.UWPL_COVER_DET;
         rowIDx number := 5;
          seq number := 1;
        v_NGV                            NUMBER (18, 2);
        V_RET                 NUMBER := 0; --1.3
        --1.4
        --116958_ALLIANZ SHIELD PLUS start
        V_SELECTED_RISK_SQL VARCHAR2(4000):='SELECT (CASE  WHEN RCP.ID_VALUE1 is null THEN  RCP.ID_VALUE2 else RCP.ID_VALUE1 END) AS NRIC_OTH,
        (CASE WHEN RCP.ID_TYPE1 = ''NRIC'' THEN RCP.ID_VALUE1 WHEN RCP.ID_TYPE2 = ''NRIC'' THEN RCP.ID_VALUE2 END) AS NRIC, RCP.NAME_EXT AS MEMBER_FULL_NAME,
        RCP.DATE_OF_BIRTH AS DATE_OF_BIRTH,RCP.SEX,(CASE WHEN RCP.marital_status=''0'' THEN ''S'' WHEN RCP.marital_status=''1'' THEN ''M'' WHEN RCP.marital_status=''2'' THEN ''D'' END) AS MARITAL_STATUS,
        URP.INSURED_TYPE,URP.EMPLOYEE_ID,(CASE WHEN URP.INSURED_TYPE=''P'' THEN ''P'' ELSE (CASE WHEN URP.RELATIONSHIP IN(''03'',''072'') THEN ''H'' WHEN URP.RELATIONSHIP IN(''02'',''107'') THEN ''W'' WHEN URP.RELATIONSHIP IN(''05'',''019'') then ''D'' WHEN URP.RELATIONSHIP IN(''04'',''087'') then ''S'' ELSE '''' END)END) AS RELATIONSHIP,URP.TEMINATE_DATE,
        UR.EFF_DATE AS RISK_EFF_DATE,UR.EXP_DATE AS RISK_EXP_DATE,URP.JOIN_DATE AS ORIGINAL_JOIN_DATE,
        (case when URP.INSURED_TYPE=''D'' then (select a.COV_SEQ_REF from uwge_cover a where UCOV.CONTRACT_ID =A.CONTRACT_ID
        AND UCOV.VERSION_NO=a.VERSION_NO AND  a.RISK_ID=UR.RISK_PARENT_ID and COV_PARENT_ID is null and rownum=1) else '''' end) as Parent_cov_seq_no
        ,UCOV.COV_ID,UCOV.COV_SEQ_REF,UR.RISK_ID ,UR.RISK_PARENT_ID
        ,(SELECT COUNT(*) FROM UWGE_COVER CSUB WHERE UCOV.CONTRACT_ID =CSUB.CONTRACT_ID
        AND UCOV.VERSION_NO=CSUB.VERSION_NO AND UCOV.COV_ID =CSUB.COV_PARENT_ID
        AND CSUB.COV_CODE IN (''OP'',''OP1'',''OP2'') ) AS OP_SUB_COV,
        (SELECT NVL(F.FEE_AMT,0)  FROM UWGE_COVER_FEES F WHERE F.CONTRACT_ID = UR.CONTRACT_ID AND F.COV_ID=UCOV.COV_ID AND TOP_INDICATOR=''Y''
         AND F.FEE_CODE=''MCO'' ) AS MCO_FEE,                          
        (SELECT NVL(F.FEE_AMT,0) FROM UWGE_COVER_FEES F WHERE F.CONTRACT_ID = UR.CONTRACT_ID AND F.COV_ID=                 
                   (SELECT CV.COV_ID  FROM UWGE_COVER CV WHERE CV.COV_PARENT_ID = UCOV.COV_ID and TOP_INDICATOR=''Y''  AND COV_CODE=''IMA'' AND ROWNUM=1  )
                    AND TOP_INDICATOR=''Y'' AND F.FEE_CODE=''IMA''  AND ROWNUM =1) AS IMA_FEE,
        NULL AS IMPORT_TYPE,
        NULL AS PREV_POL_OP_IND,
        NULL AS DEPARTMENT
        from UWGE_RISK UR
        INNER JOIN UWPL_RISK_PERSON URP
        ON URP.CONTRACT_ID =UR.CONTRACT_ID
        AND UR.RISK_ID =URP.RISK_ID
        AND URP.VERSION_NO = UR.VERSION_NO
        AND URP.action_code <> ''D''
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(URP.RISK_PART_ID, URP.RISK_PART_VER)) RCP
        ON RCP.PART_ID=URP.RISK_PART_ID
        AND RCP.VERSION=URP.RISK_PART_VER
        INNER JOIN UWGE_COVER UCOV
        ON UCOV.CONTRACT_ID =UR.CONTRACT_ID
        AND UR.RISK_ID =UCOV.RISK_ID
        AND UCOV.VERSION_NO =UR.VERSION_NO
        AND UCOV.COV_PARENT_ID IS NULL
        WHERE UR.CONTRACT_ID  = :BIND_CONTRACT_ID
        AND UR.version_no =  :BIND_VERSION_NO
        AND UR.action_code <> ''D''
        ORDER BY  to_number(UCOV.cov_seq_ref)';
        V_ALL_RISK_SQL VARCHAR2(4000) :='SELECT (CASE  WHEN RCP.ID_VALUE1 is null THEN  RCP.ID_VALUE2 else RCP.ID_VALUE1 END) AS NRIC_OTH,
                  (CASE WHEN RCP.ID_TYPE1 = ''NRIC'' THEN RCP.ID_VALUE1 WHEN RCP.ID_TYPE2 = ''NRIC'' THEN RCP.ID_VALUE2 END) AS NRIC, RCP.NAME_EXT AS MEMBER_FULL_NAME,
                  RCP.DATE_OF_BIRTH AS DATE_OF_BIRTH,RCP.SEX,(CASE WHEN RCP.marital_status=''0'' THEN ''S'' WHEN RCP.marital_status=''1'' THEN ''M'' WHEN RCP.marital_status=''2'' THEN ''D'' END) AS MARITAL_STATUS,
                  URP.INSURED_TYPE,URP.EMPLOYEE_ID,(CASE WHEN URP.INSURED_TYPE=''P'' THEN ''P'' ELSE (CASE WHEN URP.RELATIONSHIP IN(''03'',''072'') THEN ''H'' WHEN URP.RELATIONSHIP IN(''02'',''107'') THEN ''W'' WHEN URP.RELATIONSHIP IN(''05'',''019'') then ''D'' WHEN URP.RELATIONSHIP IN(''04'',''087'') then ''S'' ELSE '''' END)END) AS RELATIONSHIP,URP.TEMINATE_DATE,
                  UR.EFF_DATE AS RISK_EFF_DATE,UR.EXP_DATE AS RISK_EXP_DATE,URP.JOIN_DATE AS ORIGINAL_JOIN_DATE,
                  (case when URP.INSURED_TYPE=''D'' then (select a.COV_SEQ_REF from uwge_cover a where UCOV.CONTRACT_ID =A.CONTRACT_ID
                  AND UCOV.VERSION_NO=a.VERSION_NO AND a.RISK_ID=UR.RISK_PARENT_ID and COV_PARENT_ID is null and rownum=1) else '''' end) as Parent_cov_seq_no
                  ,UCOV.COV_ID,UCOV.COV_SEQ_REF,UR.RISK_ID ,UR.RISK_PARENT_ID
                  ,(SELECT COUNT(*) FROM UWGE_COVER CSUB WHERE UCOV.CONTRACT_ID =CSUB.CONTRACT_ID
                  AND UCOV.VERSION_NO=CSUB.VERSION_NO AND UCOV.COV_ID =CSUB.COV_PARENT_ID
                  AND CSUB.COV_CODE IN (''OP'',''OP1'',''OP2'') ) AS OP_SUB_COV ,              
                 (SELECT NVL(F.FEE_AMT,0)  FROM UWGE_COVER_FEES F WHERE F.CONTRACT_ID = UR.CONTRACT_ID AND F.COV_ID=UCOV.COV_ID AND TOP_INDICATOR=''Y''
                  AND F.FEE_CODE=''MCO'' ) AS MCO_FEE,                          
                 (SELECT NVL(F.FEE_AMT,0) FROM UWGE_COVER_FEES F WHERE F.CONTRACT_ID = UR.CONTRACT_ID AND F.COV_ID=                 
                   (SELECT CV.COV_ID  FROM UWGE_COVER CV WHERE CV.COV_PARENT_ID = UCOV.COV_ID and TOP_INDICATOR=''Y'' AND COV_CODE=''IMA'' AND ROWNUM=1  )
                    AND TOP_INDICATOR=''Y'' AND F.FEE_CODE=''IMA''  AND ROWNUM =1) AS IMA_FEE,
                  NULL AS IMPORT_TYPE,
                  NULL AS PREV_POL_OP_IND,
                  NULL AS DEPARTMENT
                  from UWGE_RISK UR
                  INNER JOIN UWPL_RISK_PERSON URP
                  ON URP.CONTRACT_ID =UR.CONTRACT_ID
                  AND UR.RISK_ID =URP.RISK_ID
                  AND URP.VERSION_NO =(SELECT MAX (b.version_no)
                  FROM UWPL_RISK_PERSON b
                  WHERE b.contract_id = UR.CONTRACT_ID
                  AND URP.object_id = b.object_id
                  AND b.version_no <= :BIND_VERSION_NO
                  AND b.reversing_version IS NULL)
                  AND URP.action_code <> ''D''
                  INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(URP.RISK_PART_ID, URP.RISK_PART_VER)) RCP
                  ON RCP.PART_ID=URP.RISK_PART_ID
                  AND RCP.VERSION=URP.RISK_PART_VER
                  INNER JOIN UWGE_COVER UCOV
                  ON UCOV.CONTRACT_ID =UR.CONTRACT_ID
                  AND UR.RISK_ID =UCOV.RISK_ID
                  AND UCOV.VERSION_NO = :BIND_VERSION_NO_1
                  AND UCOV.COV_PARENT_ID IS NULL
                  WHERE UR.CONTRACT_ID  = :BIND_CONTRACT_ID
                  AND UR.version_no = (SELECT MAX (c.version_no)
                  FROM UWGE_RISK c
                  WHERE c.contract_id = :BIND_CONTRACT_ID_1
                  AND UR.object_id = c.object_id
                  AND c.version_no <= :BIND_VERSION_NO_2
                  AND c.reversing_version IS NULL)
                  AND UR.action_code <> ''D''
                  ORDER BY  to_number(UCOV.cov_seq_ref)'; --116958_ALLIANZ SHIELD PLUS End
        RISK_DET   PG_TPA_UTILS.AAN_PA_HC_RISK_DET_TBL;
        V_ROW_NUM          NUMBER (5);
        V_ENDT_NARR_ARRAY  PG_TPA_UTILS.p_array_v;
    BEGIN
--        --dbms_output.put_line (
--                  'P_START_DT :  ' || P_START_DT);
            V_STEPS := '001';
             FILENAME1   := TO_CHAR(P_START_DT, 'YYYYMMDD')||'_HC' || chr(38) || ' PA_POLEND.xlsx';
        V_STEPS := '002';
         PG_EXCEL_UTILS.clear_workbook;
        PG_EXCEL_UTILS.new_sheet;
            PG_EXCEL_UTILS.CELL(1,1,'BORDEREAUX (POLICY &'||' ENDORSEMENT)');
        V_STEPS := '003';
        PG_EXCEL_UTILS.MERGECELLS(1,1,3,1);
        PG_EXCEL_UTILS.CELL(1,2,'FROM : ALLIANZ GENERAL INSURANCE COMPANY (MALAYSIA) BERHAD');
        PG_EXCEL_UTILS.MERGECELLS(1,2,3,2);
        PG_EXCEL_UTILS.CELL(1,3,'DATE :');
        PG_EXCEL_UTILS.CELL(2,3,TO_CHAR(P_START_DT, 'DD/MM/YYYY'));
        V_STEPS := '004';
        PG_EXCEL_UTILS.SET_ROW(4
        ,p_fontId => PG_EXCEL_UTILS.get_font( 'Arial',p_bold => true));
        PG_EXCEL_UTILS.CELL(1,4,'No.');
        PG_EXCEL_UTILS.CELL(2,4,'Import Type');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(2,20);
        PG_EXCEL_UTILS.CELL(3,4,'Member Full Name');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(3,40);
        PG_EXCEL_UTILS.CELL(4,4,'Address 1');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(4,20);
        PG_EXCEL_UTILS.CELL(5,4,'Address 2');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(5,20);
        PG_EXCEL_UTILS.CELL(6,4,'Address 3');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(6,40);
        PG_EXCEL_UTILS.CELL(7,4,'Address 4');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(7,20);
        PG_EXCEL_UTILS.CELL(8,4,'Gender');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(8,20);
        V_STEPS := '005';
        PG_EXCEL_UTILS.CELL(9,4,'DOB');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(9,20);
        PG_EXCEL_UTILS.CELL(10,4,'NRIC');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(10,20);
        PG_EXCEL_UTILS.CELL(11,4,'Other IC');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(11,20);
        PG_EXCEL_UTILS.CELL(12,4,'External Ref Id (aka Client)');
        V_STEPS := '006';
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(12,20);
        PG_EXCEL_UTILS.CELL(13,4,'Internal Ref Id (aka AAN)');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(13,20);
        PG_EXCEL_UTILS.CELL(14,4,'Employee ID');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(14,20);
        PG_EXCEL_UTILS.CELL(15,4,'Marital Status');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(15,20);
        PG_EXCEL_UTILS.CELL(16,4,'Race');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(16,20);
        PG_EXCEL_UTILS.CELL(17,4,'Phone');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(17,20);
        PG_EXCEL_UTILS.CELL(18,4,'VIP');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(18,20);
        PG_EXCEL_UTILS.CELL(19,4,'Special Condition');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(19,20);
        PG_EXCEL_UTILS.CELL(20,4,'Relationship');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(20,20);
         PG_EXCEL_UTILS.CELL(21,4,'Principal Int Ref Id (aka AAN)');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(21,20);
        PG_EXCEL_UTILS.CELL(22,4,'Principal Ext Ref Id (aka Client)');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(22,20);
        PG_EXCEL_UTILS.CELL(23,4,'Principal Name');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(23,20);
        PG_EXCEL_UTILS.CELL(24,4,'Principal NRIC');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(24,20);
        PG_EXCEL_UTILS.CELL(25,4,'Principal Other Ic');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(25,20);
        PG_EXCEL_UTILS.CELL(26,4,'Program Id');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(26,20);
        PG_EXCEL_UTILS.CELL(27,4,'Policy Type');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(27,20);
        PG_EXCEL_UTILS.CELL(28,4,'Policy Num');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(28,20);
        PG_EXCEL_UTILS.CELL(29,4,'Policy Eff Date');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(29,20);
        PG_EXCEL_UTILS.CELL(30,4,'Policy Expiry Date');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(30,20);
        PG_EXCEL_UTILS.CELL(31,4,'Previous Policy Num');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(31,20);
        PG_EXCEL_UTILS.CELL(32,4,'Previous Policy End Date');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(32,20);
        PG_EXCEL_UTILS.CELL(33,4,'Customer Owner Name');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(33,20);
        PG_EXCEL_UTILS.CELL(34,4,'External Plan Code');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(34,20);
        PG_EXCEL_UTILS.CELL(35,4,'Internal Plan Code Id');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(35,20);
        PG_EXCEL_UTILS.CELL(36,4,'Original Join Date');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(36,20);
        PG_EXCEL_UTILS.CELL(37,4,'Plan Attach Date');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(37,20);
        PG_EXCEL_UTILS.CELL(38,4,'Plan Expiry Date');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(38,20);
        PG_EXCEL_UTILS.CELL(39,4,'Subsidiary Name');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(39,20);
        PG_EXCEL_UTILS.CELL(40,4,'Agent Name');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(40,20);
        PG_EXCEL_UTILS.CELL(41,4,'Agent Code');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(41,20);
        PG_EXCEL_UTILS.CELL(42,4,'Insurer MCO Fees');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(42,20);
        PG_EXCEL_UTILS.CELL(43,4,'IMA Service?');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(43,20);
        PG_EXCEL_UTILS.CELL(44,4,'IMA Limit');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(44,20);
        PG_EXCEL_UTILS.CELL(45,4,'Date Received by AAN');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(45,20);
        PG_EXCEL_UTILS.CELL(46,4,'Termination Date');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(46,20);
        PG_EXCEL_UTILS.CELL(47,4,'Free text remark');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(47,20);
        PG_EXCEL_UTILS.CELL(48,4,'Questionnaire');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(48,20);
        PG_EXCEL_UTILS.CELL(49,4,'Plan-Remarks');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(49,20);
        PG_EXCEL_UTILS.CELL(50,4,'Diagnosis');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(50,20);
        PG_EXCEL_UTILS.CELL(51,4,'Outpatient Subcover');--1.4
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(51,20);--1.4
DBMS_OUTPUT.ENABLE (buffer_size => NULL);
        FOR REC IN C_TPA_AAN_PA
          LOOP
          V_STEPS := '007AA';
          IF((REC.POLICY_VERSION =1 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_NPOL,REC.PRODUCT_CONFIG_CODE) = 'N') OR REC.POLICY_VERSION >1)
          THEN
          IF (REC.POLICY_VERSION =1 AND REC.ASST_FEE_AMT >0 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_ASST,REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (REC.POLICY_VERSION >1 AND REC.ASST_FEE_AMT >=0 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_ASST,REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (REC.POLICY_VERSION =1 AND REC.IMA_FEE_AMT >0  AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_IMA,REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (REC.POLICY_VERSION >1 AND REC.IMA_FEE_AMT >=0 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_IMA,REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (REC.POLICY_VERSION =1 AND REC.MCO_FEE_AMT >0  AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_MCO,REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (REC.POLICY_VERSION >1 AND REC.MCO_FEE_AMT >=0 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_MCO,REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_RISK_LEVEL_DTLS,REC.PRODUCT_CONFIG_CODE) = 'Y')--116958_ALLIANZ SHIELD PLUS 
          THEN
           --1.4
          RISK_DET.DELETE;
          IF     REC.ENDT_CODE IS NOT NULL
               AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                      V_AAN_ENDT_CODE_R,REC.ENDT_CODE) = 'Y'
           THEN
             BEGIN EXECUTE IMMEDIATE V_SELECTED_RISK_SQL
              BULK COLLECT INTO RISK_DET
              using REC.CONTRACT_ID,REC.POLICY_VERSION;
          END;
           ELSE
             BEGIN EXECUTE IMMEDIATE V_ALL_RISK_SQL
              BULK COLLECT INTO RISK_DET
              using REC.POLICY_VERSION,REC.POLICY_VERSION,REC.CONTRACT_ID,REC.CONTRACT_ID,REC.POLICY_VERSION;
            END;
           END IF;
           V_ROW_NUM   := 0;
          FOR V_ROW_NUM IN 1 .. RISK_DET.COUNT
         LOOP--116958_ALLIANZ SHIELD PLUS start
           IF  (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_RISK_LEVEL_DTLS,REC.PRODUCT_CONFIG_CODE) = 'N' 
                 OR (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_RISK_LEVEL_DTLS,REC.PRODUCT_CONFIG_CODE) = 'Y' 
                      AND (NVL(RISK_DET(V_ROW_NUM).IMA_FEE,0) >0 OR  NVL(RISK_DET(V_ROW_NUM).MCO_FEE,0) >0 ) AND REC.POLICY_VERSION =1) 
                 OR (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_RISK_LEVEL_DTLS,REC.PRODUCT_CONFIG_CODE) = 'Y' AND REC.POLICY_VERSION >1 ) ) THEN 
                 --116958_ALLIANZ SHIELD PLUS end
            PG_EXCEL_UTILS.CELL(1,rowIDx,seq);
            IF REC.POLICY_VERSION =1 and REC.PREV_POL_NO is not null THEN
             PG_EXCEL_UTILS.CELL(2,rowIDx,'R');
             ELSIF REC.POLICY_VERSION =1 and REC.PREV_POL_NO is  null THEN
             PG_EXCEL_UTILS.CELL(2,rowIDx,'N');
             ELSIF REC.POLICY_VERSION > 1 AND REC.ENDT_CODE IN('96','97') THEN
             PG_EXCEL_UTILS.CELL(2,rowIDx,'X');
             ELSE
             PG_EXCEL_UTILS.CELL(2,rowIDx,'E');
            END IF;
            V_STEPS := '007A';
            PG_EXCEL_UTILS.CELL(3,rowIDx,NVL(RISK_DET(V_ROW_NUM).MEMBER_FULL_NAME,' '));
            PG_EXCEL_UTILS.CELL(4,rowIDx,NVL(REC.ADDRESS_LINE1,' ')||NVL(REC.ADDRESS_LINE2,' '));
            PG_EXCEL_UTILS.CELL(5,rowIDx,NVL(REC.ADDRESS_LINE3,' '));
            PG_EXCEL_UTILS.CELL(6,rowIDx,NVL(REC.POSTCODE,' ')||' '||NVL(REC.CITY,' '));
            PG_EXCEL_UTILS.CELL(7,rowIDx,NVL(REC.STATE,' '));
            PG_EXCEL_UTILS.CELL(8,rowIDx,NVL(RISK_DET(V_ROW_NUM).SEX,' '));
             --1.4
             IF RISK_DET(V_ROW_NUM).DATE_OF_BIRTH IS NULL THEN
              PG_EXCEL_UTILS.CELL(9,rowIDx,' ');
            ELSE
             PG_EXCEL_UTILS.CELL(9,rowIDx,TO_CHAR(RISK_DET(V_ROW_NUM).DATE_OF_BIRTH, 'DD/MM/YYYY'));
            END IF;
            PG_EXCEL_UTILS.CELL(10,rowIDx,NVL(RISK_DET(V_ROW_NUM).NRIC,' '));
            if RISK_DET(V_ROW_NUM).NRIC is  null then
            PG_EXCEL_UTILS.CELL(11,rowIDx,NVL(RISK_DET(V_ROW_NUM).NRIC_OTH,' '));
             else
              PG_EXCEL_UTILS.CELL(11,rowIDx,' ');
              end if;
            PG_EXCEL_UTILS.CELL(12,rowIDx,RISK_DET(V_ROW_NUM).RISK_ID||'-'||REC.POLICY_REF||'-'||RISK_DET(V_ROW_NUM).COV_SEQ_REF);
            PG_EXCEL_UTILS.CELL(13,rowIDx,' ');
            PG_EXCEL_UTILS.CELL(14,rowIDx,NVL(RISK_DET(V_ROW_NUM).EMPLOYEE_ID,' '));
            PG_EXCEL_UTILS.CELL(15,rowIDx,NVL(RISK_DET(V_ROW_NUM).MARITAL_STATUS,' '));
            PG_EXCEL_UTILS.CELL(16,rowIDx,' ');
            PG_EXCEL_UTILS.CELL(17,rowIDx,NVL(REC.PhoneNumber,' '));
            PG_EXCEL_UTILS.CELL(18,rowIDx,' ');
            PG_EXCEL_UTILS.CELL(19,rowIDx,' ');
            PG_EXCEL_UTILS.CELL(20,rowIDx,NVL(RISK_DET(V_ROW_NUM).RELATIONSHIP,' '));
            PG_EXCEL_UTILS.CELL(21,rowIDx,' ');
            --1.1
            if RISK_DET(V_ROW_NUM).INSURED_TYPE='P' then
                PG_EXCEL_UTILS.CELL(22,rowIDx,RISK_DET(V_ROW_NUM).RISK_ID||'-'||REC.POLICY_REF||'-'||RISK_DET(V_ROW_NUM).COV_SEQ_REF);
                PG_EXCEL_UTILS.CELL(23,rowIDx,NVL(RISK_DET(V_ROW_NUM).MEMBER_FULL_NAME,' '));
                PG_EXCEL_UTILS.CELL(24,rowIDx,NVL(RISK_DET(V_ROW_NUM).NRIC,' '));
                if RISK_DET(V_ROW_NUM).NRIC is null then
                PG_EXCEL_UTILS.CELL(25,rowIDx,NVL(RISK_DET(V_ROW_NUM).NRIC_OTH,' '));
                else
                PG_EXCEL_UTILS.CELL(25,rowIDx,' ');
                end if;
            else
                PG_EXCEL_UTILS.CELL(22,rowIDx,RISK_DET(V_ROW_NUM).RISK_PARENT_ID||'-'||REC.POLICY_REF||'-'||RISK_DET(V_ROW_NUM).Parent_cov_seq_no);
                V_PRINCIPAL_DET := PG_TPA_UTILS.FN_GET_PRINCIPAL_DET(REC.CONTRACT_ID,REC.POLICY_VERSION,RISK_DET(V_ROW_NUM).RISK_PARENT_ID);
                PG_EXCEL_UTILS.CELL(23,rowIDx,NVL(V_PRINCIPAL_DET.MEMBER_FULL_NAME,' '));
                PG_EXCEL_UTILS.CELL(24,rowIDx,NVL(V_PRINCIPAL_DET.NRIC,' '));
                if V_PRINCIPAL_DET.NRIC is null then
                PG_EXCEL_UTILS.CELL(25,rowIDx,NVL(V_PRINCIPAL_DET.NRIC_OTH,' '));
                else
                PG_EXCEL_UTILS.CELL(25,rowIDx,' ');
                end if;
            end if;

            PG_EXCEL_UTILS.CELL(26,rowIDx,' ');
            PG_EXCEL_UTILS.CELL(27,rowIDx,'IG');
            PG_EXCEL_UTILS.CELL(28,rowIDx,NVL(REC.POLICY_REF,' '));
            PG_EXCEL_UTILS.CELL(29,rowIDx,REC.EFF_DATE);
            PG_EXCEL_UTILS.CELL(30,rowIDx,REC.EXP_DATE);
            PG_EXCEL_UTILS.CELL(31,rowIDx,NVL(REC.prev_pol,' '));
            PG_EXCEL_UTILS.CELL(32,rowIDx,REC.prev_exp_date);
            PG_EXCEL_UTILS.CELL(33,rowIDx,NVL(REC.NAME_EXT,' '));
            V_UWPL_COVER_DET := PG_TPA_UTILS.FN_GET_UWPL_COVER_DET(REC.CONTRACT_ID,REC.POLICY_VERSION,RISK_DET(V_ROW_NUM).COV_ID);
            PG_EXCEL_UTILS.CELL(34,rowIDx,NVL(V_UWPL_COVER_DET.PLAN_CODE,' '));
            PG_EXCEL_UTILS.CELL(35,rowIDx,' ');
            --1.4
            IF RISK_DET(V_ROW_NUM).ORIGINAL_JOIN_DATE IS NULL THEN
              PG_EXCEL_UTILS.CELL(36,rowIDx,' ');
            ELSE
             PG_EXCEL_UTILS.CELL(36,rowIDx,TO_CHAR(RISK_DET(V_ROW_NUM).ORIGINAL_JOIN_DATE, 'DD/MM/YYYY'));
            END IF;
            PG_EXCEL_UTILS.CELL(37,rowIDx,RISK_DET(V_ROW_NUM).RISK_EFF_DATE);
            PG_EXCEL_UTILS.CELL(38,rowIDx,RISK_DET(V_ROW_NUM).RISK_EXP_DATE);
            PG_EXCEL_UTILS.CELL(39,rowIDx,REC.BRANCH_DESC);
            PG_EXCEL_UTILS.CELL(40,rowIDx,NVL(REC.AGENT_NAME,' '));
            PG_EXCEL_UTILS.CELL(41,rowIDx,NVL(REC.AGENT_CODE,' '));

                --116958_ALLIANZ SHIELD PLUS start
                IF (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_RISK_LEVEL_DTLS,REC.PRODUCT_CONFIG_CODE) = 'Y') THEN

                   PG_EXCEL_UTILS.CELL(42,rowIDx,NVL(RISK_DET(V_ROW_NUM).MCO_FEE,0));
                     IF NVL(RISK_DET(V_ROW_NUM).IMA_FEE,0) >0 THEN
                      PG_EXCEL_UTILS.CELL(43,rowIDx,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment(p_vertical =>'center',p_horizontal =>'center',p_wrapText=>true));
                    ELSE
                      PG_EXCEL_UTILS.CELL(43,rowIDx,'N',p_alignment => PG_EXCEL_UTILS.get_alignment(p_vertical =>'center',p_horizontal =>'center',p_wrapText=>true));
                    END IF;

                    IF NVL(RISK_DET(V_ROW_NUM).IMA_FEE,0) >0 THEN

                         IF (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_IMA_LMT_2M,REC.PRODUCT_CONFIG_CODE) = 'Y') THEN

                            PG_EXCEL_UTILS.CELL(44,rowIDx,'2000000');
                         ELSE
                            PG_EXCEL_UTILS.CELL(44,rowIDx,'1000000');
                         END IF;
                    ELSE
                     PG_EXCEL_UTILS.CELL(44,rowIDx,' ');
                    END IF;                
                ELSE 
                --116958_ALLIANZ SHIELD PLUS end
            PG_EXCEL_UTILS.CELL(42,rowIDx,NVL(REC.MCO_FEE_AMT,0)+NVL(REC.MCOI_FEE_AMT,0)+NVL(REC.MCOO_FEE_AMT,0));
             IF REC.IMA_FEE_AMT >0 THEN
              PG_EXCEL_UTILS.CELL(43,rowIDx,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment(p_vertical =>'center',p_horizontal =>'center',p_wrapText=>true));
            ELSE
              PG_EXCEL_UTILS.CELL(43,rowIDx,'N',p_alignment => PG_EXCEL_UTILS.get_alignment(p_vertical =>'center',p_horizontal =>'center',p_wrapText=>true));
            END IF;
            IF REC.IMA_FEE_AMT >0 THEN
                         --116958_ALLIANZ SHIELD PLUS start
                         IF (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_IMA_LMT_2M,REC.PRODUCT_CONFIG_CODE) = 'Y') THEN                    
                            PG_EXCEL_UTILS.CELL(44,rowIDx,'2000000');
                         ELSE
                         --116958_ALLIANZ SHIELD PLUS end    
            PG_EXCEL_UTILS.CELL(44,rowIDx,'1000000');
                         END IF;--116958_ALLIANZ SHIELD PLUS                 

            ELSE
             PG_EXCEL_UTILS.CELL(44,rowIDx,' ');
             END IF;
                END IF;     --116958_ALLIANZ SHIELD PLUS              

            PG_EXCEL_UTILS.CELL(45,rowIDx,REC.DateReceivedbyAAN);
            IF REC.POLICY_VERSION > 1 AND REC.ENDT_CODE IN('96','97') THEN

            PG_EXCEL_UTILS.CELL(46,rowIDx,REC.ENDT_EFF_DATE);
            ELSE
            PG_EXCEL_UTILS.CELL(46,rowIDx,RISK_DET(V_ROW_NUM).TEMINATE_DATE);
            END IF;
            --1.4
            --dbms_output.put_line ('V_ROW_NUM::'||V_ROW_NUM);
            IF V_ROW_NUM =1  THEN
                IF DBMS_LOB.getlength(REC.ENDT_NARR) > 32000 THEN
                V_ENDT_NARR_ARRAY:=PG_TPA_UTILS.FN_SPLIT_CLOB(REC.ENDT_NARR);
                FOR I IN 1 .. V_ENDT_NARR_ARRAY.COUNT
                LOOP
                --dbms_output.put_line('I::'||V_ENDT_NARR_ARRAY(I));
                  IF I =1 THEN
                    PG_EXCEL_UTILS.CELL(47,rowIDx,NVL( V_ENDT_NARR_ARRAY(1),' '));
                  ELSE
                   PG_EXCEL_UTILS.CELL(50+I,rowIDx,NVL( V_ENDT_NARR_ARRAY(I),' '));
                  END IF;
                 END LOOP;
               ELSE
                PG_EXCEL_UTILS.CELL(47,rowIDx,NVL( REC.ENDT_NARR,' '));
               END IF;
            ELSE
                PG_EXCEL_UTILS.CELL(47,rowIDx,' ');
            END IF;
            PG_EXCEL_UTILS.CELL(48,rowIDx,NVL(PG_TPA_UTILS.FN_GET_RISK_QUESTION(REC.CONTRACT_ID,REC.POLICY_VERSION,RISK_DET(V_ROW_NUM).RISK_ID),' '));
            PG_EXCEL_UTILS.CELL(49,rowIDx,NVL(V_UWPL_COVER_DET.REMARKS,' '));
            PG_EXCEL_UTILS.CELL(50,rowIDx,NVL(PG_TPA_UTILS.FN_GET_COVER_DIAGNOSIS(REC.CONTRACT_ID,REC.POLICY_VERSION,RISK_DET(V_ROW_NUM).RISK_ID,RISK_DET(V_ROW_NUM).COV_ID),' '));
            --1.4
            IF RISK_DET(V_ROW_NUM).OP_SUB_COV >0 THEN
              PG_EXCEL_UTILS.CELL(51,rowIDx,'Y');
            ELSE
              PG_EXCEL_UTILS.CELL(51,rowIDx,'N');
            END IF;
            rowIDx :=rowIDx+1;
            seq :=seq+1;
                END IF;    --116958_ALLIANZ SHIELD PLUS 
            END LOOP;
             --dbms_output.put_line ('RISK_DET::'||RISK_DET.COUNT);
          IF RISK_DET.COUNT >0 THEN
            --V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.POLICY_VERSION,'AAN'); --1.3 --2.0 comment
            V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD_STS(REC.CONTRACT_ID,REC.POLICY_VERSION,'AAN', 'R'); --2.0 add
          END IF;
          END IF;
        END IF;

      END LOOP;
           V_STEPS := '010';
           FOR REC IN C_TPA_AAN_HC
          LOOP
          IF (REC.POLICY_VERSION =1 AND REC.MCOI_FEE_AMT >0) OR
            (REC.POLICY_VERSION >1 AND REC.MCOI_FEE_AMT >=0) OR
            (REC.POLICY_VERSION =1 AND REC.IMA_FEE_AMT >0) OR
            (REC.POLICY_VERSION >1 AND REC.IMA_FEE_AMT >=0) OR
            (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_MCOO,REC.PRODUCT_CONFIG_CODE) = 'Y'
            AND ((REC.POLICY_VERSION =1 AND REC.MCOO_FEE_AMT >0) OR
            (REC.POLICY_VERSION >1 AND REC.MCOO_FEE_AMT >=0)  )) OR
            (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_DMA,REC.PRODUCT_CONFIG_CODE) = 'Y'
            AND ((REC.POLICY_VERSION =1 AND (REC.MCOO_FEE_AMT >0 OR REC.DMA_FEE_AMT >0)) OR
            (REC.POLICY_VERSION >1 AND (REC.MCOO_FEE_AMT >=0 OR REC.DMA_FEE_AMT >=0))  ) )
--          IF ((REC.POLICY_VERSION =1 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_MCOI,REC.PRODUCT_CONFIG_CODE) = 'Y')
--          AND ((REC.MCOI_FEE_AMT >0 AND REC.IMA_FEE_AMT >0) OR REC.IMA_FEE_AMT >0))
--            OR ((REC.POLICY_VERSION >1 AND  PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_MCOI,REC.PRODUCT_CONFIG_CODE) = 'Y')
--             AND ((REC.MCOI_FEE_AMT >=0 AND REC.IMA_FEE_AMT >=0) OR REC.IMA_FEE_AMT >=0))
--            OR ((REC.POLICY_VERSION =1 AND  PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_MCOO,REC.PRODUCT_CONFIG_CODE) = 'Y')
--             AND ((REC.MCOI_FEE_AMT >0 AND REC.IMA_FEE_AMT >0) OR (REC.MCOI_FEE_AMT >0 AND REC.MCOO_FEE_AMT >0) OR REC.IMA_FEE_AMT >0))
--            OR ((REC.POLICY_VERSION >1 AND  PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_MCOO,REC.PRODUCT_CONFIG_CODE) = 'Y')
--            AND ((REC.MCOI_FEE_AMT >=0 AND REC.IMA_FEE_AMT >=0) OR (REC.MCOI_FEE_AMT >=0 AND REC.MCOO_FEE_AMT >=0) OR REC.IMA_FEE_AMT >=0))
--            OR ((REC.POLICY_VERSION =1 AND  PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_DMA,REC.PRODUCT_CONFIG_CODE) = 'Y')
--             AND ((REC.MCOI_FEE_AMT >0 AND REC.IMA_FEE_AMT >0) OR (REC.MCOI_FEE_AMT >0 AND REC.MCOO_FEE_AMT >0)
--             OR (REC.MCOI_FEE_AMT >0 AND REC.DMA_FEE_AMT >0) OR REC.IMA_FEE_AMT >0 OR REC.DMA_FEE_AMT >0))
--            OR ((REC.POLICY_VERSION >1 AND  PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_DMA,REC.PRODUCT_CONFIG_CODE) = 'Y')
--            AND ((REC.MCOI_FEE_AMT >=0 AND REC.IMA_FEE_AMT >=0) OR (REC.MCOI_FEE_AMT >=0 AND REC.MCOO_FEE_AMT >=0)
--            OR (REC.MCOI_FEE_AMT >=0 AND REC.DMA_FEE_AMT >=0) OR REC.IMA_FEE_AMT >=0 OR REC.DMA_FEE_AMT >=0 ))
          THEN
          RISK_DET.DELETE;
          IF     REC.ENDT_CODE IS NOT NULL
               AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                      V_AAN_ENDT_CODE_R,REC.ENDT_CODE) = 'Y'
           THEN
             BEGIN EXECUTE IMMEDIATE V_SELECTED_RISK_SQL
              BULK COLLECT INTO RISK_DET
              using REC.CONTRACT_ID,REC.POLICY_VERSION;
          END;
           ELSE
             BEGIN EXECUTE IMMEDIATE V_ALL_RISK_SQL
              BULK COLLECT INTO RISK_DET
              using REC.POLICY_VERSION,REC.POLICY_VERSION,REC.CONTRACT_ID,REC.CONTRACT_ID,REC.POLICY_VERSION;
            END;
           END IF;
           V_ROW_NUM   := 0;
          FOR V_ROW_NUM IN 1 .. RISK_DET.COUNT
         LOOP
          V_STEPS := '011';
            PG_EXCEL_UTILS.CELL(1,rowIDx,seq);
            IF REC.POLICY_VERSION =1 and REC.PREV_POL_NO is not null THEN
             PG_EXCEL_UTILS.CELL(2,rowIDx,'R');
             ELSIF REC.POLICY_VERSION =1 and REC.PREV_POL_NO is  null THEN
             PG_EXCEL_UTILS.CELL(2,rowIDx,'N');
             ELSIF REC.POLICY_VERSION > 1 AND REC.ENDT_CODE IN('96','97') THEN
             PG_EXCEL_UTILS.CELL(2,rowIDx,'X');
             ELSE
             PG_EXCEL_UTILS.CELL(2,rowIDx,'E');
            END IF;
            PG_EXCEL_UTILS.CELL(3,rowIDx,NVL(RISK_DET(V_ROW_NUM).MEMBER_FULL_NAME,' '));
            PG_EXCEL_UTILS.CELL(4,rowIDx,NVL(REC.ADDRESS_LINE1,' ')||NVL(REC.ADDRESS_LINE2,' '));
            PG_EXCEL_UTILS.CELL(5,rowIDx,NVL(REC.ADDRESS_LINE3,' '));
            PG_EXCEL_UTILS.CELL(6,rowIDx,NVL(REC.POSTCODE,' ')||' '||NVL(REC.CITY,' '));
            PG_EXCEL_UTILS.CELL(7,rowIDx,NVL(REC.STATE,' '));
            PG_EXCEL_UTILS.CELL(8,rowIDx,NVL(RISK_DET(V_ROW_NUM).SEX,' '));
            IF RISK_DET(V_ROW_NUM).DATE_OF_BIRTH IS NULL THEN
              PG_EXCEL_UTILS.CELL(9,rowIDx,' ');
            ELSE
             PG_EXCEL_UTILS.CELL(9,rowIDx,TO_CHAR(RISK_DET(V_ROW_NUM).DATE_OF_BIRTH, 'DD/MM/YYYY'));
            END IF;
            PG_EXCEL_UTILS.CELL(10,rowIDx,NVL(RISK_DET(V_ROW_NUM).NRIC,' '));
             if RISK_DET(V_ROW_NUM).NRIC is  null then
            PG_EXCEL_UTILS.CELL(11,rowIDx,NVL(RISK_DET(V_ROW_NUM).NRIC_OTH,' '));
             else
              PG_EXCEL_UTILS.CELL(11,rowIDx,' ');
              end if;
            PG_EXCEL_UTILS.CELL(12,rowIDx,RISK_DET(V_ROW_NUM).RISK_ID||'-'||REC.POLICY_REF||'-'||RISK_DET(V_ROW_NUM).COV_SEQ_REF);
            PG_EXCEL_UTILS.CELL(13,rowIDx,' ');
            PG_EXCEL_UTILS.CELL(14,rowIDx,NVL(RISK_DET(V_ROW_NUM).EMPLOYEE_ID,' '));
            PG_EXCEL_UTILS.CELL(15,rowIDx,NVL(RISK_DET(V_ROW_NUM).MARITAL_STATUS,' '));
            PG_EXCEL_UTILS.CELL(16,rowIDx,' ');
            PG_EXCEL_UTILS.CELL(17,rowIDx,NVL(REC.PhoneNumber,' '));
            PG_EXCEL_UTILS.CELL(18,rowIDx,' ');
            PG_EXCEL_UTILS.CELL(19,rowIDx,' ');
            PG_EXCEL_UTILS.CELL(20,rowIDx,NVL(RISK_DET(V_ROW_NUM).RELATIONSHIP,' '));
            PG_EXCEL_UTILS.CELL(21,rowIDx,' ');
             --1.1
            if RISK_DET(V_ROW_NUM).INSURED_TYPE='P' then
                PG_EXCEL_UTILS.CELL(22,rowIDx,RISK_DET(V_ROW_NUM).RISK_ID||'-'||REC.POLICY_REF||'-'||RISK_DET(V_ROW_NUM).COV_SEQ_REF);
                PG_EXCEL_UTILS.CELL(23,rowIDx,NVL(RISK_DET(V_ROW_NUM).MEMBER_FULL_NAME,' '));
                PG_EXCEL_UTILS.CELL(24,rowIDx,NVL(RISK_DET(V_ROW_NUM).NRIC,' '));
                if RISK_DET(V_ROW_NUM).NRIC is null then
                PG_EXCEL_UTILS.CELL(25,rowIDx,NVL(RISK_DET(V_ROW_NUM).NRIC_OTH,' '));
                else
                PG_EXCEL_UTILS.CELL(25,rowIDx,' ');
                end if;
            else
                PG_EXCEL_UTILS.CELL(22,rowIDx,RISK_DET(V_ROW_NUM).RISK_PARENT_ID||'-'||REC.POLICY_REF||'-'||RISK_DET(V_ROW_NUM).Parent_cov_seq_no);
                V_PRINCIPAL_DET := PG_TPA_UTILS.FN_GET_PRINCIPAL_DET(REC.CONTRACT_ID,REC.POLICY_VERSION,RISK_DET(V_ROW_NUM).RISK_PARENT_ID);
                PG_EXCEL_UTILS.CELL(23,rowIDx,NVL(V_PRINCIPAL_DET.MEMBER_FULL_NAME,' '));
                PG_EXCEL_UTILS.CELL(24,rowIDx,NVL(V_PRINCIPAL_DET.NRIC,' '));
                if V_PRINCIPAL_DET.NRIC is null then
                PG_EXCEL_UTILS.CELL(25,rowIDx,NVL(V_PRINCIPAL_DET.NRIC_OTH,' '));
                else
                PG_EXCEL_UTILS.CELL(25,rowIDx,' ');
                end if;
            end if;
            PG_EXCEL_UTILS.CELL(26,rowIDx,' ');
            PG_EXCEL_UTILS.CELL(27,rowIDx,'IG');
            PG_EXCEL_UTILS.CELL(28,rowIDx,NVL(REC.POLICY_REF,' '));
            PG_EXCEL_UTILS.CELL(29,rowIDx,REC.EFF_DATE);
            PG_EXCEL_UTILS.CELL(30,rowIDx,REC.EXP_DATE);
            PG_EXCEL_UTILS.CELL(31,rowIDx,NVL(REC.prev_pol,' '));
            PG_EXCEL_UTILS.CELL(32,rowIDx,REC.prev_exp_date);
            PG_EXCEL_UTILS.CELL(33,rowIDx,NVL(REC.NAME_EXT,' '));
            V_UWPL_COVER_DET := PG_TPA_UTILS.FN_GET_UWPL_COVER_DET(REC.CONTRACT_ID,REC.POLICY_VERSION,RISK_DET(V_ROW_NUM).COV_ID);
            PG_EXCEL_UTILS.CELL(34,rowIDx,NVL(V_UWPL_COVER_DET.PLAN_CODE,' '));
            PG_EXCEL_UTILS.CELL(35,rowIDx,' ');
            --1.4
            IF RISK_DET(V_ROW_NUM).ORIGINAL_JOIN_DATE IS NULL THEN
              PG_EXCEL_UTILS.CELL(36,rowIDx,' ');
            ELSE
             PG_EXCEL_UTILS.CELL(36,rowIDx,TO_CHAR(RISK_DET(V_ROW_NUM).ORIGINAL_JOIN_DATE, 'DD/MM/YYYY'));
            END IF;
            PG_EXCEL_UTILS.CELL(37,rowIDx,RISK_DET(V_ROW_NUM).RISK_EFF_DATE);
            PG_EXCEL_UTILS.CELL(38,rowIDx,RISK_DET(V_ROW_NUM).RISK_EXP_DATE);
            PG_EXCEL_UTILS.CELL(39,rowIDx,REC.BRANCH_DESC);

            PG_EXCEL_UTILS.CELL(40,rowIDx,NVL(REC.AGENT_NAME,' '));
            PG_EXCEL_UTILS.CELL(41,rowIDx,NVL(REC.AGENT_CODE,' '));
            PG_EXCEL_UTILS.CELL(42,rowIDx,NVL(REC.MCO_FEE_AMT,0)+NVL(REC.MCOI_FEE_AMT,0)+NVL(REC.MCOO_FEE_AMT,0));
             IF REC.IMA_FEE_AMT >0 THEN
              PG_EXCEL_UTILS.CELL(43,rowIDx,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment(p_vertical =>'center',p_horizontal =>'center',p_wrapText=>true));
            ELSE
              PG_EXCEL_UTILS.CELL(43,rowIDx,'N',p_alignment => PG_EXCEL_UTILS.get_alignment(p_vertical =>'center',p_horizontal =>'center',p_wrapText=>true));
            END IF;
            IF REC.IMA_FEE_AMT >0 THEN
            PG_EXCEL_UTILS.CELL(44,rowIDx,'1000000');
            ELSE
            PG_EXCEL_UTILS.CELL(44,rowIDx,' ');
            END IF;
            PG_EXCEL_UTILS.CELL(45,rowIDx,REC.DateReceivedbyAAN);
            IF REC.POLICY_VERSION > 1 AND REC.ENDT_CODE IN('96','97') THEN

            PG_EXCEL_UTILS.CELL(46,rowIDx,REC.ENDT_EFF_DATE);
            ELSE
            PG_EXCEL_UTILS.CELL(46,rowIDx,RISK_DET(V_ROW_NUM).TEMINATE_DATE);
            END IF;
            --dbms_output.put_line ('V_ROW_NUM::'||V_ROW_NUM);
            IF V_ROW_NUM =1  THEN
            --1.4
            --dbms_output.put_line ('ENDT_NARR::'||DBMS_LOB.getlength(REC.ENDT_NARR));
               IF DBMS_LOB.getlength(REC.ENDT_NARR) > 32000 THEN
                V_ENDT_NARR_ARRAY:=PG_TPA_UTILS.FN_SPLIT_CLOB(REC.ENDT_NARR);
                FOR I IN 1 .. V_ENDT_NARR_ARRAY.COUNT
                LOOP
                --dbms_output.put_line('I::'||V_ENDT_NARR_ARRAY(I));
                  IF I =1 THEN
                    PG_EXCEL_UTILS.CELL(47,rowIDx,NVL( V_ENDT_NARR_ARRAY(1),' '));
                  ELSE
                   PG_EXCEL_UTILS.CELL(50+I,rowIDx,NVL( V_ENDT_NARR_ARRAY(I),' '));
                  END IF;
                 END LOOP;
               ELSE
                PG_EXCEL_UTILS.CELL(47,rowIDx,NVL( REC.ENDT_NARR,' '));
               END IF;
            ELSE
              PG_EXCEL_UTILS.CELL(47,rowIDx,' ');
            END IF;
            PG_EXCEL_UTILS.CELL(48,rowIDx,NVL(PG_TPA_UTILS.FN_GET_RISK_QUESTION(REC.CONTRACT_ID,REC.POLICY_VERSION,RISK_DET(V_ROW_NUM).RISK_ID),' '));
            PG_EXCEL_UTILS.CELL(49,rowIDx,NVL(V_UWPL_COVER_DET.REMARKS,' '));
            PG_EXCEL_UTILS.CELL(50,rowIDx,NVL(PG_TPA_UTILS.FN_GET_COVER_DIAGNOSIS(REC.CONTRACT_ID,REC.POLICY_VERSION,RISK_DET(V_ROW_NUM).RISK_ID,RISK_DET(V_ROW_NUM).COV_ID),' '));
            --1.4
            IF RISK_DET(V_ROW_NUM).OP_SUB_COV >0 THEN
              PG_EXCEL_UTILS.CELL(51,rowIDx,'Y');
            ELSE
              PG_EXCEL_UTILS.CELL(51,rowIDx,'N');
            END IF;
            rowIDx :=rowIDx+1;
            seq :=seq+1;
            END LOOP;

           --dbms_output.put_line ('RISK_DET::'||RISK_DET.COUNT);
          IF RISK_DET.COUNT >0 THEN
            --V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.POLICY_VERSION,'AAN'); --1.3 --2.0 comment
            V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD_STS(REC.CONTRACT_ID,REC.POLICY_VERSION,'AAN', 'R'); --2.0 add
          END IF;
          END IF;
      END LOOP;
          V_STEPS := '016';
          DBMS_OUTPUT.ENABLE (buffer_size => NULL);
            PG_EXCEL_UTILS.save( v_file_dir, FILENAME1 );
    EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                    --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
    END PC_TPA_AAN_HC_PA_POL_ENDT;
  PROCEDURE PC_TPA_CARD_PRINTING_POL_ENDT(P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE) IS

     CURSOR C_TPA_CARD_PRINTING
      IS
        SELECT (CASE WHEN UPV.VERSION_NO =1 THEN 'PL' ELSE 'EN' END) AS TRANSACTION_TYPE,UPC.PRODUCT_CONFIG_CODE,
(SELECT CODE_DESC FROM CMGE_CODE CC WHERE CC.CODE_CD=UPC.PRODUCT_CONFIG_CODE AND CC.CAT_CODE = UPC.LOB||'_PRODUCT') AS PRODUCT_DESC,
CP.NAME_EXT,(SELECT PC_VALUE_DESCRIPTION FROM PDC_V_PROD_ATTRIBUTE_VALUES where VALUE=URP.INSURED_TYPE  and ATTRIBUTE='INSURED_TYPE' and PRODUCT_CODE=UPC.PRODUCT_CONFIG_CODE) AS INSURED_TYPE,
(CASE WHEN UPV.VERSION_NO =1 THEN OPB.POLICY_REF ELSE UPV.ENDT_NO END) AS POL_NO
,UPLC.PLAN_CODE,
(SELECT CODE_DESC FROM CMGE_CODE CC WHERE CC.CAT_CODE ='MS_PLAN' AND CC.CODE_CD=UPLC.PLAN_CODE) AS PLAN_DESC,
TO_CHAR(UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,UPB.ISSUE_OFFICE,(case when UPV.VERSION_NO >1 then (UPV.endt_Code||'-'||(select endt_descp from cmuw_endt where endt_code=UPV.endt_Code)) else ' ' end) as ENDT_DESC,
NVL(UPV.ENDT_NARR,' ') AS ENDT_NARR,
(SELECT COUNT(1) FROM UWGE_RISK TUR WHERE TUR.CONTRACT_ID =UPV.CONTRACT_ID
        AND TUR.VERSION_NO =UPV.VERSION_NO AND UR.RISK_ID =TUR.RISK_ID ) AS ISSHOWRISK
        ,UPV.endt_Code,UPV.VERSION_NO AS POLICY_VERSION,UPV.CONTRACT_ID --1.3
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD --1.3
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
         INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =UPV.VERSION_NO
--        INNER JOIN SB_UWPL_POLICY_BASES PLPB
--        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
--        AND PLPB.POLICY_VERSION =UPV.VERSION_NO
        INNER JOIN SB_UWGE_RISK UR
        ON UR.CONTRACT_ID =UPV.CONTRACT_ID
        AND UR.POLICY_VERSION =UPV.VERSION_NO
        INNER JOIN UWGE_COVER UCOV
        ON UCOV.CONTRACT_ID =UPV.CONTRACT_ID
        AND UCOV.VERSION_NO =UPV.VERSION_NO
        AND UR.RISK_ID =UCOV.RISK_ID
        AND UCOV.COV_PARENT_ID IS NULL
        INNER JOIN SB_UWPL_COVER UPLC
        ON UPLC.CONTRACT_ID =UCOV.CONTRACT_ID
         AND UPLC.COV_ID =UCOV.COV_ID
         AND UPLC.POLICY_VERSION =UCOV.VERSION_NO
        INNER JOIN SB_UWPL_RISK_PERSON URP
        ON URP.CONTRACT_ID =UPV.CONTRACT_ID
        AND URP.POLICY_VERSION =UPV.VERSION_NO
        AND UR.RISK_ID =URP.RISK_ID
         INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(URP.RISK_PART_ID, URP.RISK_PART_VER)) CP
        ON CP.PART_ID=URP.RISK_PART_ID
        AND CP.VERSION=URP.RISK_PART_VER
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'CP'),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'CP'), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS IN('A','C','E')
        --AND PLPB.CARD_IND = 'Y'
        AND (UPV.ENDT_CODE IS NULL OR UPV.ENDT_CODE  IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'TPA_ENDT_CODE'),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'TPA_ENDT_CODE'), '[^,]+', 1, level) is not null ))
       -- AND PLPB.CARD_PROVIDER='Allianz'
        AND UPV.ACTION_CODE IN('A','C')
        --AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy');
        AND UPCD.DLOAD_STATUS ='P'
        AND UPCD.TPA_NAME='GM_CARD_ALLIANZ'
        ORDER BY TRUNC(UCOV.COV_SEQ_REF);--1.3

        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_CARD_PRINTING_POL_ENDT';
        FILENAME  UTL_FILE.FILE_TYPE;
        FILENAME1 VARCHAR2(1000);
        v_file_dir VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_GMCARD_DIR');
         rowIDx number := 5;
          seq number := 1;
          V_IS_HAVE_DATA VARCHAR2(1):='N';
          V_RET                 NUMBER := 0; --1.3

    BEGIN
            V_STEPS := '001';
             FILENAME1   := TO_CHAR(P_START_DT, 'YYYYMMDD')||'_CARDPRINTING_POLEND.xlsx';

         PG_EXCEL_UTILS.clear_workbook;
        PG_EXCEL_UTILS.new_sheet;
            PG_EXCEL_UTILS.CELL(1,1,'CARD PRINTING BORDEREAUX (POLICY &'||' ENDORSEMENT)');

        PG_EXCEL_UTILS.MERGECELLS(1,1,3,1);
        PG_EXCEL_UTILS.CELL(1,2,'FROM : ALLIANZ GENERAL INSURANCE COMPANY (MALAYSIA) BERHAD');
        PG_EXCEL_UTILS.MERGECELLS(1,2,3,2);
        PG_EXCEL_UTILS.CELL(1,3,'DATE :');
        PG_EXCEL_UTILS.CELL(2,3,TO_CHAR(SYSDATE, 'DD/MM/YYYY'));

        PG_EXCEL_UTILS.SET_ROW(4
        ,p_fontId => PG_EXCEL_UTILS.get_font( 'Arial',p_bold => true));
        PG_EXCEL_UTILS.CELL(1,4,'No.');
        PG_EXCEL_UTILS.CELL(2,4,'Transaction Type');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(2,20);
        PG_EXCEL_UTILS.CELL(3,4,'Product Code');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(3,40);
        PG_EXCEL_UTILS.CELL(4,4,'Product Name');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(4,20);
        PG_EXCEL_UTILS.CELL(5,4,'Name');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(5,20);
        PG_EXCEL_UTILS.CELL(6,4,'Insured Type');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(6,40);
        PG_EXCEL_UTILS.CELL(7,4,'Policy Number');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(7,20);
        PG_EXCEL_UTILS.CELL(8,4,'Plan');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(8,20);
        PG_EXCEL_UTILS.CELL(9,4,'Plan Description');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(9,20);
        PG_EXCEL_UTILS.CELL(10,4,'Expiry Date');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(10,20);
        PG_EXCEL_UTILS.CELL(11,4,'Issue Office');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(11,20);
        PG_EXCEL_UTILS.CELL(12,4,'Endorsement Code &'||' Description');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(12,20);
        PG_EXCEL_UTILS.CELL(13,4,'Text Decription');
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(13,20);

        FOR REC IN C_TPA_CARD_PRINTING
          LOOP
          V_IS_HAVE_DATA :='Y';
          IF REC.POLICY_VERSION =1  OR (REC.POLICY_VERSION >1 AND REC.ISSHOWRISK =1)
          OR (REC.POLICY_VERSION >1 AND REC.ENDT_CODE ='107')
           THEN
            PG_EXCEL_UTILS.CELL(1,rowIDx,seq);
            PG_EXCEL_UTILS.CELL(2,rowIDx,REC.TRANSACTION_TYPE);
            PG_EXCEL_UTILS.CELL(3,rowIDx,REC.PRODUCT_CONFIG_CODE);
            PG_EXCEL_UTILS.CELL(4,rowIDx,REC.PRODUCT_DESC);
            PG_EXCEL_UTILS.CELL(5,rowIDx,REC.NAME_EXT);
            PG_EXCEL_UTILS.CELL(6,rowIDx,REC.INSURED_TYPE);
            PG_EXCEL_UTILS.CELL(7,rowIDx,REC.POL_NO);
            PG_EXCEL_UTILS.CELL(8,rowIDx,REC.PLAN_CODE);
            PG_EXCEL_UTILS.CELL(9,rowIDx,REC.PLAN_DESC);
            PG_EXCEL_UTILS.CELL(10,rowIDx,REC.EXP_DATE);
            PG_EXCEL_UTILS.CELL(11,rowIDx,REC.ISSUE_OFFICE);
            PG_EXCEL_UTILS.CELL(12,rowIDx,REC.ENDT_DESC);
            PG_EXCEL_UTILS.CELL(13,rowIDx,REC.ENDT_NARR);

            rowIDx :=rowIDx+1;
            seq :=seq+1;
            V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.POLICY_VERSION,'GM_CARD_ALLIANZ'); --1.3
          END IF;
      END LOOP;
          IF V_IS_HAVE_DATA ='Y'THEN
            PG_EXCEL_UTILS.save( v_file_dir, FILENAME1 );
            END IF;
    EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                    --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
    END PC_TPA_CARD_PRINTING_POL_ENDT;

 PROCEDURE PC_TPA_FWCS_CARD_POL_ENDT(P_ISSUE_OFFICE IN UWGE_POLICY_BASES.ISSUE_OFFICE%TYPE,P_BRANCH_PREFIX IN CMDM_BRANCH.BRANCH_PREFIX%TYPE,P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE)
 IS

     CURSOR C_TPA_FWCS_PRINTING
      IS
           SELECT OPB.POLICY_REF||'-'||(case when length(TRUNC(UCOV.COV_SEQ_REF)) =1 then '000'||TRUNC(UCOV.COV_SEQ_REF)
    when length(TRUNC(UCOV.COV_SEQ_REF)) =2 then '00'||TRUNC(UCOV.COV_SEQ_REF) when length(TRUNC(UCOV.COV_SEQ_REF)) =3 then '0'||TRUNC(UCOV.COV_SEQ_REF) else ''||TRUNC(UCOV.COV_SEQ_REF) end) AS POLICY_NO,
    UPV.VERSION_NO,UPB.AGENT_CODE,PLPB.CARD_PROVIDER ,LTrim(RCP.NAME_EXT) AS EMP_NAME,URP.CARD_NO,
    (CASE  WHEN RCP.ID_VALUE1 is null THEN  RCP.ID_VALUE2 else RCP.ID_VALUE1 END) AS PASSPORT,
    (select NCC.CODE_DESC||'-'||URP.FW_NATIONALITY from CMGE_CODE NCC where NCC.CODE_CD=URP.FW_NATIONALITY AND NCC.CAT_CODE='FW_NATIONALITY') AS NATIONALITY,
    CP.NAME_EXT AS EMPLOYER_NAME,TO_CHAR(UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,TO_CHAR(UPB.EXP_DATE, 'DD-MM-YYYY') AS EXP_DATE,
    TO_CHAR(UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,UPB.ISSUE_OFFICE,
    (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'STATE' AND CODE_CD = CPA.STATE) AS STATE, UPV.ENDT_NO||'-'||(case when length(TRUNC(UCOV.COV_SEQ_REF)) =1 then '000'||TRUNC(UCOV.COV_SEQ_REF)
    when length(TRUNC(UCOV.COV_SEQ_REF)) =2 then '00'||TRUNC(UCOV.COV_SEQ_REF) when length(TRUNC(UCOV.COV_SEQ_REF)) =3 then '0'||TRUNC(UCOV.COV_SEQ_REF) else ''||TRUNC(UCOV.COV_SEQ_REF) end) AS ENDT_NO,
    (CASE WHEN CP.MOBILE_NO1 IS NOT NULL THEN CP.MOBILE_CODE1||CP.MOBILE_NO1
    WHEN CP.MOBILE_NO2 IS NOT NULL THEN CP.MOBILE_CODE2||CP.MOBILE_NO2 ELSE CPA.PHONE_CODE||CPA.PHONE_NO END) AS MOBILE_NO,
    (select CODE_DESC from CMGE_CODE cc where cc.CODE_CD=UPB.SECTOR and CAT_CODE='FW_SECTOR_CODE')||'-'||UPB.SECTOR AS SECTOR_DESC
    ,UPV.CONTRACT_ID --1.3
  from UWGE_POLICY_VERSIONS  UPV
  INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD --1.3
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
            INNER JOIN UWGE_POLICY_CONTRACTS UPC
            ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
             INNER JOIN OCP_POLICY_BASES OPB
            ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
            INNER JOIN UWGE_POLICY_BASES UPB
            ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
            AND UPB.VERSION_NO =UPV.VERSION_NO
            INNER JOIN UWPL_POLICY_BASES PLPB
            ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
            AND PLPB.VERSION_NO =UPV.VERSION_NO
            INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
            ON CP.PART_ID=UPB.CP_PART_ID
            AND CP.VERSION=UPB.CP_VERSION
           INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (UPB.CP_ADDR_ID,UPB.CP_ADDR_VERSION)) CPA
            ON CPA.ADD_ID = UPB.CP_ADDR_ID
            AND CPA.VERSION = UPB.CP_ADDR_VERSION
            INNER JOIN UWGE_RISK UR
            ON UR.CONTRACT_ID =UPV.CONTRACT_ID
            AND UR.VERSION_NO =UPV.VERSION_NO
           INNER JOIN UWPL_RISK_PERSON URP
            ON URP.CONTRACT_ID =UPV.CONTRACT_ID
            AND URP.VERSION_NO =UPV.VERSION_NO
            AND UR.RISK_ID =URP.RISK_ID
            INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(URP.RISK_PART_ID, URP.RISK_PART_VER)) RCP
             ON RCP.PART_ID=URP.RISK_PART_ID
             AND RCP.VERSION=URP.RISK_PART_VER
            INNER JOIN UWGE_COVER UCOV
            ON UCOV.CONTRACT_ID =UPV.CONTRACT_ID
            AND UCOV.VERSION_NO =UPV.VERSION_NO
            AND UR.RISK_ID =UCOV.RISK_ID
            AND UCOV.COV_PARENT_ID IS NULL
            INNER JOIN UWPL_COVER UPLC
            ON UPLC.CONTRACT_ID =UCOV.CONTRACT_ID
             AND UPLC.COV_ID =UCOV.COV_ID
             AND UPLC.VERSION_NO =UCOV.VERSION_NO
             WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'),'[^,]+', 1, level) from dual
            connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'), '[^,]+', 1, level) is not null)
            AND UPC.POLICY_STATUS IN('A','C','E')
            AND PLPB.CARD_PROVIDER = 'SALAFEE'
            AND UPB.ISSUE_OFFICE=P_ISSUE_OFFICE
            AND UPV.ACTION_CODE IN ('A','C')
            AND (UPV.ENDT_CODE IS NULL OR UPV.ENDT_CODE='72')
            --AND UPV.ISSUE_DATE = to_date(P_START_DT,'dd-MON-yy')
            AND UPCD.DLOAD_STATUS ='P' --1.3
            AND UPCD.TPA_NAME='SALAFEE'
            order by OPB.POLICY_REF,UPV.ENDT_NO,TRUNC(UCOV.COV_SEQ_REF);

        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_FWCS_CARD_POL_ENDT';
        V_FILE_POL  UTL_FILE.FILE_TYPE;
        V_FILE_ENDT  UTL_FILE.FILE_TYPE;
        FILENAME1 VARCHAR2(1000);
        FILENAME2 VARCHAR2(1000);
        v_file_dir VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_SALAFEE_DIR');
         rowIDx number := 5;
          seq number := 1;
          V_IS_HAVE_DATA VARCHAR2(1):='N';
          V_RET                 NUMBER := 0; --1.3

    BEGIN
            V_STEPS := '001';
        FILENAME1   := TO_CHAR(P_START_DT, 'MM')||'P'||TO_CHAR(P_START_DT, 'DD')||'00'||P_BRANCH_PREFIX||'.TXT';

        FILENAME2   := TO_CHAR(P_START_DT, 'MM')||'E'||TO_CHAR(P_START_DT, 'DD')||'00'||P_BRANCH_PREFIX||'.TXT';
        V_FILE_POL    := UTL_FILE.FOPEN(v_file_dir, FILENAME1, 'W',32767);
        V_FILE_ENDT    := UTL_FILE.FOPEN(v_file_dir, FILENAME2, 'W',32767);

        FOR REC IN C_TPA_FWCS_PRINTING
          LOOP
          V_IS_HAVE_DATA :='Y';
          IF (REC.VERSION_NO =1)
           THEN
             UTL_FILE.PUT_LINE(V_FILE_POL,
                                          '"' || REC.EMP_NAME || '""' ||
                                          REC.PASSPORT || '""'|| REC.NATIONALITY || '""' || REC.SECTOR_DESC || '""' ||REC.EMPLOYER_NAME || '""'
                                              || REC.MOBILE_NO || '""' || REC.POLICY_NO || '""' || REC.EXP_DATE || '""' || REC.CARD_NO
                                          || '""' || REC.AGENT_CODE || '""' || REC.STATE||'"');
            ELSE
            UTL_FILE.PUT_LINE(V_FILE_ENDT,'"' || REC.EMP_NAME || '""' ||
                                          REC.PASSPORT || '""'|| REC.NATIONALITY || '""' || REC.SECTOR_DESC || '""' ||REC.EMPLOYER_NAME || '""'
                                          || REC.MOBILE_NO || '""' || REC.ENDT_NO || '""' || REC.EXP_DATE || '""' || REC.CARD_NO
                                          || '""' || REC.AGENT_CODE || '""' || REC.STATE||'"');
            END IF;
        V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.VERSION_NO,'SALAFEE'); --1.3
      END LOOP;
           UTL_FILE.FCLOSE(V_FILE_POL);
           UTL_FILE.FCLOSE(V_FILE_ENDT);
    EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                    --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
    END PC_TPA_FWCS_CARD_POL_ENDT;
-- 1.2
PROCEDURE PC_TPA_SPIKPA_POL(P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE,
                            V_COUNT_SUCCESS IN VARCHAR2,V_RUN_HOUR IN VARCHAR2)
 IS
  CURSOR C_TPA_SPIKPA
      IS
        SELECT CASE WHEN UPC.SOURCE_SYSTEM='IIMS' THEN 'FWM' ELSE'PRA' END AS TPCACd,
        OPB.POLICY_REF AS POLICY_NO,OPB.CONTRACT_ID AS CONTRACT_ID,
        TO_CHAR(UPV.ISSUE_DATE, 'YYYYMMDD') AS ISSUE_DATE,TO_CHAR(UPB.EFF_DATE, 'YYYYMMDD') AS COVER_FROM,
        TO_CHAR(UPB.EXP_DATE, 'YYYYMMDD') AS COVER_TILL, UPB.GROSS_PREM
        AS GROSS_PREM,PLPB.EMPLOYER_TYPE,CP.ID_VALUE1,CP.NAME_EXT,
        REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
        REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,
        REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3,CPA.POSTCODE,
        (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE='CITY' AND CODE_CD=CPA.CITY) AS CITY,
        CPA.STATE,UPB.AGENT_CODE,(CPA.PHONE_CODE||CPA.PHONE_NO) as MTelNo,CP.EMAIL,
        LTRIM (TO_CHAR (NVL(UPB.REBATE_AMT,'0.0'), '999999999990.99')) AS REBATE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =1
        AND UPF.FEE_CODE  in('MCOI','MCO','MCOO')) AS MCO_FEE_AMT,
        LTRIM (TO_CHAR (NVL(UPB.GST_AMT,'0.0'), '999999999990.99')) AS GST_AMT,
        UPB.ISSUE_OFFICE,CP.MOBILE_CODE1||CP.MOBILE_NO1 AS MOBILE_NO,UPB.SECTOR AS OccupSectCd
        ,UPV.VERSION_NO,(CASE  WHEN UPB.PREV_POL_NO is null THEN  'N' else 'R' END) AS InsuredStatus
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD --1.3
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =1
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =1
        INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
        ON CP.PART_ID=UPB.CP_PART_ID
        AND CP.VERSION=UPB.CP_VERSION
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (UPB.CP_ADDR_ID,UPB.CP_ADDR_VERSION)) CPA
        ON CPA.ADD_ID = UPB.CP_ADDR_ID
        AND CPA.VERSION = UPB.CP_ADDR_VERSION
        INNER JOIN DMAG_VI_AGENT DVA
        ON DVA.AGENTCODE =UPB.AGENT_CODE
        WHERE UPC.PRODUCT_CONFIG_CODE IN('105002')
        AND UPC.POLICY_STATUS IN('A')
        AND UPV.VERSION_NO =1
        AND PLPB.TPA_NAME = 'P000052'
        AND NVL(UPB.MST_POL_IND,'N') <>'Y'
        AND UPB.GROSS_PREM >0
        --AND UPV.ISSUE_DATE = to_date(P_START_DT,'dd-MON-yy')
        AND UPCD.DLOAD_STATUS ='P'
        AND UPCD.TPA_NAME='SPIKPA'
        ORDER BY OPB.POLICY_REF ASC;
        CURSOR C_TPA_FW(P_CONTRACT_ID IN NUMBER)
        IS
        SELECT RCP.NAME_EXT AS FWName,URP.RISK_ID,URP.OCCUP_CODE,
        (CASE  WHEN RCP.ID_VALUE1 is null THEN  RCP.ID_VALUE2 else RCP.ID_VALUE1 END) AS PASSPORT,
        (select URP.FW_NATIONALITY from CMGE_CODE NCC where NCC.CODE_CD=URP.FW_NATIONALITY AND NCC.CAT_CODE='FW_NATIONALITY') AS NAT_CD,
        URP.WORK_PERMIT_NO,TO_CHAR(URP.WORK_PERMIT_EXP_DATE, 'YYYYMMDD') AS WORK_PERMIT_EXP_DATE,URP.INSURED_FOR,
        TO_CHAR(RCP.DATE_OF_BIRTH, 'YYYYMMDD') AS DateOfBirth,RCP.SEX ,
        LTRIM (TO_CHAR (NVL(UCOV.COV_BASIC_PREM,'0.0'), '999999999990.99')) AS COV_BASIC_PREM
        ,(CASE WHEN UPC.SOURCE_SYSTEM='IIMS' AND UPLC.LIMIT_AMT=0 THEN 10000
        WHEN  UPLC.LIMIT_AMT=0 THEN 20000 ELSE UPLC.LIMIT_AMT END) AS LIMIT_AMT
        ,LTRIM (TO_CHAR (NVL(( SELECT UPF.FEE_AMT from UWGE_COVER_FEES UPF
        WHERE UPF.CONTRACT_ID =UCOV.CONTRACT_ID
        AND UPF.COV_ID =UCOV.COV_ID
        AND UPF.VERSION_NO =UCOV.VERSION_NO
        AND UPF.FEE_CODE  in('MCOI','MCO','MCOO')),'0.0'), '999999999990.99')) AS COV_MCO_FEE_AMT,
        URP.RISK_PART_ID--1.8
        from UWPL_RISK_PERSON URP
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON URP.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN UWGE_COVER UCOV
        ON UCOV.CONTRACT_ID =URP.CONTRACT_ID
        AND URP.RISK_ID =UCOV.RISK_ID
        AND UCOV.COV_PARENT_ID IS NULL
        AND UCOV.VERSION_NO =1
        INNER JOIN UWPL_COVER UPLC
        ON UPLC.CONTRACT_ID =UCOV.CONTRACT_ID
        AND UPLC.COV_ID =UCOV.COV_ID
        AND UPLC.VERSION_NO =UCOV.VERSION_NO
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(URP.RISK_PART_ID, URP.RISK_PART_VER)) RCP
        ON RCP.PART_ID=URP.RISK_PART_ID
        AND RCP.VERSION=URP.RISK_PART_VER
        WHERE URP.CONTRACT_ID = P_CONTRACT_ID
        AND URP.VERSION_NO =1;


        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_SPIKPA_POL';
        V_FILE_POL  UTL_FILE.FILE_TYPE;
        V_FILE_FW  UTL_FILE.FILE_TYPE;
        V_FILE_TTL  UTL_FILE.FILE_TYPE;
        FILENAME1 VARCHAR2(1000);
        FILENAME2 VARCHAR2(1000);
        FILENAME3 VARCHAR2(1000);
        v_file_dir VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_SPIKPA_'||V_RUN_HOUR||'00');
        totalNoOfPolicys number := 0;
        totalNoOfFws number := 0;
        V_TOT_GROSS_PREM      NUMBER (18, 2);
        V_TOT_TPCA_FEE        NUMBER (18, 2);
        V_NO_OF_RISK_POL      NUMBER;
        V_TOT_LIMIT_AMT_POL   NUMBER (18, 2);
        V_RET                 NUMBER := 0;

    BEGIN
            V_STEPS := '001';
            PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
            V_PKG_NAME || V_FUNC_NAME,
            1,
            ':S:' || 'V_COUNT_SUCCESS=' || V_COUNT_SUCCESS || ':V_RUN_HOUR:' || V_RUN_HOUR||' :v_file_dir:'||v_file_dir);
            FILENAME1   := 'IEAGINP'||V_COUNT_SUCCESS||'.TXT';
            FILENAME2   := 'IEAGINB'||V_COUNT_SUCCESS||'.TXT';
            FILENAME3   := 'IEAGINW'||V_COUNT_SUCCESS||'.TXT';
            V_FILE_POL    := UTL_FILE.FOPEN(v_file_dir, FILENAME1, 'W',32767);
            V_FILE_TTL   := UTL_FILE.FOPEN(v_file_dir, FILENAME2, 'W',32767);
            V_FILE_FW   := UTL_FILE.FOPEN(v_file_dir, FILENAME3, 'W',32767);

            FOR REC IN C_TPA_SPIKPA
            LOOP
            IF REC.MCO_FEE_AMT >0 THEN
            FOR RECC IN C_TPA_FW(REC.CONTRACT_ID)
            LOOP
            --1.8
                UTL_FILE.PUT_LINE(V_FILE_FW,'AGI|'||REC.POLICY_NO||'|'||PG_TPA_UTILS.FN_GET_WORKER_ID(REC.POLICY_NO,RECC.RISK_PART_ID,RECC.RISK_ID)||'|'||REC.POLICY_NO
                ||'|'||RECC.NAT_CD||'|'||RECC.PASSPORT||'|'||RECC.OCCUP_CODE||'|'
                ||RECC.FWName||'|'||RECC.SEX||'|'||RECC.DateOfBirth||'|'
                ||LTRIM (TO_CHAR (NVL(RECC.LIMIT_AMT,'0.0'), '999,999,999,990.99'))||'|'||RECC.COV_BASIC_PREM||'|'||RECC.COV_MCO_FEE_AMT||'|'||REC.InsuredStatus
                ||'|'||RECC.INSURED_FOR||'|'||RECC.WORK_PERMIT_NO||'|'||RECC.WORK_PERMIT_EXP_DATE||chr(13));
                totalNoOfFws :=totalNoOfFws+1;
                V_NO_OF_RISK_POL := NVL(V_NO_OF_RISK_POL,0)+1;
                V_TOT_LIMIT_AMT_POL := NVL (V_TOT_LIMIT_AMT_POL, 0) +NVL (RECC.LIMIT_AMT,0);

            END LOOP;
            UTL_FILE.PUT_LINE(V_FILE_POL,'AGI|'||REC.TPCACd||'|'||REC.POLICY_NO||'|'||REC.POLICY_NO||'|'||REC.ISSUE_DATE
                ||'|'||REC.COVER_FROM||'|'||REC.COVER_TILL||'|'||REC.EMPLOYER_TYPE||'|'||REC.ID_VALUE1
                ||'|'||REC.NAME_EXT||'|'||REC.ADDRESS_LINE1||'|'||REC.ADDRESS_LINE2||'|'||REC.ADDRESS_LINE3||'|'
                ||'|'||REC.CITY||'|'||REC.POSTCODE||'|'||REC.STATE||'|'||REC.OccupSectCd
                ||'|'||REC.AGENT_CODE||'|'||REC.MTelNo||'|'||'|'||REC.MOBILE_NO||'|'||REC.EMAIL
                ||'|'||LTRIM (TO_CHAR (NVL(V_TOT_LIMIT_AMT_POL,'0.0'), '999,999,999,990.99'))||'|'||V_NO_OF_RISK_POL||'|'||LTRIM (TO_CHAR (NVL(REC.GROSS_PREM,'0.0'), '999999999990.99'))||'|'||
                LTRIM (TO_CHAR (NVL(REC.MCO_FEE_AMT,'0.0'), '999999999990.99'))
                ||'|'||REC.GST_AMT||'|'||'0.00|'||REC.REBATE_AMT||chr(13));
                totalNoOfPolicys :=totalNoOfPolicys+1;
                V_TOT_GROSS_PREM := NVL (V_TOT_GROSS_PREM, 0) +NVL (REC.GROSS_PREM,0);
                V_TOT_TPCA_FEE := NVL (V_TOT_TPCA_FEE, 0) +NVL (REC.MCO_FEE_AMT,0);
                V_NO_OF_RISK_POL := 0;
                V_TOT_LIMIT_AMT_POL :=0;
                V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.VERSION_NO,'SPIKPA'); --1.3
             END IF;
            END LOOP;
            UTL_FILE.PUT_LINE(V_FILE_TTL,NVL (totalNoOfPolicys, 0)||'|'||NVL (totalNoOfFws, 0)
            ||'|'||LTRIM (TO_CHAR (NVL(V_TOT_GROSS_PREM,'0.0'), '999999999990.99'))||'|'||
            LTRIM (TO_CHAR (NVL(V_TOT_TPCA_FEE,'0.0'), '999999999990.99'))||chr(13));

            UTL_FILE.FCLOSE(V_FILE_POL);
            UTL_FILE.FCLOSE(V_FILE_TTL);
            UTL_FILE.FCLOSE(V_FILE_FW);
            PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
            V_PKG_NAME || V_FUNC_NAME,
            1,
            ':E:' || 'V_COUNT_SUCCESS=' || V_COUNT_SUCCESS || ':V_RUN_HOUR:' || V_RUN_HOUR||' :v_file_dir:'||v_file_dir);
            EXCEPTION
            WHEN OTHERS
            THEN
            PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
            V_PKG_NAME || V_FUNC_NAME,
            1,
            '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
            --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);

END PC_TPA_SPIKPA_POL;
PROCEDURE PC_TPA_SPIKPA_ENDT(P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE,
                            V_COUNT_SUCCESS IN VARCHAR2,V_RUN_HOUR IN VARCHAR2)
 IS
  CURSOR C_TPA_SPIKPA
      IS
        SELECT CASE WHEN UPC.SOURCE_SYSTEM='IIMS' THEN 'FWM' ELSE'PRA' END AS TPCACd,
        OPB.POLICY_REF AS POLICY_NO,OPB.CONTRACT_ID AS CONTRACT_ID,UPV.ENDT_NO,
        TO_CHAR(UPV.ISSUE_DATE, 'YYYYMMDD') AS ISSUE_DATE,TO_CHAR(UPV.ENDT_EFF_DATE, 'YYYYMMDD') AS ENDT_EFF_DATE,
        TO_CHAR(UPV.ENDT_EFF_DATE, 'YYYYMMDD') AS COVER_FROM,TO_CHAR(UPV.ENDT_EXP_DATE, 'YYYYMMDD') AS COVER_TILL,
        UPB.DIFF_GROSS_PREM   AS GROSS_PREM,PLPB.EMPLOYER_TYPE,CP.ID_VALUE1,CP.NAME_EXT,
        REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
        REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,
        REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3,
        CPA.POSTCODE,(SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE='CITY' AND CODE_CD=CPA.CITY) AS CITY,
        CPA.STATE,UPB.AGENT_CODE,(CPA.PHONE_CODE||CPA.PHONE_NO) as MTelNo,CP.EMAIL,
        ( SELECT SUM(UPF.DIFF_FEE_AMT) from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO = UPV.VERSION_NO
        AND UPF.EXCL_IND ='Y'
        AND UPF.FEE_CODE  in('MCOI','MCO','MCOO')) AS MCO_FEE_AMT,
        LTRIM (TO_CHAR (NVL(UPB.DIFF_GST_AMT,0), '999999999990.99')) AS tax,
        LTRIM (TO_CHAR (NVL(UPB.DIFF_REBATE_AMT,'0.0'), '999999999990.99')) AS REBATE_AMT,
        (CASE  WHEN UPB.PREV_POL_NO is null THEN  'N' else 'R' END) AS InsuredStatus,
        UPB.ISSUE_OFFICE,CP.MOBILE_CODE1||CP.MOBILE_NO1 AS MOBILE_NO,UPB.SECTOR AS OccupSectCd,
        UPV.endt_rsn_code,UPV.VERSION_NO AS POLICY_VERSION,(CASE WHEN  LENGTH(UPV.ENDT_CNT||'') =1 THEN '00'||UPV.ENDT_CNT
        WHEN  LENGTH(UPV.ENDT_CNT||'') =2 THEN '0'||UPV.ENDT_CNT
        ELSE UPV.ENDT_CNT||'' END) AS ENDT_NO_CNT,UPV.ENDT_CODE
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO = UPV.VERSION_NO
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
        ON CP.PART_ID=UPB.CP_PART_ID
        AND CP.VERSION=UPB.CP_VERSION
        INNER  JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (UPB.CP_ADDR_ID,UPB.CP_ADDR_VERSION)) CPA
        ON CPA.ADD_ID = UPB.CP_ADDR_ID
        AND CPA.VERSION = UPB.CP_ADDR_VERSION
        INNER JOIN DMAG_VI_AGENT DVA
        ON DVA.AGENTCODE =UPB.AGENT_CODE
        WHERE UPC.PRODUCT_CONFIG_CODE IN('105002')
        AND UPC.POLICY_STATUS IN('A')
        AND UPV.VERSION_NO >1
        AND NVL(UPB.MST_POL_IND,'N') <>'Y'
        --AND UPV.ISSUE_DATE = to_date(P_START_DT,'dd-MON-yy')
        AND UPCD.DLOAD_STATUS ='P'
        AND UPCD.TPA_NAME='SPIKPA'
        ORDER BY UPV.ENDT_NO ASC;

      CURSOR C_TPA_FW(P_CONTRACT_ID IN NUMBER,P_POLICY_VERSION IN NUMBER)
      IS
        SELECT RCP.NAME_EXT AS FWName,
        LTRIM (TO_CHAR (NVL(UCOV.DIFF_COV_BASIC_PREM,'0.0'), '999999999990.99')) AS COV_BASIC_PREM
        ,LTRIM (TO_CHAR (NVL(( SELECT UPF.DIFF_FEE_AMT from UWGE_COVER_FEES UPF
        WHERE UPF.CONTRACT_ID =UCOV.CONTRACT_ID
        AND UPF.COV_ID =UCOV.COV_ID
        AND UPF.VERSION_NO =UCOV.VERSION_NO
        AND UPF.FEE_CODE  in('MCOI','MCO','MCOO')),'0.0'), '999999999990.99')) AS MCO_FEE_AMT,URP.RISK_ID,
        (CASE  WHEN RCP.ID_VALUE1 is null THEN  RCP.ID_VALUE2 else RCP.ID_VALUE1 END) AS PASSPORT,
        (select URP.FW_NATIONALITY from CMGE_CODE NCC where NCC.CODE_CD=URP.FW_NATIONALITY AND NCC.CAT_CODE='FW_NATIONALITY') AS NationalityCd,
        URP.WORK_PERMIT_NO,TO_CHAR(URP.WORK_PERMIT_EXP_DATE, 'YYYYMMDD') AS WorkPermitExpiry,URP.INSURED_FOR,
        URP.OCCUP_CODE AS OccupSectCd,TO_CHAR(RCP.DATE_OF_BIRTH, 'YYYYMMDD') AS DateOfBirth,RCP.SEX,URP.CANCEL_CODE
        ,URP.TEMINATE_DATE,URP.ACTION_CODE
        ,URP.VERSION_NO,(CASE WHEN UPC.SOURCE_SYSTEM='IIMS' AND UPLC.LIMIT_AMT=0 THEN 10000
            WHEN  UPLC.LIMIT_AMT=0 THEN 20000 ELSE UPLC.LIMIT_AMT END) AS LIMIT_AMT,URP.RISK_PART_ID--1.8
        from SB_UWPL_RISK_PERSON URP
         INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPC.CONTRACT_ID =P_CONTRACT_ID
        INNER JOIN UWGE_COVER UCOV
        ON UCOV.CONTRACT_ID =P_CONTRACT_ID
        AND URP.RISK_ID =UCOV.RISK_ID
        AND UCOV.COV_PARENT_ID IS NULL
        AND UCOV.VERSION_NO =P_POLICY_VERSION
        INNER JOIN SB_UWPL_COVER UPLC
        ON UPLC.CONTRACT_ID =P_CONTRACT_ID
        AND UPLC.COV_ID =UCOV.COV_ID
        AND UPLC.POLICY_VERSION =P_POLICY_VERSION
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(URP.RISK_PART_ID, URP.RISK_PART_VER)) RCP
        ON RCP.PART_ID=URP.RISK_PART_ID
        AND RCP.VERSION=URP.RISK_PART_VER
        WHERE URP.CONTRACT_ID=P_CONTRACT_ID
        AND URP.POLICY_VERSION =P_POLICY_VERSION;

     CURSOR C_TPA_CNCL
       IS
        SELECT CASE WHEN UPC.SOURCE_SYSTEM='IIMS' THEN 'FWM' ELSE'PRA' END AS TPCACd,
        OPB.POLICY_REF AS POLICY_NO,UPV.ENDT_NO,LTRIM (TO_CHAR (UPB.DIFF_GROSS_PREM, '999999999990.99'))
        AS GROSS_PREM,TO_CHAR(UPV.ENDT_EFF_DATE, 'YYYYMMDD') AS ENDT_EFF_DATE,
        ( SELECT SUM(NVL(UPF.DIFF_FEE_AMT,0)) from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO = UPV.VERSION_NO
        AND UPF.FEE_CODE in('MCOI','MCO','MCOO')) AS MCO_FEE_AMT,
        TO_CHAR(UPB.EFF_DATE, 'YYYYMMDD') AS EFF_DATE,
        TO_CHAR(UPV.ISSUE_DATE, 'YYYYMMDD') AS ISSUE_DATE,UPB.ISSUE_OFFICE,
        (CASE  WHEN UPB.PREV_POL_NO is null THEN  'N' else 'R' END) AS InsuredStatus,
        UPB.SECTOR AS OccupSectCd,
        LTRIM (TO_CHAR (UPB.DIFF_GST_AMT, '999999999990.99')) AS tax,
        UPV.endt_rsn_code,(CASE WHEN  LENGTH(UPV.ENDT_CNT||'') =1 THEN '00'||UPV.ENDT_CNT
            WHEN  LENGTH(UPV.ENDT_CNT||'') =2 THEN '0'||UPV.ENDT_CNT
               ELSE UPV.ENDT_CNT||'' END) AS ENDT_NO_CNT
               ,UPV.CONTRACT_ID,UPV.VERSION_NO
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO = UPV.VERSION_NO
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        WHERE UPC.PRODUCT_CONFIG_CODE IN('105002')
        AND UPC.POLICY_STATUS='C'
        AND UPV.VERSION_NO >1
        AND UPCD.DLOAD_STATUS ='P'
        AND UPCD.TPA_NAME='SPIKPA'
        AND UPV.ENDT_CODE IN ('96','97')
        --AND UPV.ISSUE_DATE = to_date(P_START_DT,'dd-MON-yy')
        ORDER BY UPV.ENDT_NO ASC;

        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_SPIKPA_ENDT';
        V_FILE_POL  UTL_FILE.FILE_TYPE;
        V_FILE_FW  UTL_FILE.FILE_TYPE;
        V_FILE_CNCL  UTL_FILE.FILE_TYPE;
        V_FILE_TTL  UTL_FILE.FILE_TYPE;

        FILENAME1 VARCHAR2(1000);
        FILENAME2 VARCHAR2(1000);
        FILENAME3 VARCHAR2(1000);
        FILENAME4 VARCHAR2(1000);

        v_file_dir VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_SPIKPA_'||V_RUN_HOUR||'00');
        totalNoOfEnds number := 0;
        totalNoOfFws number := 0;
        totalNoOfCncls number := 0;
        grossPremium number := 0;
        totalTPCAFees number := 0;
        V_TOT_GROSS_PREM      NUMBER (18, 2);
        V_TOT_TPCA_FEE        NUMBER (18, 2);
        V_NO_OF_RISK_POL      NUMBER;
        V_TOT_LIMIT_AMT_POL   NUMBER (18, 2);
        V_RET                 NUMBER := 0;
        V_TYPE_OF_ENDT        VARCHAR2 (1);
       BEGIN
            V_STEPS := '001';
            FILENAME1   := 'IEAGIEP'||V_COUNT_SUCCESS||'.TXT';
            FILENAME2   := 'IEAGIEB'||V_COUNT_SUCCESS||'.TXT';
            FILENAME3   := 'IEAGIEW'||V_COUNT_SUCCESS||'.TXT';
            FILENAME4   := 'IEAGIEX'||V_COUNT_SUCCESS||'.TXT';
            V_FILE_POL    := UTL_FILE.FOPEN(v_file_dir, FILENAME1, 'W',32767);
            V_FILE_TTL   := UTL_FILE.FOPEN(v_file_dir, FILENAME2, 'W',32767);
            V_FILE_FW   := UTL_FILE.FOPEN(v_file_dir, FILENAME3, 'W',32767);
            V_FILE_CNCL  := UTL_FILE.FOPEN(v_file_dir, FILENAME4, 'W',32767);
            FOR REC IN C_TPA_SPIKPA
            LOOP
                FOR RECC IN C_TPA_FW(REC.CONTRACT_ID,REC.POLICY_VERSION)
                LOOP
                IF REC.ENDT_CODE ='72' THEN
                  IF REC.POLICY_VERSION = RECC.VERSION_NO THEN
                   V_TYPE_OF_ENDT:='A';
                   ELSE
                   IF RECC.TEMINATE_DATE IS NOT NULL THEN
                    V_TYPE_OF_ENDT :='D';
                  ELSE
                    V_TYPE_OF_ENDT :='U';
                  END IF;
                 END IF;
                ELSE
                  IF RECC.TEMINATE_DATE IS NOT NULL THEN
                    V_TYPE_OF_ENDT :='D';
                  ELSE
                    V_TYPE_OF_ENDT :='U';
                  END IF;
                END IF;
                --1.8
                UTL_FILE.PUT_LINE(V_FILE_FW,'AGI|'||REC.POLICY_NO||'|'||PG_TPA_UTILS.FN_GET_WORKER_ID(REC.POLICY_NO,RECC.RISK_PART_ID,RECC.RISK_ID)||'|'||
                REC.ENDT_NO||'|'||REC.ISSUE_DATE||'|'||REC.POLICY_NO||'|'||REC.ENDT_EFF_DATE||'|'||
                V_TYPE_OF_ENDT  ||'|'||RECC.NationalityCd||'|'||RECC.PASSPORT||'|'||RECC.OccupSectCd||'|'||
                RECC.FWName||'|'||RECC.SEX||'|'||RECC.DateOfBirth||'|'||LTRIM (TO_CHAR (NVL(RECC.LIMIT_AMT,'0.0'), '999,999,999,990.99'))||'|'||
                RECC.COV_BASIC_PREM||'|'||RECC.MCO_FEE_AMT||'|'||REC.InsuredStatus||'|'||
                RECC.CANCEL_CODE||'|'||RECC.INSURED_FOR||'|'||RECC.WORK_PERMIT_NO||'|'||
                RECC.WorkPermitExpiry||chr(13));
                totalNoOfFws :=NVL (totalNoOfFws,0)+1;
                V_NO_OF_RISK_POL :=NVL (V_NO_OF_RISK_POL,0)+1;
                END LOOP;
            UTL_FILE.PUT_LINE(V_FILE_POL,'AGI|'||REC.TPCACd||'|'||REC.POLICY_NO||'|'||REC.ENDT_NO||'|'
            ||REC.ISSUE_DATE||'|'||REC.ENDT_EFF_DATE||'|'||REC.POLICY_NO||'|'||REC.COVER_FROM||'|'||REC.COVER_TILL
            ||'|'||REC.EMPLOYER_TYPE||'|'||REC.ID_VALUE1||'|'||REC.NAME_EXT
            ||'|'||REC.ADDRESS_LINE1||'|'||REC.ADDRESS_LINE2||'|'||REC.ADDRESS_LINE3||'||'||REC.CITY
            ||'|'||REC.POSTCODE||'|'||REC.STATE||'|'||REC.OccupSectCd||'|'||REC.AGENT_CODE||'|'||
            REC.MTelNo||'|'||'|'||REC.MOBILE_NO||'|'||REC.EMAIL
                ||'|'||V_NO_OF_RISK_POL||'|'||REC.GROSS_PREM||'|'||REC.MCO_FEE_AMT
            ||'|'||REC.tax||'|'||'0.0'||'|'||REC.REBATE_AMT||chr(13));
            totalNoOfEnds :=NVL (totalNoOfEnds,0)+1;
            V_TOT_GROSS_PREM := NVL (V_TOT_GROSS_PREM, 0) +NVL (REC.GROSS_PREM,0);
                V_TOT_TPCA_FEE := NVL (V_TOT_TPCA_FEE, 0) +NVL (REC.MCO_FEE_AMT,0);
                V_NO_OF_RISK_POL := 0;
                V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.POLICY_VERSION,'SPIKPA'); --1.3
            END LOOP;
            FOR RECCC IN C_TPA_CNCL
                LOOP
                UTL_FILE.PUT_LINE(V_FILE_CNCL,'AGI|'||RECCC.TPCACd||'|'||RECCC.POLICY_NO||'|'||RECCC.ENDT_NO||'|'
                ||RECCC.ISSUE_DATE||'|'||RECCC.ENDT_EFF_DATE||'|'||RECCC.POLICY_NO||'|'||RECCC.EFF_DATE||'|'||
                RECCC.GROSS_PREM||'|'||LTRIM (TO_CHAR (NVL(RECCC.MCO_FEE_AMT,'0.0'), '999999999990.99'))||'|'||
                RECCC.tax||'|'||'0.00'||'|'||RECCC.endt_rsn_code||chr(13));
                totalNoOfCncls :=NVL (totalNoOfCncls,0)+1;
                V_TOT_GROSS_PREM := NVL (V_TOT_GROSS_PREM, 0) +NVL (RECCC.GROSS_PREM,0);
                V_TOT_TPCA_FEE := NVL (V_TOT_TPCA_FEE, 0) +NVL (RECCC.MCO_FEE_AMT,0);
                V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(RECCC.CONTRACT_ID,RECCC.VERSION_NO,'SPIKPA');--1.3
                END LOOP;
            UTL_FILE.PUT_LINE(V_FILE_TTL,NVL (totalNoOfEnds, 0)||'|'||NVL (totalNoOfFws, 0)
            ||'|'||LTRIM (TO_CHAR (NVL(V_TOT_GROSS_PREM,'0.0'), '999999999990.99'))||'|'||
            LTRIM (TO_CHAR (NVL(V_TOT_TPCA_FEE,'0.0'), '999999999990.99'))||'|'||NVL (totalNoOfCncls, 0)||chr(13));
            UTL_FILE.FCLOSE(V_FILE_POL);
            UTL_FILE.FCLOSE(V_FILE_TTL);
            UTL_FILE.FCLOSE(V_FILE_FW);
            UTL_FILE.FCLOSE(V_FILE_CNCL);
           EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                    --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
   END PC_TPA_SPIKPA_ENDT;
 PROCEDURE PC_TPA_SPPA_POL(P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE,
                            V_COUNT_SUCCESS IN VARCHAR2,V_RUN_HOUR IN VARCHAR2)
 IS
  CURSOR C_TPA_SPPA
      IS
        SELECT OPB.POLICY_REF AS POLICY_NO,OPB.CONTRACT_ID AS CONTRACT_ID,TO_CHAR(UPV.ISSUE_DATE, 'YYYYMMDD') AS ISSUE_DATE,
        TO_CHAR(UPB.EFF_DATE, 'YYYYMMDD') AS COVER_FROM,TO_CHAR(UPB.EXP_DATE, 'YYYYMMDD') AS COVER_TILL,
        UPB.GROSS_PREM  AS GROSS_PREM,PLPB.EMPLOYER_TYPE,CP.ID_VALUE1,CP.NAME_EXT,REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
        REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,
        REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3,CPA.POSTCODE,
        (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE='CITY' AND CODE_CD=CPA.CITY) AS CITY,CPA.STATE,(SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'STATE'
        AND CODE_CD = CPA.STATE) AS STATE_DESC,UPB.AGENT_CODE,(CPA.PHONE_CODE||CPA.PHONE_NO) as MTelNo,CP.EMAIL,UPB.GST_AMT,
        UPB.ISSUE_OFFICE,CP.MOBILE_CODE1||CP.MOBILE_NO1 AS MOBILE_NO,UPB.SECTOR AS OccupSectCd,LTRIM (TO_CHAR (UPB.ANNUAL_PREM, '999999999990.99')) AS AnnPremium
        ,UPV.VERSION_NO
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =1
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =1
        INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
        ON CP.PART_ID=UPB.CP_PART_ID
        AND CP.VERSION=UPB.CP_VERSION
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (UPB.CP_ADDR_ID,UPB.CP_ADDR_VERSION)) CPA
        ON CPA.ADD_ID = UPB.CP_ADDR_ID
        AND CPA.VERSION = UPB.CP_ADDR_VERSION
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'),'[^,]+', 1, level) from dual
            connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS IN('A')
        AND UPV.VERSION_NO =1
        AND NVL(UPB.MST_POL_IND,'N') <>'Y'
        AND UPB.GROSS_PREM >0
        --AND UPV.ISSUE_DATE = to_date(P_START_DT,'dd-MON-yy')
        AND UPCD.DLOAD_STATUS ='P'
        AND UPCD.TPA_NAME='SPPA'
        ORDER BY OPB.POLICY_REF ASC;
     CURSOR C_TPA_FW(P_CONTRACT_ID IN NUMBER)
      IS
        SELECT RCP.NAME_EXT AS FWName,
        (CASE  WHEN RCP.ID_VALUE1 is null THEN  RCP.ID_VALUE2 else RCP.ID_VALUE1 END) AS PASSPORT,
        (select URP.FW_NATIONALITY from CMGE_CODE NCC where NCC.CODE_CD=URP.FW_NATIONALITY
        AND NCC.CAT_CODE='FW_NATIONALITY') AS NationalityCd,URP.WORK_PERMIT_NO,
        TO_CHAR(URP.WORK_PERMIT_EXP_DATE, 'YYYYMMDD') AS WorkPermitExpiry,URP.INSURED_FOR,
        URP.OCCUP_CODE AS OccupSectCd,TO_CHAR(RCP.DATE_OF_BIRTH, 'YYYYMMDD') AS DateOfBirth,RCP.SEX,
        UPW.WORKPLACE_REF,
        UPW.WORKPLACE_NAME, UPW.ADDRESS_LINE1,UPW.ADDRESS_LINE2,UPW.ADDRESS_LINE3,
        UPW.POSTCODE,UPW.STATE,(SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'STATE'
        AND CODE_CD = UPW.STATE) AS STATE_DESC,URP.CARD_NO,
        UCOV.COV_BASIC_PREM AS COV_BASIC_PREM
        ,( SELECT UPF.FEE_AMT from UWGE_COVER_FEES UPF
        WHERE UPF.CONTRACT_ID =UCOV.CONTRACT_ID
        AND UPF.COV_ID =UCOV.COV_ID
        AND UPF.VERSION_NO =UCOV.VERSION_NO
        AND UPF.FEE_CODE  in('SPPA')) AS COV_FEE_AMT,
        URP.RISK_ID,UR.SEQ_NO,UCOV.COV_GROSS_PREM
        from  UWGE_RISK UR
        INNER JOIN UWPL_RISK_PERSON URP
        ON URP.CONTRACT_ID =UR.CONTRACT_ID
        AND URP.RISK_ID =UR.RISK_ID
        AND URP.VERSION_NO=1
        INNER JOIN UWPL_POLICY_WORKPLACE UPW
        ON UPW.CONTRACT_ID =UR.CONTRACT_ID
        AND URP.WORKPLACE_ID =UPW.WORKPLACE_ID
        AND UPW.VERSION_NO =1
        INNER JOIN UWGE_COVER UCOV
        ON UCOV.CONTRACT_ID =UR.CONTRACT_ID
        AND URP.RISK_ID =UCOV.RISK_ID
        AND UCOV.COV_PARENT_ID IS NULL
        AND UCOV.VERSION_NO =1
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(URP.RISK_PART_ID, URP.RISK_PART_VER)) RCP
        ON RCP.PART_ID=URP.RISK_PART_ID
        AND RCP.VERSION=URP.RISK_PART_VER
        WHERE UR.CONTRACT_ID=P_CONTRACT_ID
        AND UR.VERSION_NO =1;

        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_SPPA_POL';
        V_FILE_POL            UTL_FILE.FILE_TYPE;
        V_FILE_WP             UTL_FILE.FILE_TYPE;
        V_FILE_FW             UTL_FILE.FILE_TYPE;
        V_FILE_DEPT           UTL_FILE.FILE_TYPE;
        V_FILE_TTL            UTL_FILE.FILE_TYPE;
        FILENAME1             VARCHAR2(1000);
        FILENAME2             VARCHAR2(1000);
        FILENAME3             VARCHAR2(1000);
        FILENAME4             VARCHAR2(1000);
        FILENAME5             VARCHAR2(1000);
        v_file_dir            VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_SPPA_'||V_RUN_HOUR||'00');
        totalNoOfPolicys      NUMBER := 0;
        totalNoOfFws          NUMBER := 0;
        totalGrossPremium     UWGE_COVER.COV_BASIC_PREM%TYPE;
        totalTPCAFees         UWGE_COVER_FEES.FEE_AMT%TYPE;
        totalGstAmount        NUMBER := 0;
        V_IS_HAVE_DATA        VARCHAR2(1):='N';
        V_NO_OF_RISK_POL      NUMBER := 0;
        V_COV_GROSS_PREM      UWGE_COVER.COV_GROSS_PREM%TYPE;
        V_TOT_TPCA_FEE_POL    UWGE_COVER_FEES.FEE_AMT%TYPE;
        V_TOT_GROSS_PREM_POL  UWGE_COVER.COV_BASIC_PREM%TYPE;
        V_RET                 NUMBER := 0;

    BEGIN
        V_STEPS := '001';
        FILENAME1   := 'CSAGINP'|| V_COUNT_SUCCESS||'.TXT';
        FILENAME2   := 'CSAGINE'|| V_COUNT_SUCCESS||'.TXT';
        FILENAME3   := 'CSAGINW'|| V_COUNT_SUCCESS||'.TXT';
        FILENAME4   := 'CSAGIND'|| V_COUNT_SUCCESS||'.TXT';
        FILENAME5   := 'CSAGINB'|| V_COUNT_SUCCESS||'.TXT';

        V_FILE_POL    := UTL_FILE.FOPEN(v_file_dir, FILENAME1, 'W',32767);
        V_FILE_WP   := UTL_FILE.FOPEN(v_file_dir, FILENAME2, 'W',32767);
        V_FILE_FW   := UTL_FILE.FOPEN(v_file_dir, FILENAME3, 'W',32767);
        V_FILE_DEPT   := UTL_FILE.FOPEN(v_file_dir, FILENAME4, 'W',32767);
        V_FILE_TTL   := UTL_FILE.FOPEN(v_file_dir, FILENAME5, 'W',32767);

        FOR REC IN C_TPA_SPPA
        LOOP
            FOR RECC IN C_TPA_FW(REC.CONTRACT_ID)
            LOOP
            UTL_FILE.PUT_LINE(V_FILE_FW,'AGI|'||REC.POLICY_NO||'|'||RECC.NationalityCd||'|'
            ||RECC.PASSPORT||'|'||RECC.OccupSectCd||'|'||RECC.FWName||'|'||RECC.SEX||'|'||
            RECC.DateOfBirth||'|'||RECC.WorkPermitExpiry||'|*'||RECC.CARD_NO||'|'||REC.POLICY_NO
            ||'|'||REC.POLICY_NO||RECC.WORKPLACE_REF||'|'||
            REC.POLICY_NO||'-'||RECC.RISK_ID||'-'||RECC.SEQ_NO||'|'||
            LTRIM (TO_CHAR (NVL(67,'0.0'), '999999999990.99'))||'|'||
            LTRIM (TO_CHAR (NVL(5,'0.0'), '999999999990.99'))||'|'||RECC.INSURED_FOR||chr(13));

            UTL_FILE.PUT_LINE(V_FILE_WP,'AGI|'||REC.POLICY_NO||'|'||RECC.WORKPLACE_REF||'|'||
            RECC.WORKPLACE_NAME||'|'||RECC.ADDRESS_LINE1||'|'||RECC.ADDRESS_LINE2||'|'||
            RECC.ADDRESS_LINE3||'|||'||RECC.POSTCODE||'|'||
            REC.STATE_DESC||'|'||RECC.STATE||'|'||REC.OccupSectCd||'|'||REC.POLICY_NO||'|'||
            REC.POLICY_NO||RECC.WORKPLACE_REF||chr(13));
            totalNoOfFws :=NVL (totalNoOfFws,0)+1;
            V_NO_OF_RISK_POL :=NVL (V_NO_OF_RISK_POL,0)+1;
            V_COV_GROSS_PREM :=72;
            V_TOT_TPCA_FEE_POL := NVL (V_TOT_TPCA_FEE_POL,0)+5;
            totalTPCAFees := NVL (totalTPCAFees,0)+5;
            V_TOT_GROSS_PREM_POL :=NVL (V_TOT_GROSS_PREM_POL,0)+67;
            totalGrossPremium :=NVL (totalGrossPremium,0)+67;
            BEGIN
                FOR GET_NOMINEE
                IN (SELECT NOMINEE_NAME,(SELECT CC.CODE_DESC FROM CMGE_CODE CC WHERE CC.CAT_CODE='FW_NOMREL'
                    AND CC.CODE_CD=RELATIONSHIP AND ROWNUM=1) AS RELATIONSHIP
                    ,ADDRESS_LINE1,ADDRESS_LINE2,ADDRESS_LINE3,
                    CASE WHEN PHONE_NO IS NULL THEN MOBILE_CODE||MOBILE_NO
                    ELSE PHONE_CODE||PHONE_NO END AS MOBILE_NO,
                    TO_NUMBER (TO_CHAR (SYSDATE, 'YYYY')) - TO_NUMBER (TO_CHAR (DATE_OF_BIRTH, 'YYYY')) AS NOM_AGE
                    FROM UWPL_RISK_NOMINEE WHERE   CONTRACT_ID =REC.CONTRACT_ID
                    AND VERSION_NO =1 AND RISK_ID =RECC.RISK_ID)
             LOOP
             UTL_FILE.PUT_LINE(V_FILE_DEPT,'AGI|'||REC.POLICY_NO||'|'||GET_NOMINEE.NOMINEE_NAME||'|'||
            GET_NOMINEE.RELATIONSHIP||'|'||GET_NOMINEE.NOM_AGE||'|'||GET_NOMINEE.ADDRESS_LINE1||'|'||GET_NOMINEE.ADDRESS_LINE2||'|'||
            GET_NOMINEE.ADDRESS_LINE3||'|||'||GET_NOMINEE.MOBILE_NO||'|'||
            REC.POLICY_NO||'|'||REC.POLICY_NO||'-'||RECC.RISK_ID||'-'||RECC.SEQ_NO||'|'||REC.POLICY_NO||'-'||RECC.RISK_ID||'-'||RECC.SEQ_NO||'-D'||chr(13));
             END LOOP;
            EXCEPTION
            WHEN OTHERS
            THEN
            PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
            END;
            END LOOP;
            UTL_FILE.PUT_LINE(V_FILE_POL,'AGI|'||REC.ISSUE_OFFICE||'|'||REC.POLICY_NO
            ||'|'||REC.ID_VALUE1||'|'||REC.NAME_EXT||'|'||REC.ADDRESS_LINE1||'|'||
            REC.ADDRESS_LINE2||'|'||REC.ADDRESS_LINE3||'|||'||REC.POSTCODE||'|'||
            REC.STATE_DESC||'|'||REC.STATE||'|'||REC.OccupSectCd||'|'||REC.MTelNo||'|'||
            ''||'|'||REC.MOBILE_NO||'|'||
            REC.EMAIL||'|'||REC.COVER_FROM||'|'||REC.COVER_TILL||'|'||
            LTRIM (TO_CHAR (NVL(V_COV_GROSS_PREM,'0.0'), '999999999990.99'))||'|'||
            V_NO_OF_RISK_POL||'|'||LTRIM (TO_CHAR (NVL(V_TOT_GROSS_PREM_POL,0), '999999999990.99'))||'|'||REC.ISSUE_DATE||'|'||
            LTRIM (TO_CHAR (NVL(V_TOT_TPCA_FEE_POL,'0.0'), '999999999990.99'))||'|'||
            REC.POLICY_NO||'|'||REC.EMPLOYER_TYPE||'|'||REC.AGENT_CODE||chr(13));

            totalNoOfPolicys :=NVL (totalNoOfPolicys,0)+1;
            totalGstAmount :=NVL (totalGstAmount,0)+NVL (REC.GST_AMT,0);
            V_NO_OF_RISK_POL :=0;
            V_COV_GROSS_PREM :=0;
            V_TOT_TPCA_FEE_POL :=0;
            V_TOT_GROSS_PREM_POL :=0;
            V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.VERSION_NO,'SPPA'); --1.3
        END LOOP;

           UTL_FILE.PUT_LINE(V_FILE_TTL,totalNoOfPolicys||'|'||totalNoOfFws||'|'||
           LTRIM (TO_CHAR (NVL(totalGrossPremium,'0.0'), '999999999990.99'))||'|'||
           LTRIM (TO_CHAR (NVL(totalTPCAFees,'0.0'), '999999999990.99'))||chr(13));
           UTL_FILE.FCLOSE(V_FILE_POL);
           UTL_FILE.FCLOSE(V_FILE_TTL);
           UTL_FILE.FCLOSE(V_FILE_FW);
           UTL_FILE.FCLOSE(V_FILE_WP);
           UTL_FILE.FCLOSE(V_FILE_DEPT);
           EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                    --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);

END PC_TPA_SPPA_POL;

PROCEDURE PC_TPA_SPPA_ENDT(P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE,
                            V_COUNT_SUCCESS IN VARCHAR2,V_RUN_HOUR IN VARCHAR2)
 IS
  CURSOR C_TPA_SPPA
      IS
        SELECT OPB.POLICY_REF AS POLICY_NO,OPB.CONTRACT_ID AS CONTRACT_ID,
        UPV.ENDT_NO,TO_CHAR(UPV.ENDT_EFF_DATE, 'YYYYMMDD') AS ENDT_EFF_DATE,
        TO_CHAR(UPV.ENDT_EXP_DATE, 'YYYYMMDD') AS ENDT_EXP_DATE,
        TO_CHAR(UPV.ISSUE_DATE, 'YYYYMMDD') AS ISSUE_DATE,
        TO_CHAR(UPB.EFF_DATE, 'YYYYMMDD') AS COVER_FROM,TO_CHAR(UPB.EXP_DATE, 'YYYYMMDD') AS COVER_TILL,
        UPB.DIFF_GROSS_PREM  AS GROSS_PREM,PLPB.EMPLOYER_TYPE,CP.ID_VALUE1,CP.NAME_EXT,REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
        REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3,
        CPA.POSTCODE,(SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE='CITY' AND CODE_CD=CPA.CITY) AS CITY,CPA.STATE,
        (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'STATE' AND CODE_CD = CPA.STATE) AS STATE_DESC,
        UPB.AGENT_CODE,(CPA.PHONE_CODE||CPA.PHONE_NO) as MTelNo,CP.EMAIL,UPB.DIFF_GST_AMT AS GST_AMT,
        UPB.ISSUE_OFFICE,CP.MOBILE_CODE1||CP.MOBILE_NO1 AS MOBILE_NO,UPB.SECTOR AS OccupSectCd,
        LTRIM (TO_CHAR (UPB.ANNUAL_PREM, '999999999990.99')) AS AnnPremium,UPV.VERSION_NO
        ,(CASE WHEN  LENGTH(UPV.ENDT_CNT||'') =1 THEN '00'||UPV.ENDT_CNT
            WHEN  LENGTH(UPV.ENDT_CNT||'') =2 THEN '0'||UPV.ENDT_CNT
               ELSE UPV.ENDT_CNT||'' END) AS ENDT_NO_CNT,UPV.ENDT_CODE
        FROM UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO = UPV.VERSION_NO
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO = UPV.VERSION_NO
        INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
        ON CP.PART_ID=UPB.CP_PART_ID
        AND CP.VERSION=UPB.CP_VERSION
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (UPB.CP_ADDR_ID,UPB.CP_ADDR_VERSION)) CPA
        ON CPA.ADD_ID = UPB.CP_ADDR_ID
        AND CPA.VERSION = UPB.CP_ADDR_VERSION
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'),'[^,]+', 1, level) from dual
            connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS IN('A')
        AND UPV.VERSION_NO >1
        AND NVL(UPB.MST_POL_IND,'N') <>'Y'
       -- AND UPV.ISSUE_DATE = to_date(P_START_DT,'dd-MON-yy')
        AND UPCD.DLOAD_STATUS ='P'
        AND UPCD.TPA_NAME='SPPA'
        ORDER BY UPV.ENDT_NO ASC;
     CURSOR C_TPA_FW(P_CONTRACT_ID IN NUMBER,P_POLICY_VERSION IN NUMBER)
      IS
        SELECT RCP.NAME_EXT AS FWName,UR.POLICY_VERSION,
        (CASE  WHEN RCP.ID_VALUE1 is null THEN  RCP.ID_VALUE2 else RCP.ID_VALUE1 END) AS PASSPORT,
        (select URP.FW_NATIONALITY from CMGE_CODE NCC where NCC.CODE_CD=URP.FW_NATIONALITY
        AND NCC.CAT_CODE='FW_NATIONALITY') AS NationalityCd,URP.WORK_PERMIT_NO,
        TO_CHAR(URP.WORK_PERMIT_EXP_DATE, 'YYYYMMDD') AS WorkPermitExpiry,URP.INSURED_FOR,

        URP.OCCUP_CODE AS OccupSectCd,TO_CHAR(RCP.DATE_OF_BIRTH, 'YYYYMMDD') AS DateOfBirth,RCP.SEX,
        UPW.WORKPLACE_REF,
        UPW.WORKPLACE_NAME, UPW.ADDRESS_LINE1,UPW.ADDRESS_LINE2,UPW.ADDRESS_LINE3,
        UPW.POSTCODE,UPW.STATE,(SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'STATE'
        AND CODE_CD = UPW.STATE) AS STATE_DESC,URP.CARD_NO,
        UCOV.DIFF_COV_BASIC_PREM AS DIFF_COV_BASIC_PREM
        ,LTRIM (TO_CHAR (NVL(UCOV.COV_BASIC_PREM,'0.0'), '999999999990.99')) AS COV_BASIC_PREM
        ,LTRIM (TO_CHAR (NVL(( SELECT UPF.FEE_AMT from UWGE_COVER_FEES UPF
        WHERE UPF.CONTRACT_ID =UCOV.CONTRACT_ID
        AND UPF.COV_ID =UCOV.COV_ID
        AND UPF.VERSION_NO = UR.POLICY_VERSION
        AND UPF.FEE_CODE  in('SPPA')),'0.0'), '999999999990.99')) AS COV_FEE_AMT,
        ( SELECT UPF.DIFF_FEE_AMT from UWGE_COVER_FEES UPF
        WHERE UPF.CONTRACT_ID =UCOV.CONTRACT_ID
        AND UPF.COV_ID =UCOV.COV_ID
        AND UPF.VERSION_NO = UR.POLICY_VERSION
        AND UPF.FEE_CODE  in('SPPA')) AS DIFF_COV_FEE_AMT,
        URP.RISK_ID,UR.SEQ_NO,UCOV.COV_GROSS_PREM,
        UR.ACTION_CODE ,URP.TEMINATE_DATE
        ,URP.CANCEL_CODE,URP.VERSION_NO
        from SB_UWGE_RISK UR
        INNER JOIN SB_UWPL_RISK_PERSON URP
        ON URP.CONTRACT_ID =UR.CONTRACT_ID
        AND URP.RISK_ID =UR.RISK_ID
        AND URP.POLICY_VERSION = UR.POLICY_VERSION
        INNER JOIN SB_UWPL_POLICY_WORKPLACE UPW
        ON UPW.CONTRACT_ID =UR.CONTRACT_ID
        AND URP.WORKPLACE_ID =UPW.WORKPLACE_ID
        AND UPW.POLICY_VERSION = UR.POLICY_VERSION
        INNER JOIN UWGE_COVER UCOV
        ON UCOV.CONTRACT_ID =UR.CONTRACT_ID
        AND URP.RISK_ID =UCOV.RISK_ID
        AND UCOV.COV_PARENT_ID IS NULL
        AND UCOV.VERSION_NO = UR.POLICY_VERSION
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(URP.RISK_PART_ID, URP.RISK_PART_VER)) RCP
        ON RCP.PART_ID=URP.RISK_PART_ID
        AND RCP.VERSION=URP.RISK_PART_VER
        WHERE  UR.CONTRACT_ID=P_CONTRACT_ID
        AND UR.POLICY_VERSION =P_POLICY_VERSION;

        CURSOR C_TPA_CNCL
          IS
            SELECT OPB.POLICY_REF AS POLICY_NO,UPV.ENDT_NO,
            UPB.DIFF_GROSS_PREM  AS GROSS_PREM,
            TO_CHAR(UPV.ENDT_EFF_DATE, 'YYYYMMDD') AS ENDT_EFF_DATE,
            ( SELECT SUM(NVL(UPF.DIFF_FEE_AMT,0)) from UWGE_POLICY_FEES UPF
            WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
            AND UPF.VERSION_NO = UPV.VERSION_NO
            AND UPF.FEE_CODE ='SPPA') AS FEE_AMT,
            TO_CHAR(UPB.EFF_DATE, 'YYYYMMDD') AS EFF_DATE,
            TO_CHAR(UPV.ISSUE_DATE, 'YYYYMMDD') AS ISSUE_DATE
            ,(CASE WHEN  LENGTH(UPV.ENDT_CNT||'') =1 THEN '00'||UPV.ENDT_CNT
            WHEN  LENGTH(UPV.ENDT_CNT||'') =2 THEN '0'||UPV.ENDT_CNT
               ELSE UPV.ENDT_CNT||'' END) AS ENDT_NO_CNT
               ,UPV.CONTRACT_ID,UPV.VERSION_NO,UPV.ENDT_RSN_CODE
            from UWGE_POLICY_VERSIONS  UPV
            INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD
            ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
            AND UPV.VERSION_NO =UPCD.VERSION_NO
            INNER JOIN UWGE_POLICY_CONTRACTS UPC
            ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
            INNER JOIN OCP_POLICY_BASES OPB
            ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
            INNER JOIN UWGE_POLICY_BASES UPB
            ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
            AND UPB.VERSION_NO = UPV.VERSION_NO
            INNER JOIN UWPL_POLICY_BASES PLPB
            ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
            AND PLPB.VERSION_NO =UPV.VERSION_NO
            WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'),'[^,]+', 1, level) from dual
            connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'), '[^,]+', 1, level) is not null)
            AND UPC.POLICY_STATUS='C'
            AND UPV.VERSION_NO >1
            AND UPV.ENDT_CODE IN ('96','97')
            --AND UPV.ISSUE_DATE = to_date(P_START_DT,'dd-MON-yy')
            AND UPCD.DLOAD_STATUS ='P'
            AND UPCD.TPA_NAME='SPPA'
            ORDER BY UPV.ENDT_NO ASC;

        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_SPPA_ENDT';
        V_FILE_POL            UTL_FILE.FILE_TYPE;
        V_FILE_WP             UTL_FILE.FILE_TYPE;
        V_FILE_FW             UTL_FILE.FILE_TYPE;
        V_FILE_DEPT           UTL_FILE.FILE_TYPE;
        V_FILE_TTL            UTL_FILE.FILE_TYPE;
        V_FILE_CNCL           UTL_FILE.FILE_TYPE;
        FILENAME1             VARCHAR2(1000);
        FILENAME2             VARCHAR2(1000);
        FILENAME3             VARCHAR2(1000);
        FILENAME4             VARCHAR2(1000);
        FILENAME5             VARCHAR2(1000);
        FILENAME6             VARCHAR2(1000);
        v_file_dir            VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_SPPA_'||V_RUN_HOUR||'00');
        totalNoOfPolicys      NUMBER := 0;
        totalNoOfFws          NUMBER := 0;
        totalGrossPremium     UWGE_COVER.DIFF_COV_BASIC_PREM%TYPE;
        totalTPCAFees         UWGE_COVER_FEES.DIFF_FEE_AMT%TYPE;
        totalGstAmount        NUMBER := 0;
        V_IS_HAVE_DATA        VARCHAR2(1):='N';
        V_NO_OF_RISK_POL      NUMBER := 0;
        V_COV_GROSS_PREM      UWGE_COVER.COV_GROSS_PREM%TYPE;
        V_TOT_TPCA_FEE_POL    UWGE_COVER_FEES.DIFF_FEE_AMT%TYPE;
        V_TOT_GROSS_PREM_POL  UWGE_COVER.DIFF_COV_BASIC_PREM%TYPE;
        V_RET                 NUMBER := 0;
        V_TYPE_OF_ENDT        VARCHAR2 (1);

    BEGIN
        V_STEPS := '001';
        FILENAME1   := 'CSAGIEP'|| V_COUNT_SUCCESS||'.TXT';
        FILENAME2   := 'CSAGIEE'|| V_COUNT_SUCCESS||'.TXT';
        FILENAME3   := 'CSAGIEW'|| V_COUNT_SUCCESS||'.TXT';
        FILENAME4   := 'CSAGIED'|| V_COUNT_SUCCESS||'.TXT';
        FILENAME5   := 'CSAGIEB'|| V_COUNT_SUCCESS||'.TXT';
        FILENAME6   := 'CSAGIET'|| V_COUNT_SUCCESS||'.TXT';

        V_FILE_POL    := UTL_FILE.FOPEN(v_file_dir, FILENAME1, 'W',32767);
        V_FILE_WP   := UTL_FILE.FOPEN(v_file_dir, FILENAME2, 'W',32767);
        V_FILE_FW   := UTL_FILE.FOPEN(v_file_dir, FILENAME3, 'W',32767);
        V_FILE_DEPT   := UTL_FILE.FOPEN(v_file_dir, FILENAME4, 'W',32767);
        V_FILE_TTL   := UTL_FILE.FOPEN(v_file_dir, FILENAME5, 'W',32767);
        V_FILE_CNCL   := UTL_FILE.FOPEN(v_file_dir, FILENAME6, 'W',32767);

        FOR REC IN C_TPA_SPPA
        LOOP
            FOR RECC IN C_TPA_FW(REC.CONTRACT_ID,REC.VERSION_NO)
            LOOP
            IF REC.ENDT_CODE ='72' THEN
                  IF REC.VERSION_NO = RECC.VERSION_NO THEN
                   V_TYPE_OF_ENDT:='A';
                   ELSE
                   IF RECC.TEMINATE_DATE IS NOT NULL THEN
                    V_TYPE_OF_ENDT :='D';
                  ELSE
                    V_TYPE_OF_ENDT :='U';
                  END IF;
                 END IF;
                ELSE
                  IF RECC.TEMINATE_DATE IS NOT NULL THEN
                    V_TYPE_OF_ENDT :='D';
                  ELSE
                    V_TYPE_OF_ENDT :='U';
                  END IF;
                END IF;
            UTL_FILE.PUT_LINE(V_FILE_FW,'AGI|'||REC.POLICY_NO||'|'||REC.ENDT_NO_CNT||'|'||
            RECC.WORKPLACE_REF||'|'||RECC.NationalityCd||'|'
            ||RECC.PASSPORT||'|'||RECC.OccupSectCd||'|'||RECC.FWName||'|'||RECC.SEX||'|'||
            RECC.DateOfBirth||'|'||RECC.WorkPermitExpiry||'|*'||RECC.CARD_NO||'|'||REC.POLICY_NO
            ||'|'||REC.POLICY_NO||RECC.WORKPLACE_REF||'|'||
            REC.POLICY_NO||'-'||RECC.RISK_ID||'-'||RECC.SEQ_NO||'|'||
            REC.COVER_FROM||'|'||REC.ENDT_EFF_DATE||'|'||
            V_TYPE_OF_ENDT||'|'||RECC.CANCEL_CODE||'|'||
            LTRIM (TO_CHAR (NVL(RECC.DIFF_COV_BASIC_PREM,'0.0'), '999999999990.99'))
            ||'|'||LTRIM (TO_CHAR (NVL(RECC.DIFF_COV_FEE_AMT,'0.0'), '999999999990.99'))||'|'||
            RECC.COV_BASIC_PREM||'|'||RECC.COV_FEE_AMT||'|'||RECC.INSURED_FOR||chr(13));

            UTL_FILE.PUT_LINE(V_FILE_WP,'AGI|'||REC.POLICY_NO||'|'||REC.ENDT_NO_CNT||'|'||RECC.WORKPLACE_REF||'|'||
            RECC.WORKPLACE_NAME||'|'||RECC.ADDRESS_LINE1||'|'||RECC.ADDRESS_LINE2||'|'||
            RECC.ADDRESS_LINE3||'|||'||RECC.POSTCODE||'|'||
            REC.STATE_DESC||'|'||RECC.STATE||'|'||REC.OccupSectCd||'|'||REC.POLICY_NO||'|'||
            REC.POLICY_NO||RECC.WORKPLACE_REF||chr(13));
            totalNoOfFws :=NVL (totalNoOfFws,0)+1;
            V_NO_OF_RISK_POL :=NVL (V_NO_OF_RISK_POL,0)+1;
            V_COV_GROSS_PREM :=RECC.COV_GROSS_PREM;
            V_TOT_TPCA_FEE_POL := NVL (V_TOT_TPCA_FEE_POL,0)+RECC.DIFF_COV_FEE_AMT;
            totalTPCAFees := NVL (totalTPCAFees,0)+RECC.DIFF_COV_FEE_AMT;
            V_TOT_GROSS_PREM_POL :=NVL (V_TOT_GROSS_PREM_POL,0)+RECC.DIFF_COV_BASIC_PREM;
            totalGrossPremium :=NVL (totalGrossPremium,0)+RECC.DIFF_COV_BASIC_PREM;
            BEGIN
                FOR GET_NOMINEE
                IN (SELECT A.NOMINEE_NAME,(SELECT CC.CODE_DESC FROM CMGE_CODE CC WHERE CC.CAT_CODE='FW_NOMREL'
                    AND CC.CODE_CD=A.RELATIONSHIP AND ROWNUM=1) AS RELATIONSHIP
                    ,REPLACE(REPLACE(A.ADDRESS_LINE1, CHR(13), ''), CHR(10), '')  AS ADDRESS_LINE1,
                    REPLACE(REPLACE(A.ADDRESS_LINE2, CHR(13), ''), CHR(10), '') AS ADDRESS_LINE2,
                    REPLACE(REPLACE(A.ADDRESS_LINE3, CHR(13), ''), CHR(10), '') AS ADDRESS_LINE3,
                    CASE WHEN A.PHONE_NO IS NULL THEN A.MOBILE_CODE||A.MOBILE_NO
                    ELSE A.PHONE_CODE||A.PHONE_NO END AS MOBILE_NO,
                    TO_NUMBER (TO_CHAR (SYSDATE, 'YYYY')) - TO_NUMBER (TO_CHAR (A.DATE_OF_BIRTH, 'YYYY')) AS NOM_AGE
                    FROM UWPL_RISK_NOMINEE A WHERE   A.CONTRACT_ID =REC.CONTRACT_ID
                    AND A.VERSION_NO = (SELECT MAX (b.version_no)
                    FROM  UWPL_RISK_NOMINEE b
                    WHERE b.contract_id  = REC.CONTRACT_ID
                    AND   a.object_id      = b.object_id
                    AND   b.version_no  <= RECC.POLICY_VERSION
                    AND   b.reversing_version IS NULL)
                    AND A.RISK_ID =RECC.RISK_ID)
             LOOP
             UTL_FILE.PUT_LINE(V_FILE_DEPT,'AGI|'||REC.POLICY_NO||'|'||REC.ENDT_NO_CNT||'|'||
             RECC.NationalityCd||'|'||RECC.PASSPORT||'|'||GET_NOMINEE.NOMINEE_NAME||'|'||
            GET_NOMINEE.RELATIONSHIP||'|'||GET_NOMINEE.NOM_AGE||'|'||GET_NOMINEE.ADDRESS_LINE1||'|'||GET_NOMINEE.ADDRESS_LINE2||'|'||
            GET_NOMINEE.ADDRESS_LINE3||'|||'||GET_NOMINEE.MOBILE_NO||'|'||
            REC.POLICY_NO||'|'||REC.POLICY_NO||'-'||RECC.RISK_ID||'-'||RECC.SEQ_NO||'|'||REC.POLICY_NO||'-'||RECC.RISK_ID||'-'||RECC.SEQ_NO||'-D'
            ||'|'||V_TYPE_OF_ENDT||chr(13));
             END LOOP;
            EXCEPTION
            WHEN OTHERS
            THEN
            PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
            END;
            END LOOP;
            UTL_FILE.PUT_LINE(V_FILE_POL,'AGI|'||REC.ISSUE_OFFICE||'|'||REC.POLICY_NO
            ||'|'||REC.ENDT_NO_CNT||'|'||REC.ID_VALUE1||'|'||REC.NAME_EXT||'|'||REC.ADDRESS_LINE1||'|'||
            REC.ADDRESS_LINE2||'|'||REC.ADDRESS_LINE3||'|||'||REC.POSTCODE||'|'||
            REC.STATE_DESC||'|'||REC.STATE||'|'||REC.OccupSectCd||'|'||REC.MTelNo||'|'||
            ''||'|'||REC.MOBILE_NO||'|'|| REC.EMAIL||'|'||REC.COVER_FROM||'|'||REC.COVER_TILL
            ||'|'||LTRIM (TO_CHAR (NVL(72,'0.0'), '999999999990.99'))||'|'||V_NO_OF_RISK_POL||'|'||
            LTRIM (TO_CHAR (NVL(V_TOT_GROSS_PREM_POL,0), '999999999990.99'))||'|'||REC.ISSUE_DATE||'|'||
            LTRIM (TO_CHAR (NVL(V_TOT_TPCA_FEE_POL,'0.0'), '999999999990.99'))||'|'||
            REC.POLICY_NO||'|'||REC.ISSUE_DATE||'|'||REC.EMPLOYER_TYPE||'|'||REC.AGENT_CODE||chr(13));

            totalNoOfPolicys :=NVL (totalNoOfPolicys,0)+1;
            --totalGrossPremium :=NVL (totalGrossPremium,0)+NVL (REC.GROSS_PREM,0);
            totalGstAmount :=NVL (totalGstAmount,0)+NVL (REC.GST_AMT,0);
            V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.VERSION_NO,'SPPA'); --1.3
            V_NO_OF_RISK_POL :=0;
            V_COV_GROSS_PREM :=0;
            V_TOT_GROSS_PREM_POL:=0;
            V_TOT_TPCA_FEE_POL :=0;
        END LOOP;
           FOR RECCC IN C_TPA_CNCL
            LOOP
                UTL_FILE.PUT_LINE(V_FILE_CNCL,'AGI|'||RECCC.POLICY_NO||'|'||RECCC.ENDT_NO_CNT||'|'
                ||LTRIM (TO_CHAR (NVL(RECCC.GROSS_PREM,'0.0'), '999999999990.99'))||'|'||
                LTRIM (TO_CHAR (NVL(RECCC.FEE_AMT,'0.0'), '999999999990.99'))||'|'||RECCC.ENDT_EFF_DATE||'|'||
                RECCC.ISSUE_DATE||'|'||RECCC.POLICY_NO||'|'||RECCC.ENDT_EFF_DATE||'|'||RECCC.ENDT_RSN_CODE||chr(13));
            totalTPCAFees := NVL (totalTPCAFees,0)+RECCC.FEE_AMT;
            totalGrossPremium :=NVL (totalGrossPremium,0)+RECCC.GROSS_PREM;
            V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(RECCC.CONTRACT_ID,RECCC.VERSION_NO,'SPPA'); --1.3
            END LOOP;
            UTL_FILE.PUT_LINE(V_FILE_TTL,totalNoOfPolicys||'|'||totalNoOfFws||'|'||
           LTRIM (TO_CHAR (NVL(totalGrossPremium,'0.0'), '999999999990.99'))||'|'||
           LTRIM (TO_CHAR (NVL(totalTPCAFees,'0.0'), '999999999990.99'))||chr(13));
           UTL_FILE.FCLOSE(V_FILE_POL);
           UTL_FILE.FCLOSE(V_FILE_TTL);
           UTL_FILE.FCLOSE(V_FILE_FW);
           UTL_FILE.FCLOSE(V_FILE_WP);
           UTL_FILE.FCLOSE(V_FILE_DEPT);
           UTL_FILE.FCLOSE(V_FILE_CNCL);
           EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                    --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);

END PC_TPA_SPPA_ENDT;
PROCEDURE PC_TPA_SPPA_CLAIMS(P_START_DT IN CLNM_PYMT.TRAN_DATE%TYPE,
                            V_COUNT_SUCCESS IN VARCHAR2,V_RUN_HOUR IN VARCHAR2)
 IS
  CURSOR C_TPA_SPPA_CLAIMS
      IS
        SELECT 'AGI' AS INSURER_CD, B.POL_NO, A.CLM_NO,
        CASE WHEN (A.FW_COUNTRY IS NOT NULL ) THEN A.FW_COUNTRY  END AS NationalityCd,
        CASE WHEN (A.FW_PASSPORT IS NOT NULL ) THEN A.FW_PASSPORT  END AS PassportNo,
        CASE WHEN (A.FW_CARD_ID IS NOT NULL ) THEN A.FW_CARD_ID  END AS WorkerId,
        TO_CHAR(A.LOSS_DATE, 'YYYYMMDD') as AccidentDate,
        --to_char(to_date(A.FW_ACCIDENT_TIME, 'DD/MM/YYYY HH:MI:SS AM'), 'DD/MM/YYYY HH24:MI:SS') as AccidentTime
        A.FW_ACCIDENT_TIME as AccidentTime, A.FW_ACCIDENT_PLACE,
        CASE WHEN (A.FW_ACCIDENT_OCCUR IS NOT NULL ) THEN A.FW_ACCIDENT_OCCUR  END AS ACCIDENT_OCCUR,
        CASE WHEN (A.FW_DISABLEMENT_TYPE IS NOT NULL ) THEN A.FW_DISABLEMENT_TYPE  END AS DISABLE_TYPE,
        CASE WHEN (A.LOSS_CAUSE = 'WC01') THEN '01'
        WHEN (A.LOSS_CAUSE = 'WC02') THEN '02'
        WHEN (A.LOSS_CAUSE = 'WC03') THEN '03'
        WHEN (A.LOSS_CAUSE = 'WC04') THEN '04'
        WHEN (A.LOSS_CAUSE = 'WC05') THEN '05'
        WHEN (A.LOSS_CAUSE = 'WC06') THEN '06'
        WHEN (A.LOSS_CAUSE = 'WC07') THEN '07'
        WHEN (A.LOSS_CAUSE = 'WC08') THEN '08'
        WHEN (A.LOSS_CAUSE = 'WC09') THEN '09'
        WHEN (A.LOSS_CAUSE = 'WC10') THEN '10' ELSE '10' END AS LOSS_CODE,
        CASE WHEN (A.FW_INJURY_TYPE IS NOT NULL ) THEN A.FW_INJURY_TYPE END AS INJURY_TYPE,
        TO_CHAR(A.FW_CEASED_DATE,'YYYYMMDD') AS DateCeaseWork, TO_CHAR(A.FW_START_DATE,'YYYYMMDD') AS DateStartWork,
        CASE WHEN (B.PYMT_TYPE = '44') THEN B.PYMT_AMT ELSE 0.00 END AS MEDICAL_AMT,
        CASE WHEN (B.PYMT_TYPE = '46') THEN B.PYMT_AMT ELSE 0.00 END AS REPAT_AMT, 0.00 AS FUNRL_AMOUNT,
        CASE WHEN (B.PYMT_TYPE IN ('40', '41', '43'))THEN B.PYMT_AMT ELSE 0.00 END AS COMPNST_PAYOUT,
        B.PYMT_NO as VoucherNo,TO_CHAR(B.TRAN_DATE,'YYYYMMDD') as VoucherDate,
        (select TO_CHAR(EFF_DATE,'YYYYMMDD') from uwge_risk where CONTRACT_ID=B.contract_id AND ROWNUM=1) AS CVR_FRM,
        A.FW_ACCIDENT_STATE, A.FW_CLM_LEGITIMATE,B.PAYEE,
        A.RISK_ID,(SELECT UR.SEQ_NO FROM UWGE_RISK UR WHERE  UR.CONTRACT_ID =A.CONTRACT_ID
        AND UR.RISK_ID =A.RISK_ID AND ROWNUM =1) AS SEQ_NO
        FROM CLNM_MAST A
        INNER JOIN CLNM_PYMT B ON (A.CLM_NO = B.CLM_NO)
        INNER JOIN acpy_paylink  AP
        ON AP.ac_no =B.APV_NO
        WHERE
        --(A.PROC_YR,A.PROC_MTH) IN (SELECT CAST(YEAR AS INT),CAST(MONTH AS INT) FROM  SAPM_PROC WHERE TYPE_CODE='UW' and CURRENT_DATE BETWEEN START_DATE and END_DATE)
        --AND
        B.PRODUCT_CODE IN (select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'),'[^,]+', 1, level) from dual
            connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'), '[^,]+', 1, level) is not null)
        AND B.SPPA_DATE IS NULL AND B.PYMT_TYPE IN ('44', '46', '40', '41', '43')
        AND B.TRAN_DATE = to_date(P_START_DT,'dd-MON-yy')
        AND B.SPPA_DATE is null;

        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_SPPA_CLAIMS';
        V_FILE_CLAIMS  UTL_FILE.FILE_TYPE;
        V_FILE_SUMMERY  UTL_FILE.FILE_TYPE;
        FILENAME1 VARCHAR2(1000);
        FILENAME2 VARCHAR2(1000);
        v_file_dir VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_SPPA_'||V_RUN_HOUR||'00');
        lDblTTLClaimAmt NUMBER :=0;
        N_POL_NO    NUMBER     :=0;
        N_TTL_CLAIM_AMT NUMBER (18, 2);

    BEGIN
          V_STEPS := '001';
          FILENAME1   := 'CSAGICP'|| V_COUNT_SUCCESS||'.TXT';
          FILENAME2   := 'CSAGICB'|| V_COUNT_SUCCESS||'.TXT';
          V_FILE_CLAIMS    := UTL_FILE.FOPEN(v_file_dir, FILENAME1, 'W',32767);
          V_FILE_SUMMERY   := UTL_FILE.FOPEN(v_file_dir, FILENAME2, 'W',32767);

    FOR REC IN C_TPA_SPPA_CLAIMS
          LOOP
          lDblTTLClaimAmt := NVL(REC.MEDICAL_AMT,0) + NVL(REC.REPAT_AMT,0) + NVL(REC.FUNRL_AMOUNT,0)+ NVL(REC.COMPNST_PAYOUT,0);
          UTL_FILE.PUT_LINE(V_FILE_CLAIMS,REC.INSURER_CD||'|'||REC.POL_NO||'|'||REC.CLM_NO||'|'||REC.NationalityCd||'|'||REC.PassportNo||'|'||REC.WorkerId||'|'||REC.AccidentDate||'|'||
           REC.AccidentTime||'|'||REC.FW_ACCIDENT_PLACE||'|'||REC.ACCIDENT_OCCUR||'|'||REC.DISABLE_TYPE||'|'||REC.LOSS_CODE||'|'||REC.INJURY_TYPE||'|'||REC.DateCeaseWork||'|'||REC.DateStartWork||'|'||
           REC.MEDICAL_AMT||'|'||REC.REPAT_AMT||'|'||REC.FUNRL_AMOUNT||'|'||REC.COMPNST_PAYOUT||'|'||lDblTTLClaimAmt||'|'||REC.VoucherNo||'|'||REC.VoucherDate||'|'||
           REC.POL_NO||'|'||REC.POL_NO||'-'||REC.RISK_ID||'-'||REC.SEQ_NO||'|'||REC.CVR_FRM||'|'||REC.CLM_NO||'|'||REC.FW_ACCIDENT_STATE||'|'||REC.FW_CLM_LEGITIMATE||'|'||
           REC.PAYEE||chr(13));
           N_POL_NO :=NVL(N_POL_NO,0) +1;
           N_TTL_CLAIM_AMT := NVL(N_TTL_CLAIM_AMT,0) + NVL(lDblTTLClaimAmt,0);
           END LOOP;
           UTL_FILE.PUT_LINE(V_FILE_SUMMERY,N_POL_NO||'|'||LTRIM (TO_CHAR (NVL(N_TTL_CLAIM_AMT,'0.0'), '999999999990.99'))||chr(13));

           UTL_FILE.FCLOSE(V_FILE_CLAIMS);
           UTL_FILE.FCLOSE(V_FILE_SUMMERY);
    EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                    --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
    END PC_TPA_SPPA_CLAIMS;

  PROCEDURE PC_TPA_SPPA_REPORT(P_RUN_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE)
   IS
        V_STEPS           VARCHAR2(10);
        V_FUNC_NAME       VARCHAR2(100) :='PC_TPA_SPPA_REPORT';
        V_FILE_REP        UTL_FILE.FILE_TYPE;
        FILENAME1         VARCHAR2(1000);
        v_file_dir        VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_SPPA_DIR');
        V_NO_OF_RISK_POL  NUMBER := 0;
        V_NO_OF_RISK_ENDT NUMBER := 0;
        V_POL_GROSS_PREM  UWGE_POLICY_BASES.GROSS_PREM%TYPE;
        V_POL_FEE_AMT     UWGE_POLICY_FEES.FEE_AMT%TYPE;
        V_ENDT_GROSS_PREM UWGE_POLICY_BASES.DIFF_GROSS_PREM%TYPE;
        V_ENDT_FEE_AMT    UWGE_POLICY_FEES.DIFF_FEE_AMT%TYPE;
        V_START_DT        DATE := Last_Day(ADD_MONTHS(P_RUN_DT,-2))+1;
        V_END_DT          DATE := Last_Day(ADD_MONTHS(P_RUN_DT,-1));
        V_NB_START_DT     DATE;
        V_NB_END_DT       DATE;
        V_RUN_MONTH       NUMBER :=EXTRACT(MONTH FROM ADD_MONTHS(P_RUN_DT,-1));
        V_RUN_YEAR        NUMBER :=EXTRACT(YEAR  FROM ADD_MONTHS(P_RUN_DT,-1));
        V_NO_OF_POL       NUMBER := 0;
        V_NO_OF_ENDT      NUMBER := 0;
        V_CLAIMS_AMT      CLNM_PYMT.PYMT_AMT%TYPE;

    BEGIN
        V_STEPS := '001';
        FILENAME1   := 'AGIBIL'|| TO_CHAR(V_END_DT, 'YYYYMM')||'.TXT';
        V_FILE_REP    := UTL_FILE.FOPEN(v_file_dir, FILENAME1, 'W',32767);
        BEGIN
          SELECT START_DATE,END_DATE INTO V_NB_START_DT,V_NB_END_DT  FROM  SAPM_PROC
          WHERE TYPE_CODE='UW' AND YEAR =V_RUN_YEAR AND MONTH = V_RUN_MONTH;
        EXCEPTION
            WHEN OTHERS
            THEN
               V_NB_START_DT := NULL;
         END;
        BEGIN
            SELECT SUM(NVL(UPB.GROSS_PREM,0))  AS GROSS_PREM,COUNT(1) AS RISK_COUNT ,
            SUM(NVL(UPF.FEE_AMT,0))AS FEE_AMT INTO V_POL_GROSS_PREM,V_NO_OF_RISK_POL,
            V_POL_FEE_AMT
            FROM UWGE_POLICY_VERSIONS  UPV
            INNER JOIN UWGE_POLICY_CONTRACTS UPC
            ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
            INNER JOIN UWGE_POLICY_BASES UPB
            ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
            AND UPB.VERSION_NO = UPV.VERSION_NO
            INNER JOIN UWPL_POLICY_BASES PLPB
            ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
            AND PLPB.VERSION_NO = UPV.VERSION_NO
            INNER JOIN  UWGE_POLICY_FEES UPF
            ON UPF.CONTRACT_ID =UPV.CONTRACT_ID
            AND UPF.VERSION_NO =UPV.VERSION_NO
            AND UPF.FEE_CODE='SPPA'
            WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'),'[^,]+', 1, level) from dual
            connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'), '[^,]+', 1, level) is not null)
            AND UPC.POLICY_STATUS IN('A')
            AND UPV.VERSION_NO =1
            AND NVL(UPB.MST_POL_IND,'N') <>'Y'
            AND UPV.ISSUE_DATE BETWEEN to_date(V_NB_START_DT,'dd-MON-yy') and to_date(V_NB_END_DT,'dd-MON-yy');
         EXCEPTION
            WHEN OTHERS
            THEN
               V_POL_GROSS_PREM := 0;
               V_NO_OF_RISK_POL := 0;
               V_POL_FEE_AMT := 0;
         END;
         BEGIN
                SELECT COUNT(1) AS POLICY_COUNT  INTO V_NO_OF_POL
                FROM UWGE_POLICY_VERSIONS  UPV
                INNER JOIN UWGE_POLICY_CONTRACTS UPC
                ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
                INNER JOIN UWGE_POLICY_BASES UPB
                ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
                AND UPB.VERSION_NO =1
                WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'),'[^,]+', 1, level) from dual
            connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'), '[^,]+', 1, level) is not null)
                AND UPC.POLICY_STATUS IN('A')
                AND UPV.VERSION_NO =1
                AND NVL(UPB.MST_POL_IND,'N') <>'Y'
                AND UPV.ISSUE_DATE BETWEEN to_date(V_NB_START_DT,'dd-MON-yy') and to_date(V_NB_END_DT,'dd-MON-yy');
             EXCEPTION
                WHEN OTHERS
                THEN
                   V_NO_OF_POL := 0;
             END;
         BEGIN
            SELECT SUM(NVL(UPB.DIFF_GROSS_PREM,0))  AS ENDT_GROSS_PREM,COUNT(1) AS ENDT_RISK_COUNT ,
            SUM(NVL(UPF.DIFF_FEE_AMT,0))AS ENDT_FEE_AMT INTO V_ENDT_GROSS_PREM,
            V_NO_OF_RISK_ENDT,V_ENDT_FEE_AMT
            FROM UWGE_POLICY_VERSIONS  UPV
            INNER JOIN UWGE_POLICY_CONTRACTS UPC
            ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
            INNER JOIN UWGE_POLICY_BASES UPB
            ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
            AND UPB.VERSION_NO = UPV.VERSION_NO
            INNER JOIN UWPL_POLICY_BASES PLPB
            ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
            AND PLPB.VERSION_NO = UPV.VERSION_NO
            INNER JOIN  UWGE_POLICY_FEES UPF
            ON UPF.CONTRACT_ID =UPV.CONTRACT_ID
            AND UPF.VERSION_NO =UPV.VERSION_NO
            AND UPF.FEE_CODE='SPPA'
            WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'),'[^,]+', 1, level) from dual
            connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'), '[^,]+', 1, level) is not null)
            AND UPC.POLICY_STATUS IN('A')
            AND UPV.VERSION_NO >1
            AND NVL(UPB.MST_POL_IND,'N') <>'Y'
            AND UPV.UW_YR=V_RUN_YEAR
            AND UPV.UW_MTH=V_RUN_MONTH;
         EXCEPTION
            WHEN OTHERS
            THEN
               V_ENDT_GROSS_PREM := 0;
               V_NO_OF_RISK_ENDT := 0;
               V_ENDT_FEE_AMT := 0;
         END;
        BEGIN
            SELECT COUNT(1) AS ENDT_COUNT  INTO V_NO_OF_ENDT
            FROM UWGE_POLICY_VERSIONS  UPV
            INNER JOIN UWGE_POLICY_CONTRACTS UPC
            ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
            INNER JOIN UWGE_POLICY_BASES UPB
            ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
            AND UPB.VERSION_NO = UPV.VERSION_NO
            WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'),'[^,]+', 1, level) from dual
            connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'), '[^,]+', 1, level) is not null)
            AND UPC.POLICY_STATUS IN('A')
            AND UPV.VERSION_NO >1
            AND NVL(UPB.MST_POL_IND,'N') <>'Y'
            AND UPV.UW_YR=V_RUN_YEAR
            AND UPV.UW_MTH=V_RUN_MONTH;
         EXCEPTION
            WHEN OTHERS
            THEN
               V_NO_OF_ENDT := 0;
         END;
         BEGIN
            SELECT  SUM(NVL(B.PYMT_AMT,0)) INTO V_CLAIMS_AMT
            FROM CLNM_MAST A
            INNER JOIN CLNM_PYMT B ON (A.CLM_NO = B.CLM_NO)
            WHERE A.PROC_YR = V_RUN_YEAR AND A.PROC_MTH=V_RUN_MONTH
            AND B.PRODUCT_CODE IN (select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'),'[^,]+', 1, level) from dual
            connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'FWCS'), '[^,]+', 1, level) is not null)
            AND B.SPPA_DATE IS NULL AND B.PYMT_TYPE IN ('44', '46', '40', '41', '43')
            AND B.TRAN_DATE BETWEEN to_date(V_START_DT,'dd-MON-yy') and to_date(V_END_DT,'dd-MON-yy');
             EXCEPTION
                WHEN OTHERS
                THEN
                   V_CLAIMS_AMT := 0;
             END;
        UTL_FILE.PUT_LINE(V_FILE_REP,'ALLIANZ GENERAL INSURANCE COMPANY (MALAYSIA) BERHAD');
        UTL_FILE.PUT_LINE(V_FILE_REP,'Total New Policies Issued for the Month                :'||NVL(V_NO_OF_POL,0));
        UTL_FILE.PUT_LINE(V_FILE_REP,'Total Gross Premium on New Policies for the Month      :'||LTRIM (TO_CHAR (NVL(V_POL_GROSS_PREM,'0.0'), '999999999990.99')));
        UTL_FILE.PUT_LINE(V_FILE_REP,'Total Service Fees on New Policies for the Month       :'||LTRIM (TO_CHAR (NVL(V_POL_FEE_AMT,'0.0'), '999999999990.99')));
        UTL_FILE.PUT_LINE(V_FILE_REP,'Total Workman Insured under New Policies for the Month :'||NVL(V_NO_OF_RISK_POL,0));
        UTL_FILE.PUT_LINE(V_FILE_REP,'Total Endorsement Policies Raised for the Month        :'||NVL(V_NO_OF_ENDT,0));
        UTL_FILE.PUT_LINE(V_FILE_REP,'Total Endorsed Gross Premium for the Month             :'||LTRIM (TO_CHAR (NVL(V_ENDT_GROSS_PREM,'0.0'), '999999999990.99')));
        UTL_FILE.PUT_LINE(V_FILE_REP,'Total Endorsed Service Fees for the Month              :'||LTRIM (TO_CHAR (NVL(V_ENDT_FEE_AMT,'0.0'), '999999999990.99')));
        UTL_FILE.PUT_LINE(V_FILE_REP,'Total Workman Insured under Endt for the Month         :'||NVL(V_NO_OF_RISK_ENDT,0));
        UTL_FILE.PUT_LINE(V_FILE_REP,'Total Claims Paid for the month                        :'||LTRIM (TO_CHAR (NVL(V_CLAIMS_AMT,'0.0'), '999999999990.99')));
        UTL_FILE.PUT_LINE(V_FILE_REP,'Start Date                                             :'||TO_CHAR(V_START_DT, 'DD-MM-YYYY'));
        UTL_FILE.PUT_LINE(V_FILE_REP,'End Date                                               :'||TO_CHAR(V_END_DT, 'DD-MM-YYYY'));
           UTL_FILE.FCLOSE(V_FILE_REP);
           EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                    --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);

    END PC_TPA_SPPA_REPORT;
--1.2

 --1.6 start
    PROCEDURE PC_TPA_AAN_CHECKSUM(P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE) IS

    CURSOR C_TPA_AAN_DPL
      IS
        select A.TPA_NAME,CASE WHEN A.VERSION_NO =1 THEN b.POLICY_REF
        ELSE (select UPV.ENDT_NO from UWGE_POLICY_VERSIONS  UPV
        WHERE UPV.CONTRACT_ID =A.CONTRACT_ID
        AND UPV.VERSION_NO =A.VERSION_NO) END AS POLICY_REF,
        CASE WHEN A.VERSION_NO =1 THEN 'POLICY' ELSE 'ENDT' END AS TRRANCTION_TYPE
        ,A.DLOAD_DATE,(SELECT count(*) FROM customer.SB_UWGE_RISK RR
        WHERE RR.CONTRACT_ID = A.CONTRACT_ID and RR.POLICY_VERSION=A.VERSION_NO ) AS RISK_COUNT
        ,( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =A.CONTRACT_ID
        AND UPF.VERSION_NO =A.VERSION_NO
        AND UPF.FEE_CODE ='IMA') AS IMA_FEE_AMT,
        (SELECT COUNT(*) FROM UWGE_COVER CSUB WHERE a.CONTRACT_ID =CSUB.CONTRACT_ID
        AND A.VERSION_NO=CSUB.VERSION_NO AND CSUB.COV_CODE IN ('OP','OP1','OP2') ) AS OP_SUB_COV
        ,UPC.PRODUCT_CONFIG_CODE
        from UWGE_POLICY_CTRL_DLOAD A,OCP_POLICY_BASES b,UWGE_POLICY_CONTRACTS UPC
        where A.CONTRACT_ID=b.CONTRACT_ID AND A.CONTRACT_ID =UPC.CONTRACT_ID
        AND  A.DLOAD_DATE  >= to_timestamp(sysdate||' 00:00:00', 'dd-MON-yy hh24:mi:ss')
        AND A.DLOAD_DATE  <= to_timestamp(sysdate||' 23:59:59', 'dd-MON-yy hh24:mi:ss')
        AND A.DLOAD_STATUS ='D' and a.TPA_NAME='AAN';

     CURSOR C_TPA_AAN_TTL_DPL  --1.7
      IS
        SELECT CASE WHEN UPV.VERSION_NO =1 THEN OPB.POLICY_REF ELSE UPV.ENDT_NO END AS POLICY_REF
        ,UPV.VERSION_NO AS POLICY_VERSION,'AAN_MISC' as DD_TYPE,
        NVL((SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='ASST'),0) AS ASST_FEE_AMT,
        NVL((SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='DMA'),0) AS DMA_FEE_AMT,
        0.00 AS OCR_FEE_AMT,
        0.00 AS CRF_FEE_AMT,0.00 AS DEALER3A_FEE,
        0.00 AS IMA_FEE_AMT,0.00 AS MCO_FEE_AMT,
        0.00 AS MCOI_FEE_AMT,0.00 AS MCOO_FEE_AMT,
        UPC.PRODUCT_CONFIG_CODE,
        (SELECT count(*) FROM customer.SB_UWGE_RISK RR
        WHERE RR.CONTRACT_ID = UPCD.CONTRACT_ID and RR.POLICY_VERSION=UPCD.VERSION_NO )AS RISK_COUNT,
        'AAN' as TPA_NAME,CASE WHEN UPCD.VERSION_NO =1 THEN 'POLICY' ELSE 'ENDT' END AS TRRANCTION_TYPE
        ,0 AS OP_SUB_COV
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
    INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_MS'),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_MS'), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS IN('A','C','E')
        AND PLPB.TPA_NAME = 'A'
        AND (UPV.ENDT_CODE IS NULL OR UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE_R,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE_R, '[^,]+', 1, level) is not null))
        AND UPV.ACTION_CODE IN('A','C')
        AND UPC.LOB='MS'
        AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy')
    AND UPCD.DLOAD_STATUS ='P'
        UNION ALL --1.4
        SELECT CASE WHEN UPV.VERSION_NO =1 THEN OPB.POLICY_REF ELSE UPV.ENDT_NO END AS POLICY_REF
        ,UPV.VERSION_NO AS POLICY_VERSION,'AAN_MISC' as DD_TYPE,
        NVL((SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='ASST'),0) AS ASST_FEE_AMT,
        NVL((SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='DMA'),0) AS DMA_FEE_AMT,
        0.00 AS OCR_FEE_AMT,
        0.00 AS CRF_FEE_AMT,0.00 AS DEALER3A_FEE,
        0.00 AS IMA_FEE_AMT,0.00 AS MCO_FEE_AMT,
        0.00 AS MCOI_FEE_AMT,0.00 AS MCOO_FEE_AMT,
        UPC.PRODUCT_CONFIG_CODE,
        (SELECT count(*) FROM customer.SB_UWGE_RISK RR
        WHERE RR.CONTRACT_ID = UPCD.CONTRACT_ID and RR.POLICY_VERSION=UPCD.VERSION_NO ) AS RISK_COUNT,
        'AAN' as TPA_NAME,CASE WHEN UPCD.VERSION_NO =1 THEN 'POLICY' ELSE 'ENDT' END AS TRRANCTION_TYPE
        ,0 AS OP_SUB_COV
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_MS'),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_MS'), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS IN('A','C','E')
        AND PLPB.TPA_NAME = 'A'
        AND UPV.VERSION_NO >1
        AND (UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE_A,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE_A, '[^,]+', 1, level) is not null))
        AND UPV.ACTION_CODE IN('A','C')
        AND UPC.LOB='MS'
        AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy')
    AND UPCD.DLOAD_STATUS ='P'
        UNION ALL
        SELECT CASE WHEN UPV.VERSION_NO =1 THEN OPB.POLICY_REF ELSE UPV.ENDT_NO END AS POLICY_REF
        ,UPV.VERSION_NO AS POLICY_VERSION,'AAN_Towing' as DD_TYPE,
        (SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='ASST') AS ASST_FEE_AMT,
        0.00 AS  DMA_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='OCR') AS OCR_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='CRF') AS CRF_FEE_AMT,
        0.00 AS  DEALER3A_FEE,
        0.00 AS  IMA_FEE_AMT,0.00 AS  MCO_FEE_AMT,
        0.00 AS  MCOI_FEE_AMT,0.00 AS  MCOO_FEE_AMT,
        UPC.PRODUCT_CONFIG_CODE,
        (SELECT count(*) FROM customer.SB_UWGE_RISK RR
        WHERE RR.CONTRACT_ID = UPCD.CONTRACT_ID and RR.POLICY_VERSION=UPCD.VERSION_NO ) AS RISK_COUNT,
        'AAN' as TPA_NAME,CASE WHEN UPCD.VERSION_NO =1 THEN 'POLICY' ELSE 'ENDT' END AS TRRANCTION_TYPE
        ,0 AS OP_SUB_COV
        from UWGE_POLICY_VERSIONS  UPV
    INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_TOWING'),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_TOWING'), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS ='A'
        AND (UPV.ENDT_CODE IS NULL OR UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE_R,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE_R, '[^,]+', 1, level) is not null ))
        AND UPV.ACTION_CODE IN('A','C')
        AND PLPB.TPA_NAME = 'AAN'
       AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy')
       AND UPCD.DLOAD_STATUS ='P'
        UNION ALL
        SELECT CASE WHEN UPV.VERSION_NO =1 THEN OPB.POLICY_REF ELSE UPV.ENDT_NO END AS POLICY_REF
        ,UPV.VERSION_NO AS POLICY_VERSION,'AAN_Towing' as DD_TYPE,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='ASST') AS ASST_FEE_AMT,
         0.00 AS  DMA_FEE_AMT,
        (SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='OCR') AS OCR_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='CRF') AS CRF_FEE_AMT,
        0.00 AS  DEALER3A_FEE,
        0.00 AS  IMA_FEE_AMT,0.00 AS  MCO_FEE_AMT,
        0.00 AS  MCOI_FEE_AMT,0.00 AS  MCOO_FEE_AMT,
        UPC.PRODUCT_CONFIG_CODE,
        (SELECT count(*) FROM customer.SB_UWGE_RISK RR
        WHERE RR.CONTRACT_ID = UPCD.CONTRACT_ID and RR.POLICY_VERSION=UPCD.VERSION_NO ) AS RISK_COUNT,
        'AAN' as TPA_NAME,CASE WHEN UPCD.VERSION_NO =1 THEN 'POLICY' ELSE 'ENDT' END AS TRRANCTION_TYPE
        ,0 AS OP_SUB_COV
        from UWGE_POLICY_VERSIONS  UPV
    INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_TOWING'),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_TOWING'), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS ='A'
        AND UPV.VERSION_NO >1
        AND (UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE_A,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE_A, '[^,]+', 1, level) is not null))
        AND UPV.ACTION_CODE IN('A','C')
        AND PLPB.TPA_NAME = 'AAN'
        AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy')
        AND UPCD.DLOAD_STATUS ='P'
        UNION ALL
        SELECT CASE WHEN UPV.VERSION_NO =1 THEN OPB.POLICY_REF ELSE UPV.ENDT_NO END AS POLICY_REF
        ,UPV.VERSION_NO AS POLICY_VERSION,'AAN_Motor_Towing' as DD_TYPE,
        0.00 AS  ASST_FEE_AMT,0.00 AS  DMA_FEE_AMT,0.00 AS  OCR_FEE_AMT,
        0.00 AS  CRF_FEE_AMT,UPM.DEALER3A_FEE,
        0.00 AS  IMA_FEE_AMT,0.00 AS  MCO_FEE_AMT,
        0.00 AS  MCOI_FEE_AMT,0.00 AS  MCOO_FEE_AMT,
        UPC.PRODUCT_CONFIG_CODE,
        (SELECT count(*) FROM customer.SB_UWGE_RISK RR
        WHERE RR.CONTRACT_ID = UPCD.CONTRACT_ID and RR.POLICY_VERSION=UPCD.VERSION_NO ) AS RISK_COUNT,
        'AAN' as TPA_NAME,CASE WHEN UPCD.VERSION_NO =1 THEN 'POLICY' ELSE 'ENDT' END AS TRRANCTION_TYPE
        ,0 AS OP_SUB_COV
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_MT UPM
        ON UPM.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPM.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWGE_POLICY_BASES UPB  ---2.1 start
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWGE_RISK_VEH URV
        ON URV.CONTRACT_ID =UPV.CONTRACT_ID
        AND URV.VERSION_NO =UPV.VERSION_NO

        INNER JOIN
               ( Select * from
                  (select UPM.Contract_ID, UPM.Version_no, UPC.Product_Config_Code, UPC.Lob, UPC.Policy_Status, UPM.DEALER3A_FEE, '0' as Benz_ind 
                         from UWGE_POLICY_CONTRACTS UPC,       UWGE_POLICY_MT UPM
                    Where UPC.CONTRACT_ID = UPM.CONTRACT_ID
                      AND (UPC.PRODUCT_CONFIG_CODE in( 
                            select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_TOWING'),'[^,]+', 1, level) from dual--2.1 end
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_TOWING'), '[^,]+', 1, level) is not null)
                      AND UPM.DEALER_PROVIDER = 'AAN') -- 2.1 start
                    Union
                   select UPM.Contract_ID, UPM.Version_no, UPC.Product_Config_Code, UPC.Lob, UPC.Policy_Status, UPM.DEALER3A_FEE, '1' as Benz_ind 
                         from UWGE_POLICY_CONTRACTS UPC, UWGE_POLICY_MT UPM
                    Where UPC.CONTRACT_ID = UPM.CONTRACT_ID
                      AND UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'MERCEDES_BENZ_PRODUCTS'),'[^,]+', 1, level)
                                                       from dual
                                                       connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'MERCEDES_BENZ_PRODUCTS'), '[^,]+', 1, level) is not null)
                      --AND CMV1.VEH_MODEL_CODE=URV.VEH_MODEL)
                      AND (UPM.DEALER_PROVIDER = 'AAN' OR UPM.DEALER_PROVIDER IS NULL)
                  ))  UPC_UPM
         ON UPC_UPM.Contract_ID = UPV.Contract_ID AND UPC_UPM.Version_no = UPV.Version_No
        WHERE 1=1       --2.1 end      

        AND UPC.POLICY_STATUS ='A'
        --AND UPM.DEALER_PROVIDER = 'AAN' -- 2.1 -- moved this condition to up (other than MERCEDES)
         ---2.1 end
        AND (UPV.ENDT_CODE IS NULL OR UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE_R,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE_R, '[^,]+', 1, level) is not null))
        AND UPV.ACTION_CODE IN('A','C')
        AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy')
        AND UPCD.DLOAD_STATUS ='P'
        --- 2.1 start
        AND ((UPC_UPM.Benz_ind = '1'
         AND URV.VEH_MODEL IN (SELECT CMV1.VEH_MODEL_CODE FROM CMUW_MODEL_VEH CMV1 WHERE UPPER(CMV1.VEH_MODEL_DESC) LIKE '%MERCEDES%' AND CMV1.VEH_MODEL_CODE=URV.VEH_MODEL)
         AND UPB.AGENT_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'MERCEDES_BENZ_AGENTS'),'[^,]+', 1, level) from dual
                               connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'MERCEDES_BENZ_AGENTS'), '[^,]+', 1, level) is not null)
         )
         OR
         UPC_UPM.Benz_ind = '0'
        )--- 2.1 end
        UNION ALL
        SELECT CASE WHEN UPV.VERSION_NO =1 THEN OPB.POLICY_REF ELSE UPV.ENDT_NO END AS POLICY_REF
        ,UPV.VERSION_NO AS POLICY_VERSION,'AAN_Motor_Towing' as DD_TYPE,
        0.00 AS  ASST_FEE_AMT,0.00 AS  DMA_FEE_AMT,0.00 AS  OCR_FEE_AMT,
        0.00 AS  CRF_FEE_AMT,UPM.DEALER3A_FEE,
        0.00 AS  IMA_FEE_AMT,0.00 AS  MCO_FEE_AMT,
        0.00 AS  MCOI_FEE_AMT,0.00 AS  MCOO_FEE_AMT,
        UPC.PRODUCT_CONFIG_CODE,
        (SELECT count(*) FROM customer.SB_UWGE_RISK RR
        WHERE RR.CONTRACT_ID = UPCD.CONTRACT_ID and RR.POLICY_VERSION=UPCD.VERSION_NO ) AS RISK_COUNT,
        'AAN' as TPA_NAME,CASE WHEN UPCD.VERSION_NO =1 THEN 'POLICY' ELSE 'ENDT' END AS TRRANCTION_TYPE
        ,0 AS OP_SUB_COV
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_MT UPM
        ON UPM.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPM.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWGE_POLICY_BASES UPB  ---2.1 start
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWGE_RISK_VEH URV
        ON URV.CONTRACT_ID =UPV.CONTRACT_ID
        AND URV.VERSION_NO =(SELECT MAX (b.version_no)
        FROM UWGE_RISK_VEH b
        WHERE b.contract_id = UPV.CONTRACT_ID
        AND URV.object_id = b.object_id
        AND b.version_no <= UPV.VERSION_NO
        AND b.reversing_version IS NULL)
        AND URV.action_code <> 'D'
                INNER JOIN
               ( Select * from
                  (select UPM.Contract_ID, UPM.Version_no, UPC.Product_Config_Code, UPC.Lob, UPC.Policy_Status, UPM.DEALER3A_FEE, '0' as Benz_ind 
                         from UWGE_POLICY_CONTRACTS UPC, UWGE_POLICY_MT UPM
                    Where UPC.CONTRACT_ID = UPM.CONTRACT_ID
                      AND UPC.PRODUCT_CONFIG_CODE in(  
                            select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_TOWING'),'[^,]+', 1, level) from dual
        --2.1 end
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_TOWING'), '[^,]+', 1, level) is not null)
                      AND UPM.DEALER_PROVIDER = 'AAN' -- 2.1
        AND UPC.POLICY_STATUS ='A'
                    --2.1 start
                    Union
                   select UPM.Contract_ID, UPM.Version_no, UPC.Product_Config_Code, UPC.Lob,UPC.Policy_Status, UPM.DEALER3A_FEE, '1' as Benz_ind 
                        from UWGE_POLICY_CONTRACTS UPC, UWGE_POLICY_MT UPM
                    Where UPC.CONTRACT_ID = UPM.CONTRACT_ID
                      AND UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'MERCEDES_BENZ_PRODUCTS'),'[^,]+', 1, level)
                                                       from dual
                                                       connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'MERCEDES_BENZ_PRODUCTS'), '[^,]+', 1, level) is not null)
                      AND (UPM.DEALER_PROVIDER = 'AAN' OR UPM.DEALER_PROVIDER IS NULL)
                      AND UPC.POLICY_STATUS IN('A','C','E')
                  ))  UPC_UPM
         ON UPC_UPM.Contract_ID = UPV.Contract_ID AND UPC_UPM.Version_no = UPV.Version_No
        WHERE 1=1                 

        --AND UPC.POLICY_STATUS ='A' -- moved this condition to up (other than MERCEDES)            
        --AND UPM.DEALER_PROVIDER = 'AAN' -- 2.1 -- moved this condition to up (other than MERCEDES)
         ---2.1 end    
        AND UPV.VERSION_NO >1
        AND UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE_A,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE_A, '[^,]+', 1, level) is not null)
        AND UPV.ACTION_CODE IN('A','C')
        AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy')
        AND UPCD.DLOAD_STATUS ='P'
        -- 2.1 start ; --1.3
            AND ((UPC_UPM.Benz_ind = '1'
          AND URV.VEH_MODEL IN (SELECT CMV1.VEH_MODEL_CODE FROM CMUW_MODEL_VEH CMV1 WHERE UPPER(CMV1.VEH_MODEL_DESC) LIKE '%MERCEDES%' AND CMV1.VEH_MODEL_CODE=URV.VEH_MODEL)
          AND UPB.AGENT_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'MERCEDES_BENZ_AGENTS'),'[^,]+', 1, level) from dual
                                connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'MERCEDES_BENZ_AGENTS'), '[^,]+', 1, level) is not null)
          )
          OR
          UPC_UPM.Benz_ind = '0'
                )-- 2.1 end
        UNION ALL
        SELECT CASE WHEN UPV.VERSION_NO =1 THEN OPB.POLICY_REF ELSE UPV.ENDT_NO END AS POLICY_REF
        ,UPV.VERSION_NO AS POLICY_VERSION,'AAN_PA' as DD_TYPE,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='ASST') AS ASST_FEE_AMT,
         0.00 AS  DMA_FEE_AMT, 0.00 AS  OCR_FEE_AMT,
         0.00 AS  CRF_FEE_AMT,0.00 AS  DEALER3A_FEE,
       (SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='IMA') AS IMA_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='MCO') AS MCO_FEE_AMT,
         ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='MCOI') AS MCOI_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='MCOO') AS MCOO_FEE_AMT,
        UPC.PRODUCT_CONFIG_CODE,
        (SELECT count(*) FROM customer.SB_UWGE_RISK RR
        WHERE RR.CONTRACT_ID = UPCD.CONTRACT_ID and RR.POLICY_VERSION=UPCD.VERSION_NO )  AS RISK_COUNT,
        'AAN' as TPA_NAME,CASE WHEN UPCD.VERSION_NO =1 THEN 'POLICY' ELSE 'ENDT' END AS TRRANCTION_TYPE
        ,(SELECT COUNT(*) FROM UWGE_COVER CSUB WHERE UPV.CONTRACT_ID =CSUB.CONTRACT_ID
        AND UPV.VERSION_NO=CSUB.VERSION_NO AND CSUB.COV_CODE IN ('OP','OP1','OP2') ) AS OP_SUB_COV
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_PA'),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_PA'), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS IN('A','C','E')
        AND PLPB.TPA_NAME = 'AAN'
        AND UPV.ACTION_CODE IN('A','C')
        AND (UPV.ENDT_CODE IS NULL OR UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE, '[^,]+', 1, level) is not null  )) --1.4
        AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy')
        AND UPCD.DLOAD_STATUS ='P'
        UNION ALL
        SELECT CASE WHEN UPV.VERSION_NO =1 THEN OPB.POLICY_REF ELSE UPV.ENDT_NO END AS POLICY_REF
        ,UPV.VERSION_NO AS POLICY_VERSION,'AAN_HC' as DD_TYPE,
        0.00 AS  ASST_FEE_AMT,
        (SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='MCODMA') AS DMA_FEE_AMT,
        0.00 AS  OCR_FEE_AMT,0.00 AS  CRF_FEE_AMT,
        0.00 AS  DEALER3A_FEE,
        (SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='IMA') AS IMA_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='MCO') AS MCO_FEE_AMT,
         ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='MCOI') AS MCOI_FEE_AMT,
        ( SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='MCOO') AS MCOO_FEE_AMT,
        UPC.PRODUCT_CONFIG_CODE,
        (SELECT count(*) FROM customer.SB_UWGE_RISK RR
        WHERE RR.CONTRACT_ID = UPCD.CONTRACT_ID and RR.POLICY_VERSION=UPCD.VERSION_NO ) AS RISK_COUNT,
        'AAN' as TPA_NAME,CASE WHEN UPCD.VERSION_NO =1 THEN 'POLICY' ELSE 'ENDT' END AS TRRANCTION_TYPE
        ,(SELECT COUNT(*) FROM UWGE_COVER CSUB WHERE UPV.CONTRACT_ID =CSUB.CONTRACT_ID
        AND UPV.VERSION_NO=CSUB.VERSION_NO AND CSUB.COV_CODE IN ('OP','OP1','OP2') ) AS OP_SUB_COV
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        WHERE UPC.PRODUCT_CONFIG_CODE IN(select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_HC'),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_HC'), '[^,]+', 1, level) is not null)
        AND UPC.POLICY_STATUS IN('A','C','E')
        AND UPV.ACTION_CODE IN('A','C')
        AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy')
        AND (UPV.ENDT_CODE IS NULL OR UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE, '[^,]+', 1, level) is not null ))
        AND (PLPB.TPA_NAME = 'AAN' OR PLPB.TPA_NAME IS NULL)
        AND UPCD.DLOAD_STATUS ='P';

        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_AAN_TTL_DPL';
        FILENAME  UTL_FILE.FILE_TYPE;
        FILENAME1 VARCHAR2(1000);
        v_file_dir VARCHAR2 (100) :=PG_TPA_UTILS.FN_GET_SYS_PARAM( 'PG_RNGE_POL', 'DWN_DIR');
        rowIDx number := 2;
        V_OCR VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'OCR_TOWING');
        V_CRF VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'CRF_TOWING');
        V_NPOL VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'POL_TOWING');
        V_ASST VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'PA_ASST');
        V_IMA VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'PA_IMA');
        V_MCO VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'PA_MCO');
        V_NPOL1 VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'POL_PA');
        V_HC_MCOI VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'HC_MCOI');
        V_HC_MCOO VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'HC_MCOO');
        V_HC_DMA VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'HC_DMA');
        V_FILE_NAME_LIST PG_TPA_UTILS.p_array_v
                                      := PG_TPA_UTILS.p_array_v ();
       arrayCount number :=3;
       V_Return varchar2(10);
       rowIDx1 number := 2;
       rowIDx2 number := 2;
       BEGIN

          V_STEPS := '001';
      FILENAME1   := TO_CHAR(P_START_DT, 'YYYYMMDD')||'_AAN_POLEND.xlsx';

        PG_EXCEL_UTILS.clear_workbook;
        PG_EXCEL_UTILS.new_sheet('ISSUED');
        PG_EXCEL_UTILS.SET_ROW( 1,p_fontId =>  PG_EXCEL_UTILS.get_font('Arial',p_bold => true), p_alignment => PG_EXCEL_UTILS.get_alignment( p_horizontal => 'center' ), p_fillId => PG_EXCEL_UTILS.get_fill( 'solid', '00009900' ),p_borderId => PG_EXCEL_UTILS.get_border( 'thin', 'thin', 'thin', 'thin' ),p_sheet => 1 ) ;
        PG_EXCEL_UTILS.CELL(1,1,'Policy Number',p_sheet => 1);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(1,32,p_sheet => 1);
        PG_EXCEL_UTILS.CELL(2,1,'TPA Name',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(2,10,p_sheet => 1);
        PG_EXCEL_UTILS.CELL(3,1,'Product Code',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(3,15,p_sheet => 1);
        PG_EXCEL_UTILS.CELL(4,1,'Risk Count',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(4,10,p_sheet => 1);
        PG_EXCEL_UTILS.CELL(5,1,'Issue Type',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(5,15,p_sheet => 1);
        PG_EXCEL_UTILS.CELL(6,1,'OP' ,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(6,10,p_sheet => 1);
        PG_EXCEL_UTILS.CELL(7,1,'IMA',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(7,10,p_sheet => 1);

        PG_EXCEL_UTILS.new_sheet('DOWNLOAD SUCCESS');
        PG_EXCEL_UTILS.SET_ROW( 1,p_fontId =>  PG_EXCEL_UTILS.get_font('Arial',p_bold => true), p_alignment => PG_EXCEL_UTILS.get_alignment( p_horizontal => 'center' ), p_fillId => PG_EXCEL_UTILS.get_fill( 'solid', '00009900' ),p_borderId => PG_EXCEL_UTILS.get_border( 'thin', 'thin', 'thin', 'thin' ),p_sheet => 2 ) ;
        PG_EXCEL_UTILS.CELL(1,1,'Policy Number',p_sheet => 2);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(1,32,p_sheet => 2);
        PG_EXCEL_UTILS.CELL(2,1,'TPA Name',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 2);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(2,10,p_sheet => 2);
        PG_EXCEL_UTILS.CELL(3,1,'Product Code',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 2);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(3,15,p_sheet => 2);
        PG_EXCEL_UTILS.CELL(4,1,'Risk Count',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 2);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(4,10,p_sheet => 2);
        PG_EXCEL_UTILS.CELL(5,1,'Issue Type',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 2);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(5,15,p_sheet => 2);
        PG_EXCEL_UTILS.CELL(6,1,'OP',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 2);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(6,10,p_sheet => 2);
        PG_EXCEL_UTILS.CELL(7,1,'IMA',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 2);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(7,10,p_sheet => 2);
    PG_EXCEL_UTILS.new_sheet('DOWNLOAD FAIL');
        PG_EXCEL_UTILS.SET_ROW( 1,p_fontId =>  PG_EXCEL_UTILS.get_font('Arial',p_bold => true), p_alignment => PG_EXCEL_UTILS.get_alignment( p_horizontal => 'center' ), p_fillId => PG_EXCEL_UTILS.get_fill( 'solid', '00009900' ),p_borderId => PG_EXCEL_UTILS.get_border( 'thin', 'thin', 'thin', 'thin' ),p_sheet => 3 ) ;
        PG_EXCEL_UTILS.CELL(1,1,'Policy Number',p_sheet => 3);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(1,32,p_sheet => 3);
        PG_EXCEL_UTILS.CELL(2,1,'TPA Name',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(2,10,p_sheet => 3);
        PG_EXCEL_UTILS.CELL(3,1,'Product Code',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(3,15,p_sheet => 3);
        PG_EXCEL_UTILS.CELL(4,1,'Risk Count',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(4,10,p_sheet => 3);
        PG_EXCEL_UTILS.CELL(5,1,'Issue Type',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(5,15,p_sheet => 3);
        PG_EXCEL_UTILS.CELL(6,1,'OP',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(6,10,p_sheet => 3);
        PG_EXCEL_UTILS.CELL(7,1,'IMA',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
        PG_EXCEL_UTILS.SET_COLUMN_WIDTH(7,10,p_sheet => 3);

    FOR REC IN C_TPA_AAN_DPL
          LOOP
            PG_EXCEL_UTILS.SET_ROW( rowIDx, p_borderId => PG_EXCEL_UTILS.get_border( 'thin', 'thin', 'thin', 'thin' ),p_sheet => 1 ) ;
            PG_EXCEL_UTILS.SET_ROW( rowIDx1, p_borderId => PG_EXCEL_UTILS.get_border( 'thin', 'thin', 'thin', 'thin' ),p_sheet => 2 ) ;
            PG_EXCEL_UTILS.CELL(1,rowIDx,REC.POLICY_REF,p_sheet => 1);
            PG_EXCEL_UTILS.CELL(2,rowIDx,REC.TPA_NAME,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
            PG_EXCEL_UTILS.CELL(3,rowIDx,REC.PRODUCT_CONFIG_CODE, p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
            PG_EXCEL_UTILS.CELL(4,rowIDx,REC.RISK_COUNT, p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
            PG_EXCEL_UTILS.CELL(5,rowIDx,REC.TRRANCTION_TYPE, p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
            IF REC.OP_SUB_COV >0 THEN
              PG_EXCEL_UTILS.CELL(6,rowIDx,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
            ELSE
              PG_EXCEL_UTILS.CELL(6,rowIDx,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
            END IF;
            IF REC.IMA_FEE_AMT >0 THEN
              PG_EXCEL_UTILS.CELL(7,rowIDx,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
            ELSE
              PG_EXCEL_UTILS.CELL(7,rowIDx,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
            END IF;
            rowIDx :=rowIDx+1;
            PG_EXCEL_UTILS.CELL(1,rowIDx1,REC.POLICY_REF,p_sheet => 2);
            PG_EXCEL_UTILS.CELL(2,rowIDx1,REC.TPA_NAME,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 2);
            PG_EXCEL_UTILS.CELL(3,rowIDx1,REC.PRODUCT_CONFIG_CODE, p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 2);
            PG_EXCEL_UTILS.CELL(4,rowIDx1,REC.RISK_COUNT,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 2);
            PG_EXCEL_UTILS.CELL(5,rowIDx1,REC.TRRANCTION_TYPE, p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 2);
            IF REC.OP_SUB_COV >0 THEN
              PG_EXCEL_UTILS.CELL(6,rowIDx1,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 2);
            ELSE
              PG_EXCEL_UTILS.CELL(6,rowIDx1,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 2);
            END IF;
            IF REC.IMA_FEE_AMT >0 THEN
              PG_EXCEL_UTILS.CELL(7,rowIDx1,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 2);
            ELSE
              PG_EXCEL_UTILS.CELL(7,rowIDx1,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 2);
            END IF;
            rowIDx1 :=rowIDx1+1;
      END LOOP;
          FOR REC IN C_TPA_AAN_TTL_DPL
          LOOP
          IF REC.DD_TYPE='AAN_MISC'
          THEN
       IF (REC.POLICY_VERSION =1 AND REC.ASST_FEE_AMT >0 AND REC.PRODUCT_CONFIG_CODE ='012104')
            OR (REC.POLICY_VERSION >1 AND REC.ASST_FEE_AMT >=0 AND REC.PRODUCT_CONFIG_CODE ='012104')
            OR (REC.POLICY_VERSION >1 AND REC.DMA_FEE_AMT >=0 AND REC.PRODUCT_CONFIG_CODE ='012102')
            OR (REC.POLICY_VERSION =1 AND REC.DMA_FEE_AMT >0 AND REC.PRODUCT_CONFIG_CODE ='012102')
           THEN
                PG_EXCEL_UTILS.SET_ROW( rowIDx, p_borderId => PG_EXCEL_UTILS.get_border( 'thin', 'thin', 'thin', 'thin' ),p_sheet => 1 ) ;
                PG_EXCEL_UTILS.SET_ROW( rowIDx2, p_borderId => PG_EXCEL_UTILS.get_border( 'thin', 'thin', 'thin', 'thin' ),p_sheet => 3 ) ;
                PG_EXCEL_UTILS.CELL(1,rowIDx,REC.POLICY_REF,p_sheet => 1);
                PG_EXCEL_UTILS.CELL(2,rowIDx,REC.TPA_NAME,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                PG_EXCEL_UTILS.CELL(3,rowIDx,REC.PRODUCT_CONFIG_CODE, p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                PG_EXCEL_UTILS.CELL(4,rowIDx,REC.RISK_COUNT,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                PG_EXCEL_UTILS.CELL(5,rowIDx,REC.TRRANCTION_TYPE,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                IF REC.OP_SUB_COV >0 THEN
                  PG_EXCEL_UTILS.CELL(6,rowIDx,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                ELSE
                  PG_EXCEL_UTILS.CELL(6,rowIDx,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                END IF;
                IF REC.IMA_FEE_AMT >0 THEN
                  PG_EXCEL_UTILS.CELL(7,rowIDx,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                ELSE
                  PG_EXCEL_UTILS.CELL(7,rowIDx,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                END IF;
                rowIDx :=rowIDx+1;
                PG_EXCEL_UTILS.CELL(1,rowIDx2,REC.POLICY_REF,p_sheet => 3);
                PG_EXCEL_UTILS.CELL(2,rowIDx2,REC.TPA_NAME,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                PG_EXCEL_UTILS.CELL(3,rowIDx2,REC.PRODUCT_CONFIG_CODE, p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                PG_EXCEL_UTILS.CELL(4,rowIDx2,REC.RISK_COUNT,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                PG_EXCEL_UTILS.CELL(5,rowIDx2,REC.TRRANCTION_TYPE,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                IF REC.OP_SUB_COV >0 THEN
                  PG_EXCEL_UTILS.CELL(6,rowIDx2,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                ELSE
                  PG_EXCEL_UTILS.CELL(6,rowIDx2,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                END IF;
                IF REC.IMA_FEE_AMT >0 THEN
                  PG_EXCEL_UTILS.CELL(7,rowIDx2,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                ELSE
                  PG_EXCEL_UTILS.CELL(7,rowIDx2,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                END IF;
                rowIDx2 :=rowIDx2+1;
            END IF;
            ELSIF REC.DD_TYPE='AAN_Towing'
            THEN
    IF(REC.POLICY_VERSION =1 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_NPOL,REC.PRODUCT_CONFIG_CODE) = 'N')
           THEN
           IF (REC.POLICY_VERSION =1 AND REC.ASST_FEE_AMT >0 )
            OR (REC.POLICY_VERSION >1 AND REC.ASST_FEE_AMT >=0 )
            OR (REC.POLICY_VERSION =1 AND REC.OCR_FEE_AMT >0  AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_OCR,REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (REC.POLICY_VERSION >1 AND REC.OCR_FEE_AMT >=0 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_OCR,REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (REC.POLICY_VERSION =1 AND REC.CRF_FEE_AMT >0  AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_CRF,REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (REC.POLICY_VERSION >1 AND REC.CRF_FEE_AMT >=0 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_CRF,REC.PRODUCT_CONFIG_CODE) = 'Y')
            THEN
        PG_EXCEL_UTILS.SET_ROW( rowIDx, p_borderId => PG_EXCEL_UTILS.get_border( 'thin', 'thin', 'thin', 'thin' ),p_sheet => 1 ) ;
                PG_EXCEL_UTILS.SET_ROW( rowIDx2, p_borderId => PG_EXCEL_UTILS.get_border( 'thin', 'thin', 'thin', 'thin' ),p_sheet => 3 ) ;
                PG_EXCEL_UTILS.CELL(1,rowIDx,REC.POLICY_REF,p_sheet => 1);
                PG_EXCEL_UTILS.CELL(2,rowIDx,REC.TPA_NAME,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                PG_EXCEL_UTILS.CELL(3,rowIDx,REC.PRODUCT_CONFIG_CODE, p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                PG_EXCEL_UTILS.CELL(4,rowIDx,REC.RISK_COUNT,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                PG_EXCEL_UTILS.CELL(5,rowIDx,REC.TRRANCTION_TYPE,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                IF REC.OP_SUB_COV >0 THEN
                  PG_EXCEL_UTILS.CELL(6,rowIDx,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                ELSE
                  PG_EXCEL_UTILS.CELL(6,rowIDx,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                END IF;
                IF REC.IMA_FEE_AMT >0 THEN
                  PG_EXCEL_UTILS.CELL(7,rowIDx,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                ELSE
                  PG_EXCEL_UTILS.CELL(7,rowIDx,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                END IF;
                rowIDx :=rowIDx+1;
                PG_EXCEL_UTILS.CELL(1,rowIDx2,REC.POLICY_REF,p_sheet => 3);
                PG_EXCEL_UTILS.CELL(2,rowIDx2,REC.TPA_NAME,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                PG_EXCEL_UTILS.CELL(3,rowIDx2,REC.PRODUCT_CONFIG_CODE, p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                PG_EXCEL_UTILS.CELL(4,rowIDx2,REC.RISK_COUNT,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                PG_EXCEL_UTILS.CELL(5,rowIDx2,REC.TRRANCTION_TYPE,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                IF REC.OP_SUB_COV >0 THEN
                  PG_EXCEL_UTILS.CELL(6,rowIDx2,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                ELSE
                  PG_EXCEL_UTILS.CELL(6,rowIDx2,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                END IF;
                IF REC.IMA_FEE_AMT >0 THEN
                  PG_EXCEL_UTILS.CELL(7,rowIDx2,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                ELSE
                  PG_EXCEL_UTILS.CELL(7,rowIDx2,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                END IF;
                rowIDx2 :=rowIDx2+1;
            END IF;
            END IF;
            ELSIF REC.DD_TYPE='AAN_Motor_Towing'
            THEN
            IF(REC.POLICY_VERSION =1 AND REC.DEALER3A_FEE >0) OR  (REC.POLICY_VERSION >1 AND REC.DEALER3A_FEE >=0)
            THEN
        PG_EXCEL_UTILS.SET_ROW( rowIDx, p_borderId => PG_EXCEL_UTILS.get_border( 'thin', 'thin', 'thin', 'thin' ),p_sheet => 1 ) ;
                PG_EXCEL_UTILS.SET_ROW( rowIDx2, p_borderId => PG_EXCEL_UTILS.get_border( 'thin', 'thin', 'thin', 'thin' ),p_sheet => 3 ) ;
                PG_EXCEL_UTILS.CELL(1,rowIDx,REC.POLICY_REF,p_sheet => 1);
                PG_EXCEL_UTILS.CELL(2,rowIDx,REC.TPA_NAME,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                PG_EXCEL_UTILS.CELL(3,rowIDx,REC.PRODUCT_CONFIG_CODE, p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                PG_EXCEL_UTILS.CELL(4,rowIDx,REC.RISK_COUNT,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                PG_EXCEL_UTILS.CELL(5,rowIDx,REC.TRRANCTION_TYPE,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                IF REC.OP_SUB_COV >0 THEN
                  PG_EXCEL_UTILS.CELL(6,rowIDx,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                ELSE
                  PG_EXCEL_UTILS.CELL(6,rowIDx,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                END IF;
                IF REC.IMA_FEE_AMT >0 THEN
                  PG_EXCEL_UTILS.CELL(7,rowIDx,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                ELSE
                  PG_EXCEL_UTILS.CELL(7,rowIDx,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                END IF;
                rowIDx :=rowIDx+1;
                PG_EXCEL_UTILS.CELL(1,rowIDx2,REC.POLICY_REF,p_sheet => 3);
                PG_EXCEL_UTILS.CELL(2,rowIDx2,REC.TPA_NAME,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                PG_EXCEL_UTILS.CELL(3,rowIDx2,REC.PRODUCT_CONFIG_CODE, p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                PG_EXCEL_UTILS.CELL(4,rowIDx2,REC.RISK_COUNT,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                PG_EXCEL_UTILS.CELL(5,rowIDx2,REC.TRRANCTION_TYPE,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                IF REC.OP_SUB_COV >0 THEN
                  PG_EXCEL_UTILS.CELL(6,rowIDx2,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                ELSE
                  PG_EXCEL_UTILS.CELL(6,rowIDx2,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                END IF;
                IF REC.IMA_FEE_AMT >0 THEN
                  PG_EXCEL_UTILS.CELL(7,rowIDx2,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                ELSE
                  PG_EXCEL_UTILS.CELL(7,rowIDx2,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                END IF;
                rowIDx2 :=rowIDx2+1;
            END IF;
            ELSIF REC.DD_TYPE='AAN_PA'
            THEN
             IF((REC.POLICY_VERSION =1 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_NPOL1,REC.PRODUCT_CONFIG_CODE) = 'N') OR REC.POLICY_VERSION >1)
             THEN
            IF (REC.POLICY_VERSION =1 AND REC.ASST_FEE_AMT >0 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_ASST,REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (REC.POLICY_VERSION >1 AND REC.ASST_FEE_AMT >=0 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_ASST,REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (REC.POLICY_VERSION =1 AND REC.IMA_FEE_AMT >0  AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_IMA,REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (REC.POLICY_VERSION >1 AND REC.IMA_FEE_AMT >=0 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_IMA,REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (REC.POLICY_VERSION =1 AND REC.MCO_FEE_AMT >0  AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_MCO,REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (REC.POLICY_VERSION >1 AND REC.MCO_FEE_AMT >=0 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_MCO,REC.PRODUCT_CONFIG_CODE) = 'Y')
            THEN
        PG_EXCEL_UTILS.SET_ROW( rowIDx, p_borderId => PG_EXCEL_UTILS.get_border( 'thin', 'thin', 'thin', 'thin' ),p_sheet => 1 ) ;
                PG_EXCEL_UTILS.SET_ROW( rowIDx2, p_borderId => PG_EXCEL_UTILS.get_border( 'thin', 'thin', 'thin', 'thin' ),p_sheet => 3 ) ;
                PG_EXCEL_UTILS.CELL(1,rowIDx,REC.POLICY_REF,p_sheet => 1);
                PG_EXCEL_UTILS.CELL(2,rowIDx,REC.TPA_NAME,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                PG_EXCEL_UTILS.CELL(3,rowIDx,REC.PRODUCT_CONFIG_CODE, p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                PG_EXCEL_UTILS.CELL(4,rowIDx,REC.RISK_COUNT,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                PG_EXCEL_UTILS.CELL(5,rowIDx,REC.TRRANCTION_TYPE,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                IF REC.OP_SUB_COV >0 THEN
                  PG_EXCEL_UTILS.CELL(6,rowIDx,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                ELSE
                  PG_EXCEL_UTILS.CELL(6,rowIDx,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                END IF;
                IF REC.IMA_FEE_AMT >0 THEN
                  PG_EXCEL_UTILS.CELL(7,rowIDx,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                ELSE
                  PG_EXCEL_UTILS.CELL(7,rowIDx,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                END IF;
                rowIDx :=rowIDx+1;
                PG_EXCEL_UTILS.CELL(1,rowIDx2,REC.POLICY_REF,p_sheet => 3);
                PG_EXCEL_UTILS.CELL(2,rowIDx2,REC.TPA_NAME,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                PG_EXCEL_UTILS.CELL(3,rowIDx2,REC.PRODUCT_CONFIG_CODE, p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                PG_EXCEL_UTILS.CELL(4,rowIDx2,REC.RISK_COUNT,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                PG_EXCEL_UTILS.CELL(5,rowIDx2,REC.TRRANCTION_TYPE,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                IF REC.OP_SUB_COV >0 THEN
                  PG_EXCEL_UTILS.CELL(6,rowIDx2,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                ELSE
                  PG_EXCEL_UTILS.CELL(6,rowIDx2,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                END IF;
                IF REC.IMA_FEE_AMT >0 THEN
                  PG_EXCEL_UTILS.CELL(7,rowIDx2,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                ELSE
                  PG_EXCEL_UTILS.CELL(7,rowIDx2,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                END IF;
                rowIDx2 :=rowIDx2+1;
            END IF;
            END IF;
            ELSIF REC.DD_TYPE='AAN_HC'
            THEN
            IF (REC.POLICY_VERSION =1 AND REC.MCOI_FEE_AMT >0) OR
            (REC.POLICY_VERSION >1 AND REC.MCOI_FEE_AMT >=0) OR
            (REC.POLICY_VERSION =1 AND REC.IMA_FEE_AMT >0) OR
            (REC.POLICY_VERSION >1 AND REC.IMA_FEE_AMT >=0) OR
            (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_MCOO,REC.PRODUCT_CONFIG_CODE) = 'Y'
            AND ((REC.POLICY_VERSION =1 AND REC.MCOO_FEE_AMT >0) OR
            (REC.POLICY_VERSION >1 AND REC.MCOO_FEE_AMT >=0)  )) OR
            (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_DMA,REC.PRODUCT_CONFIG_CODE) = 'Y'
            AND ((REC.POLICY_VERSION =1 AND (REC.MCOO_FEE_AMT >0 OR REC.DMA_FEE_AMT >0)) OR
            (REC.POLICY_VERSION >1 AND (REC.MCOO_FEE_AMT >=0 OR REC.DMA_FEE_AMT >=0))  ) )
            THEN
        PG_EXCEL_UTILS.SET_ROW( rowIDx, p_borderId => PG_EXCEL_UTILS.get_border( 'thin', 'thin', 'thin', 'thin' ),p_sheet => 1 ) ;
                PG_EXCEL_UTILS.SET_ROW( rowIDx2, p_borderId => PG_EXCEL_UTILS.get_border( 'thin', 'thin', 'thin', 'thin' ),p_sheet => 3 ) ;
                PG_EXCEL_UTILS.CELL(1,rowIDx,REC.POLICY_REF,p_sheet => 1);
                PG_EXCEL_UTILS.CELL(2,rowIDx,REC.TPA_NAME,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                PG_EXCEL_UTILS.CELL(3,rowIDx,REC.PRODUCT_CONFIG_CODE, p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                PG_EXCEL_UTILS.CELL(4,rowIDx,REC.RISK_COUNT,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                PG_EXCEL_UTILS.CELL(5,rowIDx,REC.TRRANCTION_TYPE,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                IF REC.OP_SUB_COV >0 THEN
                  PG_EXCEL_UTILS.CELL(6,rowIDx,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                ELSE
                  PG_EXCEL_UTILS.CELL(6,rowIDx,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                END IF;
                IF REC.IMA_FEE_AMT >0 THEN
                  PG_EXCEL_UTILS.CELL(7,rowIDx,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                ELSE
                  PG_EXCEL_UTILS.CELL(7,rowIDx,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 1);
                END IF;
                rowIDx :=rowIDx+1;
                PG_EXCEL_UTILS.CELL(1,rowIDx2,REC.POLICY_REF,p_sheet => 3);
                PG_EXCEL_UTILS.CELL(2,rowIDx2,REC.TPA_NAME,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                PG_EXCEL_UTILS.CELL(3,rowIDx2,REC.PRODUCT_CONFIG_CODE, p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                PG_EXCEL_UTILS.CELL(4,rowIDx2,REC.RISK_COUNT,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                PG_EXCEL_UTILS.CELL(5,rowIDx2,REC.TRRANCTION_TYPE,p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                IF REC.OP_SUB_COV >0 THEN
                  PG_EXCEL_UTILS.CELL(6,rowIDx2,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                ELSE
                  PG_EXCEL_UTILS.CELL(6,rowIDx2,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                END IF;
                IF REC.IMA_FEE_AMT >0 THEN
                  PG_EXCEL_UTILS.CELL(7,rowIDx2,'Y',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                ELSE
                  PG_EXCEL_UTILS.CELL(7,rowIDx2,'N',p_alignment => PG_EXCEL_UTILS.get_alignment('center','center'),p_sheet => 3);
                END IF;
                rowIDx2 :=rowIDx2+1;
            END IF;
        END IF;
    END LOOP;

        PG_EXCEL_UTILS.save( v_file_dir, FILENAME1 );

        V_FILE_NAME_LIST.EXTEND;
        V_FILE_NAME_LIST(1):=FILENAME1;
--            V_FILE_NAME_LIST.EXTEND;
--            V_FILE_NAME_LIST(2):=FILENAME2;
      V_Return:=   PG_TPA_UTILS.FN_SEND_EMAIL_JOB('AAN',v_file_dir,V_FILE_NAME_LIST,P_START_DT);

    EXCEPTION
    WHEN OTHERS
    THEN
        PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
            V_PKG_NAME || V_FUNC_NAME,
            1,
            '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
            --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
    END PC_TPA_AAN_CHECKSUM;
    --1.6 end

    -->> 2.0 start
/* Formatted on 24-Apr-20 1:39:35 AM (QP5 v5.227.12220.39754) */
PROCEDURE PC_TPA_AAN_HC_PA_POL_ENDT_2 (
   P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE)
IS
   CURSOR C_TPA_AAN_PA
   IS
        SELECT TO_CHAR (UPV.ENDT_EFF_DATE, 'DD/MM/YYYY') AS ENDT_EFF_DATE,
               OPB.POLICY_REF,
               UPB.PREV_POL_NO,
               UPV.ENDT_CODE,
               UPV.VERSION_NO AS POLICY_VERSION,
               UPB.AGENT_CODE,
               DVA.NAME AS AGENT_NAME,
               (CASE
                   WHEN upb.PREV_POL_NO IS NOT NULL THEN upb.PREV_POL_NO
                   ELSE upb.PREV_POL_NO_IIMS
                END)
                  AS prev_pol,
               (CASE
                   WHEN CP.ID_VALUE1 IS NULL THEN CP.ID_VALUE2
                   ELSE CP.ID_VALUE1
                END)
                  AS P_NRIC_OTH,
               (CASE
                   WHEN CP.ID_TYPE1 = 'NRIC' THEN CP.ID_VALUE1
                   WHEN CP.ID_TYPE2 = 'NRIC' THEN CP.ID_VALUE2
                END)
                  AS P_NRIC,
               REGEXP_REPLACE (
                  (CASE
                      WHEN     CP.MOBILE_NO1 IS NOT NULL
                           AND CP.MOBILE_CODE1 IS NOT NULL
                      THEN
                         CP.MOBILE_CODE1 || CP.MOBILE_NO1
                      ELSE
                         CP.MOBILE_CODE2 || CP.MOBILE_NO2
                   END),
                  '[^0-9]')
                  AS PhoneNumber,
               REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
               REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,
               REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3,
               CPA.POSTCODE,
               (SELECT CODE_DESC
                  FROM CMGE_CODE
                 WHERE CAT_CODE = 'CITY' AND CODE_CD = CPA.CITY)
                  AS CITY,
               (SELECT CODE_DESC
                  FROM CMGE_CODE
                 WHERE CAT_CODE = 'STATE' AND CODE_CD = CPA.STATE)
                  AS STATE,
               UPC.PRODUCT_CONFIG_CODE,
               CP.NAME_EXT,
               UPB.LONG_NAME,
               TO_CHAR (UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,
               TO_CHAR (UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
               TO_CHAR (UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,
               (SELECT UPF.FEE_AMT
                  FROM UWGE_POLICY_FEES UPF
                 WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                       AND UPF.VERSION_NO = UPV.VERSION_NO
                       AND UPF.FEE_CODE = 'ASST')
                  AS ASST_FEE_AMT,
               (SELECT UPF.FEE_AMT
                  FROM UWGE_POLICY_FEES UPF
                 WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                       AND UPF.VERSION_NO = UPV.VERSION_NO
                       AND UPF.FEE_CODE = 'MCO')
                  AS MCO_FEE_AMT,
               (SELECT UPF.FEE_AMT
                  FROM UWGE_POLICY_FEES UPF
                 WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                       AND UPF.VERSION_NO = UPV.VERSION_NO
                       AND UPF.FEE_CODE = 'MCOO')
                  AS MCOO_FEE_AMT,
               (SELECT UPF.FEE_AMT
                  FROM UWGE_POLICY_FEES UPF
                 WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                       AND UPF.VERSION_NO = UPV.VERSION_NO
                       AND UPF.FEE_CODE = 'MCOI')
                  AS MCOI_FEE_AMT,
               (SELECT UPF.FEE_AMT
                  FROM UWGE_POLICY_FEES UPF
                 WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                       AND UPF.VERSION_NO = UPV.VERSION_NO
                       AND UPF.FEE_CODE = 'IMA')
                  AS IMA_FEE_AMT,
               NVL (UPV.ENDT_NARR, ' ') AS ENDT_NARR,
               UPV.ENDT_NO,
               TO_CHAR (SYSDATE, 'DD/MM/YYYY') AS DateReceivedbyAAN,
               UPB.ISSUE_OFFICE,
               (SELECT BRANCH_NAME
                  FROM CMDM_BRANCH
                 WHERE BRANCH_CODE = UPB.ISSUE_OFFICE)
                  AS BRANCH_DESC,
               (SELECT EXP_DATE
                  FROM uwge_policy_bases
                 WHERE     CONTRACT_ID =
                              (SELECT CONTRACT_ID
                                 FROM OCP_POLICY_BASES
                                WHERE     policy_ref =
                                             (CASE
                                                 WHEN upb.PREV_POL_NO
                                                         IS NOT NULL
                                                 THEN
                                                    upb.PREV_POL_NO
                                                 ELSE
                                                    upb.PREV_POL_NO_IIMS
                                              END)
                                      AND ROWNUM = 1)
                       AND uwge_policy_bases.TOP_INDICATOR = 'Y'
                       AND ROWNUM = 1)
                  AS prev_exp_date,
               OPB.CONTRACT_ID
          FROM UWGE_POLICY_VERSIONS UPV
               INNER JOIN
               UWGE_POLICY_CTRL_DLOAD UPCD                               --1.3
                  ON     UPV.CONTRACT_ID = UPCD.CONTRACT_ID
                     AND UPV.VERSION_NO = UPCD.VERSION_NO
               INNER JOIN UWGE_POLICY_CONTRACTS UPC
                  ON UPV.CONTRACT_ID = UPC.CONTRACT_ID
               INNER JOIN OCP_POLICY_BASES OPB
                  ON UPV.CONTRACT_ID = OPB.CONTRACT_ID
               INNER JOIN
               UWGE_POLICY_BASES UPB
                  ON     UPB.CONTRACT_ID = UPV.CONTRACT_ID
                     AND UPB.VERSION_NO = UPV.VERSION_NO
               INNER JOIN
               UWPL_POLICY_BASES PLPB
                  ON     PLPB.CONTRACT_ID = UPV.CONTRACT_ID
                     AND PLPB.VERSION_NO = UPV.VERSION_NO
               INNER JOIN
               TABLE (
                  CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (UPB.CP_PART_ID,
                                                            UPB.CP_VERSION)) CP
                  ON     CP.PART_ID = UPB.CP_PART_ID
                     AND CP.VERSION = UPB.CP_VERSION
               INNER JOIN
               TABLE (
                  CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (
                     UPB.CP_ADDR_ID,
                     UPB.CP_ADDR_VERSION)) CPA
                  ON     CPA.ADD_ID = UPB.CP_ADDR_ID
                     AND CPA.VERSION = UPB.CP_ADDR_VERSION
               INNER JOIN DMAG_VI_AGENT DVA ON DVA.AGENTCODE = UPB.AGENT_CODE
         WHERE     UPC.PRODUCT_CONFIG_CODE IN
                      (    SELECT REGEXP_SUBSTR (
                                     PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA',
                                                                    'AAN_PA'),
                                     '[^,]+',
                                     1,
                                     LEVEL)
                             FROM DUAL
                       CONNECT BY REGEXP_SUBSTR (
                                     PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA',
                                                                    'AAN_PA'),
                                     '[^,]+',
                                     1,
                                     LEVEL)
                                     IS NOT NULL)
               AND UPC.POLICY_STATUS IN ('A', 'C', 'E')
               AND PLPB.TPA_NAME = 'AAN'
               AND UPV.ACTION_CODE IN ('A', 'C')
               AND (   UPV.ENDT_CODE IS NULL
                    OR UPV.ENDT_CODE IN
                          (    SELECT REGEXP_SUBSTR (V_AAN_ENDT_CODE,
                                                     '[^,]+',
                                                     1,
                                                     LEVEL)
                                 FROM DUAL
                           CONNECT BY REGEXP_SUBSTR (V_AAN_ENDT_CODE,
                                                     '[^,]+',
                                                     1,
                                                     LEVEL)
                                         IS NOT NULL))                   --1.4
               --AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy')
               --AND UPCD.DLOAD_STATUS = 'P'                               --1.3
               AND UPCD.DLOAD_STATUS = 'R'
               AND UPCD.TPA_NAME = 'AAN'
      ORDER BY OPB.policy_ref ASC, UPV.VERSION_NO ASC;


   CURSOR C_TPA_AAN_HC
   IS
        SELECT TO_CHAR (UPV.ENDT_EFF_DATE, 'DD/MM/YYYY') AS ENDT_EFF_DATE,
               OPB.POLICY_REF,
               UPB.PREV_POL_NO,
               UPV.ENDT_CODE,
               UPV.VERSION_NO AS POLICY_VERSION,
               UPB.AGENT_CODE,
               DVA.NAME AS AGENT_NAME,
               (CASE
                   WHEN upb.PREV_POL_NO IS NOT NULL THEN upb.PREV_POL_NO
                   ELSE upb.PREV_POL_NO_IIMS
                END)
                  AS prev_pol,
               (CASE
                   WHEN CP.ID_VALUE1 IS NULL THEN CP.ID_VALUE2
                   ELSE CP.ID_VALUE1
                END)
                  AS P_NRIC_OTH,
               (CASE
                   WHEN CP.ID_TYPE1 = 'NRIC' THEN CP.ID_VALUE1
                   WHEN CP.ID_TYPE2 = 'NRIC' THEN CP.ID_VALUE2
                END)
                  AS P_NRIC,
               REGEXP_REPLACE (
                  (CASE
                      WHEN     CP.MOBILE_NO1 IS NOT NULL
                           AND CP.MOBILE_CODE1 IS NOT NULL
                      THEN
                         CP.MOBILE_CODE1 || CP.MOBILE_NO1
                      ELSE
                         CP.MOBILE_CODE2 || CP.MOBILE_NO2
                   END),
                  '[^0-9]')
                  AS PhoneNumber,
               REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
               REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,
               REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3,
               CPA.POSTCODE,
               (SELECT CODE_DESC
                  FROM CMGE_CODE
                 WHERE CAT_CODE = 'CITY' AND CODE_CD = CPA.CITY)
                  AS CITY,
               (SELECT CODE_DESC
                  FROM CMGE_CODE
                 WHERE CAT_CODE = 'STATE' AND CODE_CD = CPA.STATE)
                  AS STATE,
               UPC.PRODUCT_CONFIG_CODE,
               CP.NAME_EXT,
               UPB.LONG_NAME,
               TO_CHAR (UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,
               TO_CHAR (UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
               TO_CHAR (UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,
               (SELECT UPF.FEE_AMT
                  FROM UWGE_POLICY_FEES UPF
                 WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                       AND UPF.VERSION_NO = UPV.VERSION_NO
                       AND UPF.FEE_CODE = 'MCOO')
                  AS MCOO_FEE_AMT,
               (SELECT UPF.FEE_AMT
                  FROM UWGE_POLICY_FEES UPF
                 WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                       AND UPF.VERSION_NO = UPV.VERSION_NO
                       AND UPF.FEE_CODE = 'MCOI')
                  AS MCOI_FEE_AMT,
               (SELECT UPF.FEE_AMT
                  FROM UWGE_POLICY_FEES UPF
                 WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                       AND UPF.VERSION_NO = UPV.VERSION_NO
                       AND UPF.FEE_CODE = 'MCO')
                  AS MCO_FEE_AMT,
               (SELECT UPF.FEE_AMT
                  FROM UWGE_POLICY_FEES UPF
                 WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                       AND UPF.VERSION_NO = UPV.VERSION_NO
                       AND UPF.FEE_CODE = 'IMA')
                  AS IMA_FEE_AMT,
               (SELECT UPF.FEE_AMT
                  FROM UWGE_POLICY_FEES UPF
                 WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                       AND UPF.VERSION_NO = UPV.VERSION_NO
                       AND UPF.FEE_CODE = 'MCODMA')
                  AS DMA_FEE_AMT,
               NVL (UPV.ENDT_NARR, ' ') AS ENDT_NARR,
               UPV.ENDT_NO,
               TO_CHAR (SYSDATE, 'DD/MM/YYYY') AS DateReceivedbyAAN,
               (SELECT BRANCH_NAME
                  FROM CMDM_BRANCH
                 WHERE BRANCH_CODE = UPB.ISSUE_OFFICE)
                  AS BRANCH_DESC,
               (SELECT EXP_DATE
                  FROM uwge_policy_bases
                 WHERE     CONTRACT_ID =
                              (SELECT CONTRACT_ID
                                 FROM OCP_POLICY_BASES
                                WHERE     policy_ref =
                                             (CASE
                                                 WHEN upb.PREV_POL_NO
                                                         IS NOT NULL
                                                 THEN
                                                    upb.PREV_POL_NO
                                                 ELSE
                                                    upb.PREV_POL_NO_IIMS
                                              END)
                                      AND ROWNUM = 1)
                       AND uwge_policy_bases.TOP_INDICATOR = 'Y'
                       AND ROWNUM = 1)
                  AS prev_exp_date,
               OPB.CONTRACT_ID
          FROM UWGE_POLICY_VERSIONS UPV
               INNER JOIN
               UWGE_POLICY_CTRL_DLOAD UPCD                               --1.3
                  ON     UPV.CONTRACT_ID = UPCD.CONTRACT_ID
                     AND UPV.VERSION_NO = UPCD.VERSION_NO
               INNER JOIN UWGE_POLICY_CONTRACTS UPC
                  ON UPV.CONTRACT_ID = UPC.CONTRACT_ID
               INNER JOIN OCP_POLICY_BASES OPB
                  ON UPV.CONTRACT_ID = OPB.CONTRACT_ID
               INNER JOIN
               UWGE_POLICY_BASES UPB
                  ON     UPB.CONTRACT_ID = UPV.CONTRACT_ID
                     AND UPB.VERSION_NO = UPV.VERSION_NO
               INNER JOIN
               UWPL_POLICY_BASES PLPB
                  ON     PLPB.CONTRACT_ID = UPV.CONTRACT_ID
                     AND PLPB.VERSION_NO = UPV.VERSION_NO
               INNER JOIN
               TABLE (
                  CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (UPB.CP_PART_ID,
                                                            UPB.CP_VERSION)) CP
                  ON     CP.PART_ID = UPB.CP_PART_ID
                     AND CP.VERSION = UPB.CP_VERSION
               INNER JOIN
               TABLE (
                  CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (
                     UPB.CP_ADDR_ID,
                     UPB.CP_ADDR_VERSION)) CPA
                  ON     CPA.ADD_ID = UPB.CP_ADDR_ID
                     AND CPA.VERSION = UPB.CP_ADDR_VERSION
               INNER JOIN DMAG_VI_AGENT DVA ON DVA.AGENTCODE = UPB.AGENT_CODE
         WHERE     UPC.PRODUCT_CONFIG_CODE IN
                      (    SELECT REGEXP_SUBSTR (
                                     PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA',
                                                                    'AAN_HC'),
                                     '[^,]+',
                                     1,
                                     LEVEL)
                             FROM DUAL
                       CONNECT BY REGEXP_SUBSTR (
                                     PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA',
                                                                    'AAN_HC'),
                                     '[^,]+',
                                     1,
                                     LEVEL)
                                     IS NOT NULL)
               AND UPC.POLICY_STATUS IN ('A', 'C', 'E')
               AND UPV.ACTION_CODE IN ('A', 'C')
               AND (   UPV.ENDT_CODE IS NULL
                    OR UPV.ENDT_CODE IN
                          (    SELECT REGEXP_SUBSTR (V_AAN_ENDT_CODE,
                                                     '[^,]+',
                                                     1,
                                                     LEVEL)
                                 FROM DUAL
                           CONNECT BY REGEXP_SUBSTR (V_AAN_ENDT_CODE,
                                                     '[^,]+',
                                                     1,
                                                     LEVEL)
                                         IS NOT NULL))                   --1.4
               AND (PLPB.TPA_NAME = 'AAN' OR PLPB.TPA_NAME IS NULL)
               --AND UPV.ISSUE_DATE= to_date(P_START_DT,'dd-MON-yy')
               --AND UPCD.DLOAD_STATUS = 'P'                               --1.3
               AND UPCD.DLOAD_STATUS = 'R'
               AND UPCD.TPA_NAME = 'AAN'
      ORDER BY OPB.policy_ref ASC, UPV.VERSION_NO ASC;

   V_STEPS                   VARCHAR2 (10);
   V_FUNC_NAME               VARCHAR2 (100) := 'PC_TPA_AAN_HC_PA_POL_ENDT';
   FILENAME                  UTL_FILE.FILE_TYPE;
   FILENAME1                 VARCHAR2 (1000);
   v_file_dir                VARCHAR2 (100)
      := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'TPA_AAN_DIR');
   V_ASST                    VARCHAR2 (100)
                                := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'PA_ASST');
   V_IMA                     VARCHAR2 (100)
                                := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'PA_IMA');
   V_MCO                     VARCHAR2 (100)
                                := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'PA_MCO');
   V_NPOL                    VARCHAR2 (100)
                                := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'POL_PA');
   V_HC_MCOI                 VARCHAR2 (100)
                                := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'HC_MCOI');
   V_HC_MCOO                 VARCHAR2 (100)
                                := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'HC_MCOO');
   V_HC_DMA                  VARCHAR2 (100)
                                := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'HC_DMA');

   V_RISK_LEVEL_DTLS         VARCHAR2 (100)
      := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'RISK_LEVEL_DTLS'); --116958_ALLIANZ SHIELD PLUS
   V_IMA_LMT_2M              VARCHAR2 (100)
      := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'IMA_LMT_2M'); --116958_ALLIANZ SHIELD PLUS

   V_PRINCIPAL_DET           PG_TPA_UTILS.RISK_PERSON_PARTNERS_ALL_DET;  --1.1
   V_UWPL_COVER_DET          PG_TPA_UTILS.UWPL_COVER_DET;
   rowIDx                    NUMBER := 5;
   seq                       NUMBER := 1;
   v_NGV                     NUMBER (18, 2);
   V_RET                     NUMBER := 0;                                --1.3
   --1.4
   --116958_ALLIANZ SHIELD PLUS start
   V_SELECTED_RISK_SQL       VARCHAR2 (10000)
      := 'SELECT (CASE
                     WHEN RCP.ID_VALUE1 IS NULL THEN RCP.ID_VALUE2
                     ELSE RCP.ID_VALUE1
                  END)
                    AS NRIC_OTH,
                 (CASE
                     WHEN RCP.ID_TYPE1 = ''NRIC'' THEN RCP.ID_VALUE1
                     WHEN RCP.ID_TYPE2 = ''NRIC'' THEN RCP.ID_VALUE2
                  END)
                    AS NRIC,
                 RCP.NAME_EXT AS MEMBER_FULL_NAME,
                 RCP.DATE_OF_BIRTH AS DATE_OF_BIRTH,
                 RCP.SEX,
                 (CASE
                     WHEN RCP.marital_status = ''0'' THEN ''S''
                     WHEN RCP.marital_status = ''1'' THEN ''M''
                     WHEN RCP.marital_status = ''2'' THEN ''D''
                  END)
                    AS MARITAL_STATUS,
                 URP.INSURED_TYPE,
                 URP.EMPLOYEE_ID,
                 (CASE
                     WHEN URP.INSURED_TYPE = ''P''
                     THEN
                        ''P''
                     ELSE
                        (CASE
                            WHEN URP.RELATIONSHIP IN (''03'', ''072'') THEN ''H''
                            WHEN URP.RELATIONSHIP IN (''02'', ''107'') THEN ''W''
                            WHEN URP.RELATIONSHIP IN (''05'', ''019'') THEN ''D''
                            WHEN URP.RELATIONSHIP IN (''04'', ''087'') THEN ''S''
                            ELSE ''''
                         END)
                  END)
                    AS RELATIONSHIP,
                 URP.TEMINATE_DATE,
                 UR.EFF_DATE AS RISK_EFF_DATE,
                 UR.EXP_DATE AS RISK_EXP_DATE,
                 URP.JOIN_DATE AS ORIGINAL_JOIN_DATE,
                 (CASE
                     WHEN URP.INSURED_TYPE = ''D''
                     THEN
                        (SELECT a.COV_SEQ_REF
                           FROM uwge_cover a
                          WHERE     UCOV.CONTRACT_ID = A.CONTRACT_ID
                                AND UCOV.VERSION_NO = a.VERSION_NO
                                AND a.RISK_ID = UR.RISK_PARENT_ID
                                AND COV_PARENT_ID IS NULL
                                AND ROWNUM = 1)
                     ELSE
                        ''''
                  END)
                    AS Parent_cov_seq_no,
                 UCOV.COV_ID,
                 UCOV.COV_SEQ_REF,
                 UR.RISK_ID,
                 UR.RISK_PARENT_ID,
                 (SELECT COUNT (*)
                    FROM UWGE_COVER CSUB
                   WHERE     UCOV.CONTRACT_ID = CSUB.CONTRACT_ID
                         AND UCOV.VERSION_NO = CSUB.VERSION_NO
                         AND UCOV.COV_ID = CSUB.COV_PARENT_ID
                         AND CSUB.COV_CODE IN (''OP'', ''OP1'', ''OP2''))
                    AS OP_SUB_COV,
                 (SELECT NVL (F.FEE_AMT, 0)
                    FROM UWGE_COVER_FEES F
                   WHERE     F.CONTRACT_ID = UR.CONTRACT_ID
                         AND F.COV_ID = UCOV.COV_ID
                         AND TOP_INDICATOR = ''Y''
                         AND F.FEE_CODE = ''MCO'')
                    AS MCO_FEE,
                 (SELECT NVL (F.FEE_AMT, 0)
                    FROM UWGE_COVER_FEES F
                   WHERE     F.CONTRACT_ID = UR.CONTRACT_ID
                         AND F.COV_ID =
                                (SELECT CV.COV_ID
                                   FROM UWGE_COVER CV
                                  WHERE     CV.COV_PARENT_ID = UCOV.COV_ID
                                        AND TOP_INDICATOR = ''Y''
                                        AND COV_CODE = ''IMA''
                                        AND ROWNUM = 1)
                         AND TOP_INDICATOR = ''Y''
                         AND F.FEE_CODE = ''IMA''
                         AND ROWNUM = 1)
                    AS IMA_FEE,
                 null as import_type,
                 null as prev_pol_op_ind,
                 urp.department
            FROM UWGE_RISK UR
                 INNER JOIN
                 UWPL_RISK_PERSON URP
                    ON     URP.CONTRACT_ID = UR.CONTRACT_ID
                       AND UR.RISK_ID = URP.RISK_ID
                       AND URP.VERSION_NO = UR.VERSION_NO
                       AND URP.action_code <> ''D''
                 INNER JOIN
                 TABLE (
                    CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (URP.RISK_PART_ID,
                                                              URP.RISK_PART_VER)) RCP
                    ON     RCP.PART_ID = URP.RISK_PART_ID
                       AND RCP.VERSION = URP.RISK_PART_VER
                 INNER JOIN
                 UWGE_COVER UCOV
                    ON     UCOV.CONTRACT_ID = UR.CONTRACT_ID
                       AND UR.RISK_ID = UCOV.RISK_ID
                       AND UCOV.VERSION_NO = UR.VERSION_NO
                       AND UCOV.COV_PARENT_ID IS NULL
           WHERE     UR.CONTRACT_ID = :BIND_CONTRACT_ID
                 AND UR.version_no = :BIND_VERSION_NO
                 AND UR.action_code <> ''D''
        ORDER BY urp.department, TO_NUMBER (UCOV.cov_seq_ref)';

   V_ALL_RISK_SQL            VARCHAR2 (10000)
      := 'SELECT (CASE
                 WHEN RCP.ID_VALUE1 IS NULL THEN RCP.ID_VALUE2
                 ELSE RCP.ID_VALUE1
              END)
                AS NRIC_OTH,
             (CASE
                 WHEN RCP.ID_TYPE1 = ''NRIC'' THEN RCP.ID_VALUE1
                 WHEN RCP.ID_TYPE2 = ''NRIC'' THEN RCP.ID_VALUE2
              END)
                AS NRIC,
             RCP.NAME_EXT AS MEMBER_FULL_NAME,
             RCP.DATE_OF_BIRTH AS DATE_OF_BIRTH,
             RCP.SEX,
             (CASE
                 WHEN RCP.marital_status = ''0'' THEN ''S''
                 WHEN RCP.marital_status = ''1'' THEN ''M''
                 WHEN RCP.marital_status = ''2'' THEN ''D''
              END)
                AS MARITAL_STATUS,
             URP.INSURED_TYPE,
             URP.EMPLOYEE_ID,
             (CASE
                 WHEN URP.INSURED_TYPE = ''P''
                 THEN
                    ''P''
                 ELSE
                    (CASE
                        WHEN URP.RELATIONSHIP IN (''03'', ''072'') THEN ''H''
                        WHEN URP.RELATIONSHIP IN (''02'', ''107'') THEN ''W''
                        WHEN URP.RELATIONSHIP IN (''05'', ''019'') THEN ''D''
                        WHEN URP.RELATIONSHIP IN (''04'', ''087'') THEN ''S''
                        ELSE ''''
                     END)
              END)
                AS RELATIONSHIP,
             URP.TEMINATE_DATE,
             UR.EFF_DATE AS RISK_EFF_DATE,
             UR.EXP_DATE AS RISK_EXP_DATE,
             URP.JOIN_DATE AS ORIGINAL_JOIN_DATE,
             (CASE
                 WHEN URP.INSURED_TYPE = ''D''
                 THEN
                    (SELECT a.COV_SEQ_REF
                       FROM uwge_cover a
                      WHERE     UCOV.CONTRACT_ID = A.CONTRACT_ID
                            AND UCOV.VERSION_NO = a.VERSION_NO
                            AND a.RISK_ID = UR.RISK_PARENT_ID
                            AND COV_PARENT_ID IS NULL
                            AND ROWNUM = 1)
                 ELSE
                    ''''
              END)
                AS Parent_cov_seq_no,
             UCOV.COV_ID,
             UCOV.COV_SEQ_REF,
             UR.RISK_ID,
             UR.RISK_PARENT_ID,
             (SELECT COUNT (*)
                FROM UWGE_COVER CSUB
               WHERE     UCOV.CONTRACT_ID = CSUB.CONTRACT_ID
                     AND UCOV.VERSION_NO = CSUB.VERSION_NO
                     AND UCOV.COV_ID = CSUB.COV_PARENT_ID
                     AND CSUB.COV_CODE IN (''OP'', ''OP1'', ''OP2''))
                AS OP_SUB_COV,
             (SELECT NVL (F.FEE_AMT, 0)
                FROM UWGE_COVER_FEES F
               WHERE     F.CONTRACT_ID = UR.CONTRACT_ID
                     AND F.COV_ID = UCOV.COV_ID
                     AND TOP_INDICATOR = ''Y''
                     AND F.FEE_CODE = ''MCO'')
                AS MCO_FEE,
             (SELECT NVL (F.FEE_AMT, 0)
                FROM UWGE_COVER_FEES F
               WHERE     F.CONTRACT_ID = UR.CONTRACT_ID
                     AND F.COV_ID =
                            (SELECT CV.COV_ID
                               FROM UWGE_COVER CV
                              WHERE     CV.COV_PARENT_ID = UCOV.COV_ID
                                    AND TOP_INDICATOR = ''Y''
                                    AND COV_CODE = ''IMA''
                                    AND ROWNUM = 1)
                     AND TOP_INDICATOR = ''Y''
                     AND F.FEE_CODE = ''IMA''
                     AND ROWNUM = 1)
                AS IMA_FEE,
             NULL AS import_type,
             NULL AS prev_pol_op_ind,
             urp.department
        FROM UWGE_RISK UR
             INNER JOIN
             UWPL_RISK_PERSON URP
                ON     URP.CONTRACT_ID = UR.CONTRACT_ID
                   AND UR.RISK_ID = URP.RISK_ID
                   AND URP.VERSION_NO =
                          (SELECT MAX (b.version_no)
                             FROM UWPL_RISK_PERSON b
                            WHERE     b.contract_id = UR.CONTRACT_ID
                                  AND URP.object_id = b.object_id
                                  AND b.version_no <= :BIND_VERSION_NO
                                  AND b.reversing_version IS NULL)
                   AND URP.action_code <> ''D''
             INNER JOIN
             TABLE (
                CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (URP.RISK_PART_ID,
                                                          URP.RISK_PART_VER)) RCP
                ON     RCP.PART_ID = URP.RISK_PART_ID
                   AND RCP.VERSION = URP.RISK_PART_VER
             INNER JOIN
             UWGE_COVER UCOV
                ON     UCOV.CONTRACT_ID = UR.CONTRACT_ID
                   AND UR.RISK_ID = UCOV.RISK_ID
                   AND UCOV.VERSION_NO = :BIND_VERSION_NO_1
                   AND UCOV.COV_PARENT_ID IS NULL
       WHERE     UR.CONTRACT_ID = :BIND_CONTRACT_ID
             AND UR.version_no =
                    (SELECT MAX (c.version_no)
                       FROM UWGE_RISK c
                      WHERE     c.contract_id = :BIND_CONTRACT_ID_1
                            AND UR.object_id = c.object_id
                            AND c.version_no <= :BIND_VERSION_NO_2
                            AND c.reversing_version IS NULL)
             AND UR.action_code <> ''D''
    ORDER BY urp.department, TO_NUMBER (UCOV.cov_seq_ref)'; --116958_ALLIANZ SHIELD PLUS End


   -->> 2.0 start
   V_SELECTED_RISK_SQL_TPA   VARCHAR2 (10000)
      := 'SELECT (CASE
               WHEN rcp.id_value1 IS NULL THEN rcp.id_value2
               ELSE rcp.id_value1
            END)
              AS nric_oth,
           (CASE
               WHEN rcp.id_type1 = ''NRIC'' THEN rcp.id_value1
               WHEN rcp.id_type2 = ''NRIC'' THEN rcp.id_value2
            END)
              AS nric,
           rcp.name_ext AS member_full_name,
           rcp.date_of_birth AS date_of_birth,
           rcp.sex,
           (CASE
               WHEN rcp.marital_status = ''0'' THEN ''S''
               WHEN rcp.marital_status = ''1'' THEN ''M''
               WHEN rcp.marital_status = ''2'' THEN ''D''
            END)
              AS marital_status,
           urp.insured_type,
           urp.employee_id,
           (CASE
               WHEN urp.insured_type = ''P''
               THEN
                  ''P''
               ELSE
                  (CASE
                      WHEN urp.relationship IN (''03'', ''072'') THEN ''H''
                      WHEN urp.relationship IN (''02'', ''107'') THEN ''W''
                      WHEN urp.relationship IN (''05'', ''019'') THEN ''D''
                      WHEN urp.relationship IN (''04'', ''087'') THEN ''S''
                      ELSE ''''
                   END)
            END)
              AS relationship,
           urp.teminate_date,
           ur.eff_date AS risk_eff_date,
           ur.exp_date AS risk_exp_date,
           urp.join_date AS original_join_date,
           (CASE
               WHEN urp.insured_type = ''D''
               THEN
                  (SELECT a.cov_seq_ref
                     FROM uwge_cover a
                    WHERE     ucov.contract_id = a.contract_id
                          AND ucov.version_no = a.version_no
                          AND a.risk_id = ur.risk_parent_id
                          AND cov_parent_id IS NULL
                          AND ROWNUM = 1)
               ELSE
                  ''''
            END)
              AS parent_cov_seq_no,
           ucov.cov_id,
           ucov.cov_seq_ref,
           ur.risk_id,
           ur.risk_parent_id,
           (SELECT COUNT (*)
              FROM uwge_cover csub
             WHERE     ucov.contract_id = csub.contract_id
                   AND ucov.version_no = csub.version_no
                   AND ucov.cov_id = csub.cov_parent_id
                   AND csub.cov_code IN (''OP'', ''OP1'', ''OP2''))
              AS op_sub_cov,
           (SELECT NVL (f.fee_amt, 0)
              FROM uwge_cover_fees f
             WHERE     f.contract_id = ur.contract_id
                   AND f.cov_id = ucov.cov_id
                   AND top_indicator = ''Y''
                   AND f.fee_code = ''MCO'')
              AS mco_fee,
           (SELECT NVL (f.fee_amt, 0)
              FROM uwge_cover_fees f
             WHERE     f.contract_id = ur.contract_id
                   AND f.cov_id =
                          (SELECT CV.cov_id
                             FROM uwge_cover CV
                            WHERE     CV.cov_parent_id = ucov.cov_id
                                  AND top_indicator = ''Y''
                                  AND cov_code = ''IMA''
                                  AND ROWNUM = 1)
                   AND top_indicator = ''Y''
                   AND f.fee_code = ''IMA''
                   AND ROWNUM = 1)
              AS ima_fee,
           tpa.import_type,
           tpa.prev_pol_op_ind,
           urp.department
      FROM uwge_risk_tpa_download tpa
           INNER JOIN
           uwge_cover ucov
              ON     ucov.contract_id = tpa.contract_id
                 AND ucov.risk_id = tpa.risk_id
                 AND ucov.version_no = tpa.version_no
                 AND ucov.cov_parent_id IS NULL
           LEFT OUTER JOIN
           uwge_risk ur
              ON     ur.contract_id = tpa.contract_id
                 AND ur.risk_id = tpa.risk_id
                 AND ur.action_code <> ''D''
                 AND ur.version_no =
                        (SELECT MAX (version_no)
                           FROM uwge_risk ur2
                          WHERE     ur2.contract_id = ur.contract_id
                                AND ur2.object_id = ur.object_id
                                AND ur2.version_no <= tpa.version_no
                                AND ur2.reversing_version IS NULL)
           LEFT OUTER JOIN
           uwpl_risk_person urp
              ON     urp.contract_id = tpa.contract_id
                 AND urp.risk_id = tpa.risk_id
                 AND urp.action_code <> ''D''
                 AND urp.version_no =
                        (SELECT MAX (version_no)
                           FROM uwpl_risk_person urp2
                          WHERE     urp2.contract_id = urp.contract_id
                                AND urp2.object_id = urp.object_id
                                AND urp2.version_no <= tpa.version_no
                                AND urp2.reversing_version IS NULL)
           INNER JOIN
           TABLE (
              customer.pg_cp_gen_table.fn_gen_cp_table (urp.risk_part_id,
                                                        urp.risk_part_ver)) rcp
              ON     rcp.part_id = urp.risk_part_id
                 AND rcp.version = urp.risk_part_ver
     WHERE     tpa.contract_id = :bind_contract_id
           AND tpa.version_no = :bind_version_no
           AND tpa.tpa_name = ''AAN''
      ORDER BY urp.department, TO_NUMBER (ucov.cov_seq_ref)';

   --<< 2.0 end

   RISK_DET                  PG_TPA_UTILS.AAN_PA_HC_RISK_DET_TBL;
   V_ROW_NUM                 NUMBER (5);
   V_ENDT_NARR_ARRAY         PG_TPA_UTILS.p_array_v;
   V_COUNT_TPA_RISK          NUMBER (5);                                 --2.0
BEGIN
   --        --dbms_output.put_line (
   --                  'P_START_DT :  ' || P_START_DT);
   V_STEPS := '001';
   FILENAME1 :=
         TO_CHAR (P_START_DT, 'YYYYMMDD')
      || '_HC'
      || CHR (38)
      || ' PA_POLEND_2.xlsx';
   V_STEPS := '002';
   PG_EXCEL_UTILS.clear_workbook;
   PG_EXCEL_UTILS.new_sheet;
   PG_EXCEL_UTILS.CELL (1, 1, 'BORDEREAUX (POLICY &' || ' ENDORSEMENT)');
   V_STEPS := '003';
   PG_EXCEL_UTILS.MERGECELLS (1,
                              1,
                              3,
                              1);
   PG_EXCEL_UTILS.CELL (
      1,
      2,
      'FROM : ALLIANZ GENERAL INSURANCE COMPANY (MALAYSIA) BERHAD');
   PG_EXCEL_UTILS.MERGECELLS (1,
                              2,
                              3,
                              2);
   PG_EXCEL_UTILS.CELL (1, 3, 'DATE :');
   PG_EXCEL_UTILS.CELL (2, 3, TO_CHAR (P_START_DT, 'DD/MM/YYYY'));
   V_STEPS := '004';
   PG_EXCEL_UTILS.SET_ROW (
      4,
      p_fontId   => PG_EXCEL_UTILS.get_font ('Arial', p_bold => TRUE));
   PG_EXCEL_UTILS.CELL (1, 4, 'No.');
   PG_EXCEL_UTILS.CELL (2, 4, 'Import Type');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (2, 20);
   PG_EXCEL_UTILS.CELL (3, 4, 'Member Full Name');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (3, 40);
   PG_EXCEL_UTILS.CELL (4, 4, 'Address 1');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (4, 20);
   PG_EXCEL_UTILS.CELL (5, 4, 'Address 2');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (5, 20);
   PG_EXCEL_UTILS.CELL (6, 4, 'Address 3');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (6, 40);
   PG_EXCEL_UTILS.CELL (7, 4, 'Address 4');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (7, 20);
   PG_EXCEL_UTILS.CELL (8, 4, 'Gender');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (8, 20);
   V_STEPS := '005';
   PG_EXCEL_UTILS.CELL (9, 4, 'DOB');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (9, 20);
   PG_EXCEL_UTILS.CELL (10, 4, 'NRIC');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (10, 20);
   PG_EXCEL_UTILS.CELL (11, 4, 'Other IC');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (11, 20);
   PG_EXCEL_UTILS.CELL (12, 4, 'External Ref Id (aka Client)');
   V_STEPS := '006';
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (12, 20);
   PG_EXCEL_UTILS.CELL (13, 4, 'Internal Ref Id (aka AAN)');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (13, 20);
   PG_EXCEL_UTILS.CELL (14, 4, 'Employee ID');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (14, 20);
   PG_EXCEL_UTILS.CELL (15, 4, 'Marital Status');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (15, 20);
   PG_EXCEL_UTILS.CELL (16, 4, 'Race');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (16, 20);
   PG_EXCEL_UTILS.CELL (17, 4, 'Phone');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (17, 20);
   PG_EXCEL_UTILS.CELL (18, 4, 'VIP');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (18, 20);
   PG_EXCEL_UTILS.CELL (19, 4, 'Special Condition');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (19, 20);
   PG_EXCEL_UTILS.CELL (20, 4, 'Relationship');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (20, 20);
   PG_EXCEL_UTILS.CELL (21, 4, 'Principal Int Ref Id (aka AAN)');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (21, 20);
   PG_EXCEL_UTILS.CELL (22, 4, 'Principal Ext Ref Id (aka Client)');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (22, 20);
   PG_EXCEL_UTILS.CELL (23, 4, 'Principal Name');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (23, 20);
   PG_EXCEL_UTILS.CELL (24, 4, 'Principal NRIC');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (24, 20);
   PG_EXCEL_UTILS.CELL (25, 4, 'Principal Other Ic');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (25, 20);
   PG_EXCEL_UTILS.CELL (26, 4, 'Program Id');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (26, 20);
   PG_EXCEL_UTILS.CELL (27, 4, 'Policy Type');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (27, 20);
   PG_EXCEL_UTILS.CELL (28, 4, 'Policy Num');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (28, 20);
   PG_EXCEL_UTILS.CELL (29, 4, 'Policy Eff Date');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (29, 20);
   PG_EXCEL_UTILS.CELL (30, 4, 'Policy Expiry Date');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (30, 20);
   PG_EXCEL_UTILS.CELL (31, 4, 'Previous Policy Num');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (31, 20);
   PG_EXCEL_UTILS.CELL (32, 4, 'Previous Policy End Date');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (32, 20);
   PG_EXCEL_UTILS.CELL (33, 4, 'Customer Owner Name');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (33, 20);
   PG_EXCEL_UTILS.CELL (34, 4, 'External Plan Code');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (34, 20);
   PG_EXCEL_UTILS.CELL (35, 4, 'Internal Plan Code Id');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (35, 20);
   PG_EXCEL_UTILS.CELL (36, 4, 'Original Join Date');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (36, 20);
   PG_EXCEL_UTILS.CELL (37, 4, 'Plan Attach Date');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (37, 20);
   PG_EXCEL_UTILS.CELL (38, 4, 'Plan Expiry Date');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (38, 20);
   PG_EXCEL_UTILS.CELL (39, 4, 'Subsidiary Name');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (39, 20);
   PG_EXCEL_UTILS.CELL (40, 4, 'Agent Name');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (40, 20);
   PG_EXCEL_UTILS.CELL (41, 4, 'Agent Code');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (41, 20);
   PG_EXCEL_UTILS.CELL (42, 4, 'Insurer MCO Fees');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (42, 20);
   PG_EXCEL_UTILS.CELL (43, 4, 'IMA Service?');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (43, 20);
   PG_EXCEL_UTILS.CELL (44, 4, 'IMA Limit');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (44, 20);
   PG_EXCEL_UTILS.CELL (45, 4, 'Date Received by AAN');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (45, 20);
   PG_EXCEL_UTILS.CELL (46, 4, 'Termination Date');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (46, 20);
   PG_EXCEL_UTILS.CELL (47, 4, 'Free text remark');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (47, 20);
   PG_EXCEL_UTILS.CELL (48, 4, 'Questionnaire');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (48, 20);
   PG_EXCEL_UTILS.CELL (49, 4, 'Plan-Remarks');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (49, 20);
   PG_EXCEL_UTILS.CELL (50, 4, 'Diagnosis');
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (50, 20);
   PG_EXCEL_UTILS.CELL (51, 4, 'Outpatient Subcover');                   --1.4
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (51, 20);                             --1.4
   PG_EXCEL_UTILS.CELL (52, 4, 'Previous Outpatient Subcover');          --2.0
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (52, 20);                             --2.0
   PG_EXCEL_UTILS.CELL (53, 4, 'Department');                            --2.0
   PG_EXCEL_UTILS.SET_COLUMN_WIDTH (53, 20);                             --2.0
   DBMS_OUTPUT.ENABLE (buffer_size => NULL);

   FOR REC IN C_TPA_AAN_PA
   LOOP
      V_STEPS := '007AA_01';

      IF (   (    REC.POLICY_VERSION = 1
              AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                     V_NPOL,
                     REC.PRODUCT_CONFIG_CODE) = 'N')
          OR REC.POLICY_VERSION > 1)
      THEN
         IF    (    REC.POLICY_VERSION = 1
                AND REC.ASST_FEE_AMT > 0
                AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                       V_ASST,
                       REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (    REC.POLICY_VERSION > 1
                AND REC.ASST_FEE_AMT >= 0
                AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                       V_ASST,
                       REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (    REC.POLICY_VERSION = 1
                AND REC.IMA_FEE_AMT > 0
                AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                       V_IMA,
                       REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (    REC.POLICY_VERSION > 1
                AND REC.IMA_FEE_AMT >= 0
                AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                       V_IMA,
                       REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (    REC.POLICY_VERSION = 1
                AND REC.MCO_FEE_AMT > 0
                AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                       V_MCO,
                       REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (    REC.POLICY_VERSION > 1
                AND REC.MCO_FEE_AMT >= 0
                AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                       V_MCO,
                       REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_RISK_LEVEL_DTLS,
                                                    REC.PRODUCT_CONFIG_CODE) =
                   'Y')                           --116958_ALLIANZ SHIELD PLUS
         THEN
            --1.4
            RISK_DET.DELETE;

            -->> 2.0 start comment
            /*IF     REC.ENDT_CODE IS NOT NULL
               AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_AAN_ENDT_CODE_R,
                                                       REC.ENDT_CODE) = 'Y'
            THEN
               BEGIN
                  EXECUTE IMMEDIATE V_SELECTED_RISK_SQL
                     BULK COLLECT INTO RISK_DET
                     USING REC.CONTRACT_ID, REC.POLICY_VERSION;
               END;
            ELSE
               BEGIN
                  EXECUTE IMMEDIATE V_ALL_RISK_SQL
                     BULK COLLECT INTO RISK_DET
                     USING REC.POLICY_VERSION,
                           REC.POLICY_VERSION,
                           REC.CONTRACT_ID,
                           REC.CONTRACT_ID,
                           REC.POLICY_VERSION;
               END;
            END IF;*/
            --<< 2.0 end comment

            -->> 2.0 start
            SELECT COUNT (1)
              INTO V_COUNT_TPA_RISK
              FROM UWGE_RISK_TPA_DOWNLOAD TPA
             WHERE     TPA.CONTRACT_ID = REC.CONTRACT_ID
                   AND TPA.VERSION_NO = REC.POLICY_VERSION;

            IF V_COUNT_TPA_RISK > 0
            THEN
               BEGIN
                  EXECUTE IMMEDIATE V_SELECTED_RISK_SQL_TPA
                     BULK COLLECT INTO RISK_DET
                     USING REC.CONTRACT_ID, REC.POLICY_VERSION;
               END;
            ELSE
               IF     REC.ENDT_CODE IS NOT NULL
                  AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_AAN_ENDT_CODE_R,
                                                          REC.ENDT_CODE) =
                         'Y'
               THEN
                  BEGIN
                     EXECUTE IMMEDIATE V_SELECTED_RISK_SQL
                        BULK COLLECT INTO RISK_DET
                        USING REC.CONTRACT_ID, REC.POLICY_VERSION;
                  END;
               ELSE
                  BEGIN
                     EXECUTE IMMEDIATE V_ALL_RISK_SQL
                        BULK COLLECT INTO RISK_DET
                        USING REC.POLICY_VERSION,
                              REC.POLICY_VERSION,
                              REC.CONTRACT_ID,
                              REC.CONTRACT_ID,
                              REC.POLICY_VERSION;
                  END;
               END IF;
            END IF;

            --<< 2.0 end

            V_ROW_NUM := 0;

            FOR V_ROW_NUM IN 1 .. RISK_DET.COUNT
            LOOP                            --116958_ALLIANZ SHIELD PLUS start
               IF (   PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                         V_RISK_LEVEL_DTLS,
                         REC.PRODUCT_CONFIG_CODE) = 'N'
                   OR (    PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                              V_RISK_LEVEL_DTLS,
                              REC.PRODUCT_CONFIG_CODE) = 'Y'
                       AND (   NVL (RISK_DET (V_ROW_NUM).IMA_FEE, 0) > 0
                            OR NVL (RISK_DET (V_ROW_NUM).MCO_FEE, 0) > 0)
                       AND REC.POLICY_VERSION = 1)
                   OR (    PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                              V_RISK_LEVEL_DTLS,
                              REC.PRODUCT_CONFIG_CODE) = 'Y'
                       AND REC.POLICY_VERSION > 1))
               THEN
                  --116958_ALLIANZ SHIELD PLUS end
                  PG_EXCEL_UTILS.CELL (1, rowIDx, seq);

                  -->> 2.0 start comment
                  /*IF REC.POLICY_VERSION = 1 AND REC.PREV_POL_NO IS NOT NULL
                  THEN
                     PG_EXCEL_UTILS.CELL (2, rowIDx, 'R');
                  ELSIF REC.POLICY_VERSION = 1 AND REC.PREV_POL_NO IS NULL
                  THEN
                     PG_EXCEL_UTILS.CELL (2, rowIDx, 'N');
                  ELSIF     REC.POLICY_VERSION > 1
                        AND REC.ENDT_CODE IN ('96', '97')
                  THEN
                     PG_EXCEL_UTILS.CELL (2, rowIDx, 'X');
                  ELSE
                     PG_EXCEL_UTILS.CELL (2, rowIDx, 'E');
                  END IF;*/
                  --<< 2.0 end comment

                  -->> 2.0 start
                  IF RISK_DET (V_ROW_NUM).IMPORT_TYPE IS NOT NULL
                  THEN
                     PG_EXCEL_UTILS.CELL (2,
                                          rowIDx,
                                          RISK_DET (V_ROW_NUM).IMPORT_TYPE);
                  ELSE
                     IF     REC.POLICY_VERSION = 1
                        AND REC.PREV_POL_NO IS NOT NULL
                     THEN
                        PG_EXCEL_UTILS.CELL (2, rowIDx, 'R');
                     ELSIF REC.POLICY_VERSION = 1 AND REC.PREV_POL_NO IS NULL
                     THEN
                        PG_EXCEL_UTILS.CELL (2, rowIDx, 'N');
                     ELSIF     REC.POLICY_VERSION > 1
                           AND REC.ENDT_CODE IN ('96', '97')
                     THEN
                        PG_EXCEL_UTILS.CELL (2, rowIDx, 'X');
                     ELSE
                        PG_EXCEL_UTILS.CELL (2, rowIDx, 'E');
                     END IF;
                  END IF;

                  --<< 2.0 end


                  V_STEPS := '007A';
                  PG_EXCEL_UTILS.CELL (
                     3,
                     rowIDx,
                     NVL (RISK_DET (V_ROW_NUM).MEMBER_FULL_NAME, ' '));
                  PG_EXCEL_UTILS.CELL (
                     4,
                     rowIDx,
                        NVL (REC.ADDRESS_LINE1, ' ')
                     || NVL (REC.ADDRESS_LINE2, ' '));
                  PG_EXCEL_UTILS.CELL (5,
                                       rowIDx,
                                       NVL (REC.ADDRESS_LINE3, ' '));
                  PG_EXCEL_UTILS.CELL (
                     6,
                     rowIDx,
                     NVL (REC.POSTCODE, ' ') || ' ' || NVL (REC.CITY, ' '));
                  PG_EXCEL_UTILS.CELL (7, rowIDx, NVL (REC.STATE, ' '));
                  PG_EXCEL_UTILS.CELL (8,
                                       rowIDx,
                                       NVL (RISK_DET (V_ROW_NUM).SEX, ' '));

                  --1.4
                  IF RISK_DET (V_ROW_NUM).DATE_OF_BIRTH IS NULL
                  THEN
                     PG_EXCEL_UTILS.CELL (9, rowIDx, ' ');
                  ELSE
                     PG_EXCEL_UTILS.CELL (
                        9,
                        rowIDx,
                        TO_CHAR (RISK_DET (V_ROW_NUM).DATE_OF_BIRTH,
                                 'DD/MM/YYYY'));
                  END IF;

                  PG_EXCEL_UTILS.CELL (10,
                                       rowIDx,
                                       NVL (RISK_DET (V_ROW_NUM).NRIC, ' '));

                  IF RISK_DET (V_ROW_NUM).NRIC IS NULL
                  THEN
                     PG_EXCEL_UTILS.CELL (
                        11,
                        rowIDx,
                        NVL (RISK_DET (V_ROW_NUM).NRIC_OTH, ' '));
                  ELSE
                     PG_EXCEL_UTILS.CELL (11, rowIDx, ' ');
                  END IF;

                  PG_EXCEL_UTILS.CELL (
                     12,
                     rowIDx,
                        RISK_DET (V_ROW_NUM).RISK_ID
                     || '-'
                     || REC.POLICY_REF
                     || '-'
                     || RISK_DET (V_ROW_NUM).COV_SEQ_REF);
                  PG_EXCEL_UTILS.CELL (13, rowIDx, ' ');
                  PG_EXCEL_UTILS.CELL (
                     14,
                     rowIDx,
                     NVL (RISK_DET (V_ROW_NUM).EMPLOYEE_ID, ' '));
                  PG_EXCEL_UTILS.CELL (
                     15,
                     rowIDx,
                     NVL (RISK_DET (V_ROW_NUM).MARITAL_STATUS, ' '));
                  PG_EXCEL_UTILS.CELL (16, rowIDx, ' ');
                  PG_EXCEL_UTILS.CELL (17,
                                       rowIDx,
                                       NVL (REC.PhoneNumber, ' '));
                  PG_EXCEL_UTILS.CELL (18, rowIDx, ' ');
                  PG_EXCEL_UTILS.CELL (19, rowIDx, ' ');
                  PG_EXCEL_UTILS.CELL (
                     20,
                     rowIDx,
                     NVL (RISK_DET (V_ROW_NUM).RELATIONSHIP, ' '));
                  PG_EXCEL_UTILS.CELL (21, rowIDx, ' ');

                  --1.1
                  IF RISK_DET (V_ROW_NUM).INSURED_TYPE = 'P'
                  THEN
                     PG_EXCEL_UTILS.CELL (
                        22,
                        rowIDx,
                           RISK_DET (V_ROW_NUM).RISK_ID
                        || '-'
                        || REC.POLICY_REF
                        || '-'
                        || RISK_DET (V_ROW_NUM).COV_SEQ_REF);
                     PG_EXCEL_UTILS.CELL (
                        23,
                        rowIDx,
                        NVL (RISK_DET (V_ROW_NUM).MEMBER_FULL_NAME, ' '));
                     PG_EXCEL_UTILS.CELL (
                        24,
                        rowIDx,
                        NVL (RISK_DET (V_ROW_NUM).NRIC, ' '));

                     IF RISK_DET (V_ROW_NUM).NRIC IS NULL
                     THEN
                        PG_EXCEL_UTILS.CELL (
                           25,
                           rowIDx,
                           NVL (RISK_DET (V_ROW_NUM).NRIC_OTH, ' '));
                     ELSE
                        PG_EXCEL_UTILS.CELL (25, rowIDx, ' ');
                     END IF;
                  ELSE
                     PG_EXCEL_UTILS.CELL (
                        22,
                        rowIDx,
                           RISK_DET (V_ROW_NUM).RISK_PARENT_ID
                        || '-'
                        || REC.POLICY_REF
                        || '-'
                        || RISK_DET (V_ROW_NUM).Parent_cov_seq_no);
                     V_PRINCIPAL_DET :=
                        PG_TPA_UTILS.FN_GET_PRINCIPAL_DET (
                           REC.CONTRACT_ID,
                           REC.POLICY_VERSION,
                           RISK_DET (V_ROW_NUM).RISK_PARENT_ID);
                     PG_EXCEL_UTILS.CELL (
                        23,
                        rowIDx,
                        NVL (V_PRINCIPAL_DET.MEMBER_FULL_NAME, ' '));
                     PG_EXCEL_UTILS.CELL (24,
                                          rowIDx,
                                          NVL (V_PRINCIPAL_DET.NRIC, ' '));

                     IF V_PRINCIPAL_DET.NRIC IS NULL
                     THEN
                        PG_EXCEL_UTILS.CELL (
                           25,
                           rowIDx,
                           NVL (V_PRINCIPAL_DET.NRIC_OTH, ' '));
                     ELSE
                        PG_EXCEL_UTILS.CELL (25, rowIDx, ' ');
                     END IF;
                  END IF;

                  PG_EXCEL_UTILS.CELL (26, rowIDx, ' ');
                  PG_EXCEL_UTILS.CELL (27, rowIDx, 'IG');
                  PG_EXCEL_UTILS.CELL (28, rowIDx, NVL (REC.POLICY_REF, ' '));
                  PG_EXCEL_UTILS.CELL (29, rowIDx, REC.EFF_DATE);
                  PG_EXCEL_UTILS.CELL (30, rowIDx, REC.EXP_DATE);
                  PG_EXCEL_UTILS.CELL (31, rowIDx, NVL (REC.prev_pol, ' '));
                  PG_EXCEL_UTILS.CELL (32, rowIDx, REC.prev_exp_date);
                  PG_EXCEL_UTILS.CELL (33, rowIDx, NVL (REC.NAME_EXT, ' '));
                  V_UWPL_COVER_DET :=
                     PG_TPA_UTILS.FN_GET_UWPL_COVER_DET (
                        REC.CONTRACT_ID,
                        REC.POLICY_VERSION,
                        RISK_DET (V_ROW_NUM).COV_ID);
                  PG_EXCEL_UTILS.CELL (34,
                                       rowIDx,
                                       NVL (V_UWPL_COVER_DET.PLAN_CODE, ' '));
                  PG_EXCEL_UTILS.CELL (35, rowIDx, ' ');

                  --1.4
                  IF RISK_DET (V_ROW_NUM).ORIGINAL_JOIN_DATE IS NULL
                  THEN
                     PG_EXCEL_UTILS.CELL (36, rowIDx, ' ');
                  ELSE
                     PG_EXCEL_UTILS.CELL (
                        36,
                        rowIDx,
                        TO_CHAR (RISK_DET (V_ROW_NUM).ORIGINAL_JOIN_DATE,
                                 'DD/MM/YYYY'));
                  END IF;

                  PG_EXCEL_UTILS.CELL (37,
                                       rowIDx,
                                       RISK_DET (V_ROW_NUM).RISK_EFF_DATE);
                  PG_EXCEL_UTILS.CELL (38,
                                       rowIDx,
                                       RISK_DET (V_ROW_NUM).RISK_EXP_DATE);
                  PG_EXCEL_UTILS.CELL (39, rowIDx, REC.BRANCH_DESC);
                  PG_EXCEL_UTILS.CELL (40, rowIDx, NVL (REC.AGENT_NAME, ' '));
                  PG_EXCEL_UTILS.CELL (41, rowIDx, NVL (REC.AGENT_CODE, ' '));

                  --116958_ALLIANZ SHIELD PLUS start
                  IF (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                         V_RISK_LEVEL_DTLS,
                         REC.PRODUCT_CONFIG_CODE) = 'Y')
                  THEN
                     PG_EXCEL_UTILS.CELL (
                        42,
                        rowIDx,
                        NVL (RISK_DET (V_ROW_NUM).MCO_FEE, 0));

                     IF NVL (RISK_DET (V_ROW_NUM).IMA_FEE, 0) > 0
                     THEN
                        PG_EXCEL_UTILS.CELL (
                           43,
                           rowIDx,
                           'Y',
                           p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                              p_vertical     => 'center',
                                              p_horizontal   => 'center',
                                              p_wrapText     => TRUE));
                     ELSE
                        PG_EXCEL_UTILS.CELL (
                           43,
                           rowIDx,
                           'N',
                           p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                              p_vertical     => 'center',
                                              p_horizontal   => 'center',
                                              p_wrapText     => TRUE));
                     END IF;

                     IF NVL (RISK_DET (V_ROW_NUM).IMA_FEE, 0) > 0
                     THEN
                        IF (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                               V_IMA_LMT_2M,
                               REC.PRODUCT_CONFIG_CODE) = 'Y')
                        THEN
                           PG_EXCEL_UTILS.CELL (44, rowIDx, '2000000');
                        ELSE
                           PG_EXCEL_UTILS.CELL (44, rowIDx, '1000000');
                        END IF;
                     ELSE
                        PG_EXCEL_UTILS.CELL (44, rowIDx, ' ');
                     END IF;
                  ELSE
                     --116958_ALLIANZ SHIELD PLUS end
                     PG_EXCEL_UTILS.CELL (
                        42,
                        rowIDx,
                          NVL (REC.MCO_FEE_AMT, 0)
                        + NVL (REC.MCOI_FEE_AMT, 0)
                        + NVL (REC.MCOO_FEE_AMT, 0));

                     IF REC.IMA_FEE_AMT > 0
                     THEN
                        PG_EXCEL_UTILS.CELL (
                           43,
                           rowIDx,
                           'Y',
                           p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                              p_vertical     => 'center',
                                              p_horizontal   => 'center',
                                              p_wrapText     => TRUE));
                     ELSE
                        PG_EXCEL_UTILS.CELL (
                           43,
                           rowIDx,
                           'N',
                           p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                              p_vertical     => 'center',
                                              p_horizontal   => 'center',
                                              p_wrapText     => TRUE));
                     END IF;

                     IF REC.IMA_FEE_AMT > 0
                     THEN
                        --116958_ALLIANZ SHIELD PLUS start
                        IF (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                               V_IMA_LMT_2M,
                               REC.PRODUCT_CONFIG_CODE) = 'Y')
                        THEN
                           PG_EXCEL_UTILS.CELL (44, rowIDx, '2000000');
                        ELSE
                           --116958_ALLIANZ SHIELD PLUS end
                           PG_EXCEL_UTILS.CELL (44, rowIDx, '1000000');
                        END IF;                   --116958_ALLIANZ SHIELD PLUS
                     ELSE
                        PG_EXCEL_UTILS.CELL (44, rowIDx, ' ');
                     END IF;
                  END IF;                         --116958_ALLIANZ SHIELD PLUS

                  PG_EXCEL_UTILS.CELL (45, rowIDx, REC.DateReceivedbyAAN);

                  IF REC.POLICY_VERSION > 1 AND REC.ENDT_CODE IN ('96', '97')
                  THEN
                     PG_EXCEL_UTILS.CELL (46, rowIDx, REC.ENDT_EFF_DATE);
                  ELSE
                     PG_EXCEL_UTILS.CELL (46,
                                          rowIDx,
                                          RISK_DET (V_ROW_NUM).TEMINATE_DATE);
                  END IF;

                  --1.4
                  --dbms_output.put_line ('V_ROW_NUM::'||V_ROW_NUM);
                  IF V_ROW_NUM = 1
                  THEN
                     IF DBMS_LOB.getlength (REC.ENDT_NARR) > 32000
                     THEN
                        V_ENDT_NARR_ARRAY :=
                           PG_TPA_UTILS.FN_SPLIT_CLOB (REC.ENDT_NARR);

                        FOR I IN 1 .. V_ENDT_NARR_ARRAY.COUNT
                        LOOP
                           --dbms_output.put_line('I::'||V_ENDT_NARR_ARRAY(I));
                           IF I = 1
                           THEN
                              PG_EXCEL_UTILS.CELL (
                                 47,
                                 rowIDx,
                                 NVL (V_ENDT_NARR_ARRAY (1), ' '));
                           ELSE
                              PG_EXCEL_UTILS.CELL (
                                 50 + I,
                                 rowIDx,
                                 NVL (V_ENDT_NARR_ARRAY (I), ' '));
                           END IF;
                        END LOOP;
                     ELSE
                        PG_EXCEL_UTILS.CELL (47,
                                             rowIDx,
                                             NVL (REC.ENDT_NARR, ' '));
                     END IF;
                  ELSE
                     PG_EXCEL_UTILS.CELL (47, rowIDx, ' ');
                  END IF;

                  PG_EXCEL_UTILS.CELL (
                     48,
                     rowIDx,
                     NVL (
                        PG_TPA_UTILS.FN_GET_RISK_QUESTION (
                           REC.CONTRACT_ID,
                           REC.POLICY_VERSION,
                           RISK_DET (V_ROW_NUM).RISK_ID),
                        ' '));
                  PG_EXCEL_UTILS.CELL (49,
                                       rowIDx,
                                       NVL (V_UWPL_COVER_DET.REMARKS, ' '));
                  PG_EXCEL_UTILS.CELL (50,
                                       rowIDx,
                                       NVL (PG_TPA_UTILS.FN_GET_COVER_DIAGNOSIS (
                                               REC.CONTRACT_ID,
                                               REC.POLICY_VERSION,
                                               RISK_DET (V_ROW_NUM).RISK_ID,
                                               RISK_DET (V_ROW_NUM).COV_ID),
                                            ' '));

                  --1.4
                  IF RISK_DET (V_ROW_NUM).OP_SUB_COV > 0
                     AND (RISK_DET (V_ROW_NUM).IMPORT_TYPE IS NOT NULL AND RISK_DET (V_ROW_NUM).IMPORT_TYPE <> 'XO')
                  THEN
                     PG_EXCEL_UTILS.CELL (51, rowIDx, 'Y');
                  ELSE
                     PG_EXCEL_UTILS.CELL (51, rowIDx, 'N');
                  END IF;

                  PG_EXCEL_UTILS.CELL (
                     52,
                     rowIDx,
                     NVL (RISK_DET (V_ROW_NUM).PREV_POL_OP_IND, ' '));   --2.0

                  PG_EXCEL_UTILS.CELL (
                     53,
                     rowIDx,
                     NVL (RISK_DET (V_ROW_NUM).DEPARTMENT, ' '));        --2.0

                  rowIDx := rowIDx + 1;
                  seq := seq + 1;
               END IF;                            --116958_ALLIANZ SHIELD PLUS
            END LOOP;

            --dbms_output.put_line ('RISK_DET::'||RISK_DET.COUNT);
            IF RISK_DET.COUNT > 0
            THEN
               V_RET :=
                  PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD (REC.CONTRACT_ID,
                                                      REC.POLICY_VERSION,
                                                      'AAN');            --1.3
            END IF;
         END IF;
      END IF;
   END LOOP;

   V_STEPS := '010';

   FOR REC IN C_TPA_AAN_HC
   LOOP
      IF    (REC.POLICY_VERSION = 1 AND REC.MCOI_FEE_AMT > 0)
         OR (REC.POLICY_VERSION > 1 AND REC.MCOI_FEE_AMT >= 0)
         OR (REC.POLICY_VERSION = 1 AND REC.IMA_FEE_AMT > 0)
         OR (REC.POLICY_VERSION > 1 AND REC.IMA_FEE_AMT >= 0)
         OR (    PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_MCOO,
                                                     REC.PRODUCT_CONFIG_CODE) =
                    'Y'
             AND (   (REC.POLICY_VERSION = 1 AND REC.MCOO_FEE_AMT > 0)
                  OR (REC.POLICY_VERSION > 1 AND REC.MCOO_FEE_AMT >= 0)))
         OR (    PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_DMA,
                                                     REC.PRODUCT_CONFIG_CODE) =
                    'Y'
             AND (   (    REC.POLICY_VERSION = 1
                      AND (REC.MCOO_FEE_AMT > 0 OR REC.DMA_FEE_AMT > 0))
                  OR (    REC.POLICY_VERSION > 1
                      AND (REC.MCOO_FEE_AMT >= 0 OR REC.DMA_FEE_AMT >= 0))))
      --          IF ((REC.POLICY_VERSION =1 AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_MCOI,REC.PRODUCT_CONFIG_CODE) = 'Y')
      --          AND ((REC.MCOI_FEE_AMT >0 AND REC.IMA_FEE_AMT >0) OR REC.IMA_FEE_AMT >0))
      --            OR ((REC.POLICY_VERSION >1 AND  PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_MCOI,REC.PRODUCT_CONFIG_CODE) = 'Y')
      --             AND ((REC.MCOI_FEE_AMT >=0 AND REC.IMA_FEE_AMT >=0) OR REC.IMA_FEE_AMT >=0))
      --            OR ((REC.POLICY_VERSION =1 AND  PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_MCOO,REC.PRODUCT_CONFIG_CODE) = 'Y')
      --             AND ((REC.MCOI_FEE_AMT >0 AND REC.IMA_FEE_AMT >0) OR (REC.MCOI_FEE_AMT >0 AND REC.MCOO_FEE_AMT >0) OR REC.IMA_FEE_AMT >0))
      --            OR ((REC.POLICY_VERSION >1 AND  PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_MCOO,REC.PRODUCT_CONFIG_CODE) = 'Y')
      --            AND ((REC.MCOI_FEE_AMT >=0 AND REC.IMA_FEE_AMT >=0) OR (REC.MCOI_FEE_AMT >=0 AND REC.MCOO_FEE_AMT >=0) OR REC.IMA_FEE_AMT >=0))
      --            OR ((REC.POLICY_VERSION =1 AND  PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_DMA,REC.PRODUCT_CONFIG_CODE) = 'Y')
      --             AND ((REC.MCOI_FEE_AMT >0 AND REC.IMA_FEE_AMT >0) OR (REC.MCOI_FEE_AMT >0 AND REC.MCOO_FEE_AMT >0)
      --             OR (REC.MCOI_FEE_AMT >0 AND REC.DMA_FEE_AMT >0) OR REC.IMA_FEE_AMT >0 OR REC.DMA_FEE_AMT >0))
      --            OR ((REC.POLICY_VERSION >1 AND  PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_DMA,REC.PRODUCT_CONFIG_CODE) = 'Y')
      --            AND ((REC.MCOI_FEE_AMT >=0 AND REC.IMA_FEE_AMT >=0) OR (REC.MCOI_FEE_AMT >=0 AND REC.MCOO_FEE_AMT >=0)
      --            OR (REC.MCOI_FEE_AMT >=0 AND REC.DMA_FEE_AMT >=0) OR REC.IMA_FEE_AMT >=0 OR REC.DMA_FEE_AMT >=0 ))
      THEN
         RISK_DET.DELETE;

         -->> 2.0 start comment
         /*IF     REC.ENDT_CODE IS NOT NULL
            AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_AAN_ENDT_CODE_R,
                                                    REC.ENDT_CODE) = 'Y'
         THEN
            BEGIN
               EXECUTE IMMEDIATE V_SELECTED_RISK_SQL
                  BULK COLLECT INTO RISK_DET
                  USING REC.CONTRACT_ID, REC.POLICY_VERSION;
            END;
         ELSE
            BEGIN
               EXECUTE IMMEDIATE V_ALL_RISK_SQL
                  BULK COLLECT INTO RISK_DET
                  USING REC.POLICY_VERSION,
                        REC.POLICY_VERSION,
                        REC.CONTRACT_ID,
                        REC.CONTRACT_ID,
                        REC.POLICY_VERSION;
            END;
         END IF;*/
         --<< 2.0 end comment

         -->> 2.0 start
         SELECT COUNT (1)
           INTO V_COUNT_TPA_RISK
           FROM UWGE_RISK_TPA_DOWNLOAD TPA
          WHERE     TPA.CONTRACT_ID = REC.CONTRACT_ID
                AND TPA.VERSION_NO = REC.POLICY_VERSION;

         IF V_COUNT_TPA_RISK > 0
         THEN
            BEGIN
               EXECUTE IMMEDIATE V_SELECTED_RISK_SQL_TPA
                  BULK COLLECT INTO RISK_DET
                  USING REC.CONTRACT_ID, REC.POLICY_VERSION;
            END;
         ELSE
            IF     REC.ENDT_CODE IS NOT NULL
               AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_AAN_ENDT_CODE_R,
                                                       REC.ENDT_CODE) = 'Y'
            THEN
               BEGIN
                  EXECUTE IMMEDIATE V_SELECTED_RISK_SQL
                     BULK COLLECT INTO RISK_DET
                     USING REC.CONTRACT_ID, REC.POLICY_VERSION;
               END;
            ELSE
               BEGIN
                  EXECUTE IMMEDIATE V_ALL_RISK_SQL
                     BULK COLLECT INTO RISK_DET
                     USING REC.POLICY_VERSION,
                           REC.POLICY_VERSION,
                           REC.CONTRACT_ID,
                           REC.CONTRACT_ID,
                           REC.POLICY_VERSION;
               END;
            END IF;
         END IF;

         --<< 2.0 end

         V_ROW_NUM := 0;

         FOR V_ROW_NUM IN 1 .. RISK_DET.COUNT
         LOOP
            V_STEPS := '011';
            PG_EXCEL_UTILS.CELL (1, rowIDx, seq);

            -->> 2.0 start comment
            /*IF REC.POLICY_VERSION = 1 AND REC.PREV_POL_NO IS NOT NULL
            THEN
               PG_EXCEL_UTILS.CELL (2, rowIDx, 'R');
            ELSIF REC.POLICY_VERSION = 1 AND REC.PREV_POL_NO IS NULL
            THEN
               PG_EXCEL_UTILS.CELL (2, rowIDx, 'N');
            ELSIF REC.POLICY_VERSION > 1 AND REC.ENDT_CODE IN ('96', '97')
            THEN
               PG_EXCEL_UTILS.CELL (2, rowIDx, 'X');
            ELSE
               PG_EXCEL_UTILS.CELL (2, rowIDx, 'E');
            END IF;*/
            -->> 2.0 end comment

            -->> 2.0 start
            IF RISK_DET (V_ROW_NUM).IMPORT_TYPE IS NOT NULL
            THEN
               PG_EXCEL_UTILS.CELL (2,
                                    rowIDx,
                                    RISK_DET (V_ROW_NUM).IMPORT_TYPE);
            ELSE
               IF REC.POLICY_VERSION = 1 AND REC.PREV_POL_NO IS NOT NULL
               THEN
                  PG_EXCEL_UTILS.CELL (2, rowIDx, 'R');
               ELSIF REC.POLICY_VERSION = 1 AND REC.PREV_POL_NO IS NULL
               THEN
                  PG_EXCEL_UTILS.CELL (2, rowIDx, 'N');
               ELSIF REC.POLICY_VERSION > 1 AND REC.ENDT_CODE IN ('96', '97')
               THEN
                  PG_EXCEL_UTILS.CELL (2, rowIDx, 'X');
               ELSE
                  PG_EXCEL_UTILS.CELL (2, rowIDx, 'E');
               END IF;
            END IF;

            --<< 2.0 end

            PG_EXCEL_UTILS.CELL (
               3,
               rowIDx,
               NVL (RISK_DET (V_ROW_NUM).MEMBER_FULL_NAME, ' '));
            PG_EXCEL_UTILS.CELL (
               4,
               rowIDx,
               NVL (REC.ADDRESS_LINE1, ' ') || NVL (REC.ADDRESS_LINE2, ' '));
            PG_EXCEL_UTILS.CELL (5, rowIDx, NVL (REC.ADDRESS_LINE3, ' '));
            PG_EXCEL_UTILS.CELL (
               6,
               rowIDx,
               NVL (REC.POSTCODE, ' ') || ' ' || NVL (REC.CITY, ' '));
            PG_EXCEL_UTILS.CELL (7, rowIDx, NVL (REC.STATE, ' '));
            PG_EXCEL_UTILS.CELL (8,
                                 rowIDx,
                                 NVL (RISK_DET (V_ROW_NUM).SEX, ' '));

            IF RISK_DET (V_ROW_NUM).DATE_OF_BIRTH IS NULL
            THEN
               PG_EXCEL_UTILS.CELL (9, rowIDx, ' ');
            ELSE
               PG_EXCEL_UTILS.CELL (
                  9,
                  rowIDx,
                  TO_CHAR (RISK_DET (V_ROW_NUM).DATE_OF_BIRTH, 'DD/MM/YYYY'));
            END IF;

            PG_EXCEL_UTILS.CELL (10,
                                 rowIDx,
                                 NVL (RISK_DET (V_ROW_NUM).NRIC, ' '));

            IF RISK_DET (V_ROW_NUM).NRIC IS NULL
            THEN
               PG_EXCEL_UTILS.CELL (11,
                                    rowIDx,
                                    NVL (RISK_DET (V_ROW_NUM).NRIC_OTH, ' '));
            ELSE
               PG_EXCEL_UTILS.CELL (11, rowIDx, ' ');
            END IF;

            PG_EXCEL_UTILS.CELL (
               12,
               rowIDx,
                  RISK_DET (V_ROW_NUM).RISK_ID
               || '-'
               || REC.POLICY_REF
               || '-'
               || RISK_DET (V_ROW_NUM).COV_SEQ_REF);
            PG_EXCEL_UTILS.CELL (13, rowIDx, ' ');
            PG_EXCEL_UTILS.CELL (14,
                                 rowIDx,
                                 NVL (RISK_DET (V_ROW_NUM).EMPLOYEE_ID, ' '));
            PG_EXCEL_UTILS.CELL (
               15,
               rowIDx,
               NVL (RISK_DET (V_ROW_NUM).MARITAL_STATUS, ' '));
            PG_EXCEL_UTILS.CELL (16, rowIDx, ' ');
            PG_EXCEL_UTILS.CELL (17, rowIDx, NVL (REC.PhoneNumber, ' '));
            PG_EXCEL_UTILS.CELL (18, rowIDx, ' ');
            PG_EXCEL_UTILS.CELL (19, rowIDx, ' ');
            PG_EXCEL_UTILS.CELL (
               20,
               rowIDx,
               NVL (RISK_DET (V_ROW_NUM).RELATIONSHIP, ' '));
            PG_EXCEL_UTILS.CELL (21, rowIDx, ' ');

            --1.1
            IF RISK_DET (V_ROW_NUM).INSURED_TYPE = 'P'
            THEN
               PG_EXCEL_UTILS.CELL (
                  22,
                  rowIDx,
                     RISK_DET (V_ROW_NUM).RISK_ID
                  || '-'
                  || REC.POLICY_REF
                  || '-'
                  || RISK_DET (V_ROW_NUM).COV_SEQ_REF);
               PG_EXCEL_UTILS.CELL (
                  23,
                  rowIDx,
                  NVL (RISK_DET (V_ROW_NUM).MEMBER_FULL_NAME, ' '));
               PG_EXCEL_UTILS.CELL (24,
                                    rowIDx,
                                    NVL (RISK_DET (V_ROW_NUM).NRIC, ' '));

               IF RISK_DET (V_ROW_NUM).NRIC IS NULL
               THEN
                  PG_EXCEL_UTILS.CELL (
                     25,
                     rowIDx,
                     NVL (RISK_DET (V_ROW_NUM).NRIC_OTH, ' '));
               ELSE
                  PG_EXCEL_UTILS.CELL (25, rowIDx, ' ');
               END IF;
            ELSE
               PG_EXCEL_UTILS.CELL (
                  22,
                  rowIDx,
                     RISK_DET (V_ROW_NUM).RISK_PARENT_ID
                  || '-'
                  || REC.POLICY_REF
                  || '-'
                  || RISK_DET (V_ROW_NUM).Parent_cov_seq_no);
               V_PRINCIPAL_DET :=
                  PG_TPA_UTILS.FN_GET_PRINCIPAL_DET (
                     REC.CONTRACT_ID,
                     REC.POLICY_VERSION,
                     RISK_DET (V_ROW_NUM).RISK_PARENT_ID);
               PG_EXCEL_UTILS.CELL (
                  23,
                  rowIDx,
                  NVL (V_PRINCIPAL_DET.MEMBER_FULL_NAME, ' '));
               PG_EXCEL_UTILS.CELL (24,
                                    rowIDx,
                                    NVL (V_PRINCIPAL_DET.NRIC, ' '));

               IF V_PRINCIPAL_DET.NRIC IS NULL
               THEN
                  PG_EXCEL_UTILS.CELL (25,
                                       rowIDx,
                                       NVL (V_PRINCIPAL_DET.NRIC_OTH, ' '));
               ELSE
                  PG_EXCEL_UTILS.CELL (25, rowIDx, ' ');
               END IF;
            END IF;

            PG_EXCEL_UTILS.CELL (26, rowIDx, ' ');
            PG_EXCEL_UTILS.CELL (27, rowIDx, 'IG');
            PG_EXCEL_UTILS.CELL (28, rowIDx, NVL (REC.POLICY_REF, ' '));
            PG_EXCEL_UTILS.CELL (29, rowIDx, REC.EFF_DATE);
            PG_EXCEL_UTILS.CELL (30, rowIDx, REC.EXP_DATE);
            PG_EXCEL_UTILS.CELL (31, rowIDx, NVL (REC.prev_pol, ' '));
            PG_EXCEL_UTILS.CELL (32, rowIDx, REC.prev_exp_date);
            PG_EXCEL_UTILS.CELL (33, rowIDx, NVL (REC.NAME_EXT, ' '));
            V_UWPL_COVER_DET :=
               PG_TPA_UTILS.FN_GET_UWPL_COVER_DET (
                  REC.CONTRACT_ID,
                  REC.POLICY_VERSION,
                  RISK_DET (V_ROW_NUM).COV_ID);
            PG_EXCEL_UTILS.CELL (34,
                                 rowIDx,
                                 NVL (V_UWPL_COVER_DET.PLAN_CODE, ' '));
            PG_EXCEL_UTILS.CELL (35, rowIDx, ' ');

            --1.4
            IF RISK_DET (V_ROW_NUM).ORIGINAL_JOIN_DATE IS NULL
            THEN
               PG_EXCEL_UTILS.CELL (36, rowIDx, ' ');
            ELSE
               PG_EXCEL_UTILS.CELL (
                  36,
                  rowIDx,
                  TO_CHAR (RISK_DET (V_ROW_NUM).ORIGINAL_JOIN_DATE,
                           'DD/MM/YYYY'));
            END IF;

            PG_EXCEL_UTILS.CELL (37,
                                 rowIDx,
                                 RISK_DET (V_ROW_NUM).RISK_EFF_DATE);
            PG_EXCEL_UTILS.CELL (38,
                                 rowIDx,
                                 RISK_DET (V_ROW_NUM).RISK_EXP_DATE);
            PG_EXCEL_UTILS.CELL (39, rowIDx, REC.BRANCH_DESC);

            PG_EXCEL_UTILS.CELL (40, rowIDx, NVL (REC.AGENT_NAME, ' '));
            PG_EXCEL_UTILS.CELL (41, rowIDx, NVL (REC.AGENT_CODE, ' '));
            PG_EXCEL_UTILS.CELL (
               42,
               rowIDx,
                 NVL (REC.MCO_FEE_AMT, 0)
               + NVL (REC.MCOI_FEE_AMT, 0)
               + NVL (REC.MCOO_FEE_AMT, 0));

            IF REC.IMA_FEE_AMT > 0
            THEN
               PG_EXCEL_UTILS.CELL (
                  43,
                  rowIDx,
                  'Y',
                  p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                     p_vertical     => 'center',
                                     p_horizontal   => 'center',
                                     p_wrapText     => TRUE));
            ELSE
               PG_EXCEL_UTILS.CELL (
                  43,
                  rowIDx,
                  'N',
                  p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                     p_vertical     => 'center',
                                     p_horizontal   => 'center',
                                     p_wrapText     => TRUE));
            END IF;

            IF REC.IMA_FEE_AMT > 0
            THEN
               PG_EXCEL_UTILS.CELL (44, rowIDx, '1000000');
            ELSE
               PG_EXCEL_UTILS.CELL (44, rowIDx, ' ');
            END IF;

            PG_EXCEL_UTILS.CELL (45, rowIDx, REC.DateReceivedbyAAN);

            IF REC.POLICY_VERSION > 1 AND REC.ENDT_CODE IN ('96', '97')
            THEN
               PG_EXCEL_UTILS.CELL (46, rowIDx, REC.ENDT_EFF_DATE);
            ELSE
               PG_EXCEL_UTILS.CELL (46,
                                    rowIDx,
                                    RISK_DET (V_ROW_NUM).TEMINATE_DATE);
            END IF;

            --dbms_output.put_line ('V_ROW_NUM::'||V_ROW_NUM);
            IF V_ROW_NUM = 1
            THEN
               --1.4
               --dbms_output.put_line ('ENDT_NARR::'||DBMS_LOB.getlength(REC.ENDT_NARR));
               IF DBMS_LOB.getlength (REC.ENDT_NARR) > 32000
               THEN
                  V_ENDT_NARR_ARRAY :=
                     PG_TPA_UTILS.FN_SPLIT_CLOB (REC.ENDT_NARR);

                  FOR I IN 1 .. V_ENDT_NARR_ARRAY.COUNT
                  LOOP
                     --dbms_output.put_line('I::'||V_ENDT_NARR_ARRAY(I));
                     IF I = 1
                     THEN
                        PG_EXCEL_UTILS.CELL (
                           47,
                           rowIDx,
                           NVL (V_ENDT_NARR_ARRAY (1), ' '));
                     ELSE
                        PG_EXCEL_UTILS.CELL (
                           50 + I,
                           rowIDx,
                           NVL (V_ENDT_NARR_ARRAY (I), ' '));
                     END IF;
                  END LOOP;
               ELSE
                  PG_EXCEL_UTILS.CELL (47, rowIDx, NVL (REC.ENDT_NARR, ' '));
               END IF;
            ELSE
               PG_EXCEL_UTILS.CELL (47, rowIDx, ' ');
            END IF;

            PG_EXCEL_UTILS.CELL (
               48,
               rowIDx,
               NVL (
                  PG_TPA_UTILS.FN_GET_RISK_QUESTION (
                     REC.CONTRACT_ID,
                     REC.POLICY_VERSION,
                     RISK_DET (V_ROW_NUM).RISK_ID),
                  ' '));
            PG_EXCEL_UTILS.CELL (49,
                                 rowIDx,
                                 NVL (V_UWPL_COVER_DET.REMARKS, ' '));
            PG_EXCEL_UTILS.CELL (50,
                                 rowIDx,
                                 NVL (PG_TPA_UTILS.FN_GET_COVER_DIAGNOSIS (
                                         REC.CONTRACT_ID,
                                         REC.POLICY_VERSION,
                                         RISK_DET (V_ROW_NUM).RISK_ID,
                                         RISK_DET (V_ROW_NUM).COV_ID),
                                      ' '));

            --1.4
            IF RISK_DET (V_ROW_NUM).OP_SUB_COV > 0 
               AND (RISK_DET (V_ROW_NUM).IMPORT_TYPE IS NOT NULL AND RISK_DET (V_ROW_NUM).IMPORT_TYPE <> 'XO')
            THEN
               PG_EXCEL_UTILS.CELL (51, rowIDx, 'Y');
            ELSE
               PG_EXCEL_UTILS.CELL (51, rowIDx, 'N');
            END IF;

            PG_EXCEL_UTILS.CELL (
               52,
               rowIDx,
               NVL (RISK_DET (V_ROW_NUM).PREV_POL_OP_IND, ' '));         --2.0

            PG_EXCEL_UTILS.CELL (53,
                                 rowIDx,
                                 NVL (RISK_DET (V_ROW_NUM).DEPARTMENT, ' ')); --2.0

            rowIDx := rowIDx + 1;
            seq := seq + 1;
         END LOOP;

         --dbms_output.put_line ('RISK_DET::'||RISK_DET.COUNT);
         IF RISK_DET.COUNT > 0
         THEN
            V_RET :=
               PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD (REC.CONTRACT_ID,
                                                   REC.POLICY_VERSION,
                                                   'AAN');               --1.3
         END IF;
      END IF;
   END LOOP;

   V_STEPS := '016';
   DBMS_OUTPUT.ENABLE (buffer_size => NULL);
   PG_EXCEL_UTILS.save (v_file_dir, FILENAME1);
EXCEPTION
   WHEN OTHERS
   THEN
      PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
         V_PKG_NAME || V_FUNC_NAME,
         1,
         '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
--dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
END PC_TPA_AAN_HC_PA_POL_ENDT_2;
--<< 2.0 end
--<< START 3.0
PROCEDURE PC_TPA_MT_MONDIAL_POL(P_DOWNLOAD_TYPE IN VARCHAR2,
                                P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE) IS

  CURSOR C_TPA_MONDIAL_POL (P_COV_CODE IN UWGE_COVER.COV_CODE%TYPE)
      IS
       SELECT         UPV.VERSION_NO,
            UPV.ENDT_NO,
            OPB.POLICY_REF,
            UPV.CONTRACT_ID,
            (CASE  WHEN CP.ID_VALUE1 IS  NULL THEN 
                    CP.ID_VALUE2 
                   WHEN LENGTH(CP.ID_VALUE1)=12    THEN
                    SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
                   ELSE CP.ID_VALUE1
            END) AS NRIC_NUMBER,    
            CP.NAME_EXT,
            REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
            REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,
            REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3,
            CPA.POSTCODE,
            (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'CITY'  AND CODE_CD = CPA.CITY) AS CITY,
            (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'STATE' AND CODE_CD = CPA.STATE) AS STATE,
            UPB.CNOTE_NO,
            UPB.LONG_NAME,
            --TO_CHAR(UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE, --3.2 Comment
            --TO_CHAR(UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE, --3.2 Comment
            TO_CHAR(UPB.EFF_DATE, 'YYYYMMDD') AS EFF_DATE, --3.2 Added
            TO_CHAR(UPB.EXP_DATE, 'YYYYMMDD') AS EXP_DATE, --3.2 Added
            UPB.AGENT_CODE,
            --TO_CHAR(UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE, --3.2 Comment
            TO_CHAR(UPV.ISSUE_DATE, 'YYYYMMDD') AS ISSUE_DATE, --3.2 Added
            CP.EMAIL,
            regexp_replace((CASE  WHEN  CP.MOBILE_NO1 is not null and CP.MOBILE_CODE1 is not null THEN 
                                        CP.MOBILE_CODE1||CP.MOBILE_NO1   
                                   else CP.MOBILE_CODE2||CP.MOBILE_NO2 
                                   END),'[^0-9]') AS PhoneNumber,
            CPA.PHONE_CODE,
            CPA.PHONE_NO,
            URV.VEH_NO,
            URV.VEH_MODEL,
            NVL((select  CMV.VEH_MODEL_DESC from  CMUW_MODEL_VEH CMV where CMV.VEH_MODEL_CODE=URV.VEH_MODEL),' ') AS VEH_MODEL_DESC,
            (CASE WHEN  URV.VEH_MAKE_YEAR='0' THEN '0000' 
                  ELSE  URV.VEH_MAKE_YEAR||'' END)AS VEH_MAKE_YEAR,
            URV.VEH_CHASSIS,
            (SELECT UPLC.PLAN_CODE 
               FROM UWPL_COVER UPLC 
              WHERE UPLC.CONTRACT_ID = UCOV.CONTRACT_ID
                AND UPLC.COV_ID =UCOV.COV_ID 
                AND UPLC.VERSION_NO =UCOV.VERSION_NO) AS PLAN_CODE,
            (SELECT CODE_DESC 
               FROM CMGE_CODE 
              WHERE CAT_CODE ='POL_STATUS' 
                AND CODE_CD =UPC.POLICY_STATUS) AS POLICY_STATUS,
            --UPV.ENDT_EFF_DATE, --3.2 Comment
            TO_CHAR(UPV.ENDT_EFF_DATE, 'YYYYMMDD') AS ENDT_EFF_DATE, --3.2 Added
            UPV.INT_REMARK,
            UCOV.COV_CODE
       from UWGE_POLICY_VERSIONS  UPV
 INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD  ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID 
        AND UPV.VERSION_NO =UPCD.VERSION_NO
 INNER JOIN OCP_POLICY_BASES OPB      ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
 INNER JOIN UWGE_POLICY_CONTRACTS UPC ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
 INNER JOIN UWGE_POLICY_BASES UPB     ON UPB.CONTRACT_ID =UPV.CONTRACT_ID   
        AND UPB.VERSION_NO =UPV.VERSION_NO
 INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
            ON CP.PART_ID=UPB.CP_PART_ID      
        AND CP.VERSION=UPB.CP_VERSION
 INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (UPB.CP_ADDR_ID,UPB.CP_ADDR_VERSION)) CPA           ON CPA.ADD_ID = UPB.CP_ADDR_ID  
        AND CPA.VERSION = UPB.CP_ADDR_VERSION
 INNER JOIN UWGE_RISK_VEH URV      ON URV.CONTRACT_ID =UPV.CONTRACT_ID    
        AND URV.VERSION_NO = UPV.VERSION_NO
 INNER JOIN UWGE_COVER UCOV        ON UCOV.CONTRACT_ID =UPV.CONTRACT_ID   
        AND URV.RISK_ID =UCOV.RISK_ID 
        AND UCOV.VERSION_NO = UPV.VERSION_NO
      WHERE UCOV.COV_CODE = P_COV_CODE
        AND UCOV.ACTION_CODE <> 'T'
        AND UPC.PRODUCT_CONFIG_CODE IN(
        select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', P_DOWNLOAD_TYPE),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', P_DOWNLOAD_TYPE), '[^,]+', 1, level) is not null )
        AND ( UPCD.MERW_STATUS IS NULL OR UPCD.MERW_STATUS <>'D') 
        AND UPCD.TPA_NAME='MONDIAL'
        AND UPC.POLICY_STATUS ='A'
        AND UPV.VERSION_NO =1
        AND UPV.ENDT_NO IS NULL;

        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_MT_MONDIAL_POL';
        FILENAME  UTL_FILE.FILE_TYPE;
      FILENAME1 VARCHAR2(1000);
      v_file_dir VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_MONDIAL_DIR');
          --REC C_TPA_MONDIAL%rowtype;
          seq number := 1;
         V_RET                 NUMBER := 0;
         V_DOWNLOAD_TYPE     VARCHAR2(25):= NULL;


    BEGIN
            V_STEPS := '001';
        FOR COV IN 
                (SELECT REGEXP_SUBSTR (PG_RNGE_POL_BUILD.FN_GET_SYS_PARAM (
                                                                    'PG_TPA',
                                                                    'MONDIAL_MT_COV_CODE'),
                                                                    '[^,]+',
                                                                    1,
                                                                    LEVEL) MT_COV
                                                FROM DUAL
                                          CONNECT BY REGEXP_SUBSTR (PG_RNGE_POL_BUILD.FN_GET_SYS_PARAM (
                                                                    'PG_TPA',
                                                                    'MONDIAL_MT_COV_CODE'),
                                                                    '[^,]+',
                                                                    1,
                                                                    LEVEL)
                                              IS NOT NULL
                )
        LOOP
            V_DOWNLOAD_TYPE := 'M'||REPLACE(COV.MT_COV,'PAB-');
            FILENAME1   := TO_CHAR(P_START_DT, 'YYYYMMDD')||'_' || V_DOWNLOAD_TYPE || 'POL_MONDIAL.CSV';
            FILENAME    := UTL_FILE.FOPEN(v_file_dir, FILENAME1, 'W',32767); 

            UTL_FILE.PUT_LINE(FILENAME,
                       'Sequence Number'             || ',' || 
                       'Cover Note Number'             || ',' || 
                       'Policy Number'                 || ',' ||
                        'Chassis Number'             || ',' || 
                        'Vehicle Number'            || ',' || 
                        'Vehicle Make/Model'          || ',' || 
                        'Year Manufactured'            || ',' || 
                        'Program'                    || ',' || 
                        'Plan Code'                 || ',' || 
                        'Attaching Motor/Personal Accident Insurance'|| ',' || 
                        'Insured Name'                || ',' || 
                        'Insured''s IC/ID Number'    || ',' || 
                        'Insured''s Phone Number'     || ',' || 
                        'Insured''s Email Address'    || ',' || 
                        'Insured''s Home Address'     || ',' || 
                        'Postcode'                    || ',' || 
                        'City'                        || ',' || 
                        'State'                        || ',' || 
                        'Effective Date'             || ',' || 
                        'Expiry Date'                || ',' || 
                        'Transaction Date'            || ',' || 
                        'Policy Status'             || ',' || 
                        ' Endorsement Effective Date'|| ',' || 
                        ' Endorsement Remark'
                        );
            FOR REC IN C_TPA_MONDIAL_POL (COV.MT_COV )
            LOOP
                UTL_FILE.PUT_LINE(FILENAME,
                          seq                 
                          || ' ,' || '"'    || REC.CNOTE_NO         || '"' 
                          || ' ,' || '"'    || REC.POLICY_REF         || '"' 
                          || ' ,' || '"=""' || REC.VEH_CHASSIS         || '"""' 
                          || ' ,' || '"=""' || REC.VEH_NO            || '"""'
                          || ' ,' || '"'    || REC.VEH_MODEL_DESC    || '"'
                          || ' ,' || '"'     || REC.VEH_MAKE_YEAR    || '"'
                          || ' ,' || '"'     || V_DOWNLOAD_TYPE        || '"'
                          || ' ,' || '"'     || REC.PLAN_CODE        || '"'
                          || ' ,' || '""'    
                          || ' ,' || '"'     || REC.NAME_EXT            || '"'
                          || ' ,' || '"=""' || REC.NRIC_NUMBER        || '"""'
                          || ' ,' ||'"=""'    || REC.PhoneNumber        || '"""'
                          || ' ,' || '"'     || REC.EMAIL            || '"'
                          || ' ,' || '"'     || REC.ADDRESS_LINE1||' '||REC.ADDRESS_LINE2||' '||REC.ADDRESS_LINE3|| '"'
                          || ' ,' || '"'     || REC.POSTCODE            || '"'
                          || ' ,' || '"'     || REC.CITY                || '"'
                          || ' ,' || '"'     || REC.STATE            || '"'
                          || ' ,' || '"'     || REC.EFF_DATE            || '"'
                          || ' ,' || '"'     || REC.EXP_DATE            || '"'
                          || ' ,' || '"'     || REC.ISSUE_DATE        || '"'
                          || ' ,' || '"'     || REC.POLICY_STATUS    || '"'
                          || ' ,' || '"'     || REC.ENDT_EFF_DATE    || '"'
                          || ' ,' || '"'     || REC.INT_REMARK        || '"'
                          );


                V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD_MERW(REC.CONTRACT_ID,REC.VERSION_NO,'MONDIAL','D'); 
                seq :=seq+1;
            END LOOP;
            UTL_FILE.FCLOSE(FILENAME);
            V_DOWNLOAD_TYPE := NULL;
            seq             := 1;
        END LOOP;
    EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
    END PC_TPA_MT_MONDIAL_POL;

  PROCEDURE PC_TPA_MT_MONDIAL_ENDT(P_DOWNLOAD_TYPE IN VARCHAR2,
                                   P_START_DT IN UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE) IS

  CURSOR C_TPA_MONDIAL_ENDT (P_COV_CODE IN UWGE_COVER.COV_CODE%TYPE)
      IS
        SELECT         UPV.VERSION_NO AS POLICY_VERSION,
            UPV.ENDT_NO,
            OPB.POLICY_REF,
            UPV.CONTRACT_ID,
            (CASE  WHEN CP.ID_VALUE1 IS  NULL THEN 
                    CP.ID_VALUE2 
                   WHEN LENGTH(CP.ID_VALUE1)=12    THEN
                    SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
                   ELSE CP.ID_VALUE1
            END) AS NRIC_NUMBER,    
            CP.NAME_EXT,
            REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
            REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,
            REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3,
            CPA.POSTCODE,
            (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'CITY'  AND CODE_CD = CPA.CITY) AS CITY,
            (SELECT CODE_DESC FROM CMGE_CODE WHERE CAT_CODE = 'STATE' AND CODE_CD = CPA.STATE) AS STATE,
            UPB.CNOTE_NO,
            UPB.LONG_NAME,
            --TO_CHAR(UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE, --3.2 Comment
            --TO_CHAR(UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE, --3.2 Comment
            TO_CHAR(UPB.EFF_DATE, 'YYYYMMDD') AS EFF_DATE, --3.2 Added
            TO_CHAR(UPB.EXP_DATE, 'YYYYMMDD') AS EXP_DATE, --3.2 Added
            UPB.AGENT_CODE,
            --TO_CHAR(UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE, --3.2 Comment
            TO_CHAR(UPV.ISSUE_DATE, 'YYYYMMDD') AS ISSUE_DATE, --3.2 Added
            CP.EMAIL,
            regexp_replace((CASE  WHEN  CP.MOBILE_NO1 is not null and CP.MOBILE_CODE1 is not null THEN 
                                        CP.MOBILE_CODE1||CP.MOBILE_NO1   
                                   else CP.MOBILE_CODE2||CP.MOBILE_NO2 
                                   END),'[^0-9]') AS PhoneNumber,
            CPA.PHONE_CODE,
            CPA.PHONE_NO,
            URV.VEH_NO,
            URV.VEH_MODEL,
            NVL((select  CMV.VEH_MODEL_DESC from  CMUW_MODEL_VEH CMV where CMV.VEH_MODEL_CODE=URV.VEH_MODEL),' ') AS VEH_MODEL_DESC,
            (CASE WHEN  URV.VEH_MAKE_YEAR='0' THEN '0000' 
                  ELSE  URV.VEH_MAKE_YEAR||'' END)AS VEH_MAKE_YEAR,
            URV.VEH_CHASSIS,
            (SELECT UPLC.PLAN_CODE 
               FROM UWPL_COVER UPLC 
              WHERE UPLC.CONTRACT_ID = UCOV.CONTRACT_ID
                AND UPLC.COV_ID =UCOV.COV_ID 
                AND UPLC.VERSION_NO =UCOV.VERSION_NO) AS PLAN_CODE,
            (SELECT CODE_DESC 
               FROM CMGE_CODE 
              WHERE CAT_CODE ='POL_STATUS' 
                AND CODE_CD =UPC.POLICY_STATUS) AS POLICY_STATUS,
            --UPV.ENDT_EFF_DATE, --3.2 Comment
            TO_CHAR(UPV.ENDT_EFF_DATE, 'YYYYMMDD') AS ENDT_EFF_DATE, --3.2 Added
            UPV.INT_REMARK,
            UCOV.COV_CODE,
            --NVL(UPV.ENDT_NARR,' ') AS ENDT_NARR,
            REPLACE (UPV.ENDT_NARR, CHR (10), '') AS ENDT_NARR, -- Urgent fixes 3.1
            UCOV.COV_ID        
       from UWGE_POLICY_VERSIONS  UPV
 INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD  ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID 
        AND UPV.VERSION_NO =UPCD.VERSION_NO
 INNER JOIN OCP_POLICY_BASES OPB      ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
 INNER JOIN UWGE_POLICY_CONTRACTS UPC ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
 INNER JOIN UWGE_POLICY_BASES UPB     ON UPB.CONTRACT_ID =UPV.CONTRACT_ID   
        AND UPB.VERSION_NO =UPV.VERSION_NO
 INNER JOIN TABLE(CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(UPB.CP_PART_ID, UPB.CP_VERSION)) CP
            ON CP.PART_ID=UPB.CP_PART_ID      
        AND CP.VERSION=UPB.CP_VERSION
 INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (UPB.CP_ADDR_ID,UPB.CP_ADDR_VERSION)) CPA           ON CPA.ADD_ID = UPB.CP_ADDR_ID  
        AND CPA.VERSION = UPB.CP_ADDR_VERSION
 INNER JOIN SB_UWGE_RISK_VEH URV      ON URV.CONTRACT_ID =UPV.CONTRACT_ID    
        AND URV.POLICY_VERSION = UPV.VERSION_NO
 INNER JOIN UWGE_COVER UCOV        ON UCOV.CONTRACT_ID =UPV.CONTRACT_ID   
        AND URV.RISK_ID =UCOV.RISK_ID 
        AND UCOV.VERSION_NO = UPV.VERSION_NO
      WHERE UCOV.COV_CODE = P_COV_CODE
        AND UCOV.ACTION_CODE <> 'T'
        AND UPC.PRODUCT_CONFIG_CODE IN(
        select regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', P_DOWNLOAD_TYPE),'[^,]+', 1, level) from dual
        connect by regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', P_DOWNLOAD_TYPE), '[^,]+', 1, level) is not null )
        AND ( UPCD.MERW_STATUS IS NULL OR UPCD.MERW_STATUS <>'D') 
        AND UPCD.TPA_NAME='MONDIAL'
        AND UPC.POLICY_STATUS IN('A','C','E')
        AND UPV.VERSION_NO >1
        AND UPV.ENDT_NO IS NOT NULL; 

        V_STEPS             VARCHAR2(10);
        V_FUNC_NAME         VARCHAR2(100) :='PC_TPA_MT_MONDIAL_ENDT';
        FILENAME              UTL_FILE.FILE_TYPE;
        FILENAME1             VARCHAR2(1000);
        v_file_dir             VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_MONDIAL_DIR');
        V_RET               NUMBER := 0; 
        V_UWPL_COVER_DET       PG_TPA_UTILS.UWPL_COVER_DET;
        V_DOWNLOAD_TYPE     VARCHAR2(25):= NULL;
        V_PROG_TYPE            VARCHAR2(10):= NULL;

    BEGIN
          V_STEPS := '001';
        FOR COV IN 
                (SELECT REGEXP_SUBSTR (PG_RNGE_POL_BUILD.FN_GET_SYS_PARAM (
                                                                    'PG_TPA',
                                                                    'MONDIAL_MT_COV_CODE'),
                                                                    '[^,]+',
                                                                    1,
                                                                    LEVEL) MT_COV
                                                FROM DUAL
                                          CONNECT BY REGEXP_SUBSTR (PG_RNGE_POL_BUILD.FN_GET_SYS_PARAM (
                                                                    'PG_TPA',
                                                                    'MONDIAL_MT_COV_CODE'),
                                                                    '[^,]+',
                                                                    1,
                                                                    LEVEL)
                                              IS NOT NULL
                )
        LOOP
            V_DOWNLOAD_TYPE := 'M'||REPLACE(COV.MT_COV,'PAB-');
            FILENAME1   := TO_CHAR(P_START_DT, 'YYYYMMDD')||'_' || V_DOWNLOAD_TYPE || 'END_MONDIAL.CSV';
            FILENAME    := UTL_FILE.FOPEN(v_file_dir, FILENAME1, 'W',32767);
            V_PROG_TYPE := replace(COV.MT_COV,'PAB-','MT_');

            UTL_FILE.PUT_LINE(FILENAME,
                                'Sequence Number' 
                        || ',' || 'Cover Note Number' 
                        || ',' || 'Policy Number' 
                        || ',' || 'Chassis Number' 
                        || ',' || 'Vehicle Number'
                        || ',' || 'Vehicle Make/Model'
                        || ',' || 'Year Manufactured'
                        || ',' || 'Program'
                        || ',' || 'Plan Code'
                        || ',' || 'Attaching Motor/Personal Accident Insurance'
                        || ',' || 'Insured Name'
                        || ',' || 'Insured''s IC/ID Number'
                        || ',' || 'Insured''s Phone Number'
                        || ',' || 'Insured''s Email Address'
                        || ',' || 'Insured''s Home Address'
                        || ',' || 'Postcode'
                        || ',' || 'City'
                        || ',' || 'State'
                        || ',' || 'Effective Date'
                        || ',' || 'Expiry Date'
                        || ',' || 'Transaction Date'
                        || ',' || 'Policy Status'
                        || ',' || ' Endorsement Effective Date'
                        || ',' || ' Endorsement Remark');

            FOR REC IN C_TPA_MONDIAL_ENDT (COV.MT_COV )
            LOOP
                V_UWPL_COVER_DET := PG_TPA_UTILS.FN_GET_UWPL_COVER_DET(REC.CONTRACT_ID,REC.POLICY_VERSION,REC.COV_ID);
                UTL_FILE.PUT_LINE(FILENAME,
                          '"'||' '||'"' || ' ,' || '"' ||
                          REC.CNOTE_NO || '"' || ' ,' || '"' ||
                          REC.ENDT_NO || '"' || ' ,' || '"=""' ||
                          REC.VEH_CHASSIS || '"""' || ' ,' || '"=""' ||
                          REC.VEH_NO|| '"""'|| ' ,' || '"' ||
                          REC.VEH_MODEL_DESC|| '"'|| ' ,' || '"' ||
                          REC.VEH_MAKE_YEAR|| '"'|| ' ,' || '"' ||
                          V_PROG_TYPE|| '"'|| ' ,' || '"' ||
                          NVL(V_UWPL_COVER_DET.PLAN_CODE,' ')|| '"'|| ' ,' || '""'|| ' ,' || '"' ||
                          REC.NAME_EXT|| '"'|| ' ,' || '"=""' ||
                          REC.NRIC_NUMBER|| '"""'|| ' ,' || '"=""' ||
                          REC.PhoneNumber|| '"""'|| ' ,' || '"' ||
                          REC.EMAIL|| '"'|| ' ,' || '"' ||
                          REC.ADDRESS_LINE1||' '||REC.ADDRESS_LINE2||' '||REC.ADDRESS_LINE3|| '"'|| ' ,' || '"' ||
                          REC.POSTCODE|| '"'|| ' ,' || '"' ||
                          REC.CITY|| '"'|| ' ,' || '"' ||
                          REC.STATE|| '"'|| ' ,' || '"' ||
                          REC.EFF_DATE|| '"'|| ' ,' || '"' ||
                          REC.EXP_DATE|| '"'|| ' ,' || '"' ||
                          REC.ISSUE_DATE|| '"'|| ' ,' || '"'||REC.POLICY_STATUS||'"'|| ' ,' || '"' ||
                          REC.ENDT_EFF_DATE|| '"'|| ' ,' || '"' ||
                          REC.ENDT_NARR|| '"');

                V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD_MERW(REC.CONTRACT_ID,REC.POLICY_VERSION,'MONDIAL','D'); 
            END LOOP;
            UTL_FILE.FCLOSE(FILENAME);
            V_DOWNLOAD_TYPE := NULL;
        END LOOP;
    EXCEPTION
            WHEN OTHERS
            THEN
                PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                    V_PKG_NAME || V_FUNC_NAME,
                    1,
                    '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
    END PC_TPA_MT_MONDIAL_ENDT;
-->> END 3.0    
/* 3.5 START */
PROCEDURE PC_TPA_AAN_MS_POL_ENDT_ADHOC(P_START_DT IN UWGE_POLICY_BASES.EXP_DATE%TYPE) IS
    CURSOR C_TPA_AAN_MS
    IS
        SELECT OPB.POLICY_REF,UPV.VERSION_NO AS POLICY_VERSION,
        (CASE  WHEN CP.ID_VALUE1 is not null AND length(ID_VALUE1) =12 THEN  SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
        WHEN CP.ID_VALUE1 is not null THEN CP.ID_VALUE1 else CP.ID_VALUE2  END) AS ID_VALUE,
        (CASE WHEN ID_TYPE1 IS NULL THEN ID_TYPE2 ELSE ID_TYPE1 END) ID_TYPE,
        UPC.PRODUCT_CONFIG_CODE,(SELECT CODE_DESC FROM CMGE_CODE CC WHERE CC.CODE_CD=UPC.PRODUCT_CONFIG_CODE AND CC.CAT_CODE = UPC.LOB||'_PRODUCT') AS PRODUCT_DESC,
        CP.NAME_EXT,UPB.LONG_NAME,TO_CHAR(UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,TO_CHAR(UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
        TO_CHAR(UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,
        NVL((SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='ASST'),0) AS ASST_FEE_AMT,
        NVL((SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='DMA'),0) AS DMA_FEE_AMT,
        NVL(UPV.ENDT_NARR,' ') AS ENDT_NARR,UPV.ENDT_NO
        ,UPV.CONTRACT_ID
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWPL_RISK_PERSON URP
        ON URP.CONTRACT_ID =UPV.CONTRACT_ID
        AND URP.VERSION_NO =UPV.VERSION_NO
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(URP.RISK_PART_ID, URP.RISK_PART_VER)) CP
        ON CP.PART_ID=URP.RISK_PART_ID
        AND CP.VERSION=URP.RISK_PART_VER
        WHERE UPC.PRODUCT_CONFIG_CODE IN('012102')
        AND UPC.POLICY_STATUS IN('A')
        AND (UPV.ENDT_CODE IS NULL OR UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE_R,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE_R, '[^,]+', 1, level) is not null ))
        AND UPV.ACTION_CODE IN('A','C')
        AND UPC.LOB='MS'
        AND PLPB.TPA_NAME='AAN'
        AND UPB.EXP_DATE >= P_START_DT
        UNION ALL
        SELECT OPB.POLICY_REF,UPV.VERSION_NO AS POLICY_VERSION,
        (CASE  WHEN CP.ID_VALUE1 is not null AND length(ID_VALUE1) =12 THEN  SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
        WHEN CP.ID_VALUE1 is not null THEN CP.ID_VALUE1 else CP.ID_VALUE2  END) AS ID_VALUE,
        (CASE WHEN ID_TYPE1 IS NULL THEN ID_TYPE2 ELSE ID_TYPE1 END) ID_TYPE,
        UPC.PRODUCT_CONFIG_CODE,(SELECT CODE_DESC FROM CMGE_CODE CC WHERE CC.CODE_CD=UPC.PRODUCT_CONFIG_CODE AND CC.CAT_CODE = UPC.LOB||'_PRODUCT') AS PRODUCT_DESC,
        CP.NAME_EXT,UPB.LONG_NAME,TO_CHAR(UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,TO_CHAR(UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
        TO_CHAR(UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,
        NVL((SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='ASST'),0) AS ASST_FEE_AMT,
        NVL((SELECT UPF.FEE_AMT from UWGE_POLICY_FEES UPF
        WHERE UPF.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPF.VERSION_NO =UPV.VERSION_NO
        AND UPF.FEE_CODE ='DMA'),0) AS DMA_FEE_AMT,
        NVL(UPV.ENDT_NARR,' ') AS ENDT_NARR,UPV.ENDT_NO
        ,UPV.CONTRACT_ID
        from UWGE_POLICY_VERSIONS  UPV
        INNER JOIN UWGE_POLICY_CTRL_DLOAD UPCD
        ON UPV.CONTRACT_ID =UPCD.CONTRACT_ID
        AND UPV.VERSION_NO =UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
        ON UPV.CONTRACT_ID =UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
        ON UPV.CONTRACT_ID =OPB.CONTRACT_ID
        INNER JOIN UWGE_POLICY_BASES UPB
        ON UPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND UPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWPL_POLICY_BASES PLPB
        ON PLPB.CONTRACT_ID =UPV.CONTRACT_ID
        AND PLPB.VERSION_NO =UPV.VERSION_NO
        INNER JOIN UWPL_RISK_PERSON URP
        ON URP.CONTRACT_ID =UPV.CONTRACT_ID
        AND URP.VERSION_NO =(SELECT MAX (b.version_no)
        FROM UWPL_RISK_PERSON b
        WHERE b.contract_id = UPV.CONTRACT_ID
        AND URP.object_id = b.object_id
        AND b.version_no <= UPV.VERSION_NO
        AND b.reversing_version IS NULL)
        AND URP.action_code <> 'D'
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(URP.RISK_PART_ID, URP.RISK_PART_VER)) CP
        ON CP.PART_ID=URP.RISK_PART_ID
        AND CP.VERSION=URP.RISK_PART_VER
        WHERE UPC.PRODUCT_CONFIG_CODE IN('012102')
        AND UPC.POLICY_STATUS IN('A')
        AND UPV.VERSION_NO >1
        AND (UPV.ENDT_CODE  IN(select regexp_substr(V_AAN_ENDT_CODE_A,'[^,]+', 1, level) from dual
        connect by regexp_substr(V_AAN_ENDT_CODE_A, '[^,]+', 1, level) is not null ))
        AND UPV.ACTION_CODE IN('A','C')
        AND UPC.LOB='MS'
        AND PLPB.TPA_NAME='AAN'
        AND UPB.EXP_DATE >= P_START_DT;

        V_STEPS         VARCHAR2(10);
        V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_AAN_MS_POL_ENDT_ADHOC';
        FILENAME        UTL_FILE.FILE_TYPE;
        FILENAME1       VARCHAR2(1000);
        v_file_dir      VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_AAN_DIR');
        rowIDx          number := 5;
        seq             number := 1;
        V_RET           NUMBER := 0;

BEGIN
    V_STEPS := '001';
    FILENAME1   := 'Active Policy_MISC_' || TO_CHAR(P_START_DT, 'YYYYMMDD')||'.xlsx';

    PG_EXCEL_UTILS.clear_workbook;
    PG_EXCEL_UTILS.new_sheet;
    PG_EXCEL_UTILS.CELL(1,1,'BORDEREAUX (POLICY &'||' ENDORSEMENT)');

    PG_EXCEL_UTILS.MERGECELLS(1,1,3,1);
    PG_EXCEL_UTILS.CELL(1,2,'FROM : ALLIANZ GENERAL INSURANCE COMPANY (MALAYSIA) BERHAD');
    PG_EXCEL_UTILS.MERGECELLS(1,2,3,2);
    PG_EXCEL_UTILS.CELL(1,3,'DATE :');
    PG_EXCEL_UTILS.CELL(2,3,TO_CHAR(P_START_DT, 'DD/MM/YYYY'));

    PG_EXCEL_UTILS.SET_ROW(4
    ,p_fontId => PG_EXCEL_UTILS.get_font( 'Arial',p_bold => true));
    PG_EXCEL_UTILS.CELL(1,4,'No.');
    PG_EXCEL_UTILS.CELL(2,4,'Transaction Type');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH(2,30);
    PG_EXCEL_UTILS.CELL(3,4,'Product Code');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH(3,40);
    PG_EXCEL_UTILS.CELL(4,4,'Product Name');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH(4,20);
    PG_EXCEL_UTILS.CELL(5,4,'Name');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH(5,20);
    PG_EXCEL_UTILS.CELL(6,4,'ID Type');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH(6,20);
    PG_EXCEL_UTILS.CELL(7,4,'ID No.');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH(7,20);
    PG_EXCEL_UTILS.CELL(8,4,'Policy No.');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH(8,20);
    PG_EXCEL_UTILS.CELL(9,4,'Effective Date');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH(9,20);
    PG_EXCEL_UTILS.CELL(10,4,'Expiry Date');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH(10,20);
    PG_EXCEL_UTILS.CELL(11,4,'Text Decription');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH(11,20);

    FOR REC IN C_TPA_AAN_MS
    LOOP
        IF (REC.POLICY_VERSION =1 AND REC.ASST_FEE_AMT >0 AND REC.PRODUCT_CONFIG_CODE ='012104')
            OR (REC.POLICY_VERSION >1 AND REC.ASST_FEE_AMT >=0 AND REC.PRODUCT_CONFIG_CODE ='012104')
            OR (REC.POLICY_VERSION >1 AND REC.DMA_FEE_AMT >=0 AND REC.PRODUCT_CONFIG_CODE ='012102')
            OR (REC.POLICY_VERSION =1 AND REC.DMA_FEE_AMT >0 AND REC.PRODUCT_CONFIG_CODE ='012102')
        THEN
            PG_EXCEL_UTILS.CELL(1,rowIDx,seq);
            IF REC.POLICY_VERSION =1 THEN
                PG_EXCEL_UTILS.CELL(2,rowIDx,'PL');
            ELSE
                PG_EXCEL_UTILS.CELL(2,rowIDx,'EN');
            END IF;
            PG_EXCEL_UTILS.CELL(3,rowIDx,REC.PRODUCT_CONFIG_CODE);
            PG_EXCEL_UTILS.CELL(4,rowIDx,REC.PRODUCT_DESC);
            PG_EXCEL_UTILS.CELL(5,rowIDx,REC.NAME_EXT);
            PG_EXCEL_UTILS.CELL(6,rowIDx,REC.ID_TYPE);
            PG_EXCEL_UTILS.CELL(7,rowIDx,REC.ID_VALUE);
            IF REC.POLICY_VERSION =1 THEN
                PG_EXCEL_UTILS.CELL(8,rowIDx,REC.POLICY_REF);
            ELSE
                PG_EXCEL_UTILS.CELL(8,rowIDx,REC.ENDT_NO);
            END IF;
            PG_EXCEL_UTILS.CELL(9,rowIDx,REC.EFF_DATE);
            PG_EXCEL_UTILS.CELL(10,rowIDx,REC.EXP_DATE);
            PG_EXCEL_UTILS.CELL(11,rowIDx,REC.ENDT_NARR);

            rowIDx :=rowIDx+1;
            seq :=seq+1;
            --V_RET :=PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.POLICY_VERSION,'AAN'); --1.3
        END IF;
    END LOOP;

    PG_EXCEL_UTILS.save( v_file_dir, FILENAME1 );
EXCEPTION
    WHEN OTHERS
        THEN
            PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                V_PKG_NAME || V_FUNC_NAME,
                1,
                '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
END PC_TPA_AAN_MS_POL_ENDT_ADHOC;

PROCEDURE PC_TPA_AAN_HC_PA_POL_ENDT_ADHOC(P_START_DT IN UWGE_POLICY_BASES.EXP_DATE%TYPE,
                                          P_END_DT   IN UWGE_POLICY_BASES.EXP_DATE%TYPE) IS
    CURSOR C_TPA_AAN_PA
    IS
        SELECT TO_CHAR (UPV.ENDT_EFF_DATE, 'DD/MM/YYYY') AS ENDT_EFF_DATE,
        OPB.POLICY_REF,
        UPB.PREV_POL_NO,
        UPV.ENDT_CODE,
        UPV.VERSION_NO AS POLICY_VERSION,
        UPB.AGENT_CODE,
        DVA.NAME AS AGENT_NAME,
        (CASE
            WHEN upb.PREV_POL_NO IS NOT NULL THEN upb.PREV_POL_NO
            ELSE upb.PREV_POL_NO_IIMS
         END)
           AS prev_pol,
        (CASE
            WHEN CP.ID_VALUE1 IS NULL THEN CP.ID_VALUE2
            ELSE CP.ID_VALUE1
         END)
           AS P_NRIC_OTH,
        (CASE
            WHEN CP.ID_TYPE1 = 'NRIC' THEN CP.ID_VALUE1
            WHEN CP.ID_TYPE2 = 'NRIC' THEN CP.ID_VALUE2
         END)
           AS P_NRIC,
        REGEXP_REPLACE (
           (CASE
               WHEN     CP.MOBILE_NO1 IS NOT NULL
                    AND CP.MOBILE_CODE1 IS NOT NULL
               THEN
                  CP.MOBILE_CODE1 || CP.MOBILE_NO1
               ELSE
                  CP.MOBILE_CODE2 || CP.MOBILE_NO2
            END),
           '[^0-9]')
           AS PhoneNumber,
        REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
        REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,
        REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3,
        CPA.POSTCODE,
        (SELECT CODE_DESC
           FROM CMGE_CODE
          WHERE CAT_CODE = 'CITY' AND CODE_CD = CPA.CITY)
           AS CITY,
        (SELECT CODE_DESC
           FROM CMGE_CODE
          WHERE CAT_CODE = 'STATE' AND CODE_CD = CPA.STATE)
           AS STATE,
        UPC.PRODUCT_CONFIG_CODE,
        CP.NAME_EXT,
        UPB.LONG_NAME,
        TO_CHAR (UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,
        TO_CHAR (UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
        TO_CHAR (UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,
        (SELECT UPF.FEE_AMT
           FROM UWGE_POLICY_FEES UPF
          WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                AND UPF.VERSION_NO = UPV.VERSION_NO
                AND UPF.FEE_CODE = 'ASST')
           AS ASST_FEE_AMT,
        (SELECT UPF.FEE_AMT
           FROM UWGE_POLICY_FEES UPF
          WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                AND UPF.VERSION_NO = UPV.VERSION_NO
                AND UPF.FEE_CODE = 'MCO')
           AS MCO_FEE_AMT,
        (SELECT UPF.FEE_AMT
           FROM UWGE_POLICY_FEES UPF
          WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                AND UPF.VERSION_NO = UPV.VERSION_NO
                AND UPF.FEE_CODE = 'MCOO')
           AS MCOO_FEE_AMT,
        (SELECT UPF.FEE_AMT
           FROM UWGE_POLICY_FEES UPF
          WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                AND UPF.VERSION_NO = UPV.VERSION_NO
                AND UPF.FEE_CODE = 'MCOI')
           AS MCOI_FEE_AMT,
        (SELECT UPF.FEE_AMT
           FROM UWGE_POLICY_FEES UPF
          WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                AND UPF.VERSION_NO = UPV.VERSION_NO
                AND UPF.FEE_CODE = 'IMA')
           AS IMA_FEE_AMT,
        NVL (UPV.ENDT_NARR, ' ') AS ENDT_NARR,
        UPV.ENDT_NO,
        TO_CHAR (SYSDATE, 'DD/MM/YYYY') AS DateReceivedbyAAN,
        UPB.ISSUE_OFFICE,
        (SELECT BRANCH_NAME
           FROM CMDM_BRANCH
          WHERE BRANCH_CODE = UPB.ISSUE_OFFICE)
           AS BRANCH_DESC,
        (SELECT EXP_DATE
           FROM uwge_policy_bases
          WHERE     CONTRACT_ID =
                       (SELECT CONTRACT_ID
                          FROM OCP_POLICY_BASES
                         WHERE     policy_ref =
                                      (CASE
                                          WHEN upb.PREV_POL_NO
                                                  IS NOT NULL
                                          THEN
                                             upb.PREV_POL_NO
                                          ELSE
                                             upb.PREV_POL_NO_IIMS
                                       END)
                               AND ROWNUM = 1)
                AND uwge_policy_bases.TOP_INDICATOR = 'Y'
                AND ROWNUM = 1)
           AS prev_exp_date,
        OPB.CONTRACT_ID
        FROM UWGE_POLICY_VERSIONS UPV
        INNER JOIN
        UWGE_POLICY_CTRL_DLOAD UPCD
           ON     UPV.CONTRACT_ID = UPCD.CONTRACT_ID
              AND UPV.VERSION_NO = UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
           ON UPV.CONTRACT_ID = UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
           ON UPV.CONTRACT_ID = OPB.CONTRACT_ID
        INNER JOIN
        UWGE_POLICY_BASES UPB
           ON     UPB.CONTRACT_ID = UPV.CONTRACT_ID
              AND UPB.VERSION_NO = UPV.VERSION_NO
        INNER JOIN
        UWPL_POLICY_BASES PLPB
           ON     PLPB.CONTRACT_ID = UPV.CONTRACT_ID
              AND PLPB.VERSION_NO = UPV.VERSION_NO
        INNER JOIN
        TABLE (
           CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (UPB.CP_PART_ID,
                                                     UPB.CP_VERSION)) CP
           ON     CP.PART_ID = UPB.CP_PART_ID
              AND CP.VERSION = UPB.CP_VERSION
        INNER JOIN
        TABLE (
           CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (
              UPB.CP_ADDR_ID,
              UPB.CP_ADDR_VERSION)) CPA
           ON     CPA.ADD_ID = UPB.CP_ADDR_ID
              AND CPA.VERSION = UPB.CP_ADDR_VERSION
        INNER JOIN DMAG_VI_AGENT DVA ON DVA.AGENTCODE = UPB.AGENT_CODE
        WHERE     UPC.PRODUCT_CONFIG_CODE IN
        (    SELECT REGEXP_SUBSTR (
                       PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA',
                                                      'AAN_PA'),
                       '[^,]+',
                       1,
                       LEVEL)
               FROM DUAL
         CONNECT BY REGEXP_SUBSTR (
                       PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA',
                                                      'AAN_PA'),
                       '[^,]+',
                       1,
                       LEVEL)
                       IS NOT NULL)
        AND UPC.POLICY_STATUS IN ('A')
        AND PLPB.TPA_NAME = 'AAN'
        AND UPV.ACTION_CODE IN ('A', 'C')
        AND (   UPV.ENDT_CODE IS NULL
             OR UPV.ENDT_CODE IN
                   (    SELECT REGEXP_SUBSTR (V_AAN_ENDT_CODE,
                                              '[^,]+',
                                              1,
                                              LEVEL)
                          FROM DUAL
                    CONNECT BY REGEXP_SUBSTR (V_AAN_ENDT_CODE,
                                              '[^,]+',
                                              1,
                                              LEVEL)
                                  IS NOT NULL))
        AND UPCD.TPA_NAME = 'AAN'
        AND UPB.EXP_DATE BETWEEN P_START_DT AND P_END_DT
        ORDER BY OPB.policy_ref ASC, UPV.VERSION_NO ASC;

    CURSOR C_TPA_AAN_HC
    IS
        SELECT TO_CHAR (UPV.ENDT_EFF_DATE, 'DD/MM/YYYY') AS ENDT_EFF_DATE,
        OPB.POLICY_REF,
        UPB.PREV_POL_NO,
        UPV.ENDT_CODE,
        UPV.VERSION_NO AS POLICY_VERSION,
        UPB.AGENT_CODE,
        DVA.NAME AS AGENT_NAME,
        (CASE
            WHEN upb.PREV_POL_NO IS NOT NULL THEN upb.PREV_POL_NO
            ELSE upb.PREV_POL_NO_IIMS
         END)
           AS prev_pol,
        (CASE
            WHEN CP.ID_VALUE1 IS NULL THEN CP.ID_VALUE2
            ELSE CP.ID_VALUE1
         END)
           AS P_NRIC_OTH,
        (CASE
            WHEN CP.ID_TYPE1 = 'NRIC' THEN CP.ID_VALUE1
            WHEN CP.ID_TYPE2 = 'NRIC' THEN CP.ID_VALUE2
         END)
           AS P_NRIC,
        REGEXP_REPLACE (
           (CASE
               WHEN     CP.MOBILE_NO1 IS NOT NULL
                    AND CP.MOBILE_CODE1 IS NOT NULL
               THEN
                  CP.MOBILE_CODE1 || CP.MOBILE_NO1
               ELSE
                  CP.MOBILE_CODE2 || CP.MOBILE_NO2
            END),
           '[^0-9]')
           AS PhoneNumber,
        REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
        REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,
        REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3,
        CPA.POSTCODE,
        (SELECT CODE_DESC
           FROM CMGE_CODE
          WHERE CAT_CODE = 'CITY' AND CODE_CD = CPA.CITY)
           AS CITY,
        (SELECT CODE_DESC
           FROM CMGE_CODE
          WHERE CAT_CODE = 'STATE' AND CODE_CD = CPA.STATE)
           AS STATE,
        UPC.PRODUCT_CONFIG_CODE,
        CP.NAME_EXT,
        UPB.LONG_NAME,
        TO_CHAR (UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,
        TO_CHAR (UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
        TO_CHAR (UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,
        (SELECT UPF.FEE_AMT
           FROM UWGE_POLICY_FEES UPF
          WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                AND UPF.VERSION_NO = UPV.VERSION_NO
                AND UPF.FEE_CODE = 'MCOO')
           AS MCOO_FEE_AMT,
        (SELECT UPF.FEE_AMT
           FROM UWGE_POLICY_FEES UPF
          WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                AND UPF.VERSION_NO = UPV.VERSION_NO
                AND UPF.FEE_CODE = 'MCOI')
           AS MCOI_FEE_AMT,
        (SELECT UPF.FEE_AMT
           FROM UWGE_POLICY_FEES UPF
          WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                AND UPF.VERSION_NO = UPV.VERSION_NO
                AND UPF.FEE_CODE = 'MCO')
           AS MCO_FEE_AMT,
        (SELECT UPF.FEE_AMT
           FROM UWGE_POLICY_FEES UPF
          WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                AND UPF.VERSION_NO = UPV.VERSION_NO
                AND UPF.FEE_CODE = 'IMA')
           AS IMA_FEE_AMT,
        (SELECT UPF.FEE_AMT
           FROM UWGE_POLICY_FEES UPF
          WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
                AND UPF.VERSION_NO = UPV.VERSION_NO
                AND UPF.FEE_CODE = 'MCODMA')
           AS DMA_FEE_AMT,
        NVL (UPV.ENDT_NARR, ' ') AS ENDT_NARR,
        UPV.ENDT_NO,
        TO_CHAR (SYSDATE, 'DD/MM/YYYY') AS DateReceivedbyAAN,
        (SELECT BRANCH_NAME
           FROM CMDM_BRANCH
          WHERE BRANCH_CODE = UPB.ISSUE_OFFICE)
           AS BRANCH_DESC,
        (SELECT EXP_DATE
           FROM uwge_policy_bases
          WHERE     CONTRACT_ID =
                       (SELECT CONTRACT_ID
                          FROM OCP_POLICY_BASES
                         WHERE     policy_ref =
                                      (CASE
                                          WHEN upb.PREV_POL_NO
                                                  IS NOT NULL
                                          THEN
                                             upb.PREV_POL_NO
                                          ELSE
                                             upb.PREV_POL_NO_IIMS
                                       END)
                               AND ROWNUM = 1)
                AND uwge_policy_bases.TOP_INDICATOR = 'Y'
                AND ROWNUM = 1)
           AS prev_exp_date,
        OPB.CONTRACT_ID
        FROM UWGE_POLICY_VERSIONS UPV
        INNER JOIN
        UWGE_POLICY_CTRL_DLOAD UPCD
           ON     UPV.CONTRACT_ID = UPCD.CONTRACT_ID
              AND UPV.VERSION_NO = UPCD.VERSION_NO
        INNER JOIN UWGE_POLICY_CONTRACTS UPC
           ON UPV.CONTRACT_ID = UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
           ON UPV.CONTRACT_ID = OPB.CONTRACT_ID
        INNER JOIN
        UWGE_POLICY_BASES UPB
           ON     UPB.CONTRACT_ID = UPV.CONTRACT_ID
              AND UPB.VERSION_NO = UPV.VERSION_NO
        INNER JOIN
        UWPL_POLICY_BASES PLPB
           ON     PLPB.CONTRACT_ID = UPV.CONTRACT_ID
              AND PLPB.VERSION_NO = UPV.VERSION_NO
        INNER JOIN
        TABLE (
           CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (UPB.CP_PART_ID,
                                                     UPB.CP_VERSION)) CP
           ON     CP.PART_ID = UPB.CP_PART_ID
              AND CP.VERSION = UPB.CP_VERSION
        INNER JOIN
        TABLE (
           CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (
              UPB.CP_ADDR_ID,
              UPB.CP_ADDR_VERSION)) CPA
           ON     CPA.ADD_ID = UPB.CP_ADDR_ID
              AND CPA.VERSION = UPB.CP_ADDR_VERSION
        INNER JOIN DMAG_VI_AGENT DVA ON DVA.AGENTCODE = UPB.AGENT_CODE
        WHERE     UPC.PRODUCT_CONFIG_CODE IN
        (    SELECT REGEXP_SUBSTR (
                       PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA',
                                                      'AAN_HC'),
                       '[^,]+',
                       1,
                       LEVEL)
               FROM DUAL
         CONNECT BY REGEXP_SUBSTR (
                       PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA',
                                                      'AAN_HC'),
                       '[^,]+',
                       1,
                       LEVEL)
                       IS NOT NULL)
        AND UPC.POLICY_STATUS IN ('A')
        AND UPV.ACTION_CODE IN ('A', 'C')
        AND (   UPV.ENDT_CODE IS NULL
             OR UPV.ENDT_CODE IN
                   (    SELECT REGEXP_SUBSTR (V_AAN_ENDT_CODE,
                                              '[^,]+',
                                              1,
                                              LEVEL)
                          FROM DUAL
                    CONNECT BY REGEXP_SUBSTR (V_AAN_ENDT_CODE,
                                              '[^,]+',
                                              1,
                                              LEVEL)
                                  IS NOT NULL))
        AND (PLPB.TPA_NAME = 'AAN' OR PLPB.TPA_NAME IS NULL)
        AND UPCD.TPA_NAME = 'AAN'
        AND UPB.EXP_DATE BETWEEN P_START_DT AND P_END_DT
        ORDER BY OPB.policy_ref ASC, UPV.VERSION_NO ASC;

    V_STEPS                   VARCHAR2 (10);
    V_FUNC_NAME               VARCHAR2 (100) := 'PC_TPA_AAN_HC_PA_POL_ENDT_ADHOC';
    FILENAME                  UTL_FILE.FILE_TYPE;
    FILENAME1                 VARCHAR2 (1000);
    v_file_dir                VARCHAR2 (100)
       := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'TPA_AAN_DIR');
    V_ASST                    VARCHAR2 (100)
                                 := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'PA_ASST');
    V_IMA                     VARCHAR2 (100)
                                 := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'PA_IMA');
    V_MCO                     VARCHAR2 (100)
                                 := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'PA_MCO');
    V_NPOL                    VARCHAR2 (100)
                                 := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'POL_PA');
    V_HC_MCOI                 VARCHAR2 (100)
                                 := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'HC_MCOI');
    V_HC_MCOO                 VARCHAR2 (100)
                                 := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'HC_MCOO');
    V_HC_DMA                  VARCHAR2 (100)
                                 := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'HC_DMA');

    V_RISK_LEVEL_DTLS         VARCHAR2 (100)
       := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'RISK_LEVEL_DTLS');
    V_IMA_LMT_2M              VARCHAR2 (100)
       := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'IMA_LMT_2M');

    V_PRINCIPAL_DET           PG_TPA_UTILS.RISK_PERSON_PARTNERS_ALL_DET;
    V_UWPL_COVER_DET          PG_TPA_UTILS.UWPL_COVER_DET;
    rowIDx                    NUMBER := 5;
    seq                       NUMBER := 1;
    v_NGV                     NUMBER (18, 2);
    V_RET                     NUMBER := 0;

    V_SELECTED_RISK_SQL       VARCHAR2 (10000)
       := 'SELECT (CASE
                      WHEN RCP.ID_VALUE1 IS NULL THEN RCP.ID_VALUE2
                      ELSE RCP.ID_VALUE1
                   END)
                     AS NRIC_OTH,
                  (CASE
                      WHEN RCP.ID_TYPE1 = ''NRIC'' THEN RCP.ID_VALUE1
                      WHEN RCP.ID_TYPE2 = ''NRIC'' THEN RCP.ID_VALUE2
                   END)
                     AS NRIC,
                  RCP.NAME_EXT AS MEMBER_FULL_NAME,
                  RCP.DATE_OF_BIRTH AS DATE_OF_BIRTH,
                  RCP.SEX,
                  (CASE
                      WHEN RCP.marital_status = ''0'' THEN ''S''
                      WHEN RCP.marital_status = ''1'' THEN ''M''
                      WHEN RCP.marital_status = ''2'' THEN ''D''
                   END)
                     AS MARITAL_STATUS,
                  URP.INSURED_TYPE,
                  URP.EMPLOYEE_ID,
                  (CASE
                      WHEN URP.INSURED_TYPE = ''P''
                      THEN
                         ''P''
                      ELSE
                         (CASE
                             WHEN URP.RELATIONSHIP IN (''03'', ''072'') THEN ''H''
                             WHEN URP.RELATIONSHIP IN (''02'', ''107'') THEN ''W''
                             WHEN URP.RELATIONSHIP IN (''05'', ''019'') THEN ''D''
                             WHEN URP.RELATIONSHIP IN (''04'', ''087'') THEN ''S''
                             ELSE ''''
                          END)
                   END)
                     AS RELATIONSHIP,
                  URP.TEMINATE_DATE,
                  UR.EFF_DATE AS RISK_EFF_DATE,
                  UR.EXP_DATE AS RISK_EXP_DATE,
                  URP.JOIN_DATE AS ORIGINAL_JOIN_DATE,
                  (CASE
                      WHEN URP.INSURED_TYPE = ''D''
                      THEN
                         (SELECT a.COV_SEQ_REF
                            FROM uwge_cover a
                           WHERE     UCOV.CONTRACT_ID = A.CONTRACT_ID
                                 AND UCOV.VERSION_NO = a.VERSION_NO
                                 AND a.RISK_ID = UR.RISK_PARENT_ID
                                 AND COV_PARENT_ID IS NULL
                                 AND ROWNUM = 1)
                      ELSE
                         ''''
                   END)
                     AS Parent_cov_seq_no,
                  UCOV.COV_ID,
                  UCOV.COV_SEQ_REF,
                  UR.RISK_ID,
                  UR.RISK_PARENT_ID,
                  (SELECT COUNT (*)
                     FROM UWGE_COVER CSUB
                    WHERE     UCOV.CONTRACT_ID = CSUB.CONTRACT_ID
                          AND UCOV.VERSION_NO = CSUB.VERSION_NO
                          AND UCOV.COV_ID = CSUB.COV_PARENT_ID
                          AND CSUB.COV_CODE IN (''OP'', ''OP1'', ''OP2''))
                     AS OP_SUB_COV,
                  (SELECT NVL (F.FEE_AMT, 0)
                     FROM UWGE_COVER_FEES F
                    WHERE     F.CONTRACT_ID = UR.CONTRACT_ID
                          AND F.COV_ID = UCOV.COV_ID
                          AND TOP_INDICATOR = ''Y''
                          AND F.FEE_CODE = ''MCO'')
                     AS MCO_FEE,
                  (SELECT NVL (F.FEE_AMT, 0)
                     FROM UWGE_COVER_FEES F
                    WHERE     F.CONTRACT_ID = UR.CONTRACT_ID
                          AND F.COV_ID =
                                 (SELECT CV.COV_ID
                                    FROM UWGE_COVER CV
                                   WHERE     CV.COV_PARENT_ID = UCOV.COV_ID
                                         AND TOP_INDICATOR = ''Y''
                                         AND COV_CODE = ''IMA''
                                         AND ROWNUM = 1)
                          AND TOP_INDICATOR = ''Y''
                          AND F.FEE_CODE = ''IMA''
                          AND ROWNUM = 1)
                     AS IMA_FEE,
                  null as import_type,
                  null as prev_pol_op_ind,
                  urp.department
             FROM UWGE_RISK UR
                  INNER JOIN
                  UWPL_RISK_PERSON URP
                     ON     URP.CONTRACT_ID = UR.CONTRACT_ID
                        AND UR.RISK_ID = URP.RISK_ID
                        AND URP.VERSION_NO = UR.VERSION_NO
                        AND URP.action_code <> ''D''
                  INNER JOIN
                  TABLE (
                     CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (URP.RISK_PART_ID,
                                                               URP.RISK_PART_VER)) RCP
                     ON     RCP.PART_ID = URP.RISK_PART_ID
                        AND RCP.VERSION = URP.RISK_PART_VER
                  INNER JOIN
                  UWGE_COVER UCOV
                     ON     UCOV.CONTRACT_ID = UR.CONTRACT_ID
                        AND UR.RISK_ID = UCOV.RISK_ID
                        AND UCOV.VERSION_NO = UR.VERSION_NO
                        AND UCOV.COV_PARENT_ID IS NULL
            WHERE     UR.CONTRACT_ID = :BIND_CONTRACT_ID
                  AND UR.version_no = :BIND_VERSION_NO
                  AND UR.action_code <> ''D''
                                AND UR.EXP_DATE >= ''01-MAR-25''
         ORDER BY urp.department, TO_NUMBER (UCOV.cov_seq_ref)';

    V_ALL_RISK_SQL            VARCHAR2 (10000)
       := 'SELECT (CASE
                  WHEN RCP.ID_VALUE1 IS NULL THEN RCP.ID_VALUE2
                  ELSE RCP.ID_VALUE1
               END)
                 AS NRIC_OTH,
              (CASE
                  WHEN RCP.ID_TYPE1 = ''NRIC'' THEN RCP.ID_VALUE1
                  WHEN RCP.ID_TYPE2 = ''NRIC'' THEN RCP.ID_VALUE2
               END)
                 AS NRIC,
              RCP.NAME_EXT AS MEMBER_FULL_NAME,
              RCP.DATE_OF_BIRTH AS DATE_OF_BIRTH,
              RCP.SEX,
              (CASE
                  WHEN RCP.marital_status = ''0'' THEN ''S''
                  WHEN RCP.marital_status = ''1'' THEN ''M''
                  WHEN RCP.marital_status = ''2'' THEN ''D''
               END)
                 AS MARITAL_STATUS,
              URP.INSURED_TYPE,
              URP.EMPLOYEE_ID,
              (CASE
                  WHEN URP.INSURED_TYPE = ''P''
                  THEN
                     ''P''
                  ELSE
                     (CASE
                         WHEN URP.RELATIONSHIP IN (''03'', ''072'') THEN ''H''
                         WHEN URP.RELATIONSHIP IN (''02'', ''107'') THEN ''W''
                         WHEN URP.RELATIONSHIP IN (''05'', ''019'') THEN ''D''
                         WHEN URP.RELATIONSHIP IN (''04'', ''087'') THEN ''S''
                         ELSE ''''
                      END)
               END)
                 AS RELATIONSHIP,
              URP.TEMINATE_DATE,
              UR.EFF_DATE AS RISK_EFF_DATE,
              UR.EXP_DATE AS RISK_EXP_DATE,
              URP.JOIN_DATE AS ORIGINAL_JOIN_DATE,
              (CASE
                  WHEN URP.INSURED_TYPE = ''D''
                  THEN
                     (SELECT a.COV_SEQ_REF
                        FROM uwge_cover a
                       WHERE     UCOV.CONTRACT_ID = A.CONTRACT_ID
                             AND UCOV.VERSION_NO = a.VERSION_NO
                             AND a.RISK_ID = UR.RISK_PARENT_ID
                             AND COV_PARENT_ID IS NULL
                             AND ROWNUM = 1)
                  ELSE
                     ''''
               END)
                 AS Parent_cov_seq_no,
              UCOV.COV_ID,
              UCOV.COV_SEQ_REF,
              UR.RISK_ID,
              UR.RISK_PARENT_ID,
              (SELECT COUNT (*)
                 FROM UWGE_COVER CSUB
                WHERE     UCOV.CONTRACT_ID = CSUB.CONTRACT_ID
                      AND UCOV.VERSION_NO = CSUB.VERSION_NO
                      AND UCOV.COV_ID = CSUB.COV_PARENT_ID
                      AND CSUB.COV_CODE IN (''OP'', ''OP1'', ''OP2''))
                 AS OP_SUB_COV,
              (SELECT NVL (F.FEE_AMT, 0)
                 FROM UWGE_COVER_FEES F
                WHERE     F.CONTRACT_ID = UR.CONTRACT_ID
                      AND F.COV_ID = UCOV.COV_ID
                      AND TOP_INDICATOR = ''Y''
                      AND F.FEE_CODE = ''MCO'')
                 AS MCO_FEE,
              (SELECT NVL (F.FEE_AMT, 0)
                 FROM UWGE_COVER_FEES F
                WHERE     F.CONTRACT_ID = UR.CONTRACT_ID
                      AND F.COV_ID =
                             (SELECT CV.COV_ID
                                FROM UWGE_COVER CV
                               WHERE     CV.COV_PARENT_ID = UCOV.COV_ID
                                     AND TOP_INDICATOR = ''Y''
                                     AND COV_CODE = ''IMA''
                                     AND ROWNUM = 1)
                      AND TOP_INDICATOR = ''Y''
                      AND F.FEE_CODE = ''IMA''
                      AND ROWNUM = 1)
                 AS IMA_FEE,
              NULL AS import_type,
              NULL AS prev_pol_op_ind,
              urp.department
         FROM UWGE_RISK UR
              INNER JOIN
              UWPL_RISK_PERSON URP
                 ON     URP.CONTRACT_ID = UR.CONTRACT_ID
                    AND UR.RISK_ID = URP.RISK_ID
                    AND URP.VERSION_NO =
                           (SELECT MAX (b.version_no)
                              FROM UWPL_RISK_PERSON b
                             WHERE     b.contract_id = UR.CONTRACT_ID
                                   AND URP.object_id = b.object_id
                                   AND b.version_no <= :BIND_VERSION_NO
                                   AND b.reversing_version IS NULL)
                    AND URP.action_code <> ''D''
              INNER JOIN
              TABLE (
                 CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (URP.RISK_PART_ID,
                                                           URP.RISK_PART_VER)) RCP
                 ON     RCP.PART_ID = URP.RISK_PART_ID
                    AND RCP.VERSION = URP.RISK_PART_VER
              INNER JOIN
              UWGE_COVER UCOV
                 ON     UCOV.CONTRACT_ID = UR.CONTRACT_ID
                    AND UR.RISK_ID = UCOV.RISK_ID
                    AND UCOV.VERSION_NO = :BIND_VERSION_NO_1
                    AND UCOV.COV_PARENT_ID IS NULL
        WHERE     UR.CONTRACT_ID = :BIND_CONTRACT_ID
              AND UR.version_no =
                     (SELECT MAX (c.version_no)
                        FROM UWGE_RISK c
                       WHERE     c.contract_id = :BIND_CONTRACT_ID_1
                             AND UR.object_id = c.object_id
                             AND c.version_no <= :BIND_VERSION_NO_2
                             AND c.reversing_version IS NULL)
              AND UR.action_code <> ''D''
                        AND UR.EXP_DATE >= ''01-MAR-25''
     ORDER BY urp.department, TO_NUMBER (UCOV.cov_seq_ref)';

    V_SELECTED_RISK_SQL_TPA   VARCHAR2 (10000)
       := 'SELECT (CASE
                WHEN rcp.id_value1 IS NULL THEN rcp.id_value2
                ELSE rcp.id_value1
             END)
               AS nric_oth,
            (CASE
                WHEN rcp.id_type1 = ''NRIC'' THEN rcp.id_value1
                WHEN rcp.id_type2 = ''NRIC'' THEN rcp.id_value2
             END)
               AS nric,
            rcp.name_ext AS member_full_name,
            rcp.date_of_birth AS date_of_birth,
            rcp.sex,
            (CASE
                WHEN rcp.marital_status = ''0'' THEN ''S''
                WHEN rcp.marital_status = ''1'' THEN ''M''
                WHEN rcp.marital_status = ''2'' THEN ''D''
             END)
               AS marital_status,
            urp.insured_type,
            urp.employee_id,
            (CASE
                WHEN urp.insured_type = ''P''
                THEN
                   ''P''
                ELSE
                   (CASE
                       WHEN urp.relationship IN (''03'', ''072'') THEN ''H''
                       WHEN urp.relationship IN (''02'', ''107'') THEN ''W''
                       WHEN urp.relationship IN (''05'', ''019'') THEN ''D''
                       WHEN urp.relationship IN (''04'', ''087'') THEN ''S''
                       ELSE ''''
                    END)
             END)
               AS relationship,
            urp.teminate_date,
            ur.eff_date AS risk_eff_date,
            ur.exp_date AS risk_exp_date,
            urp.join_date AS original_join_date,
            (CASE
                WHEN urp.insured_type = ''D''
                THEN
                   (SELECT a.cov_seq_ref
                      FROM uwge_cover a
                     WHERE     ucov.contract_id = a.contract_id
                           AND ucov.version_no = a.version_no
                           AND a.risk_id = ur.risk_parent_id
                           AND cov_parent_id IS NULL
                           AND ROWNUM = 1)
                ELSE
                   ''''
             END)
               AS parent_cov_seq_no,
            ucov.cov_id,
            ucov.cov_seq_ref,
            ur.risk_id,
            ur.risk_parent_id,
            (SELECT COUNT (*)
               FROM uwge_cover csub
              WHERE     ucov.contract_id = csub.contract_id
                    AND ucov.version_no = csub.version_no
                    AND ucov.cov_id = csub.cov_parent_id
                    AND csub.cov_code IN (''OP'', ''OP1'', ''OP2''))
               AS op_sub_cov,
            (SELECT NVL (f.fee_amt, 0)
               FROM uwge_cover_fees f
              WHERE     f.contract_id = ur.contract_id
                    AND f.cov_id = ucov.cov_id
                    AND top_indicator = ''Y''
                    AND f.fee_code = ''MCO'')
               AS mco_fee,
            (SELECT NVL (f.fee_amt, 0)
               FROM uwge_cover_fees f
              WHERE     f.contract_id = ur.contract_id
                    AND f.cov_id =
                           (SELECT CV.cov_id
                              FROM uwge_cover CV
                             WHERE     CV.cov_parent_id = ucov.cov_id
                                   AND top_indicator = ''Y''
                                   AND cov_code = ''IMA''
                                   AND ROWNUM = 1)
                    AND top_indicator = ''Y''
                    AND f.fee_code = ''IMA''
                    AND ROWNUM = 1)
               AS ima_fee,
            tpa.import_type,
            tpa.prev_pol_op_ind,
            urp.department
       FROM uwge_risk_tpa_download tpa
            INNER JOIN
            uwge_cover ucov
               ON     ucov.contract_id = tpa.contract_id
                  AND ucov.risk_id = tpa.risk_id
                  AND ucov.version_no = tpa.version_no
                  AND ucov.cov_parent_id IS NULL
            LEFT OUTER JOIN
            uwge_risk ur
               ON     ur.contract_id = tpa.contract_id
                  AND ur.risk_id = tpa.risk_id
                  AND ur.action_code <> ''D''
                  AND ur.version_no =
                         (SELECT MAX (version_no)
                            FROM uwge_risk ur2
                           WHERE     ur2.contract_id = ur.contract_id
                                 AND ur2.object_id = ur.object_id
                                 AND ur2.version_no <= tpa.version_no
                                 AND ur2.reversing_version IS NULL)
            LEFT OUTER JOIN
            uwpl_risk_person urp
               ON     urp.contract_id = tpa.contract_id
                  AND urp.risk_id = tpa.risk_id
                  AND urp.action_code <> ''D''
                  AND urp.version_no =
                         (SELECT MAX (version_no)
                            FROM uwpl_risk_person urp2
                           WHERE     urp2.contract_id = urp.contract_id
                                 AND urp2.object_id = urp.object_id
                                 AND urp2.version_no <= tpa.version_no
                                 AND urp2.reversing_version IS NULL)
            INNER JOIN
            TABLE (
               customer.pg_cp_gen_table.fn_gen_cp_table (urp.risk_part_id,
                                                         urp.risk_part_ver)) rcp
               ON     rcp.part_id = urp.risk_part_id
                  AND rcp.version = urp.risk_part_ver
      WHERE     tpa.contract_id = :bind_contract_id
            AND tpa.version_no = :bind_version_no
            AND tpa.tpa_name = ''AAN''
                    AND ur.EXP_DATE >= ''01-MAR-25''
       ORDER BY urp.department, TO_NUMBER (ucov.cov_seq_ref)';

    RISK_DET                  PG_TPA_UTILS.AAN_PA_HC_RISK_DET_TBL;
    V_ROW_NUM                 NUMBER (5);
    V_ENDT_NARR_ARRAY         PG_TPA_UTILS.p_array_v;
    V_COUNT_TPA_RISK          NUMBER (5);
BEGIN
    --        --dbms_output.put_line (
    --                  'P_START_DT :  ' || P_START_DT);
    V_STEPS := '001';
    FILENAME1 := 'Active Policy_HC'|| CHR (38)||'PA_'||TO_CHAR (P_START_DT, 'YYYYMMDD')||'.xlsx';
    V_STEPS := '002';
    PG_EXCEL_UTILS.clear_workbook;
    PG_EXCEL_UTILS.new_sheet;
    PG_EXCEL_UTILS.CELL (1, 1, 'BORDEREAUX (POLICY &' || ' ENDORSEMENT)');
    V_STEPS := '003';
    PG_EXCEL_UTILS.MERGECELLS (1,
                               1,
                               3,
                               1);
    PG_EXCEL_UTILS.CELL (
       1,
       2,
       'FROM : ALLIANZ GENERAL INSURANCE COMPANY (MALAYSIA) BERHAD');
    PG_EXCEL_UTILS.MERGECELLS (1,
                               2,
                               3,
                               2);
    PG_EXCEL_UTILS.CELL (1, 3, 'DATE :');
    PG_EXCEL_UTILS.CELL (2, 3, TO_CHAR (P_START_DT, 'DD/MM/YYYY'));
    V_STEPS := '004';
    PG_EXCEL_UTILS.SET_ROW (
       4,
       p_fontId   => PG_EXCEL_UTILS.get_font ('Arial', p_bold => TRUE));
    PG_EXCEL_UTILS.CELL (1, 4, 'No.');
    PG_EXCEL_UTILS.CELL (2, 4, 'Import Type');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (2, 20);
    PG_EXCEL_UTILS.CELL (3, 4, 'Member Full Name');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (3, 40);
    PG_EXCEL_UTILS.CELL (4, 4, 'Address 1');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (4, 20);
    PG_EXCEL_UTILS.CELL (5, 4, 'Address 2');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (5, 20);
    PG_EXCEL_UTILS.CELL (6, 4, 'Address 3');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (6, 40);
    PG_EXCEL_UTILS.CELL (7, 4, 'Address 4');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (7, 20);
    PG_EXCEL_UTILS.CELL (8, 4, 'Gender');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (8, 20);
    V_STEPS := '005';
    PG_EXCEL_UTILS.CELL (9, 4, 'DOB');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (9, 20);
    PG_EXCEL_UTILS.CELL (10, 4, 'NRIC');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (10, 20);
    PG_EXCEL_UTILS.CELL (11, 4, 'Other IC');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (11, 20);
    PG_EXCEL_UTILS.CELL (12, 4, 'External Ref Id (aka Client)');
    V_STEPS := '006';
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (12, 20);
    PG_EXCEL_UTILS.CELL (13, 4, 'Internal Ref Id (aka AAN)');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (13, 20);
    PG_EXCEL_UTILS.CELL (14, 4, 'Employee ID');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (14, 20);
    PG_EXCEL_UTILS.CELL (15, 4, 'Marital Status');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (15, 20);
    PG_EXCEL_UTILS.CELL (16, 4, 'Race');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (16, 20);
    PG_EXCEL_UTILS.CELL (17, 4, 'Phone');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (17, 20);
    PG_EXCEL_UTILS.CELL (18, 4, 'VIP');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (18, 20);
    PG_EXCEL_UTILS.CELL (19, 4, 'Special Condition');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (19, 20);
    PG_EXCEL_UTILS.CELL (20, 4, 'Relationship');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (20, 20);
    PG_EXCEL_UTILS.CELL (21, 4, 'Principal Int Ref Id (aka AAN)');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (21, 20);
    PG_EXCEL_UTILS.CELL (22, 4, 'Principal Ext Ref Id (aka Client)');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (22, 20);
    PG_EXCEL_UTILS.CELL (23, 4, 'Principal Name');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (23, 20);
    PG_EXCEL_UTILS.CELL (24, 4, 'Principal NRIC');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (24, 20);
    PG_EXCEL_UTILS.CELL (25, 4, 'Principal Other Ic');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (25, 20);
    PG_EXCEL_UTILS.CELL (26, 4, 'Program Id');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (26, 20);
    PG_EXCEL_UTILS.CELL (27, 4, 'Policy Type');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (27, 20);
    PG_EXCEL_UTILS.CELL (28, 4, 'Policy Num');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (28, 20);
    PG_EXCEL_UTILS.CELL (29, 4, 'Policy Eff Date');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (29, 20);
    PG_EXCEL_UTILS.CELL (30, 4, 'Policy Expiry Date');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (30, 20);
    PG_EXCEL_UTILS.CELL (31, 4, 'Previous Policy Num');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (31, 20);
    PG_EXCEL_UTILS.CELL (32, 4, 'Previous Policy End Date');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (32, 20);
    PG_EXCEL_UTILS.CELL (33, 4, 'Customer Owner Name');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (33, 20);
    PG_EXCEL_UTILS.CELL (34, 4, 'External Plan Code');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (34, 20);
    PG_EXCEL_UTILS.CELL (35, 4, 'Internal Plan Code Id');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (35, 20);
    PG_EXCEL_UTILS.CELL (36, 4, 'Original Join Date');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (36, 20);
    PG_EXCEL_UTILS.CELL (37, 4, 'Plan Attach Date');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (37, 20);
    PG_EXCEL_UTILS.CELL (38, 4, 'Plan Expiry Date');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (38, 20);
    PG_EXCEL_UTILS.CELL (39, 4, 'Subsidiary Name');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (39, 20);
    PG_EXCEL_UTILS.CELL (40, 4, 'Agent Name');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (40, 20);
    PG_EXCEL_UTILS.CELL (41, 4, 'Agent Code');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (41, 20);
    PG_EXCEL_UTILS.CELL (42, 4, 'Insurer MCO Fees');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (42, 20);
    PG_EXCEL_UTILS.CELL (43, 4, 'IMA Service?');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (43, 20);
    PG_EXCEL_UTILS.CELL (44, 4, 'IMA Limit');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (44, 20);
    PG_EXCEL_UTILS.CELL (45, 4, 'Date Received by AAN');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (45, 20);
    PG_EXCEL_UTILS.CELL (46, 4, 'Termination Date');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (46, 20);
    PG_EXCEL_UTILS.CELL (47, 4, 'Free text remark');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (47, 20);
    PG_EXCEL_UTILS.CELL (48, 4, 'Questionnaire');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (48, 20);
    PG_EXCEL_UTILS.CELL (49, 4, 'Plan-Remarks');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (49, 20);
    PG_EXCEL_UTILS.CELL (50, 4, 'Diagnosis');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (50, 20);
    PG_EXCEL_UTILS.CELL (51, 4, 'Outpatient Subcover');         
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (51, 20);                   
    PG_EXCEL_UTILS.CELL (52, 4, 'Previous Outpatient Subcover');
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (52, 20);                   
    PG_EXCEL_UTILS.CELL (53, 4, 'Department');                  
    PG_EXCEL_UTILS.SET_COLUMN_WIDTH (53, 20);                   
    DBMS_OUTPUT.ENABLE (buffer_size => NULL);

    FOR REC IN C_TPA_AAN_PA
    LOOP
        V_STEPS := '007AA_01';

        IF (   (    REC.POLICY_VERSION = 1
                AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                       V_NPOL,
                       REC.PRODUCT_CONFIG_CODE) = 'N')
            OR REC.POLICY_VERSION > 1)
        THEN
            IF    (    REC.POLICY_VERSION = 1
                   AND REC.ASST_FEE_AMT > 0
                   AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                          V_ASST,
                          REC.PRODUCT_CONFIG_CODE) = 'Y')
               OR (    REC.POLICY_VERSION > 1
                   AND REC.ASST_FEE_AMT >= 0
                   AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                          V_ASST,
                          REC.PRODUCT_CONFIG_CODE) = 'Y')
               OR (    REC.POLICY_VERSION = 1
                   AND REC.IMA_FEE_AMT > 0
                   AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                          V_IMA,
                          REC.PRODUCT_CONFIG_CODE) = 'Y')
               OR (    REC.POLICY_VERSION > 1
                   AND REC.IMA_FEE_AMT >= 0
                   AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                          V_IMA,
                          REC.PRODUCT_CONFIG_CODE) = 'Y')
               OR (    REC.POLICY_VERSION = 1
                   AND REC.MCO_FEE_AMT > 0
                   AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                          V_MCO,
                          REC.PRODUCT_CONFIG_CODE) = 'Y')
               OR (    REC.POLICY_VERSION > 1
                   AND REC.MCO_FEE_AMT >= 0
                   AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                          V_MCO,
                          REC.PRODUCT_CONFIG_CODE) = 'Y')
               OR (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_RISK_LEVEL_DTLS,
                                                       REC.PRODUCT_CONFIG_CODE) =
                      'Y')
            THEN
                    RISK_DET.DELETE;

                    SELECT COUNT (1)
                        INTO V_COUNT_TPA_RISK
                        FROM UWGE_RISK_TPA_DOWNLOAD TPA
                     WHERE     TPA.CONTRACT_ID = REC.CONTRACT_ID
                                 AND TPA.VERSION_NO = REC.POLICY_VERSION;

                    IF V_COUNT_TPA_RISK > 0
                    THEN
                         BEGIN
                                EXECUTE IMMEDIATE V_SELECTED_RISK_SQL_TPA
                                     BULK COLLECT INTO RISK_DET
                                     USING REC.CONTRACT_ID, REC.POLICY_VERSION;
                         END;
                    ELSE
                         IF     REC.ENDT_CODE IS NOT NULL
                                AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_AAN_ENDT_CODE_R,
                                                                                                                REC.ENDT_CODE) =
                                             'Y'
                         THEN
                                BEGIN
                                     EXECUTE IMMEDIATE V_SELECTED_RISK_SQL
                                            BULK COLLECT INTO RISK_DET
                                            USING REC.CONTRACT_ID, REC.POLICY_VERSION;
                                END;
                         ELSE
                                BEGIN
                                     EXECUTE IMMEDIATE V_ALL_RISK_SQL
                                            BULK COLLECT INTO RISK_DET
                                            USING REC.POLICY_VERSION,
                                                        REC.POLICY_VERSION,
                                                        REC.CONTRACT_ID,
                                                        REC.CONTRACT_ID,
                                                        REC.POLICY_VERSION;
                                END;
                         END IF;
                    END IF;

                    V_ROW_NUM := 0;

                    FOR V_ROW_NUM IN 1 .. RISK_DET.COUNT
                    LOOP
                         IF (   PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                                             V_RISK_LEVEL_DTLS,
                                             REC.PRODUCT_CONFIG_CODE) = 'N'
                                 OR (    PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                                                        V_RISK_LEVEL_DTLS,
                                                        REC.PRODUCT_CONFIG_CODE) = 'Y'
                                         AND (   NVL (RISK_DET (V_ROW_NUM).IMA_FEE, 0) > 0
                                                    OR NVL (RISK_DET (V_ROW_NUM).MCO_FEE, 0) > 0)
                                         AND REC.POLICY_VERSION = 1)
                                 OR (    PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                                                        V_RISK_LEVEL_DTLS,
                                                        REC.PRODUCT_CONFIG_CODE) = 'Y'
                                         AND REC.POLICY_VERSION > 1))
                         THEN

                                PG_EXCEL_UTILS.CELL (1, rowIDx, seq);

                                IF RISK_DET (V_ROW_NUM).IMPORT_TYPE IS NOT NULL
                                THEN
                                     PG_EXCEL_UTILS.CELL (2,
                                                                                rowIDx,
                                                                                RISK_DET (V_ROW_NUM).IMPORT_TYPE);
                                ELSE
                                     IF     REC.POLICY_VERSION = 1
                                            AND REC.PREV_POL_NO IS NOT NULL
                                     THEN
                                            PG_EXCEL_UTILS.CELL (2, rowIDx, 'R');
                                     ELSIF REC.POLICY_VERSION = 1 AND REC.PREV_POL_NO IS NULL
                                     THEN
                                            PG_EXCEL_UTILS.CELL (2, rowIDx, 'N');
                                     ELSIF     REC.POLICY_VERSION > 1
                                                 AND REC.ENDT_CODE IN ('96', '97')
                                     THEN
                                            PG_EXCEL_UTILS.CELL (2, rowIDx, 'X');
                                     ELSE
                                            PG_EXCEL_UTILS.CELL (2, rowIDx, 'E');
                                     END IF;
                                END IF;

                                V_STEPS := '007A';
                                PG_EXCEL_UTILS.CELL (
                                     3,
                                     rowIDx,
                                     NVL (RISK_DET (V_ROW_NUM).MEMBER_FULL_NAME, ' '));
                                PG_EXCEL_UTILS.CELL (
                                     4,
                                     rowIDx,
                                            NVL (REC.ADDRESS_LINE1, ' ')
                                     || NVL (REC.ADDRESS_LINE2, ' '));
                                PG_EXCEL_UTILS.CELL (5,
                                                                         rowIDx,
                                                                         NVL (REC.ADDRESS_LINE3, ' '));
                                PG_EXCEL_UTILS.CELL (
                                     6,
                                     rowIDx,
                                     NVL (REC.POSTCODE, ' ') || ' ' || NVL (REC.CITY, ' '));
                                PG_EXCEL_UTILS.CELL (7, rowIDx, NVL (REC.STATE, ' '));
                                PG_EXCEL_UTILS.CELL (8,
                                                                         rowIDx,
                                                                         NVL (RISK_DET (V_ROW_NUM).SEX, ' '));

                                IF RISK_DET (V_ROW_NUM).DATE_OF_BIRTH IS NULL
                                THEN
                                     PG_EXCEL_UTILS.CELL (9, rowIDx, ' ');
                                ELSE
                                     PG_EXCEL_UTILS.CELL (
                                            9,
                                            rowIDx,
                                            TO_CHAR (RISK_DET (V_ROW_NUM).DATE_OF_BIRTH,
                                                             'DD/MM/YYYY'));
                                END IF;

                                PG_EXCEL_UTILS.CELL (10,
                                                                         rowIDx,
                                                                         NVL (RISK_DET (V_ROW_NUM).NRIC, ' '));

                                IF RISK_DET (V_ROW_NUM).NRIC IS NULL
                                THEN
                                     PG_EXCEL_UTILS.CELL (
                                            11,
                                            rowIDx,
                                            NVL (RISK_DET (V_ROW_NUM).NRIC_OTH, ' '));
                                ELSE
                                     PG_EXCEL_UTILS.CELL (11, rowIDx, ' ');
                                END IF;

                                PG_EXCEL_UTILS.CELL (
                                     12,
                                     rowIDx,
                                            RISK_DET (V_ROW_NUM).RISK_ID
                                     || '-'
                                     || REC.POLICY_REF
                                     || '-'
                                     || RISK_DET (V_ROW_NUM).COV_SEQ_REF);
                                PG_EXCEL_UTILS.CELL (13, rowIDx, ' ');
                                PG_EXCEL_UTILS.CELL (
                                     14,
                                     rowIDx,
                                     NVL (RISK_DET (V_ROW_NUM).EMPLOYEE_ID, ' '));
                                PG_EXCEL_UTILS.CELL (
                                     15,
                                     rowIDx,
                                     NVL (RISK_DET (V_ROW_NUM).MARITAL_STATUS, ' '));
                                PG_EXCEL_UTILS.CELL (16, rowIDx, ' ');
                                PG_EXCEL_UTILS.CELL (17,
                                                                         rowIDx,
                                                                         NVL (REC.PhoneNumber, ' '));
                                PG_EXCEL_UTILS.CELL (18, rowIDx, ' ');
                                PG_EXCEL_UTILS.CELL (19, rowIDx, ' ');
                                PG_EXCEL_UTILS.CELL (
                                     20,
                                     rowIDx,
                                     NVL (RISK_DET (V_ROW_NUM).RELATIONSHIP, ' '));
                                PG_EXCEL_UTILS.CELL (21, rowIDx, ' ');

                                IF RISK_DET (V_ROW_NUM).INSURED_TYPE = 'P'
                                THEN
                                     PG_EXCEL_UTILS.CELL (
                                            22,
                                            rowIDx,
                                                 RISK_DET (V_ROW_NUM).RISK_ID
                                            || '-'
                                            || REC.POLICY_REF
                                            || '-'
                                            || RISK_DET (V_ROW_NUM).COV_SEQ_REF);
                                     PG_EXCEL_UTILS.CELL (
                                            23,
                                            rowIDx,
                                            NVL (RISK_DET (V_ROW_NUM).MEMBER_FULL_NAME, ' '));
                                     PG_EXCEL_UTILS.CELL (
                                            24,
                                            rowIDx,
                                            NVL (RISK_DET (V_ROW_NUM).NRIC, ' '));

                                     IF RISK_DET (V_ROW_NUM).NRIC IS NULL
                                     THEN
                                            PG_EXCEL_UTILS.CELL (
                                                 25,
                                                 rowIDx,
                                                 NVL (RISK_DET (V_ROW_NUM).NRIC_OTH, ' '));
                                     ELSE
                                            PG_EXCEL_UTILS.CELL (25, rowIDx, ' ');
                                     END IF;
                                ELSE
                                     PG_EXCEL_UTILS.CELL (
                                            22,
                                            rowIDx,
                                                 RISK_DET (V_ROW_NUM).RISK_PARENT_ID
                                            || '-'
                                            || REC.POLICY_REF
                                            || '-'
                                            || RISK_DET (V_ROW_NUM).Parent_cov_seq_no);
                                     V_PRINCIPAL_DET :=
                                            PG_TPA_UTILS.FN_GET_PRINCIPAL_DET (
                                                 REC.CONTRACT_ID,
                                                 REC.POLICY_VERSION,
                                                 RISK_DET (V_ROW_NUM).RISK_PARENT_ID);
                                     PG_EXCEL_UTILS.CELL (
                                            23,
                                            rowIDx,
                                            NVL (V_PRINCIPAL_DET.MEMBER_FULL_NAME, ' '));
                                     PG_EXCEL_UTILS.CELL (24,
                                                                                rowIDx,
                                                                                NVL (V_PRINCIPAL_DET.NRIC, ' '));

                                     IF V_PRINCIPAL_DET.NRIC IS NULL
                                     THEN
                                            PG_EXCEL_UTILS.CELL (
                                                 25,
                                                 rowIDx,
                                                 NVL (V_PRINCIPAL_DET.NRIC_OTH, ' '));
                                     ELSE
                                            PG_EXCEL_UTILS.CELL (25, rowIDx, ' ');
                                     END IF;
                                END IF;

                                PG_EXCEL_UTILS.CELL (26, rowIDx, ' ');
                                PG_EXCEL_UTILS.CELL (27, rowIDx, 'IG');
                                PG_EXCEL_UTILS.CELL (28, rowIDx, NVL (REC.POLICY_REF, ' '));
                                PG_EXCEL_UTILS.CELL (29, rowIDx, REC.EFF_DATE);
                                PG_EXCEL_UTILS.CELL (30, rowIDx, REC.EXP_DATE);
                                PG_EXCEL_UTILS.CELL (31, rowIDx, NVL (REC.prev_pol, ' '));
                                PG_EXCEL_UTILS.CELL (32, rowIDx, REC.prev_exp_date);
                                PG_EXCEL_UTILS.CELL (33, rowIDx, NVL (REC.NAME_EXT, ' '));
                                V_UWPL_COVER_DET :=
                                     PG_TPA_UTILS.FN_GET_UWPL_COVER_DET (
                                            REC.CONTRACT_ID,
                                            REC.POLICY_VERSION,
                                            RISK_DET (V_ROW_NUM).COV_ID);
                                PG_EXCEL_UTILS.CELL (34,
                                                                         rowIDx,
                                                                         NVL (V_UWPL_COVER_DET.PLAN_CODE, ' '));
                                PG_EXCEL_UTILS.CELL (35, rowIDx, ' ');

                                IF RISK_DET (V_ROW_NUM).ORIGINAL_JOIN_DATE IS NULL
                                THEN
                                     PG_EXCEL_UTILS.CELL (36, rowIDx, ' ');
                                ELSE
                                     PG_EXCEL_UTILS.CELL (
                                            36,
                                            rowIDx,
                                            TO_CHAR (RISK_DET (V_ROW_NUM).ORIGINAL_JOIN_DATE,
                                                             'DD/MM/YYYY'));
                                END IF;

                                PG_EXCEL_UTILS.CELL (37,
                                                                         rowIDx,
                                                                         RISK_DET (V_ROW_NUM).RISK_EFF_DATE);
                                PG_EXCEL_UTILS.CELL (38,
                                                                         rowIDx,
                                                                         RISK_DET (V_ROW_NUM).RISK_EXP_DATE);
                                PG_EXCEL_UTILS.CELL (39, rowIDx, REC.BRANCH_DESC);
                                PG_EXCEL_UTILS.CELL (40, rowIDx, NVL (REC.AGENT_NAME, ' '));
                                PG_EXCEL_UTILS.CELL (41, rowIDx, NVL (REC.AGENT_CODE, ' '));

                                IF (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                                             V_RISK_LEVEL_DTLS,
                                             REC.PRODUCT_CONFIG_CODE) = 'Y')
                                THEN
                                     PG_EXCEL_UTILS.CELL (
                                            42,
                                            rowIDx,
                                            NVL (RISK_DET (V_ROW_NUM).MCO_FEE, 0));

                                     IF NVL (RISK_DET (V_ROW_NUM).IMA_FEE, 0) > 0
                                     THEN
                                            PG_EXCEL_UTILS.CELL (
                                                 43,
                                                 rowIDx,
                                                 'Y',
                                                 p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                                                                        p_vertical     => 'center',
                                                                                        p_horizontal   => 'center',
                                                                                        p_wrapText     => TRUE));
                                     ELSE
                                            PG_EXCEL_UTILS.CELL (
                                                 43,
                                                 rowIDx,
                                                 'N',
                                                 p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                                                                        p_vertical     => 'center',
                                                                                        p_horizontal   => 'center',
                                                                                        p_wrapText     => TRUE));
                                     END IF;

                                     IF NVL (RISK_DET (V_ROW_NUM).IMA_FEE, 0) > 0
                                     THEN
                                            IF (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                                                         V_IMA_LMT_2M,
                                                         REC.PRODUCT_CONFIG_CODE) = 'Y')
                                            THEN
                                                 PG_EXCEL_UTILS.CELL (44, rowIDx, '2000000');
                                            ELSE
                                                 PG_EXCEL_UTILS.CELL (44, rowIDx, '1000000');
                                            END IF;
                                     ELSE
                                            PG_EXCEL_UTILS.CELL (44, rowIDx, ' ');
                                     END IF;
                                ELSE
                                     PG_EXCEL_UTILS.CELL (
                                            42,
                                            rowIDx,
                                                NVL (REC.MCO_FEE_AMT, 0)
                                            + NVL (REC.MCOI_FEE_AMT, 0)
                                            + NVL (REC.MCOO_FEE_AMT, 0));

                                     IF REC.IMA_FEE_AMT > 0
                                     THEN
                                            PG_EXCEL_UTILS.CELL (
                                                 43,
                                                 rowIDx,
                                                 'Y',
                                                 p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                                                                        p_vertical     => 'center',
                                                                                        p_horizontal   => 'center',
                                                                                        p_wrapText     => TRUE));
                                     ELSE
                                            PG_EXCEL_UTILS.CELL (
                                                 43,
                                                 rowIDx,
                                                 'N',
                                                 p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                                                                        p_vertical     => 'center',
                                                                                        p_horizontal   => 'center',
                                                                                        p_wrapText     => TRUE));
                                     END IF;

                                     IF REC.IMA_FEE_AMT > 0
                                     THEN
                                            IF (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                                                         V_IMA_LMT_2M,
                                                         REC.PRODUCT_CONFIG_CODE) = 'Y')
                                            THEN
                                                 PG_EXCEL_UTILS.CELL (44, rowIDx, '2000000');
                                            ELSE
                                                 PG_EXCEL_UTILS.CELL (44, rowIDx, '1000000');
                                            END IF;
                                     ELSE
                                            PG_EXCEL_UTILS.CELL (44, rowIDx, ' ');
                                     END IF;
                                END IF;

                                PG_EXCEL_UTILS.CELL (45, rowIDx, REC.DateReceivedbyAAN);

                                IF REC.POLICY_VERSION > 1 AND REC.ENDT_CODE IN ('96', '97')
                                THEN
                                     PG_EXCEL_UTILS.CELL (46, rowIDx, REC.ENDT_EFF_DATE);
                                ELSE
                                     PG_EXCEL_UTILS.CELL (46,
                                                                                rowIDx,
                                                                                RISK_DET (V_ROW_NUM).TEMINATE_DATE);
                                END IF;

                                --dbms_output.put_line ('V_ROW_NUM::'||V_ROW_NUM);
                                IF V_ROW_NUM = 1
                                THEN
                                     IF DBMS_LOB.getlength (REC.ENDT_NARR) > 32000
                                     THEN
                                            V_ENDT_NARR_ARRAY :=
                                                 PG_TPA_UTILS.FN_SPLIT_CLOB (REC.ENDT_NARR);

                                            FOR I IN 1 .. V_ENDT_NARR_ARRAY.COUNT
                                            LOOP
                                                 --dbms_output.put_line('I::'||V_ENDT_NARR_ARRAY(I));
                                                 IF I = 1
                                                 THEN
                                                        PG_EXCEL_UTILS.CELL (
                                                             47,
                                                             rowIDx,
                                                             NVL (V_ENDT_NARR_ARRAY (1), ' '));
                                                 ELSE
                                                        PG_EXCEL_UTILS.CELL (
                                                             50 + I,
                                                             rowIDx,
                                                             NVL (V_ENDT_NARR_ARRAY (I), ' '));
                                                 END IF;
                                            END LOOP;
                                     ELSE
                                            PG_EXCEL_UTILS.CELL (47,
                                                                                     rowIDx,
                                                                                     NVL (REC.ENDT_NARR, ' '));
                                     END IF;
                                ELSE
                                     PG_EXCEL_UTILS.CELL (47, rowIDx, ' ');
                                END IF;

                                PG_EXCEL_UTILS.CELL (
                                     48,
                                     rowIDx,
                                     NVL (
                                            PG_TPA_UTILS.FN_GET_RISK_QUESTION (
                                                 REC.CONTRACT_ID,
                                                 REC.POLICY_VERSION,
                                                 RISK_DET (V_ROW_NUM).RISK_ID),
                                            ' '));
                                PG_EXCEL_UTILS.CELL (49,
                                                                         rowIDx,
                                                                         NVL (V_UWPL_COVER_DET.REMARKS, ' '));
                                PG_EXCEL_UTILS.CELL (50,
                                                                         rowIDx,
                                                                         NVL (PG_TPA_UTILS.FN_GET_COVER_DIAGNOSIS (
                                                                                         REC.CONTRACT_ID,
                                                                                         REC.POLICY_VERSION,
                                                                                         RISK_DET (V_ROW_NUM).RISK_ID,
                                                                                         RISK_DET (V_ROW_NUM).COV_ID),
                                                                                    ' '));

                                IF RISK_DET (V_ROW_NUM).OP_SUB_COV > 0
                                     AND (RISK_DET (V_ROW_NUM).IMPORT_TYPE IS NOT NULL AND RISK_DET (V_ROW_NUM).IMPORT_TYPE <> 'XO')
                                THEN
                                     PG_EXCEL_UTILS.CELL (51, rowIDx, 'Y');
                                ELSE
                                     PG_EXCEL_UTILS.CELL (51, rowIDx, 'N');
                                END IF;

                                PG_EXCEL_UTILS.CELL (
                                     52,
                                     rowIDx,
                                     NVL (RISK_DET (V_ROW_NUM).PREV_POL_OP_IND, ' '));

                                PG_EXCEL_UTILS.CELL (
                                     53,
                                     rowIDx,
                                     NVL (RISK_DET (V_ROW_NUM).DEPARTMENT, ' '));

                                rowIDx := rowIDx + 1;
                                seq := seq + 1;
                         END IF;
                    END LOOP;

                    --dbms_output.put_line ('RISK_DET::'||RISK_DET.COUNT);
                    /*IF RISK_DET.COUNT > 0
                    THEN
                         V_RET :=
                                PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD (REC.CONTRACT_ID,
                                                                                                        REC.POLICY_VERSION,
                                                                                                        'AAN');
                    END IF;*/
             END IF;
        END IF;
    END LOOP;

    V_STEPS := '010';

    FOR REC IN C_TPA_AAN_HC
    LOOP
       IF    (REC.POLICY_VERSION = 1 AND REC.MCOI_FEE_AMT > 0)
          OR (REC.POLICY_VERSION > 1 AND REC.MCOI_FEE_AMT >= 0)
          OR (REC.POLICY_VERSION = 1 AND REC.IMA_FEE_AMT > 0)
          OR (REC.POLICY_VERSION > 1 AND REC.IMA_FEE_AMT >= 0)
          OR (    PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_MCOO,
                                                      REC.PRODUCT_CONFIG_CODE) =
                     'Y'
              AND (   (REC.POLICY_VERSION = 1 AND REC.MCOO_FEE_AMT > 0)
                   OR (REC.POLICY_VERSION > 1 AND REC.MCOO_FEE_AMT >= 0)))
          OR (    PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_DMA,
                                                      REC.PRODUCT_CONFIG_CODE) =
                     'Y'
              AND (   (    REC.POLICY_VERSION = 1
                       AND (REC.MCOO_FEE_AMT > 0 OR REC.DMA_FEE_AMT > 0))
                   OR (    REC.POLICY_VERSION > 1
                       AND (REC.MCOO_FEE_AMT >= 0 OR REC.DMA_FEE_AMT >= 0))))
       THEN
          RISK_DET.DELETE;

          SELECT COUNT (1)
            INTO V_COUNT_TPA_RISK
            FROM UWGE_RISK_TPA_DOWNLOAD TPA
           WHERE     TPA.CONTRACT_ID = REC.CONTRACT_ID
                 AND TPA.VERSION_NO = REC.POLICY_VERSION;

          IF V_COUNT_TPA_RISK > 0
          THEN
             BEGIN
                EXECUTE IMMEDIATE V_SELECTED_RISK_SQL_TPA
                   BULK COLLECT INTO RISK_DET
                   USING REC.CONTRACT_ID, REC.POLICY_VERSION;
             END;
          ELSE
             IF     REC.ENDT_CODE IS NOT NULL
                AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_AAN_ENDT_CODE_R,
                                                        REC.ENDT_CODE) = 'Y'
             THEN
                BEGIN
                   EXECUTE IMMEDIATE V_SELECTED_RISK_SQL
                      BULK COLLECT INTO RISK_DET
                      USING REC.CONTRACT_ID, REC.POLICY_VERSION;
                END;
             ELSE
                BEGIN
                   EXECUTE IMMEDIATE V_ALL_RISK_SQL
                      BULK COLLECT INTO RISK_DET
                      USING REC.POLICY_VERSION,
                            REC.POLICY_VERSION,
                            REC.CONTRACT_ID,
                            REC.CONTRACT_ID,
                            REC.POLICY_VERSION;
                END;
             END IF;
          END IF;

          V_ROW_NUM := 0;

          FOR V_ROW_NUM IN 1 .. RISK_DET.COUNT
          LOOP
             V_STEPS := '011';
             PG_EXCEL_UTILS.CELL (1, rowIDx, seq);

             IF RISK_DET (V_ROW_NUM).IMPORT_TYPE IS NOT NULL
             THEN
                PG_EXCEL_UTILS.CELL (2,
                                     rowIDx,
                                     RISK_DET (V_ROW_NUM).IMPORT_TYPE);
             ELSE
                IF REC.POLICY_VERSION = 1 AND REC.PREV_POL_NO IS NOT NULL
                THEN
                   PG_EXCEL_UTILS.CELL (2, rowIDx, 'R');
                ELSIF REC.POLICY_VERSION = 1 AND REC.PREV_POL_NO IS NULL
                THEN
                   PG_EXCEL_UTILS.CELL (2, rowIDx, 'N');
                ELSIF REC.POLICY_VERSION > 1 AND REC.ENDT_CODE IN ('96', '97')
                THEN
                   PG_EXCEL_UTILS.CELL (2, rowIDx, 'X');
                ELSE
                   PG_EXCEL_UTILS.CELL (2, rowIDx, 'E');
                END IF;
             END IF;

             PG_EXCEL_UTILS.CELL (
                3,
                rowIDx,
                NVL (RISK_DET (V_ROW_NUM).MEMBER_FULL_NAME, ' '));
             PG_EXCEL_UTILS.CELL (
                4,
                rowIDx,
                NVL (REC.ADDRESS_LINE1, ' ') || NVL (REC.ADDRESS_LINE2, ' '));
             PG_EXCEL_UTILS.CELL (5, rowIDx, NVL (REC.ADDRESS_LINE3, ' '));
             PG_EXCEL_UTILS.CELL (
                6,
                rowIDx,
                NVL (REC.POSTCODE, ' ') || ' ' || NVL (REC.CITY, ' '));
             PG_EXCEL_UTILS.CELL (7, rowIDx, NVL (REC.STATE, ' '));
             PG_EXCEL_UTILS.CELL (8,
                                  rowIDx,
                                  NVL (RISK_DET (V_ROW_NUM).SEX, ' '));

             IF RISK_DET (V_ROW_NUM).DATE_OF_BIRTH IS NULL
             THEN
                PG_EXCEL_UTILS.CELL (9, rowIDx, ' ');
             ELSE
                PG_EXCEL_UTILS.CELL (
                   9,
                   rowIDx,
                   TO_CHAR (RISK_DET (V_ROW_NUM).DATE_OF_BIRTH, 'DD/MM/YYYY'));
             END IF;

             PG_EXCEL_UTILS.CELL (10,
                                  rowIDx,
                                  NVL (RISK_DET (V_ROW_NUM).NRIC, ' '));

             IF RISK_DET (V_ROW_NUM).NRIC IS NULL
             THEN
                PG_EXCEL_UTILS.CELL (11,
                                     rowIDx,
                                     NVL (RISK_DET (V_ROW_NUM).NRIC_OTH, ' '));
             ELSE
                PG_EXCEL_UTILS.CELL (11, rowIDx, ' ');
             END IF;

             PG_EXCEL_UTILS.CELL (
                12,
                rowIDx,
                   RISK_DET (V_ROW_NUM).RISK_ID
                || '-'
                || REC.POLICY_REF
                || '-'
                || RISK_DET (V_ROW_NUM).COV_SEQ_REF);
             PG_EXCEL_UTILS.CELL (13, rowIDx, ' ');
             PG_EXCEL_UTILS.CELL (14,
                                  rowIDx,
                                  NVL (RISK_DET (V_ROW_NUM).EMPLOYEE_ID, ' '));
             PG_EXCEL_UTILS.CELL (
                15,
                rowIDx,
                NVL (RISK_DET (V_ROW_NUM).MARITAL_STATUS, ' '));
             PG_EXCEL_UTILS.CELL (16, rowIDx, ' ');
             PG_EXCEL_UTILS.CELL (17, rowIDx, NVL (REC.PhoneNumber, ' '));
             PG_EXCEL_UTILS.CELL (18, rowIDx, ' ');
             PG_EXCEL_UTILS.CELL (19, rowIDx, ' ');
             PG_EXCEL_UTILS.CELL (
                20,
                rowIDx,
                NVL (RISK_DET (V_ROW_NUM).RELATIONSHIP, ' '));
             PG_EXCEL_UTILS.CELL (21, rowIDx, ' ');

             IF RISK_DET (V_ROW_NUM).INSURED_TYPE = 'P'
             THEN
                PG_EXCEL_UTILS.CELL (
                   22,
                   rowIDx,
                      RISK_DET (V_ROW_NUM).RISK_ID
                   || '-'
                   || REC.POLICY_REF
                   || '-'
                   || RISK_DET (V_ROW_NUM).COV_SEQ_REF);
                PG_EXCEL_UTILS.CELL (
                   23,
                   rowIDx,
                   NVL (RISK_DET (V_ROW_NUM).MEMBER_FULL_NAME, ' '));
                PG_EXCEL_UTILS.CELL (24,
                                     rowIDx,
                                     NVL (RISK_DET (V_ROW_NUM).NRIC, ' '));

                IF RISK_DET (V_ROW_NUM).NRIC IS NULL
                THEN
                   PG_EXCEL_UTILS.CELL (
                      25,
                      rowIDx,
                      NVL (RISK_DET (V_ROW_NUM).NRIC_OTH, ' '));
                ELSE
                   PG_EXCEL_UTILS.CELL (25, rowIDx, ' ');
                END IF;
             ELSE
                PG_EXCEL_UTILS.CELL (
                   22,
                   rowIDx,
                      RISK_DET (V_ROW_NUM).RISK_PARENT_ID
                   || '-'
                   || REC.POLICY_REF
                   || '-'
                   || RISK_DET (V_ROW_NUM).Parent_cov_seq_no);
                V_PRINCIPAL_DET :=
                   PG_TPA_UTILS.FN_GET_PRINCIPAL_DET (
                      REC.CONTRACT_ID,
                      REC.POLICY_VERSION,
                      RISK_DET (V_ROW_NUM).RISK_PARENT_ID);
                PG_EXCEL_UTILS.CELL (
                   23,
                   rowIDx,
                   NVL (V_PRINCIPAL_DET.MEMBER_FULL_NAME, ' '));
                PG_EXCEL_UTILS.CELL (24,
                                     rowIDx,
                                     NVL (V_PRINCIPAL_DET.NRIC, ' '));

                IF V_PRINCIPAL_DET.NRIC IS NULL
                THEN
                   PG_EXCEL_UTILS.CELL (25,
                                        rowIDx,
                                        NVL (V_PRINCIPAL_DET.NRIC_OTH, ' '));
                ELSE
                   PG_EXCEL_UTILS.CELL (25, rowIDx, ' ');
                END IF;
             END IF;

             PG_EXCEL_UTILS.CELL (26, rowIDx, ' ');
             PG_EXCEL_UTILS.CELL (27, rowIDx, 'IG');
             PG_EXCEL_UTILS.CELL (28, rowIDx, NVL (REC.POLICY_REF, ' '));
             PG_EXCEL_UTILS.CELL (29, rowIDx, REC.EFF_DATE);
             PG_EXCEL_UTILS.CELL (30, rowIDx, REC.EXP_DATE);
             PG_EXCEL_UTILS.CELL (31, rowIDx, NVL (REC.prev_pol, ' '));
             PG_EXCEL_UTILS.CELL (32, rowIDx, REC.prev_exp_date);
             PG_EXCEL_UTILS.CELL (33, rowIDx, NVL (REC.NAME_EXT, ' '));
             V_UWPL_COVER_DET :=
                PG_TPA_UTILS.FN_GET_UWPL_COVER_DET (
                   REC.CONTRACT_ID,
                   REC.POLICY_VERSION,
                   RISK_DET (V_ROW_NUM).COV_ID);
             PG_EXCEL_UTILS.CELL (34,
                                  rowIDx,
                                  NVL (V_UWPL_COVER_DET.PLAN_CODE, ' '));
             PG_EXCEL_UTILS.CELL (35, rowIDx, ' ');

             IF RISK_DET (V_ROW_NUM).ORIGINAL_JOIN_DATE IS NULL
             THEN
                PG_EXCEL_UTILS.CELL (36, rowIDx, ' ');
             ELSE
                PG_EXCEL_UTILS.CELL (
                   36,
                   rowIDx,
                   TO_CHAR (RISK_DET (V_ROW_NUM).ORIGINAL_JOIN_DATE,
                            'DD/MM/YYYY'));
             END IF;

             PG_EXCEL_UTILS.CELL (37,
                                  rowIDx,
                                  RISK_DET (V_ROW_NUM).RISK_EFF_DATE);
             PG_EXCEL_UTILS.CELL (38,
                                  rowIDx,
                                  RISK_DET (V_ROW_NUM).RISK_EXP_DATE);
             PG_EXCEL_UTILS.CELL (39, rowIDx, REC.BRANCH_DESC);

             PG_EXCEL_UTILS.CELL (40, rowIDx, NVL (REC.AGENT_NAME, ' '));
             PG_EXCEL_UTILS.CELL (41, rowIDx, NVL (REC.AGENT_CODE, ' '));
             PG_EXCEL_UTILS.CELL (
                42,
                rowIDx,
                  NVL (REC.MCO_FEE_AMT, 0)
                + NVL (REC.MCOI_FEE_AMT, 0)
                + NVL (REC.MCOO_FEE_AMT, 0));

             IF REC.IMA_FEE_AMT > 0
             THEN
                PG_EXCEL_UTILS.CELL (
                   43,
                   rowIDx,
                   'Y',
                   p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                      p_vertical     => 'center',
                                      p_horizontal   => 'center',
                                      p_wrapText     => TRUE));
             ELSE
                PG_EXCEL_UTILS.CELL (
                   43,
                   rowIDx,
                   'N',
                   p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                      p_vertical     => 'center',
                                      p_horizontal   => 'center',
                                      p_wrapText     => TRUE));
             END IF;

             IF REC.IMA_FEE_AMT > 0
             THEN
                PG_EXCEL_UTILS.CELL (44, rowIDx, '1000000');
             ELSE
                PG_EXCEL_UTILS.CELL (44, rowIDx, ' ');
             END IF;

             PG_EXCEL_UTILS.CELL (45, rowIDx, REC.DateReceivedbyAAN);

             IF REC.POLICY_VERSION > 1 AND REC.ENDT_CODE IN ('96', '97')
             THEN
                PG_EXCEL_UTILS.CELL (46, rowIDx, REC.ENDT_EFF_DATE);
             ELSE
                PG_EXCEL_UTILS.CELL (46,
                                     rowIDx,
                                     RISK_DET (V_ROW_NUM).TEMINATE_DATE);
             END IF;

             --dbms_output.put_line ('V_ROW_NUM::'||V_ROW_NUM);
             IF V_ROW_NUM = 1
             THEN
                --dbms_output.put_line ('ENDT_NARR::'||DBMS_LOB.getlength(REC.ENDT_NARR));
                IF DBMS_LOB.getlength (REC.ENDT_NARR) > 32000
                THEN
                   V_ENDT_NARR_ARRAY :=
                      PG_TPA_UTILS.FN_SPLIT_CLOB (REC.ENDT_NARR);

                   FOR I IN 1 .. V_ENDT_NARR_ARRAY.COUNT
                   LOOP
                      --dbms_output.put_line('I::'||V_ENDT_NARR_ARRAY(I));
                      IF I = 1
                      THEN
                         PG_EXCEL_UTILS.CELL (
                            47,
                            rowIDx,
                            NVL (V_ENDT_NARR_ARRAY (1), ' '));
                      ELSE
                         PG_EXCEL_UTILS.CELL (
                            50 + I,
                            rowIDx,
                            NVL (V_ENDT_NARR_ARRAY (I), ' '));
                      END IF;
                   END LOOP;
                ELSE
                   PG_EXCEL_UTILS.CELL (47, rowIDx, NVL (REC.ENDT_NARR, ' '));
                END IF;
             ELSE
                PG_EXCEL_UTILS.CELL (47, rowIDx, ' ');
             END IF;

             PG_EXCEL_UTILS.CELL (
                48,
                rowIDx,
                NVL (
                   PG_TPA_UTILS.FN_GET_RISK_QUESTION (
                      REC.CONTRACT_ID,
                      REC.POLICY_VERSION,
                      RISK_DET (V_ROW_NUM).RISK_ID),
                   ' '));
             PG_EXCEL_UTILS.CELL (49,
                                  rowIDx,
                                  NVL (V_UWPL_COVER_DET.REMARKS, ' '));
             PG_EXCEL_UTILS.CELL (50,
                                  rowIDx,
                                  NVL (PG_TPA_UTILS.FN_GET_COVER_DIAGNOSIS (
                                          REC.CONTRACT_ID,
                                          REC.POLICY_VERSION,
                                          RISK_DET (V_ROW_NUM).RISK_ID,
                                          RISK_DET (V_ROW_NUM).COV_ID),
                                       ' '));

             IF RISK_DET (V_ROW_NUM).OP_SUB_COV > 0 
                AND (RISK_DET (V_ROW_NUM).IMPORT_TYPE IS NOT NULL AND RISK_DET (V_ROW_NUM).IMPORT_TYPE <> 'XO')
             THEN
                PG_EXCEL_UTILS.CELL (51, rowIDx, 'Y');
             ELSE
                PG_EXCEL_UTILS.CELL (51, rowIDx, 'N');
             END IF;

             PG_EXCEL_UTILS.CELL (
                52,
                rowIDx,
                NVL (RISK_DET (V_ROW_NUM).PREV_POL_OP_IND, ' '));

             PG_EXCEL_UTILS.CELL (53,
                                  rowIDx,
                                  NVL (RISK_DET (V_ROW_NUM).DEPARTMENT, ' '));

             rowIDx := rowIDx + 1;
             seq := seq + 1;
          END LOOP;

          --dbms_output.put_line ('RISK_DET::'||RISK_DET.COUNT);
          /*IF RISK_DET.COUNT > 0
          THEN
             V_RET :=
                PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD (REC.CONTRACT_ID,
                                                    REC.POLICY_VERSION,
                                                    'AAN');               --1.3
          END IF;*/
       END IF;
    END LOOP;

    V_STEPS := '016';
    DBMS_OUTPUT.ENABLE (buffer_size => NULL);
    PG_EXCEL_UTILS.save (v_file_dir, FILENAME1);
EXCEPTION
    WHEN OTHERS
        THEN
      PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
                V_PKG_NAME || V_FUNC_NAME,
                1,
                '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
                --dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
END PC_TPA_AAN_HC_PA_POL_ENDT_ADHOC;
/* 3.5 END */
  -- 3.4 START
	PROCEDURE PC_TPA_MEDIX_POL_ENDT(P_START_DT  IN  UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE,
	                                FILENAME1   IN  VARCHAR2,
	                                FILENAME2   IN  VARCHAR2)
	IS
		V_STEPS          VARCHAR2(10);
		V_FUNC_NAME      VARCHAR2(100) := 'PC_TPA_MEDIX_POL_ENDT';
		V_FILE_NAME_LIST PG_TPA_UTILS.p_array_v := PG_TPA_UTILS.p_array_v();
		V_Return         VARCHAR2(10);

		v_file_dir      VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_AAN_DIR');

	BEGIN

		--V_STEPS := '001';
		--COMPRESSING FILES
		--BEGIN
			--EXECUTE IMMEDIATE '!/bin/zip /opus/download/tpa/aan/MEDIX.zip /opus/download/tpa/aan/'||FILENAME1||' /opus/download/tpa/aan/'||FILENAME2;
		--END;

		V_FILE_NAME_LIST.EXTEND;
		--V_FILE_NAME_LIST(1) := 'MEDIX.zip';
		V_FILE_NAME_LIST(1) := FILENAME1;
		V_FILE_NAME_LIST.EXTEND;
		V_FILE_NAME_LIST(2) := FILENAME2;

		V_STEPS := '002';
		--SENDING EMAIL WITH ATTACHMENT
		V_Return := PG_TPA_UTILS.FN_DDLOADING_SEND_EMAIL('MEDIX',v_file_dir,V_FILE_NAME_LIST,P_START_DT);

	EXCEPTION
			WHEN OTHERS
			THEN
				PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
					V_PKG_NAME || V_FUNC_NAME,
					1,
					v_file_dir || FILENAME1 ||
					'::' ||
					v_file_dir || FILENAME2 ||
					'::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
	END PC_TPA_MEDIX_POL_ENDT;

  PROCEDURE PC_TPA_MEDIX_MS_POL_ENDT(
		P_START_DT  IN  UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE,
		P_FILE_NAME OUT VARCHAR2)
	IS
		CURSOR C_TPA_MEDIX_MS
			IS
				SELECT
					OPB.POLICY_REF,
					UPV.VERSION_NO AS POLICY_VERSION,
					(CASE
						WHEN CP.ID_VALUE1 IS NOT NULL AND LENGTH(ID_VALUE1) = 12
						THEN SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
						WHEN CP.ID_VALUE1 IS NOT NULL
						THEN CP.ID_VALUE1 
						ELSE CP.ID_VALUE2
					END) AS ID_VALUE,
					(CASE 
						WHEN ID_TYPE1 IS NULL 
						THEN ID_TYPE2 
						ELSE ID_TYPE1 
					END) ID_TYPE,
					UPC.PRODUCT_CONFIG_CODE,
					(SELECT CODE_DESC FROM CUSTOMER.CMGE_CODE CC 
						WHERE CC.CODE_CD = UPC.PRODUCT_CONFIG_CODE AND CC.CAT_CODE = UPC.LOB||'_PRODUCT'
					) AS PRODUCT_DESC,
					CP.NAME_EXT,
					UPB.LONG_NAME,
					TO_CHAR(UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,
					TO_CHAR(UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
					TO_CHAR(UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,
					NVL(
						(SELECT UPF.FEE_AMT FROM CUSTOMER.UWGE_POLICY_FEES UPF
						 WHERE UPF.CONTRACT_ID = UPV.CONTRACT_ID
						 AND UPF.VERSION_NO = UPV.VERSION_NO
						 AND UPF.FEE_CODE = 'ASST'), 0
					) AS ASST_FEE_AMT,
					NVL(
						(SELECT UPF.FEE_AMT from CUSTOMER.UWGE_POLICY_FEES UPF
						WHERE UPF.CONTRACT_ID = UPV.CONTRACT_ID
						AND UPF.VERSION_NO = UPV.VERSION_NO
						AND UPF.FEE_CODE = 'DMA'), 0
					) AS DMA_FEE_AMT,
					NVL(UPV.ENDT_NARR,' ') AS ENDT_NARR,
					UPV.ENDT_NO,
					UPV.CONTRACT_ID
        FROM CUSTOMER.UWGE_POLICY_VERSIONS UPV
        INNER JOIN CUSTOMER.UWGE_POLICY_CTRL_DLOAD UPCD ON UPV.CONTRACT_ID = UPCD.CONTRACT_ID AND UPV.VERSION_NO = UPCD.VERSION_NO
        INNER JOIN CUSTOMER.UWGE_POLICY_CONTRACTS UPC ON UPV.CONTRACT_ID = UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB ON UPV.CONTRACT_ID = OPB.CONTRACT_ID
        INNER JOIN CUSTOMER.UWGE_POLICY_BASES UPB ON UPB.CONTRACT_ID = UPV.CONTRACT_ID AND UPB.VERSION_NO = UPV.VERSION_NO
        INNER JOIN CUSTOMER.UWPL_POLICY_BASES PLPB ON PLPB.CONTRACT_ID = UPV.CONTRACT_ID AND PLPB.VERSION_NO = UPV.VERSION_NO
        INNER JOIN CUSTOMER.UWPL_RISK_PERSON URP ON URP.CONTRACT_ID = UPV.CONTRACT_ID AND URP.VERSION_NO = UPV.VERSION_NO
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(URP.RISK_PART_ID, URP.RISK_PART_VER)) CP
					ON CP.PART_ID = URP.RISK_PART_ID
					AND CP.VERSION = URP.RISK_PART_VER
				WHERE
					UPC.PRODUCT_CONFIG_CODE IN(
						SELECT regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_MS'),'[^,]+', 1, LEVEL) FROM dual
						CONNECT BY regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_MS'), '[^,]+', 1, LEVEL) IS NOT NULL)
					AND UPC.POLICY_STATUS IN ('A','C','E')
					AND PLPB.TPA_NAME = 'MEDIX'
					AND (UPV.ENDT_CODE IS NULL OR UPV.ENDT_CODE IN(
						SELECT regexp_substr(V_AAN_ENDT_CODE_R,'[^,]+', 1, LEVEL) FROM dual
						CONNECT BY regexp_substr(V_AAN_ENDT_CODE_R, '[^,]+', 1, LEVEL) IS NOT NULL ))
					AND UPV.ACTION_CODE IN ('A','C')
					AND UPC.LOB = 'MS'
					AND UPCD.DLOAD_STATUS = 'P'
					AND UPCD.TPA_NAME = 'MEDIX'
        UNION ALL
        SELECT
					OPB.POLICY_REF,
					UPV.VERSION_NO AS POLICY_VERSION,
					(CASE WHEN CP.ID_VALUE1 is not null AND length(ID_VALUE1) =12 THEN SUBSTR(CP.ID_VALUE1,1,6)||'-'||SUBSTR(CP.ID_VALUE1,7,2)||'-'||SUBSTR(CP.ID_VALUE1,9,4)
					WHEN CP.ID_VALUE1 is not null THEN CP.ID_VALUE1 else CP.ID_VALUE2  END) AS ID_VALUE,
					(CASE
						WHEN ID_TYPE1 IS NULL 
						THEN ID_TYPE2 
						ELSE ID_TYPE1
					END) ID_TYPE,
					UPC.PRODUCT_CONFIG_CODE,
					(SELECT CODE_DESC FROM CUSTOMER.CMGE_CODE CC
						WHERE CC.CODE_CD = UPC.PRODUCT_CONFIG_CODE AND CC.CAT_CODE = UPC.LOB||'_PRODUCT'
					) AS PRODUCT_DESC,
					CP.NAME_EXT,
					UPB.LONG_NAME,
					TO_CHAR(UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,
					TO_CHAR(UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
					TO_CHAR(UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,
					NVL(
						(SELECT UPF.FEE_AMT FROM CUSTOMER.UWGE_POLICY_FEES UPF
						WHERE UPF.CONTRACT_ID = UPV.CONTRACT_ID
						AND UPF.VERSION_NO = UPV.VERSION_NO
						AND UPF.FEE_CODE = 'ASST'), 0
					) AS ASST_FEE_AMT,
					NVL(
						(SELECT UPF.FEE_AMT FROM CUSTOMER.UWGE_POLICY_FEES UPF
						WHERE UPF.CONTRACT_ID = UPV.CONTRACT_ID
						AND UPF.VERSION_NO = UPV.VERSION_NO
						AND UPF.FEE_CODE = 'DMA'), 0
					) AS DMA_FEE_AMT,
					NVL(UPV.ENDT_NARR,' ') AS ENDT_NARR,UPV.ENDT_NO,
					UPV.CONTRACT_ID
        FROM CUSTOMER.UWGE_POLICY_VERSIONS UPV
        INNER JOIN CUSTOMER.UWGE_POLICY_CTRL_DLOAD UPCD ON UPV.CONTRACT_ID = UPCD.CONTRACT_ID AND UPV.VERSION_NO = UPCD.VERSION_NO
        INNER JOIN CUSTOMER.UWGE_POLICY_CONTRACTS UPC ON UPV.CONTRACT_ID = UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB ON UPV.CONTRACT_ID = OPB.CONTRACT_ID
        INNER JOIN CUSTOMER.UWGE_POLICY_BASES UPB ON UPB.CONTRACT_ID = UPV.CONTRACT_ID AND UPB.VERSION_NO = UPV.VERSION_NO
        INNER JOIN CUSTOMER.UWPL_POLICY_BASES PLPB ON PLPB.CONTRACT_ID = UPV.CONTRACT_ID AND PLPB.VERSION_NO = UPV.VERSION_NO
        INNER JOIN CUSTOMER.UWPL_RISK_PERSON URP ON URP.CONTRACT_ID = UPV.CONTRACT_ID AND URP.VERSION_NO =(
					SELECT MAX (b.version_no) FROM CUSTOMER.UWPL_RISK_PERSON b
					WHERE b.contract_id = UPV.CONTRACT_ID
					AND URP.object_id = b.object_id
					AND b.version_no <= UPV.VERSION_NO
					AND b.reversing_version IS NULL
				)
        AND URP.action_code <> 'D'
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE(URP.RISK_PART_ID, URP.RISK_PART_VER)) CP
					ON CP.PART_ID = URP.RISK_PART_ID
					AND CP.VERSION = URP.RISK_PART_VER
        WHERE
					UPC.PRODUCT_CONFIG_CODE IN(
						SELECT regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_MS'),'[^,]+', 1, LEVEL) FROM dual
						CONNECT BY regexp_substr(PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_MS'), '[^,]+', 1, LEVEL) IS NOT NULL)
					AND UPC.POLICY_STATUS IN('A','C','E')
					AND PLPB.TPA_NAME = 'MEDIX'
					AND UPV.VERSION_NO > 1
					AND (UPV.ENDT_CODE IN(
						SELECT regexp_substr(V_AAN_ENDT_CODE_A,'[^,]+', 1, LEVEL) FROM dual
						CONNECT BY regexp_substr(V_AAN_ENDT_CODE_A, '[^,]+', 1, LEVEL) IS NOT NULL ))
					AND UPV.ACTION_CODE IN('A','C')
					AND UPC.LOB = 'MS'
					AND UPCD.DLOAD_STATUS ='P'
					AND UPCD.TPA_NAME='MEDIX';

		V_STEPS         VARCHAR2(10);
		V_FUNC_NAME     VARCHAR2(100) :='PC_TPA_MEDIX_MS_POL_ENDT';
		FILENAME        UTL_FILE.FILE_TYPE;
		FILENAME1       VARCHAR2(1000);

		v_file_dir      VARCHAR2 (100) := PG_TPA_UTILS.FN_GET_SYS_PARAM ( 'PG_TPA', 'TPA_AAN_DIR');
		rowIDx          NUMBER := 5;
		seq             NUMBER := 1;
		V_RET           NUMBER := 0;

    BEGIN
			V_STEPS := '001';
			FILENAME1   := TO_CHAR(P_START_DT, 'YYYYMMDD')||'_MISCELLANEOUS_POLEND_MEDIX.xlsx';

			PG_EXCEL_UTILS.clear_workbook;
			PG_EXCEL_UTILS.new_sheet;
			PG_EXCEL_UTILS.CELL(1,1,'BORDEREAUX (POLICY &'||' ENDORSEMENT)');

			PG_EXCEL_UTILS.MERGECELLS(1,1,3,1);
			PG_EXCEL_UTILS.CELL(1,2,'FROM : ALLIANZ GENERAL INSURANCE COMPANY (MALAYSIA) BERHAD');
			PG_EXCEL_UTILS.MERGECELLS(1,2,3,2);
			PG_EXCEL_UTILS.CELL(1,3,'DATE :');
			PG_EXCEL_UTILS.CELL(2,3,TO_CHAR(P_START_DT, 'DD/MM/YYYY'));

			PG_EXCEL_UTILS.SET_ROW(4, p_fontId => PG_EXCEL_UTILS.get_font( 'Arial', p_bold => true));
			PG_EXCEL_UTILS.CELL(1,4,'No.');
			PG_EXCEL_UTILS.CELL(2,4,'Transaction Type');
			PG_EXCEL_UTILS.SET_COLUMN_WIDTH(2,30);
			PG_EXCEL_UTILS.CELL(3,4,'Product Code');
			PG_EXCEL_UTILS.SET_COLUMN_WIDTH(3,40);
			PG_EXCEL_UTILS.CELL(4,4,'Product Name');
			PG_EXCEL_UTILS.SET_COLUMN_WIDTH(4,20);
			PG_EXCEL_UTILS.CELL(5,4,'Name');
			PG_EXCEL_UTILS.SET_COLUMN_WIDTH(5,20);
			PG_EXCEL_UTILS.CELL(6,4,'ID Type');
			PG_EXCEL_UTILS.SET_COLUMN_WIDTH(6,20);
			PG_EXCEL_UTILS.CELL(7,4,'ID No.');
			PG_EXCEL_UTILS.SET_COLUMN_WIDTH(7,20);
			PG_EXCEL_UTILS.CELL(8,4,'Policy No.');
			PG_EXCEL_UTILS.SET_COLUMN_WIDTH(8,20);
			PG_EXCEL_UTILS.CELL(9,4,'Effective Date');
			PG_EXCEL_UTILS.SET_COLUMN_WIDTH(9,20);
			PG_EXCEL_UTILS.CELL(10,4,'Expiry Date');
			PG_EXCEL_UTILS.SET_COLUMN_WIDTH(10,20);
			PG_EXCEL_UTILS.CELL(11,4,'Text Decription');
			PG_EXCEL_UTILS.SET_COLUMN_WIDTH(11,20);

			FOR REC IN C_TPA_MEDIX_MS
			LOOP
				IF (REC.POLICY_VERSION =1 AND REC.ASST_FEE_AMT >0 AND REC.PRODUCT_CONFIG_CODE ='012104')
					OR (REC.POLICY_VERSION >1 AND REC.ASST_FEE_AMT >=0 AND REC.PRODUCT_CONFIG_CODE ='012104')
					OR (REC.POLICY_VERSION >1 AND REC.DMA_FEE_AMT >=0 AND REC.PRODUCT_CONFIG_CODE ='012102')
					OR (REC.POLICY_VERSION =1 AND REC.DMA_FEE_AMT >0 AND REC.PRODUCT_CONFIG_CODE ='012102')
				THEN
					PG_EXCEL_UTILS.CELL(1,rowIDx,seq);
					IF REC.POLICY_VERSION =1 THEN
						PG_EXCEL_UTILS.CELL(2,rowIDx,'PL');
					ELSE
						PG_EXCEL_UTILS.CELL(2,rowIDx,'EN');
					END IF;

					PG_EXCEL_UTILS.CELL(3,rowIDx,REC.PRODUCT_CONFIG_CODE);
					PG_EXCEL_UTILS.CELL(4,rowIDx,REC.PRODUCT_DESC);
					PG_EXCEL_UTILS.CELL(5,rowIDx,REC.NAME_EXT);
					PG_EXCEL_UTILS.CELL(6,rowIDx,REC.ID_TYPE);
					PG_EXCEL_UTILS.CELL(7,rowIDx,REC.ID_VALUE);
					IF REC.POLICY_VERSION =1
					THEN
						PG_EXCEL_UTILS.CELL(8,rowIDx,REC.POLICY_REF);
					ELSE
						PG_EXCEL_UTILS.CELL(8,rowIDx,REC.ENDT_NO);
					END IF;

					PG_EXCEL_UTILS.CELL(9,rowIDx,REC.EFF_DATE);
					PG_EXCEL_UTILS.CELL(10,rowIDx,REC.EXP_DATE);
					PG_EXCEL_UTILS.CELL(11,rowIDx,REC.ENDT_NARR);

					rowIDx := rowIDx+1;
					seq := seq+1;
					V_RET := PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD(REC.CONTRACT_ID,REC.POLICY_VERSION,'MEDIX');

				END IF;
			END LOOP;

			P_FILE_NAME := FILENAME1;

			PG_EXCEL_UTILS.save( v_file_dir, FILENAME1 );

		EXCEPTION
			WHEN OTHERS
			THEN
				PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
					V_PKG_NAME || V_FUNC_NAME,
					1,
					'::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
				--dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
	END PC_TPA_MEDIX_MS_POL_ENDT;

  PROCEDURE PC_TPA_MEDIX_HC_PA_POL_ENDT (
		P_START_DT  IN  UWGE_POLICY_VERSIONS.ISSUE_DATE%TYPE,
		P_FILE_NAME OUT VARCHAR2)
	IS
		CURSOR C_TPA_MEDIX_PA
			IS
        SELECT
					TO_CHAR (UPV.ENDT_EFF_DATE, 'DD/MM/YYYY') AS ENDT_EFF_DATE,
					OPB.POLICY_REF,
					UPB.PREV_POL_NO,
					UPV.ENDT_CODE,
					UPV.VERSION_NO AS POLICY_VERSION,
					UPB.AGENT_CODE,
					DVA.NAME AS AGENT_NAME,
					(CASE
					    WHEN upb.PREV_POL_NO IS NOT NULL THEN upb.PREV_POL_NO
					    ELSE upb.PREV_POL_NO_IIMS
					END) AS prev_pol,
					(CASE
					    WHEN CP.ID_VALUE1 IS NULL THEN CP.ID_VALUE2
					    ELSE CP.ID_VALUE1
					END) AS P_NRIC_OTH,
					(CASE
					    WHEN CP.ID_TYPE1 = 'NRIC' THEN CP.ID_VALUE1
					    WHEN CP.ID_TYPE2 = 'NRIC' THEN CP.ID_VALUE2
					END) AS P_NRIC,
               REGEXP_REPLACE (
					(CASE
						WHEN CP.MOBILE_NO1 IS NOT NULL
						AND CP.MOBILE_CODE1 IS NOT NULL
						THEN
							CP.MOBILE_CODE1 || CP.MOBILE_NO1
						ELSE
							CP.MOBILE_CODE2 || CP.MOBILE_NO2
					END), '[^0-9]') AS PhoneNumber,
					REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
					REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,
					REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3,
					CPA.POSTCODE,
					(SELECT CODE_DESC
					FROM CUSTOMER.CMGE_CODE
					WHERE CAT_CODE = 'CITY' AND CODE_CD = CPA.CITY) AS CITY,
					(SELECT CODE_DESC
					FROM CUSTOMER.CMGE_CODE
					WHERE CAT_CODE = 'STATE' AND CODE_CD = CPA.STATE) AS STATE,
					UPC.PRODUCT_CONFIG_CODE,
					CP.NAME_EXT,
					UPB.LONG_NAME,
					TO_CHAR (UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,
					TO_CHAR (UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
					TO_CHAR (UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,
					(SELECT UPF.FEE_AMT
					FROM CUSTOMER.UWGE_POLICY_FEES UPF
					WHERE UPF.CONTRACT_ID = UPV.CONTRACT_ID
					AND UPF.VERSION_NO = UPV.VERSION_NO
					AND UPF.FEE_CODE = 'ASST') AS ASST_FEE_AMT,
					(SELECT UPF.FEE_AMT
					FROM CUSTOMER.UWGE_POLICY_FEES UPF
					WHERE UPF.CONTRACT_ID = UPV.CONTRACT_ID
					AND UPF.VERSION_NO = UPV.VERSION_NO
					AND UPF.FEE_CODE = 'MCO') AS MCO_FEE_AMT,
					(SELECT UPF.FEE_AMT
					FROM CUSTOMER.UWGE_POLICY_FEES UPF
					WHERE UPF.CONTRACT_ID = UPV.CONTRACT_ID
					AND UPF.VERSION_NO = UPV.VERSION_NO
					 AND UPF.FEE_CODE = 'MCOO') AS MCOO_FEE_AMT,
					(SELECT UPF.FEE_AMT
					FROM CUSTOMER.UWGE_POLICY_FEES UPF
					WHERE UPF.CONTRACT_ID = UPV.CONTRACT_ID
					AND UPF.VERSION_NO = UPV.VERSION_NO
					AND UPF.FEE_CODE = 'MCOI') AS MCOI_FEE_AMT,
 					(SELECT UPF.FEE_AMT
 					FROM CUSTOMER.UWGE_POLICY_FEES UPF
 					WHERE UPF.CONTRACT_ID = UPV.CONTRACT_ID
 					AND UPF.VERSION_NO = UPV.VERSION_NO
 					AND UPF.FEE_CODE = 'IMA') AS IMA_FEE_AMT,
					NVL (UPV.ENDT_NARR, ' ') AS ENDT_NARR,
					UPV.ENDT_NO,
					TO_CHAR (SYSDATE, 'DD/MM/YYYY') AS DateReceivedbyAAN,
					UPB.ISSUE_OFFICE,
					(SELECT BRANCH_NAME
					FROM CUSTOMER.CMDM_BRANCH
					WHERE BRANCH_CODE = UPB.ISSUE_OFFICE) AS BRANCH_DESC,
					(SELECT EXP_DATE
					FROM CUSTOMER.uwge_policy_bases
					WHERE CONTRACT_ID = (SELECT CONTRACT_ID
						FROM OCP_POLICY_BASES
						WHERE policy_ref =(CASE
							WHEN upb.PREV_POL_NO IS NOT NULL THEN upb.PREV_POL_NO
							ELSE upb.PREV_POL_NO_IIMS END) 
						AND ROWNUM = 1)
					AND uwge_policy_bases.TOP_INDICATOR = 'Y'
					AND ROWNUM = 1) AS prev_exp_date,
					OPB.CONTRACT_ID
        FROM CUSTOMER.UWGE_POLICY_VERSIONS UPV
				INNER JOIN CUSTOMER.UWGE_POLICY_CTRL_DLOAD UPCD
					ON UPV.CONTRACT_ID = UPCD.CONTRACT_ID
					AND UPV.VERSION_NO = UPCD.VERSION_NO
				INNER JOIN CUSTOMER.UWGE_POLICY_CONTRACTS UPC
					ON UPV.CONTRACT_ID = UPC.CONTRACT_ID
				INNER JOIN OCP_POLICY_BASES OPB
					ON UPV.CONTRACT_ID = OPB.CONTRACT_ID
				INNER JOIN CUSTOMER.UWGE_POLICY_BASES UPB
					ON UPB.CONTRACT_ID = UPV.CONTRACT_ID
					AND UPB.VERSION_NO = UPV.VERSION_NO
				INNER JOIN CUSTOMER.UWPL_POLICY_BASES PLPB
					ON PLPB.CONTRACT_ID = UPV.CONTRACT_ID
					AND PLPB.VERSION_NO = UPV.VERSION_NO
				INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (UPB.CP_PART_ID, UPB.CP_VERSION)) CP
					ON CP.PART_ID = UPB.CP_PART_ID
					AND CP.VERSION = UPB.CP_VERSION
				INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (UPB.CP_ADDR_ID,UPB.CP_ADDR_VERSION)) CPA
					ON CPA.ADD_ID = UPB.CP_ADDR_ID AND CPA.VERSION = UPB.CP_ADDR_VERSION
				INNER JOIN CUSTOMER.DMAG_VI_AGENT DVA ON DVA.AGENTCODE = UPB.AGENT_CODE
				WHERE UPC.PRODUCT_CONFIG_CODE IN (SELECT REGEXP_SUBSTR (
					PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA','AAN_PA'),'[^,]+',1,LEVEL) FROM DUAL
					CONNECT BY REGEXP_SUBSTR (PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA','AAN_PA'),'[^,]+',1,LEVEL) IS NOT NULL)
				AND UPC.POLICY_STATUS IN ('A', 'C', 'E')
				AND PLPB.TPA_NAME = 'MEDIX'
				AND UPV.ACTION_CODE IN ('A', 'C')
				AND (UPV.ENDT_CODE IS NULL OR UPV.ENDT_CODE IN (SELECT REGEXP_SUBSTR (V_AAN_ENDT_CODE,'[^,]+',1,LEVEL) FROM DUAL
					CONNECT BY REGEXP_SUBSTR (V_AAN_ENDT_CODE,'[^,]+',1,LEVEL) IS NOT NULL))
				AND UPCD.DLOAD_STATUS = 'P'
				AND UPCD.TPA_NAME = 'MEDIX'
				ORDER BY OPB.policy_ref ASC, UPV.VERSION_NO ASC;

		CURSOR C_TPA_MEDIX_HC
			IS
        SELECT
					TO_CHAR (UPV.ENDT_EFF_DATE, 'DD/MM/YYYY') AS ENDT_EFF_DATE,
					OPB.POLICY_REF,
					UPB.PREV_POL_NO,
					UPV.ENDT_CODE,
					UPV.VERSION_NO AS POLICY_VERSION,
					UPB.AGENT_CODE,
					DVA.NAME AS AGENT_NAME,
					(CASE
					    WHEN upb.PREV_POL_NO IS NOT NULL THEN upb.PREV_POL_NO
					    ELSE upb.PREV_POL_NO_IIMS
					 END)
					   AS prev_pol,
					(CASE
					    WHEN CP.ID_VALUE1 IS NULL THEN CP.ID_VALUE2
					    ELSE CP.ID_VALUE1
					 END)
					   AS P_NRIC_OTH,
					(CASE
					    WHEN CP.ID_TYPE1 = 'NRIC' THEN CP.ID_VALUE1
					    WHEN CP.ID_TYPE2 = 'NRIC' THEN CP.ID_VALUE2
					 END)
					   AS P_NRIC,
					REGEXP_REPLACE (
					   (CASE
					       WHEN     CP.MOBILE_NO1 IS NOT NULL
					            AND CP.MOBILE_CODE1 IS NOT NULL
					       THEN
					          CP.MOBILE_CODE1 || CP.MOBILE_NO1
					       ELSE
					          CP.MOBILE_CODE2 || CP.MOBILE_NO2
					    END),
					   '[^0-9]')
					   AS PhoneNumber,
					REPLACE (CPA.ADDRESS_LINE1, CHR (10), '') AS ADDRESS_LINE1,
					REPLACE (CPA.ADDRESS_LINE2, CHR (10), '') AS ADDRESS_LINE2,
					REPLACE (CPA.ADDRESS_LINE3, CHR (10), '') AS ADDRESS_LINE3,
					CPA.POSTCODE,
					(SELECT CODE_DESC
					   FROM CUSTOMER.CMGE_CODE
					  WHERE CAT_CODE = 'CITY' AND CODE_CD = CPA.CITY)
					   AS CITY,
					(SELECT CODE_DESC
					   FROM CUSTOMER.CMGE_CODE
					  WHERE CAT_CODE = 'STATE' AND CODE_CD = CPA.STATE)
					   AS STATE,
					UPC.PRODUCT_CONFIG_CODE,
					CP.NAME_EXT,
					UPB.LONG_NAME,
					TO_CHAR (UPB.EFF_DATE, 'DD/MM/YYYY') AS EFF_DATE,
					TO_CHAR (UPB.EXP_DATE, 'DD/MM/YYYY') AS EXP_DATE,
					TO_CHAR (UPV.ISSUE_DATE, 'DD/MM/YYYY') AS ISSUE_DATE,
					(SELECT UPF.FEE_AMT
					   FROM CUSTOMER.UWGE_POLICY_FEES UPF
					  WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
					        AND UPF.VERSION_NO = UPV.VERSION_NO
					        AND UPF.FEE_CODE = 'MCOO')
					   AS MCOO_FEE_AMT,
					(SELECT UPF.FEE_AMT
					   FROM CUSTOMER.UWGE_POLICY_FEES UPF
					  WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
					        AND UPF.VERSION_NO = UPV.VERSION_NO
					        AND UPF.FEE_CODE = 'MCOI')
					   AS MCOI_FEE_AMT,
					(SELECT UPF.FEE_AMT
					   FROM CUSTOMER.UWGE_POLICY_FEES UPF
					  WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
					        AND UPF.VERSION_NO = UPV.VERSION_NO
					        AND UPF.FEE_CODE = 'MCO')
					   AS MCO_FEE_AMT,
					(SELECT UPF.FEE_AMT
					   FROM CUSTOMER.UWGE_POLICY_FEES UPF
					  WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
					        AND UPF.VERSION_NO = UPV.VERSION_NO
					        AND UPF.FEE_CODE = 'IMA')
					   AS IMA_FEE_AMT,
					(SELECT UPF.FEE_AMT
					   FROM CUSTOMER.UWGE_POLICY_FEES UPF
					  WHERE     UPF.CONTRACT_ID = UPV.CONTRACT_ID
					        AND UPF.VERSION_NO = UPV.VERSION_NO
					        AND UPF.FEE_CODE = 'MCODMA')
					   AS DMA_FEE_AMT,
					NVL (UPV.ENDT_NARR, ' ') AS ENDT_NARR,
					UPV.ENDT_NO,
					TO_CHAR (SYSDATE, 'DD/MM/YYYY') AS DateReceivedbyAAN,
					(SELECT BRANCH_NAME
					   FROM CUSTOMER.CMDM_BRANCH
					  WHERE BRANCH_CODE = UPB.ISSUE_OFFICE)
					   AS BRANCH_DESC,
					(SELECT EXP_DATE
					   FROM CUSTOMER.uwge_policy_bases
					  WHERE     CONTRACT_ID =
					               (SELECT CONTRACT_ID
					                  FROM OCP_POLICY_BASES
					                 WHERE     policy_ref =
					                              (CASE
					                                  WHEN upb.PREV_POL_NO
					                                          IS NOT NULL
					                                  THEN
					                                     upb.PREV_POL_NO
					                                  ELSE
					                                     upb.PREV_POL_NO_IIMS
					                               END)
					                       AND ROWNUM = 1)
					        AND uwge_policy_bases.TOP_INDICATOR = 'Y'
					        AND ROWNUM = 1)
					   AS prev_exp_date,
					OPB.CONTRACT_ID
        FROM CUSTOMER.UWGE_POLICY_VERSIONS UPV
        INNER JOIN CUSTOMER.UWGE_POLICY_CTRL_DLOAD UPCD
					ON UPV.CONTRACT_ID = UPCD.CONTRACT_ID
					AND UPV.VERSION_NO = UPCD.VERSION_NO
        INNER JOIN CUSTOMER.UWGE_POLICY_CONTRACTS UPC
					ON UPV.CONTRACT_ID = UPC.CONTRACT_ID
        INNER JOIN OCP_POLICY_BASES OPB
					ON UPV.CONTRACT_ID = OPB.CONTRACT_ID
        INNER JOIN CUSTOMER.UWGE_POLICY_BASES UPB
					ON UPB.CONTRACT_ID = UPV.CONTRACT_ID
					AND UPB.VERSION_NO = UPV.VERSION_NO
        INNER JOIN CUSTOMER.UWPL_POLICY_BASES PLPB
					ON PLPB.CONTRACT_ID = UPV.CONTRACT_ID
					AND PLPB.VERSION_NO = UPV.VERSION_NO
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (UPB.CP_PART_ID, UPB.CP_VERSION)) CP
					ON CP.PART_ID = UPB.CP_PART_ID
					AND CP.VERSION = UPB.CP_VERSION
        INNER JOIN TABLE (CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_ADD_TABLE (UPB.CP_ADDR_ID, UPB.CP_ADDR_VERSION)) CPA
					ON CPA.ADD_ID = UPB.CP_ADDR_ID
					AND CPA.VERSION = UPB.CP_ADDR_VERSION
        INNER JOIN CUSTOMER.DMAG_VI_AGENT DVA ON DVA.AGENTCODE = UPB.AGENT_CODE
        WHERE UPC.PRODUCT_CONFIG_CODE IN (SELECT REGEXP_SUBSTR (
					PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_HC'), '[^,]+', 1, LEVEL) FROM DUAL
					CONNECT BY REGEXP_SUBSTR (PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'AAN_HC'), '[^,]+', 1, LEVEL) IS NOT NULL)
				AND UPC.POLICY_STATUS IN ('A', 'C', 'E')
				AND UPV.ACTION_CODE IN ('A', 'C')
				AND (   UPV.ENDT_CODE IS NULL OR UPV.ENDT_CODE IN (
					SELECT REGEXP_SUBSTR (V_AAN_ENDT_CODE, '[^,]+', 1, LEVEL) FROM DUAL
					CONNECT BY REGEXP_SUBSTR (V_AAN_ENDT_CODE, '[^,]+', 1, LEVEL) IS NOT NULL))
				AND (PLPB.TPA_NAME = 'MEDIX' OR PLPB.TPA_NAME IS NULL)
				AND UPCD.DLOAD_STATUS = 'P'
				AND UPCD.TPA_NAME = 'MEDIX'
        ORDER BY OPB.policy_ref ASC, UPV.VERSION_NO ASC;

		V_STEPS                   VARCHAR2 (10);
		V_FUNC_NAME               VARCHAR2 (100) := 'PC_TPA_MEDIX_HC_PA_POL_ENDT';
		FILENAME                  UTL_FILE.FILE_TYPE;
		FILENAME1                 VARCHAR2 (1000);
		v_file_dir                VARCHAR2 (100)
		   := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'TPA_AAN_DIR');
		V_ASST                    VARCHAR2 (100)
		                             := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'PA_ASST');
		V_IMA                     VARCHAR2 (100)
		                             := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'PA_IMA');
		V_MCO                     VARCHAR2 (100)
		                             := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'PA_MCO');
		V_NPOL                    VARCHAR2 (100)
		                             := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'POL_PA');
		V_HC_MCOI                 VARCHAR2 (100)
		                             := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'HC_MCOI');
		V_HC_MCOO                 VARCHAR2 (100)
		                             := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'HC_MCOO');
		V_HC_DMA                  VARCHAR2 (100)
		                             := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'HC_DMA');

		V_RISK_LEVEL_DTLS         VARCHAR2 (100)
		   := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'RISK_LEVEL_DTLS');
		V_IMA_LMT_2M              VARCHAR2 (100)
		   := PG_TPA_UTILS.FN_GET_SYS_PARAM ('PG_TPA', 'IMA_LMT_2M');

		V_PRINCIPAL_DET           PG_TPA_UTILS.RISK_PERSON_PARTNERS_ALL_DET;
		V_UWPL_COVER_DET          PG_TPA_UTILS.UWPL_COVER_DET;
		rowIDx                    NUMBER := 5;
		seq                       NUMBER := 1;
		v_NGV                     NUMBER (18, 2);
		V_RET                     NUMBER := 0;

		V_SELECTED_RISK_SQL       VARCHAR2 (10000)
      := 'SELECT (CASE
                     WHEN RCP.ID_VALUE1 IS NULL THEN RCP.ID_VALUE2
                     ELSE RCP.ID_VALUE1
                  END)
                    AS NRIC_OTH,
                 (CASE
                     WHEN RCP.ID_TYPE1 = ''NRIC'' THEN RCP.ID_VALUE1
                     WHEN RCP.ID_TYPE2 = ''NRIC'' THEN RCP.ID_VALUE2
                  END)
                    AS NRIC,
                 RCP.NAME_EXT AS MEMBER_FULL_NAME,
                 RCP.DATE_OF_BIRTH AS DATE_OF_BIRTH,
                 RCP.SEX,
                 (CASE
                     WHEN RCP.marital_status = ''0'' THEN ''S''
                     WHEN RCP.marital_status = ''1'' THEN ''M''
                     WHEN RCP.marital_status = ''2'' THEN ''D''
                  END)
                    AS MARITAL_STATUS,
                 URP.INSURED_TYPE,
                 URP.EMPLOYEE_ID,
                 (CASE
                     WHEN URP.INSURED_TYPE = ''P''
                     THEN
                        ''P''
                     ELSE
                        (CASE
                            WHEN URP.RELATIONSHIP IN (''03'', ''072'') THEN ''H''
                            WHEN URP.RELATIONSHIP IN (''02'', ''107'') THEN ''W''
                            WHEN URP.RELATIONSHIP IN (''05'', ''019'') THEN ''D''
                            WHEN URP.RELATIONSHIP IN (''04'', ''087'') THEN ''S''
                            ELSE ''''
                         END)
                  END)
                    AS RELATIONSHIP,
                 URP.TEMINATE_DATE,
                 UR.EFF_DATE AS RISK_EFF_DATE,
                 UR.EXP_DATE AS RISK_EXP_DATE,
                 URP.JOIN_DATE AS ORIGINAL_JOIN_DATE,
                 (CASE
                     WHEN URP.INSURED_TYPE = ''D''
                     THEN
                        (SELECT a.COV_SEQ_REF
                           FROM uwge_cover a
                          WHERE     UCOV.CONTRACT_ID = A.CONTRACT_ID
                                AND UCOV.VERSION_NO = a.VERSION_NO
                                AND a.RISK_ID = UR.RISK_PARENT_ID
                                AND COV_PARENT_ID IS NULL
                                AND ROWNUM = 1)
                     ELSE
                        ''''
                  END)
                    AS Parent_cov_seq_no,
                 UCOV.COV_ID,
                 UCOV.COV_SEQ_REF,
                 UR.RISK_ID,
                 UR.RISK_PARENT_ID,
                 (SELECT COUNT (*)
                    FROM UWGE_COVER CSUB
                   WHERE     UCOV.CONTRACT_ID = CSUB.CONTRACT_ID
                         AND UCOV.VERSION_NO = CSUB.VERSION_NO
                         AND UCOV.COV_ID = CSUB.COV_PARENT_ID
                         AND CSUB.COV_CODE IN (''OP'', ''OP1'', ''OP2''))
                    AS OP_SUB_COV,
                 (SELECT NVL (F.FEE_AMT, 0)
                    FROM UWGE_COVER_FEES F
                   WHERE     F.CONTRACT_ID = UR.CONTRACT_ID
                         AND F.COV_ID = UCOV.COV_ID
                         AND TOP_INDICATOR = ''Y''
                         AND F.FEE_CODE = ''MCO'')
                    AS MCO_FEE,
                 (SELECT NVL (F.FEE_AMT, 0)
                    FROM UWGE_COVER_FEES F
                   WHERE     F.CONTRACT_ID = UR.CONTRACT_ID
                         AND F.COV_ID =
                                (SELECT CV.COV_ID
                                   FROM UWGE_COVER CV
                                  WHERE     CV.COV_PARENT_ID = UCOV.COV_ID
                                        AND TOP_INDICATOR = ''Y''
                                        AND COV_CODE = ''IMA''
                                        AND ROWNUM = 1)
                         AND TOP_INDICATOR = ''Y''
                         AND F.FEE_CODE = ''IMA''
                         AND ROWNUM = 1)
                    AS IMA_FEE,
                 null as import_type,
                 null as prev_pol_op_ind,
                 urp.department,
								 RCP.EMAIL AS EMAIL,
								 (CASE
                     WHEN RCP.ID_TYPE1 IS NOT NULL THEN RCP.ID_TYPE1
                     ELSE RCP.ID_TYPE2
                  END) AS ID_TYPE
            FROM UWGE_RISK UR
                 INNER JOIN
                 UWPL_RISK_PERSON URP
                    ON     URP.CONTRACT_ID = UR.CONTRACT_ID
                       AND UR.RISK_ID = URP.RISK_ID
                       AND URP.VERSION_NO = UR.VERSION_NO
                       AND URP.action_code <> ''D''
                 INNER JOIN
                 TABLE (
                    CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (URP.RISK_PART_ID,
                                                              URP.RISK_PART_VER)) RCP
                    ON     RCP.PART_ID = URP.RISK_PART_ID
                       AND RCP.VERSION = URP.RISK_PART_VER
                 INNER JOIN
                 UWGE_COVER UCOV
                    ON     UCOV.CONTRACT_ID = UR.CONTRACT_ID
                       AND UR.RISK_ID = UCOV.RISK_ID
                       AND UCOV.VERSION_NO = UR.VERSION_NO
                       AND UCOV.COV_PARENT_ID IS NULL
           WHERE     UR.CONTRACT_ID = :BIND_CONTRACT_ID
                 AND UR.version_no = :BIND_VERSION_NO
                 AND UR.action_code <> ''D''
        ORDER BY urp.department, TO_NUMBER (UCOV.cov_seq_ref)';

		V_ALL_RISK_SQL            VARCHAR2 (10000)
      := 'SELECT (CASE
                 WHEN RCP.ID_VALUE1 IS NULL THEN RCP.ID_VALUE2
                 ELSE RCP.ID_VALUE1
              END)
                AS NRIC_OTH,
             (CASE
                 WHEN RCP.ID_TYPE1 = ''NRIC'' THEN RCP.ID_VALUE1
                 WHEN RCP.ID_TYPE2 = ''NRIC'' THEN RCP.ID_VALUE2
              END)
                AS NRIC,
             RCP.NAME_EXT AS MEMBER_FULL_NAME,
             RCP.DATE_OF_BIRTH AS DATE_OF_BIRTH,
             RCP.SEX,
             (CASE
                 WHEN RCP.marital_status = ''0'' THEN ''S''
                 WHEN RCP.marital_status = ''1'' THEN ''M''
                 WHEN RCP.marital_status = ''2'' THEN ''D''
              END)
                AS MARITAL_STATUS,
             URP.INSURED_TYPE,
             URP.EMPLOYEE_ID,
             (CASE
                 WHEN URP.INSURED_TYPE = ''P''
                 THEN
                    ''P''
                 ELSE
                    (CASE
                        WHEN URP.RELATIONSHIP IN (''03'', ''072'') THEN ''H''
                        WHEN URP.RELATIONSHIP IN (''02'', ''107'') THEN ''W''
                        WHEN URP.RELATIONSHIP IN (''05'', ''019'') THEN ''D''
                        WHEN URP.RELATIONSHIP IN (''04'', ''087'') THEN ''S''
                        ELSE ''''
                     END)
              END)
                AS RELATIONSHIP,
             URP.TEMINATE_DATE,
             UR.EFF_DATE AS RISK_EFF_DATE,
             UR.EXP_DATE AS RISK_EXP_DATE,
             URP.JOIN_DATE AS ORIGINAL_JOIN_DATE,
             (CASE
                 WHEN URP.INSURED_TYPE = ''D''
                 THEN
                    (SELECT a.COV_SEQ_REF
                       FROM uwge_cover a
                      WHERE     UCOV.CONTRACT_ID = A.CONTRACT_ID
                            AND UCOV.VERSION_NO = a.VERSION_NO
                            AND a.RISK_ID = UR.RISK_PARENT_ID
                            AND COV_PARENT_ID IS NULL
                            AND ROWNUM = 1)
                 ELSE
                    ''''
              END)
                AS Parent_cov_seq_no,
             UCOV.COV_ID,
             UCOV.COV_SEQ_REF,
             UR.RISK_ID,
             UR.RISK_PARENT_ID,
             (SELECT COUNT (*)
                FROM UWGE_COVER CSUB
               WHERE     UCOV.CONTRACT_ID = CSUB.CONTRACT_ID
                     AND UCOV.VERSION_NO = CSUB.VERSION_NO
                     AND UCOV.COV_ID = CSUB.COV_PARENT_ID
                     AND CSUB.COV_CODE IN (''OP'', ''OP1'', ''OP2''))
                AS OP_SUB_COV,
             NVL((SELECT NVL (F.FEE_AMT, 0)
                FROM UWGE_COVER_FEES F
               WHERE     F.CONTRACT_ID = UR.CONTRACT_ID
                     AND F.COV_ID = UCOV.COV_ID
                     AND TOP_INDICATOR = ''Y''
                     AND F.FEE_CODE = ''MCO''), 0)
                AS MCO_FEE,
             NVL((SELECT NVL (F.FEE_AMT, 0)
                FROM UWGE_COVER_FEES F
               WHERE     F.CONTRACT_ID = UR.CONTRACT_ID
                     AND F.COV_ID =
                            (SELECT CV.COV_ID
                               FROM UWGE_COVER CV
                              WHERE     CV.COV_PARENT_ID = UCOV.COV_ID
                                    AND TOP_INDICATOR = ''Y''
                                    AND COV_CODE = ''IMA''
                                    AND ROWNUM = 1)
                     AND TOP_INDICATOR = ''Y''
                     AND F.FEE_CODE = ''IMA''
                     AND ROWNUM = 1), 0)
                AS IMA_FEE,
             NULL AS import_type,
             NULL AS prev_pol_op_ind,
             urp.department,
						 RCP.EMAIL AS EMAIL,
						 (CASE
								 WHEN RCP.ID_TYPE1 IS NOT NULL THEN RCP.ID_TYPE1
								 ELSE RCP.ID_TYPE2
							END) AS ID_TYPE
        FROM UWGE_RISK UR
             INNER JOIN
             UWPL_RISK_PERSON URP
                ON     URP.CONTRACT_ID = UR.CONTRACT_ID
                   AND UR.RISK_ID = URP.RISK_ID
                   AND URP.VERSION_NO =
                          (SELECT MAX (b.version_no)
                             FROM UWPL_RISK_PERSON b
                            WHERE     b.contract_id = UR.CONTRACT_ID
                                  AND URP.object_id = b.object_id
                                  AND b.version_no <= :BIND_VERSION_NO
                                  AND b.reversing_version IS NULL)
                   AND URP.action_code <> ''D''
             INNER JOIN
             TABLE (
                CUSTOMER.PG_CP_GEN_TABLE.FN_GEN_CP_TABLE (URP.RISK_PART_ID,
                                                          URP.RISK_PART_VER)) RCP
                ON     RCP.PART_ID = URP.RISK_PART_ID
                   AND RCP.VERSION = URP.RISK_PART_VER
             INNER JOIN
             UWGE_COVER UCOV
                ON     UCOV.CONTRACT_ID = UR.CONTRACT_ID
                   AND UR.RISK_ID = UCOV.RISK_ID
                   AND UCOV.VERSION_NO = :BIND_VERSION_NO_1
                   AND UCOV.COV_PARENT_ID IS NULL
       WHERE     UR.CONTRACT_ID = :BIND_CONTRACT_ID
             AND UR.version_no =
                    (SELECT MAX (c.version_no)
                       FROM UWGE_RISK c
                      WHERE     c.contract_id = :BIND_CONTRACT_ID_1
                            AND UR.object_id = c.object_id
                            AND c.version_no <= :BIND_VERSION_NO_2
                            AND c.reversing_version IS NULL)
             AND UR.action_code <> ''D''
    ORDER BY urp.department, TO_NUMBER (UCOV.cov_seq_ref)';

		V_SELECTED_RISK_SQL_TPA   VARCHAR2 (10000)
      := 'SELECT (CASE
               WHEN rcp.id_value1 IS NULL THEN rcp.id_value2
               ELSE rcp.id_value1
            END)
              AS nric_oth,
           (CASE
               WHEN rcp.id_type1 = ''NRIC'' THEN rcp.id_value1
               WHEN rcp.id_type2 = ''NRIC'' THEN rcp.id_value2
            END)
              AS nric,
           rcp.name_ext AS member_full_name,
           rcp.date_of_birth AS date_of_birth,
           rcp.sex,
           (CASE
               WHEN rcp.marital_status = ''0'' THEN ''S''
               WHEN rcp.marital_status = ''1'' THEN ''M''
               WHEN rcp.marital_status = ''2'' THEN ''D''
            END)
              AS marital_status,
           urp.insured_type,
           urp.employee_id,
           (CASE
               WHEN urp.insured_type = ''P''
               THEN
                  ''P''
               ELSE
                  (CASE
                      WHEN urp.relationship IN (''03'', ''072'') THEN ''H''
                      WHEN urp.relationship IN (''02'', ''107'') THEN ''W''
                      WHEN urp.relationship IN (''05'', ''019'') THEN ''D''
                      WHEN urp.relationship IN (''04'', ''087'') THEN ''S''
                      ELSE ''''
                   END)
            END)
              AS relationship,
           urp.teminate_date,
           ur.eff_date AS risk_eff_date,
           ur.exp_date AS risk_exp_date,
           urp.join_date AS original_join_date,
           (CASE
               WHEN urp.insured_type = ''D''
               THEN
                  (SELECT a.cov_seq_ref
                     FROM uwge_cover a
                    WHERE     ucov.contract_id = a.contract_id
                          AND ucov.version_no = a.version_no
                          AND a.risk_id = ur.risk_parent_id
                          AND cov_parent_id IS NULL
                          AND ROWNUM = 1)
               ELSE
                  ''''
            END)
              AS parent_cov_seq_no,
           ucov.cov_id,
           ucov.cov_seq_ref,
           ur.risk_id,
           ur.risk_parent_id,
           (SELECT COUNT (*)
              FROM uwge_cover csub
             WHERE     ucov.contract_id = csub.contract_id
                   AND ucov.version_no = csub.version_no
                   AND ucov.cov_id = csub.cov_parent_id
                   AND csub.cov_code IN (''OP'', ''OP1'', ''OP2''))
              AS op_sub_cov,
           (SELECT NVL (f.fee_amt, 0)
              FROM uwge_cover_fees f
             WHERE     f.contract_id = ur.contract_id
                   AND f.cov_id = ucov.cov_id
                   AND top_indicator = ''Y''
                   AND f.fee_code = ''MCO'')
              AS mco_fee,
           (SELECT NVL (f.fee_amt, 0)
              FROM uwge_cover_fees f
             WHERE     f.contract_id = ur.contract_id
                   AND f.cov_id =
                          (SELECT CV.cov_id
                             FROM uwge_cover CV
                            WHERE     CV.cov_parent_id = ucov.cov_id
                                  AND top_indicator = ''Y''
                                  AND cov_code = ''IMA''
                                  AND ROWNUM = 1)
                   AND top_indicator = ''Y''
                   AND f.fee_code = ''IMA''
                   AND ROWNUM = 1)
              AS ima_fee,
           tpa.import_type,
           tpa.prev_pol_op_ind,
           urp.department,
					 rcp.EMAIL AS EMAIL,
           (CASE
           		 WHEN rcp.ID_TYPE1 IS NOT NULL THEN rcp.ID_TYPE1
           		 ELSE rcp.ID_TYPE2
           	END) AS ID_TYPE
      FROM uwge_risk_tpa_download tpa
           INNER JOIN
           uwge_cover ucov
              ON     ucov.contract_id = tpa.contract_id
                 AND ucov.risk_id = tpa.risk_id
                 AND ucov.version_no = tpa.version_no
                 AND ucov.cov_parent_id IS NULL
           LEFT OUTER JOIN
           uwge_risk ur
              ON     ur.contract_id = tpa.contract_id
                 AND ur.risk_id = tpa.risk_id
                 AND ur.action_code <> ''D''
                 AND ur.version_no =
                        (SELECT MAX (version_no)
                           FROM uwge_risk ur2
                          WHERE     ur2.contract_id = ur.contract_id
                                AND ur2.object_id = ur.object_id
                                AND ur2.version_no <= tpa.version_no
                                AND ur2.reversing_version IS NULL)
           LEFT OUTER JOIN
           uwpl_risk_person urp
              ON     urp.contract_id = tpa.contract_id
                 AND urp.risk_id = tpa.risk_id
                 AND urp.action_code <> ''D''
                 AND urp.version_no =
                        (SELECT MAX (version_no)
                           FROM uwpl_risk_person urp2
                          WHERE     urp2.contract_id = urp.contract_id
                                AND urp2.object_id = urp.object_id
                                AND urp2.version_no <= tpa.version_no
                                AND urp2.reversing_version IS NULL)
           INNER JOIN
           TABLE (
              customer.pg_cp_gen_table.fn_gen_cp_table (urp.risk_part_id,
                                                        urp.risk_part_ver)) rcp
              ON     rcp.part_id = urp.risk_part_id
                 AND rcp.version = urp.risk_part_ver
     WHERE     tpa.contract_id = :bind_contract_id
           AND tpa.version_no = :bind_version_no
           AND tpa.tpa_name = ''MEDIX''
      ORDER BY urp.department, TO_NUMBER (ucov.cov_seq_ref)';

		RISK_DET                  PG_TPA_UTILS.MEDIX_PA_HC_RISK_DET_TBL;
		V_ROW_NUM                 NUMBER (5);
		V_ENDT_NARR_ARRAY         PG_TPA_UTILS.p_array_v;
		V_COUNT_TPA_RISK          NUMBER (5);

	BEGIN
		--        --dbms_output.put_line (
		--                  'P_START_DT :  ' || P_START_DT);
		V_STEPS := '001';
		FILENAME1 :=
		      TO_CHAR (P_START_DT, 'YYYYMMDD')
		   || '_HC'
		   || CHR (38)
		   || 'PA_POLEND_MEDIX.xlsx';
		V_STEPS := '002';
		PG_EXCEL_UTILS.clear_workbook;
		PG_EXCEL_UTILS.new_sheet;
		PG_EXCEL_UTILS.CELL (1, 1, 'BORDEREAUX (POLICY &' || ' ENDORSEMENT)');
		V_STEPS := '003';
		PG_EXCEL_UTILS.MERGECELLS (1,
		                           1,
		                           3,
		                           1);
		PG_EXCEL_UTILS.CELL (
		   1,
		   2,
		   'FROM : ALLIANZ GENERAL INSURANCE COMPANY (MALAYSIA) BERHAD');
		PG_EXCEL_UTILS.MERGECELLS (1,
		                           2,
		                           3,
		                           2);
		PG_EXCEL_UTILS.CELL (1, 3, 'DATE :');
		PG_EXCEL_UTILS.CELL (2, 3, TO_CHAR (P_START_DT, 'DD/MM/YYYY'));
		V_STEPS := '004';
		PG_EXCEL_UTILS.SET_ROW (
		   4,
		   p_fontId   => PG_EXCEL_UTILS.get_font ('Arial', p_bold => TRUE));
		PG_EXCEL_UTILS.CELL (1, 4, 'No.');
		PG_EXCEL_UTILS.CELL (2, 4, 'Import Type');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (2, 20);
		PG_EXCEL_UTILS.CELL (3, 4, 'Member Full Name');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (3, 40);
		PG_EXCEL_UTILS.CELL (4, 4, 'Address 1');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (4, 20);
		PG_EXCEL_UTILS.CELL (5, 4, 'Address 2');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (5, 20);
		PG_EXCEL_UTILS.CELL (6, 4, 'Address 3');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (6, 40);
		PG_EXCEL_UTILS.CELL (7, 4, 'Address 4');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (7, 20);
		PG_EXCEL_UTILS.CELL (8, 4, 'Gender');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (8, 20);
		V_STEPS := '005';
		PG_EXCEL_UTILS.CELL (9, 4, 'DOB');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (9, 20);
		PG_EXCEL_UTILS.CELL (10, 4, 'NRIC');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (10, 20);
		PG_EXCEL_UTILS.CELL (11, 4, 'Other IC');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (11, 20);
		PG_EXCEL_UTILS.CELL (12, 4, 'External Ref Id (aka Client)');
		V_STEPS := '006';
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (12, 20);
		PG_EXCEL_UTILS.CELL (13, 4, 'Internal Ref Id (aka AAN)');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (13, 20);
		PG_EXCEL_UTILS.CELL (14, 4, 'Employee ID');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (14, 20);
		PG_EXCEL_UTILS.CELL (15, 4, 'Marital Status');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (15, 20);
		PG_EXCEL_UTILS.CELL (16, 4, 'Race');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (16, 20);
		PG_EXCEL_UTILS.CELL (17, 4, 'Phone');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (17, 20);
		PG_EXCEL_UTILS.CELL (18, 4, 'VIP');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (18, 20);
		PG_EXCEL_UTILS.CELL (19, 4, 'Special Condition');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (19, 20);
		PG_EXCEL_UTILS.CELL (20, 4, 'Relationship');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (20, 20);
		PG_EXCEL_UTILS.CELL (21, 4, 'Principal Int Ref Id (aka AAN)');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (21, 20);
		PG_EXCEL_UTILS.CELL (22, 4, 'Principal Ext Ref Id (aka Client)');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (22, 20);
		PG_EXCEL_UTILS.CELL (23, 4, 'Principal Name');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (23, 20);
		PG_EXCEL_UTILS.CELL (24, 4, 'Principal NRIC');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (24, 20);
		PG_EXCEL_UTILS.CELL (25, 4, 'Principal Other Ic');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (25, 20);
		PG_EXCEL_UTILS.CELL (26, 4, 'Program Id');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (26, 20);
		PG_EXCEL_UTILS.CELL (27, 4, 'Policy Type');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (27, 20);
		PG_EXCEL_UTILS.CELL (28, 4, 'Policy Num');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (28, 20);
		PG_EXCEL_UTILS.CELL (29, 4, 'Policy Eff Date');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (29, 20);
		PG_EXCEL_UTILS.CELL (30, 4, 'Policy Expiry Date');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (30, 20);
		PG_EXCEL_UTILS.CELL (31, 4, 'Previous Policy Num');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (31, 20);
		PG_EXCEL_UTILS.CELL (32, 4, 'Previous Policy End Date');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (32, 20);
		PG_EXCEL_UTILS.CELL (33, 4, 'Customer Owner Name');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (33, 20);
		PG_EXCEL_UTILS.CELL (34, 4, 'External Plan Code');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (34, 20);
		PG_EXCEL_UTILS.CELL (35, 4, 'Internal Plan Code Id');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (35, 20);
		PG_EXCEL_UTILS.CELL (36, 4, 'Original Join Date');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (36, 20);
		PG_EXCEL_UTILS.CELL (37, 4, 'Plan Attach Date');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (37, 20);
		PG_EXCEL_UTILS.CELL (38, 4, 'Plan Expiry Date');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (38, 20);
		PG_EXCEL_UTILS.CELL (39, 4, 'Subsidiary Name');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (39, 20);
		PG_EXCEL_UTILS.CELL (40, 4, 'Agent Name');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (40, 20);
		PG_EXCEL_UTILS.CELL (41, 4, 'Agent Code');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (41, 20);
		PG_EXCEL_UTILS.CELL (42, 4, 'Insurer MCO Fees');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (42, 20);
		PG_EXCEL_UTILS.CELL (43, 4, 'IMA Service?');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (43, 20);
		PG_EXCEL_UTILS.CELL (44, 4, 'IMA Limit');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (44, 20);
		PG_EXCEL_UTILS.CELL (45, 4, 'Date Received by AAN');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (45, 20);
		PG_EXCEL_UTILS.CELL (46, 4, 'Termination Date');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (46, 20);
		PG_EXCEL_UTILS.CELL (47, 4, 'Free text remark');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (47, 20);
		PG_EXCEL_UTILS.CELL (48, 4, 'Questionnaire');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (48, 20);
		PG_EXCEL_UTILS.CELL (49, 4, 'Plan-Remarks');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (49, 20);
		PG_EXCEL_UTILS.CELL (50, 4, 'Diagnosis');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (50, 20);
		PG_EXCEL_UTILS.CELL (51, 4, 'Outpatient Subcover');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (51, 20);
		PG_EXCEL_UTILS.CELL (52, 4, 'Previous Outpatient Subcover');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (52, 20);
		PG_EXCEL_UTILS.CELL (53, 4, 'Department');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (53, 20);
		PG_EXCEL_UTILS.CELL (54, 4, 'Email Address');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (54, 20);
		PG_EXCEL_UTILS.CELL (55, 4, 'ID Type');
		PG_EXCEL_UTILS.SET_COLUMN_WIDTH (55, 20);
		DBMS_OUTPUT.ENABLE (buffer_size => NULL);

		FOR REC IN C_TPA_MEDIX_PA
		LOOP
      V_STEPS := '007AA_01';

      IF (   (    REC.POLICY_VERSION = 1
              AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                     V_NPOL,
                     REC.PRODUCT_CONFIG_CODE) = 'N')
          OR REC.POLICY_VERSION > 1)
      THEN
         IF    (    REC.POLICY_VERSION = 1
                AND REC.ASST_FEE_AMT > 0
                AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                       V_ASST,
                       REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (    REC.POLICY_VERSION > 1
                AND REC.ASST_FEE_AMT >= 0
                AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                       V_ASST,
                       REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (    REC.POLICY_VERSION = 1
                AND REC.IMA_FEE_AMT > 0
                AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                       V_IMA,
                       REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (    REC.POLICY_VERSION > 1
                AND REC.IMA_FEE_AMT >= 0
                AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                       V_IMA,
                       REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (    REC.POLICY_VERSION = 1
                AND REC.MCO_FEE_AMT > 0
                AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                       V_MCO,
                       REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (    REC.POLICY_VERSION > 1
                AND REC.MCO_FEE_AMT >= 0
                AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                       V_MCO,
                       REC.PRODUCT_CONFIG_CODE) = 'Y')
            OR (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_RISK_LEVEL_DTLS,
                                                    REC.PRODUCT_CONFIG_CODE) =
                   'Y')
         THEN

						V_STEPS := '007AA_02';

            RISK_DET.DELETE;

            SELECT COUNT (1)
              INTO V_COUNT_TPA_RISK
              FROM UWGE_RISK_TPA_DOWNLOAD TPA
             WHERE     TPA.CONTRACT_ID = REC.CONTRACT_ID
                   AND TPA.VERSION_NO = REC.POLICY_VERSION;

            IF V_COUNT_TPA_RISK > 0
            THEN
							 V_STEPS := '007AA_03';
               BEGIN
                  EXECUTE IMMEDIATE V_SELECTED_RISK_SQL_TPA
                     BULK COLLECT INTO RISK_DET
                     USING REC.CONTRACT_ID, REC.POLICY_VERSION;
               END;
            ELSE
							 V_STEPS := '007AA_04';
               IF     REC.ENDT_CODE IS NOT NULL
                  AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_AAN_ENDT_CODE_R,
                                                          REC.ENDT_CODE) =
                         'Y'
               THEN
									V_STEPS := '007AA_05';
                  BEGIN
                     EXECUTE IMMEDIATE V_SELECTED_RISK_SQL
                        BULK COLLECT INTO RISK_DET
                        USING REC.CONTRACT_ID, REC.POLICY_VERSION;
                  END;
               ELSE
									V_STEPS := '007AA_06';
                  BEGIN
                     EXECUTE IMMEDIATE V_ALL_RISK_SQL
                        BULK COLLECT INTO RISK_DET
                        USING REC.POLICY_VERSION,
                              REC.POLICY_VERSION,
                              REC.CONTRACT_ID,
                              REC.CONTRACT_ID,
                              REC.POLICY_VERSION;
                  END;
               END IF;
            END IF;

            V_ROW_NUM := 0;

            V_STEPS := '007AA_07';
						FOR V_ROW_NUM IN 1 .. RISK_DET.COUNT
            LOOP
               IF (   PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                         V_RISK_LEVEL_DTLS,
                         REC.PRODUCT_CONFIG_CODE) = 'N'
                   OR (    PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                              V_RISK_LEVEL_DTLS,
                              REC.PRODUCT_CONFIG_CODE) = 'Y'
                       AND (   NVL (RISK_DET (V_ROW_NUM).IMA_FEE, 0) > 0
                            OR NVL (RISK_DET (V_ROW_NUM).MCO_FEE, 0) > 0)
                       AND REC.POLICY_VERSION = 1)
                   OR (    PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                              V_RISK_LEVEL_DTLS,
                              REC.PRODUCT_CONFIG_CODE) = 'Y'
                       AND REC.POLICY_VERSION > 1))
               THEN

                  PG_EXCEL_UTILS.CELL (1, rowIDx, seq);

                  IF RISK_DET (V_ROW_NUM).IMPORT_TYPE IS NOT NULL
                  THEN
                     PG_EXCEL_UTILS.CELL (2,
                                          rowIDx,
                                          RISK_DET (V_ROW_NUM).IMPORT_TYPE);
                  ELSE
                     IF     REC.POLICY_VERSION = 1
                        AND REC.PREV_POL_NO IS NOT NULL
                     THEN
                        PG_EXCEL_UTILS.CELL (2, rowIDx, 'R');
                     ELSIF REC.POLICY_VERSION = 1 AND REC.PREV_POL_NO IS NULL
                     THEN
                        PG_EXCEL_UTILS.CELL (2, rowIDx, 'N');
                     ELSIF     REC.POLICY_VERSION > 1
                           AND REC.ENDT_CODE IN ('96', '97')
                     THEN
                        PG_EXCEL_UTILS.CELL (2, rowIDx, 'X');
                     ELSE
                        PG_EXCEL_UTILS.CELL (2, rowIDx, 'E');
                     END IF;
                  END IF;

                  V_STEPS := '007A';
                  PG_EXCEL_UTILS.CELL (
                     3,
                     rowIDx,
                     NVL (RISK_DET (V_ROW_NUM).MEMBER_FULL_NAME, ' '));
                  PG_EXCEL_UTILS.CELL (
                     4,
                     rowIDx,
                        NVL (REC.ADDRESS_LINE1, ' ')
                     || NVL (REC.ADDRESS_LINE2, ' '));
                  PG_EXCEL_UTILS.CELL (5,
                                       rowIDx,
                                       NVL (REC.ADDRESS_LINE3, ' '));
                  PG_EXCEL_UTILS.CELL (
                     6,
                     rowIDx,
                     NVL (REC.POSTCODE, ' ') || ' ' || NVL (REC.CITY, ' '));
                  PG_EXCEL_UTILS.CELL (7, rowIDx, NVL (REC.STATE, ' '));
                  PG_EXCEL_UTILS.CELL (8,
                                       rowIDx,
                                       NVL (RISK_DET (V_ROW_NUM).SEX, ' '));


                  IF RISK_DET (V_ROW_NUM).DATE_OF_BIRTH IS NULL
                  THEN
                     PG_EXCEL_UTILS.CELL (9, rowIDx, ' ');
                  ELSE
                     PG_EXCEL_UTILS.CELL (
                        9,
                        rowIDx,
                        TO_CHAR (RISK_DET (V_ROW_NUM).DATE_OF_BIRTH,
                                 'DD/MM/YYYY'));
                  END IF;

                  PG_EXCEL_UTILS.CELL (10,
                                       rowIDx,
                                       NVL (RISK_DET (V_ROW_NUM).NRIC, ' '));

                  IF RISK_DET (V_ROW_NUM).NRIC IS NULL
                  THEN
                     PG_EXCEL_UTILS.CELL (
                        11,
                        rowIDx,
                        NVL (RISK_DET (V_ROW_NUM).NRIC_OTH, ' '));
                  ELSE
                     PG_EXCEL_UTILS.CELL (11, rowIDx, ' ');
                  END IF;

                  PG_EXCEL_UTILS.CELL (
                     12,
                     rowIDx,
                        RISK_DET (V_ROW_NUM).RISK_ID
                     || '-'
                     || REC.POLICY_REF
                     || '-'
                     || RISK_DET (V_ROW_NUM).COV_SEQ_REF);
                  PG_EXCEL_UTILS.CELL (13, rowIDx, ' ');
                  PG_EXCEL_UTILS.CELL (
                     14,
                     rowIDx,
                     NVL (RISK_DET (V_ROW_NUM).EMPLOYEE_ID, ' '));
                  PG_EXCEL_UTILS.CELL (
                     15,
                     rowIDx,
                     NVL (RISK_DET (V_ROW_NUM).MARITAL_STATUS, ' '));
                  PG_EXCEL_UTILS.CELL (16, rowIDx, ' ');
                  PG_EXCEL_UTILS.CELL (17,
                                       rowIDx,
                                       NVL (REC.PhoneNumber, ' '));
                  PG_EXCEL_UTILS.CELL (18, rowIDx, ' ');
                  PG_EXCEL_UTILS.CELL (19, rowIDx, ' ');
                  PG_EXCEL_UTILS.CELL (
                     20,
                     rowIDx,
                     NVL (RISK_DET (V_ROW_NUM).RELATIONSHIP, ' '));
                  PG_EXCEL_UTILS.CELL (21, rowIDx, ' ');


                  IF RISK_DET (V_ROW_NUM).INSURED_TYPE = 'P'
                  THEN
                     PG_EXCEL_UTILS.CELL (
                        22,
                        rowIDx,
                           RISK_DET (V_ROW_NUM).RISK_ID
                        || '-'
                        || REC.POLICY_REF
                        || '-'
                        || RISK_DET (V_ROW_NUM).COV_SEQ_REF);
                     PG_EXCEL_UTILS.CELL (
                        23,
                        rowIDx,
                        NVL (RISK_DET (V_ROW_NUM).MEMBER_FULL_NAME, ' '));
                     PG_EXCEL_UTILS.CELL (
                        24,
                        rowIDx,
                        NVL (RISK_DET (V_ROW_NUM).NRIC, ' '));

                     IF RISK_DET (V_ROW_NUM).NRIC IS NULL
                     THEN
                        PG_EXCEL_UTILS.CELL (
                           25,
                           rowIDx,
                           NVL (RISK_DET (V_ROW_NUM).NRIC_OTH, ' '));
                     ELSE
                        PG_EXCEL_UTILS.CELL (25, rowIDx, ' ');
                     END IF;
                  ELSE
                     PG_EXCEL_UTILS.CELL (
                        22,
                        rowIDx,
                           RISK_DET (V_ROW_NUM).RISK_PARENT_ID
                        || '-'
                        || REC.POLICY_REF
                        || '-'
                        || RISK_DET (V_ROW_NUM).Parent_cov_seq_no);
                     V_PRINCIPAL_DET :=
                        PG_TPA_UTILS.FN_GET_PRINCIPAL_DET (
                           REC.CONTRACT_ID,
                           REC.POLICY_VERSION,
                           RISK_DET (V_ROW_NUM).RISK_PARENT_ID);
                     PG_EXCEL_UTILS.CELL (
                        23,
                        rowIDx,
                        NVL (V_PRINCIPAL_DET.MEMBER_FULL_NAME, ' '));
                     PG_EXCEL_UTILS.CELL (24,
                                          rowIDx,
                                          NVL (V_PRINCIPAL_DET.NRIC, ' '));

                     IF V_PRINCIPAL_DET.NRIC IS NULL
                     THEN
                        PG_EXCEL_UTILS.CELL (
                           25,
                           rowIDx,
                           NVL (V_PRINCIPAL_DET.NRIC_OTH, ' '));
                     ELSE
                        PG_EXCEL_UTILS.CELL (25, rowIDx, ' ');
                     END IF;
                  END IF;

                  PG_EXCEL_UTILS.CELL (26, rowIDx, ' ');
                  PG_EXCEL_UTILS.CELL (27, rowIDx, 'IG');
                  PG_EXCEL_UTILS.CELL (28, rowIDx, NVL (REC.POLICY_REF, ' '));
                  PG_EXCEL_UTILS.CELL (29, rowIDx, REC.EFF_DATE);
                  PG_EXCEL_UTILS.CELL (30, rowIDx, REC.EXP_DATE);
                  PG_EXCEL_UTILS.CELL (31, rowIDx, NVL (REC.prev_pol, ' '));
                  PG_EXCEL_UTILS.CELL (32, rowIDx, REC.prev_exp_date);
                  PG_EXCEL_UTILS.CELL (33, rowIDx, NVL (REC.NAME_EXT, ' '));
                  V_UWPL_COVER_DET :=
                     PG_TPA_UTILS.FN_GET_UWPL_COVER_DET (
                        REC.CONTRACT_ID,
                        REC.POLICY_VERSION,
                        RISK_DET (V_ROW_NUM).COV_ID);
                  PG_EXCEL_UTILS.CELL (34,
                                       rowIDx,
                                       NVL (V_UWPL_COVER_DET.PLAN_CODE, ' '));
                  PG_EXCEL_UTILS.CELL (35, rowIDx, ' ');

                  IF RISK_DET (V_ROW_NUM).ORIGINAL_JOIN_DATE IS NULL
                  THEN
                     PG_EXCEL_UTILS.CELL (36, rowIDx, ' ');
                  ELSE
                     PG_EXCEL_UTILS.CELL (
                        36,
                        rowIDx,
                        TO_CHAR (RISK_DET (V_ROW_NUM).ORIGINAL_JOIN_DATE,
                                 'DD/MM/YYYY'));
                  END IF;

                  PG_EXCEL_UTILS.CELL (37,
                                       rowIDx,
                                       RISK_DET (V_ROW_NUM).RISK_EFF_DATE);
                  PG_EXCEL_UTILS.CELL (38,
                                       rowIDx,
                                       RISK_DET (V_ROW_NUM).RISK_EXP_DATE);
                  PG_EXCEL_UTILS.CELL (39, rowIDx, REC.BRANCH_DESC);
                  PG_EXCEL_UTILS.CELL (40, rowIDx, NVL (REC.AGENT_NAME, ' '));
                  PG_EXCEL_UTILS.CELL (41, rowIDx, NVL (REC.AGENT_CODE, ' '));

                  IF (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                         V_RISK_LEVEL_DTLS,
                         REC.PRODUCT_CONFIG_CODE) = 'Y')
                  THEN
                     PG_EXCEL_UTILS.CELL (
                        42,
                        rowIDx,
                        NVL (RISK_DET (V_ROW_NUM).MCO_FEE, 0));

                     IF NVL (RISK_DET (V_ROW_NUM).IMA_FEE, 0) > 0
                     THEN
                        PG_EXCEL_UTILS.CELL (
                           43,
                           rowIDx,
                           'Y',
                           p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                              p_vertical     => 'center',
                                              p_horizontal   => 'center',
                                              p_wrapText     => TRUE));
                     ELSE
                        PG_EXCEL_UTILS.CELL (
                           43,
                           rowIDx,
                           'N',
                           p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                              p_vertical     => 'center',
                                              p_horizontal   => 'center',
                                              p_wrapText     => TRUE));
                     END IF;

                     IF NVL (RISK_DET (V_ROW_NUM).IMA_FEE, 0) > 0
                     THEN
                        IF (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                               V_IMA_LMT_2M,
                               REC.PRODUCT_CONFIG_CODE) = 'Y')
                        THEN
                           PG_EXCEL_UTILS.CELL (44, rowIDx, '2000000');
                        ELSE
                           PG_EXCEL_UTILS.CELL (44, rowIDx, '1000000');
                        END IF;
                     ELSE
                        PG_EXCEL_UTILS.CELL (44, rowIDx, ' ');
                     END IF;
                  ELSE

                     PG_EXCEL_UTILS.CELL (
                        42,
                        rowIDx,
                          NVL (REC.MCO_FEE_AMT, 0)
                        + NVL (REC.MCOI_FEE_AMT, 0)
                        + NVL (REC.MCOO_FEE_AMT, 0));

                     IF REC.IMA_FEE_AMT > 0
                     THEN
                        PG_EXCEL_UTILS.CELL (
                           43,
                           rowIDx,
                           'Y',
                           p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                              p_vertical     => 'center',
                                              p_horizontal   => 'center',
                                              p_wrapText     => TRUE));
                     ELSE
                        PG_EXCEL_UTILS.CELL (
                           43,
                           rowIDx,
                           'N',
                           p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                              p_vertical     => 'center',
                                              p_horizontal   => 'center',
                                              p_wrapText     => TRUE));
                     END IF;

                     IF REC.IMA_FEE_AMT > 0
                     THEN

                        IF (PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (
                               V_IMA_LMT_2M,
                               REC.PRODUCT_CONFIG_CODE) = 'Y')
                        THEN
                           PG_EXCEL_UTILS.CELL (44, rowIDx, '2000000');
                        ELSE

                           PG_EXCEL_UTILS.CELL (44, rowIDx, '1000000');
                        END IF;                   
                     ELSE
                        PG_EXCEL_UTILS.CELL (44, rowIDx, ' ');
                     END IF;
                  END IF;                         

                  PG_EXCEL_UTILS.CELL (45, rowIDx, REC.DateReceivedbyAAN);

                  IF REC.POLICY_VERSION > 1 AND REC.ENDT_CODE IN ('96', '97')
                  THEN
                     PG_EXCEL_UTILS.CELL (46, rowIDx, REC.ENDT_EFF_DATE);
                  ELSE
                     PG_EXCEL_UTILS.CELL (46,
                                          rowIDx,
                                          RISK_DET (V_ROW_NUM).TEMINATE_DATE);
                  END IF;


                  --dbms_output.put_line ('V_ROW_NUM::'||V_ROW_NUM);
                  IF V_ROW_NUM = 1
                  THEN
                     IF DBMS_LOB.getlength (REC.ENDT_NARR) > 32000
                     THEN
                        V_ENDT_NARR_ARRAY :=
                           PG_TPA_UTILS.FN_SPLIT_CLOB (REC.ENDT_NARR);

                        FOR I IN 1 .. V_ENDT_NARR_ARRAY.COUNT
                        LOOP
                           --dbms_output.put_line('I::'||V_ENDT_NARR_ARRAY(I));
                           IF I = 1
                           THEN
                              PG_EXCEL_UTILS.CELL (
                                 47,
                                 rowIDx,
                                 NVL (V_ENDT_NARR_ARRAY (1), ' '));
                           ELSE
                              PG_EXCEL_UTILS.CELL (
                                 50 + I,
                                 rowIDx,
                                 NVL (V_ENDT_NARR_ARRAY (I), ' '));
                           END IF;
                        END LOOP;
                     ELSE
                        PG_EXCEL_UTILS.CELL (47,
                                             rowIDx,
                                             NVL (REC.ENDT_NARR, ' '));
                     END IF;
                  ELSE
                     PG_EXCEL_UTILS.CELL (47, rowIDx, ' ');
                  END IF;

                  PG_EXCEL_UTILS.CELL (
                     48,
                     rowIDx,
                     NVL (
                        PG_TPA_UTILS.FN_GET_RISK_QUESTION (
                           REC.CONTRACT_ID,
                           REC.POLICY_VERSION,
                           RISK_DET (V_ROW_NUM).RISK_ID),
                        ' '));
                  PG_EXCEL_UTILS.CELL (49,
                                       rowIDx,
                                       NVL (V_UWPL_COVER_DET.REMARKS, ' '));
                  PG_EXCEL_UTILS.CELL (50,
                                       rowIDx,
                                       NVL (PG_TPA_UTILS.FN_GET_COVER_DIAGNOSIS (
                                               REC.CONTRACT_ID,
                                               REC.POLICY_VERSION,
                                               RISK_DET (V_ROW_NUM).RISK_ID,
                                               RISK_DET (V_ROW_NUM).COV_ID),
                                            ' '));


                  IF RISK_DET (V_ROW_NUM).OP_SUB_COV > 0
                     AND (RISK_DET (V_ROW_NUM).IMPORT_TYPE IS NOT NULL AND RISK_DET (V_ROW_NUM).IMPORT_TYPE <> 'XO')
                  THEN
                     PG_EXCEL_UTILS.CELL (51, rowIDx, 'Y');
                  ELSE
                     PG_EXCEL_UTILS.CELL (51, rowIDx, 'N');
                  END IF;

                  PG_EXCEL_UTILS.CELL (
                     52,
                     rowIDx,
                     NVL (RISK_DET (V_ROW_NUM).PREV_POL_OP_IND, ' '));

                  PG_EXCEL_UTILS.CELL (
                     53,
                     rowIDx,
                     NVL (RISK_DET (V_ROW_NUM).DEPARTMENT, ' '));

									PG_EXCEL_UTILS.CELL (
                     54,
                     rowIDx,
                     NVL (RISK_DET (V_ROW_NUM).EMAIL, ' '));

									PG_EXCEL_UTILS.CELL (
                     55,
                     rowIDx,
                     NVL (RISK_DET (V_ROW_NUM).ID_TYPE, ' '));

                  rowIDx := rowIDx + 1;
                  seq := seq + 1;
               END IF;
            END LOOP;

            --dbms_output.put_line ('RISK_DET::'||RISK_DET.COUNT);
            IF RISK_DET.COUNT > 0
            THEN
               V_RET :=
                  PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD (REC.CONTRACT_ID,
                                                      REC.POLICY_VERSION,
                                                      'MEDIX');
            END IF;
         END IF;
      END IF;
		END LOOP;

		V_STEPS := '010';

		FOR REC IN C_TPA_MEDIX_HC
		LOOP
			V_STEPS := '010A';
      IF    (REC.POLICY_VERSION = 1 AND REC.MCOI_FEE_AMT > 0)
         OR (REC.POLICY_VERSION > 1 AND REC.MCOI_FEE_AMT >= 0)
         OR (REC.POLICY_VERSION = 1 AND REC.IMA_FEE_AMT > 0)
         OR (REC.POLICY_VERSION > 1 AND REC.IMA_FEE_AMT >= 0)
         OR (    PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_MCOO,
                                                     REC.PRODUCT_CONFIG_CODE) =
                    'Y'
             AND (   (REC.POLICY_VERSION = 1 AND REC.MCOO_FEE_AMT > 0)
                  OR (REC.POLICY_VERSION > 1 AND REC.MCOO_FEE_AMT >= 0)))
         OR (    PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_HC_DMA,
                                                     REC.PRODUCT_CONFIG_CODE) =
                    'Y'
             AND (   (    REC.POLICY_VERSION = 1
                      AND (REC.MCOO_FEE_AMT > 0 OR REC.DMA_FEE_AMT > 0))
                  OR (    REC.POLICY_VERSION > 1
                      AND (REC.MCOO_FEE_AMT >= 0 OR REC.DMA_FEE_AMT >= 0))))
      THEN
         RISK_DET.DELETE;

				 V_STEPS := '010B';

         SELECT COUNT (1)
           INTO V_COUNT_TPA_RISK
           FROM UWGE_RISK_TPA_DOWNLOAD TPA
          WHERE     TPA.CONTRACT_ID = REC.CONTRACT_ID
                AND TPA.VERSION_NO = REC.POLICY_VERSION;

         IF V_COUNT_TPA_RISK > 0
         THEN
						V_STEPS := '010C';
            BEGIN
               EXECUTE IMMEDIATE V_SELECTED_RISK_SQL_TPA
                  BULK COLLECT INTO RISK_DET
                  USING REC.CONTRACT_ID, REC.POLICY_VERSION;
            END;
         ELSE
						V_STEPS := '010D';
            IF     REC.ENDT_CODE IS NOT NULL
               AND PG_TPA_UTILS.FN_ARRAY_CHECK_EXISIT (V_AAN_ENDT_CODE_R,
                                                       REC.ENDT_CODE) = 'Y'
            THEN
							 V_STEPS := '010E';
               BEGIN
                  EXECUTE IMMEDIATE V_SELECTED_RISK_SQL
                     BULK COLLECT INTO RISK_DET
                     USING REC.CONTRACT_ID, REC.POLICY_VERSION;
               END;
            ELSE
							 V_STEPS := '010F';
               BEGIN
                  EXECUTE IMMEDIATE V_ALL_RISK_SQL
                     BULK COLLECT INTO RISK_DET
										 USING REC.POLICY_VERSION,
                           REC.POLICY_VERSION,
                           REC.CONTRACT_ID,
                           REC.CONTRACT_ID,
                           REC.POLICY_VERSION;
               END;
            END IF;
         END IF;

         V_ROW_NUM := 0;

         FOR V_ROW_NUM IN 1 .. RISK_DET.COUNT
         LOOP
            V_STEPS := '011';
            PG_EXCEL_UTILS.CELL (1, rowIDx, seq);

            IF RISK_DET (V_ROW_NUM).IMPORT_TYPE IS NOT NULL
            THEN
               PG_EXCEL_UTILS.CELL (2,
                                    rowIDx,
                                    RISK_DET (V_ROW_NUM).IMPORT_TYPE);
            ELSE
               IF REC.POLICY_VERSION = 1 AND REC.PREV_POL_NO IS NOT NULL
               THEN
                  PG_EXCEL_UTILS.CELL (2, rowIDx, 'R');
               ELSIF REC.POLICY_VERSION = 1 AND REC.PREV_POL_NO IS NULL
               THEN
                  PG_EXCEL_UTILS.CELL (2, rowIDx, 'N');
               ELSIF REC.POLICY_VERSION > 1 AND REC.ENDT_CODE IN ('96', '97')
               THEN
                  PG_EXCEL_UTILS.CELL (2, rowIDx, 'X');
               ELSE
                  PG_EXCEL_UTILS.CELL (2, rowIDx, 'E');
               END IF;
            END IF;

            PG_EXCEL_UTILS.CELL (
               3,
               rowIDx,
               NVL (RISK_DET (V_ROW_NUM).MEMBER_FULL_NAME, ' '));
            PG_EXCEL_UTILS.CELL (
               4,
               rowIDx,
               NVL (REC.ADDRESS_LINE1, ' ') || NVL (REC.ADDRESS_LINE2, ' '));
            PG_EXCEL_UTILS.CELL (5, rowIDx, NVL (REC.ADDRESS_LINE3, ' '));
            PG_EXCEL_UTILS.CELL (
               6,
               rowIDx,
               NVL (REC.POSTCODE, ' ') || ' ' || NVL (REC.CITY, ' '));
            PG_EXCEL_UTILS.CELL (7, rowIDx, NVL (REC.STATE, ' '));
            PG_EXCEL_UTILS.CELL (8,
                                 rowIDx,
                                 NVL (RISK_DET (V_ROW_NUM).SEX, ' '));

            IF RISK_DET (V_ROW_NUM).DATE_OF_BIRTH IS NULL
            THEN
               PG_EXCEL_UTILS.CELL (9, rowIDx, ' ');
            ELSE
               PG_EXCEL_UTILS.CELL (
                  9,
                  rowIDx,
                  TO_CHAR (RISK_DET (V_ROW_NUM).DATE_OF_BIRTH, 'DD/MM/YYYY'));
            END IF;

            PG_EXCEL_UTILS.CELL (10,
                                 rowIDx,
                                 NVL (RISK_DET (V_ROW_NUM).NRIC, ' '));

            IF RISK_DET (V_ROW_NUM).NRIC IS NULL
            THEN
               PG_EXCEL_UTILS.CELL (11,
                                    rowIDx,
                                    NVL (RISK_DET (V_ROW_NUM).NRIC_OTH, ' '));
            ELSE
               PG_EXCEL_UTILS.CELL (11, rowIDx, ' ');
            END IF;

            PG_EXCEL_UTILS.CELL (
               12,
               rowIDx,
                  RISK_DET (V_ROW_NUM).RISK_ID
               || '-'
               || REC.POLICY_REF
               || '-'
               || RISK_DET (V_ROW_NUM).COV_SEQ_REF);
            PG_EXCEL_UTILS.CELL (13, rowIDx, ' ');
            PG_EXCEL_UTILS.CELL (14,
                                 rowIDx,
                                 NVL (RISK_DET (V_ROW_NUM).EMPLOYEE_ID, ' '));
            PG_EXCEL_UTILS.CELL (
               15,
               rowIDx,
               NVL (RISK_DET (V_ROW_NUM).MARITAL_STATUS, ' '));
            PG_EXCEL_UTILS.CELL (16, rowIDx, ' ');
            PG_EXCEL_UTILS.CELL (17, rowIDx, NVL (REC.PhoneNumber, ' '));
            PG_EXCEL_UTILS.CELL (18, rowIDx, ' ');
            PG_EXCEL_UTILS.CELL (19, rowIDx, ' ');
            PG_EXCEL_UTILS.CELL (
               20,
               rowIDx,
               NVL (RISK_DET (V_ROW_NUM).RELATIONSHIP, ' '));
            PG_EXCEL_UTILS.CELL (21, rowIDx, ' ');

            --1.1
            IF RISK_DET (V_ROW_NUM).INSURED_TYPE = 'P'
            THEN
               PG_EXCEL_UTILS.CELL (
                  22,
                  rowIDx,
                     RISK_DET (V_ROW_NUM).RISK_ID
                  || '-'
                  || REC.POLICY_REF
                  || '-'
                  || RISK_DET (V_ROW_NUM).COV_SEQ_REF);
               PG_EXCEL_UTILS.CELL (
                  23,
                  rowIDx,
                  NVL (RISK_DET (V_ROW_NUM).MEMBER_FULL_NAME, ' '));
               PG_EXCEL_UTILS.CELL (24,
                                    rowIDx,
                                    NVL (RISK_DET (V_ROW_NUM).NRIC, ' '));

               IF RISK_DET (V_ROW_NUM).NRIC IS NULL
               THEN
                  PG_EXCEL_UTILS.CELL (
                     25,
                     rowIDx,
                     NVL (RISK_DET (V_ROW_NUM).NRIC_OTH, ' '));
               ELSE
                  PG_EXCEL_UTILS.CELL (25, rowIDx, ' ');
               END IF;
            ELSE
               PG_EXCEL_UTILS.CELL (
                  22,
                  rowIDx,
                     RISK_DET (V_ROW_NUM).RISK_PARENT_ID
                  || '-'
                  || REC.POLICY_REF
                  || '-'
                  || RISK_DET (V_ROW_NUM).Parent_cov_seq_no);
               V_PRINCIPAL_DET :=
                  PG_TPA_UTILS.FN_GET_PRINCIPAL_DET (
                     REC.CONTRACT_ID,
                     REC.POLICY_VERSION,
                     RISK_DET (V_ROW_NUM).RISK_PARENT_ID);
               PG_EXCEL_UTILS.CELL (
                  23,
                  rowIDx,
                  NVL (V_PRINCIPAL_DET.MEMBER_FULL_NAME, ' '));
               PG_EXCEL_UTILS.CELL (24,
                                    rowIDx,
                                    NVL (V_PRINCIPAL_DET.NRIC, ' '));

               IF V_PRINCIPAL_DET.NRIC IS NULL
               THEN
                  PG_EXCEL_UTILS.CELL (25,
                                       rowIDx,
                                       NVL (V_PRINCIPAL_DET.NRIC_OTH, ' '));
               ELSE
                  PG_EXCEL_UTILS.CELL (25, rowIDx, ' ');
               END IF;
            END IF;

            PG_EXCEL_UTILS.CELL (26, rowIDx, ' ');
            PG_EXCEL_UTILS.CELL (27, rowIDx, 'IG');
            PG_EXCEL_UTILS.CELL (28, rowIDx, NVL (REC.POLICY_REF, ' '));
            PG_EXCEL_UTILS.CELL (29, rowIDx, REC.EFF_DATE);
            PG_EXCEL_UTILS.CELL (30, rowIDx, REC.EXP_DATE);
            PG_EXCEL_UTILS.CELL (31, rowIDx, NVL (REC.prev_pol, ' '));
            PG_EXCEL_UTILS.CELL (32, rowIDx, REC.prev_exp_date);
            PG_EXCEL_UTILS.CELL (33, rowIDx, NVL (REC.NAME_EXT, ' '));
            V_UWPL_COVER_DET :=
               PG_TPA_UTILS.FN_GET_UWPL_COVER_DET (
                  REC.CONTRACT_ID,
                  REC.POLICY_VERSION,
                  RISK_DET (V_ROW_NUM).COV_ID);
            PG_EXCEL_UTILS.CELL (34,
                                 rowIDx,
                                 NVL (V_UWPL_COVER_DET.PLAN_CODE, ' '));
            PG_EXCEL_UTILS.CELL (35, rowIDx, ' ');

            IF RISK_DET (V_ROW_NUM).ORIGINAL_JOIN_DATE IS NULL
            THEN
               PG_EXCEL_UTILS.CELL (36, rowIDx, ' ');
            ELSE
               PG_EXCEL_UTILS.CELL (
                  36,
                  rowIDx,
                  TO_CHAR (RISK_DET (V_ROW_NUM).ORIGINAL_JOIN_DATE,
                           'DD/MM/YYYY'));
            END IF;

            PG_EXCEL_UTILS.CELL (37,
                                 rowIDx,
                                 RISK_DET (V_ROW_NUM).RISK_EFF_DATE);
            PG_EXCEL_UTILS.CELL (38,
                                 rowIDx,
                                 RISK_DET (V_ROW_NUM).RISK_EXP_DATE);
            PG_EXCEL_UTILS.CELL (39, rowIDx, REC.BRANCH_DESC);

            PG_EXCEL_UTILS.CELL (40, rowIDx, NVL (REC.AGENT_NAME, ' '));
            PG_EXCEL_UTILS.CELL (41, rowIDx, NVL (REC.AGENT_CODE, ' '));
            PG_EXCEL_UTILS.CELL (
               42,
               rowIDx,
                 NVL (REC.MCO_FEE_AMT, 0)
               + NVL (REC.MCOI_FEE_AMT, 0)
               + NVL (REC.MCOO_FEE_AMT, 0));

            IF REC.IMA_FEE_AMT > 0
            THEN
               PG_EXCEL_UTILS.CELL (
                  43,
                  rowIDx,
                  'Y',
                  p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                     p_vertical     => 'center',
                                     p_horizontal   => 'center',
                                     p_wrapText     => TRUE));
            ELSE
               PG_EXCEL_UTILS.CELL (
                  43,
                  rowIDx,
                  'N',
                  p_alignment   => PG_EXCEL_UTILS.get_alignment (
                                     p_vertical     => 'center',
                                     p_horizontal   => 'center',
                                     p_wrapText     => TRUE));
            END IF;

            IF REC.IMA_FEE_AMT > 0
            THEN
               PG_EXCEL_UTILS.CELL (44, rowIDx, '1000000');
            ELSE
               PG_EXCEL_UTILS.CELL (44, rowIDx, ' ');
            END IF;

            PG_EXCEL_UTILS.CELL (45, rowIDx, REC.DateReceivedbyAAN);

            IF REC.POLICY_VERSION > 1 AND REC.ENDT_CODE IN ('96', '97')
            THEN
               PG_EXCEL_UTILS.CELL (46, rowIDx, REC.ENDT_EFF_DATE);
            ELSE
               PG_EXCEL_UTILS.CELL (46,
                                    rowIDx,
                                    RISK_DET (V_ROW_NUM).TEMINATE_DATE);
            END IF;

            --dbms_output.put_line ('V_ROW_NUM::'||V_ROW_NUM);
            IF V_ROW_NUM = 1
            THEN
               --dbms_output.put_line ('ENDT_NARR::'||DBMS_LOB.getlength(REC.ENDT_NARR));
               IF DBMS_LOB.getlength (REC.ENDT_NARR) > 32000
               THEN
                  V_ENDT_NARR_ARRAY :=
                     PG_TPA_UTILS.FN_SPLIT_CLOB (REC.ENDT_NARR);

                  FOR I IN 1 .. V_ENDT_NARR_ARRAY.COUNT
                  LOOP
                     --dbms_output.put_line('I::'||V_ENDT_NARR_ARRAY(I));
                     IF I = 1
                     THEN
                        PG_EXCEL_UTILS.CELL (
                           47,
                           rowIDx,
                           NVL (V_ENDT_NARR_ARRAY (1), ' '));
                     ELSE
                        PG_EXCEL_UTILS.CELL (
                           50 + I,
                           rowIDx,
                           NVL (V_ENDT_NARR_ARRAY (I), ' '));
                     END IF;
                  END LOOP;
               ELSE
                  PG_EXCEL_UTILS.CELL (47, rowIDx, NVL (REC.ENDT_NARR, ' '));
               END IF;
            ELSE
               PG_EXCEL_UTILS.CELL (47, rowIDx, ' ');
            END IF;

            PG_EXCEL_UTILS.CELL (
               48,
               rowIDx,
               NVL (
                  PG_TPA_UTILS.FN_GET_RISK_QUESTION (
                     REC.CONTRACT_ID,
                     REC.POLICY_VERSION,
                     RISK_DET (V_ROW_NUM).RISK_ID),
                  ' '));
            PG_EXCEL_UTILS.CELL (49,
                                 rowIDx,
                                 NVL (V_UWPL_COVER_DET.REMARKS, ' '));
            PG_EXCEL_UTILS.CELL (50,
                                 rowIDx,
                                 NVL (PG_TPA_UTILS.FN_GET_COVER_DIAGNOSIS (
                                         REC.CONTRACT_ID,
                                         REC.POLICY_VERSION,
                                         RISK_DET (V_ROW_NUM).RISK_ID,
                                         RISK_DET (V_ROW_NUM).COV_ID),
                                      ' '));

            IF RISK_DET (V_ROW_NUM).OP_SUB_COV > 0 
               AND (RISK_DET (V_ROW_NUM).IMPORT_TYPE IS NOT NULL AND RISK_DET (V_ROW_NUM).IMPORT_TYPE <> 'XO')
            THEN
               PG_EXCEL_UTILS.CELL (51, rowIDx, 'Y');
            ELSE
               PG_EXCEL_UTILS.CELL (51, rowIDx, 'N');
            END IF;

            PG_EXCEL_UTILS.CELL (
               52,
               rowIDx,
               NVL (RISK_DET (V_ROW_NUM).PREV_POL_OP_IND, ' '));

            PG_EXCEL_UTILS.CELL (53,
                                 rowIDx,
                                 NVL (RISK_DET (V_ROW_NUM).DEPARTMENT, ' '));

						PG_EXCEL_UTILS.CELL (54,
                                 rowIDx,
                                 NVL (RISK_DET (V_ROW_NUM).EMAIL, ' '));

						PG_EXCEL_UTILS.CELL (55,
                                 rowIDx,
                                 NVL (RISK_DET (V_ROW_NUM).ID_TYPE, ' '));

            rowIDx := rowIDx + 1;
            seq := seq + 1;
         END LOOP;

         --dbms_output.put_line ('RISK_DET::'||RISK_DET.COUNT);
         IF RISK_DET.COUNT > 0
         THEN
            V_RET :=
               PG_TPA_UTILS.FN_UPD_POL_CTRL_DLOAD (REC.CONTRACT_ID,
                                                   REC.POLICY_VERSION,
                                                   'MEDIX');
         END IF;
      END IF;
		END LOOP;

		P_FILE_NAME := FILENAME1;

		V_STEPS := '016';
		DBMS_OUTPUT.ENABLE (buffer_size => NULL);
		PG_EXCEL_UTILS.save (v_file_dir, FILENAME1);
	EXCEPTION
	WHEN OTHERS
		THEN
      PG_UTIL_LOG_ERROR.PC_INS_Log_Error (
         V_PKG_NAME || V_FUNC_NAME,
         1,
         '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);
			--dbms_output.put_line ('FILENAME1=' || '::' || 'Steps=' || V_STEPS || '::' || SQLERRM);

	END PC_TPA_MEDIX_HC_PA_POL_ENDT;
	-- 3.4 END
END PG_TPA_BUILD;