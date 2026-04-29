
  CREATE OR REPLACE EDITIONABLE PROCEDURE "VNADSPRD"."SP_IEN_ICON_TBLS_REFRESH" AS
    v_icoe_equipment_tbl   NUMBER;
    v_icoe_card_tbl        NUMBER;
    v_icoe_slot_tbl        NUMBER;
    v_icoe_site_tbl        NUMBER;
BEGIN
/* Load ICOE Temp Tables */
    dbms_output.put_line('Step 1 start : create ICOE_EQUIPMENT_TBL temp table   - ' || systimestamp);
    SELECT
        COUNT(*)
    INTO v_icoe_equipment_tbl
    FROM
        all_tables
    WHERE
        table_name = 'ICOE_EQUIPMENT_TBL';
    IF v_icoe_equipment_tbl = 1 THEN
        EXECUTE IMMEDIATE 'DROP TABLE VNADSPRD.ICOE_EQUIPMENT_TBL PURGE';
        EXECUTE IMMEDIATE 'CREATE TABLE VNADSPRD.ICOE_EQUIPMENT_TBL PARALLEL  NOLOGGING AS  SELECT * FROM VNADSPRD.icoe_mv_logical_equipment'
        ;
        EXECUTE IMMEDIATE 'GRANT SELECT on VNADSPRD.ICOE_EQUIPMENT_TBL to VNA_SELECT';
    ELSE
        EXECUTE IMMEDIATE 'CREATE TABLE VNADSPRD.ICOE_EQUIPMENT_TBL PARALLEL  NOLOGGING AS  SELECT * FROM VNADSPRD.icoe_mv_logical_equipment'
        ;
        EXECUTE IMMEDIATE 'GRANT SELECT on VNADSPRD.ICOE_EQUIPMENT_TBL to VNA_SELECT';
    END IF;
    EXECUTE IMMEDIATE 'CREATE INDEX VNADSPRD.IDX_ICOE_EQUIP_TBL ON VNADSPRD.ICOE_EQUIPMENT_TBL(PHYSICAL_EQUIPMENT_REFERENC_ID) NOLOGGING'
    ;
    EXECUTE IMMEDIATE 'CREATE INDEX VNADSPRD.IDX1_ICOE_EQUIP_TBL ON VNADSPRD.ICOE_EQUIPMENT_TBL(PARENT_EQP_REFERENCE_ID) NOLOGGING'
    ;
    EXECUTE IMMEDIATE 'CREATE INDEX VNADSPRD.IDX2_ICOE_EQUIP_TBL ON VNADSPRD.ICOE_EQUIPMENT_TBL(SHELF_TYPE) NOLOGGING';
    EXECUTE IMMEDIATE 'CREATE INDEX VNADSPRD.IDX3_ICOE_EQUIP_TBL ON VNADSPRD.ICOE_EQUIPMENT_TBL(SITE_REFERENCE_ID) NOLOGGING';
    EXECUTE IMMEDIATE 'CREATE INDEX VNADSPRD.IDX4_ICOE_EQUIP_TBL ON VNADSPRD.ICOE_EQUIPMENT_TBL(CONTAINER) NOLOGGING';
   dbms_output.put_line('Step 1 end : create ICOE_EQUIPMENT_TBL temp table   - ' || systimestamp);

   dbms_output.put_line('Step 2 start : create ICOE_CARD_TBL temp table   - ' || systimestamp);
    SELECT
        COUNT(*)
    INTO v_icoe_card_tbl
    FROM
        all_tables
    WHERE
        table_name = 'ICOE_CARD_TBL';
    IF v_icoe_card_tbl = 1 THEN
        EXECUTE IMMEDIATE 'DROP TABLE VNADSPRD.ICOE_CARD_TBL PURGE';
        EXECUTE IMMEDIATE 'CREATE TABLE VNADSPRD.ICOE_CARD_TBL PARALLEL 8 NOLOGGING AS SELECT * FROM VNADSPRD.icoe_mv_logical_card';
        EXECUTE IMMEDIATE 'GRANT SELECT on VNADSPRD.ICOE_CARD_TBL to VNA_SELECT';
    ELSE
        EXECUTE IMMEDIATE 'CREATE TABLE VNADSPRD.ICOE_CARD_TBL PARALLEL 8 NOLOGGING AS SELECT * FROM VNADSPRD.icoe_mv_logical_card';
        EXECUTE IMMEDIATE 'GRANT SELECT on VNADSPRD.ICOE_CARD_TBL to VNA_SELECT';
    END IF;
    EXECUTE IMMEDIATE 'CREATE INDEX VNADSPRD.IDX_ICOE_CARD_TBL ON VNADSPRD.ICOE_CARD_TBL(CARD_REFERENCE_ID) NOLOGGING';
    EXECUTE IMMEDIATE 'CREATE INDEX VNADSPRD.IDX1_ICOE_CARD_TBL ON VNADSPRD.ICOE_CARD_TBL(SLOT_REFERENCE_ID) NOLOGGING';
    EXECUTE IMMEDIATE 'CREATE INDEX VNADSPRD.IDX2_ICOE_CARD_TBL ON VNADSPRD.ICOE_CARD_TBL(PARENT_CARD_REF_ID) NOLOGGING';
     dbms_output.put_line('Step 2 end : create ICOE_CARD_TBL temp table   - ' || systimestamp);

      dbms_output.put_line('Step 3 start : create ICOE_SLOT_TBL temp table   - ' || systimestamp);
    SELECT
        COUNT(*)
    INTO v_icoe_slot_tbl
    FROM
        all_tables
    WHERE
        table_name = 'ICOE_SLOT_TBL';
    IF v_icoe_slot_tbl = 1 THEN
        EXECUTE IMMEDIATE 'DROP TABLE VNADSPRD.ICOE_SLOT_TBL PURGE';
        EXECUTE IMMEDIATE 'CREATE TABLE VNADSPRD.ICOE_SLOT_TBL PARALLEL  NOLOGGING AS SELECT * FROM VNADSPRD.ICOE_PVNR_MV_LOGICAL_SLOT'
        ; /* Changed by Keerthana on 12/11/19*/
        EXECUTE IMMEDIATE 'GRANT SELECT on VNADSPRD.ICOE_SLOT_TBL to VNA_SELECT';
    ELSE
        EXECUTE IMMEDIATE 'CREATE TABLE VNADSPRD.ICOE_SLOT_TBL PARALLEL  NOLOGGING AS SELECT * FROM VNADSPRD.ICOE_PVNR_MV_LOGICAL_SLOT'
        ;/* Changed by Keerthana on 12/11/19*/
        EXECUTE IMMEDIATE 'GRANT SELECT on VNADSPRD.ICOE_SLOT_TBL to VNA_SELECT';
    END IF;
    EXECUTE IMMEDIATE 'CREATE INDEX VNADSPRD.IDX_SLOT_TBL ON VNADSPRD.ICOE_SLOT_TBL(SLOT_REFERENCE_ID) NOLOGGING';

    dbms_output.put_line('Step 3 end : create ICOE_SLOT_TBL temp table   - ' || systimestamp);

    dbms_output.put_line('Step 4 start : create ICOE_SITE_TBL temp table   - ' || systimestamp);

    SELECT
        COUNT(*)
    INTO v_icoe_site_tbl
    FROM
        all_tables
    WHERE
        table_name = 'ICOE_SITE_TBL';
    IF v_icoe_site_tbl = 1 THEN
        EXECUTE IMMEDIATE 'DROP TABLE VNADSPRD.ICOE_SITE_TBL PURGE';
        EXECUTE IMMEDIATE 'CREATE TABLE VNADSPRD.ICOE_SITE_TBL PARALLEL 8 NOLOGGING AS SELECT * FROM VNADSPRD.ICOE_MV_SITE';
        EXECUTE IMMEDIATE 'GRANT SELECT on VNADSPRD.ICOE_SITE_TBL to VNA_SELECT';
    ELSE
        EXECUTE IMMEDIATE 'CREATE TABLE VNADSPRD.ICOE_SITE_TBL PARALLEL 8 NOLOGGING AS SELECT * FROM VNADSPRD.ICOE_MV_SITE';
        EXECUTE IMMEDIATE 'GRANT SELECT on VNADSPRD.ICOE_SITE_TBL to VNA_SELECT';
    END IF;
    EXECUTE IMMEDIATE 'CREATE INDEX VNADSPRD.IDX_SITE_TBL ON VNADSPRD.ICOE_SITE_TBL(SITE_REFERENCE_ID) NOLOGGING';
    EXECUTE IMMEDIATE 'CREATE INDEX VNADSPRD.IDX1_SITE_TBL ON VNADSPRD.ICOE_SITE_TBL(CLLI) NOLOGGING';
    dbms_output.put_line('Step 4 end  : create ICOE_SITE_TBL temp table   - ' || systimestamp);

    dbms_output.put_line('Step 5 start  : gather stats on temp tables   - ' || systimestamp);
    dbms_stats.gather_table_stats(ownname => 'VNADSPRD', tabname => 'ICOE_CARD_TBL', degree => 16, method_opt => 'FOR ALL COLUMNS SIZE 1'
    , cascade => true);
    dbms_stats.gather_table_stats(ownname => 'VNADSPRD', tabname => 'ICOE_SLOT_TBL', degree => 16, method_opt => 'FOR ALL COLUMNS SIZE 1'
    , cascade => true);
    dbms_stats.gather_table_stats(ownname => 'VNADSPRD', tabname => 'ICOE_SITE_TBL', degree => 16, method_opt => 'FOR ALL COLUMNS SIZE 1'
    , cascade => true);
    dbms_stats.gather_table_stats(ownname => 'VNADSPRD', tabname => 'ICOE_EQUIPMENT_TBL', degree => 16, method_opt => 'FOR ALL COLUMNS SIZE 1'
    , cascade => true);

    dbms_output.put_line('Step 5 end  : gather stats on temp tables   - ' || systimestamp);


dbms_output.put_line('--------------------IEN_MR_CHANNEL_TEMP  load started--------------------' || systimestamp);

	   EXECUTE IMMEDIATE 'TRUNCATE TABLE vnadsprd.IEN_MR_CHANNEL_TEMP';
        EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX1_IEN_MR_CHANNEL_TEMP UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX2_IEN_MR_CHANNEL_TEMP UNUSABLE';

      INSERT /*+ APPEND PARALLEL (8) */ INTO vnadsprd.ien_mr_channel_temp (
            trail_id,
            trail_name,
            type,
            status,
            version,
            parent_trail_id,
            channel_name,
            project_id,
            aclli,
            zclli
            --,
            --network_type
        )
		SELECT VW.TRAIL_ID,

               VW.TRAIL_NAME,

               VW.TYPE,

               VW.STATUS,

               VW.VERSION,

               PARENT_TRAIL_ID,

               MIN((CHANNEL_NAME)) CHANNEL_NAME,

               VW.PROJECT_ID,

               ASITE.CLLI ACLLI,

               ZSITE.CLLI ZCLLI

          FROM vnadsprd.NAUT_TRAIL_ELEMENT TE, vnadsprd.NAUT_TRAIL VW,

           -- vnadsprd.ICOE_MV_SITE ASITE,  Changed to NAR Table

          vnadsprd.ICOE_SITE_TBL ASITE,

        -- vnadsprd.ICOE_MV_SITE ZSITE Changed to NAR Table

           vnadsprd.ICOE_SITE_TBL ZSITE

         WHERE

         --VW.TYPE = ''OTN_INFRASTRUCTURE''

            VW.TRAIL_ID = TE.TRAIL_ID

           AND TE.ELEMENT_TYPE = 'P'

           AND VW.A_SITE_ID =ASITE.SITE_REFERENCE_ID(+)

           AND VW.Z_SITE_ID =ZSITE.SITE_REFERENCE_ID(+)

         GROUP BY VW.TRAIL_ID,

                  VW.TRAIL_NAME,

                  VW.TYPE,

                  VW.STATUS,

                  VW.VERSION,

                  TE.PARENT_TRAIL_ID,

                  VW.PROJECT_ID,

                  ASITE.CLLI,

                  ZSITE.CLLI;
        COMMIT;
        EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX1_IEN_MR_CHANNEL_TEMP REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX2_IEN_MR_CHANNEL_TEMP REBUILD';
   dbms_output.put_line('--------------------IEN_MR_CHANNEL_TEMP tbl load ended--------------------' || systimestamp);	



-----vnadsprd.sp_ien_market_input_list_load_new
/***** Load IEN_MARKET_INPUT_LIST_STG********/
  dbms_output.put_line('Step 6 start  : truncate and insert IEN_MARKET_INPUT_LIST_STG   - ' || systimestamp);

        EXECUTE IMMEDIATE 'truncate table vnadsprd.IEN_MARKET_INPUT_LIST_STG';
		 EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX1_IEN_MARKET_INPUT_LIST_STG UNUSABLE';
		 EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX2_IEN_MARKET_INPUT_LIST_STG UNUSABLE';
		EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX3_IEN_MARKET_INPUT_LIST_STG UNUSABLE';
        INSERT INTO vnadsprd.IEN_MARKET_INPUT_LIST_STG (TRAIL_ID	,
	TRAIL_NAME	,
	STATUS	,
	TYPE	,
	VERSION	,
	SEQUENCE	,
	PORT_REFERENCE_ID	,
	SOURCE	,
	CHANNEL_NAME	,
	SEQUENCE_NUMBER	,
	PORT_STATUS	,
	ELEMENT_TYPE,
	NF_ID,
	ACLLI,
	ZCLLI,
	LAST_REFRESHED_TS,
	EQUIPMENT_ID )
SELECT DISTINCT VW.TRAIL_ID,
                VW.TRAIL_NAME,
                VW.STATUS,
                VW.TYPE,
                VW.VERSION,
                TCE.SEQUENCE,
                E.ELEMENT_REF_ID AS PORT_REFERENCE_ID,
                E.SOURCE,
                E.CHANNEL_NAME,
                NULL AS SEQUENCE_NUMBER,
                NULL AS PORT_STATUS,
                E.ELEMENT_TYPE,
                VW.PROJECT_ID,
                ASITE.CLLI ACLLI,
                ZSITE.CLLI ZCLLI,
                SYSTIMESTAMP,
				NULL AS EQUIPMENT_ID
  FROM vnadsprd.NAUT_TRAIL_ELEMENT E,
       vnadsprd.NAUT_TRAIL   VW,
       vnadsprd.NAUT_TRAIL_COMPONENT_ELEMENT TCE,
      -- vnadsprd.ICOE_MV_SITE ASITE,  Changed to NAR Table
       vnadsprd.ICOE_SITE_TBL ASITE,
        -- vnadsprd.ICOE_MV_SITE ZSITE Changed to NAR Table
           vnadsprd.ICOE_SITE_TBL ZSITE
   WHERE VW.TRAIL_ID = E.TRAIL_ID
   AND E.ELEMENT_TYPE = 'E'
   AND E.SOURCE IN ('IVAPP_PORT', 'IVAPP_LOGICAL', 'IVAPP_PANEL')
   AND TCE.ELEMENT_ID=E.ELEMENT_ID
   AND VW.A_SITE_ID =ASITE.SITE_REFERENCE_ID(+)
   AND VW.Z_SITE_ID =ZSITE.SITE_REFERENCE_ID(+)
UNION
SELECT X.TRAIL_ID,
       X.TRAIL_NAME,
       X.STATUS,
       X.TYPE,
       X.VERSION,
       TCE.SEQUENCE,
       T.A_PORT_ID PORT_REFERENCE_ID,
       'NAUTILUS' AS SOURCE,
       TC.CHANNEL_NAME,
       TC.SEQUENCE_NUMBER,
       TC.STATUS PORT_STATUS,
       TE.ELEMENT_TYPE,
       X.PROJECT_ID,
       X.ACLLI,
       X.ZCLLI,
       SYSTIMESTAMP,
	   NULL AS EQUIPMENT_ID
  FROM vnadsprd.NAUT_TRAIL T,
       vnadsprd.NAUT_TRAIL_CHANNEL TC,
       vnadsprd.NAUT_TRAIL_COMPONENT_ELEMENT TCE,
       vnadsprd.NAUT_TRAIL_ELEMENT TE,
	   vnadsprd.ien_mr_channel_temp X

 WHERE T.TRAIL_ID = X.PARENT_TRAIL_ID
   AND TC.PARENT_TRAIL_ID = X.PARENT_TRAIL_ID
   AND TC.CHANNEL_NAME = to_char(X.CHANNEL_NAME)
   AND TE.CHANNEL_NAME= to_char(X.CHANNEL_NAME)
   AND TE.PARENT_TRAIL_ID= X.PARENT_TRAIL_ID
   AND TCE.ELEMENT_ID=TE.ELEMENT_ID
UNION
SELECT X.TRAIL_ID,
       X.TRAIL_NAME,
       X.STATUS,
       X.TYPE,
       X.VERSION,
       TCE.SEQUENCE,
       T.Z_PORT_ID PORT_REFERENCE_ID,
       'NAUTILUS' AS SOURCE,
       TC.CHANNEL_NAME,
       TC.SEQUENCE_NUMBER,
       TC.STATUS PORT_STATUS,
       TE.ELEMENT_TYPE,
       X.PROJECT_ID,
       X.ACLLI,
       X.ZCLLI,
       SYSTIMESTAMP,
	   NULL AS EQUIPMENT_ID
  FROM vnadsprd.NAUT_TRAIL T,
       vnadsprd.NAUT_TRAIL_CHANNEL TC,
        vnadsprd.NAUT_TRAIL_COMPONENT_ELEMENT TCE,
       vnadsprd.NAUT_TRAIL_ELEMENT TE,
	   vnadsprd.ien_mr_channel_temp X

 WHERE T.TRAIL_ID = X.PARENT_TRAIL_ID
   AND TC.PARENT_TRAIL_ID = X.PARENT_TRAIL_ID
   AND TC.CHANNEL_NAME = to_char(X.CHANNEL_NAME)
    AND TE.CHANNEL_NAME= to_char(X.CHANNEL_NAME)
   AND TE.PARENT_TRAIL_ID= X.PARENT_TRAIL_ID
   AND TCE.ELEMENT_ID=TE.ELEMENT_ID
UNION
   SELECT X.TRAIL_ID,
       X.TRAIL_NAME,
       X.STATUS,
       X.TYPE,
       X.VERSION,
       TCE.SEQUENCE,
       T.A_PORT_ID_REVERSE PORT_REFERENCE_ID,
       'NAUTILUS' AS SOURCE,
       TC.CHANNEL_NAME,
       TC.SEQUENCE_NUMBER,
       TC.STATUS PORT_STATUS,
       TE.ELEMENT_TYPE,
       X.PROJECT_ID,
       X.ACLLI,
       X.ZCLLI,
       SYSTIMESTAMP,
	   NULL AS EQUIPMENT_ID
  FROM vnadsprd.NAUT_TRAIL T,
       vnadsprd.NAUT_TRAIL_CHANNEL TC,
        vnadsprd.NAUT_TRAIL_COMPONENT_ELEMENT TCE,
       vnadsprd.NAUT_TRAIL_ELEMENT TE,
	   vnadsprd.ien_mr_channel_temp X

 WHERE T.TRAIL_ID = X.PARENT_TRAIL_ID
   AND TC.PARENT_TRAIL_ID = X.PARENT_TRAIL_ID
   AND TC.CHANNEL_NAME = to_char(X.CHANNEL_NAME)
    AND TE.CHANNEL_NAME = to_char(X.CHANNEL_NAME)
   AND TE.PARENT_TRAIL_ID= X.PARENT_TRAIL_ID
   AND TCE.ELEMENT_ID=TE.ELEMENT_ID
   UNION
   SELECT X.TRAIL_ID,
       X.TRAIL_NAME,
       X.STATUS,
       X.TYPE,
       X.VERSION,
       TCE.SEQUENCE,
       T.Z_PORT_ID_REVERSE PORT_REFERENCE_ID,
       'NAUTILUS' AS SOURCE,
       TC.CHANNEL_NAME,
       TC.SEQUENCE_NUMBER,
       TC.STATUS PORT_STATUS,
       TE.ELEMENT_TYPE,
       X.PROJECT_ID,
       X.ACLLI,
       X.ZCLLI,
       SYSTIMESTAMP,
	   NULL AS EQUIPMENT_ID
  FROM vnadsprd.NAUT_TRAIL T,
       vnadsprd.NAUT_TRAIL_CHANNEL TC,
        vnadsprd.NAUT_TRAIL_COMPONENT_ELEMENT TCE,
       vnadsprd.NAUT_TRAIL_ELEMENT TE,
	    vnadsprd.ien_mr_channel_temp X

 WHERE T.TRAIL_ID = X.PARENT_TRAIL_ID
   AND TC.PARENT_TRAIL_ID = X.PARENT_TRAIL_ID
   AND TC.CHANNEL_NAME = to_char(X.CHANNEL_NAME)
    AND TE.CHANNEL_NAME= to_char(X.CHANNEL_NAME)
   AND TE.PARENT_TRAIL_ID= X.PARENT_TRAIL_ID
   AND TCE.ELEMENT_ID=TE.ELEMENT_ID;
   COMMIT;
 EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX1_IEN_MARKET_INPUT_LIST_STG REBUILD';
 EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX2_IEN_MARKET_INPUT_LIST_STG REBUILD';
 EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX3_IEN_MARKET_INPUT_LIST_STG REBUILD';
  dbms_output.put_line('Step 6 end  : truncate and insert IEN_MARKET_INPUT_LIST_STG   - ' || systimestamp);
/***** Load IEN_MARKET_INPUT_LIST_STG_TMP (LAG circuit)********/
  dbms_output.put_line('Step 7 start  : truncate and insert IEN_MARKET_INPUT_LIST_STG_TMP   - ' || systimestamp);
  EXECUTE IMMEDIATE 'truncate table vnadsprd.IEN_MARKET_INPUT_LIST_STG_TMP';
    INSERT INTO vnadsprd.IEN_MARKET_INPUT_LIST_STG_TMP (TRAIL_ID,
	TRAIL_NAME	,
	STATUS	,
	TYPE	,
	VERSION	,
	SEQUENCE,
	PORT_REFERENCE_ID,
	SOURCE	,
	CHANNEL_NAME,
	SEQUENCE_NUMBER	,
	PORT_STATUS	,
	ELEMENT_TYPE,
	NF_ID,
	ACLLI,
	ZCLLI,
	LAST_REFRESHED_TS,
	EQUIPMENT_ID )
            SELECT DISTINCT
                VW.TRAIL_ID,
                VW.TRAIL_NAME,
                VW.STATUS,
                VW.TYPE,
                VW.VERSION,
                TCE.SEQUENCE,
                NULL          PORT_REFERENCE_ID,
                E.SOURCE,
                VW.A_PORT_AID,
                NULL          AS SEQUENCE_NUMBER,
                VW.STATUS     AS PORT_STATUS,
                E.ELEMENT_TYPE,
                VW.PROJECT_ID,
                ASITE.CLLI    ACLLI,
                ZSITE.CLLI    ZCLLI,
                SYSTIMESTAMP,
			    VW.A_EQUIPMENT_ID
            FROM
                VNADSPRD.NAUT_TRAIL_ELEMENT           E,
                VNADSPRD.NAUT_TRAIL_TMP               VW,
                VNADSPRD.NAUT_TRAIL_COMPONENT_ELEMENT TCE,
                VNADSPRD.ICOE_SITE_TBL                ASITE,
                VNADSPRD.ICOE_SITE_TBL                ZSITE
            WHERE
                    VW.TRAIL_ID = E.TRAIL_ID
                AND E.ELEMENT_TYPE = 'K'
                AND UPPER(VW.TYPE) LIKE '%LAG%'
                AND TCE.ELEMENT_ID = E.ELEMENT_ID
                AND VW.A_SITE_ID = ASITE.SITE_REFERENCE_ID (+)
                AND VW.Z_SITE_ID = ZSITE.SITE_REFERENCE_ID (+)
            UNION
            SELECT DISTINCT
                VW.TRAIL_ID,
                VW.TRAIL_NAME,
                VW.STATUS,
                VW.TYPE,
                VW.VERSION,
                TCE.SEQUENCE,
                NULL          PORT_REFERENCE_ID,
                E.SOURCE,
                VW.Z_PORT_AID,
                NULL          AS SEQUENCE_NUMBER,
                VW.STATUS     AS PORT_STATUS,
                E.ELEMENT_TYPE,
                VW.PROJECT_ID,
                ASITE.CLLI    ACLLI,
                ZSITE.CLLI    ZCLLI,
                SYSTIMESTAMP,
				VW.Z_EQUIPMENT_ID
            FROM
                VNADSPRD.NAUT_TRAIL_ELEMENT           E,
                VNADSPRD.NAUT_TRAIL_TMP               VW,
                VNADSPRD.NAUT_TRAIL_COMPONENT_ELEMENT TCE,
                VNADSPRD.ICOE_SITE_TBL                ASITE,
                VNADSPRD.ICOE_SITE_TBL                ZSITE
            WHERE
                    VW.TRAIL_ID = E.TRAIL_ID
                AND E.ELEMENT_TYPE = 'K'
                AND UPPER(VW.TYPE) LIKE '%LAG%'
                AND TCE.ELEMENT_ID = E.ELEMENT_ID
                AND VW.A_SITE_ID = ASITE.SITE_REFERENCE_ID (+)
                AND VW.Z_SITE_ID = ZSITE.SITE_REFERENCE_ID (+);
        COMMIT;
  dbms_output.put_line('Step 7 end  : truncate and insert IEN_MARKET_INPUT_LIST_STG_TMP   - ' || systimestamp);
/***** Load IEN_MARKET_INPUT_LIST********/
  dbms_output.put_line('Step 8 start  : truncate and insert IEN_MARKET_INPUT_LIST   - ' || systimestamp);

        EXECUTE IMMEDIATE 'TRUNCATE TABLE vnadsprd.IEN_MARKET_INPUT_LIST';
        EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IEN_TRAIL_NAME_IDX UNUSABLE';
		dbms_output.put_line('vnadsprd.IEN_MARKET_INPUT_LIST tbl load started ' || to_char(sysdate, 'DD-MON-YYYY HH24:MI:SS'));
        INSERT INTO VNADSPRD.IEN_MARKET_INPUT_LIST (
TRAIL_ID,
MARKET_NAME	,
MARKET_STATE	,
CLLI	,
TRAIL_NAME	,
TID_LOGICAL	,
ALTERNATE_NAME	,
NFID	,
CREATED_TS
)
select distinct
TRAIL_ID,
nvl(market.VZB_MARKET_CITY_OR_VZT_REGION,'Not Available') market_name,
case when market.STATE is not null then market.STATE
else nvl(SUBSTR(S.CLLI,5,2),'N/A') end market_area,
nvl(S.CLLI,'N/A') AS EQUIP_CLLI,
G.TRAIL_NAME AS TRAIl_NAME,
(case when (E.SHELF_TYPE = 'MSERI') then MSE.TID_LOGICAL else E.TID_LOGICAL end) AS TID_LOGICAL,
(case when (E.SHELF_TYPE = 'MSERI') then MSE.ALTERNATE_NAME else E.ALTERNATE_NAME end) AS ALTERNATE_NAME,
nvl(G.NF_ID,'N/A') NF_ID,
SYSTIMESTAMP
from (((((select * from vnadsprd.IEN_MARKET_INPUT_LIST_STG where  element_type <> 'K') G join
vnadsprd.icoe_pvnr_t_logical_port P on((P.PORT_REFERENCE_ID = G.PORT_REFERENCE_ID)))
--join vnadsprd.ICOE_MV_EQUIPMENT E on((P.EQP_REFERENCE_ID = E.EQP_REFERENCE_ID))) Changed to NAR Table
join vnadsprd.ICOE_EQUIPMENT_TBL E on((P.EQP_REFERENCE_ID = E.EQP_REFERENCE_ID)))
--left join vnadsprd.ICOE_MV_EQUIPMENT MSE on((E.PHYSICAL_EQUIPMENT_REFERENC_ID = MSE.EQP_REFERENCE_ID))) Changed to NAR Table
left join vnadsprd.ICOE_EQUIPMENT_TBL MSE on((E.PHYSICAL_EQUIPMENT_REFERENC_ID = MSE.EQP_REFERENCE_ID)))
--join vnadsprd.ICOE_MV_SITE S on((E.SITE_REFERENCE_ID = S.SITE_REFERENCE_ID))) Changed to NAR Table
join vnadsprd.ICOE_SITE_TBL S on((E.SITE_REFERENCE_ID = S.SITE_REFERENCE_ID)))
left join vnadsprd.IEN_UT_PMO_TRACKER MARKET on (MARKET.SITE_CLLI = S.CLLI);
COMMIT;
  dbms_output.put_line('Step 8 end  : truncate and insert1 IEN_MARKET_INPUT_LIST   - ' || systimestamp);

    dbms_output.put_line('Step 9 start  : gather stats on  IEN_MARKET_INPUT_LIST   - ' || systimestamp);

--Stats Gathering
BEGIN
                dbms_stats.gather_table_stats(ownname => 'VNADSPRD', tabname => 'IEN_MARKET_INPUT_LIST', degree => 16, method_opt
                => 'FOR ALL COLUMNS SIZE 1', cascade => true);
            END;

    dbms_output.put_line('Step 9 end  : gather stats on  IEN_MARKET_INPUT_LIST   - ' || systimestamp);


  dbms_output.put_line('Step 10 start  : 2nd insert on IEN_MARKET_INPUT_LIST   - ' || systimestamp);
INSERT INTO VNADSPRD.IEN_MARKET_INPUT_LIST (
TRAIL_ID,
MARKET_NAME	,
MARKET_STATE	,
CLLI	,
TRAIL_NAME	,
TID_LOGICAL	,
ALTERNATE_NAME	,
NFID	,
CREATED_TS
)
select  DISTINCT
TRAIL_ID,
nvl(market.VZB_MARKET_CITY_OR_VZT_REGION,'Not Available') market_name,
case when market.STATE is not null then market.STATE
else nvl(SUBSTR(S.CLLI,5,2),'N/A') end market_area,
nvl(S.CLLI,'N/A') AS EQUIP_CLLI,
G.TRAIL_NAME AS TRAIl_NAME,
(case when (E.SHELF_TYPE = 'MSERI') then MSE.TID_LOGICAL else E.TID_LOGICAL end) AS TID_LOGICAL,
(case when (E.SHELF_TYPE = 'MSERI') then MSE.ALTERNATE_NAME else E.ALTERNATE_NAME end) AS ALTERNATE_NAME,
nvl(G.NF_ID,'N/A') NF_ID,
SYSTIMESTAMP
from ((((select * from vnadsprd.IEN_MARKET_INPUT_LIST_STG_TMP where  element_type = 'K') G
join vnadsprd.ICOE_EQUIPMENT_TBL E on((E.EQP_REFERENCE_ID = G.EQUIPMENT_ID)))
left join vnadsprd.ICOE_EQUIPMENT_TBL MSE on((E.PHYSICAL_EQUIPMENT_REFERENC_ID = MSE.EQP_REFERENCE_ID)))
join vnadsprd.ICOE_SITE_TBL S on((E.SITE_REFERENCE_ID = S.SITE_REFERENCE_ID)))
left join vnadsprd.IEN_UT_PMO_TRACKER MARKET on (MARKET.SITE_CLLI = S.CLLI);
 COMMIT;
  dbms_output.put_line('Step 10 end  : 2nd insert on IEN_MARKET_INPUT_LIST   - ' || systimestamp);
     ---   EXECUTE IMMEDIATE 'create index vnadsprd.IEN_TRAIL_NAME_IDX on vnadsprd.IEN_MARKET_INPUT_LIST(TRAIL_NAME)';
	EXECUTE IMMEDIATE 'ALTER INDEX VNADSPRD.IEN_TRAIL_NAME_IDX REBUILD';
END;