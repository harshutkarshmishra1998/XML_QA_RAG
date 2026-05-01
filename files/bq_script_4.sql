-- BLOCK 1
-- complexity_score: 0
-- used_llm: False
CREATE OR REPLACE EDITIONABLE PROCEDURE "VNADSPRD"."SP_IEN_ICON_TBLS_REFRESH" AS
    v_icoe_equipment_tbl   NUMBER

-- BLOCK 2
-- complexity_score: 0
-- used_llm: False
v_icoe_card_tbl        NUMBER

-- BLOCK 3
-- complexity_score: 0
-- used_llm: False
v_icoe_slot_tbl        NUMBER

-- BLOCK 4
-- complexity_score: 0
-- used_llm: False
v_icoe_site_tbl        NUMBER

-- BLOCK 5
-- complexity_score: 1
-- used_llm: False
-- note: Applied deterministic function mapping (NVL/SYSDATE/etc.).
BEGIN

    dbms_output.put_line('Step 1 start : create ICOE_EQUIPMENT_TBL temp table   - ' || CURRENT_TIMESTAMP())

-- BLOCK 6
-- complexity_score: 0
-- used_llm: False
-- note: Qualified simple table references.
SELECT
        COUNT(*)
    INTO `your_project.your_dataset.v_icoe_equipment_tbl`
    FROM
        `your_project.your_dataset.all_tables`
    WHERE
        table_name = 'ICOE_EQUIPMENT_TBL'

-- BLOCK 7
-- complexity_score: 4
-- used_llm: True
```sql
BEGIN
  IF v_icoe_equipment_tbl = 1 THEN
    EXECUTE IMMEDIATE 'DROP TABLE VNADSPRD.ICOE_EQUIPMENT_TBL PURGE';
  END IF;
END;
```

-- BLOCK 8
-- complexity_score: 4
-- used_llm: True
-- note: Qualified simple table references.
```sql
BEGIN
  EXECUTE IMMEDIATE """
    CREATE TABLE VNADSPRD.ICOE_EQUIPMENT_TBL
    OPTIONS (partition_expiration_days=60, description='Table created by script')
    AS
    SELECT * FROM `your_project.your_dataset.VNADSPRD`.icoe_mv_logical_equipment
  """;
END;
```

-- BLOCK 9
-- complexity_score: 4
-- used_llm: True
```sql
GRANT SELECT ON VNADSPRD.ICOE_EQUIPMENT_TBL TO VNA_SELECT
```

-- BLOCK 10
-- complexity_score: 4
-- used_llm: True
-- note: Qualified simple table references.
```sql
BEGIN
  EXECUTE IMMEDIATE """
    CREATE TABLE VNADSPRD.ICOE_EQUIPMENT_TBL
    OPTIONS (partition_expiration_days=60, description='Table created by script')
    AS
    SELECT * FROM `your_project.your_dataset.VNADSPRD`.icoe_mv_logical_equipment
  """;
END;
```

-- BLOCK 11
-- complexity_score: 4
-- used_llm: True
```sql
GRANT SELECT ON VNADSPRD.ICOE_EQUIPMENT_TBL TO VNA_SELECT
```

-- BLOCK 12
-- complexity_score: 0
-- used_llm: False
END IF

-- BLOCK 13
-- complexity_score: 4
-- used_llm: True
```sql
CREATE INDEX IF NOT EXISTS VNADSPRD.IDX_ICOE_EQUIP_TBL
ON VNADSPRD.ICOE_EQUIPMENT_TBL(PHYSICAL_EQUIPMENT_REFERENC_ID);
```

-- BLOCK 14
-- complexity_score: 4
-- used_llm: True
```sql
CREATE INDEX IF NOT EXISTS IDX1_ICOE_EQUIP_TBL
ON VNADSPRD.ICOE_EQUIPMENT_TBL(PARENT_EQP_REFERENCE_ID);
```

-- BLOCK 15
-- complexity_score: 4
-- used_llm: True
```sql
CREATE INDEX IF NOT EXISTS VNADSPRD.IDX2_ICOE_EQUIP_TBL
ON VNADSPRD.ICOE_EQUIPMENT_TBL(SHELF_TYPE);
```

-- BLOCK 16
-- complexity_score: 4
-- used_llm: True
```sql
CREATE INDEX IF NOT EXISTS VNADSPRD.IDX3_ICOE_EQUIP_TBL
ON VNADSPRD.ICOE_EQUIPMENT_TBL(SITE_REFERENCE_ID);
```

-- BLOCK 17
-- complexity_score: 4
-- used_llm: True
```sql
CREATE INDEX IF NOT EXISTS VNADSPRD.IDX4_ICOE_EQUIP_TBL
ON VNADSPRD.ICOE_EQUIPMENT_TBL(CONTAINER);
```

-- BLOCK 18
-- complexity_score: 0
-- used_llm: False
-- note: Applied deterministic function mapping (NVL/SYSDATE/etc.).
dbms_output.put_line('Step 1 end : create ICOE_EQUIPMENT_TBL temp table   - ' || CURRENT_TIMESTAMP())

-- BLOCK 19
-- complexity_score: 0
-- used_llm: False
-- note: Applied deterministic function mapping (NVL/SYSDATE/etc.).
dbms_output.put_line('Step 2 start : create ICOE_CARD_TBL temp table   - ' || CURRENT_TIMESTAMP())

-- BLOCK 20
-- complexity_score: 0
-- used_llm: False
-- note: Qualified simple table references.
SELECT
        COUNT(*)
    INTO `your_project.your_dataset.v_icoe_card_tbl`
    FROM
        `your_project.your_dataset.all_tables`
    WHERE
        table_name = 'ICOE_CARD_TBL'

-- BLOCK 21
-- complexity_score: 4
-- used_llm: True
```sql
BEGIN
  IF v_icoe_card_tbl = 1 THEN
    EXECUTE IMMEDIATE 'DROP TABLE VNADSPRD.ICOE_CARD_TBL PURGE';
  END IF;
END;
```

-- BLOCK 22
-- complexity_score: 4
-- used_llm: True
-- note: Qualified simple table references.
```sql
CREATE TABLE VNADSPRD.ICOE_CARD_TBL
OPTIONS(description = 'VNADSPRD.ICOE_CARD_TBL')
AS
SELECT * FROM `your_project.your_dataset.VNADSPRD`.icoe_mv_logical_card
OPTIONS(parallel_processing = 8);
```

-- BLOCK 23
-- complexity_score: 4
-- used_llm: True
```sql
EXECUTE IMMEDIATE "GRANT SELECT ON VNADSPRD.ICOE_CARD_TBL TO VNA_SELECT";
```

-- BLOCK 24
-- complexity_score: 4
-- used_llm: True
-- note: Qualified simple table references.
```sql
BEGIN
  EXECUTE IMMEDIATE """
    CREATE TABLE VNADSPRD.ICOE_CARD_TBL
    OPTIONS (partition_expiration_days=60, description='Table created by script')
    AS
    SELECT * FROM `your_project.your_dataset.VNADSPRD`.icoe_mv_logical_card
  """;
END;
```

-- BLOCK 25
-- complexity_score: 4
-- used_llm: True
```sql
EXECUTE IMMEDIATE 'GRANT SELECT ON VNADSPRD.ICOE_CARD_TBL TO VNA_SELECT';
```

-- BLOCK 26
-- complexity_score: 0
-- used_llm: False
END IF

-- BLOCK 27
-- complexity_score: 4
-- used_llm: True
```sql
CREATE INDEX IF NOT EXISTS VNADSPRD.IDX_ICOE_CARD_TBL
ON VNADSPRD.ICOE_CARD_TBL(CARD_REFERENCE_ID);
```

-- BLOCK 28
-- complexity_score: 4
-- used_llm: True
```sql
CREATE INDEX IF NOT EXISTS VNADSPRD.IDX1_ICOE_CARD_TBL
ON VNADSPRD.ICOE_CARD_TBL(SLOT_REFERENCE_ID);
```

-- BLOCK 29
-- complexity_score: 4
-- used_llm: True
```sql
CREATE INDEX IF NOT EXISTS VNADSPRD.IDX2_ICOE_CARD_TBL
ON VNADSPRD.ICOE_CARD_TBL(PARENT_CARD_REF_ID);
```

-- BLOCK 30
-- complexity_score: 0
-- used_llm: False
-- note: Applied deterministic function mapping (NVL/SYSDATE/etc.).
dbms_output.put_line('Step 2 end : create ICOE_CARD_TBL temp table   - ' || CURRENT_TIMESTAMP())

-- BLOCK 31
-- complexity_score: 0
-- used_llm: False
-- note: Applied deterministic function mapping (NVL/SYSDATE/etc.).
dbms_output.put_line('Step 3 start : create ICOE_SLOT_TBL temp table   - ' || CURRENT_TIMESTAMP())

-- BLOCK 32
-- complexity_score: 0
-- used_llm: False
-- note: Qualified simple table references.
SELECT
        COUNT(*)
    INTO `your_project.your_dataset.v_icoe_slot_tbl`
    FROM
        `your_project.your_dataset.all_tables`
    WHERE
        table_name = 'ICOE_SLOT_TBL'

-- BLOCK 33
-- complexity_score: 4
-- used_llm: True
```sql
BEGIN
  IF v_icoe_slot_tbl = 1 THEN
    EXECUTE IMMEDIATE 'DROP TABLE VNADSPRD.ICOE_SLOT_TBL PURGE';
  END IF;
END;
```

-- BLOCK 34
-- complexity_score: 4
-- used_llm: True
-- note: Qualified simple table references.
```sql
BEGIN
  EXECUTE IMMEDIATE """
    CREATE TABLE VNADSPRD.ICOE_SLOT_TBL
    OPTIONS (partition_expiration_days=60, description='Table created by script')
    AS
    SELECT * FROM `your_project.your_dataset.VNADSPRD`.ICOE_PVNR_MV_LOGICAL_SLOT
  """;
END;
```

-- BLOCK 35
-- complexity_score: 4
-- used_llm: True
```sql
GRANT SELECT ON VNADSPRD.ICOE_SLOT_TBL TO VNA_SELECT
```

-- BLOCK 36
-- complexity_score: 4
-- used_llm: True
-- note: Qualified simple table references.
```sql
BEGIN
  EXECUTE IMMEDIATE """
    CREATE TABLE VNADSPRD.ICOE_SLOT_TBL
    OPTIONS (partition_expiration_days=60, description='Table created by script')
    AS
    SELECT * FROM `your_project.your_dataset.VNADSPRD`.ICOE_PVNR_MV_LOGICAL_SLOT
  """;
END;
```

-- BLOCK 37
-- complexity_score: 4
-- used_llm: True
```sql
EXECUTE IMMEDIATE "GRANT SELECT ON VNADSPRD.ICOE_SLOT_TBL TO VNA_SELECT";
```

-- BLOCK 38
-- complexity_score: 0
-- used_llm: False
END IF

-- BLOCK 39
-- complexity_score: 4
-- used_llm: True
```sql
CREATE INDEX IF NOT EXISTS IDX_SLOT_TBL ON VNADSPRD.ICOE_SLOT_TBL(SLOT_REFERENCE_ID)
```

-- BLOCK 40
-- complexity_score: 0
-- used_llm: False
-- note: Applied deterministic function mapping (NVL/SYSDATE/etc.).
dbms_output.put_line('Step 3 end : create ICOE_SLOT_TBL temp table   - ' || CURRENT_TIMESTAMP())

-- BLOCK 41
-- complexity_score: 0
-- used_llm: False
-- note: Applied deterministic function mapping (NVL/SYSDATE/etc.).
dbms_output.put_line('Step 4 start : create ICOE_SITE_TBL temp table   - ' || CURRENT_TIMESTAMP())

-- BLOCK 42
-- complexity_score: 0
-- used_llm: False
-- note: Qualified simple table references.
SELECT
        COUNT(*)
    INTO `your_project.your_dataset.v_icoe_site_tbl`
    FROM
        `your_project.your_dataset.all_tables`
    WHERE
        table_name = 'ICOE_SITE_TBL'

-- BLOCK 43
-- complexity_score: 4
-- used_llm: True
```sql
BEGIN
  IF v_icoe_site_tbl = 1 THEN
    EXECUTE IMMEDIATE 'DROP TABLE VNADSPRD.ICOE_SITE_TBL PURGE';
  END IF;
END;
```

-- BLOCK 44
-- complexity_score: 4
-- used_llm: True
-- note: Qualified simple table references.
```sql
CREATE TABLE VNADSPRD.ICOE_SITE_TBL
OPTIONS(description = 'Created from ICOE_MV_SITE')
AS
SELECT * FROM `your_project.your_dataset.VNADSPRD`.ICOE_MV_SITE
OPTIONS(parallel_processing = 8);
```

-- BLOCK 45
-- complexity_score: 4
-- used_llm: True
```sql
GRANT SELECT ON VNADSPRD.ICOE_SITE_TBL TO VNA_SELECT
```

-- BLOCK 46
-- complexity_score: 4
-- used_llm: True
-- note: Qualified simple table references.
```sql
BEGIN
  EXECUTE IMMEDIATE """
    CREATE TABLE VNADSPRD.ICOE_SITE_TBL
    OPTIONS (partition_expiration_days=60, description=' ICOE_SITE_TBL')
    AS
    SELECT * FROM `your_project.your_dataset.VNADSPRD`.ICOE_MV_SITE
  """;
END;
```

-- BLOCK 47
-- complexity_score: 4
-- used_llm: True
```sql
GRANT SELECT ON VNADSPRD.ICOE_SITE_TBL TO VNA_SELECT
```

-- BLOCK 48
-- complexity_score: 0
-- used_llm: False
END IF

-- BLOCK 49
-- complexity_score: 4
-- used_llm: True
```sql
CREATE INDEX IF NOT EXISTS IDX_SITE_TBL ON VNADSPRD.ICOE_SITE_TBL(SITE_REFERENCE_ID)
```

-- BLOCK 50
-- complexity_score: 4
-- used_llm: True
```sql
CREATE INDEX IF NOT EXISTS IDX1_SITE_TBL ON VNADSPRD.ICOE_SITE_TBL(CLLI)
OPTIONS(ignore_unknown_options = true)
```

-- BLOCK 51
-- complexity_score: 0
-- used_llm: False
-- note: Applied deterministic function mapping (NVL/SYSDATE/etc.).
dbms_output.put_line('Step 4 end  : create ICOE_SITE_TBL temp table   - ' || CURRENT_TIMESTAMP())

-- BLOCK 52
-- complexity_score: 0
-- used_llm: False
-- note: Applied deterministic function mapping (NVL/SYSDATE/etc.).
dbms_output.put_line('Step 5 start  : gather stats on temp tables   - ' || CURRENT_TIMESTAMP())

-- BLOCK 53
-- complexity_score: 0
-- used_llm: False
dbms_stats.gather_table_stats(ownname => 'VNADSPRD', tabname => 'ICOE_CARD_TBL', degree => 16, method_opt => 'FOR ALL COLUMNS SIZE 1'
    , cascade => true)

-- BLOCK 54
-- complexity_score: 0
-- used_llm: False
dbms_stats.gather_table_stats(ownname => 'VNADSPRD', tabname => 'ICOE_SLOT_TBL', degree => 16, method_opt => 'FOR ALL COLUMNS SIZE 1'
    , cascade => true)

-- BLOCK 55
-- complexity_score: 0
-- used_llm: False
dbms_stats.gather_table_stats(ownname => 'VNADSPRD', tabname => 'ICOE_SITE_TBL', degree => 16, method_opt => 'FOR ALL COLUMNS SIZE 1'
    , cascade => true)

-- BLOCK 56
-- complexity_score: 0
-- used_llm: False
dbms_stats.gather_table_stats(ownname => 'VNADSPRD', tabname => 'ICOE_EQUIPMENT_TBL', degree => 16, method_opt => 'FOR ALL COLUMNS SIZE 1'
    , cascade => true)

-- BLOCK 57
-- complexity_score: 0
-- used_llm: False
-- note: Applied deterministic function mapping (NVL/SYSDATE/etc.).
dbms_output.put_line('Step 5 end  : gather stats on temp tables   - ' || CURRENT_TIMESTAMP())

-- BLOCK 58
-- complexity_score: 5
-- used_llm: True
-- note: Applied deterministic function mapping (NVL/SYSDATE/etc.).
-- note: Detected Oracle outer join markers and annotated them for LLM conversion.
-- note: Qualified simple table references.
```sql
BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE `your_project.your_dataset.vnadsprd`.IEN_MR_CHANNEL_TEMP';
  EXECUTE IMMEDIATE 'ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX1_IEN_MR_CHANNEL_TEMP SET DISABLE';
  EXECUTE IMMEDIATE 'ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX2_IEN_MR_CHANNEL_TEMP SET DISABLE';

  INSERT INTO `your_project.your_dataset.vnadsprd`.IEN_MR_CHANNEL_TEMP (
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
  )
  SELECT 
    VW.TRAIL_ID,
    VW.TRAIL_NAME,
    VW.TYPE,
    VW.STATUS,
    VW.VERSION,
    TE.PARENT_TRAIL_ID,
    MIN(CHANNEL_NAME) CHANNEL_NAME,
    VW.PROJECT_ID,
    ASITE.CLLI ACLLI,
    ZSITE.CLLI ZCLLI
  FROM 
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_ELEMENT TE,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL VW,
    `your_project.your_dataset.vnadsprd`.ICOE_SITE_TBL ASITE,
    `your_project.your_dataset.vnadsprd`.ICOE_SITE_TBL ZSITE
  WHERE 
    VW.TRAIL_ID = TE.TRAIL_ID
    AND TE.ELEMENT_TYPE = 'P'
    AND VW.A_SITE_ID = ASITE.SITE_REFERENCE_ID
    AND VW.Z_SITE_ID = ZSITE.SITE_REFERENCE_ID
  GROUP BY 
    VW.TRAIL_ID,
    VW.TRAIL_NAME,
    VW.TYPE,
    VW.STATUS,
    VW.VERSION,
    TE.PARENT_TRAIL_ID,
    VW.PROJECT_ID,
    ASITE.CLLI,
    ZSITE.CLLI;

  EXECUTE IMMEDIATE 'ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX1_IEN_MR_CHANNEL_TEMP SET ENABLE';
  EXECUTE IMMEDIATE 'ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX2_IEN_MR_CHANNEL_TEMP SET ENABLE';

  EXECUTE IMMEDIATE 'TRUNCATE TABLE `your_project.your_dataset.vnadsprd`.IEN_MARKET_INPUT_LIST_STG';
  EXECUTE IMMEDIATE 'ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX1_IEN_MARKET_INPUT_LIST_STG SET DISABLE';
  EXECUTE IMMEDIATE 'ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX2_IEN_MARKET_INPUT_LIST_STG SET DISABLE';
  EXECUTE IMMEDIATE 'ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX3_IEN_MARKET_INPUT_LIST_STG SET DISABLE';

  INSERT INTO `your_project.your_dataset.vnadsprd`.IEN_MARKET_INPUT_LIST_STG (
    TRAIL_ID,
    TRAIL_NAME,
    STATUS,
    TYPE,
    VERSION,
    SEQUENCE,
    PORT_REFERENCE_ID,
    SOURCE,
    CHANNEL_NAME,
    SEQUENCE_NUMBER,
    PORT_STATUS,
    ELEMENT_TYPE,
    NF_ID,
    ACLLI,
    ZCLLI,
    LAST_REFRESHED_TS,
    EQUIPMENT_ID
  )
  SELECT DISTINCT 
    VW.TRAIL_ID,
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
    CURRENT_TIMESTAMP(),
    NULL AS EQUIPMENT_ID
  FROM 
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_ELEMENT E,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL VW,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_COMPONENT_ELEMENT TCE,
    `your_project.your_dataset.vnadsprd`.ICOE_SITE_TBL ASITE,
    `your_project.your_dataset.vnadsprd`.ICOE_SITE_TBL ZSITE
  WHERE 
    VW.TRAIL_ID = E.TRAIL_ID
    AND E.ELEMENT_TYPE = 'E'
    AND E.SOURCE IN ('IVAPP_PORT', 'IVAPP_LOGICAL', 'IVAPP_PANEL')
    AND TCE.ELEMENT_ID = E.ELEMENT_ID
    AND VW.A_SITE_ID = ASITE.SITE_REFERENCE_ID
    AND VW.Z_SITE_ID = ZSITE.SITE_REFERENCE_ID

  UNION ALL
  SELECT 
    X.TRAIL_ID,
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
    CURRENT_TIMESTAMP(),
    NULL AS EQUIPMENT_ID
  FROM 
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL T,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_CHANNEL TC,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_COMPONENT_ELEMENT TCE,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_ELEMENT TE,
    `your_project.your_dataset.vnadsprd`.IEN_MR_CHANNEL_TEMP X
  WHERE 
    T.TRAIL_ID = X.PARENT_TRAIL_ID
    AND TC.PARENT_TRAIL_ID = X.PARENT_TRAIL_ID
    AND TC.CHANNEL_NAME = TO_CHAR(X.CHANNEL_NAME)
    AND TE.CHANNEL_NAME = TO_CHAR(X.CHANNEL_NAME)
    AND TE.PARENT_TRAIL_ID = X.PARENT_TRAIL_ID
    AND TCE.ELEMENT_ID = TE.ELEMENT_ID

  UNION ALL
  SELECT 
    X.TRAIL_ID,
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
    CURRENT_TIMESTAMP(),
    NULL AS EQUIPMENT_ID
  FROM 
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL T,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_CHANNEL TC,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_COMPONENT_ELEMENT TCE,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_ELEMENT TE,
    `your_project.your_dataset.vnadsprd`.IEN_MR_CHANNEL_TEMP X
  WHERE 
    T.TRAIL_ID = X.PARENT_TRAIL_ID
    AND TC.PARENT_TRAIL_ID = X.PARENT_TRAIL_ID
    AND TC.CHANNEL_NAME = TO_CHAR(X.CHANNEL_NAME)
    AND TE.CHANNEL_NAME = TO_CHAR(X.CHANNEL_NAME)
    AND TE.PARENT_TRAIL_ID = X.PARENT_TRAIL_ID
    AND TCE.ELEMENT_ID = TE.ELEMENT_ID

  UNION ALL
  SELECT 
    X.TRAIL_ID,
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
    CURRENT_TIMESTAMP(),
    NULL AS EQUIPMENT_ID
  FROM 
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL T,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_CHANNEL TC,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_COMPONENT_ELEMENT TCE,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_ELEMENT TE,
    `your_project.your_dataset.vnadsprd`.IEN_MR_CHANNEL_TEMP X
  WHERE 
    T.TRAIL_ID = X.PARENT_TRAIL_ID
    AND TC.PARENT_TRAIL_ID = X.PARENT_TRAIL_ID
    AND TC.CHANNEL_NAME = TO_CHAR(X.CHANNEL_NAME)
    AND TE.CHANNEL_NAME = TO_CHAR(X.CHANNEL_NAME)
    AND TE.PARENT_TRAIL_ID = X.PARENT_TRAIL_ID
    AND TCE.ELEMENT_ID = TE.ELEMENT_ID

  UNION ALL
  SELECT 
    X.TRAIL_ID,
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
    CURRENT_TIMESTAMP(),
    NULL AS EQUIPMENT_ID
  FROM 
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL T,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_CHANNEL TC,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_COMPONENT_ELEMENT TCE,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_ELEMENT TE,
    `your_project.your_dataset.vnadsprd`.IEN_MR_CHANNEL_TEMP X
  WHERE 
    T.TRAIL_ID = X.PARENT_TRAIL_ID
    AND TC.PARENT_TRAIL_ID = X.PARENT_TRAIL_ID
    AND TC.CHANNEL_NAME = TO_CHAR(X.CHANNEL_NAME)
    AND TE.CHANNEL_NAME = TO_CHAR(X.CHANNEL_NAME)
    AND TE.PARENT_TRAIL_ID = X.PARENT_TRAIL_ID
    AND TCE.ELEMENT_ID = TE.ELEMENT_ID;

  EXECUTE IMMEDIATE 'ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX1_IEN_MARKET_INPUT_LIST_STG SET ENABLE';
  EXECUTE IMMEDIATE 'ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX2_IEN_MARKET_INPUT_LIST_STG SET ENABLE';
  EXECUTE IMMEDIATE 'ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX3_IEN_MARKET_INPUT_LIST_STG SET ENABLE';

  EXECUTE IMMEDIATE 'TRUNCATE TABLE `your_project.your_dataset.vnadsprd`.IEN_MARKET_INPUT_LIST_STG_TMP';

  INSERT INTO `your_project.your_dataset.vnadsprd`.IEN_MARKET_INPUT_LIST_STG_TMP (
    TRAIL_ID,
    TRAIL_NAME,
    STATUS,
    TYPE,
    VERSION,
    SEQUENCE,
    PORT_REFERENCE_ID,
    SOURCE,
    CHANNEL_NAME,
    SEQUENCE_NUMBER,
    PORT_STATUS,
    ELEMENT_TYPE,
    NF_ID,
    ACLLI,
    ZCLLI,
    LAST_REFRESHED_TS,
    EQUIPMENT_ID
  )
  SELECT DISTINCT 
    VW.TRAIL_ID,
    VW.TRAIL_NAME,
    VW.STATUS,
    VW.TYPE,
    VW.VERSION,
    TCE.SEQUENCE,
    NULL PORT_REFERENCE_ID,
    E.SOURCE,
    VW.A_PORT_AID,
    NULL AS SEQUENCE_NUMBER,
    VW.STATUS AS PORT_STATUS,
    E.ELEMENT_TYPE,
    VW.PROJECT_ID,
    ASITE.CLLI ACLLI,
    ZSITE.CLLI ZCLLI,
    CURRENT_TIMESTAMP(),
    VW.A_EQUIPMENT_ID
  FROM 
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_ELEMENT E,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_TMP VW,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_COMPONENT_ELEMENT TCE,
    `your_project.your_dataset.vnadsprd`.ICOE_SITE_TBL ASITE,
    `your_project.your_dataset.vnadsprd`.ICOE_SITE_TBL ZSITE
  WHERE 
    VW.TRAIL_ID = E.TRAIL_ID
    AND E.ELEMENT_TYPE = 'K'
    AND UPPER(VW.TYPE) LIKE '%LAG%'
    AND TCE.ELEMENT_ID = E.ELEMENT_ID
    AND VW.A_SITE_ID = ASITE.SITE_REFERENCE_ID
    AND VW.Z_SITE_ID = ZSITE.SITE_REFERENCE_ID

  UNION ALL
  SELECT DISTINCT 
    VW.TRAIL_ID,
    VW.TRAIL_NAME,
    VW.STATUS,
    VW.TYPE,
    VW.VERSION,
    TCE.SEQUENCE,
    NULL PORT_REFERENCE_ID,
    E.SOURCE,
    VW.Z_PORT_AID,
    NULL AS SEQUENCE_NUMBER,
    VW.STATUS AS PORT_STATUS,
    E.ELEMENT_TYPE,
    VW.PROJECT_ID,
    ASITE.CLLI ACLLI,
    ZSITE.CLLI ZCLLI,
    CURRENT_TIMESTAMP(),
    VW.Z_EQUIPMENT_ID
  FROM 
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_ELEMENT E,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_TMP VW,
    `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_COMPONENT_ELEMENT TCE,
    `your_project.your_dataset.vnadsprd`.ICOE_SITE_TBL ASITE,
    `your_project.your_dataset.vnadsprd`.ICOE_SITE_TBL ZSITE
  WHERE 
    VW.TRAIL_ID = E.TRAIL_ID
    AND E.ELEMENT_TYPE = 'K'
    AND UPPER(VW.TYPE) LIKE '%LAG%'
    AND TCE.ELEMENT_ID = E.ELEMENT_ID
    AND VW.A_SITE_ID = ASITE.SITE_REFERENCE_ID
    AND VW.Z_SITE_ID = ZSITE.SITE_REFERENCE_ID;

  EXECUTE IMMEDIATE 'TRUNCATE TABLE `your_project.your_dataset.vnadsprd`.IEN_MARKET_INPUT_LIST';

  EXECUTE IMMEDIATE 'ALTER INDEX `your_project.your_dataset.vnadsprd`.IEN_TRAIL_NAME_IDX SET DISABLE';

  INSERT INTO `your_project.your_dataset.vnadsprd`.IEN_MARKET_INPUT_LIST (
    TRAIL_ID,
    MARKET_NAME,
    MARKET_STATE,
    CLLI,
    TRAIL_NAME,
    TID_LOGICAL,
    ALTERNATE_NAME,
    NFID,
    CREATED_TS
  )
  SELECT DISTINCT 
    TRAIL_ID,
    IFNULL(market.VZB_MARKET_CITY_OR_VZT_REGION, 'Not Available') market_name,
    CASE 
      WHEN market.STATE IS NOT NULL THEN market.STATE
      ELSE IFNULL(SUBSTR(S.CLLI, 5, 2), 'N/A')
    END market_area,
    IFNULL(S.CLLI, 'N/A') AS EQUIP_CLLI,
    G.TRAIL_NAME AS TRAIL_NAME,
    CASE 
      WHEN (E.SHELF_TYPE = 'MSERI') THEN MSE.TID_LOGICAL
      ELSE E.TID_LOGICAL
    END AS TID_LOGICAL,
    CASE 
      WHEN (E.SHELF_TYPE = 'MSERI') THEN MSE.ALTERNATE_NAME
      ELSE E.ALTERNATE_NAME
    END AS ALTERNATE_NAME,
    IFNULL(G.NF_ID, 'N/A') NF_ID,
    CURRENT_TIMESTAMP()
  FROM 
    (((((SELECT * FROM `your_project.your_dataset.vnadsprd`.IEN_MARKET_INPUT_LIST_STG WHERE ELEMENT_TYPE <> 'K') G
    JOIN `your_project.your_dataset.vnadsprd`.ICOE_PVNR_T_LOGICAL_PORT P ON (P.PORT_REFERENCE_ID = G.PORT_REFERENCE_ID))
    JOIN `your_project.your_dataset.vnadsprd`.ICOE_EQUIPMENT_TBL E ON (P.EQP_REFERENCE_ID = E.EQP_REFERENCE_ID))
    LEFT JOIN `your_project.your_dataset.vnadsprd`.ICOE_EQUIPMENT_TBL MSE ON (E.PHYSICAL_EQUIPMENT_REFERENC_ID = MSE.EQP_REFERENCE_ID))
    JOIN `your_project.your_dataset.vnadsprd`.ICOE_SITE_TBL S ON (E.SITE_REFERENCE_ID = S.SITE_REFERENCE_ID))
    LEFT JOIN `your_project.your_dataset.vnadsprd`.IEN_UT_PMO_TRACKER MARKET ON (MARKET.SITE_CLLI = S.CLLI);

  INSERT INTO `your_project.your_dataset.vnadsprd`.IEN_MARKET_INPUT_LIST (
    TRAIL_ID,
    MARKET_NAME,
    MARKET_STATE,
    CLLI,
    TRAIL_NAME,
    TID_LOGICAL,
    ALTERNATE_NAME,
    NFID,
    CREATED_TS
  )
  SELECT DISTINCT 
    TRAIL_ID,
    IFNULL(market.VZB_MARKET_CITY_OR_VZT_REGION, 'Not Available') market_name,
    CASE 
      WHEN market.STATE IS NOT NULL THEN market.STATE
      ELSE IFNULL(SUBSTR(S.CLLI, 5, 2), 'N/A')
    END market_area,
    IFNULL(S.CLLI, 'N/A') AS EQUIP_CLLI,
    G.TRAIL_NAME AS TRAIL_NAME,
    CASE 
      WHEN (E.SHELF_TYPE = 'MSERI') THEN MSE.TID_LOGICAL
      ELSE E.TID_LOGICAL
    END AS TID_LOGICAL,
    CASE 
      WHEN (E.SHELF_TYPE = 'MSERI') THEN MSE.ALTERNATE_NAME
      ELSE E.ALTERNATE_NAME
    END AS ALTERNATE_NAME,
    IFNULL(G.NF_ID, 'N/A') NF_ID,
    CURRENT_TIMESTAMP()
  FROM 
    (((((SELECT * FROM `your_project.your_dataset.vnadsprd`.IEN_MARKET_INPUT_LIST_STG_TMP WHERE ELEMENT_TYPE = 'K') G
    JOIN `your_project.your_dataset.vnadsprd`.ICOE_EQUIPMENT_TBL E ON (E.EQP_REFERENCE_ID = G.EQUIPMENT_ID))
    LEFT JOIN `your_project.your_dataset.vnadsprd`.ICOE_EQUIPMENT_TBL MSE ON (E.PHYSICAL_EQUIPMENT_REFERENC_ID = MSE.EQP_REFERENCE_ID))
    JOIN `your_project.your_dataset.vnadsprd`.ICOE_SITE_TBL S ON (E.SITE_REFERENCE_ID = S.SITE_REFERENCE_ID))
    LEFT JOIN `your_project.your_dataset.vnadsprd`.IEN_UT_PMO_TRACKER MARKET ON (MARKET.SITE_CLLI = S.CLLI);

  EXECUTE IMMEDIATE 'ALTER INDEX `your_project.your_dataset.vnadsprd`.IEN_TRAIL_NAME_IDX SET ENABLE';

  EXECUTE IMMEDIATE '
    CALL `your_project.your_dataset.vnadsprd`.gather_table_stats(
      ''VNADSPRD'',
      ''IEN_MARKET_INPUT_LIST'',
      16,
      ''FOR ALL COLUMNS SIZE 1'',
      TRUE
    )
  ';
END;
```)
