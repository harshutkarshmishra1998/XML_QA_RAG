CREATE OR REPLACE PROCEDURE DEMO_MARKET_DATA_LOAD
AS
BEGIN

    ------------------------------------------------------------------
    -- STEP 1: CLEAR STAGING TABLE
    ------------------------------------------------------------------
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DEMO_MARKET_STG';

    ------------------------------------------------------------------
    -- STEP 2: LOAD DATA INTO STAGING TABLE
    ------------------------------------------------------------------
    INSERT INTO DEMO_MARKET_STG (
        PROJECT_ID,
        SITE_CODE,
        REGION,
        DEVICE_TYPE,
        PORT_NUMBER,
        CHANNEL_ID,
        CREATED_DATE
    )

    SELECT DISTINCT
        SRC.PROJECT_ID,             -- Unique project identifier
        SITE.SITE_CODE,             -- Site identifier
        SITE.REGION,                -- Region/location
        DEV.DEVICE_TYPE,            -- Device info
        DEV.PORT_NUMBER,            -- Port info
        DEV.CHANNEL_ID,             -- Channel info
        SYSDATE                     -- Load timestamp

    FROM
        DEMO_PROJECTS SRC           -- Main source table

        INNER JOIN DEMO_SITES SITE
            ON SRC.SITE_ID = SITE.SITE_ID

        LEFT JOIN DEMO_DEVICES DEV
            ON SITE.SITE_ID = DEV.SITE_ID

    WHERE
        SRC.STATUS = 'ACTIVE';      -- Filter condition

    ------------------------------------------------------------------
    -- STEP 3: COMMIT CHANGES
    ------------------------------------------------------------------
    COMMIT;

END DEMO_MARKET_DATA_LOAD;
/