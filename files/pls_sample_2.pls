
  CREATE OR REPLACE EDITIONABLE PROCEDURE "VNADSPRD"."SP_IEN_MARKET_INPUT_LIST_LOAD" AS
    BEGIN

/***** Load IEN_MARKET_INPUT_LIST_STG********/
        EXECUTE IMMEDIATE 'truncate table vnadsprd.IEN_MARKET_INPUT_LIST_STG';
        EXECUTE IMMEDIATE 'INSERT INTO vnadsprd.IEN_MARKET_INPUT_LIST_STG (TRAIL_ID	,

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

	LAST_REFRESHED_TS )  

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

                SYSTIMESTAMP

  FROM vnadsprd.NAUT_TRAIL_ELEMENT E,

       vnadsprd.NAUT_TRAIL   VW,

       vnadsprd.NAUT_TRAIL_COMPONENT_ELEMENT TCE,

      -- vnadsprd.ICOE_MV_SITE ASITE,  Changed to NAR Table

       vnadsprd.ICOE_SITE_TBL ASITE,  

        -- vnadsprd.ICOE_MV_SITE ZSITE Changed to NAR Table

           vnadsprd.ICOE_SITE_TBL ZSITE

   WHERE VW.TRAIL_ID = E.TRAIL_ID

   AND E.ELEMENT_TYPE = ''E''

   AND E.SOURCE IN (''IVAPP_PORT'', ''IVAPP_LOGICAL'', ''IVAPP_PANEL'')

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

       ''NAUTILUS'' AS SOURCE, 

       TC.CHANNEL_NAME,

       TC.SEQUENCE_NUMBER,

       TC.STATUS PORT_STATUS,

       TE.ELEMENT_TYPE,

       X.PROJECT_ID,

       X.ACLLI,

       X.ZCLLI,

       SYSTIMESTAMP

  FROM vnadsprd.NAUT_TRAIL T,

       vnadsprd.NAUT_TRAIL_CHANNEL TC,

       vnadsprd.NAUT_TRAIL_COMPONENT_ELEMENT TCE,

       vnadsprd.NAUT_TRAIL_ELEMENT TE,

       (SELECT VW.TRAIL_ID,

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

         -- VW.TYPE = ''OTN_INFRASTRUCTURE''

            VW.TRAIL_ID = TE.TRAIL_ID

           AND TE.ELEMENT_TYPE = ''P''

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

                  ZSITE.CLLI) X

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

       ''NAUTILUS'' AS SOURCE,

       TC.CHANNEL_NAME,

       TC.SEQUENCE_NUMBER,

       TC.STATUS PORT_STATUS,

       TE.ELEMENT_TYPE,

       X.PROJECT_ID,

       X.ACLLI,

       X.ZCLLI,

       SYSTIMESTAMP

  FROM vnadsprd.NAUT_TRAIL T,

       vnadsprd.NAUT_TRAIL_CHANNEL TC,

        vnadsprd.NAUT_TRAIL_COMPONENT_ELEMENT TCE,

       vnadsprd.NAUT_TRAIL_ELEMENT TE,

       (SELECT VW.TRAIL_ID,

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

         -- VW.TYPE = ''OTN_INFRASTRUCTURE''

            VW.TRAIL_ID = TE.TRAIL_ID

           AND TE.ELEMENT_TYPE = ''P''

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

                  ZSITE.CLLI) X

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

       ''NAUTILUS'' AS SOURCE,

       TC.CHANNEL_NAME,

       TC.SEQUENCE_NUMBER,

       TC.STATUS PORT_STATUS,

       TE.ELEMENT_TYPE,

       X.PROJECT_ID,

       X.ACLLI,

       X.ZCLLI,

       SYSTIMESTAMP

  FROM vnadsprd.NAUT_TRAIL T,

       vnadsprd.NAUT_TRAIL_CHANNEL TC,

        vnadsprd.NAUT_TRAIL_COMPONENT_ELEMENT TCE,

       vnadsprd.NAUT_TRAIL_ELEMENT TE,

       (SELECT VW.TRAIL_ID,

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

         -- VW.TYPE = ''OTN_INFRASTRUCTURE''

            VW.TRAIL_ID = TE.TRAIL_ID

           AND TE.ELEMENT_TYPE = ''P''

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

                  ZSITE.CLLI) X

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

       ''NAUTILUS'' AS SOURCE,

       TC.CHANNEL_NAME,

       TC.SEQUENCE_NUMBER,

       TC.STATUS PORT_STATUS,

       TE.ELEMENT_TYPE,

       X.PROJECT_ID,

       X.ACLLI,

       X.ZCLLI,

       SYSTIMESTAMP

  FROM vnadsprd.NAUT_TRAIL T,

       vnadsprd.NAUT_TRAIL_CHANNEL TC,

        vnadsprd.NAUT_TRAIL_COMPONENT_ELEMENT TCE,

       vnadsprd.NAUT_TRAIL_ELEMENT TE,

       (SELECT VW.TRAIL_ID,

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

           AND TE.ELEMENT_TYPE = ''P''

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

                  ZSITE.CLLI) X

 WHERE T.TRAIL_ID = X.PARENT_TRAIL_ID

   AND TC.PARENT_TRAIL_ID = X.PARENT_TRAIL_ID

   AND TC.CHANNEL_NAME = to_char(X.CHANNEL_NAME)

    AND TE.CHANNEL_NAME= to_char(X.CHANNEL_NAME)

   AND TE.PARENT_TRAIL_ID= X.PARENT_TRAIL_ID

   AND TCE.ELEMENT_ID=TE.ELEMENT_ID'
        ;
        COMMIT;

/***** Load IEN_MARKET_INPUT_LIST********/
        EXECUTE IMMEDIATE 'TRUNCATE TABLE vnadsprd.IEN_MARKET_INPUT_LIST';
        EXECUTE IMMEDIATE 'drop index vnadsprd.IEN_TRAIL_NAME_IDX';
        EXECUTE IMMEDIATE 'insert into vnadsprd.IEN_MARKET_INPUT_LIST (

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

nvl(market.VZB_MARKET_CITY_OR_VZT_REGION,''Not Available'') market_name, 

case when market.STATE is not null then market.STATE

else nvl(SUBSTR(S.CLLI,5,2),''N/A'') end market_area,

nvl(S.CLLI,''N/A'') AS EQUIP_CLLI,

G.TRAIL_NAME AS TRAIl_NAME,

(case when (E.SHELF_TYPE = ''MSERI'') then MSE.TID_LOGICAL else E.TID_LOGICAL end) AS TID_LOGICAL,

(case when (E.SHELF_TYPE = ''MSERI'') then MSE.ALTERNATE_NAME else E.ALTERNATE_NAME end) AS ALTERNATE_NAME,

nvl(G.NF_ID,''N/A'') NF_ID,

SYSTIMESTAMP

from ((((vnadsprd.IEN_MARKET_INPUT_LIST_STG G join 
vnadsprd.icoe_pvnr_t_logical_port P on((P.PORT_REFERENCE_ID = G.PORT_REFERENCE_ID))) 

--join vnadsprd.ICOE_MV_EQUIPMENT E on((P.EQP_REFERENCE_ID = E.EQP_REFERENCE_ID))) Changed to NAR Table

join vnadsprd.ICOE_EQUIPMENT_TBL E on((P.EQP_REFERENCE_ID = E.EQP_REFERENCE_ID))) 

--left join vnadsprd.ICOE_MV_EQUIPMENT MSE on((E.PHYSICAL_EQUIPMENT_REFERENC_ID = MSE.EQP_REFERENCE_ID))) Changed to NAR Table

left join vnadsprd.ICOE_EQUIPMENT_TBL MSE on((E.PHYSICAL_EQUIPMENT_REFERENC_ID = MSE.EQP_REFERENCE_ID)))

--join vnadsprd.ICOE_MV_SITE S on((E.SITE_REFERENCE_ID = S.SITE_REFERENCE_ID))) Changed to NAR Table

join vnadsprd.ICOE_SITE_TBL S on((E.SITE_REFERENCE_ID = S.SITE_REFERENCE_ID)))

left join vnadsprd.IEN_UT_PMO_TRACKER MARKET on (MARKET.SITE_CLLI = S.CLLI)'
        ;
        COMMIT;
        EXECUTE IMMEDIATE 'create index vnadsprd.IEN_TRAIL_NAME_IDX on vnadsprd.IEN_MARKET_INPUT_LIST(TRAIL_NAME)';
    END;