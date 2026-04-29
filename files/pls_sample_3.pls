
  CREATE OR REPLACE EDITIONABLE PROCEDURE "VNADSPRD"."SP_IEN_MR_NTLS_PRTCKTS_LOAD_CND" AS
BEGIN
    DECLARE
        v_rownum NUMBER := 0;
    BEGIN

	/**********************Load naut_trail_tmp******/

        dbms_output.put_line('-------------------- naut_trail_tmp load started--------------------' || systimestamp);
        EXECUTE IMMEDIATE 'truncate table vnadsprd.naut_trail_tmp1';
        EXECUTE IMMEDIATE 'insert into  vnadsprd.naut_trail_tmp1 

select distinct trail_name,status,trail_id from vnadsprd.naut_trail

where trail_name in (

select trail_name from (

select distinct trail_name,status from vnadsprd.naut_trail)

group by trail_name

having count(1)>1)';
        COMMIT;
        EXECUTE IMMEDIATE 'truncate table vnadsprd.naut_trail_tmp2';
        EXECUTE IMMEDIATE 'insert into  vnadsprd.naut_trail_tmp2 

select trail_name,min(priority) priority,min(sub_priority) sub_priority from (

select trail.trail_name,trail.trail_status,lkp.priority,lkp.sub_priority from vnadsprd.naut_trail_tmp1 trail,vnadsprd.NAUT_TRAIL_STATUS_PRTY_LKP lkp where

trail.trail_status = lkp.trail_status

order by trail.trail_name,lkp.priority,lkp.sub_priority)

group by trail_name';
        COMMIT;
        EXECUTE IMMEDIATE 'truncate table vnadsprd.naut_trail_tmp3';
        EXECUTE IMMEDIATE 'insert into vnadsprd.naut_trail_tmp3 

select trail.trail_name,trail.trail_status,trail.trail_id from vnadsprd.naut_trail_tmp1 trail,vnadsprd.NAUT_TRAIL_STATUS_PRTY_LKP lkp,vnadsprd.naut_trail_tmp2 tst1 where

trail.trail_status = lkp.trail_status

and trail.trail_name = tst1.trail_name

and lkp.priority = tst1.priority and lkp.sub_priority =tst1.sub_priority';
        COMMIT;
        EXECUTE IMMEDIATE 'truncate table vnadsprd.naut_trail_tmp';
        EXECUTE IMMEDIATE 'insert into  vnadsprd.naut_trail_tmp 

select t.* from vnadsprd.naut_trail t where t.trail_name not in (

select trail_name from

vnadsprd.naut_trail_tmp3)and t.version in (select max(tt.version)

                 from vnadsprd.naut_trail tt where t.trail_name = tt.trail_name

group by tt.trail_name)';
        COMMIT;
        EXECUTE IMMEDIATE 'insert into  vnadsprd.naut_trail_tmp

select t.* from vnadsprd.naut_trail t  where t.trail_id  in (

select distinct trail_id from

vnadsprd.naut_trail_tmp3 ) and t.version in (select max(tt.version)

                 from vnadsprd.naut_trail tt where t.trail_name = tt.trail_name

                 and tt.trail_id in (select trail_id from

vnadsprd.naut_trail_tmp3)

group by tt.trail_name)';
        COMMIT;
        dbms_output.put_line('-------------------- naut_trail_tmp load ended--------------------' || systimestamp);

/********** end of naut_trail_tmp load ************************/

   /******* invoke port cdc procedure********/

        dbms_output.put_line('--------------------Icoe Tbl refresh load started--------------------' || systimestamp);
        vnadsprd.sp_ien_icon_tbls_refresh_cnd;
        dbms_output.put_line('--------------------Icoe Tbl refresh load Ended--------------------' || systimestamp);

   --DBMS_OUTPUT.PUT_LINE ('--------------------Icoe Port cdc Tbl load started--------------------'||SYSTIMESTAMP);

   --vnadsprd.SP_NAUT_PORT_DATA_EXTRACT();

   --DBMS_OUTPUT.PUT_LINE ('--------------------Icoe Port cdc Tbl load ended--------------------'||SYSTIMESTAMP);

   /******* Load IEN_MARKET_INPUT_LIST_STG - Input table for Java priority screen ********/

    --    dbms_output.put_line('--------------------IEN_MARKET_INPUT_LIST tbl load started--------------------' || systimestamp);
	--	  vnadsprd.sp_ien_market_input_list_load();
      ---  vnadsprd.sp_ien_market_input_list_load_new();
    --    dbms_output.put_line('--------------------IEN_MARKET_INPUT_LIST tbl load ended--------------------' || systimestamp);

   /****** Extract data from IEN_MRKT_RDNS_WRTBCK (data inserted from Java priority screen and Load TRAIL_TEMP *******/

        dbms_output.put_line('--------------------TRAIL_TEMP tbl load started--------------------' || systimestamp);
        vnadsprd.sp_ien_mr_trail_temp_load();
        dbms_output.put_line('--------------------TRAIL_TEMP tbl load ended--------------------' || systimestamp);

   /* Adding new code as part of new design for processing priority circuits Enchancement */

   /****** Extract data from TRAIL_TEMP  and Load NAUT_SA_PRTCKTS_INFO *******/

        dbms_output.put_line('--------------------NAUT_SA_PRTCKTS_INFO tbl load started--------------------' || systimestamp);
        vnadsprd.sp_ien_mr_naut_sa_prtckts_info();
        dbms_output.put_line('--------------------NAUT_SA_PRTCKTS_INFO tbl load ended--------------------' || systimestamp);

   /***********Load IEN_MR_TRAIL_MASTER table **********/

        dbms_output.put_line('--------------------IEN_MR_TRAIL_MASTER tbl load started--------------------' || systimestamp);
        EXECUTE IMMEDIATE 'TRUNCATE TABLE vnadsprd.IEN_MR_TRAIL_MASTER';
        INSERT /*+ APPEND */ INTO vnadsprd.ien_mr_trail_master (
            trail_id,
            trail_name,
            type,
            status,
            version,
            A_SITE_NAME,
            Z_SITE_NAME,
            created_ts,
            project_id,
            network_type
        )

      /* Adding new code as part of new design for processing priority circuits Enchancement */

            SELECT
                trail_id,
                trail_name,
                type,
                status,
                version,
                A_SITE_NAME,
                Z_SITE_NAME,
                systimestamp,
                project_id,
                network_id
            FROM
                (
                    SELECT DISTINCT
                        tw.trail_id,
                        tw.trail_name,
                        tw.type,
                        tw.status,
                        tw.version,
                        tw.A_SITE_NAME,
                        tw.Z_SITE_NAME,
                        tw.project_id,
                        tw.network_id
                    FROM
                        vnadsprd.naut_sa_prtckts_info t,
                        vnadsprd.naut_trail_tmp       tw
                    WHERE
                        t.trail_name = tw.trail_name
                );

        COMMIT;

   /* commenting below old code*/

   /*

   SELECT TRAIL_ID,TRAIL_NAME,TYPE,STATUS,VERSION,A_SITE_NAME,Z_SITE_NAME,SYSTIMESTAMP,PROJECT_ID,NETWORK_ID from (

   SELECT TW.TRAIL_ID, TW.TRAIL_NAME, TW.TYPE, TW.STATUS, TW.VERSION,TW.A_SITE_NAME,TW.Z_SITE_NAME,TW.PROJECT_ID,TW.NETWORK_ID

   FROM vnadsprd.TRAIL_TEMP T, vnadsprd.NAUT_TRAIL TW

    WHERE T.TRAIL_NAME = TW.TRAIL_NAME

   UNION

   SELECT DISTINCT W.TRAIL_ID, W.TRAIL_NAME, W.TYPE, W.STATUS, W.VERSION,W.A_SITE_NAME,W.Z_SITE_NAME,W.PROJECT_ID,W.NETWORK_ID

   FROM vnadsprd.TRAIL_TEMP T, vnadsprd.NAUT_TRAIL W, vnadsprd.NAUT_TRAIL_ELEMENT TE

    WHERE TE.ELEMENT_TYPE = 'P'

    AND W.TRAIL_NAME = T.TRAIL_NAME

    AND TE.PARENT_TRAIL_ID = W.TRAIL_ID

   UNION

   SELECT DISTINCT DW.TRAIL_ID, DW.TRAIL_NAME, DW.TYPE, DW.STATUS, DW.VERSION,DW.A_SITE_NAME,DW.Z_SITE_NAME,DW.PROJECT_ID,DW.NETWORK_ID

   FROM vnadsprd.TRAIL_TEMP T,

    vnadsprd.NAUT_TRAIL_ELEMENT WE,

    vnadsprd.NAUT_TRAIL_ELEMENT DE,

    vnadsprd.NAUT_TRAIL DW

    WHERE WE.ELEMENT_TYPE = 'P'

    AND DW.TRAIL_NAME = T.TRAIL_NAME

    AND WE.PARENT_TRAIL_ID = DE.TRAIL_ID

    AND DE.ELEMENT_TYPE = 'P'

    AND DE.PARENT_TRAIL_ID = DW.TRAIL_ID);

   */

        dbms_output.put_line('--------------------IEN_MR_TRAIL_MASTER tbl load ended--------------------' || systimestamp);

   /***********Load IEN_MR_NTLS_CKT_CHANNEL_STG ********/

        EXECUTE IMMEDIATE 'TRUNCATE TABLE vnadsprd.IEN_MR_NTLS_CKT_CHANNEL_STG';
        EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX_IEN_MR_NTLS_CKT_CHANNEL UNUSABLE';
        INSERT INTO vnadsprd.ien_mr_ntls_ckt_channel_stg (
                trail_id,
            parent_trail_id,
            channel_name,
            last_refreshed_ts
        )
           SELECT
                trail_id,
                parent_trail_id,
                channel_name,
                systimestamp
            FROM
                (
                    SELECT
                        trail_id,
                        parent_trail_id,
                        channel_name
                    FROM
                        vnadsprd.naut_trail_element
                    WHERE
                        ( trail_id, nvl(parent_trail_id, 0), nvl(to_number(channel_name), 0) ) IN (
                            SELECT
                                trail_id, nvl(parent_trail_id, 0), nvl(MIN(to_number(channel_name)), 0) channel_name
                            FROM
                                vnadsprd.naut_trail_element
                            WHERE
                                    element_type = 'P'
                                AND CASE
                                        WHEN TRIM(translate(channel_name, '0123456789.', ' ')) IS NULL THEN
                                            'NUMBER'
                                        ELSE
                                            'STRING'
                                    END = 'NUMBER'                                  
                            GROUP BY
                                trail_id, parent_trail_id
                        )
                        AND element_type = 'P'
                        AND CASE
                                WHEN TRIM(translate(channel_name, '0123456789.', ' ')) IS NULL THEN
                                    'NUMBER'
                                ELSE
                                    'STRING'
                            END = 'NUMBER'
                    UNION
                    SELECT
                        trail_id,
                        parent_trail_id,
                        MIN((channel_name)) channel_name
                    FROM
                        vnadsprd.naut_trail_element
                    WHERE
                            element_type = 'P'
                        AND CASE
                                WHEN TRIM(translate(channel_name, '0123456789.', ' ')) IS NULL THEN
                                    'NUMBER'
                                ELSE
                                    'STRING'
                            END = 'STRING'

                    GROUP BY
                        trail_id,
                        parent_trail_id
                ) aa
                where not exists ( select 1 from 
                (select e.trail_id, 'SVC_DESCRIPTOR' as source from vnadsprd.naut_SERVICE_EXT e where e.SVC_DESCRIPTOR_NAME like '%FLEX%'
				and e.trail_id = aa.trail_id union select a.trail_id, 'CENTRAL_FREQUENCY' as source
				from vnadsprd.naut_TRAIL_ATTRIBUTES a where a.ATTRIBUTE_NAME= 'CENTRAL_FREQUENCY'
				AND a.attribute_value is not null and a.trail_id= aa.trail_id));

        COMMIT;

		INSERT INTO vnadsprd.ien_mr_ntls_ckt_channel_stg(
            trail_id,
            parent_trail_id,
            channel_name,
            last_refreshed_ts
        )	SELECT
                trail_id,
                parent_trail_id,
                channel_name,
                systimestamp
            FROM
                (
                    SELECT DISTINCT
                        ele.trail_id,
                        parent_trail_id,
                        a.attribute_value channel_name
                    FROM
                        vnadsprd.naut_trail_element ele, vnadsprd.naut_TRAIL_ATTRIBUTES a
                    WHERE
                    ele.trail_id = a.trail_id
                    and a.ATTRIBUTE_NAME= 'CENTRAL_FREQUENCY'
                   and   ele.element_type = 'P'
                    ) aa
                    where  exists ( select 1 from 
                (select e.trail_id, 'SVC_DESCRIPTOR' as source from vnadsprd.naut_SERVICE_EXT e where e.SVC_DESCRIPTOR_NAME like '%FLEX%'
                and e.trail_id = aa.trail_id union select a.trail_id, 'CENTRAL_FREQUENCY' as source
                from vnadsprd.naut_TRAIL_ATTRIBUTES a where a.ATTRIBUTE_NAME= 'CENTRAL_FREQUENCY'
                AND a.attribute_value is not null and a.trail_id= aa.trail_id));

        COMMIT;


        EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX_IEN_MR_NTLS_CKT_CHANNEL REBUILD';

   /************** Load vnadsprd.IEN_MR_CHANNEL_TEMP ******************/

        dbms_output.put_line('--------------------IEN_MR_CHANNEL_TEMP tbl load started--------------------' || systimestamp);
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
            zclli,
            network_type
        )
            SELECT
                vw.trail_id,
                vw.trail_name,
                vw.type,
                vw.status,
                vw.version,
                parent_trail_id,
                MIN((channel_name)) channel_name,
                vw.project_id,
                asite.clli          aclli,
                zsite.clli          zclli,
                vw.network_type
            FROM
                vnadsprd.ien_mr_ntls_ckt_channel_stg te,
                vnadsprd.ien_mr_trail_master         vw,
                vnadsprd.icoe_site_tbl               asite,
                vnadsprd.icoe_site_tbl               zsite
            WHERE
                    vw.trail_id = te.trail_id

               --AND TE.ELEMENT_TYPE = 'P'

                AND vw.A_SITE_NAME = asite.clli (+)
                AND vw.Z_SITE_NAME = zsite.clli (+)
            GROUP BY
                vw.trail_id,
                vw.trail_name,
                vw.type,
                vw.status,
                vw.version,
                te.parent_trail_id,
                vw.project_id,
                asite.clli,
                zsite.clli,
                vw.network_type;

        COMMIT;
        EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX1_IEN_MR_CHANNEL_TEMP REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX2_IEN_MR_CHANNEL_TEMP REBUILD';
        BEGIN
            dbms_stats.gather_table_stats(ownname => 'vnadsprd', tabname => 'IEN_MR_CHANNEL_TEMP', degree => 16, method_opt => 'FOR ALL COLUMNS SIZE 1',
            cascade => true);
        END;

        dbms_output.put_line('--------------------IEN_MR_CHANNEL_TEMP tbl load ended--------------------' || systimestamp);

   /************** Load vnadsprd.IEN_MRKT_READINSS_NTLS_CKT_STG ******************/

        EXECUTE IMMEDIATE 'truncate table vnadsprd.IEN_MRKT_READINSS_NTLS_CKT_STG';
        EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX1_IEN_ICON_CKT_STG UNUSABLE';
        dbms_output.put_line('--------------------IEN_MRKT_READINSS_NTLS_CKT_STG tbl load started--------------------' || systimestamp);

   /* changed the codefrom min(channel_name) to min(to_number(channel_name)) by keerthana on 10/16/19*/

        INSERT INTO vnadsprd.ien_mrkt_readinss_ntls_ckt_stg (
            trail_id,
            trail_name,
            status,
            type,
            version,
            sequence,
            port_reference_id,
            source,
            channel_name,
            sequence_number,
            port_status,
            element_type,
            nf_id,
            aclli,
            zclli,
            last_refreshed_ts,
            network_type,
            equipment_id
        )
            SELECT DISTINCT
                vw.trail_id,
                vw.trail_name,
                vw.status,
                vw.type,
                vw.version,
                tce.sequence,
                e.element_ref_id AS port_reference_id,
                e.source,
                e.channel_name,
                NULL             AS sequence_number,
                NULL             AS port_status,
                e.element_type,
                vw.project_id,
                asite.clli       aclli,
                zsite.clli       zclli,
                systimestamp,
                vw.network_type,
                NULL             equipment_id
            FROM
                vnadsprd.naut_trail_element           e,
                vnadsprd.ien_mr_trail_master          vw,
                vnadsprd.naut_trail_component_element tce,

             --vnadsprd.ICOE_MV_SITE ASITE, Changed to NAR Table

                vnadsprd.icoe_site_tbl                asite,

             --vnadsprd.ICOE_MV_SITE ZSITEChanged to NAR Table

                vnadsprd.icoe_site_tbl                zsite
            WHERE
                    vw.trail_id = e.trail_id
                AND e.element_type = 'E'
                AND e.source IN ( 'IVAPP_PORT', 'IVAPP_LOGICAL', 'IVAPP_PANEL' )
                AND tce.element_id = e.element_id
                AND vw.A_SITE_NAME = asite.clli (+)
                AND vw.Z_SITE_NAME = zsite.clli (+)
            UNION
            SELECT
                x.trail_id,
                x.trail_name,
                x.status,
                x.type,
                x.version,
                tce.sequence,
                t.a_port_id port_reference_id,
                'NAUTILUS'  AS source,
                tc.channel_name,
                tc.sequence_number,
                tc.status   port_status,
                te.element_type,
                x.project_id,
                x.aclli,
                x.zclli,
                systimestamp,
                x.network_type,
                NULL        equipment_id
            FROM
                vnadsprd.naut_trail_tmp               t,
                vnadsprd.naut_trail_channel           tc,
                vnadsprd.naut_trail_component_element tce,
                vnadsprd.naut_trail_element           te,
                vnadsprd.ien_mr_channel_temp          x
            WHERE
                    t.trail_id = x.parent_trail_id
                AND tc.parent_trail_id = x.parent_trail_id
                AND tc.channel_name = to_char(x.channel_name)
                AND te.channel_name = to_char(x.channel_name)
                AND te.parent_trail_id = x.parent_trail_id
                AND tce.element_id = te.element_id
            UNION
            SELECT
                x.trail_id,
                x.trail_name,
                x.status,
                x.type,
                x.version,
                tce.sequence,
                t.z_port_id port_reference_id,
                'NAUTILUS'  AS source,
                tc.channel_name,
                tc.sequence_number,
                tc.status   port_status,
                te.element_type,
                x.project_id,
                x.aclli,
                x.zclli,
                systimestamp,
                x.network_type,
                NULL        equipment_id
            FROM
                vnadsprd.naut_trail_tmp               t,
                vnadsprd.naut_trail_channel           tc,
                vnadsprd.naut_trail_component_element tce,
                vnadsprd.naut_trail_element           te,
                vnadsprd.ien_mr_channel_temp          x
            WHERE
                    t.trail_id = x.parent_trail_id
                AND tc.parent_trail_id = x.parent_trail_id
                AND tc.channel_name = to_char(x.channel_name)
                AND te.channel_name = to_char(x.channel_name)
                AND te.parent_trail_id = x.parent_trail_id
                AND tce.element_id = te.element_id
            UNION
            SELECT
                x.trail_id,
                x.trail_name,
                x.status,
                x.type,
                x.version,
                tce.sequence,
                t.a_port_id_reverse port_reference_id,
                'NAUTILUS'          AS source,
                tc.channel_name,
                tc.sequence_number,
                tc.status           port_status,
                te.element_type,
                x.project_id,
                x.aclli,
                x.zclli,
                systimestamp,
                x.network_type,
                NULL                equipment_id
            FROM
                vnadsprd.naut_trail_tmp               t,
                vnadsprd.naut_trail_channel           tc,
                vnadsprd.naut_trail_component_element tce,
                vnadsprd.naut_trail_element           te,
                vnadsprd.ien_mr_channel_temp          x
            WHERE
                    t.trail_id = x.parent_trail_id
                AND tc.parent_trail_id = x.parent_trail_id
                AND tc.channel_name = to_char(x.channel_name)
                AND te.channel_name = to_char(x.channel_name)
                AND te.parent_trail_id = x.parent_trail_id
                AND tce.element_id = te.element_id
            UNION
            SELECT
                x.trail_id,
                x.trail_name,
                x.status,
                x.type,
                x.version,
                tce.sequence,
                t.z_port_id_reverse port_reference_id,
                'NAUTILUS'          AS source,
                tc.channel_name,
                tc.sequence_number,
                tc.status           port_status,
                te.element_type,
                x.project_id,
                x.aclli,
                x.zclli,
                systimestamp,
                x.network_type,
                NULL                equipment_id
            FROM
                vnadsprd.naut_trail_tmp               t,
                vnadsprd.naut_trail_channel           tc,
                vnadsprd.naut_trail_component_element tce,
                vnadsprd.naut_trail_element           te,
                vnadsprd.ien_mr_channel_temp          x
            WHERE
                    t.trail_id = x.parent_trail_id
                AND tc.parent_trail_id = x.parent_trail_id
                AND tc.channel_name = to_char(x.channel_name)
                AND te.channel_name = to_char(x.channel_name)
                AND te.parent_trail_id = x.parent_trail_id
                AND tce.element_id = te.element_id
            UNION
            SELECT DISTINCT
                vw.trail_id,
                vw.trail_name,
                vw.status,
                vw.type,
                vw.version,
                tce.sequence,
                NULL          port_reference_id,
                e.source,
                vw.a_port_aid,
                NULL          AS sequence_number,
                vw.status     AS port_status,
                e.element_type,
                vw.project_id,
                asite.clli    aclli,
                zsite.clli    zclli,
                systimestamp,
                vw.network_id network_type,
                vw.a_equipment_id
            FROM
                vnadsprd.naut_trail_element           e,
                vnadsprd.naut_trail_tmp               vw,
                vnadsprd.naut_trail_component_element tce,
                vnadsprd.icoe_site_tbl                asite,
                vnadsprd.icoe_site_tbl                zsite,
                vnadsprd.ien_mr_trail_master          mstr
            WHERE
                    vw.trail_id = e.trail_id
                AND e.element_type = 'K'
               --- AND upper(vw.type) LIKE '%LAG%'
                AND tce.element_id = e.element_id
                AND vw.A_SITE_NAME = asite.clli (+)
                AND vw.Z_SITE_NAME = zsite.clli (+)
                AND vw.trail_id = mstr.trail_id
            UNION
            SELECT DISTINCT
                vw.trail_id,
                vw.trail_name,
                vw.status,
                vw.type,
                vw.version,
                tce.sequence,
                NULL          port_reference_id,
                e.source,
                vw.z_port_aid,
                NULL          AS sequence_number,
                vw.status     AS port_status,
                e.element_type,
                vw.project_id,
                asite.clli    aclli,
                zsite.clli    zclli,
                systimestamp,
                vw.network_id network_type,
                vw.z_equipment_id			   -----------------------------	

            FROM
                vnadsprd.naut_trail_element           e,
                vnadsprd.naut_trail_tmp               vw,
                vnadsprd.naut_trail_component_element tce,
                vnadsprd.icoe_site_tbl                asite,
                vnadsprd.icoe_site_tbl                zsite,
                vnadsprd.ien_mr_trail_master          mstr
            WHERE
                    vw.trail_id = e.trail_id
                AND e.element_type = 'K'
             ---   AND upper(vw.type) LIKE '%LAG%'
                AND tce.element_id = e.element_id
                AND vw.A_SITE_NAME = asite.clli(+)
                AND vw.Z_SITE_NAME = zsite.clli (+)
                AND vw.trail_id = mstr.trail_id
            	UNION
        SELECT DISTINCT
            vw.trail_id,
            vw.trail_name,
            vw.status,
            vw.type,
            vw.version,
            tce.sequence,
            NULL AS port_reference_id ,
            e.source,
            vw.z_port_aid,
            NULL       AS sequence_number,
            vw.status  AS port_status,
            e.element_type,
            vw.project_id,
            asite.clli aclli,
            zsite.clli zclli,
            systimestamp,
			vw.network_id network_type,
            nvl(vw.a_equipment_id, 1) equipment_id
        FROM
            vnadsprd.naut_trail_element           e,
            vnadsprd.naut_trail_tmp               vw,
           vnadsprd.naut_trail_component_element tce,
            vnadsprd.icoe_site_tbl                asite,
            vnadsprd.icoe_site_tbl                zsite,
			vnadsprd.ien_mr_trail_master          mstr
        WHERE
                vw.trail_id = e.trail_id
            AND e.element_type = 'S'
                   AND tce.element_id = e.element_id
            AND vw.a_site_name = asite.clli (+)
            AND vw.z_site_name = zsite.clli (+)
			AND vw.trail_id = mstr.trail_id
			UNION
        SELECT DISTINCT
            vw.trail_id,
            vw.trail_name,
            vw.status,
            vw.type,
            vw.version,
            tce.sequence,
            NULL AS port_reference_id ,
            e.source,
            vw.z_port_aid,
            NULL       AS sequence_number,
            vw.status  AS port_status,
            e.element_type,
            vw.project_id,
            asite.clli aclli,
            zsite.clli zclli,
            systimestamp,
			vw.network_id network_type,
            nvl(vw.z_equipment_id,1) equipment_id
        FROM
            vnadsprd.naut_trail_element           e,
            vnadsprd.naut_trail_tmp               vw,
           vnadsprd.naut_trail_component_element tce,
            vnadsprd.icoe_site_tbl                asite,
            vnadsprd.icoe_site_tbl                zsite,
			vnadsprd.ien_mr_trail_master          mstr
        WHERE
                vw.trail_id = e.trail_id
            AND e.element_type = 'S'
            AND tce.element_id = e.element_id
            AND vw.a_site_name = asite.clli (+)
            AND vw.z_site_name = zsite.clli (+)
			AND vw.trail_id = mstr.trail_id;		

        COMMIT;
        dbms_output.put_line('--------------------IEN_MRKT_READINSS_NTLS_CKT_STG tbl load ended--------------------' || systimestamp);
        EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX1_IEN_ICON_CKT_STG REBUILD';

    /***********Load IEN_MR_NTLS_ICON_CKT_TMP **********/

        dbms_output.put_line('--------------------IEN_MR_NTLS_ICON_CKT_TMP tbl load started--------------------' || systimestamp);
        EXECUTE IMMEDIATE 'truncate table vnadsprd.IEN_MR_NTLS_ICON_CKT_TMP';
        INSERT INTO vnadsprd.ien_mr_ntls_icon_ckt_tmp (
            trail_id,
            trail_name,
            status,
            type,
            version,
            sequence,
            port_reference_id,
            source,
            channel_name,
            sequence_number,
            port_status,
            element_type,
            nf_id,
            aclli,
            zclli,
            network_type,
            pport_reference_id,
            prelated_port_ref_id,
            lport_reference_id,
            lrelated_port_ref_id,
            is_related_lport_flag,
            snapshot_dt
        )
            ( SELECT DISTINCT
                ttp.trail_id,
                ttp.trail_name,
                ttp.status,
                ttp.type,
                ttp.version,
                ttp.sequence,
                ttp.port_reference_id,
                ttp.source,
                ttp.channel_name,
                ttp.sequence_number,
                ttp.port_status,
                ttp.element_type,
                ttp.nf_id,
                ttp.aclli,
                ttp.zclli,
                ttp.network_type,
                mvp.PVNR_PORT_ID    pport_reference_id,
                mvp.related_port_ref_id  prelated_port_ref_id,
                mvlp.port_reference_id   AS lport_reference_id,
                mvlp.related_port_ref_id lrelated_port_ref_id,
                CASE
                    WHEN mvp.related_port_ref_id IS NOT NULL
                         AND mvlp1.port_reference_id IS NOT NULL
                         AND mvp.related_port_ref_id = mvlp1.port_reference_id THEN
                        'Y'
                    ELSE
                        NULL
                END                      is_related_lport_flag,
                ttp.last_refreshed_ts    snapshot_dt
            FROM
                vnadsprd.ien_mrkt_readinss_ntls_ckt_stg ttp,
                vnadsprd.ICOE_svt1plp_cnd                   mvp,
                vnadsprd.ICOE_PVNR_T_LOGICAL_PORT_CND       mvlp,
                vnadsprd.ICOE_PVNR_T_LOGICAL_PORT_CND       mvlp1
            WHERE
                    ttp.port_reference_id = mvp.PVNR_PORT_ID (+)
                AND mvp.PVNR_PORT_ID = mvlp.port_reference_id (+)
                AND mvp.related_port_ref_id = mvlp1.port_reference_id (+)
            );

        COMMIT;
        dbms_output.put_line('--------------------IEN_MR_NTLS_ICON_CKT_TMP tbl load ended--------------------' || systimestamp);

   /***********Load IEN_MR_NTLS_ICON_CKT_STG table **********/

        dbms_output.put_line('--------------------IEN_MR_NTLS_ICON_CKT_STG tbl load started--------------------' || systimestamp);
        EXECUTE IMMEDIATE 'truncate table vnadsprd.IEN_MR_NTLS_ICON_CKT_STG';
        EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX_IEN_MR_NTLS_ICON_CKT UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX1_IEN_MR_NTLS_ICON_CKT UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX2_IEN_MR_NTLS_ICON_CKT UNUSABLE';
        INSERT INTO vnadsprd.ien_mr_ntls_icon_ckt_stg (
            trail_id,
            trail_name,
            status,
            type,
            version,
            sequence,
            port_reference_id,
            source,
            channel_name,
            sequence_number,
            port_status,
            element_type,
            nf_id,
            aclli,
            zclli,
            network_type,
            pport_reference_id,
            prelated_port_ref_id,
            lport_reference_id,
            lrelated_port_ref_id,
            is_related_lport_flag,
            last_refreshed_ts,
            shelf_type,
            physical_port_reference_id
        )
            ( SELECT DISTINCT
                tmp.trail_id,
                tmp.trail_name,
                tmp.status,
                tmp.type,
                tmp.version,
                tmp.sequence,
                tmp.port_reference_id,
                tmp.source,
                tmp.channel_name,
                tmp.sequence_number,
                tmp.port_status,
                tmp.element_type,
                tmp.nf_id,
                tmp.aclli,
                tmp.zclli,
                tmp.network_type,
                tmp.pport_reference_id,
                tmp.prelated_port_ref_id,
                tmp.lport_reference_id,
                tmp.lrelated_port_ref_id,
                tmp.is_related_lport_flag,
                tmp.snapshot_dt last_refreshed_ts,
                eqp.shelf_type,
                port.physical_port_reference_id
            FROM
                vnadsprd.ien_mr_ntls_icon_ckt_tmp tmp,
                vnadsprd.icoe_equipment_tbl       eqp,
                vnadsprd.ICOE_PVNR_T_LOGICAL_PORT_CND port
            WHERE
                ( is_related_lport_flag IS NULL
                  OR lport_reference_id IS NOT NULL )
                AND tmp.port_reference_id = port.port_reference_id (+)
                AND port.eqp_reference_id = eqp.eqp_reference_id (+)
            );

        COMMIT;
        EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX_IEN_MR_NTLS_ICON_CKT REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX1_IEN_MR_NTLS_ICON_CKT REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX vnadsprd.IDX2_IEN_MR_NTLS_ICON_CKT REBUILD';
        dbms_output.put_line('--------------------IEN_MR_NTLS_ICON_CKT_STG tbl load ended--------------------' || systimestamp);
        dbms_output.put_line('IEN_MR_ICOE_PORT_TMP Load started ' || systimestamp);

/*****Load IEN_MR_ICOE_PORT_TMP  ******/
        EXECUTE IMMEDIATE 'TRUNCATE TABLE vnadsprd.IEN_MR_ICOE_PORT_TMP';
        INSERT INTO /*+ Append Parallel 8 */ vnadsprd.ien_mr_icoe_port_tmp
            SELECT DISTINCT
                phy.port_reference_id,
                virtual1.port_status,
                phy.port_type,
                virtual1.aid,
                phy.port_name,
                phy.port_number,
                virtual1.bandwidth_name,
                phy.card_reference_id,
                phy.eqp_reference_id,
                phy.parent_port_ref_id,
                phy.port_rel_nm,
                phy.slot_type,
                phy.logical_slot_name
            FROM
                VNADSPRD.ICOE_PVNR_T_LOGICAL_PORT_CND phy,
                VNADSPRD.ICOE_PVNR_T_LOGICAL_PORT_CND virtual1,
                vnadsprd.ICOE_svt1plp_cnd             mv_port
            WHERE
                    virtual1.port_reference_id = mv_port.PVNR_PORT_ID
                AND mv_port.PORT_INSTNC_ID >= - 1e20
                AND phy.port_reference_id = virtual1.physical_port_reference_id;

        COMMIT;
        dbms_output.put_line('IEN_MR_ICOE_PORT_TMP Load ENDED ' || systimestamp);

   /***********Load IEN_MARKT_READINESS_NTLS_DATA table **********/

        EXECUTE IMMEDIATE ' TRUNCATE TABLE vnadsprd.IEN_MARKT_READINESS_NTLS_DATA';
        EXECUTE IMMEDIATE 'ALTER index vnadsprd.IDX_IEN_MR_NTLS_DATA UNUSABLE';
        dbms_output.put_line('----IEN_MARKT_READINESS_NTLS_DATA tbl load started--' || systimestamp);
        INSERT /*+ APPEND PARALLEL (8) */ INTO vnadsprd.ien_markt_readiness_ntls_data (
            id,
            trail_id,
            trail_name,
            trail_status,
            type,
            version,
            source,
            eqp_status,
            eqp_reference_id,
            ne_type,
            shelf_type,
            eqp_vendor,
            functional_type,
            tid_logical,
            tid_physical,
            alternate_name,
            logical_shelf,
            physical_shelf_position,
            rack_name,
            eqp_clli,
            market_name,
            market_area,
            card_reference_id,
            card_type,
            parent_card_ref_id,
            parent_card_status,
            card_status,
            parent_card_supress_ind,
            card_supress_ind,
            parent_slot_reference_id,
            parent_slot_name,
            parent_slot_number,
            sub_slot_reference_id,
            sub_slot_name,
            sub_slot_numer,
            sub_sub_slot_reference_id,
            sub_sub_slot_name,
            sub_sub_slot_numer,
            sequence,
            port_reference_id,
            port_aid,
            port_name,
            port_number,
            port_type,
            port_status,
            sub_port_reference_id,
            sub_port_aid,
            sub_port_name,
            sub_port_number,
            sub_port_type,
            sub_port_status,
            sub_sub_port_reference_id,
            sub_sub_port_aid,
            sub_sub_port_name,
            sub_sub_port_number,
            sub_sub_port_type,
            sub_sub_port_status,
            channel_name,
            sequence_number,
            channel_status,
            nf_id,
            cktaclli,
            cktzclli,
            last_refreshed_ts,
            network_type,
            parent_slot_rel_nm,
            sub_slot_rel_nm,
            port_rel_nm,
            sub_port_rel_nm,
            sub_sub_port_rel_nm,
            sub_sub_card_reference_id,
            sub_sub_card_status,
            sub_sub_slot_rel_nm,
            shelf_model,
            sub_sub_card_supress_ind,
            icoe_port_missing_flag,
            lport_reference_id,
            pport_reference_id,
            prelated_port_ref_id,
            lrelated_port_ref_id,
            is_related_lport_flag,
            part_num,
            slot_type,
            logical_slot_name_port
        )
            SELECT /*+ LEADING (G) INDEX (G IDX1_IEN_ICON_CKT_STG) */ DISTINCT
                ROWNUM                                                     id,
                g.trail_id                                                 AS trail_id,
                g.trail_name                                               AS trail_name,
                g.status                                                   AS trail_status,
                g.type                                                     AS type,
                g.version                                                  AS version,
                g.source                                                   AS source,
                e.inv_status                                               AS eqp_status,
                e.eqp_reference_id                                         AS eqp_reference_id,
                e.eqp_type                                                 AS ne_type,
                e.shelf_type                                               AS shelf_type,
                e.eqp_vendor                                               AS eqp_vendor,
                e.functional_type                                          AS functional_type,
                e.tid_logical                                              AS tid_logical,
                e.tid_physical                                             AS tid_physical,
                e.alternate_name                                           AS alternate_name,
                e.logical_shelf                                            AS logical_shelf,
                e.physical_shelf_position                                  AS physical_shelf_position,
                r.eqp_name                                                 AS rack_name,
                s.clli                                                     AS equip_clli,
                nvl(market.vzb_market_city_or_vzt_region, 'Not Available') market_name,
                CASE
                    WHEN market.state IS NOT NULL THEN
                        market.state
                    ELSE
                        nvl(substr(s.clli, 5, 2), 'N/A')
                END                                                        market_area,
                CASE
                    WHEN pc.parent_card_ref_id IS NOT NULL THEN
                        pc.card_reference_id
                    ELSE
                        CASE
                            WHEN c.parent_card_ref_id IS NOT NULL THEN
                                    c.card_reference_id
                        END
                END                                                        card_reference_id,
                CASE
                    WHEN pc.parent_card_ref_id IS NOT NULL THEN
                        pc.card_type
                    ELSE
                        CASE
                            WHEN c.parent_card_ref_id IS NOT NULL THEN
                                    c.card_type
                        END
                END                                                        card_type,
                CASE
                    WHEN pc.parent_card_ref_id IS NOT NULL THEN
                        pc.parent_card_ref_id
                    ELSE
                        CASE
                            WHEN c.parent_card_ref_id IS NOT NULL THEN
                                    c.parent_card_ref_id
                            ELSE
                                c.card_reference_id
                        END
                END                                                        AS parent_card_ref_id,
                CASE
                    WHEN upper(p.slot_type) = 'HSLOT' THEN
                        e.inv_status
                    ELSE
                        CASE
                            WHEN (
                                    CASE
                                        WHEN pc.parent_card_ref_id IS NOT NULL THEN
                                            ppc.inv_status
                                        ELSE
                                            CASE
                                                WHEN c.parent_card_ref_id IS NOT NULL THEN
                                                        pc.inv_status
                                                ELSE
                                                    c.inv_status
                                            END
                                    END
                                ) IS NULL
                                 AND e.logical_slot_name IS NOT NULL THEN
                                    e.inv_status
                            ELSE
                                CASE
                                    WHEN pc.parent_card_ref_id IS NOT NULL THEN
                                                ppc.inv_status
                                    ELSE
                                        CASE
                                            WHEN c.parent_card_ref_id IS NOT NULL THEN
                                                            pc.inv_status
                                            ELSE
                                                c.inv_status
                                        END
                                END
                        END
                END                                                        parent_card_status,
                CASE
                    WHEN pc.parent_card_ref_id IS NOT NULL THEN
                        pc.inv_status
                    ELSE
                        CASE
                            WHEN c.parent_card_ref_id IS NOT NULL THEN
                                    c.inv_status
                        END
                END                                                        card_status,

/* Addition of Supress_Ind by Rajiv on 20200810*/

         /*       CASE
                    WHEN pc.parent_card_ref_id IS NOT NULL THEN
                        TRIM(ppc.supress_ind)
                    ELSE
                        CASE
                            WHEN c.parent_card_ref_id IS NOT NULL THEN
                                    TRIM(pc.supress_ind)
                            ELSE
                                TRIM(c.supress_ind)
                        END
                END                                                        AS parent_card_supress_ind, */

				NULL AS parent_card_supress_ind,
           /*     CASE
                    WHEN pc.parent_card_ref_id IS NOT NULL THEN
                        TRIM(pc.supress_ind)
                    ELSE
                        CASE
                            WHEN c.parent_card_ref_id IS NOT NULL THEN
                                    TRIM(c.supress_ind)
                        END
                END                                                        AS card_supress_ind, */

				NULL AS card_supress_ind,
                (
                    CASE
                        WHEN ( pc.parent_card_ref_id IS NOT NULL ) THEN
                            pps.slot_reference_id
                        ELSE
                            (
                                CASE
                                    WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                           AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                                        ps.slot_reference_id
                                    ELSE
                                        sl.slot_reference_id
                                END
                            )
                    END
                )                                                          AS parent_slot_reference_id,
                CASE
                    WHEN upper(p.slot_type) = 'HSLOT' THEN
                        p.logical_slot_name
                    ELSE
                        CASE
                            WHEN (
                                    CASE
                                        WHEN ( pc.parent_card_ref_id IS NOT NULL ) THEN
                                            pps.slot_name
                                        ELSE
                                            (
                                                CASE
                                                    WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                                           AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                                                        ps.slot_name
                                                    ELSE
                                                        sl.slot_name
                                                END
                                            )
                                    END
                                ) IS NULL
                                 AND e.logical_slot_name IS NOT NULL
                                 AND decode(e.network_type, 'ULH', e.eqp_type, e.functional_type) IN ( 'CMD44 C-BAND RED (45-88)', 'CMD44 C-BAND BLUE (1-44)',
                                                                                                       'FIM TYPE 1', 'FIBER_SHUFFLE',
                                                                                                       'FIBER SHUFFLE' ) THEN
                                    e.logical_slot_name
                            ELSE
                                CASE
                                    WHEN ( pc.parent_card_ref_id IS NOT NULL ) THEN
                                                pps.slot_name
                                    ELSE
                                        (
                                                    CASE
                                                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                                               AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                                                            ps.slot_name
                                                        ELSE
                                                            sl.slot_name
                                                    END
                                                )
                                END
                        END
                END                                                        AS parent_slot_name,
                CASE
                    WHEN upper(p.slot_type) = 'HSLOT' THEN
                        p.logical_slot_name
                    ELSE
                        CASE
                            WHEN (
                                    CASE
                                        WHEN ( pc.parent_card_ref_id IS NOT NULL ) THEN
                                            pps.slot_number
                                        ELSE
                                            (
                                                CASE
                                                    WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                                           AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                                                        ps.slot_number
                                                    ELSE
                                                        sl.slot_number
                                                END
                                            )
                                    END
                                ) IS NULL
                                 AND e.logical_slot_name IS NOT NULL
                                 AND decode(e.network_type, 'ULH', e.eqp_type, e.functional_type) IN ( 'CMD44 C-BAND RED (45-88)', 'CMD44 C-BAND BLUE (1-44)',
                                                                                                       'FIM TYPE 1', 'FIBER_SHUFFLE',
                                                                                                       'FIBER SHUFFLE' ) THEN
                                    e.logical_slot_name
                            ELSE
                                CASE
                                    WHEN ( pc.parent_card_ref_id IS NOT NULL ) THEN
                                                pps.slot_number
                                    ELSE
                                        (
                                                    CASE
                                                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                                               AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                                                            ps.slot_number
                                                        ELSE
                                                            sl.slot_number
                                                    END
                                                )
                                END
                        END
                END                                                        AS parent_slot_number,
                (
                    CASE
                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                               AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                            sl.slot_reference_id
                        ELSE
                            (
                                CASE
                                    WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                           AND ( pc.parent_card_ref_id IS NOT NULL ) ) THEN
                                        ps.slot_reference_id
                                END
                            )
                    END
                )                                                          AS sub_slot_reference_id,
                (
                    CASE
                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                               AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                            sl.slot_name
                        ELSE
                            (
                                CASE
                                    WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                           AND ( pc.parent_card_ref_id IS NOT NULL ) ) THEN
                                        ps.slot_name
                                END
                            )
                    END
                )                                                          AS sub_slot_name,
                (
                    CASE
                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                               AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                            sl.slot_number
                        ELSE
                            (
                                CASE
                                    WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                           AND ( pc.parent_card_ref_id IS NOT NULL ) ) THEN
                                        ps.slot_number
                                END
                            )
                    END
                )                                                          AS sub_slot_numer,
                (
                    CASE
                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                               AND ( pc.parent_card_ref_id IS NOT NULL ) ) THEN
                            sl.slot_reference_id
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_slot_reference_id,
                (
                    CASE
                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                               AND ( pc.parent_card_ref_id IS NOT NULL ) ) THEN
                            sl.slot_name
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_slot_name,
                (
                    CASE
                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                               AND ( pc.parent_card_ref_id IS NOT NULL ) ) THEN
                            sl.slot_number
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_slot_numer,

               /*Added new code for MSERI Enhancement*/

                g.sequence                                                 AS sequence,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                        ppp.port_reference_id
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NULL ) THEN
                                    p.port_reference_id
                                ELSE
                                    pp.port_reference_id
                            END
                        )
                END                                                        AS port_reference_id,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                        ppp.aid
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NULL ) THEN
                                    p.aid
                                ELSE
                                    pp.aid
                            END
                        )
                END                                                        AS port_aid,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                        ppp.port_name
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NULL ) THEN
                                    p.port_name
                                ELSE
                                    pp.port_name
                            END
                        )
                END                                                        AS port_name,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                        ppp.port_number
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NULL ) THEN
                                    p.port_number
                                ELSE
                                    pp.port_number
                            END
                        )
                END                                                        AS port_number,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                            CASE
                                WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                    nvl(virtualport2.virtual_port_type, ppp.port_type)
                                ELSE
                                    ppp.port_type
                            END
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NULL ) THEN
                                        CASE
                                            WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                                nvl(virtualport.virtual_port_type, p.port_type)
                                            ELSE
                                                p.port_type
                                        END
                                ELSE
                                    CASE
                                        WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                                nvl(virtualport1.virtual_port_type, pp.port_type)
                                        ELSE
                                            pp.port_type
                                    END
                            END
                        )
                END                                                        AS port_type,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                            CASE
                                WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                    nvl(virtualport2.virtual_port_status, ppp.port_status)
                                ELSE
                                    ppp.port_status
                            END
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NULL ) THEN
                                        CASE
                                            WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                                nvl(virtualport.virtual_port_status, p.port_status)
                                            ELSE
                                                p.port_status
                                        END
                                ELSE
                                    CASE
                                        WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                                nvl(virtualport1.virtual_port_status, pp.port_status)
                                        ELSE
                                            pp.port_status
                                    END
                            END
                        )
                END                                                        AS port_status,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                        pp.port_reference_id
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NOT NULL ) THEN
                                    p.port_reference_id
                                ELSE
                                    NULL
                            END
                        )
                END                                                        AS sub_port_reference_id,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                            CASE
                                WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                    nvl(virtualport1.virtual_port_aid, pp.aid)
                                ELSE
                                    pp.aid
                            END
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NOT NULL ) THEN
                                        CASE
                                            WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                                nvl(virtualport.virtual_port_aid, p.aid)
                                            ELSE
                                                p.aid
                                        END
                                ELSE
                                    NULL
                            END
                        )
                END                                                        AS sub_port_aid,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                        pp.port_name
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NOT NULL ) THEN
                                    p.port_name
                                ELSE
                                    NULL
                            END
                        )
                END                                                        AS sub_port_name,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                        pp.port_number
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NOT NULL ) THEN
                                    p.port_number
                                ELSE
                                    NULL
                            END
                        )
                END                                                        AS sub_port_number,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                            CASE
                                WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                    nvl(virtualport1.virtual_port_type, pp.port_type)
                                ELSE
                                    pp.port_type
                            END
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NOT NULL ) THEN
                                        CASE
                                            WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                                nvl(virtualport.virtual_port_type, p.port_type)
                                            ELSE
                                                p.port_type
                                        END
                                ELSE
                                    NULL
                            END
                        )
                END                                                        AS sub_port_type,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                            CASE
                                WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                    nvl(virtualport1.virtual_port_status, pp.port_status)
                                ELSE
                                    pp.port_status
                            END
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NOT NULL ) THEN
                                        CASE
                                            WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                                nvl(virtualport.virtual_port_status, p.port_status)
                                            ELSE
                                                p.port_status
                                        END
                                ELSE
                                    NULL
                            END
                        )
                END                                                        AS sub_port_status,
                (
                    CASE
                        WHEN ( p.parent_port_ref_id IS NOT NULL
                               AND pp.parent_port_ref_id IS NOT NULL ) THEN
                            p.port_reference_id
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_port_reference_id,
                (
                    CASE
                        WHEN ( p.parent_port_ref_id IS NOT NULL
                               AND pp.parent_port_ref_id IS NOT NULL ) THEN
                            p.aid
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_port_aid,
                (
                    CASE
                        WHEN ( p.parent_port_ref_id IS NOT NULL
                               AND pp.parent_port_ref_id IS NOT NULL ) THEN
                            p.port_name
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_port_name,
                (
                    CASE
                        WHEN ( p.parent_port_ref_id IS NOT NULL
                               AND pp.parent_port_ref_id IS NOT NULL ) THEN
                            p.port_number
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_port_number,
                (
                    CASE
                        WHEN ( p.parent_port_ref_id IS NOT NULL
                               AND pp.parent_port_ref_id IS NOT NULL ) THEN
                                CASE
                                    WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                        nvl(virtualport.virtual_port_type, p.port_type)
                                    ELSE
                                        p.port_type
                                END
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_port_type,
                (
                    CASE
                        WHEN ( p.parent_port_ref_id IS NOT NULL
                               AND pp.parent_port_ref_id IS NOT NULL ) THEN
                                CASE
                                    WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                        nvl(virtualport.virtual_port_status, p.port_status)
                                    ELSE
                                        p.port_status
                                END
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_port_status,
                CASE
                    WHEN g.type IN ( 'ELINE_INFRA', 'CARRIER_ETHERNET_VPN' )
                         AND g.element_type = 'P' THEN
                        NULL
                    ELSE
                        g.channel_name
                END                                                        AS channel_name,
                CASE
                    WHEN g.type IN ( 'ELINE_INFRA', 'CARRIER_ETHERNET_VPN' )
                         AND g.element_type = 'P' THEN
                        NULL
                    ELSE
                        g.sequence_number
                END                                                        AS sequence_number,
                g.port_status                                              AS channel_status,
                g.nf_id,
                g.aclli,
                g.zclli,
                g.last_refreshed_ts,
                g.network_type,
                (
                    CASE
                        WHEN ( pc.parent_card_ref_id IS NOT NULL ) THEN
                            pps.slot_rel_nm
                        ELSE
                            (
                                CASE
                                    WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                           AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                                        ps.slot_rel_nm
                                    ELSE
                                        sl.slot_rel_nm
                                END
                            )
                    END
                )                                                          AS parent_slot_rel_nm,
                (
                    CASE
                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                               AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                            sl.slot_rel_nm
                        ELSE
                            (
                                CASE
                                    WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                           AND ( pc.parent_card_ref_id IS NOT NULL ) ) THEN
                                        ps.slot_rel_nm
                                END
                            )
                    END
                )                                                          AS sub_slot_rel_nm,
                CASE
                    WHEN pp.parent_port_ref_id IS NOT NULL THEN
                        ppp.port_rel_nm
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NULL ) THEN
                                    p.port_rel_nm
                                ELSE
                                    pp.port_rel_nm
                            END
                        )
                END                                                        AS port_rel_nm,
                CASE
                    WHEN pp.parent_port_ref_id IS NOT NULL THEN
                        pp.port_rel_nm
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NOT NULL ) THEN
                                    p.port_rel_nm
                                ELSE
                                    NULL
                            END
                        )
                END                                                        AS sub_port_rel_nm,
                (
                    CASE
                        WHEN ( p.parent_port_ref_id IS NOT NULL
                               AND pp.parent_port_ref_id IS NOT NULL ) THEN
                            p.port_rel_nm
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_port_rel_nm,

				  /* Column addition for sub sub slot */

                CASE
                    WHEN c.parent_card_ref_id IS NOT NULL
                         AND pc.parent_card_ref_id IS NOT NULL THEN
                        c.card_reference_id
                    ELSE
                        NULL
                END                                                        AS sub_sub_card_reference_id,
                CASE
                    WHEN c.parent_card_ref_id IS NOT NULL
                         AND pc.parent_card_ref_id IS NOT NULL THEN
                        c.inv_status
                    ELSE
                        NULL
                END                                                        AS sub_sub_card_status,
                (
                    CASE
                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                               AND ( pc.parent_card_ref_id IS NOT NULL ) ) THEN
                            sl.slot_rel_nm
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_slot_rel_nm,
                e.eqp_model                                                AS shelf_model,
            /*    CASE
                    WHEN c.parent_card_ref_id IS NOT NULL
                         AND pc.parent_card_ref_id IS NOT NULL THEN
                        TRIM(c.supress_ind)
                    ELSE
                        NULL
                END                                                        AS sub_sub_card_supress_ind, */

				NULL AS sub_sub_card_supress_ind,
                CASE
                    WHEN g.lport_reference_id IS NULL
                         AND g.is_related_lport_flag IS NULL THEN
                        'Y'
                    ELSE
                        'N'
                END                                                        icoe_port_missing_flag,
                g.lport_reference_id,
                g.pport_reference_id,
                g.prelated_port_ref_id,
                g.lrelated_port_ref_id,
                g.is_related_lport_flag,
                e.part_num                                                 AS part_num,
                p.slot_type,
                p.logical_slot_name
            FROM
                (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (
                                                                        (
                                                                            SELECT
                                                                                *
                                                                            FROM
                                                                                vnadsprd.ien_mr_ntls_icon_ckt_stg
                                                                            WHERE
                                                                                lport_reference_id IS NOT NULL
                                                                                AND type NOT LIKE '%LAG%'
                                                                        )                                 g
                                                                        LEFT JOIN VNADSPRD.ICOE_PVNR_T_LOGICAL_PORT_CND p ON ( ( p.port_reference_id =
                                                                        CASE
                                                                                                                                                     WHEN
                                                                                                                                                     g.
                                                                                                                                                     shelf_type =
                                                                                                                                                     'MSERI'
                                                                                                                                                     THEN
                                                                                                                                                         g.
                                                                                                                                                         physical_port_reference_id
                                                                                                                                                     ELSE
                                                                                                                                                         g.
                                                                                                                                                         port_reference_id
                                                                                                                                                 END ) )
                                                                    )
                                                                    LEFT JOIN vnadsprd.icoe_equipment_tbl       e ON ( ( p.eqp_reference_id =
                                                                    e.eqp_reference_id and upper(e.container)= 'SHELF' ) )
                                                                )
                                                                LEFT JOIN vnadsprd.icoe_equipment_tbl       r ON ( ( e.parent_eqp_reference_id =
                                                                r.eqp_reference_id and upper(r.container)='RACK') )
                                                            )
                                                            LEFT JOIN vnadsprd.icoe_card_tbl            c ON ( ( p.card_reference_id =
                                                            c.card_reference_id ) )
                                                        )
                                                        LEFT JOIN vnadsprd.icoe_site_tbl            s ON ( ( e.location_clli = s.clli ) )
                                                    )
                                                    LEFT JOIN vnadsprd.icoe_slot_tbl            sl ON ( ( c.slot_reference_id = sl.slot_reference_id ) )
                                                ) /*Added new code for MSERI Enhancement*/
                                                LEFT JOIN vnadsprd.icoe_card_tbl            pc ON ( ( c.parent_card_ref_id = pc.card_reference_id ) )
                                            )
                                            LEFT JOIN vnadsprd.icoe_card_tbl            ppc ON ( ( pc.parent_card_ref_id = ppc.card_reference_id ) )
                                        )
                                        LEFT JOIN vnadsprd.icoe_slot_tbl            ps ON ( ( ps.slot_reference_id = pc.slot_reference_id ) )
                                    )
                                    LEFT JOIN vnadsprd.icoe_slot_tbl            pps ON ( ( pps.slot_reference_id = ppc.slot_reference_id ) )
                                )
                                LEFT JOIN VNADSPRD.ICOE_PVNR_T_LOGICAL_PORT_CND pp ON ( ( pp.port_reference_id = p.parent_port_ref_id ) )
                            )
                            LEFT JOIN VNADSPRD.ICOE_PVNR_T_LOGICAL_PORT_CND ppp ON ( ( ppp.port_reference_id = pp.parent_port_ref_id ) )
                        )
                        LEFT JOIN vnadsprd.ien_ut_pmo_tracker       market ON ( market.site_clli = CASE
                                                                                                 WHEN length(s.clli) > 8 THEN
                                                                                                     substr(s.clli, 0, 8)
                                                                                                 ELSE
                                                                                                     s.clli
                                                                                             END )
                        LEFT JOIN vnadsprd.ien_mr_icoe_port_tmp     virtualport ON ( virtualport.port_reference_id = p.port_reference_id )
                    )
                    LEFT JOIN vnadsprd.ien_mr_icoe_port_tmp     virtualport1 ON ( virtualport1.port_reference_id = pp.port_reference_id )
                )
                LEFT JOIN vnadsprd.ien_mr_icoe_port_tmp     virtualport2 ON ( virtualport2.port_reference_id = ppp.port_reference_id )
            ORDER BY
                g.trail_id,
                g.sequence;

        COMMIT;
        SELECT
            NVL (MAX(id),1)
        INTO v_rownum
        FROM
            vnadsprd.ien_markt_readiness_ntls_data;

        dbms_output.put_line('----IEN_MARKT_READINESS_NTLS_DATA tbl load1 started--' || systimestamp);
        INSERT /*+ APPEND PARALLEL (8) */ INTO vnadsprd.ien_markt_readiness_ntls_data (
            id,
            trail_id,
            trail_name,
            trail_status,
            type,
            version,
            source,
            eqp_status,
            eqp_reference_id,
            ne_type,
            shelf_type,
            eqp_vendor,
            functional_type,
            tid_logical,
            tid_physical,
            alternate_name,
            logical_shelf,
            physical_shelf_position,
            rack_name,
            eqp_clli,
            market_name,
            market_area,
            card_reference_id,
            card_type,
            parent_card_ref_id,
            parent_card_status,
            card_status,
            parent_card_supress_ind,
            card_supress_ind,
            parent_slot_reference_id,
            parent_slot_name,
            parent_slot_number,
            sub_slot_reference_id,
            sub_slot_name,
            sub_slot_numer,
            sub_sub_slot_reference_id,
            sub_sub_slot_name,
            sub_sub_slot_numer,
            sequence,
            port_reference_id,
            port_aid,
            port_name,
            port_number,
            port_type,
            port_status,
            sub_port_reference_id,
            sub_port_aid,
            sub_port_name,
            sub_port_number,
            sub_port_type,
            sub_port_status,
            sub_sub_port_reference_id,
            sub_sub_port_aid,
            sub_sub_port_name,
            sub_sub_port_number,
            sub_sub_port_type,
            sub_sub_port_status,
            channel_name,
            sequence_number,
            channel_status,
            nf_id,
            cktaclli,
            cktzclli,
            last_refreshed_ts,
            network_type,
            parent_slot_rel_nm,
            sub_slot_rel_nm,
            port_rel_nm,
            sub_port_rel_nm,
            sub_sub_port_rel_nm,
            sub_sub_card_reference_id,
            sub_sub_card_status,
            sub_sub_slot_rel_nm,
            shelf_model,
            sub_sub_card_supress_ind,
            icoe_port_missing_flag,
            lport_reference_id,
            pport_reference_id,
            prelated_port_ref_id,
            lrelated_port_ref_id,
            is_related_lport_flag,
            part_num,
            slot_type,
            logical_slot_name_port
        )
            SELECT /*+ LEADING (G) INDEX (G IDX1_IEN_ICON_CKT_STG) */ DISTINCT
                v_rownum + ROWNUM                                          id,
                g.trail_id                                                 AS trail_id,
                g.trail_name                                               AS trail_name,
                g.status                                                   AS trail_status,
                g.type                                                     AS type,
                g.version                                                  AS version,
                g.source                                                   AS source,
                e.inv_status                                               AS eqp_status,
                e.eqp_reference_id                                         AS eqp_reference_id,
                e.eqp_type                                                 AS ne_type,
                e.shelf_type                                               AS shelf_type,
                e.eqp_vendor                                               AS eqp_vendor,
                e.functional_type                                          AS functional_type,
                e.tid_logical                                              AS tid_logical,
                e.tid_physical                                             AS tid_physical,
                e.alternate_name                                           AS alternate_name,
                e.logical_shelf                                            AS logical_shelf,
                e.physical_shelf_position                                  AS physical_shelf_position,
                r.eqp_name                                                 AS rack_name,
                s.clli                                                     AS equip_clli,
                nvl(market.vzb_market_city_or_vzt_region, 'Not Available') market_name,
                CASE
                    WHEN market.state IS NOT NULL THEN
                        market.state
                    ELSE
                        nvl(substr(s.clli, 5, 2), 'N/A')
                END                                                        market_area,
                CASE
                    WHEN pc.parent_card_ref_id IS NOT NULL THEN
                        pc.card_reference_id
                    ELSE
                        CASE
                            WHEN c.parent_card_ref_id IS NOT NULL THEN
                                    c.card_reference_id
                        END
                END                                                        card_reference_id,
                CASE
                    WHEN pc.parent_card_ref_id IS NOT NULL THEN
                        pc.card_type
                    ELSE
                        CASE
                            WHEN c.parent_card_ref_id IS NOT NULL THEN
                                    c.card_type
                        END
                END                                                        card_type,
                CASE
                    WHEN pc.parent_card_ref_id IS NOT NULL THEN
                        pc.parent_card_ref_id
                    ELSE
                        CASE
                            WHEN c.parent_card_ref_id IS NOT NULL THEN
                                    c.parent_card_ref_id
                            ELSE
                                c.card_reference_id
                        END
                END                                                        AS parent_card_ref_id,
                CASE
                    WHEN upper(p.slot_type) = 'HSLOT' THEN
                        e.inv_status
                    ELSE
                        CASE
                            WHEN (
                                    CASE
                                        WHEN pc.parent_card_ref_id IS NOT NULL THEN
                                            ppc.inv_status
                                        ELSE
                                            CASE
                                                WHEN c.parent_card_ref_id IS NOT NULL THEN
                                                        pc.inv_status
                                                ELSE
                                                    c.inv_status
                                            END
                                    END
                                ) IS NULL
                                 AND e.logical_slot_name IS NOT NULL THEN
                                    e.inv_status
                            ELSE
                                CASE
                                    WHEN pc.parent_card_ref_id IS NOT NULL THEN
                                                ppc.inv_status
                                    ELSE
                                        CASE
                                            WHEN c.parent_card_ref_id IS NOT NULL THEN
                                                            pc.inv_status
                                            ELSE
                                                c.inv_status
                                        END
                                END
                        END
                END                                                        parent_card_status,
                CASE
                    WHEN pc.parent_card_ref_id IS NOT NULL THEN
                        pc.inv_status
                    ELSE
                        CASE
                            WHEN c.parent_card_ref_id IS NOT NULL THEN
                                    c.inv_status
                        END
                END                                                        card_status,

/* Addition of Supress_Ind by Rajiv on 20200810*/

           /*     CASE
                    WHEN pc.parent_card_ref_id IS NOT NULL THEN
                        TRIM(ppc.supress_ind)
                    ELSE
                        CASE
                            WHEN c.parent_card_ref_id IS NOT NULL THEN
                                    TRIM(pc.supress_ind)
                            ELSE
                                TRIM(c.supress_ind)
                        END
                END                                                        AS parent_card_supress_ind, */

				NULL  AS parent_card_supress_ind, 
              /*  CASE
                    WHEN pc.parent_card_ref_id IS NOT NULL THEN
                        TRIM(pc.supress_ind)
                    ELSE
                        CASE
                            WHEN c.parent_card_ref_id IS NOT NULL THEN
                                    TRIM(c.supress_ind)
                        END
                END                                                        AS card_supress_ind, */

				NULL AS card_supress_ind,
                (
                    CASE
                        WHEN ( pc.parent_card_ref_id IS NOT NULL ) THEN
                            pps.slot_reference_id
                        ELSE
                            (
                                CASE
                                    WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                           AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                                        ps.slot_reference_id
                                    ELSE
                                        sl.slot_reference_id
                                END
                            )
                    END
                )                                                          AS parent_slot_reference_id,
                CASE
                    WHEN upper(p.slot_type) = 'HSLOT' THEN
                        p.logical_slot_name
                    ELSE
                        CASE
                            WHEN (
                                    CASE
                                        WHEN ( pc.parent_card_ref_id IS NOT NULL ) THEN
                                            pps.slot_name
                                        ELSE
                                            (
                                                CASE
                                                    WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                                           AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                                                        ps.slot_name
                                                    ELSE
                                                        sl.slot_name
                                                END
                                            )
                                    END
                                ) IS NULL
                                 AND e.logical_slot_name IS NOT NULL
                                 AND decode(e.network_type, 'ULH', e.eqp_type, e.functional_type) IN ( 'CMD44 C-BAND RED (45-88)', 'CMD44 C-BAND BLUE (1-44)',
                                                                                                       'FIM TYPE 1', 'FIBER_SHUFFLE',
                                                                                                       'FIBER SHUFFLE' ) THEN
                                    e.logical_slot_name
                            ELSE
                                CASE
                                    WHEN ( pc.parent_card_ref_id IS NOT NULL ) THEN
                                                pps.slot_name
                                    ELSE
                                        (
                                                    CASE
                                                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                                               AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                                                            ps.slot_name
                                                        ELSE
                                                            sl.slot_name
                                                    END
                                                )
                                END
                        END
                END                                                        AS parent_slot_name,
                CASE
                    WHEN upper(p.slot_type) = 'HSLOT' THEN
                        p.logical_slot_name
                    ELSE
                        CASE
                            WHEN (
                                    CASE
                                        WHEN ( pc.parent_card_ref_id IS NOT NULL ) THEN
                                            pps.slot_number
                                        ELSE
                                            (
                                                CASE
                                                    WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                                           AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                                                        ps.slot_number
                                                    ELSE
                                                        sl.slot_number
                                                END
                                            )
                                    END
                                ) IS NULL
                                 AND e.logical_slot_name IS NOT NULL
                                 AND decode(e.network_type, 'ULH', e.eqp_type, e.functional_type) IN ( 'CMD44 C-BAND RED (45-88)', 'CMD44 C-BAND BLUE (1-44)',
                                                                                                       'FIM TYPE 1', 'FIBER_SHUFFLE',
                                                                                                       'FIBER SHUFFLE' ) THEN
                                    e.logical_slot_name
                            ELSE
                                CASE
                                    WHEN ( pc.parent_card_ref_id IS NOT NULL ) THEN
                                                pps.slot_number
                                    ELSE
                                        (
                                                    CASE
                                                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                                               AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                                                            ps.slot_number
                                                        ELSE
                                                            sl.slot_number
                                                    END
                                                )
                                END
                        END
                END                                                        AS parent_slot_number,
                (
                    CASE
                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                               AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                            sl.slot_reference_id
                        ELSE
                            (
                                CASE
                                    WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                           AND ( pc.parent_card_ref_id IS NOT NULL ) ) THEN
                                        ps.slot_reference_id
                                END
                            )
                    END
                )                                                          AS sub_slot_reference_id,
                (
                    CASE
                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                               AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                            sl.slot_name
                        ELSE
                            (
                                CASE
                                    WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                           AND ( pc.parent_card_ref_id IS NOT NULL ) ) THEN
                                        ps.slot_name
                                END
                            )
                    END
                )                                                          AS sub_slot_name,
                (
                    CASE
                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                               AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                            sl.slot_number
                        ELSE
                            (
                                CASE
                                    WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                           AND ( pc.parent_card_ref_id IS NOT NULL ) ) THEN
                                        ps.slot_number
                                END
                            )
                    END
                )                                                          AS sub_slot_numer,
                (
                    CASE
                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                               AND ( pc.parent_card_ref_id IS NOT NULL ) ) THEN
                            sl.slot_reference_id
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_slot_reference_id,
                (
                    CASE
                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                               AND ( pc.parent_card_ref_id IS NOT NULL ) ) THEN
                            sl.slot_name
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_slot_name,
                (
                    CASE
                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                               AND ( pc.parent_card_ref_id IS NOT NULL ) ) THEN
                            sl.slot_number
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_slot_numer,

               /*Added new code for MSERI Enhancement*/

                g.sequence                                                 AS sequence,
                nvl((
                    CASE
                        WHEN(pp.parent_port_ref_id IS NOT NULL) THEN
                            ppp.port_reference_id
                        ELSE
                            (
                                CASE
                                    WHEN(p.parent_port_ref_id IS NULL) THEN
                                        p.port_reference_id
                                    ELSE
                                        pp.port_reference_id
                                END
                            )
                    END
                ), g.port_reference_id)                                    AS port_reference_id,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                        ppp.aid
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NULL ) THEN
                                    p.aid
                                ELSE
                                    pp.aid
                            END
                        )
                END                                                        AS port_aid,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                        ppp.port_name
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NULL ) THEN
                                    p.port_name
                                ELSE
                                    pp.port_name
                            END
                        )
                END                                                        AS port_name,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                        ppp.port_number
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NULL ) THEN
                                    p.port_number
                                ELSE
                                    pp.port_number
                            END
                        )
                END                                                        AS port_number,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                            CASE
                                WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                    nvl(virtualport2.virtual_port_type, ppp.port_type)
                                ELSE
                                    ppp.port_type
                            END
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NULL ) THEN
                                        CASE
                                            WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                                nvl(virtualport.virtual_port_type, p.port_type)
                                            ELSE
                                                p.port_type
                                        END
                                ELSE
                                    CASE
                                        WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                                nvl(virtualport1.virtual_port_type, pp.port_type)
                                        ELSE
                                            pp.port_type
                                    END
                            END
                        )
                END                                                        AS port_type,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                            CASE
                                WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                    nvl(virtualport2.virtual_port_status, ppp.port_status)
                                ELSE
                                    ppp.port_status
                            END
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NULL ) THEN
                                        CASE
                                            WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                                nvl(virtualport.virtual_port_status, p.port_status)
                                            ELSE
                                                p.port_status
                                        END
                                ELSE
                                    CASE
                                        WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                                nvl(virtualport1.virtual_port_status, pp.port_status)
                                        ELSE
                                            pp.port_status
                                    END
                            END
                        )
                END                                                        AS port_status,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                        pp.port_reference_id
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NOT NULL ) THEN
                                    p.port_reference_id
                                ELSE
                                    NULL
                            END
                        )
                END                                                        AS sub_port_reference_id,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                            CASE
                                WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                    nvl(virtualport1.virtual_port_aid, pp.aid)
                                ELSE
                                    pp.aid
                            END
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NOT NULL ) THEN
                                        CASE
                                            WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                                nvl(virtualport.virtual_port_aid, p.aid)
                                            ELSE
                                                p.aid
                                        END
                                ELSE
                                    NULL
                            END
                        )
                END                                                        AS sub_port_aid,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                        pp.port_name
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NOT NULL ) THEN
                                    p.port_name
                                ELSE
                                    NULL
                            END
                        )
                END                                                        AS sub_port_name,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                        pp.port_number
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NOT NULL ) THEN
                                    p.port_number
                                ELSE
                                    NULL
                            END
                        )
                END                                                        AS sub_port_number,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                            CASE
                                WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                    nvl(virtualport1.virtual_port_type, pp.port_type)
                                ELSE
                                    pp.port_type
                            END
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NOT NULL ) THEN
                                        CASE
                                            WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                                nvl(virtualport.virtual_port_type, p.port_type)
                                            ELSE
                                                p.port_type
                                        END
                                ELSE
                                    NULL
                            END
                        )
                END                                                        AS sub_port_type,
                CASE
                    WHEN ( pp.parent_port_ref_id IS NOT NULL ) THEN
                            CASE
                                WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                    nvl(virtualport1.virtual_port_status, pp.port_status)
                                ELSE
                                    pp.port_status
                            END
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NOT NULL ) THEN
                                        CASE
                                            WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                                nvl(virtualport.virtual_port_status, p.port_status)
                                            ELSE
                                                p.port_status
                                        END
                                ELSE
                                    NULL
                            END
                        )
                END                                                        AS sub_port_status,
                (
                    CASE
                        WHEN ( p.parent_port_ref_id IS NOT NULL
                               AND pp.parent_port_ref_id IS NOT NULL ) THEN
                            p.port_reference_id
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_port_reference_id,
                (
                    CASE
                        WHEN ( p.parent_port_ref_id IS NOT NULL
                               AND pp.parent_port_ref_id IS NOT NULL ) THEN
                            p.aid
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_port_aid,
                (
                    CASE
                        WHEN ( p.parent_port_ref_id IS NOT NULL
                               AND pp.parent_port_ref_id IS NOT NULL ) THEN
                            p.port_name
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_port_name,
                (
                    CASE
                        WHEN ( p.parent_port_ref_id IS NOT NULL
                               AND pp.parent_port_ref_id IS NOT NULL ) THEN
                            p.port_number
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_port_number,
                (
                    CASE
                        WHEN ( p.parent_port_ref_id IS NOT NULL
                               AND pp.parent_port_ref_id IS NOT NULL ) THEN
                                CASE
                                    WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                        nvl(virtualport.virtual_port_type, p.port_type)
                                    ELSE
                                        p.port_type
                                END
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_port_type,
                (
                    CASE
                        WHEN ( p.parent_port_ref_id IS NOT NULL
                               AND pp.parent_port_ref_id IS NOT NULL ) THEN
                                CASE
                                    WHEN ( e.shelf_type LIKE '%MSE%' ) THEN
                                        nvl(virtualport.virtual_port_status, p.port_status)
                                    ELSE
                                        p.port_status
                                END
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_port_status,
                CASE
                    WHEN g.type IN ( 'ELINE_INFRA', 'CARRIER_ETHERNET_VPN' )
                         AND g.element_type = 'P' THEN
                        NULL
                    ELSE
                        g.channel_name
                END                                                        AS channel_name,
                CASE
                    WHEN g.type IN ( 'ELINE_INFRA', 'CARRIER_ETHERNET_VPN' )
                         AND g.element_type = 'P' THEN
                        NULL
                    ELSE
                        g.sequence_number
                END                                                        AS sequence_number,
                g.port_status                                              AS channel_status,
                g.nf_id,
                g.aclli,
                g.zclli,
                g.last_refreshed_ts,
                g.network_type,
                (
                    CASE
                        WHEN ( pc.parent_card_ref_id IS NOT NULL ) THEN
                            pps.slot_rel_nm
                        ELSE
                            (
                                CASE
                                    WHEN ( ( c.parent_card_ref_id ) IS NOT NULL )
                                         AND ( pc.parent_card_ref_id IS NULL ) THEN
                                        ps.slot_rel_nm
                                    ELSE
                                        sl.slot_rel_nm
                                END
                            )
                    END
                )                                                          AS parent_slot_rel_nm,
                (
                    CASE
                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                               AND ( pc.parent_card_ref_id IS NULL ) ) THEN
                            sl.slot_rel_nm
                        ELSE
                            (
                                CASE
                                    WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                                           AND ( pc.parent_card_ref_id IS NOT NULL ) ) THEN
                                        ps.slot_rel_nm
                                END
                            )
                    END
                )                                                          AS sub_slot_rel_nm,
                CASE
                    WHEN pp.parent_port_ref_id IS NOT NULL THEN
                        ppp.port_rel_nm
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NULL ) THEN
                                    p.port_rel_nm
                                ELSE
                                    pp.port_rel_nm
                            END
                        )
                END                                                        AS port_rel_nm,
                CASE
                    WHEN pp.parent_port_ref_id IS NOT NULL THEN
                        pp.port_rel_nm
                    ELSE
                        (
                            CASE
                                WHEN ( p.parent_port_ref_id IS NOT NULL ) THEN
                                    p.port_rel_nm
                                ELSE
                                    NULL
                            END
                        )
                END                                                        AS sub_port_rel_nm,
                (
                    CASE
                        WHEN ( p.parent_port_ref_id IS NOT NULL
                               AND pp.parent_port_ref_id IS NOT NULL ) THEN
                            p.port_rel_nm
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_port_rel_nm,

				  /* Column addition for sub sub slot */

                CASE
                    WHEN c.parent_card_ref_id IS NOT NULL
                         AND pc.parent_card_ref_id IS NOT NULL THEN
                        c.card_reference_id
                    ELSE
                        NULL
                END                                                        AS sub_sub_card_reference_id,
                CASE
                    WHEN c.parent_card_ref_id IS NOT NULL
                         AND pc.parent_card_ref_id IS NOT NULL THEN
                        c.inv_status
                    ELSE
                        NULL
                END                                                        AS sub_sub_card_status,
                (
                    CASE
                        WHEN ( ( c.parent_card_ref_id IS NOT NULL )
                               AND ( pc.parent_card_ref_id IS NOT NULL ) ) THEN
                            sl.slot_rel_nm
                        ELSE
                            NULL
                    END
                )                                                          AS sub_sub_slot_rel_nm,
                e.eqp_model                                                AS shelf_model,
            /*    CASE
                    WHEN c.parent_card_ref_id IS NOT NULL
                         AND pc.parent_card_ref_id IS NOT NULL THEN
                        TRIM(c.supress_ind)
                    ELSE
                        NULL
                END                                                        AS sub_sub_card_supress_ind, */

				NULL AS  sub_sub_card_supress_ind,
                CASE
                    WHEN g.lport_reference_id IS NULL
                         AND g.is_related_lport_flag IS NULL THEN
                        'Y'
                    ELSE
                        'N'
                END                                                        icoe_port_missing_flag,
                g.lport_reference_id,
                g.pport_reference_id,
                g.prelated_port_ref_id,
                g.lrelated_port_ref_id,
                g.is_related_lport_flag,
                e.part_num                                                 AS part_num,
                p.slot_type,
                p.logical_slot_name
            FROM
                (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (
                                                                        (
                                                                            SELECT
                                                                                *
                                                                            FROM
                                                                                vnadsprd.ien_mr_ntls_icon_ckt_stg
                                                                            WHERE
                                                                                lport_reference_id IS NULL
                                                                                AND source <> 'NAUTILUS'
                                                                                AND type NOT LIKE '%LAG%'
                                                                        )                                 g
                                                                        LEFT JOIN VNADSPRD.ICOE_PVNR_T_LOGICAL_PORT_CND p ON ( ( p.port_reference_id =
                                                                        CASE
                                                                                                                                                     WHEN
                                                                                                                                                     g.
                                                                                                                                                     shelf_type =
                                                                                                                                                     'MSERI'
                                                                                                                                                     THEN
                                                                                                                                                         g.
                                                                                                                                                         physical_port_reference_id
                                                                                                                                                     ELSE
                                                                                                                                                         g.
                                                                                                                                                         port_reference_id
                                                                                                                                                 END ) )
                                                                    )
                                                                    LEFT JOIN vnadsprd.icoe_equipment_tbl       e ON ( ( p.eqp_reference_id =
                                                                    e.eqp_reference_id and upper(e.container)= 'SHELF') )
                                                                )
                                                                LEFT JOIN vnadsprd.icoe_equipment_tbl       r ON ( ( e.parent_eqp_reference_id =
                                                                r.eqp_reference_id  and upper(r.container)='RACK') )
                                                            )
                                                            LEFT JOIN vnadsprd.icoe_card_tbl            c ON ( ( p.card_reference_id =
                                                            c.card_reference_id ) )
                                                        )
                                                        LEFT JOIN vnadsprd.icoe_site_tbl            s ON ( ( e.location_clli = s.clli ) )
                                                    )
                                                    LEFT JOIN vnadsprd.icoe_slot_tbl            sl ON ( ( c.slot_reference_id = sl.slot_reference_id ) )
                                                ) /*Added new code for MSERI Enhancement*/
                                                LEFT JOIN vnadsprd.icoe_card_tbl            pc ON ( ( c.parent_card_ref_id = pc.card_reference_id ) )
                                            )
                                            LEFT JOIN vnadsprd.icoe_card_tbl            ppc ON ( ( pc.parent_card_ref_id = ppc.card_reference_id ) )
                                        )
                                        LEFT JOIN vnadsprd.icoe_slot_tbl            ps ON ( ( ps.slot_reference_id = pc.slot_reference_id ) )
                                    )
                                    LEFT JOIN vnadsprd.icoe_slot_tbl            pps ON ( ( pps.slot_reference_id = ppc.slot_reference_id ) )
                                )
                                LEFT JOIN VNADSPRD.ICOE_PVNR_T_LOGICAL_PORT_CND pp ON ( ( pp.port_reference_id = p.parent_port_ref_id ) )
                            )
                            LEFT JOIN VNADSPRD.ICOE_PVNR_T_LOGICAL_PORT_CND ppp ON ( ( ppp.port_reference_id = pp.parent_port_ref_id ) )
                        )
                        LEFT JOIN vnadsprd.ien_ut_pmo_tracker       market ON ( market.site_clli = CASE
                                                                                                 WHEN length(s.clli) > 8 THEN
                                                                                                     substr(s.clli, 0, 8)
                                                                                                 ELSE
                                                                                                     s.clli
                                                                                             END )
                        LEFT JOIN vnadsprd.ien_mr_icoe_port_tmp     virtualport ON ( virtualport.port_reference_id = p.port_reference_id )
                    )
                    LEFT JOIN vnadsprd.ien_mr_icoe_port_tmp     virtualport1 ON ( virtualport1.port_reference_id = pp.port_reference_id )
                )
                LEFT JOIN vnadsprd.ien_mr_icoe_port_tmp     virtualport2 ON ( virtualport2.port_reference_id = ppp.port_reference_id )
            ORDER BY
                g.trail_id,
                g.sequence;

        COMMIT;
        SELECT
            NVL (MAX(id),1)
        INTO v_rownum
        FROM
            vnadsprd.ien_markt_readiness_ntls_data;

        dbms_output.put_line('----IEN_MARKT_READINESS_NTLS_DATA tbl load2 started--' || systimestamp);
        INSERT /*+ APPEND PARALLEL (8) */ INTO vnadsprd.ien_markt_readiness_ntls_data (
            id,
            trail_id,
            trail_name,
            trail_status,
            type,
            version,
            source,
            eqp_status,
            eqp_reference_id,
            ne_type,
            shelf_type,
            eqp_vendor,
            functional_type,
            tid_logical,
            tid_physical,
            alternate_name,
            logical_shelf,
            physical_shelf_position,
            rack_name,
            eqp_clli,
            market_name,
            market_area,
            sequence,
            channel_name,
            sequence_number,
            channel_status,
            equipment_id
        )
            SELECT /*+ LEADING (G) INDEX (G IDX1_IEN_ICON_CKT_STG) */ DISTINCT
                v_rownum + ROWNUM                                          id,
                g.trail_id                                                 AS trail_id,
                g.trail_name                                               AS trail_name,
                g.status                                                   AS trail_status,
                g.type                                                     AS type,
                g.version                                                  AS version,
                g.source                                                   AS source,
                (
                    CASE
                        WHEN ( e.shelf_type = 'MSERI' ) THEN
                            mse.inv_status
                        ELSE
                            e.inv_status
                    END
                )                                                          AS eqp_status,
                (
                    CASE
                        WHEN ( e.shelf_type = 'MSERI' ) THEN
                            mse.eqp_reference_id
                        ELSE
                            e.eqp_reference_id
                    END
                )                                                          AS eqp_reference_id,
                (
                    CASE
                        WHEN ( e.shelf_type = 'MSERI' ) THEN
                            mse.eqp_type
                        ELSE
                            e.eqp_type
                    END
                )                                                          AS ne_type,
                (
                    CASE
                        WHEN ( e.shelf_type = 'MSERI' ) THEN
                            mse.shelf_type
                        ELSE
                            e.shelf_type
                    END
                )                                                          AS shelf_type,
                (
                    CASE
                        WHEN ( e.shelf_type = 'MSERI' ) THEN
                            mse.eqp_vendor
                        ELSE
                            e.eqp_vendor
                    END
                )                                                          AS eqp_vendor,
                (
                    CASE
                        WHEN ( e.shelf_type = 'MSERI' ) THEN
                            mse.functional_type
                        ELSE
                            e.functional_type
                    END
                )                                                          AS functional_type,
                (
                    CASE
                        WHEN ( e.shelf_type = 'MSERI' ) THEN
                            mse.tid_logical
                        ELSE
                            e.tid_logical
                    END
                )                                                          AS tid_logical,
                (
                    CASE
                        WHEN ( e.shelf_type = 'MSERI' ) THEN
                            mse.tid_physical
                        ELSE
                            e.tid_physical
                    END
                )                                                          AS tid_physical,
                (
                    CASE
                        WHEN ( e.shelf_type = 'MSERI' ) THEN
                            mse.alternate_name
                        ELSE
                            e.alternate_name
                    END
                )                                                          AS alternate_name,
                e.logical_shelf                                            AS logical_shelf,
                e.physical_shelf_position                                  AS physical_shelf_position,
                r.eqp_name                                                 AS rack_name,
                s.clli                                                     AS equip_clli,
                nvl(market.vzb_market_city_or_vzt_region, 'Not Available') market_name,
                CASE
                    WHEN market.state IS NOT NULL THEN
                        market.state
                    ELSE
                        nvl(substr(s.clli, 5, 2), 'N/A')
                END                                                        market_area,
                g.sequence,
                CASE
                    WHEN g.type IN ( 'ELINE_INFRA', 'CARRIER_ETHERNET_VPN' )
                         AND g.element_type = 'P' THEN
                        NULL
                    ELSE
                        g.channel_name
                END                                                        AS channel_name,
                CASE
                    WHEN g.type IN ( 'ELINE_INFRA', 'CARRIER_ETHERNET_VPN' )
                         AND g.element_type = 'P' THEN
                        NULL
                    ELSE
                        g.sequence_number
                END                                                        AS sequence_number,
                g.port_status                                              AS channel_status,
                g.equipment_id
            FROM
                (
                    (
                        (
                            (
                                (
                                    SELECT
                                        *
                                    FROM
                                        vnadsprd.ien_mrkt_readinss_ntls_ckt_stg
                                    WHERE
                                        element_type in ( 'K' ,'S')
                                )                           g
                                LEFT JOIN vnadsprd.icoe_equipment_tbl e ON  ( ((nvl(e.eqp_reference_id,1)) =  nvl(g.equipment_id,1)  ) )
                            )
                            LEFT JOIN vnadsprd.icoe_equipment_tbl mse ON ( ( nvl(e.eqp_reference_id,1) = nvl(mse.eqp_reference_id,1)and upper(mse.container)= 'SHELF' ) )
                        )
                        LEFT JOIN vnadsprd.icoe_equipment_tbl r ON ( ( nvl(e.parent_eqp_reference_id,1) = nvl(r.eqp_reference_id,1)  and upper(r.container)='RACK' ) )
                    )
                    LEFT JOIN vnadsprd.icoe_site_tbl  s ON ( ( nvl(e.location_clli,1) = nvl(s.clli,1) ) )
                )
                LEFT JOIN vnadsprd.ien_ut_pmo_tracker market ON ( market.site_clli = CASE
                                                                                         WHEN length(s.clli) > 8 THEN
                                                                                             substr(s.clli, 0, 8)
                                                                                         ELSE
                                                                                             s.clli
                                                                                     END )
            ORDER BY
                g.trail_id,
                g.sequence;

        COMMIT;
        DELETE FROM vnadsprd.ien_markt_readiness_ntls_data
        WHERE
            ROWID NOT IN (
                SELECT
                    MIN(ROWID)
                FROM
                    vnadsprd.ien_markt_readiness_ntls_data
                GROUP BY
                    trail_id
            )
            AND trail_id IN (
                SELECT DISTINCT
                    ( trail_id )
                FROM
                    vnadsprd.ien_mrkt_readinss_ntls_ckt_stg
                WHERE
                    trail_id NOT IN (
                        SELECT
                            trail_id
                        FROM
                            vnadsprd.ien_mrkt_readinss_ntls_ckt_stg
                        WHERE
                            element_type = 'E'
                    )
                    AND element_type = 'P'
            );

        COMMIT;
        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            equipment_id = NULL,
            eqp_status = NULL,
            eqp_reference_id = NULL,
            shelf_type = NULL,
            eqp_vendor = NULL,
            functional_type = NULL,
            tid_logical = NULL,
            tid_physical = NULL,
            alternate_name = NULL,
            logical_shelf = NULL,
            physical_shelf_position = NULL,
            rack_name = NULL,
            eqp_clli = NULL,
            market_name = NULL,
            market_area = NULL,
            card_reference_id = NULL,
            card_type = NULL,
            parent_card_ref_id = NULL,
            card_status = NULL,
            parent_slot_reference_id = NULL,
            parent_slot_name = NULL,
            parent_slot_number = NULL,
            sub_slot_reference_id = NULL,
            sub_slot_name = NULL,
            sub_slot_numer = NULL,
            sub_sub_slot_reference_id = NULL,
            sub_sub_slot_name = NULL,
            sub_sub_slot_numer = NULL,
            sequence = NULL,
            port_reference_id = NULL,
            port_aid = NULL,
            port_name = NULL,
            port_number = NULL,
            port_type = NULL,
            port_status = NULL,
            sub_port_reference_id = NULL,
            sub_port_aid = NULL,
            sub_port_name = NULL,
            sub_port_number = NULL,
            sub_port_type = NULL,
            sub_port_status = NULL,
            channel_name = NULL,
            sequence_number = NULL,
            channel_status = NULL,
            nf_id = NULL,
            cktaclli = NULL,
            cktzclli = NULL,
            parent_card_status = NULL,
            npreinventory_flag = NULL,
            network_type = NULL,
            parent_slot_rel_nm = NULL,
            sub_slot_rel_nm = NULL,
            port_rel_nm = NULL,
            sub_port_rel_nm = NULL,
            parent_card_supress_ind = NULL,
            card_supress_ind = NULL,
            sub_sub_card_reference_id = NULL,
            sub_sub_card_status = NULL,
            sub_sub_slot_rel_nm = NULL,
            shelf_model = NULL,
            sub_sub_card_supress_ind = NULL,
            icoe_port_missing_flag = NULL,
            lport_reference_id = NULL,
            pport_reference_id = NULL,
            prelated_port_ref_id = NULL,
            lrelated_port_ref_id = NULL,
            is_related_lport_flag = NULL,
            part_num = NULL,
            comments = NULL,
            slot_type = NULL,
            logical_slot_name_port = NULL,
            ne_type = NULL
        WHERE
            trail_id IN (
                SELECT DISTINCT
                    ( trail_id )
                FROM
                    vnadsprd.ien_mrkt_readinss_ntls_ckt_stg
                WHERE
                    trail_id NOT IN (
                        SELECT
                            trail_id
                        FROM
                            vnadsprd.ien_mrkt_readinss_ntls_ckt_stg
                        WHERE
                            element_type = 'E'
                    )
                    AND element_type = 'P'
            );

        COMMIT;
        EXECUTE IMMEDIATE 'ALTER index vnadsprd.IDX_IEN_MR_NTLS_DATA REBUILD';
        dbms_output.put_line('----IEN_MARKT_READINESS_NTLS_DATA tbl load ended--' || systimestamp);

   /***********Load TEMP_IEN_MR_NTLS table **********/

        EXECUTE IMMEDIATE 'truncate table vnadsprd.TEMP_IEN_MR_NTLS';
        EXECUTE IMMEDIATE ' drop index vnadsprd.IDX_TEMP_IEN_MR_NTLS';
        dbms_output.put_line('--------------------TEMP_IEN_MR_NTLS tbl load started--------------------' || systimestamp);
        INSERT INTO vnadsprd.temp_ien_mr_ntls (
            trail_name,
            npreinventory_flag
        )
            SELECT DISTINCT
                trail_name,
                (
                    CASE
                        WHEN npreinventory_flag > 0 THEN
                            'Y'
                        ELSE
                            'N'
                    END
                ) AS npreinventory_flag
            FROM
                (
                    SELECT
                        ntls.trail_name,
                        SUM(
                            CASE
                                WHEN trail_status = 'IN EFFECT'
                                     AND(eqp_status = 'PRE_INVENTORY'
                                         OR parent_card_status = 'PRE_INVENTORY'
                                         OR card_status = 'PRE_INVENTORY'
                                         OR port_status = 'PRE_INVENTORY'
                                         OR sub_port_status = 'PRE_INVENTORY'
                                         OR channel_status = 'PRE_INVENTORY') THEN
                                    1
                                ELSE
                                    0
                            END
                        ) npreinventory_flag
                    FROM
                        vnadsprd.ien_markt_readiness_ntls_data ntls,
                        vnadsprd.ien_mr_trail_master           mstr
                    WHERE
                        ntls.trail_name = mstr.trail_name
                    GROUP BY
                        ntls.trail_name
                );

        COMMIT;
        dbms_output.put_line('--------------------TEMP_IEN_MR_NTLS tbl load ended--------------------' || systimestamp);
        EXECUTE IMMEDIATE ' create index vnadsprd.IDX_TEMP_IEN_MR_NTLS on vnadsprd.TEMP_IEN_MR_NTLS(TRAIL_NAME)';

   --Update NPREINVENTORY_FLAG in IEN_MARKT_READINESS_NTLS_DATA

        UPDATE vnadsprd.ien_markt_readiness_ntls_data b
        SET
            ( b.npreinventory_flag ) = (
                SELECT DISTINCT
                    a.npreinventory_flag
                FROM
                    vnadsprd.temp_ien_mr_ntls a
                WHERE
                    a.trail_name = b.trail_name
            );

        COMMIT;

   /**************Load IEN_NTLS_CKT_HIER_LKP Table *****************/

        EXECUTE IMMEDIATE 'truncate table vnadsprd.IEN_NTLS_CKT_HIER_LKP';
        dbms_output.put_line('--------------------IEN_NTLS_CKT_HIER_LKP tbl load started--------------------' || systimestamp);
        INSERT INTO vnadsprd.ien_ntls_ckt_hier_lkp (
            trail_id,
            trail_name,
            type,
            status,
            version,

                                               --ELEMENT_TYPE  ,

            level1trail,
            level2trail,
            level3trail
        )
            SELECT DISTINCT
                t.trail_id,
                t.trail_name,
                t.type,
                t.status,
                t.version,
                t1.parent_trail_id level1trail,
                t2.parent_trail_id level2trail,
                t3.parent_trail_id level3trail
            FROM
                vnadsprd.naut_trail t,
                (
                    SELECT DISTINCT
                        trail_id,
                        parent_trail_id
                    FROM
                        vnadsprd.naut_trail_element te
                    WHERE
                        te.element_type = 'P'
                )                   t1,
                (
                    SELECT DISTINCT
                        trail_id,
                        parent_trail_id
                    FROM
                        vnadsprd.naut_trail_element te
                    WHERE
                        te.element_type = 'P'
                )                   t2,
                (
                    SELECT DISTINCT
                        trail_id,
                        parent_trail_id
                    FROM
                        vnadsprd.naut_trail_element te
                    WHERE
                        te.element_type = 'P'
                )                   t3
            WHERE
                    t.trail_id = t1.trail_id (+)
                AND t1.parent_trail_id = t2.trail_id (+)
                AND t2.parent_trail_id = t3.trail_id (+);

        COMMIT;
        UPDATE vnadsprd.ien_ntls_ckt_hier_lkp
        SET
            level1trail = NULL
        WHERE
            trail_id = level1trail;

        COMMIT;
        UPDATE vnadsprd.ien_ntls_ckt_hier_lkp
        SET
            level2trail = NULL
        WHERE
            trail_id = level2trail;

        COMMIT;
        UPDATE vnadsprd.ien_ntls_ckt_hier_lkp
        SET
            level3trail = NULL
        WHERE
            trail_id = level3trail;

        COMMIT;
        dbms_output.put_line('--------------------IEN_NTLS_CKT_HIER_LKP tbl load ended--------------------' || systimestamp);


	    /*********Update icon integrity issues in the comments field in ien_markt_readiness_ntls_data table *****/

        dbms_output.put_line('--------------------icon integrity issues update started------------' || systimestamp);

	 --dbms_output.put_line('--------------------1.slot_reference_id is null for card update started------------' || systimestamp);

      ----1.slot_reference_id is null -----

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments = 'SLOT_REFERENCE_ID is not found in Logical Slot table for CARD'
        WHERE
            parent_slot_reference_id IS NULL
            AND parent_card_ref_id IS NOT NULL
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and FUNCTIONAL_TYPE not in ('FIBER SHUFFLE')
                                    ;

        COMMIT;

	  --dbms_output.put_line('--------------------1.slot_reference_id is null for CARD update ended------------' || systimestamp);

	  --dbms_output.put_line('--------------------2.SLOT_REFERENCE_ID is null for SUB CARD update started------------' || systimestamp);

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'SLOT_REFERENCE_ID is not found in Logical Slot table for SUB CARD'
                    ELSE
                        comments
                        || '; '
                        || 'SLOT_REFERENCE_ID is not found in Logical Slot table for SUB CARD'
                END
        WHERE
            sub_slot_reference_id IS NULL
            AND card_reference_id IS NOT NULL
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and FUNCTIONAL_TYPE not in ('FIBER SHUFFLE')
                                    ;

        COMMIT;

      --dbms_output.put_line('--------------------2.SLOT_REFERENCE_ID is null for SUB CARD update ended------------' || systimestamp);

	  --dbms_output.put_line('--------------------3.SLOT_REFERENCE_ID is null for SUB SUB CARD update started------------' || systimestamp);

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'SLOT_REFERENCE_ID is not found in Logical Slot table for SUB SUB CARD'
                    ELSE
                        comments
                        || '; '
                        || 'SLOT_REFERENCE_ID is not found in Logical Slot table for SUB SUB CARD'
                END
        WHERE
            sub_sub_slot_reference_id IS NULL
            AND sub_sub_card_reference_id IS NOT NULL
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and FUNCTIONAL_TYPE not in ('FIBER SHUFFLE')
                                    ;

        COMMIT;

	  --dbms_output.put_line('--------------------3.SLOT_REFERENCE_ID is null for SUB SUB CARD update ended------------' || systimestamp);

      ----2. parent_card_ref_id is null when sub card exists-------

      --dbms_output.put_line('--------------------4.PARENT_CARD_REF_ID is null for SUB CARD update started------------' || systimestamp);

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'PARENT_CARD_REF_ID is not found in MV CARD for SUB CARD'
                    ELSE
                        comments
                        || '; '
                        || 'PARENT_CARD_REF_ID is not found in MV CARD for SUB CARD'
                END
        WHERE
            parent_card_ref_id IS NULL
            AND card_reference_id IS NOT NULL
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and FUNCTIONAL_TYPE not in ('FIBER SHUFFLE')
                                    ;

        COMMIT;

	  --dbms_output.put_line('--------------------4.PARENT_CARD_REF_ID is null for SUB CARD update ended------------' || systimestamp);

	  --dbms_output.put_line('--------------------5.CARD_REFERENCE_ID is null for SUB SUB CARD update started------------' || systimestamp);

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'CARD_REFERENCE_ID is not found in MV CARD for SUB SUB CARD'
                    ELSE
                        comments
                        || '; '
                        || 'CARD_REFERENCE_ID is not found in MV CARD for SUB SUB CARD'
                END
        WHERE
            card_reference_id IS NULL
            AND sub_sub_card_reference_id IS NOT NULL
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and FUNCTIONAL_TYPE not in ('FIBER SHUFFLE')
                                    ;

        COMMIT;

	  --dbms_output.put_line('--------------------5.CARD_REFERENCE_ID is null for SUB SUB CARD update ended------------' || systimestamp);

      --dbms_output.put_line('--------------------6.SLOT_NAME or SLOT_NUMBER is null for  CARD update started------------' || systimestamp);

      ----3. slot name is null----

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'SLOT_NAME or SLOT_NUMBER is null for CARD'
                    ELSE
                        comments
                        || '; '
                        || 'SLOT_NAME is null for  CARD'
                END
        WHERE
            parent_slot_reference_id IS NOT NULL
            AND parent_card_ref_id IS NOT NULL
            AND ( parent_slot_name IS NULL
                  OR parent_slot_number IS NULL )
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and FUNCTIONAL_TYPE not in ('FIBER SHUFFLE')
                                    ;

        COMMIT;

	  --dbms_output.put_line('--------------------6.SLOT_NAME or SLOT_NUMBER is nullfor  CARD update ended------------' || systimestamp);

	  --dbms_output.put_line('--------------------7.SLOT_NAME or SLOT_NUMBER is null for SUB CARD update started------------' || systimestamp);

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'SLOT_NAME OR SLOT_NUMBER is null for SUB CARD'
                    ELSE
                        comments
                        || '; '
                        || 'SLOT_NAME OR SLOT_NUMBER is null for SUB CARD'
                END
        WHERE
            sub_slot_reference_id IS NOT NULL
            AND card_reference_id IS NOT NULL
            AND ( sub_slot_name IS NULL
                  OR sub_slot_numer IS NULL )
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and FUNCTIONAL_TYPE not in ('FIBER SHUFFLE')
                                    ;

        COMMIT;

	  	 --dbms_output.put_line('--------------------7.SLOT_NAME or SLOT_NUMBER is null for SUB CARD update ended------------' || systimestamp);

		 --dbms_output.put_line('--------------------8.SLOT_NAME or SLOT_NUMBER is null for SUB  SUB CARD update started------------' || systimestamp);

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'SLOT_NAME or SLOT_NUMBER is null for SUB SUB CARD'
                    ELSE
                        comments
                        || '; '
                        || 'SLOT_NAME or SLOT_NUMBER is null for SUB SUB CARD'
                END
        WHERE
            sub_sub_slot_reference_id IS NOT NULL
            AND sub_sub_card_reference_id IS NOT NULL
            AND ( sub_sub_slot_name IS NULL
                  OR sub_sub_slot_numer IS NULL )
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and FUNCTIONAL_TYPE not in ('FIBER SHUFFLE')
                                    ;

        COMMIT;

	  		 --dbms_output.put_line('--------------------8.SLOT_NAME or SLOT_NUM is null for SUB  SUB CARD update ended------------' || systimestamp);


      -----9.port_reference_id is null ------

	  	  	--dbms_output.put_line('--------------------9.PORT_REFERENCE_ID is not found for SUB PORT update started------------' || systimestamp);

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'PORT_REFERENCE_ID is not found for SUB PORT'
                    ELSE
                        comments
                        || '; '
                        || 'PORT_REFERENCE_ID is not found for SUB PORT'
                END
        WHERE
            port_reference_id IS NULL
            AND sub_port_reference_id IS NOT NULL
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and port_type <> 'MPO' and port_status <> 'SPARE'
                                    ;

        COMMIT;

	  	  	--dbms_output.put_line('--------------------9.PORT_REFERENCE_ID is not found for SUB PORT update ended------------' || systimestamp);

			--dbms_output.put_line('--------------------10.PORT_REFERENCE_ID is not found for CHANNEL NAME/NUMBER update started------------' || systimestamp);

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'PORT_REFERENCE_ID is not found for CHANNEL NAME/NUMBER'
                    ELSE
                        comments
                        || '; '
                        || 'PORT_REFERENCE_ID is not found for CHANNEL NAME/NUMBER'
                END
        WHERE
            port_reference_id IS NULL
            AND channel_name IS NOT NULL
            AND sequence_number IS NOT NULL
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and port_type <> 'MPO' and port_status <> 'SPARE'
                                    ;

        COMMIT;

	  	  --dbms_output.put_line('--------------------10.PORT_REFERENCE_ID is not found for CHANNEL NAME/NUMBER update ended------------' || systimestamp);

      ----6.port name is null---

          --dbms_output.put_line('--------------------11.PORT_NAME or PORT_NUMBER is not found for PORT in MV Logical Port update started------------' || systimestamp);

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'PORT_NAME or PORT_NUMBER is not found for PORT in MV Logical Port'
                    ELSE
                        comments
                        || '; '
                        || 'PORT_NAME or PORT_NUMBER is not found for PORT in MV Logical Port'
                END
        WHERE
            port_reference_id IS NOT NULL
            AND ( port_name IS NULL
                  OR port_number IS NULL )
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and port_type <> 'MPO' and port_status <> 'SPARE'
                                    ;

        COMMIT;

	            --dbms_output.put_line('--------------------11.PORT_NAME or PORT_NUMBER is not found for PORT in MV Logical Port ended------------' || systimestamp);

				--dbms_output.put_line('--------------------12.PORT_NAME or PORT_NUMBER is not found for SUB PORT in MV Logical Port started------------' || systimestamp);

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'PORT_NAME or PORT_NUMBER is not found for SUB PORT in MV Logical Port'
                    ELSE
                        comments
                        || '; '
                        || 'PORT_NAME or PORT_NUMBER is not found for SUB PORT in MV Logical Port'
                END
        WHERE
            sub_port_reference_id IS NOT NULL
            AND ( sub_port_name IS NULL
                  OR sub_port_number IS NULL )
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and port_type <> 'MPO' and port_status <> 'SPARE'
                                    ;

        COMMIT;

	  --dbms_output.put_line('--------------------12.PORT_NAME or PORT_NUMBER is not found for SUB PORT in MV Logical Port update ended------------' || systimestamp);



				--dbms_output.put_line('--------------------13.TID_PHYSICAL is null  update started------------' || systimestamp);

      -----13.TID_PHYSICAL is null -----

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'TID_PHYSICAL IS NULL IN MV EQP'
                    ELSE
                        comments
                        || '; '
                        || 'TID_PHYSICAL IS NULL IN MV EQP'
                END
        WHERE
            tid_physical IS NULL
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND nvl(shelf_type, 'Not Available') NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB',
                                                          'TL', 'LGX' )

      --and port_type <> 'MPO' and port_status <> 'SPARE'
                                                          ;

        COMMIT;

	  				--dbms_output.put_line('--------------------13.TID_PHYSICAL is null  update ended------------' || systimestamp);

					--dbms_output.put_line('--------------------14.LOGICAL_SHELF is null  update started------------' || systimestamp);

      -----14.LOGICAL_SHELF is null -----

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'LOGICAL_SHELF IS NULL IN MV EQP'
                    ELSE
                        comments
                        || '; '
                        || 'LOGICAL_SHELF IS NULL IN MV EQP'
                END
        WHERE
            logical_shelf IS NULL
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and port_type <> 'MPO' and port_status <> 'SPARE'
                                    ;

        COMMIT;

	  					--dbms_output.put_line('--------------------14.LOGICAL_SHELF is null  update ended------------' || systimestamp);

						--dbms_output.put_line('--------------------15.TID_LOGICAL is null  update started------------' || systimestamp);

	 -----15.TID_LOGICAL IS null -----

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'TID_LOGICAL IS NULL IN MV EQP'
                    ELSE
                        comments
                        || '; '
                        || 'TID_LOGICAL IS NULL IN MV EQP'
                END
        WHERE
            tid_logical IS NULL
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and port_type <> 'MPO' and port_status <> 'SPARE'
                                    ;

        COMMIT;

	  						--dbms_output.put_line('--------------------15.TID_LOGICAL is null  update ended------------' || systimestamp);

	  						--dbms_output.put_line('--------------------16.RACK_NAME is null  update started------------' || systimestamp);

	  	 -----16.RACK_NAME IS null -----

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'RACK_NAME IS NULL IN MV EQP'
                    ELSE
                        comments
                        || '; '
                        || 'RACK_NAME IS NULL IN MV EQP'
                END
        WHERE
            rack_name IS NULL
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and port_type <> 'MPO' and port_status <> 'SPARE'
                                    ;

        COMMIT;

	  	  						--dbms_output.put_line('--------------------16.RACK_NAME is null  update ended------------' || systimestamp);


									--dbms_output.put_line('--------------------17.ALTERNATE_NAME is null  update started------------' || systimestamp);

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'ALTERNATE_NAME IS NULL IN MV EQP'
                    ELSE
                        comments
                        || '; '
                        || 'ALTERNATE_NAME IS NULL IN MV EQP'
                END
        WHERE
            alternate_name IS NULL
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and port_type <> 'MPO' and port_status <> 'SPARE'
                                    ;

        COMMIT;

	  	  						--dbms_output.put_line('--------------------17.ALTERNATE_NAME is null  update ended------------' || systimestamp);


								--dbms_output.put_line('--------------------18.Port Present in NTLS missing from CND  update started------------' || systimestamp);

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'Port Present in NTLS missing from CND'
                    ELSE
                        comments
                        || '; '
                        || 'Port Present in NTLS missing from CND'
                END
        WHERE
                icoe_port_missing_flag = 'Y'
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and port_type <> 'MPO' and port_status <> 'SPARE'
                                    ;

        COMMIT;

	  	  						--dbms_output.put_line('--------------------18.Port Present in NTLS missing from CND  update ended------------' || systimestamp);


								--dbms_output.put_line('--------------------19.LOGICAL_SLOT_NAME IS MISSING update started------------' || systimestamp);

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'LOGICAL_SLOT_NAME IS MISSING'
                    ELSE
                        comments
                        || '; '
                        || 'LOGICAL_SLOT_NAME IS MISSING'
                END
        WHERE
            parent_slot_name IS NULL
            AND parent_card_ref_id IS NULL
            AND port_reference_id IS NOT NULL
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )
            AND replace(eqp_reference_id, 'IVAPP:', '') IN (
                SELECT
                    eqp_reference_id
                FROM
                    vnadsprd.icoe_equipment_tbl
                WHERE
                    decode(network_type, 'ULH', eqp_type, functional_type) IN ( 'CMD44 C-BAND RED (45-88)', 'CMD44 C-BAND BLUE (1-44)',
                                                                                'FIM TYPE 1', 'FIBER_SHUFFLE', 'FIBER SHUFFLE' )
            );

        COMMIT;

	  	  					--dbms_output.put_line('--------------------19.LOGICAL_SLOT_NAME IS MISSING update ended------------' || systimestamp);

						    --dbms_output.put_line('--------------------20.CARD is supressed in MV CARD update started------------' || systimestamp);

  /*      UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'CARD is supressed in MV CARD'
                    ELSE
                        comments
                        || '; '
                        || 'CARD is supressed in MV CARD'
                END
        WHERE
                parent_card_supress_ind = 'S'
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and port_type <> 'MPO' and port_status <> 'SPARE'
                                    ;

        COMMIT; */

	  	  					--dbms_output.put_line('--------------------20.CARD is supressed in MV CARD update ended------------' || systimestamp);

							--dbms_output.put_line('--------------------21.SUB CARD is supressed in MV CARD update started------------' || systimestamp);

    /*    UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'SUB CARD is supressed in MV CARD'
                    ELSE
                        comments
                        || '; '
                        || 'SUB CARD is supressed in MV CARD'
                END
        WHERE
                card_supress_ind = 'S'
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and port_type <> 'MPO' and port_status <> 'SPARE'
                                    ;

        COMMIT; */

	  	  					--dbms_output.put_line('--------------------21.SUB CARD is supressed in MV CARD update ended------------' || systimestamp);

							--dbms_output.put_line('--------------------22.SUB SUB CARD is supressed in MV CARD update started------------' || systimestamp);

    /*    UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            comments =
                CASE
                    WHEN comments IS NULL THEN
                        'SUB SUB CARD is supressed in MV CARD'
                    ELSE
                        comments
                        || '; '
                        || 'SUB SUB CARD is supressed in MV CARD'
                END
        WHERE
                sub_sub_card_supress_ind = 'S'
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' )

      --and port_type <> 'MPO' and port_status <> 'SPARE'
                                    ;

        COMMIT; */

	  	  					--dbms_output.put_line('--------------------22.SUB SUB CARD is supressed in MV CARD update ended------------' || systimestamp);

        UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
            icoe_port_missing_flag = 'N'
        WHERE
                trail_name = 'I1001/OTUC2/LNBHNYLB/LYBRNYLB'
            AND port_reference_id IN ( 50779832, 50779789, 50779789, 50779832 );

        COMMIT;
	----update statatment for eqp_status  = 'PRE_INVENTORY'
--	dbms_output.put_line('--------------------23.Shelf satuts is PRE INVENTORY update started------------' || systimestamp);

	UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
		comments =
                CASE
                    WHEN comments IS NULL THEN
                        'Shelf Status is PRE INVENTORY'
                    ELSE
                        comments
                        || '; '
                        || 'Shelf Status is PRE INVENTORY'
                END
        WHERE
            eqp_status = 'PRE_INVENTORY'
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' ) ;
        COMMIT;

	--dbms_output.put_line('--------------------23.Shelf satuts is PRE INVENTORY update ended------------' || systimestamp);	

----update statatment for card_status  = 'PRE_INVENTORY'
--	dbms_output.put_line('--------------------24.Card satuts is PRE INVENTORY update started------------' || systimestamp);

	UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
		comments =
                CASE
                    WHEN comments IS NULL THEN
                        'Card Status is PRE INVENTORY'
                    ELSE
                        comments
                        || '; '
                        || 'Card Status is PRE INVENTORY'
                END
        WHERE
            parent_card_status = 'PRE_INVENTORY'
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' ) ;
        COMMIT;		
	--dbms_output.put_line('--------------------24.card satuts is PRE INVENTORY update ended------------' || systimestamp);	


	----update statatment for Subard_status  = 'PRE_INVENTORY'
	--dbms_output.put_line('--------------------24.Card satuts is PRE INVENTORY update started------------' || systimestamp);

	UPDATE vnadsprd.ien_markt_readiness_ntls_data

		SET		
		comments =
                CASE
                    WHEN comments IS NULL THEN
                        'Sub Card status is PRE INVENTORY'
                    ELSE
                        comments
                        || '; '
                        || 'Sub Card status is PRE INVENTORY'
                END
        WHERE
            card_status = 'PRE_INVENTORY'
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' ) ;
        COMMIT;		
	--dbms_output.put_line('--------------------24.card satuts is PRE INVENTORY update ended------------' || systimestamp);	

	----update statatment for port_status  = 'PRE_INVENTORY'
--	dbms_output.put_line('--------------------25.Port satuts is PRE INVENTORY update started------------' || systimestamp);

	UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
		comments =
                CASE
                    WHEN comments IS NULL THEN
                        'Port satuts is PRE INVENTORY'
                    ELSE
                        comments
                        || '; '
                        || 'Port satuts is PRE INVENTORY'
                END
        WHERE
            port_status = 'PRE_INVENTORY'
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' ) ;
        COMMIT;		
	dbms_output.put_line('--------------------25.Port status is PRE INVENTORY update ended------------' || systimestamp);	

		----update statatment for sub_port_status  = 'PRE_INVENTORY'
--	dbms_output.put_line('--------------------25.Port status is PRE INVENTORY update started------------' || systimestamp);

	UPDATE vnadsprd.ien_markt_readiness_ntls_data
        SET
		    comments =
                CASE
                    WHEN comments IS NULL THEN
                        'SubPort status is PRE INVENTORY'
                    ELSE
                        comments
                        || '; '
                        || 'SubPort status is PRE INVENTORY'
                END
         WHERE
            sub_port_status = 'PRE_INVENTORY'
            AND type NOT IN ( 'ODN', 'LCI', 'CCORE_OFFNET_INFRASTRUCTURE', 'CCORE_LAG_INFRASTRUCTURE', 'CCORE_INFRASTRUCTURE',
                              'CCORE_INTF_INFRASTRUCTURE' )
            AND trail_status NOT IN ( 'IMPLEMENT ERROR', 'PENDING DESIGN', 'PENDING_DESIGN', 'DESIGN_COMPLETE', 'PENDING_CANCEL',
                                      'PENDING CANCEL', 'DESIGN COMPLETE', 'PND FIBER DSGN', 'FIBER DSGN COMPL', 'IN ACTIVE',
                                      'DISCONNECTED', 'DELETED', 'CANCELLED', 'PND CHANGE CANCEL', 'RESERVATION_COPIED',
                                      'RESERVATION_CREATED', 'RESERVATION_EXPIRED' )
            AND shelf_type NOT IN ( 'PATCH_PANEL', 'PATCH', 'PATCH PANEL', 'FDB', 'TL',
                                    'LGX' ) ;
        COMMIT;		
	--dbms_output.put_line('--------------------26.Port status is PRE INVENTORY update ended------------' || systimestamp);			
    END;
END;