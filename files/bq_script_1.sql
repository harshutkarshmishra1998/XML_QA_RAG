-- BLOCK 1
-- complexity_score: 2
-- used_llm: False
CREATE OR REPLACE EDITIONABLE PROCEDURE "VNADSPRD"."SP_IEN_MR_NTLS_PRTCKTS_LOAD_CND" AS
BEGIN
    DECLARE
        v_rownum NUMBER := 0

-- BLOCK 2
-- complexity_score: 5
-- used_llm: True
-- note: Applied deterministic function mapping (NVL/SYSDATE/etc.).
-- note: Applied simple scalar assignment rewrites.
-- note: Detected Oracle outer join markers and annotated them for LLM conversion.
-- note: Qualified simple table references.
```sql
BEGIN
    TRUNCATE TABLE `your_project.your_dataset.vnadsprd`.naut_trail_tmp1;
    INSERT INTO `your_project.your_dataset.vnadsprd`.naut_trail_tmp1
    SELECT DISTINCT 
        trail_name, 
        status, 
        trail_id 
    FROM 
        `your_project.your_dataset.vnadsprd`.naut_trail 
    WHERE 
        trail_name IN (
            SELECT 
                trail_name 
            FROM 
                `your_project.your_dataset.vnadsprd`.naut_trail 
            GROUP BY 
                trail_name 
            HAVING 
                COUNT(1) > 1
        );

    COMMIT;

    TRUNCATE TABLE `your_project.your_dataset.vnadsprd`.naut_trail_tmp2;
    INSERT INTO `your_project.your_dataset.vnadsprd`.naut_trail_tmp2
    SELECT 
        trail_name, 
        MIN(priority) AS priority, 
        MIN(sub_priority) AS sub_priority 
    FROM 
        (
            SELECT 
                trail.trail_name, 
                trail.trail_status, 
                lkp.priority, 
                lkp.sub_priority 
            FROM 
                `your_project.your_dataset.vnadsprd`.naut_trail_tmp1 trail, 
                `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_STATUS_PRTY_LKP lkp 
            WHERE 
                trail.trail_status = lkp.trail_status 
            ORDER BY 
                trail.trail_name, 
                lkp.priority, 
                lkp.sub_priority
        ) 
    GROUP BY 
        trail_name;

    COMMIT;

    TRUNCATE TABLE `your_project.your_dataset.vnadsprd`.naut_trail_tmp3;
    INSERT INTO `your_project.your_dataset.vnadsprd`.naut_trail_tmp3
    SELECT 
        trail.trail_name, 
        trail.trail_status, 
        trail.trail_id 
    FROM 
        `your_project.your_dataset.vnadsprd`.naut_trail_tmp1 trail, 
        `your_project.your_dataset.vnadsprd`.NAUT_TRAIL_STATUS_PRTY_LKP lkp, 
        `your_project.your_dataset.vnadsprd`.naut_trail_tmp2 tst1 
    WHERE 
        trail.trail_status = lkp.trail_status 
        AND trail.trail_name = tst1.trail_name 
        AND lkp.priority = tst1.priority 
        AND lkp.sub_priority = tst1.sub_priority;

    COMMIT;

    TRUNCATE TABLE `your_project.your_dataset.vnadsprd`.naut_trail_tmp;
    INSERT INTO `your_project.your_dataset.vnadsprd`.naut_trail_tmp
    SELECT 
        t.* 
    FROM 
        `your_project.your_dataset.vnadsprd`.naut_trail t 
    WHERE 
        t.trail_name NOT IN (
            SELECT 
                trail_name 
            FROM 
                `your_project.your_dataset.vnadsprd`.naut_trail_tmp3
        ) 
        AND t.version IN (
            SELECT 
                MAX(tt.version) 
            FROM 
                `your_project.your_dataset.vnadsprd`.naut_trail tt 
            WHERE 
                t.trail_name = tt.trail_name 
            GROUP BY 
                tt.trail_name
        );

    COMMIT;

    INSERT INTO `your_project.your_dataset.vnadsprd`.naut_trail_tmp
    SELECT 
        t.* 
    FROM 
        `your_project.your_dataset.vnadsprd`.naut_trail t 
    WHERE 
        t.trail_id IN (
            SELECT 
                DISTINCT trail_id 
            FROM 
                `your_project.your_dataset.vnadsprd`.naut_trail_tmp3
        ) 
        AND t.version IN (
            SELECT 
                MAX(tt.version) 
            FROM 
                `your_project.your_dataset.vnadsprd`.naut_trail tt 
            WHERE 
                t.trail_name = tt.trail_name 
                AND tt.trail_id IN (
                    SELECT 
                        trail_id 
                    FROM 
                        `your_project.your_dataset.vnadsprd`.naut_trail_tmp3
                ) 
            GROUP BY 
                tt.trail_name
        );

    COMMIT;

    TRUNCATE TABLE `your_project.your_dataset.vnadsprd`.ien_mr_trail_master;
    INSERT INTO `your_project.your_dataset.vnadsprd`.ien_mr_trail_master (
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
    SELECT 
        trail_id,
        trail_name,
        type,
        status,
        version,
        A_SITE_NAME,
        Z_SITE_NAME,
        CURRENT_TIMESTAMP(),
        project_id,
        network_id
    FROM 
        (
            SELECT 
                DISTINCT tw.trail_id,
                tw.trail_name,
                tw.type,
                tw.status,
                tw.version,
                tw.A_SITE_NAME,
                tw.Z_SITE_NAME,
                tw.project_id,
                tw.network_id
            FROM 
                `your_project.your_dataset.vnadsprd`.naut_sa_prtckts_info t,
                `your_project.your_dataset.vnadsprd`.naut_trail_tmp tw
            WHERE 
                t.trail_name = tw.trail_name
        );

    COMMIT;

    TRUNCATE TABLE `your_project.your_dataset.vnadsprd`.ien_mr_ntls_ckt_channel_stg;
    ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX_IEN_MR_NTLS_CKT_CHANNEL UNUSABLE;
    INSERT INTO `your_project.your_dataset.vnadsprd`.ien_mr_ntls_ckt_channel_stg (
        trail_id,
        parent_trail_id,
        channel_name,
        last_refreshed_ts
    )
    SELECT 
        trail_id,
        parent_trail_id,
        channel_name,
        CURRENT_TIMESTAMP()
    FROM 
        (
            SELECT 
                trail_id,
                parent_trail_id,
                channel_name
            FROM 
                `your_project.your_dataset.vnadsprd`.naut_trail_element
            WHERE 
                (trail_id, IFNULL(parent_trail_id, 0), IFNULL(to_number(channel_name), 0)) IN (
                    SELECT 
                        trail_id, IFNULL(parent_trail_id, 0), IFNULL(MIN(to_number(channel_name)), 0) channel_name
                    FROM 
                        `your_project.your_dataset.vnadsprd`.naut_trail_element
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
                `your_project.your_dataset.vnadsprd`.naut_trail_element
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
        WHERE NOT EXISTS (
            SELECT 
                1 
            FROM 
                (
                    SELECT 
                        e.trail_id, 
                        'SVC_DESCRIPTOR' AS source 
                    FROM 
                        `your_project.your_dataset.vnadsprd`.naut_SERVICE_EXT e 
                    WHERE 
                        e.SVC_DESCRIPTOR_NAME LIKE '%FLEX%'
                        AND e.trail_id = aa.trail_id 
                    UNION 
                    SELECT 
                        a.trail_id, 
                        'CENTRAL_FREQUENCY' AS source 
                    FROM 
                        `your_project.your_dataset.vnadsprd`.naut_TRAIL_ATTRIBUTES a 
                    WHERE 
                        a.ATTRIBUTE_NAME = 'CENTRAL_FREQUENCY'
                        AND a.attribute_value IS NOT NULL 
                        AND a.trail_id = aa.trail_id
                )
        );

    COMMIT;

    INSERT INTO `your_project.your_dataset.vnadsprd`.ien_mr_ntls_ckt_channel_stg (
        trail_id,
        parent_trail_id,
        channel_name,
        last_refreshed_ts
    )
    SELECT 
        trail_id,
        parent_trail_id,
        channel_name,
        CURRENT_TIMESTAMP()
    FROM 
        (
            SELECT 
                DISTINCT ele.trail_id,
                parent_trail_id,
                a.attribute_value channel_name
            FROM 
                `your_project.your_dataset.vnadsprd`.naut_trail_element ele, 
                `your_project.your_dataset.vnadsprd`.naut_TRAIL_ATTRIBUTES a
            WHERE 
                ele.trail_id = a.trail_id
                AND a.ATTRIBUTE_NAME = 'CENTRAL_FREQUENCY'
                AND ele.element_type = 'P'
        ) aa
        WHERE EXISTS (
            SELECT 
                1 
            FROM 
                (
                    SELECT 
                        e.trail_id, 
                        'SVC_DESCRIPTOR' AS source 
                    FROM 
                        `your_project.your_dataset.vnadsprd`.naut_SERVICE_EXT e 
                    WHERE 
                        e.SVC_DESCRIPTOR_NAME LIKE '%FLEX%'
                        AND e.trail_id = aa.trail_id 
                    UNION 
                    SELECT 
                        a.trail_id, 
                        'CENTRAL_FREQUENCY' AS source 
                    FROM 
                        `your_project.your_dataset.vnadsprd`.naut_TRAIL_ATTRIBUTES a 
                    WHERE 
                        a.ATTRIBUTE_NAME = 'CENTRAL_FREQUENCY'
                        AND a.attribute_value IS NOT NULL 
                        AND a.trail_id = aa.trail_id
                )
        );

    COMMIT;

    ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX_IEN_MR_NTLS_CKT_CHANNEL REBUILD;

    TRUNCATE TABLE `your_project.your_dataset.vnadsprd`.ien_mr_channel_temp;
    ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX1_IEN_MR_CHANNEL_TEMP UNUSABLE;
    ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX2_IEN_MR_CHANNEL_TEMP UNUSABLE;
    INSERT INTO `your_project.your_dataset.vnadsprd`.ien_mr_channel_temp (
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
        asite.clli AS aclli,
        zsite.clli AS zclli,
        vw.network_type
    FROM 
        `your_project.your_dataset.vnadsprd`.ien_mr_ntls_ckt_channel_stg te,
        `your_project.your_dataset.vnadsprd`.ien_mr_trail_master vw,
        `your_project.your_dataset.vnadsprd`.icoe_site_tbl asite,
        `your_project.your_dataset.vnadsprd`.icoe_site_tbl zsite
    WHERE 
            vw.trail_id = te.trail_id
        AND vw.A_SITE_NAME = asite.clli 
        AND vw.Z_SITE_NAME = zsite.clli 
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

    ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX1_IEN_MR_CHANNEL_TEMP REBUILD;
    ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX2_IEN_MR_CHANNEL_TEMP REBUILD;

    TRUNCATE TABLE `your_project.your_dataset.vnadsprd`.ien_mrkt_readinss_ntls_ckt_stg;
    ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX1_IEN_ICON_CKT_STG UNUSABLE;
    INSERT INTO `your_project.your_dataset.vnadsprd`.ien_mrkt_readinss_ntls_ckt_stg (
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
        NULL AS sequence_number,
        NULL AS port_status,
        e.element_type,
        vw.project_id,
        asite.clli AS aclli,
        zsite.clli AS zclli,
        CURRENT_TIMESTAMP(),
        vw.network_type,
        NULL AS equipment_id
    FROM 
        `your_project.your_dataset.vnadsprd`.naut_trail_element e,
        `your_project.your_dataset.vnadsprd`.ien_mr_trail_master vw,
        `your_project.your_dataset.vnadsprd`.naut_trail_component_element tce,
        `your_project.your_dataset.vnadsprd`.icoe_site_tbl asite,
        `your_project.your_dataset.vnadsprd`.icoe_site_tbl zsite
    WHERE 
            vw.trail_id = e.trail_id
        AND e.element_type = 'E'
        AND e.source IN ('IVAPP_PORT', 'IVAPP_LOGICAL', 'IVAPP_PANEL')
        AND tce.element_id = e.element_id
        AND vw.A_SITE_NAME = asite.clli 
        AND vw.Z_SITE_NAME = zsite.clli 
    UNION 
    SELECT 
        x.trail_id,
        x.trail_name,
        x.status,
        x.type,
        x.version,
        tce.sequence,
        t.a_port_id port_reference_id,
        'NAUTILUS' AS source,
        tc.channel_name,
        tc.sequence_number,
        tc.status AS port_status,
        te.element_type,
        x.project_id,
        x.aclli,
        x.zclli,
        CURRENT_TIMESTAMP(),
        x.network_type,
        NULL AS equipment_id
    FROM 
        `your_project.your_dataset.vnadsprd`.naut_trail_tmp t,
        `your_project.your_dataset.vnadsprd`.naut_trail_channel tc,
        `your_project.your_dataset.vnadsprd`.naut_trail_component_element tce,
        `your_project.your_dataset.vnadsprd`.naut_trail_element te,
        `your_project.your_dataset.vnadsprd`.ien_mr_channel_temp x
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
        'NAUTILUS' AS source,
        tc.channel_name,
        tc.sequence_number,
        tc.status AS port_status,
        te.element_type,
        x.project_id,
        x.aclli,
        x.zclli,
        CURRENT_TIMESTAMP(),
        x.network_type,
        NULL AS equipment_id
    FROM 
        `your_project.your_dataset.vnadsprd`.naut_trail_tmp t,
        `your_project.your_dataset.vnadsprd`.naut_trail_channel tc,
        `your_project.your_dataset.vnadsprd`.naut_trail_component_element tce,
        `your_project.your_dataset.vnadsprd`.naut_trail_element te,
        `your_project.your_dataset.vnadsprd`.ien_mr_channel_temp x
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
        'NAUTILUS' AS source,
        tc.channel_name,
        tc.sequence_number,
        tc.status AS port_status,
        te.element_type,
        x.project_id,
        x.aclli,
        x.zclli,
        CURRENT_TIMESTAMP(),
        x.network_type,
        NULL AS equipment_id
    FROM 
        `your_project.your_dataset.vnadsprd`.naut_trail_tmp t,
        `your_project.your_dataset.vnadsprd`.naut_trail_channel tc,
        `your_project.your_dataset.vnadsprd`.naut_trail_component_element tce,
        `your_project.your_dataset.vnadsprd`.naut_trail_element te,
        `your_project.your_dataset.vnadsprd`.ien_mr_channel_temp x
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
        'NAUTILUS' AS source,
        tc.channel_name,
        tc.sequence_number,
        tc.status AS port_status,
        te.element_type,
        x.project_id,
        x.aclli,
        x.zclli,
        CURRENT_TIMESTAMP(),
        x.network_type,
        NULL AS equipment_id
    FROM 
        `your_project.your_dataset.vnadsprd`.naut_trail_tmp t,
        `your_project.your_dataset.vnadsprd`.naut_trail_channel tc,
        `your_project.your_dataset.vnadsprd`.naut_trail_component_element tce,
        `your_project.your_dataset.vnadsprd`.naut_trail_element te,
        `your_project.your_dataset.vnadsprd`.ien_mr_channel_temp x
    WHERE 
            t.trail_id = x.parent_trail_id
        AND tc.parent_trail_id = x.parent_trail_id
        AND tc.channel_name = to_char(x.channel_name)
        AND te.channel_name = to_char(x.channel_name)
        AND te
