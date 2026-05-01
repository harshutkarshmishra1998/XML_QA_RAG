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
    FROM (
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
    FROM (
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
        AND tce.element_id = e.element_id 
        AND vw.A_SITE_NAME = asite.clli 
        AND vw.Z_SITE_NAME = zsite.clli;

    UNION ALL

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
        AND tc.channel_name = TO_CHAR(x.channel_name) 
        AND te.channel_name = TO_CHAR(x.channel_name) 
        AND te.parent_trail_id = x.parent_trail_id 
        AND tce.element_id = te.element_id;

    UNION ALL

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
        AND tc.channel_name = TO_CHAR(x.channel_name) 
        AND te.channel_name = TO_CHAR(x.channel_name) 
        AND te.parent_trail_id = x.parent_trail_id 
        AND tce.element_id = te.element_id;

    UNION ALL

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
        AND tc.channel_name = TO_CHAR(x.channel_name) 
        AND te.channel_name = TO_CHAR(x.channel_name) 
        AND te.parent_trail_id = x.parent_trail_id 
        AND tce.element_id = te.element_id;

    UNION ALL

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
        AND tc.channel_name = TO_CHAR(x.channel_name) 
        AND te.channel_name = TO_CHAR(x.channel_name) 
        AND te.parent_trail_id = x.parent_trail_id 
        AND tce.element_id = te.element_id;

    UNION ALL

    SELECT DISTINCT 
        vw.trail_id,
        vw.trail_name,
        vw.status,
        vw.type,
        vw.version,
        tce.sequence,
        NULL AS port_reference_id,
        e.source,
        vw.a_port_aid,
        NULL AS sequence_number,
        vw.status AS port_status,
        e.element_type,
        vw.project_id,
        asite.clli AS aclli,
        zsite.clli AS zclli,
        CURRENT_TIMESTAMP(),
        vw.network_id AS network_type,
        vw.a_equipment_id AS equipment_id
    FROM 
        `your_project.your_dataset.vnadsprd`.naut_trail_element e,
        `your_project.your_dataset.vnadsprd`.naut_trail_tmp vw,
        `your_project.your_dataset.vnadsprd`.naut_trail_component_element tce,
        `your_project.your_dataset.vnadsprd`.icoe_site_tbl asite,
        `your_project.your_dataset.vnadsprd`.icoe_site_tbl zsite,
        `your_project.your_dataset.vnadsprd`.ien_mr_trail_master mstr
    WHERE 
        vw.trail_id = e.trail_id 
        AND e.element_type = 'K' 
        AND tce.element_id = e.element_id 
        AND vw.A_SITE_NAME = asite.clli 
        AND vw.Z_SITE_NAME = zsite.clli 
        AND vw.trail_id = mstr.trail_id;

    UNION ALL

    SELECT DISTINCT 
        vw.trail_id,
        vw.trail_name,
        vw.status,
        vw.type,
        vw.version,
        tce.sequence,
        NULL AS port_reference_id,
        e.source,
        vw.z_port_aid,
        NULL AS sequence_number,
        vw.status AS port_status,
        e.element_type,
        vw.project_id,
        asite.clli AS aclli,
        zsite.clli AS zclli,
        CURRENT_TIMESTAMP(),
        vw.network_id AS network_type,
        vw.z_equipment_id AS equipment_id
    FROM 
        `your_project.your_dataset.vnadsprd`.naut_trail_element e,
        `your_project.your_dataset.vnadsprd`.naut_trail_tmp vw,
        `your_project.your_dataset.vnadsprd`.naut_trail_component_element tce,
        `your_project.your_dataset.vnadsprd`.icoe_site_tbl asite,
        `your_project.your_dataset.vnadsprd`.icoe_site_tbl zsite,
        `your_project.your_dataset.vnadsprd`.ien_mr_trail_master mstr
    WHERE 
        vw.trail_id = e.trail_id 
        AND e.element_type = 'K' 
        AND tce.element_id = e.element_id 
        AND vw.A_SITE_NAME = asite.clli 
        AND vw.Z_SITE_NAME = zsite.clli 
        AND vw.trail_id = mstr.trail_id;

    UNION ALL

    SELECT DISTINCT 
        vw.trail_id,
        vw.trail_name,
        vw.status,
        vw.type,
        vw.version,
        tce.sequence,
        NULL AS port_reference_id,
        e.source,
        vw.z_port_aid,
        NULL AS sequence_number,
        vw.status AS port_status,
        e.element_type,
        vw.project_id,
        asite.clli AS aclli,
        zsite.clli AS zclli,
        CURRENT_TIMESTAMP(),
        vw.network_id AS network_type,
        IFNULL(vw.a_equipment_id, 1) AS equipment_id
    FROM 
        `your_project.your_dataset.vnadsprd`.naut_trail_element e,
        `your_project.your_dataset.vnadsprd`.naut_trail_tmp vw,
        `your_project.your_dataset.vnadsprd`.naut_trail_component_element tce,
        `your_project.your_dataset.vnadsprd`.icoe_site_tbl asite,
        `your_project.your_dataset.vnadsprd`.icoe_site_tbl zsite,
        `your_project.your_dataset.vnadsprd`.ien_mr_trail_master mstr
    WHERE 
        vw.trail_id = e.trail_id 
        AND e.element_type = 'S' 
        AND tce.element_id = e.element_id 
        AND vw.A_SITE_NAME = asite.clli 
        AND vw.Z_SITE_NAME = zsite.clli 
        AND vw.trail_id = mstr.trail_id;

    UNION ALL

    SELECT DISTINCT 
        vw.trail_id,
        vw.trail_name,
        vw.status,
        vw.type,
        vw.version,
        tce.sequence,
        NULL AS port_reference_id,
        e.source,
        vw.z_port_aid,
        NULL AS sequence_number,
        vw.status AS port_status,
        e.element_type,
        vw.project_id,
        asite.clli AS aclli,
        zsite.clli AS zclli,
        CURRENT_TIMESTAMP(),
        vw.network_id AS network_type,
        IFNULL(vw.z_equipment_id, 1) AS equipment_id
    FROM 
        `your_project.your_dataset.vnadsprd`.naut_trail_element e,
        `your_project.your_dataset.vnadsprd`.naut_trail_tmp vw,
        `your_project.your_dataset.vnadsprd`.naut_trail_component_element tce,
        `your_project.your_dataset.vnadsprd`.icoe_site_tbl asite,
        `your_project.your_dataset.vnadsprd`.icoe_site_tbl zsite,
        `your_project.your_dataset.vnadsprd`.ien_mr_trail_master mstr
    WHERE 
        vw.trail_id = e.trail_id 
        AND e.element_type = 'S' 
        AND tce.element_id = e.element_id 
        AND vw.A_SITE_NAME = asite.clli 
        AND vw.Z_SITE_NAME = zsite.clli 
        AND vw.trail_id = mstr.trail_id;

    COMMIT;

    ALTER INDEX `your_project.your_dataset.vnadsprd`.IDX_IEN_MR_NTLS_CKT_CHANNEL REBUILD;

    TRUNCATE TABLE `your_project.your_dataset.vnadsprd`.ien_mr_icoe_port_tmp;
    INSERT INTO `your_project.your_dataset.vnadsprd`.ien_mr_icoe_port_tmp (
        port_reference_id,
        port_status,
        port_type,
        aid,
        port_name,
        port_number,
        bandwidth_name,
        card_reference_id,
        eqp_reference_id,
        parent_port_ref_id,
        port_rel_nm,
        slot_type,
        logical_slot_name
    )
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
        phy.parent_port_ref
