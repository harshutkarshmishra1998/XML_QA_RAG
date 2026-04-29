truncate table vnadsprd.naut_trail_tmp1

```sql
INSERT INTO `your_project.your_dataset.vnadsprd`.naut_trail_tmp1
SELECT DISTINCT trail_name, status, trail_id
FROM `your_project.your_dataset.vnadsprd`.naut_trail
WHERE trail_name IN (
  SELECT trail_name
  FROM (
    SELECT trail_name, COUNT(*) AS count
    FROM `your_project.your_dataset.vnadsprd`.naut_trail
    GROUP BY trail_name
  )
  WHERE count > 1
)
```

truncate table vnadsprd.naut_trail_tmp2

```sql
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
    `your_project.your_dataset.vnadsprd`.naut_trail_tmp1 trail
  JOIN 
    vnadsprd.NAUT_TRAIL_STATUS_PRTY_LKP lkp
  ON 
    trail.trail_status = lkp.trail_status
)
GROUP BY 
  trail_name
```

truncate table vnadsprd.naut_trail_tmp3

```sql
INSERT INTO `your_project.your_dataset.vnadsprd`.naut_trail_tmp3
SELECT 
  trail.trail_name, 
  trail.trail_status, 
  trail.trail_id
FROM 
  `your_project.your_dataset.vnadsprd`.naut_trail_tmp1 trail
INNER JOIN 
  vnadsprd.NAUT_TRAIL_STATUS_PRTY_LKP lkp
  ON trail.trail_status = lkp.trail_status
INNER JOIN 
  vnadsprd.naut_trail_tmp2 tst1
  ON trail.trail_name = tst1.trail_name
  AND lkp.priority = tst1.priority
  AND lkp.sub_priority = tst1.sub_priority
```

truncate table vnadsprd.naut_trail_tmp

```sql
INSERT INTO `your_project.your_dataset.vnadsprd`.naut_trail_tmp
SELECT t.*
FROM `your_project.your_dataset.vnadsprd`.naut_trail t
WHERE t.trail_name NOT IN (
  SELECT trail_name
  FROM `your_project.your_dataset.vnadsprd`.naut_trail_tmp3
)
AND t.version IN (
  SELECT MAX(tt.version)
  FROM `your_project.your_dataset.vnadsprd`.naut_trail tt
  WHERE t.trail_name = tt.trail_name
  GROUP BY tt.trail_name
)
```

```sql
INSERT INTO `your_project.your_dataset.vnadsprd`.naut_trail_tmp
SELECT t.*
FROM `your_project.your_dataset.vnadsprd`.naut_trail t
WHERE t.trail_id IN (
  SELECT DISTINCT trail_id
  FROM `your_project.your_dataset.vnadsprd`.naut_trail_tmp3
)
AND t.version IN (
  SELECT MAX(tt.version)
  FROM `your_project.your_dataset.vnadsprd`.naut_trail tt
  WHERE t.trail_name = tt.trail_name
  AND tt.trail_id IN (
    SELECT trail_id
    FROM `your_project.your_dataset.vnadsprd`.naut_trail_tmp3
  )
  GROUP BY tt.trail_name
)
```

TRUNCATE TABLE vnadsprd.IEN_MR_TRAIL_MASTER

TRUNCATE TABLE vnadsprd.IEN_MR_NTLS_CKT_CHANNEL_STG

ALTER INDEX vnadsprd.IDX_IEN_MR_NTLS_CKT_CHANNEL UNUSABLE

ALTER INDEX vnadsprd.IDX_IEN_MR_NTLS_CKT_CHANNEL REBUILD

TRUNCATE TABLE vnadsprd.IEN_MR_CHANNEL_TEMP

ALTER INDEX vnadsprd.IDX1_IEN_MR_CHANNEL_TEMP UNUSABLE

ALTER INDEX vnadsprd.IDX2_IEN_MR_CHANNEL_TEMP UNUSABLE

ALTER INDEX vnadsprd.IDX1_IEN_MR_CHANNEL_TEMP REBUILD

ALTER INDEX vnadsprd.IDX2_IEN_MR_CHANNEL_TEMP REBUILD

truncate table vnadsprd.IEN_MRKT_READINSS_NTLS_CKT_STG

ALTER INDEX vnadsprd.IDX1_IEN_ICON_CKT_STG UNUSABLE

ALTER INDEX vnadsprd.IDX1_IEN_ICON_CKT_STG REBUILD

truncate table vnadsprd.IEN_MR_NTLS_ICON_CKT_TMP

truncate table vnadsprd.IEN_MR_NTLS_ICON_CKT_STG

ALTER INDEX vnadsprd.IDX_IEN_MR_NTLS_ICON_CKT UNUSABLE

ALTER INDEX vnadsprd.IDX1_IEN_MR_NTLS_ICON_CKT UNUSABLE

ALTER INDEX vnadsprd.IDX2_IEN_MR_NTLS_ICON_CKT UNUSABLE

ALTER INDEX vnadsprd.IDX_IEN_MR_NTLS_ICON_CKT REBUILD

ALTER INDEX vnadsprd.IDX1_IEN_MR_NTLS_ICON_CKT REBUILD

ALTER INDEX vnadsprd.IDX2_IEN_MR_NTLS_ICON_CKT REBUILD

TRUNCATE TABLE vnadsprd.IEN_MR_ICOE_PORT_TMP

 TRUNCATE TABLE vnadsprd.IEN_MARKT_READINESS_NTLS_DATA

ALTER index vnadsprd.IDX_IEN_MR_NTLS_DATA UNUSABLE

ALTER index vnadsprd.IDX_IEN_MR_NTLS_DATA REBUILD

truncate table vnadsprd.TEMP_IEN_MR_NTLS

 drop index vnadsprd.IDX_TEMP_IEN_MR_NTLS

 create index vnadsprd.IDX_TEMP_IEN_MR_NTLS on vnadsprd.TEMP_IEN_MR_NTLS(TRAIL_NAME)

truncate table vnadsprd.IEN_NTLS_CKT_HIER_LKP

TRUNCATE TABLE vnadsprd.naut_trail_tmp1

```sql
INSERT INTO `your_project.your_dataset.vnadsprd`.naut_trail_tmp1
SELECT DISTINCT trail_name, status, trail_id
FROM `your_project.your_dataset.vnadsprd`.naut_trail
WHERE trail_name IN (
  SELECT trail_name
  FROM (
    SELECT DISTINCT trail_name, status
    FROM `your_project.your_dataset.vnadsprd`.naut_trail
  )
  GROUP BY trail_name
  HAVING COUNT(*) > 1
)
```

```sql
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
    `your_project.your_dataset.vnadsprd`.naut_trail_tmp1 trail
  JOIN 
    vnadsprd.NAUT_TRAIL_STATUS_PRTY_LKP lkp
  ON 
    trail.trail_status = lkp.trail_status
)
GROUP BY 
  trail_name
```

```sql
INSERT INTO `your_project.your_dataset.vnadsprd`.naut_trail_tmp3
SELECT 
  trail.trail_name, 
  trail.trail_status, 
  trail.trail_id
FROM 
  `your_project.your_dataset.vnadsprd`.naut_trail_tmp1 trail
INNER JOIN 
  vnadsprd.NAUT_TRAIL_STATUS_PRTY_LKP lkp
  ON trail.trail_status = lkp.trail_status
INNER JOIN 
  vnadsprd.naut_trail_tmp2 tst1
  ON trail.trail_name = tst1.trail_name
  AND lkp.priority = tst1.priority
  AND lkp.sub_priority = tst1.sub_priority;
```

```sql
INSERT INTO `your_project.your_dataset.vnadsprd`.naut_trail_tmp
SELECT t.*
FROM `your_project.your_dataset.vnadsprd`.naut_trail t
WHERE t.trail_name NOT IN (
  SELECT trail_name FROM `your_project.your_dataset.vnadsprd`.naut_trail_tmp3
)
AND t.version IN (
  SELECT MAX(tt.version)
  FROM `your_project.your_dataset.vnadsprd`.naut_trail tt
  WHERE t.trail_name = tt.trail_name
  GROUP BY tt.trail_name
);
```

```sql
INSERT INTO `your_project.your_dataset.vnadsprd`.naut_trail_tmp
SELECT t.*
FROM `your_project.your_dataset.vnadsprd`.naut_trail t
WHERE t.trail_id IN (
  SELECT DISTINCT trail_id
  FROM `your_project.your_dataset.vnadsprd`.naut_trail_tmp3
)
AND t.version = (
  SELECT MAX(tt.version)
  FROM `your_project.your_dataset.vnadsprd`.naut_trail tt
  WHERE t.trail_name = tt.trail_name
  AND tt.trail_id IN (
    SELECT trail_id
    FROM `your_project.your_dataset.vnadsprd`.naut_trail_tmp3
  )
);
```

```sql
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
  CURRENT_TIMESTAMP
FROM (
  SELECT
    trail_id,
    parent_trail_id,
    channel_name
  FROM
    `your_project.your_dataset.vnadsprd`.naut_trail_element
  WHERE
    (trail_id, IFNULL(parent_trail_id, 0), IFNULL(CAST(channel_name AS INT64), 0)) IN (
      SELECT
        trail_id,
        IFNULL(parent_trail_id, 0),
        IFNULL(MIN(CAST(channel_name AS INT64)), 0) AS channel_name
      FROM
        `your_project.your_dataset.vnadsprd`.naut_trail_element
      WHERE
        element_type = 'P'
        AND REGEXP_CONTAINS(channel_name, r'^[0-9\.]+$') = TRUE
      GROUP BY
        trail_id,
        parent_trail_id
    )
    AND element_type = 'P'
    AND REGEXP_CONTAINS(channel_name, r'^[0-9\.]+$') = TRUE
  UNION ALL
  SELECT
    trail_id,
    parent_trail_id,
    MIN(channel_name) AS channel_name
  FROM
    `your_project.your_dataset.vnadsprd`.naut_trail_element
  WHERE
    element_type = 'P'
    AND NOT REGEXP_CONTAINS(channel_name, r'^[0-9\.]+$')
  GROUP BY
    trail_id,
    parent_trail_id
) AS aa
WHERE
  NOT EXISTS (
    SELECT
      1
    FROM (
      SELECT
        e.trail_id,
        'SVC_DESCRIPTOR' AS source
      FROM
        `your_project.your_dataset.vnadsprd`.naut_SERVICE_EXT e
      WHERE
        e.SVC_DESCRIPTOR_NAME LIKE '%FLEX%'
        AND e.trail_id = aa.trail_id
      UNION ALL
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
```

```sql
INSERT INTO `your_project.your_dataset.vnadsprd`.ien_mr_ntls_ckt_channel_stg(
  trail_id,
  parent_trail_id,
  channel_name,
  last_refreshed_ts
)
SELECT
  trail_id,
  parent_trail_id,
  channel_name,
  CURRENT_TIMESTAMP
FROM (
  SELECT DISTINCT
    ele.trail_id,
    ele.parent_trail_id,
    a.attribute_value AS channel_name
  FROM
    `your_project.your_dataset.vnadsprd`.naut_trail_element ele
  JOIN
    `your_project.your_dataset.vnadsprd`.naut_TRAIL_ATTRIBUTES a
  ON
    ele.trail_id = a.trail_id
    AND a.ATTRIBUTE_NAME = 'CENTRAL_FREQUENCY'
    AND ele.element_type = 'P'
) aa
WHERE
  EXISTS (
    SELECT
      1
    FROM (
      SELECT
        e.trail_id,
        'SVC_DESCRIPTOR' AS source
      FROM
        `your_project.your_dataset.vnadsprd`.naut_SERVICE_EXT e
      WHERE
        e.SVC_DESCRIPTOR_NAME LIKE '%FLEX%'
        AND e.trail_id = aa.trail_id
      UNION ALL
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
```

```sql
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
  vnadsprd.ien_mr_trail_master vw,
  vnadsprd.naut_trail_component_element tce,
  vnadsprd.icoe_site_tbl asite,
  vnadsprd.icoe_site_tbl zsite
WHERE
  vw.trail_id = e.trail_id
  AND e.element_type = 'E'
  AND e.source IN ('IVAPP_PORT', 'IVAPP_LOGICAL', 'IVAPP_PANEL')
  AND tce.element_id = e.element_id
  AND vw.A_SITE_NAME = asite.clli
  AND vw.Z_SITE_NAME = zsite.clli

UNION ALL
SELECT
  x.trail_id,
  x.trail_name,
  x.status,
  x.type,
  x.version,
  tce.sequence,
  t.a_port_id AS port_reference_id,
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
  vnadsprd.naut_trail_channel tc,
  vnadsprd.naut_trail_component_element tce,
  vnadsprd.naut_trail_element te,
  vnadsprd.ien_mr_channel_temp x
WHERE
  t.trail_id = x.parent_trail_id
  AND tc.parent_trail_id = x.parent_trail_id
  AND tc.channel_name = CAST(x.channel_name AS STRING)
  AND te.channel_name = CAST(x.channel_name AS STRING)
  AND te.parent_trail_id = x.parent_trail_id
  AND tce.element_id = te.element_id

UNION ALL
SELECT
  x.trail_id,
  x.trail_name,
  x.status,
  x.type,
  x.version,
  tce.sequence,
  t.z_port_id AS port_reference_id,
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
  vnadsprd.naut_trail_channel tc,
  vnadsprd.naut_trail_component_element tce,
  vnadsprd.naut_trail_element te,
  vnadsprd.ien_mr_channel_temp x
WHERE
  t.trail_id = x.parent_trail_id
  AND tc.parent_trail_id = x.parent_trail_id
  AND tc.channel_name = CAST(x.channel_name AS STRING)
  AND te.channel_name = CAST(x.channel_name AS STRING)
  AND te.parent_trail_id = x.parent_trail_id
  AND tce.element_id = te.element_id

UNION ALL
SELECT
  x.trail_id,
  x.trail_name,
  x.status,
  x.type,
  x.version,
  tce.sequence,
  t.a_port_id_reverse AS port_reference_id,
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
  vnadsprd.naut_trail_channel tc,
  vnadsprd.naut_trail_component_element tce,
  vnadsprd.naut_trail_element te,
  vnadsprd.ien_mr_channel_temp x
WHERE
  t.trail_id = x.parent_trail_id
  AND tc.parent_trail_id = x.parent_trail_id
  AND tc.channel_name = CAST(x.channel_name AS STRING)
  AND te.channel_name = CAST(x.channel_name AS STRING)
  AND te.parent_trail_id = x.parent_trail_id
  AND tce.element_id = te.element_id

UNION ALL
SELECT
  x.trail_id,
  x.trail_name,
  x.status,
  x.type,
  x.version,
  tce.sequence,
  t.z_port_id_reverse AS port_reference_id,
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
  vnadsprd.naut_trail_channel tc,
  vnadsprd.naut_trail_component_element tce,
  vnadsprd.naut_trail_element te,
  vnadsprd.ien_mr_channel_temp x
WHERE
  t.trail_id = x.parent_trail_id
  AND tc.parent_trail_id = x.parent_trail_id
  AND tc.channel_name = CAST(x.channel_name AS STRING)
  AND te.channel_name = CAST(x.channel_name AS STRING)
  AND te.parent_trail_id = x.parent_trail_id
  AND tce.element_id = te.element_id

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
  vnadsprd.naut_trail_tmp vw,
  vnadsprd.naut_trail_component_element tce,
  vnadsprd.icoe_site_tbl asite,
  vnadsprd.icoe_site_tbl zsite,
  vnadsprd.ien_mr_trail_master mstr
WHERE
  vw.trail_id = e.trail_id
  AND e.element_type = 'K'
  AND tce.element_id = e.element_id
  AND vw.A_SITE_NAME = asite.clli
  AND vw.Z_SITE_NAME = zsite.clli
  AND vw.trail_id = mstr.trail_id

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
  vnadsprd.naut_trail_tmp vw,
  vnadsprd.naut_trail_component_element tce,
  vnadsprd.icoe_site_tbl asite,
  vnadsprd.icoe_site_tbl zsite,
  vnadsprd.ien_mr_trail_master mstr
WHERE
  vw.trail_id = e.trail_id
  AND e.element_type = 'K'
  AND tce.element_id = e.element_id
  AND vw.A_SITE_NAME = asite.clli
  AND vw.Z_SITE_NAME = zsite.clli
  AND vw.trail_id = mstr.trail_id

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
  vnadsprd.naut_trail_tmp vw,
  vnadsprd.naut_trail_component_element tce,
  vnadsprd.icoe_site_tbl asite,
  vnadsprd.icoe_site_tbl zsite,
  vnadsprd.ien_mr_trail_master mstr
WHERE
  vw.trail_id = e.trail_id
  AND e.element_type = 'S'
  AND tce.element_id = e.element_id
  AND vw.A_SITE_NAME = asite.clli
  AND vw.Z_SITE_NAME = zsite.clli
  AND vw.trail_id = mstr.trail_id

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
  vnadsprd.naut_trail_tmp vw,
  vnadsprd.naut_trail_component_element tce,
  vnadsprd.icoe_site_tbl asite,
  vnadsprd.icoe_site_tbl zsite,
  vnadsprd.ien_mr_trail_master mstr
WHERE
  vw.trail_id = e.trail_id
  AND e.element_type = 'S'
  AND tce.element_id = e.element_id
  AND vw.A_SITE_NAME = asite.clli
  AND vw.Z_SITE_NAME = zsite.clli
  AND vw.trail_id = mstr.trail_id;
```

```sql
INSERT INTO `your_project.your_dataset.vnadsprd`.ien_mr_ntls_icon_ckt_tmp (
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
SELECT DISTINCT
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
  mvp.PVNR_PORT_ID AS pport_reference_id,
  mvp.related_port_ref_id AS prelated_port_ref_id,
  mvlp.port_reference_id AS lport_reference_id,
  mvlp.related_port_ref_id AS lrelated_port_ref_id,
  CASE
    WHEN mvp.related_port_ref_id IS NOT NULL
      AND mvlp1.port_reference_id IS NOT NULL
      AND mvp.related_port_ref_id = mvlp1.port_reference_id THEN 'Y'
    ELSE NULL
  END AS is_related_lport_flag,
  ttp.last_refreshed_ts AS snapshot_dt
FROM 
  `your_project.your_dataset.vnadsprd`.ien_mrkt_readinss_ntls_ckt_stg ttp
  JOIN `your_project.your_dataset.vnadsprd`.ICOE_svt1plp_cnd mvp
    ON ttp.port_reference_id = mvp.PVNR_PORT_ID
  JOIN `your_project.your_dataset.vnadsprd`.ICOE_PVNR_T_LOGICAL_PORT_CND mvlp
    ON mvp.PVNR_PORT_ID = mvlp.port_reference_id
  JOIN `your_project.your_dataset.vnadsprd`.ICOE_PVNR_T_LOGICAL_PORT_CND mvlp1
    ON mvp.related_port_ref_id = mvlp1.port_reference_id;
```

```sql
INSERT INTO `your_project.your_dataset.vnadsprd`.ien_mr_ntls_icon_ckt_stg (
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
SELECT DISTINCT
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
  tmp.snapshot_dt AS last_refreshed_ts,
  eqp.shelf_type,
  port.physical_port_reference_id
FROM 
  `your_project.your_dataset.vnadsprd`.ien_mr_ntls_icon_ckt_tmp tmp
  JOIN vnadsprd.icoe_equipment_tbl eqp
  ON port.eqp_reference_id = eqp.eqp_reference_id
  JOIN vnadsprd.icoe_pvnr_t_logical_port_cnd port
  ON tmp.port_reference_id = port.port_reference_id
WHERE 
  (is_related_lport_flag IS NULL OR lport_reference_id IS NOT NULL);
```

```sql
INSERT INTO `your_project.your_dataset.vnadsprd`.ien_mr_icoe_port_tmp
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
    `your_project.your_dataset.VNADSPRD`.ICOE_PVNR_T_LOGICAL_PORT_CND phy
INNER JOIN 
    `your_project.your_dataset.VNADSPRD`.ICOE_PVNR_T_LOGICAL_PORT_CND virtual1
ON 
    virtual1.port_reference_id = phy.physical_port_reference_id
INNER JOIN 
    `your_project.your_dataset.vnadsprd`.ICOE_svt1plp_cnd mv_port
ON 
    virtual1.port_reference_id = mv_port.PVNR_PORT_ID
WHERE 
    mv_port.PORT_INSTNC_ID >= -1e20;
```

```sql
INSERT INTO `your_project.your_dataset.vnadsprd`.temp_ien_mr_ntls (
  trail_name,
  npreinventory_flag
)
SELECT DISTINCT
  trail_name,
  IF(npreinventory_flag > 0, 'Y', 'N') AS npreinventory_flag
FROM (
  SELECT
    ntls.trail_name,
    SUM(
      IF(
        trail_status = 'IN EFFECT'
        AND (
          eqp_status = 'PRE_INVENTORY'
          OR parent_card_status = 'PRE_INVENTORY'
          OR card_status = 'PRE_INVENTORY'
          OR port_status = 'PRE_INVENTORY'
          OR sub_port_status = 'PRE_INVENTORY'
          OR channel_status = 'PRE_INVENTORY'
        ),
        1,
        0
      )
    ) AS npreinventory_flag
  FROM
    `your_project.your_dataset.vnadsprd`.ien_markt_readiness_ntls_data ntls
  JOIN
    `your_project.your_dataset.vnadsprd`.ien_mr_trail_master mstr
  ON
    ntls.trail_name = mstr.trail_name
  GROUP BY
    ntls.trail_name
);
```

```sql
INSERT INTO `your_project.your_dataset.vnadsprd`.ien_ntls_ckt_hier_lkp (
  trail_id,
  trail_name,
  type,
  status,
  version,
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
  t1.parent_trail_id AS level1trail,
  t2.parent_trail_id AS level2trail,
  t3.parent_trail_id AS level3trail
FROM 
  `your_project.your_dataset.vnadsprd`.naut_trail t
  JOIN (
    SELECT DISTINCT
      trail_id,
      parent_trail_id
    FROM 
      `your_project.your_dataset.vnadsprd`.naut_trail_element
    WHERE 
      element_type = 'P'
  ) t1
  ON t.trail_id = t1.trail_id
  JOIN (
    SELECT DISTINCT
      trail_id,
      parent_trail_id
    FROM 
      `your_project.your_dataset.vnadsprd`.naut_trail_element
    WHERE 
      element_type = 'P'
  ) t2
  ON t1.parent_trail_id = t2.trail_id
  JOIN (
    SELECT DISTINCT
      trail_id,
      parent_trail_id
    FROM 
      `your_project.your_dataset.vnadsprd`.naut_trail_element
    WHERE 
      element_type = 'P'
  ) t3
  ON t2.parent_trail_id = t3.trail_id;
```

