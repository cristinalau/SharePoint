/* ============================================================================================
   Purpose:
     - Count occurrences of URL-based attachments and URL text references across multiple
       ENTITY types (ASSET, CONDITIONS, WORKORDERS, PROJECT, etc.).
     - This version removes the 24?month timeframe completely.
     - URL matching now uses normalized, case-insensitive URL matching.
     - It handles all three requested improvements:
           * case-insensitive matching
           * partial URL matching (query params, fragments, trailing slashes)
           * domain-level matching when the same site/domain appears
     - It is also optimized to run faster by:
           * normalizing the URL list once in a small CTE
           * using INSTR instead of LIKE for substring checks
           * materializing the small lookup CTEs before scanning large tables
     - URL prefix list is controlled via url_prefixes CTE.
   ============================================================================================ */

WITH

/* ------------------------------
   Site Scope (Water + Drainage)
   ------------------------------ */
sites AS (
    SELECT 107 AS site_oi FROM dual UNION ALL
    SELECT 58  FROM dual UNION ALL
    SELECT 50  FROM dual UNION ALL
    SELECT 30  FROM dual UNION ALL
    SELECT 40  FROM dual UNION ALL
    SELECT 38  FROM dual
),

/* -------------------------------------------------------
   URL Prefix List
   ------------------------------------------------------- */
url_prefixes AS (
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/drainage/sites/HS' AS url_pref FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/drainage/sites/HS/HSTeam' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/drainage/sites/SS/OpMetrics' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/drainage/sites/SS/OpMetrics/PBR Measures' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/drainage/sites/SS/TechTrain/team-restricted' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/drainage/sites/SS/TechTrain/team-restricted/development' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/drainage/sites/projects/capital/PBR2022' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/water/sites/fkb/fin' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/water/sites/fkb/WSCFP' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/water/sites/teams/HS' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/water/sites/teams/sc' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/water/sites/teams/Training' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/water/sites/teams/Training/Competency' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/water/sites/teams/Training/OperatorResourceCentre' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/water/sites/teams/Training/Rainy Day Resources' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/water/sites/teams/Training/team-resticted' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/water/sites/EDM/OE' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/water/sites/EDM/OE/KPI' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/water/sites/projects' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/water/sites/projects/CapitalProjects' FROM dual
    UNION ALL
    SELECT 'https://epcorweb.epcor.ca/en-ca/departments/water/sites/projects/frc' FROM dual
    UNION ALL
    SELECT 'https://extranet.epcor.ca/EPDTA' FROM dual
    UNION ALL
    SELECT 'https://extranet.epcor.ca/water/Depreciation' FROM dual
    UNION ALL
    SELECT 'https://extranet.epcor.ca/water/goldbar' FROM dual
    UNION ALL
    SELECT 'https://extranet.epcor.ca/water/WWCDS' FROM dual
),

/* -------------------------------------------------------
   Normalized URL Terms
   -------------------------------------------------------
   Build a small, reusable lookup table of match terms once.
   Matching rules:
     1) Case-insensitive: all values are LOWER(...)
     2) Partial URL aware: strips query strings, fragments, and trailing slashes
     3) Domain aware: also matches by domain alone when needed

   Why this is faster:
     - The URL list is normalized once, not inside every table scan
     - INSTR(...) is generally lighter than LIKE '%...%'
     - MATERIALIZE encourages Oracle to cache the small lookup CTE
   ------------------------------------------------------- */
url_normalized AS (
    SELECT /*+ MATERIALIZE */
           LOWER(TRIM(url_pref)) AS original_url,
           LOWER(REGEXP_REPLACE(TRIM(url_pref), '[?#].*$', '')) AS no_query_fragment,
           LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(url_pref), '[?#].*$', ''), '/+$', '')) AS no_trailing_slash,
           LOWER(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(url_pref), '[?#].*$', ''), '/+$', ''), '^https?://', '')) AS no_scheme,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(url_pref), '[?#].*$', ''), '/+$', ''), '^https?://', ''), '^[^/]+')) AS host_full,
           LOWER(REGEXP_SUBSTR(REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(url_pref), '[?#].*$', ''), '/+$', ''), '^https?://', ''), '^[^/]+'), '^[^.]+')) AS host_short,
           LOWER(REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(url_pref), '[?#].*$', ''), '/+$', ''), '^https?://', ''), '/.*$')) AS path_only
      FROM url_prefixes
),
url_match_terms AS (
    SELECT /*+ MATERIALIZE */ DISTINCT match_term
      FROM (
            /* Exact full URL */
            SELECT original_url AS match_term
              FROM url_normalized
             WHERE original_url IS NOT NULL
               AND original_url <> ''

            UNION ALL

            /* Same URL without query strings/fragments/trailing slashes */
            SELECT no_scheme AS match_term
              FROM url_normalized
             WHERE no_scheme IS NOT NULL
               AND no_scheme <> ''

            UNION ALL

            /* Full host name, e.g. epcorweb.epcor.ca */
            SELECT host_full AS match_term
              FROM url_normalized
             WHERE host_full IS NOT NULL
               AND host_full <> ''

            UNION ALL

            /* Short host token fallback, e.g. epcorweb or extranet */
            SELECT host_short AS match_term
              FROM url_normalized
             WHERE host_short IS NOT NULL
               AND host_short <> ''
           )
)

/* ========================================================
   UNION of all entity-specific URL-based counts
   ======================================================== */
unioned AS (

    /* ================================================================================ 
       URL ATTACHMENTS  NON-EMBEDDED
       ================================================================================ */

    SELECT 'ASSET' AS entity,
           (SELECT COUNT(DISTINCT c.filename)
              FROM mnt.asset a
              JOIN oq.attcontainter b ON a.attachmentco_oi = b.attachmentcontaineroi
              JOIN oq.attachment c ON b.attachmentcontaineroi = c.attcontainer_oi
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND NVL(c.embedded,0) = 0
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(c.filename), u.match_term) > 0)
           ) AS count_value
      FROM dual

    UNION ALL

    /* ASSET embedded = 1 */
    SELECT 'ASSET',
           (SELECT COUNT(*)
              FROM mnt.asset a
              JOIN oq.attcontainter b ON a.attachmentco_oi = b.attachmentcontaineroi
              JOIN oq.attachment c ON b.attachmentcontaineroi = c.attcontainer_oi
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND c.embedded = 1
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(c.filename), u.match_term) > 0)
           )
      FROM dual

    UNION ALL

    /* CONDITIONINDICATOR */
    SELECT 'CONDITIONINDICATOR',
           (SELECT COUNT(DISTINCT c.filename)
              FROM mnt.conditionindicator a
              JOIN oq.attcontainter b ON a.attachmentco_oi = b.attachmentcontaineroi
              JOIN oq.attachment c ON b.attachmentcontaineroi = c.attcontainer_oi
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND NVL(c.embedded,0) = 0
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(c.filename), u.match_term) > 0)
           )
      FROM dual

    UNION ALL

    /* INDICATORREADING */
    SELECT 'INDICATORREADING',
           (SELECT COUNT(DISTINCT c.filename)
              FROM mnt.indicatorreading a
              JOIN oq.attcontainter b ON a.attachmentco_oi = b.attachmentcontaineroi
              JOIN oq.attachment c ON b.attachmentcontaineroi = c.attcontainer_oi
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND NVL(c.embedded,0) = 0
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(c.filename), u.match_term) > 0)
           )
      FROM dual

    UNION ALL

    /* MTCESCHEDULE */
    SELECT 'MTCESCHEDULE',
           (SELECT COUNT(DISTINCT c.filename)
              FROM mnt.mtceschedule a
              JOIN oq.attcontainter b ON a.attachmentco_oi = b.attachmentcontaineroi
              JOIN oq.attachment c ON b.attachmentcontaineroi = c.attcontainer_oi
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND NVL(c.embedded,0) = 0
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(c.filename), u.match_term) > 0)
           )
      FROM dual

    UNION ALL

    /* PROJECT */
    SELECT 'PROJECT',
           (SELECT COUNT(DISTINCT c.filename)
              FROM mnt.project a
              JOIN oq.attcontainter b ON a.attachmentco_oi = b.attachmentcontaineroi
              JOIN oq.attachment c ON b.attachmentcontaineroi = c.attcontainer_oi
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND NVL(c.embedded,0) = 0
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(c.filename), u.match_term) > 0)
           )
      FROM dual

    UNION ALL

    /* STANDARDTASK */
    SELECT 'STANDARDTASK',
           (SELECT COUNT(DISTINCT c.filename)
              FROM mnt.standardtask a
              JOIN oq.attcontainter b ON a.attachmentco_oi = b.attachmentcontaineroi
              JOIN oq.attachment c ON b.attachmentcontaineroi = c.attcontainer_oi
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND NVL(c.embedded,0) = 0
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(c.filename), u.match_term) > 0)
           )
      FROM dual

    UNION ALL

    /* WORKORDERS */
    SELECT 'WORKORDERS',
           (SELECT COUNT(DISTINCT c.filename)
              FROM mnt.workorders a
              JOIN oq.attcontainter b ON a.attachmentco_oi = b.attachmentcontaineroi
              JOIN oq.attachment c ON b.attachmentcontaineroi = c.attcontainer_oi
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND NVL(c.embedded,0) = 0
               AND a.wostatus IN (1,3,4,6,30,50,70)
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(c.filename), u.match_term) > 0)
           )
      FROM dual

    UNION ALL

    /* WORKORDERTASK */
    SELECT 'WORKORDERTASK',
           (SELECT COUNT(DISTINCT c.filename)
              FROM mnt.workordertask a
              JOIN oq.attcontainter b ON a.attachmentco_oi = b.attachmentcontaineroi
              JOIN oq.attachment c ON b.attachmentcontaineroi = c.attcontainer_oi
              JOIN mnt.workorders wo ON a.workorder_oi = wo.workordersoi
             WHERE wo.site_oi IN (SELECT site_oi FROM sites)
               AND NVL(c.embedded,0) = 0
               AND wo.wostatus IN (1,3,4,6,30,50,70)
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(c.filename), u.match_term) > 0)
           )
      FROM dual

    UNION ALL

    /* WORKREQUEST */
    SELECT 'WORKREQUEST',
           (SELECT COUNT(DISTINCT c.filename)
              FROM mnt.workrequest a
              JOIN oq.attcontainter b ON a.attachmentco_oi = b.attachmentcontaineroi
              JOIN oq.attachment c ON b.attachmentcontaineroi = c.attcontainer_oi
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND NVL(c.embedded,0) = 0
               AND a.wrstatus IN (1,2,3,6)
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(c.filename), u.match_term) > 0)
           )
      FROM dual

    /* TEXT FIELDS  CONTAINS MATCH */
    UNION ALL

    /* ASSET (image) */
    SELECT 'ASSET',
           (SELECT COUNT(*)
              FROM mnt.asset a
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(a.image), u.match_term) > 0)
           )
      FROM dual

    UNION ALL

    /* STANDARDTASK (longdescript) */
    SELECT 'STANDARDTASK',
           (SELECT COUNT(*)
              FROM mnt.standardtask a
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(a.longdescript), u.match_term) > 0)
           )
      FROM dual

    UNION ALL

    /* STANDARDJOB */
    SELECT 'STANDARDJOB',
           (SELECT COUNT(*)
              FROM mnt.standardjob a
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(a.longdescript), u.match_term) > 0)
           )
      FROM dual

    UNION ALL

    /* WORKREQUEST (longdescript) */
    SELECT 'WORKREQUEST',
           (SELECT COUNT(*)
              FROM mnt.workrequest a
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND a.wrstatus IN (1,2,3,6)
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(a.longdescript), u.match_term) > 0)
           )
      FROM dual

    UNION ALL

    /* WORKORDERTASK (longdescript) */
    SELECT 'WORKORDERTASK',
           (SELECT COUNT(*)
              FROM mnt.workordertask a
              JOIN mnt.workorders wo ON a.workorder_oi = wo.workordersoi
             WHERE wo.site_oi IN (SELECT site_oi FROM sites)
               AND wo.wostatus IN (1,3,4,6,30,50,70)
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(a.longdescript), u.match_term) > 0)
           )
      FROM dual

    UNION ALL

    /* WORKREQUESTCOMMENT */
    SELECT 'WORKREQUESTCOMMENT',
           (SELECT COUNT(*)
              FROM mnt.workrequestcomment a
              JOIN mnt.workrequest wr ON a.workrequest_oi = wr.workrequestoi
             WHERE wr.site_oi IN (SELECT site_oi FROM sites)
               AND wr.wrstatus IN (1,2,3,6)
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(a.commen), u.match_term) > 0)
           )
      FROM dual

    UNION ALL

    /* EPWORKORDERMILESTO */
    SELECT 'EPWORKORDERMILESTO',
           (SELECT COUNT(*)
              FROM mnt.workorders wo
              JOIN customerdata.epworkordermilesto e ON e.workorders_oi = wo.workordersoi
             WHERE wo.site_oi IN (SELECT site_oi FROM sites)
               AND wo.wostatus IN (1,3,4,6,30,50,70)
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(e.mscomment), u.match_term) > 0)
           )
      FROM dual

    UNION ALL

    /* ACTIVITYREPORT */
    SELECT 'ACTIVITYREPORT',
           (SELECT COUNT(*)
              FROM mnt.activityreport a
              JOIN mnt.workordertask wot ON a.wotask_oi = wot.workordertaskoi
              JOIN mnt.workorders w ON wot.workorder_oi = w.workordersoi
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND w.wostatus IN (1,3,4,6,30,50,70)
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(a.taskcomment), u.match_term) > 0)
           )
      FROM dual

    UNION ALL

    /* EPRECOMMENDATIONS */
    SELECT 'EPRECOMMENDATIONS',
           (SELECT COUNT(*)
              FROM mnt.asset a
              JOIN customerdata.eprecommendations e ON e.asset_oi = a.assetoi
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND EXISTS (
                     SELECT 1
                       FROM url_match_terms u
                      WHERE INSTR(LOWER(e.epprojectsha), u.match_term) > 0
                         OR INSTR(LOWER(e.epdescriptio), u.match_term) > 0
               )
           )
      FROM dual

    UNION ALL

    /* STDPROCEDUREVERSN */
    SELECT 'STDPROCEDUREVERSN',
           (SELECT COUNT(*)
              FROM (
                    (fnd.stdprocedures s
                      LEFT JOIN (fnd.site s1
                                LEFT JOIN mnt.asset a ON s1.topasset_oi = a.assetoi)
                        ON s.site_oi = s1.siteoi)
                      LEFT JOIN fnd.stdprocdoctypes s2
                        ON s.stddproctype_oi = s2.stdproceduresanddocstypesoi
                   )
              LEFT JOIN (
                    fnd.stdprocedureversn s3
                      LEFT JOIN (fnd.stdprocedures s4
                        LEFT JOIN (fnd.site s5
                          LEFT JOIN oq.timezone t ON s5.timezone_oi = t.omtimezoneoi)
                        ON s4.site_oi = s5.siteoi)
                    ON s3.stdprocedure_oi = s4.stdprocedureanddocumentsoi
                 )
                ON s.currentrev_oi = s3.stdprocedureversionoi
             WHERE EXISTS (
                     SELECT 1
                       FROM mnt.asset_1000036528
                      WHERE parent_oi = a.assetoi
                        AND child_oi  = 11
                   )
               AND EXISTS (SELECT 1 FROM url_match_terms u
                            WHERE INSTR(LOWER(s3.docmgmtfile), u.match_term) > 0)
           )
      FROM dual
)

/* ===============================
   FINAL ROLLUP BY ENTITY
   =============================== */
SELECT entity,
       SUM(count_value) AS total_count
  FROM unioned
 GROUP BY entity
 ORDER BY entity;
