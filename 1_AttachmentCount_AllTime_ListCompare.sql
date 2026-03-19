/* ============================================================================================
   Purpose:
     - Count occurrences of URL-based attachments and URL text references across multiple
       ENTITY types (ASSET, CONDITIONS, WORKORDERS, PROJECT, etc.).
     - This version removes the 24?month timeframe completely.
     - URL matching now uses CONTAINS matching:
           filename LIKE '%' || prefix || '%'
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
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE c.filename LIKE '%' || u.url_pref || '%')
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
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE c.filename LIKE '%' || u.url_pref || '%')
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
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE c.filename LIKE '%' || u.url_pref || '%')
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
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE c.filename LIKE '%' || u.url_pref || '%')
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
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE c.filename LIKE '%' || u.url_pref || '%')
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
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE c.filename LIKE '%' || u.url_pref || '%')
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
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE c.filename LIKE '%' || u.url_pref || '%')
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
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE c.filename LIKE '%' || u.url_pref || '%')
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
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE c.filename LIKE '%' || u.url_pref || '%')
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
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE c.filename LIKE '%' || u.url_pref || '%')
           )
      FROM dual

    /* TEXT FIELDS  CONTAINS MATCH */
    UNION ALL

    /* ASSET (image) */
    SELECT 'ASSET',
           (SELECT COUNT(*)
              FROM mnt.asset a
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE a.image LIKE '%' || u.url_pref || '%')
           )
      FROM dual

    UNION ALL

    /* STANDARDTASK (longdescript) */
    SELECT 'STANDARDTASK',
           (SELECT COUNT(*)
              FROM mnt.standardtask a
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE a.longdescript LIKE '%' || u.url_pref || '%')
           )
      FROM dual

    UNION ALL

    /* STANDARDJOB */
    SELECT 'STANDARDJOB',
           (SELECT COUNT(*)
              FROM mnt.standardjob a
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE a.longdescript LIKE '%' || u.url_pref || '%')
           )
      FROM dual

    UNION ALL

    /* WORKREQUEST (longdescript) */
    SELECT 'WORKREQUEST',
           (SELECT COUNT(*)
              FROM mnt.workrequest a
             WHERE a.site_oi IN (SELECT site_oi FROM sites)
               AND a.wrstatus IN (1,2,3,6)
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE a.longdescript LIKE '%' || u.url_pref || '%')
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
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE a.longdescript LIKE '%' || u.url_pref || '%')
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
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE a.commen LIKE '%' || u.url_pref || '%')
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
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE e.mscomment LIKE '%' || u.url_pref || '%')
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
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE a.taskcomment LIKE '%' || u.url_pref || '%')
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
                     SELECT 1 FROM url_prefixes u
                      WHERE e.epprojectsha LIKE '%' || u.url_pref || '%'
                         OR e.epdescriptio LIKE '%' || u.url_pref || '%'
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
               AND EXISTS (SELECT 1 FROM url_prefixes u
                            WHERE s3.docmgmtfile LIKE '%' || u.url_pref || '%')
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
